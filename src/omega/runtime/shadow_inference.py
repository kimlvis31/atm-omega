"""Bounded data-only shadow owner; model Python runs only in research children.

GET methods read memory. One private thread owns candidate hashing/copying,
controller mutation and result verification. No result reaches a TS or broker.
"""

import json
import os
import re
import stat
import threading
import time
from collections import deque
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from omega.domain.errors import StorageError, ValidationError
from omega.research.artifacts import _status_descriptor
from omega.research.deployment import (
    ShadowCandidate,
    ShadowDeployment,
    ShadowModelContract,
    prepare_candidate,
    verify_fixture,
)
from omega.research.training_view import training_view
from omega.runtime import raw_shadow
from omega.runtime.settings import atomic_json

MAX_BYTES = 4_194_304
KINDS = frozenset(
    {
        "inspect_training",
        "prepare_fixture",
        "activate",
        "reset_stream",
        "resume",
        "pause",
        "predict",
        "predict_raw",
        "refresh",
    }
)
_PENDING = {"queued", "running", "waiting"}
_TERMINAL_CHILD = {"complete", "failed", "cancelled", "interrupted"}


class _Retired(RuntimeError):
    pass


def _json(value, limit=MAX_BYTES):
    raw = json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()
    if len(raw) > limit:
        raise ValidationError("shadow metadata capacity reached")
    return raw


def _copy(value):
    return json.loads(_json(value))


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate shadow field")
        result[key] = value
    return result


def _hex(value, size=32):
    return type(value) is str and re.fullmatch(f"[a-f0-9]{{{size}}}", value) is not None


def _id(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValidationError("invalid shadow operation identity")


def _parameters(kind, value):
    if type(kind) is not str or kind not in KINDS or type(value) is not dict:
        raise ValidationError("invalid shadow operation")
    result = json.loads(_json(value, 65_536))
    if kind == "predict_raw":
        return raw_shadow.parameters(result)
    required = {
        "inspect_training": {"training_job_id"},
        "prepare_fixture": {"training_job_id", "contract", "fixture_batch", "trusted_module"},
        "activate": {"candidate_id", "expected_revision"},
        "reset_stream": {"stream", "instrument", "expected_revision", "reason"},
        "resume": {"expected_revision"},
        "pause": {"expected_revision", "reason"},
        "predict": {
            "expected_revision",
            "stream",
            "instrument",
            "sequence",
            "open_time_ms",
            "as_of_ms",
            "deadline_ms",
            "input_digest",
            "batch",
            "schema_id",
            "features",
            "lookback",
        },
        "refresh": set(),
    }[kind]
    optional = {"fixture_expected"} if kind == "prepare_fixture" else set()
    if not required <= result.keys() or result.keys() - required - optional:
        raise ValidationError("unexpected or missing shadow parameters")
    if "expected_revision" in result and (
        type(result["expected_revision"]) is not int or result["expected_revision"] < 0
    ):
        raise ValidationError("shadow expected revision must be a nonnegative integer")
    if kind == "prepare_fixture":
        if not _hex(result["training_job_id"]) or result["trusted_module"] is not True:
            raise ValidationError("shadow fixture needs an explicit trusted completed training job")
        ShadowModelContract.parse(result["contract"])
    if kind == "inspect_training" and not _hex(result["training_job_id"]):
        raise ValidationError("training preview requires a completed job identity")
    if kind == "activate" and not _hex(result["candidate_id"], 64):
        raise ValidationError("invalid shadow candidate ID")
    return result


class ShadowInferenceService:
    def __init__(self, root, research, *, core_epoch, clock_ms=None):
        _id(core_epoch)
        self.root, self.research = Path(root).resolve(), research
        if (
            Path(research.root).resolve() != self.root
            or clock_ms is not None
            and not callable(clock_ms)
        ):
            raise ValidationError("shadow requires the same explicit research root and clock")
        self.core_epoch, self.clock_ms = (
            core_epoch,
            clock_ms or (lambda: time.time_ns() // 1_000_000),
        )
        self.path = self.root / "state/shadow-inference.json"
        self._condition = threading.Condition(threading.RLock())
        self._queue, self._thread = deque(), None
        self._closed = self._storage_failed = self._ready = False
        self._error, self._deployment = None, None
        self._data = None
        try:
            if self.path.exists():
                self._data = self._read()
            else:
                private = self.root / "state/shadow-candidates"
                if private.exists() and any(private.iterdir()):
                    raise StorageError("shadow state is missing beside existing candidates")
                deployment = ShadowDeployment(core_epoch)
                self._data = {
                    "schema_version": 1,
                    "core_epoch": core_epoch,
                    "deployment": deployment.snapshot(),
                    "candidates": {},
                    "operations": {},
                }
                self._deployment = deployment
                self._write(self._data)
        except Exception as error:
            self._storage_failed, self._error = True, type(error).__name__

    def _guard_paths(self):
        for path in (self.path.parent, self.path):
            if path.is_symlink() or path.is_junction():
                raise StorageError("shadow metadata cannot use linked paths")

    def _read(self):
        self._guard_paths()
        descriptor = _status_descriptor(self.path)
        try:
            details = os.fstat(descriptor)
            if (
                not stat.S_ISREG(details.st_mode)
                or getattr(details, "st_file_attributes", 0) & 0x400
                or details.st_size > MAX_BYTES
            ):
                raise StorageError("shadow metadata must be a bounded regular non-reparse file")
            stream = os.fdopen(descriptor, "rb")
            descriptor = None
            with stream:
                raw = stream.read(MAX_BYTES + 1)
        finally:
            if descriptor is not None:
                os.close(descriptor)
        if len(raw) > MAX_BYTES:
            raise StorageError("shadow metadata exceeds capacity")
        value = json.loads(raw, object_pairs_hook=_unique)
        if type(value) is not dict or set(value) != {"data", "checksum"}:
            raise StorageError("invalid shadow metadata envelope")
        data = value["data"]
        if (
            type(data) is not dict
            or set(data)
            != {"schema_version", "core_epoch", "deployment", "candidates", "operations"}
            or type(data["schema_version"]) is not int
            or data["schema_version"] != 1
            or type(data["candidates"]) is not dict
            or len(data["candidates"]) > 128
            or type(data["operations"]) is not dict
            or len(data["operations"]) > 128
            or value["checksum"] != sha256(_json(data)).hexdigest()
        ):
            raise StorageError("shadow metadata identity/checksum failed")
        _id(data["core_epoch"])
        for identity, row in data["operations"].items():
            _id(identity)
            if (
                type(row) is not dict
                or set(row)
                != {
                    "id",
                    "kind",
                    "parameters",
                    "execution_id",
                    "core_epoch",
                    "state",
                    "job_id",
                    "intent",
                    "candidate_id",
                    "result",
                    "error",
                    "cancellation",
                }
                or row.get("id") != identity
                or row.get("kind") not in KINDS
                or not _hex(row.get("execution_id"))
                or row.get("state")
                not in {"queued", "running", "waiting", "completed", "failed", "unknown", "expired"}
            ):
                raise StorageError("invalid shadow operation history")
            _id(row["core_epoch"])
            if (
                row["job_id"] is not None
                and not _hex(row["job_id"])
                or row["candidate_id"] is not None
                and not _hex(row["candidate_id"], 64)
                or row["error"] is not None
                and type(row["error"]) is not str
                or row["cancellation"]
                not in {None, "requested", "confirmed", "unknown", "unknown_job_id"}
                or row["intent"] is not None
                and (
                    type(row["intent"]) is not dict
                    or set(row["intent"]) != {"parameters", "request_id"}
                    or type(row["intent"]["parameters"]) is not dict
                    or row["intent"]["request_id"] is not None
                    and not _hex(row["intent"]["request_id"])
                )
            ):
                raise StorageError("invalid shadow dispatch history")
            _parameters(row["kind"], row["parameters"])
        for identity, candidate in data["candidates"].items():
            if (
                not _hex(identity, 64)
                or type(candidate) is not dict
                or set(candidate) != {"candidate_id", "path", "training_job_id", "fixture_verified"}
                or candidate.get("candidate_id") != identity
                or type(candidate.get("path")) is not str
                or not _hex(candidate.get("training_job_id"))
                or type(candidate.get("fixture_verified")) is not bool
            ):
                raise StorageError("invalid cached shadow candidate")
            self._private(candidate["path"])
        return data

    def _private(self, path):
        target = Path(path)
        base = self.root / "state/shadow-candidates"
        if (
            not target.is_absolute()
            or not target.is_relative_to(base)
            or len(target.relative_to(base).parts) != 2
        ):
            raise StorageError("shadow candidate escaped its private namespace")
        parts = target.relative_to(base).parts
        if not _hex(parts[0]) or parts[1] != "candidate":
            raise StorageError("invalid private candidate execution namespace")
        for check in (base, target.parent, target):
            if check.is_symlink() or check.is_junction():
                raise StorageError("private shadow candidates cannot use linked paths")
        return target

    def _write(self, data):
        self._guard_paths()
        envelope = {"data": data, "checksum": sha256(_json(data)).hexdigest()}
        _json(envelope)
        try:
            atomic_json(self.path, envelope)
        except OSError:
            self._storage_failed = True
            raise StorageError("shadow state commit failed") from None

    def _alive(self):
        if self._closed or self._storage_failed:
            raise _Retired("shadow owner is retired or recording unavailable")

    def _commit(self, identity=None, updates=None, *, deployment=None, candidates=None):
        with self._condition:
            self._alive()
            data = deepcopy(self._data)
            data["core_epoch"] = self.core_epoch
            if deployment is not None:
                data["deployment"] = deployment.snapshot()
            if identity is not None:
                data["operations"][identity].update(_copy(updates))
            if candidates:
                data["candidates"].update(_copy(candidates))
            if data == self._data:
                return
            self._write(data)
            self._data = data
            if deployment is not None:
                self._deployment = deployment
            self._condition.notify_all()

    def start(self):
        with self._condition:
            self._alive()
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._loop, name="Omega-ShadowInference", daemon=True
                )
                self._thread.start()
        return self.snapshot()

    def submit(self, identity, kind, parameters):
        _id(identity)
        checked = _parameters(kind, parameters)
        with self._condition:
            self._alive()
            if not self._ready:
                raise ValidationError("shadow owner is still starting or unavailable")
            previous = self._data["operations"].get(identity)
            if previous:
                if previous["kind"] != kind or previous["parameters"] != checked:
                    raise ValidationError("shadow operation ID conflicts with its original request")
                return deepcopy(previous)
            if kind in {"predict", "predict_raw"} and any(
                row["kind"] == "predict_raw"
                and row["core_epoch"] == self.core_epoch
                and row["state"] in _PENDING | {"unknown"}
                and row["parameters"]["stream"] == checked.get("stream")
                and row["parameters"]["instrument"] == checked.get("instrument")
                and row["parameters"]["expected_revision"] == checked.get("expected_revision")
                for row in self._data["operations"].values()
            ):
                raise ValidationError("raw preparation already owns this shadow stream")
            if len(self._queue) >= 8 or len(self._data["operations"]) >= 128:
                raise ValidationError("shadow operation queue/history capacity reached")
            row = {
                "id": identity,
                "kind": kind,
                "parameters": checked,
                "execution_id": uuid4().hex,
                "core_epoch": self.core_epoch,
                "state": "queued",
                "job_id": None,
                "intent": None,
                "candidate_id": None,
                "result": None,
                "error": None,
                "cancellation": None,
            }
            data = deepcopy(self._data)
            data["operations"][identity] = row
            self._write(data)
            self._data = data
            self._queue.append(identity)
            self._condition.notify_all()
            return deepcopy(row)

    def status(self, identity):
        _id(identity)
        with self._condition:
            if self._data is None or identity not in self._data["operations"]:
                raise ValidationError("shadow operation not found")
            result = deepcopy(self._data["operations"][identity])
            if self._storage_failed and result["state"] in _PENDING:
                result.update(state="unknown", error="shadow_recording_unavailable")
            return result

    def snapshot(self):
        with self._condition:
            data = deepcopy(self._data)
            deployment = None if data is None else data["deployment"]
            if deployment is not None and (not self._ready or self._closed or self._storage_failed):
                deployment["document"]["paused"] = True
                deployment["checksum"] = sha256(_json(deployment["document"])).hexdigest()
            return {
                "schema_version": 1,
                "scope": "paper_shadow_only",
                "core_epoch": self.core_epoch,
                "state": "unavailable"
                if self._storage_failed or self._error
                else "ready"
                if self._ready
                else "starting",
                "closed": self._closed,
                "error": self._error,
                "deployment": deployment,
                "candidates": [] if data is None else list(data["candidates"].values()),
                "operations": []
                if data is None
                else [self.status(key) for key in data["operations"]],
            }

    def _now(self):
        value = self.clock_ms()
        if type(value) is not int or not 0 <= value <= 253402300799000:
            raise ValidationError("invalid actual shadow clock")
        return value

    def _bootstrap(self):
        if self._deployment is None:
            previous = self._data["deployment"]
            ref = previous.get("document", {}).get("candidate")
            if ref is not None:
                cached = self._data["candidates"].get(ref.get("candidate_id"))
                if cached is None or cached["path"] != ref.get("path"):
                    raise StorageError("active shadow candidate is not in its registered catalog")
                self._private(ref["path"])
            if previous["document"]["core_epoch"] == self.core_epoch:
                raise StorageError("reuse of a previous shadow core epoch is forbidden")
            deployment = ShadowDeployment.restore(previous, core_epoch=self.core_epoch)
            # No old-core result may be accepted or silently re-submitted.
            for identity, row in list(self._data["operations"].items()):
                if row["state"] in _PENDING:
                    self._commit(identity, {"state": "unknown", "error": "core_restarted"})
            self._commit(deployment=deployment)
        with self._condition:
            self._alive()
            self._ready, self._error = True, None
            self._condition.notify_all()

    def _training(self, identity):
        job = self.research.status(identity)
        target = self.root / "artifacts" / identity / "training/run.json"
        if (
            job.get("id") != identity
            or job.get("kind") != "train"
            or job.get("state") != "complete"
            or type(job.get("result_file")) is not str
            or Path(job["result_file"]).absolute() != target
        ):
            raise ValidationError("shadow source must be its completed training job artifact")
        for path in (target, target.parent, target.parent.parent, target.parent.parent.parent):
            if path.is_symlink() or path.is_junction():
                raise ValidationError("training source cannot use linked artifacts")
        return target.parent

    def _candidate(self, identity):
        value = self._data["candidates"].get(identity)
        if value is None:
            raise ValidationError("shadow candidate is not registered")
        return ShadowCandidate(str(self._private(value["path"])), identity)

    def _dispatch(
        self,
        identity,
        clone,
        parameters,
        *,
        request_id=None,
        candidate=None,
        job_kind="nn_predict",
    ):
        intent = {"parameters": parameters, "request_id": request_id}
        self._commit(
            identity,
            {"state": "running", "intent": intent, "job_id": None},
            deployment=clone,
        )
        job_id = None
        try:
            # Only the small scheduler submission and binding share this fence.
            # Candidate/model IO never holds it. Close cannot retire between the
            # permission check and an irreversible child submission.
            with self._condition:
                self._alive()
                try:
                    job = self.research.submit(job_kind, parameters)
                except Exception:
                    self._commit(identity, {"state": "unknown", "error": "research_submit_unknown"})
                    return
                if type(job) is dict and _hex(job.get("id")):
                    job_id = job["id"]
                if (
                    job_id is None
                    or job.get("kind") != job_kind
                    or job.get("parameters") != parameters
                ):
                    self._commit(
                        identity,
                        {
                            "state": "unknown",
                            "job_id": job_id,
                            "error": "research_dispatch_identity_unknown",
                        },
                    )
                    return
                if request_id is not None:
                    clone.bind_job(request_id, job, research_root=self.root)
                self._commit(
                    identity,
                    {
                        "state": "waiting",
                        "job_id": job_id,
                        "candidate_id": candidate.candidate_id
                        if candidate
                        else clone.candidate.candidate_id,
                    },
                    deployment=clone,
                )
        except Exception:
            if job_id is not None:
                try:
                    self.research.cancel(job_id)
                except Exception:
                    pass
            raise

    def _execute(self, identity):
        row = self.status(identity)
        if row["core_epoch"] != self.core_epoch:
            raise ValidationError("shadow operation belongs to an old core")
        clone = deepcopy(self._deployment)
        parameters, kind = row["parameters"], row["kind"]
        self._commit(identity, {"state": "running"})
        if kind == "inspect_training":
            source = self._training(parameters["training_job_id"])
            preview = training_view(source, parameters["training_job_id"])
            self._commit(identity, {"state": "completed", "result": preview})
            return
        if kind == "predict_raw":
            candidate, prepared = raw_shadow.prepare(clone, parameters, self._now())
            self._dispatch(identity, clone, prepared, candidate=candidate, job_kind="nn_prepare")
            return
        if kind == "prepare_fixture":
            source = self._training(parameters["training_job_id"])
            destination = self._private(
                str(self.root / "state/shadow-candidates" / row["execution_id"] / "candidate")
            )
            candidate = prepare_candidate(
                source,
                destination,
                ShadowModelContract.parse(parameters["contract"]),
                fixture_batch=parameters["fixture_batch"],
                fixture_expected=parameters.get("fixture_expected"),
                trusted_module=True,
            )
            registered = self._data["candidates"].get(candidate.candidate_id)
            if registered is not None:
                # Content identity owns one immutable path/proof. A new private
                # snapshot remains an unreferenced orphan, never a registry overwrite.
                previous = self._candidate(candidate.candidate_id)
                if previous.metadata() != candidate.metadata():
                    raise ValidationError("shadow candidate content identity conflict")
                candidate = previous
                if candidate.verified:
                    self._commit(
                        identity,
                        {
                            "state": "completed",
                            "candidate_id": candidate.candidate_id,
                            "result": {"candidate_id": candidate.candidate_id},
                        },
                    )
                    return
                frozen = candidate.parameters()
                existing = next(
                    (
                        item
                        for item in self._data["operations"].values()
                        if item["id"] != identity
                        and item["core_epoch"] == self.core_epoch
                        and item["kind"] == "prepare_fixture"
                        and item["candidate_id"] == candidate.candidate_id
                        and item["job_id"] is not None
                        and item["state"] in {"running", "waiting", "unknown"}
                        and item["intent"] == {"parameters": frozen, "request_id": None}
                    ),
                    None,
                )
                if existing is not None:
                    # One actual fixture proof owns one job ID. Join the same
                    # durable job instead of racing two immutable proof writers.
                    self._commit(
                        identity,
                        {
                            "state": "waiting",
                            "candidate_id": candidate.candidate_id,
                            "intent": existing["intent"],
                            "job_id": existing["job_id"],
                        },
                    )
                    return
                self._commit(identity, {"candidate_id": candidate.candidate_id})
                self._dispatch(identity, clone, frozen, candidate=candidate)
                return
            metadata = {
                "candidate_id": candidate.candidate_id,
                "path": candidate.path,
                "training_job_id": parameters["training_job_id"],
                "fixture_verified": False,
            }
            self._commit(
                identity,
                {"candidate_id": candidate.candidate_id},
                candidates={candidate.candidate_id: metadata},
            )
            self._dispatch(identity, clone, candidate.parameters(), candidate=candidate)
            return
        if kind == "predict":
            if parameters["expected_revision"] != clone.revision:
                raise ValidationError("shadow deployment revision conflict")
            values = {
                key: value
                for key, value in parameters.items()
                if key not in {"expected_revision", "stream", "instrument"}
            }
            values["features"] = tuple(values["features"])
            request = clone.request(
                parameters["stream"], parameters["instrument"], now_ms=self._now(), **values
            )
            self._dispatch(identity, clone, request["parameters"], request_id=request["id"])
            return
        if kind == "activate":
            clone.activate(
                self._candidate(parameters["candidate_id"]),
                expected_revision=parameters["expected_revision"],
            )
        elif kind == "reset_stream":
            clone.reset_stream(**parameters)
        elif kind == "resume":
            clone.resume(**parameters)
        elif kind == "pause":
            clone.pause(**parameters)
        elif kind == "refresh":
            for operation in list(self._data["operations"].values()):
                if (
                    operation["core_epoch"] == self.core_epoch
                    and operation["state"] == "unknown"
                    and operation["job_id"]
                ):
                    self._commit(operation["id"], {"state": "waiting", "error": None})
            self._poll()
            clone = deepcopy(self._deployment)
        self._commit(identity, {"state": "completed", "result": clone.snapshot()}, deployment=clone)

    def _cancel(self, identity, job_id, reason):
        try:
            status = self.research.cancel(job_id)
            state = "confirmed" if status.get("state") in _TERMINAL_CHILD else "requested"
        except Exception:
            state = "unknown"
        self._commit(
            identity,
            {
                "state": "expired" if reason == "inference_deadline_exceeded" else "failed",
                "error": reason,
                "cancellation": state,
            },
        )

    def _poll(self):
        # Preparing a batch owns no controller request/cursor yet. Its original
        # deadline and exact revision still fence that independent child stage.
        for row in list(self._data["operations"].values()):
            if (
                row["kind"] != "predict_raw"
                or row["core_epoch"] != self.core_epoch
                or row["state"] not in _PENDING | {"unknown"}
                or row["intent"] is None
                or row["intent"]["request_id"] is not None
            ):
                continue
            try:
                raw_shadow.current(self._deployment, row, self._now())
            except (ValueError, ValidationError):
                reason = (
                    "inference_deadline_exceeded"
                    if (self._now() >= row["parameters"]["deadline_ms"])
                    else "request_invalidated"
                )
                if row["job_id"] is not None:
                    self._cancel(row["id"], row["job_id"], reason)
                else:
                    self._commit(
                        row["id"],
                        {
                            "state": "expired"
                            if reason == "inference_deadline_exceeded"
                            else "failed",
                            "error": reason,
                            "cancellation": "unknown_job_id",
                        },
                    )
        clone = deepcopy(self._deployment)
        expired = clone.expire(now_ms=self._now())
        if expired:
            self._commit(deployment=clone)
            for item in expired:
                row = next(
                    (
                        value
                        for value in self._data["operations"].values()
                        if value.get("intent")
                        and value["intent"]["request_id"] == item["request_id"]
                    ),
                    None,
                )
                if row is not None and item["job_id"] is not None:
                    self._cancel(row["id"], item["job_id"], "inference_deadline_exceeded")
                elif row is not None:
                    self._commit(
                        row["id"],
                        {
                            "state": "expired",
                            "error": "inference_deadline_exceeded",
                            "cancellation": "unknown_job_id",
                        },
                    )
        rows = [
            deepcopy(value)
            for value in self._data["operations"].values()
            if value["core_epoch"] == self.core_epoch
            and value["job_id"] is not None
            and (
                value["state"] == "waiting" or value.get("cancellation") in {"requested", "unknown"}
            )
        ]
        for row in rows:
            try:
                self._poll_one(row)
            except _Retired:
                raise
            except Exception as error:
                clone = deepcopy(self._deployment)
                request_id = row["intent"]["request_id"]
                if request_id is not None and request_id in clone.requests:
                    clone.fail(request_id, reason="inference_result_invalid")
                self._commit(
                    row["id"], {"state": "failed", "error": type(error).__name__}, deployment=clone
                )

    def _poll_one(self, row):
        try:
            job = self.research.status(row["job_id"])
        except Exception:
            self._commit(row["id"], {"state": "unknown", "error": "research_metadata_unavailable"})
            return
        preparing = row["kind"] == "predict_raw" and row["intent"]["request_id"] is None
        if job.get("id") != row["job_id"] or job.get("kind") != (
            "nn_prepare" if preparing else "nn_predict"
        ):
            raise ValidationError("research returned another shadow child")
        if row["state"] != "waiting":
            if job.get("state") in _TERMINAL_CHILD:
                self._commit(row["id"], {"cancellation": "confirmed"})
            return
        request_id = row["intent"]["request_id"]
        if request_id is not None and request_id not in self._deployment.requests:
            self._cancel(row["id"], row["job_id"], "request_invalidated")
            return
        if job.get("state") not in _TERMINAL_CHILD:
            if job.get("state") not in {"queued", "running"}:
                self._commit(row["id"], {"state": "unknown", "error": "research_metadata_unknown"})
            return
        if job.get("state") != "complete":
            clone = deepcopy(self._deployment)
            if request_id is not None:
                clone.fail(request_id, reason="inference_child_failed")
            self._commit(
                row["id"], {"state": "failed", "error": "inference_child_failed"}, deployment=clone
            )
            return
        if job.get("parameters") != row["intent"]["parameters"]:
            raise ValidationError("research completion changed its frozen parameters")
        clone = deepcopy(self._deployment)
        if preparing:
            request, proof = raw_shadow.accept_prepared(clone, row, job, self.root, self._now())
            if self._now() >= request["deadline_ms"]:
                self._cancel(row["id"], row["job_id"], "inference_deadline_exceeded")
                return
            self._commit(row["id"], {"result": {"preparation": proof}})
            self._dispatch(row["id"], clone, request["parameters"], request_id=request["id"])
        elif row["kind"] == "prepare_fixture":
            candidate = self._candidate(row["candidate_id"])
            verify_fixture(candidate, job, research_root=self.root)
            cached = {**self._data["candidates"][candidate.candidate_id], "fixture_verified": True}
            self._commit(
                row["id"],
                {"state": "completed", "result": {"candidate_id": candidate.candidate_id}},
                candidates={candidate.candidate_id: cached},
            )
        else:
            deadline = clone.requests[request_id]["deadline_ms"]
            try:
                if row["kind"] == "predict_raw":
                    raw_shadow.verify_proof(
                        row["result"]["preparation"], clone.requests[request_id], self.root
                    )
                result = clone.accept(request_id, job, now_ms=self._now())
                if row["kind"] == "predict_raw":
                    result["scope"] = "offline_retrospective_shadow_only"
                    result["preparation"] = deepcopy(row["result"]["preparation"])
            except Exception:
                if request_id in clone.requests:
                    clone.fail(request_id, reason="inference_result_invalid")
                self._commit(
                    row["id"],
                    {"state": "failed", "error": "inference_result_invalid"},
                    deployment=clone,
                )
                return
            # Artifact hashing may take longer than the remaining deadline.
            # Discard the provisional acceptance; fail the original controller.
            if self._now() >= deadline:
                expired = deepcopy(self._deployment)
                expired.fail(request_id, reason="inference_deadline_exceeded")
                self._commit(deployment=expired)
                self._cancel(row["id"], row["job_id"], "inference_deadline_exceeded")
                return
            self._commit(row["id"], {"state": "completed", "result": result}, deployment=clone)

    def _loop(self):
        try:
            self._bootstrap()
        except Exception as error:
            with self._condition:
                self._error = type(error).__name__
            return
        while True:
            with self._condition:
                if self._closed or self._storage_failed:
                    return
                if not self._queue:
                    waiting = bool(self._deployment.requests) or any(
                        value["state"] == "waiting"
                        for value in self._data["operations"].values()
                        if value["core_epoch"] == self.core_epoch
                    )
                    cancellation = any(
                        value.get("cancellation") in {"requested", "unknown"}
                        for value in self._data["operations"].values()
                        if value["core_epoch"] == self.core_epoch
                    )
                    # A live inference retains its deadline cadence. An already
                    # invalidated child with an unconfirmed cancellation only
                    # needs bounded retirement checks; new commands wake either.
                    self._condition.wait(0.25 if waiting else 3.0 if cancellation else None)
                    if self._closed or self._storage_failed:
                        return
                identity = self._queue.popleft() if self._queue else None
            try:
                if identity is not None:
                    self._execute(identity)
                self._poll()
            except _Retired:
                return
            except Exception as error:
                try:
                    if identity is not None:
                        self._commit(identity, {"state": "failed", "error": type(error).__name__})
                    else:
                        with self._condition:
                            self._error = type(error).__name__
                except Exception:
                    return

    def close(self):
        jobs = []
        with self._condition:
            if self._closed:
                return {
                    "closed": True,
                    "durable": not self._storage_failed,
                    "worker_alive": bool(self._thread and self._thread.is_alive()),
                }
            durable = not self._storage_failed
            if self._data is not None and not self._storage_failed:
                data = deepcopy(self._data)
                if self._deployment is not None:
                    clone = deepcopy(self._deployment)
                    clone.pause(expected_revision=clone.revision, reason="shadow_service_closed")
                    data["deployment"] = clone.snapshot()
                for row in data["operations"].values():
                    if (
                        row["state"] in _PENDING
                        or row["core_epoch"] == self.core_epoch
                        and row["state"] == "unknown"
                    ):
                        if row["job_id"] is not None:
                            jobs.append(row["job_id"])
                        row.update(state="unknown", error="owner_closed")
                try:
                    self._write(data)
                    self._data = data
                except Exception:
                    durable = False
            self._closed = True
            self._condition.notify_all()
        for identity in jobs:
            try:
                self.research.cancel(identity)
            except Exception:
                pass
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(1)
        return {
            "closed": True,
            "durable": durable,
            "worker_alive": bool(self._thread and self._thread.is_alive()),
        }
