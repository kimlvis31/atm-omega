"""Verified CPU candidates and pure Paper-shadow request/state ownership.

No plugin is imported here. Actual fixture/inference execution uses the existing
ResearchService ``nn_predict`` child and PredictionSession. The caller owns
serialization and durable commits of controller snapshots; this is not a live
worker, broker, automatic model switch or financial state store.
"""

# Imports
import json
import os
import re
import stat
from copy import deepcopy
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from omega import __version__
from omega.domain.model_contract import ShadowModelContract, _tensor
from omega.domain.models import Instrument, Market
from omega.research.artifacts import _status_descriptor, child_path, read_status_json, write_json

MAX_BYTES = 1_048_576
MAX_ARTIFACT_BYTES = 512 * 1024 * 1024


def _json(value):
    text = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(text.encode()) > MAX_BYTES:
        raise ValueError("shadow record exceeds capacity")
    return text


def _copy(value):
    return json.loads(_json(value))


def _hash(value):
    return sha256(_json(value).encode()).hexdigest()


def _hex(value, size=64):
    return type(value) is str and re.fullmatch(f"[a-f0-9]{{{size}}}", value) is not None


def _name(value):
    return type(value) is str and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is not None


def _time(value):
    return type(value) is int and 0 <= value <= 253_402_300_799_000


def _read(path):
    # Shared actual-fd reader: no following the final reparse point, regular
    # file check, bounded 2 MiB raw read. Our canonical record budget stays 1 MiB.
    value = read_status_json(Path(path))
    _json(value)
    return value


def _artifact_stream(path):
    # Reuse the package's platform opener, including Windows DELETE sharing.
    # Check the opened file rather than a path predicate that can race replacement.
    descriptor = _status_descriptor(Path(path))
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
            raise ValueError("candidate artifact must be a regular non-reparse file")
        if details.st_size > MAX_ARTIFACT_BYTES:
            raise ValueError("candidate artifact exceeds the 512 MiB CPU snapshot limit")
        stream = os.fdopen(descriptor, "rb")
        descriptor = None
        return stream
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _chunks(stream):
    count = 0
    while chunk := stream.read(1024 * 1024):
        count += len(chunk)
        if count > MAX_ARTIFACT_BYTES:
            raise ValueError("candidate artifact grew beyond its bounded CPU snapshot limit")
        yield chunk


def _artifact_hash(path):
    digest = sha256()
    with _artifact_stream(path) as stream:
        for chunk in _chunks(stream):
            digest.update(chunk)
    return digest.hexdigest()


def _copy_artifact(source, destination):
    # The newly created candidate directory is owned by the serialized caller.
    # Exclusive output preserves any existing bytes after a failed preparation.
    with _artifact_stream(source) as stream, Path(destination).open("xb") as target:
        for chunk in _chunks(stream):
            target.write(chunk)
        target.flush()
        os.fsync(target.fileno())


def prepare_candidate(
    source,
    destination,
    contract,
    *,
    fixture_batch,
    fixture_expected=None,
    checkpoint=None,
    trusted_module=False,
    device="cpu",
    timeout_seconds=15,
):
    """Data-only snapshot; verify its fixture via an existing nn_predict child.

    This function never imports a module or executes model code. Source path is
    trusted local input; validation of arbitrary Python remains an explicit choice.
    """
    if (
        trusted_module is not True
        or device != "cpu"
        or not isinstance(contract, ShadowModelContract)
    ):
        raise ValueError("shadow candidates require trusted Python, CPU and an explicit contract")
    if type(timeout_seconds) not in {int, float} or not 0 < timeout_seconds <= 120:
        raise ValueError("shadow fixture timeout must be within (0,120]")
    fixture_batch = _copy(fixture_batch)
    _tensor(fixture_batch, contract.input_shape)
    if fixture_expected is not None:
        _tensor(
            fixture_expected,
            contract.output_shape,
            batch=len(fixture_batch),
            nullable=contract.allow_no_signal,
        )
    source, destination = Path(source).resolve(), Path(destination).resolve()
    run = _read(source / "run.json")
    descriptor = run.get("module", {})
    if (
        run.get("state") != "complete"
        or run.get("cleanup_completed") is not True
        or descriptor.get("core_version") != __version__
        or type(descriptor.get("api_version")) is not int
        or descriptor["api_version"] != 1
        or (run.get("outcome") or {}).get("device") != "cpu"
    ):
        raise ValueError("candidate requires completed compatible CPU training and cleanup")
    inputs = descriptor.get("input") or {}
    if (
        inputs.get("schema_id") != contract.schema_id
        or inputs.get("preprocessing_owner") != "module"
        or inputs.get("features", list(contract.features)) != list(contract.features)
        or "lookback" in run["config"]
        and run["config"]["lookback"] != contract.lookback
    ):
        raise ValueError("candidate feature schema/order/lookback mismatch")
    capabilities = descriptor.get("capabilities") or {}
    if (
        capabilities.get("predict") is not True
        or capabilities.get("dynamic_topology") is True
        or capabilities.get("online_learning") is True
        or contract.state_shape is not None
        and capabilities.get("stateful_inference") is not True
    ):
        raise ValueError("unsupported shadow candidate capabilities")
    selected = checkpoint or (run.get("model") or {}).get("file")
    artifacts = [
        row
        for row in run.get("artifacts", [])
        if row.get("file") == selected and row.get("kind") == "checkpoint"
    ]
    if len(artifacts) != 1 or selected in {"run.json", "instance_module.py"}:
        raise ValueError("candidate requires one registered checkpoint")
    original = child_path(source, selected)
    module = child_path(source, "instance_module.py")
    if _artifact_hash(module) != run.get("module_source_sha256") or _artifact_hash(
        original
    ) != artifacts[0].get("sha256"):
        raise ValueError("candidate source/checkpoint hash mismatch")
    origin_hash = _artifact_hash(source / "run.json")
    destination.mkdir(parents=True, exist_ok=False)
    bundle = destination / "bundle"
    bundle.mkdir()
    frozen = child_path(bundle, selected)
    frozen.parent.mkdir(parents=True, exist_ok=True)
    _copy_artifact(original, frozen)
    _copy_artifact(module, bundle / "instance_module.py")
    if (
        _artifact_hash(frozen) != artifacts[0]["sha256"]
        or _artifact_hash(bundle / "instance_module.py") != run["module_source_sha256"]
        or _artifact_hash(original) != artifacts[0]["sha256"]
        or _artifact_hash(module) != run["module_source_sha256"]
        or _artifact_hash(source / "run.json") != origin_hash
    ):
        raise ValueError("candidate source changed while snapshotting")
    # A minimal prediction-compatible completed run; original training is untouched.
    frozen_run = {
        **run,
        "artifacts": artifacts,
        "model": {**(run.get("model") or {}), "file": selected},
    }
    write_json(bundle / "run.json", frozen_run)
    document = {
        "schema_version": 1,
        "scope": "paper_shadow_only",
        "device": "cpu",
        "origin_run_sha256": origin_hash,
        "bundle_run_sha256": _artifact_hash(bundle / "run.json"),
        "module": descriptor,
        "module_sha256": run["module_source_sha256"],
        "checkpoint": selected,
        "checkpoint_sha256": artifacts[0]["sha256"],
        "preprocessing_identity": artifacts[0]["sha256"],
        "contract": contract.document(),
        "fixture_batch": fixture_batch,
        "fixture_expected": _copy(fixture_expected),
        "timeout_seconds": timeout_seconds,
    }
    identity = _hash(document)
    write_json(destination / "candidate.json", {"candidate_id": identity, "document": document})
    return load_candidate(destination)


@dataclass(frozen=True, slots=True)
class ShadowCandidate:
    path: str
    candidate_id: str

    def metadata(self):
        return _candidate(self)

    def parameters(self, batch=None, state=None):
        document = self.metadata()
        return {
            "training_run": str(Path(self.path) / "bundle"),
            "checkpoint": document["checkpoint"],
            "batch": _copy(document["fixture_batch"] if batch is None else batch),
            "state": _copy(state),
            "device": "cpu",
            "trusted_module": True,
            "timeout_seconds": document["timeout_seconds"],
        }

    @property
    def verified(self):
        path = Path(self.path) / "verification.json"
        if not path.exists():
            return False
        value = _read(path)
        valid = (
            type(value) is dict
            and set(value) == {"document", "checksum"}
            and type(value["document"]) is dict
            and set(value["document"]) == {"candidate_id", "job_id", "result_sha256"}
            and value["checksum"] == _hash(value["document"])
            and value["document"].get("candidate_id") == self.candidate_id
            and _hex(value["document"].get("job_id"), 32)
            and _hex(value["document"].get("result_sha256"))
        )
        if not valid:
            return False
        result_path = Path(self.path) / "fixture-result.json"
        if _artifact_hash(result_path) != value["document"]["result_sha256"]:
            raise ValueError("candidate fixture result changed")
        document = self.metadata()
        result = _read(result_path)
        _validate_result(document, result, len(document["fixture_batch"]))
        if (
            document["fixture_expected"] is not None
            and result["prediction"]["values"] != document["fixture_expected"]
        ):
            raise ValueError("candidate fixture expected values changed")
        return True


def load_candidate(path):
    path = Path(path).resolve()
    value = _read(path / "candidate.json")
    if (
        type(value) is not dict
        or set(value) != {"candidate_id", "document"}
        or not _hex(value["candidate_id"])
        or _hash(value["document"]) != value["candidate_id"]
    ):
        raise ValueError("candidate manifest checksum mismatch")
    result = ShadowCandidate(str(path), value["candidate_id"])
    result.metadata()
    return result


def _candidate(candidate):
    value = _read(Path(candidate.path) / "candidate.json")
    document = value["document"]
    if (
        value["candidate_id"] != candidate.candidate_id
        or _hash(document) != candidate.candidate_id
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or document["scope"] != "paper_shadow_only"
        or document["device"] != "cpu"
        or document["preprocessing_identity"] != document["checkpoint_sha256"]
    ):
        raise ValueError("candidate identity/scope changed")
    contract = ShadowModelContract.parse(document["contract"])
    _tensor(document["fixture_batch"], contract.input_shape)
    bundle = Path(candidate.path) / "bundle"
    if (
        _artifact_hash(bundle / "run.json") != document["bundle_run_sha256"]
        or _artifact_hash(bundle / "instance_module.py") != document["module_sha256"]
        or _artifact_hash(child_path(bundle, document["checkpoint"]))
        != document["checkpoint_sha256"]
    ):
        raise ValueError("candidate frozen bundle changed")
    return document


def _validate_result(document, result, batch_size):
    if (
        type(result) is not dict
        or type(result.get("schema_version")) is not int
        or result.get("schema_version") != 1
        or result.get("module") != document["module"]
        or result.get("checkpoint_sha256") != document["checkpoint_sha256"]
        or result.get("scope") != "offline_research; no account/trading integration"
    ):
        raise ValueError("prediction result candidate identity mismatch")
    contract = ShadowModelContract.parse(document["contract"])
    return contract.validate_prediction(result.get("prediction"), batch_size)


def _job_result(candidate, job, research_root, parameters):
    if (
        type(job) is not dict
        or not _hex(job.get("id"), 32)
        or job.get("kind") != "nn_predict"
        or job.get("state") != "complete"
        or job.get("parameters") != parameters
    ):
        raise ValueError("shadow inference job identity/state/parameters mismatch")
    path = child_path(Path(research_root), f"artifacts/{job['id']}/prediction/prediction.json")
    if Path(job.get("result_file", "")).resolve() != path:
        raise ValueError("prediction result must belong to its actual research job")
    result = _read(path)
    document = candidate.metadata()
    no_signal = _validate_result(document, result, len(parameters["batch"]))
    return result, path, no_signal


def verify_fixture(candidate, job, *, research_root):
    """Accept an actual nn_predict child artifact, never call Python in the host."""
    document = candidate.metadata()
    result, _, _ = _job_result(candidate, job, research_root, candidate.parameters())
    if (
        document["fixture_expected"] is not None
        and result["prediction"]["values"] != document["fixture_expected"]
    ):
        raise ValueError("candidate fixture output mismatch")
    path = Path(candidate.path) / "fixture-result.json"
    if path.exists():
        if _read(path) != result:
            raise ValueError("candidate fixture result is immutable")
    else:
        write_json(path, result)
    proof = {
        "candidate_id": candidate.candidate_id,
        "job_id": job["id"],
        "result_sha256": _artifact_hash(path),
    }
    target = Path(candidate.path) / "verification.json"
    envelope = {"document": proof, "checksum": _hash(proof)}
    if target.exists():
        if _read(target) != envelope:
            raise ValueError("candidate already has another fixture verification")
    else:
        write_json(target, envelope)
    return candidate


class ShadowDeployment:
    """Single-owner pure control state; caller commits snapshots before publication."""

    def __init__(self, core_epoch):
        if not _name(core_epoch):
            raise ValueError("explicit core epoch required")
        self.core_epoch, self.revision = core_epoch, 0
        self.epoch, self.candidate = None, None
        self.paused = True
        self.streams, self.requests = {}, {}

    def _revision(self, expected):
        if type(expected) is not int or expected != self.revision:
            raise ValueError("shadow deployment revision conflict")

    @staticmethod
    def _capacity(streams, requests):
        # Leave room for the small controller header and its JSON envelope.
        if len(_json({"streams": streams, "requests": requests}).encode()) > MAX_BYTES - 16384:
            raise ValueError("shadow controller capacity reached")

    def activate(self, candidate, *, expected_revision):
        self._revision(expected_revision)
        candidate.metadata()
        if not candidate.verified:
            raise ValueError("candidate fixture verification required before activation")
        self.candidate, self.epoch = candidate, uuid4().hex
        self.streams, self.requests, self.paused = {}, {}, False
        self.revision += 1
        return self.snapshot()

    def _contract(self):
        if self.candidate is None:
            raise ValueError("no explicitly activated shadow candidate")
        return ShadowModelContract.parse(self.candidate.metadata()["contract"])

    def reset_stream(self, stream, instrument, *, expected_revision, reason):
        self._revision(expected_revision)
        self._contract()
        if not _name(stream) or not _name(reason):
            raise ValueError("explicit stream/reset reason required")
        provider, market, symbol = instrument.split(":")
        if Instrument(provider, Market(market), symbol).key != instrument:
            raise ValueError("invalid shadow instrument")
        key = stream + "|" + instrument
        if key not in self.streams and len(self.streams) >= 128:
            raise ValueError("shadow stream capacity reached")
        streams, requests = (
            deepcopy(self.streams),
            {k: v for k, v in self.requests.items() if v["stream"] != key},
        )
        streams[key] = {
            "epoch": uuid4().hex,
            "sequence": 0,
            "open_ms": None,
            "as_of_ms": None,
            "accepted_at_ms": None,
            "input_digest": None,
            "state": None,
            "state_hash": _hash(None),
            "health": "ready",
            "reason": reason,
        }
        self._capacity(streams, requests)
        self.streams, self.requests = streams, requests
        self.revision += 1
        return self.snapshot()

    def resume(self, *, expected_revision):
        self._revision(expected_revision)
        self._contract()
        if any(row["health"] != "ready" for row in self.streams.values()):
            raise ValueError("reset unknown/failed streams before manual shadow resume")
        self.paused = False
        self.revision += 1
        return self.snapshot()

    def pause(self, *, expected_revision, reason):
        self._revision(expected_revision)
        if not _name(reason):
            raise ValueError("explicit shadow pause reason required")
        for request in self.requests.values():
            self.streams[request["stream"]].update(health="blocked", reason=reason)
        self.requests.clear()
        self.paused = True
        self.revision += 1
        return self.snapshot()

    def expire(self, *, now_ms):
        """Owner tick: gate missed deadlines and return actual job IDs to cancel.

        The caller still owns ResearchService.cancel/confirmed worker retirement.
        An eventual result from an expired request can never publish state.
        """
        if not _time(now_ms) or any(now_ms < row["issued_at_ms"] for row in self.requests.values()):
            raise ValueError("shadow expiry clock moved backwards")
        expired = [deepcopy(row) for row in self.requests.values() if now_ms >= row["deadline_ms"]]
        for row in expired:
            self.fail(row["id"], reason="inference_deadline_exceeded")
        return [
            {
                "request_id": row["id"],
                "job_id": row["job_id"],
                "research_root": row["research_root"],
            }
            for row in expired
        ]

    def request(
        self,
        stream,
        instrument,
        *,
        sequence,
        open_time_ms,
        as_of_ms,
        now_ms,
        deadline_ms,
        input_digest,
        batch,
        schema_id,
        features,
        lookback,
    ):
        contract = self._contract()
        key = stream + "|" + instrument
        row = self.streams.get(key)
        if (
            self.paused
            or row is None
            or row["health"] != "ready"
            or any(value["stream"] == key for value in self.requests.values())
        ):
            raise ValueError("shadow stream paused, unknown, missing or already busy")
        if (
            type(sequence) is not int
            or sequence != row["sequence"] + 1
            or not all(_time(v) for v in (open_time_ms, as_of_ms, now_ms, deadline_ms))
            or open_time_ms % 60000
            or not open_time_ms + 60000 <= as_of_ms <= now_ms < deadline_ms <= now_ms + 60000
            or row["open_ms"] is not None
            and open_time_ms != row["open_ms"] + 60000
            or row["as_of_ms"] is not None
            and as_of_ms < row["as_of_ms"]
            or row["accepted_at_ms"] is not None
            and now_ms < row["accepted_at_ms"]
            or not _hex(input_digest)
        ):
            raise ValueError("shadow request sequence/closed minute/clock/digest mismatch")
        if (
            schema_id != contract.schema_id
            or type(features) is not tuple
            or features != contract.features
            or type(lookback) is not int
            or lookback != contract.lookback
        ):
            raise ValueError("shadow request feature schema/order/lookback mismatch")
        batch = _copy(batch)
        _tensor(batch, contract.input_shape, batch=1)
        identity = uuid4().hex
        request = {
            "id": identity,
            "core_epoch": self.core_epoch,
            "deployment_epoch": self.epoch,
            "stream_epoch": row["epoch"],
            "candidate_id": self.candidate.candidate_id,
            "stream": key,
            "sequence": sequence,
            "open_ms": open_time_ms,
            "as_of_ms": as_of_ms,
            "issued_at_ms": now_ms,
            "deadline_ms": deadline_ms,
            "input_digest": input_digest,
            "batch_hash": _hash(batch),
            "state_hash": row["state_hash"],
            "parameters": self.candidate.parameters(batch, row["state"]),
            "job_id": None,
            "research_root": None,
        }
        requests = {**self.requests, identity: request}
        self._capacity(self.streams, requests)
        self.requests = requests
        self.revision += 1
        return deepcopy(request)

    def bind_job(self, request_id, job, *, research_root):
        request = self.requests.get(request_id)
        if (
            request is None
            or type(job) is not dict
            or not _hex(job.get("id"), 32)
            or job.get("kind") != "nn_predict"
            or job.get("parameters") != request["parameters"]
            or job.get("state") not in {"queued", "running", "complete"}
            or request["job_id"] not in {None, job["id"]}
        ):
            raise ValueError("shadow job differs from its frozen request")
        root = str(Path(research_root).resolve())
        if request["research_root"] not in {None, root}:
            raise ValueError("shadow research owner changed")
        request.update(job_id=job["id"], research_root=root)
        self.revision += 1

    def fail(self, request_id, *, reason):
        request = self.requests.get(request_id)
        if request is None or not _name(reason):
            raise ValueError("unknown shadow request/failure reason")
        row = self.streams[request["stream"]]
        row.update(health="blocked", reason=reason)
        del self.requests[request_id]
        self.revision += 1

    def accept(self, request_id, job, *, now_ms):
        request = self.requests.get(request_id)
        if request is None:
            raise ValueError("unknown/late/duplicate shadow response")
        if not _time(now_ms) or now_ms < request["issued_at_ms"]:
            raise ValueError("invalid actual shadow response clock")
        if now_ms >= request["deadline_ms"]:
            self.fail(request_id, reason="inference_deadline_exceeded")
            raise ValueError("shadow response missed its deadline; explicit reset required")
        row = self.streams[request["stream"]]
        if (
            request["core_epoch"] != self.core_epoch
            or request["deployment_epoch"] != self.epoch
            or request["stream_epoch"] != row["epoch"]
            or request["candidate_id"] != self.candidate.candidate_id
            or request["state_hash"] != row["state_hash"]
            or request["sequence"] != row["sequence"] + 1
            or request["job_id"] != job.get("id")
            or request["research_root"] is None
        ):
            raise ValueError("shadow response epoch/state/sequence/job mismatch")
        try:
            result, _, no_signal = _job_result(
                self.candidate, job, request["research_root"], request["parameters"]
            )
        except (ValueError, OSError, TypeError, KeyError):
            self.fail(request_id, reason="inference_result_invalid")
            raise
        state = _copy(result["prediction"].get("state"))
        streams, requests = (
            deepcopy(self.streams),
            {k: v for k, v in self.requests.items() if k != request_id},
        )
        row = streams[request["stream"]]
        row.update(
            sequence=request["sequence"],
            open_ms=request["open_ms"],
            as_of_ms=request["as_of_ms"],
            accepted_at_ms=now_ms,
            input_digest=request["input_digest"],
            state=state,
            state_hash=_hash(state),
            health="ready",
            reason="no_signal" if no_signal else "prediction_accepted",
        )
        self._capacity(streams, requests)
        self.streams, self.requests = streams, requests
        self.revision += 1
        return {
            "scope": "paper_shadow_only",
            "candidate_id": self.candidate.candidate_id,
            "deployment_epoch": self.epoch,
            "stream_epoch": row["epoch"],
            "input_digest": request["input_digest"],
            "open_time_ms": row["open_ms"],
            "sequence": row["sequence"],
            "no_signal": no_signal,
            "values": _copy(result["prediction"]["values"]),
        }

    def snapshot(self):
        document = {
            "schema_version": 1,
            "scope": "paper_shadow_only",
            "core_epoch": self.core_epoch,
            "revision": self.revision,
            "deployment_epoch": self.epoch,
            "candidate": None
            if self.candidate is None
            else {"path": self.candidate.path, "candidate_id": self.candidate.candidate_id},
            "paused": self.paused,
            "streams": deepcopy(self.streams),
            "requests": deepcopy(self.requests),
        }
        return {"document": document, "checksum": _hash(document)}

    @classmethod
    def restore(cls, checkpoint, *, core_epoch):
        checkpoint = _copy(checkpoint)
        if (
            type(checkpoint) is not dict
            or set(checkpoint) != {"document", "checksum"}
            or checkpoint["checksum"] != _hash(checkpoint["document"])
        ):
            raise ValueError("shadow checkpoint checksum mismatch")
        doc = checkpoint["document"]
        if (
            type(doc) is not dict
            or set(doc)
            != {
                "schema_version",
                "scope",
                "core_epoch",
                "revision",
                "deployment_epoch",
                "candidate",
                "paused",
                "streams",
                "requests",
            }
            or type(doc["schema_version"]) is not int
            or doc["schema_version"] != 1
            or doc["scope"] != "paper_shadow_only"
            or not _name(doc["core_epoch"])
            or doc["core_epoch"] == core_epoch
            or type(doc["revision"]) is not int
            or doc["revision"] < 0
            or type(doc["paused"]) is not bool
            or type(doc["streams"]) is not dict
            or len(doc["streams"]) > 128
            or type(doc["requests"]) is not dict
            or len(doc["requests"]) > 128
        ):
            raise ValueError("invalid shadow checkpoint identity/state")
        result = cls(core_epoch)
        result.revision = doc["revision"] + 1
        if doc["candidate"] is None:
            if (
                doc["deployment_epoch"] is not None
                or doc["streams"]
                or doc["requests"]
                or not doc["paused"]
            ):
                raise ValueError("unactivated checkpoint contains shadow state")
            return result
        ref = doc["candidate"]
        if (
            type(ref) is not dict
            or set(ref) != {"path", "candidate_id"}
            or not _hex(doc["deployment_epoch"], 32)
        ):
            raise ValueError("invalid active candidate reference")
        candidate = load_candidate(ref["path"])
        if candidate.candidate_id != ref["candidate_id"] or not candidate.verified:
            raise ValueError("active shadow candidate changed/unverified")
        contract = ShadowModelContract.parse(candidate.metadata()["contract"])
        for key, row in doc["streams"].items():
            if (
                type(key) is not str
                or "|" not in key
                or type(row) is not dict
                or set(row)
                != {
                    "epoch",
                    "sequence",
                    "open_ms",
                    "as_of_ms",
                    "accepted_at_ms",
                    "input_digest",
                    "state",
                    "state_hash",
                    "health",
                    "reason",
                }
                or not _hex(row["epoch"], 32)
                or type(row["sequence"]) is not int
                or row["sequence"] < 0
                or row["health"] not in {"ready", "blocked"}
                or not _name(row["reason"])
                or row["state_hash"] != _hash(row["state"])
                or (row["sequence"] == 0) != (row["open_ms"] is None)
                or row["open_ms"] is not None
                and (not _time(row["open_ms"]) or row["open_ms"] % 60000)
                or row["sequence"] == 0
                and any(row[k] is not None for k in ("as_of_ms", "accepted_at_ms", "input_digest"))
                or row["sequence"] > 0
                and (
                    not _time(row["as_of_ms"])
                    or not _time(row["accepted_at_ms"])
                    or not row["open_ms"] + 60000 <= row["as_of_ms"] <= row["accepted_at_ms"]
                    or not _hex(row["input_digest"])
                )
            ):
                raise ValueError("invalid shadow stream checkpoint")
            stream, instrument = key.split("|", 1)
            provider, market, symbol = instrument.split(":")
            if not _name(stream) or Instrument(provider, Market(market), symbol).key != instrument:
                raise ValueError("invalid shadow stream identity")
            if row["state"] is not None:
                if contract.state_shape is None:
                    raise ValueError("stateless checkpoint contains recurrent state")
                _tensor(row["state"], contract.state_shape)
            elif contract.state_shape is not None and row["sequence"]:
                raise ValueError("stateful checkpoint lost its committed state")
        seen_streams = set()
        request_keys = {
            "id",
            "core_epoch",
            "deployment_epoch",
            "stream_epoch",
            "candidate_id",
            "stream",
            "sequence",
            "open_ms",
            "as_of_ms",
            "issued_at_ms",
            "deadline_ms",
            "input_digest",
            "batch_hash",
            "state_hash",
            "parameters",
            "job_id",
            "research_root",
        }
        for identity, request in doc["requests"].items():
            if (
                not _hex(identity, 32)
                or type(request) is not dict
                or set(request) != request_keys
                or request.get("id") != identity
                or request.get("stream") not in doc["streams"]
                or request.get("core_epoch") != doc["core_epoch"]
                or request.get("deployment_epoch") != doc["deployment_epoch"]
                or request.get("candidate_id") != candidate.candidate_id
                or request["stream"] in seen_streams
            ):
                raise ValueError("invalid pending shadow checkpoint")
            row = doc["streams"][request["stream"]]
            if (
                request.get("stream_epoch") != row["epoch"]
                or type(request["sequence"]) is not int
                or request.get("sequence") != row["sequence"] + 1
                or request.get("state_hash") != row["state_hash"]
                or not all(
                    _time(request[k])
                    for k in ("open_ms", "as_of_ms", "issued_at_ms", "deadline_ms")
                )
                or request["open_ms"] % 60000
                or not request["open_ms"] + 60000
                <= request["as_of_ms"]
                <= request["issued_at_ms"]
                < request["deadline_ms"]
                <= request["issued_at_ms"] + 60000
                or row["open_ms"] is not None
                and request["open_ms"] != row["open_ms"] + 60000
                or row["as_of_ms"] is not None
                and request["as_of_ms"] < row["as_of_ms"]
                or row["accepted_at_ms"] is not None
                and request["issued_at_ms"] < row["accepted_at_ms"]
                or not _hex(request["input_digest"])
                or not _hex(request["batch_hash"])
                or type(request["parameters"]) is not dict
                or request["job_id"] is not None
                and not _hex(request["job_id"], 32)
                or (request["job_id"] is None) != (request["research_root"] is None)
            ):
                raise ValueError("pending shadow checkpoint cursor/state mismatch")
            batch = request["parameters"].get("batch")
            _tensor(batch, contract.input_shape, batch=1)
            if request["batch_hash"] != _hash(batch) or request[
                "parameters"
            ] != candidate.parameters(batch, row["state"]):
                raise ValueError("pending shadow checkpoint immutable parameters changed")
            if request["research_root"] is not None and (
                type(request["research_root"]) is not str
                or str(Path(request["research_root"]).resolve()) != request["research_root"]
            ):
                raise ValueError("pending shadow checkpoint research owner changed")
            seen_streams.add(request["stream"])
        result.candidate, result.epoch = candidate, uuid4().hex
        result.streams = deepcopy(doc["streams"])
        for row in result.streams.values():
            row.update(health="blocked", reason="core_restart_reset_required")
        # Unknown in-flight results never authorize state/cursor advancement.
        # Explicit stream reset + manual resume establishes a new stream epoch.
        result.paused = True
        return result
