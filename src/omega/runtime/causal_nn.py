"""Single-owner causal-feature state, with injected evidence and model-worker ports.

This component imports no model/framework and reads no market/training dataset.
Actual provenance verification belongs to a mandatory evidence port. No default
permissive verifier, raw-shadow adapter, scheduler, TS or order integration is
provided here. Calls that change state belong to the constructing thread.
"""

# Imports
import os
import stat
import threading
import time
from pathlib import Path
from uuid import uuid4

from omega.application.inference_gate import CausalInferenceGate
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.inference import (
    CAUSAL_SCOPE,
    InferenceCommitAck,
    InferenceEvidenceReceipt,
    InferenceReceipt,
    InferenceTransition,
    checksum,
    decoded,
    digest,
    encoded,
    identity,
    timestamp,
)
from omega.research.artifacts import read_status_json, validate_artifact_path
from omega.runtime.causal_store_leases import (
    InferenceStoreConstructionError,
    InferenceStoreLeases,
)
from omega.runtime.settings import atomic_json


class FileInferenceStateStore:
    """One OS-held writer and actual-file CAS; unknown writes latch until reopening."""

    def __init__(self, path, *, leases=None, role="nn_state"):
        self.closed, self.failed = False, False
        self._pid, self._thread = os.getpid(), threading.current_thread()
        self._last_ack = None
        self.lock = None
        self._lease_scope = (
            InferenceStoreLeases("private-" + uuid4().hex, capacity=1) if leases is None else leases
        )
        self._lease_binding = None
        if type(self._lease_scope) is not InferenceStoreLeases:
            raise ValidationError("typed original inference store lease scope required")
        try:
            # Register this original object before path validation/acquisition can
            # fail, independently of the caller's eventual store assignment.
            self._lease_binding = self._lease_scope.bind_store(self, role)
            self.path = Path(os.path.abspath(path))
            self.anchor_path = self.path.with_suffix(self.path.suffix + ".origin.json")
            self._guard()
            lock_path = self.path.with_suffix(self.path.suffix + ".lock")
            validate_artifact_path(lock_path, allow_missing=True)
            if lock_path.exists() and not lock_path.is_file():
                raise StorageError("inference owner lock is not a regular file")
            self.lock = self._lease_binding.acquire_lock(lock_path)
            details = os.fstat(self.lock.stream.fileno())
            if (
                not stat.S_ISREG(details.st_mode)
                or getattr(details, "st_file_attributes", 0) & 0x400
            ):
                raise StorageError("inference owner lock is not a regular file")
            self._guard()
            self._lease_binding.constructed()
        except BaseException as primary:
            binding = self._lease_binding
            if binding is None:
                try:
                    binding = self._lease_scope.binding_for(self)
                except StorageError:
                    pass
            if binding is not None:
                binding.construction_failed(primary)
                try:
                    binding.close()
                except BaseException:
                    pass  # Primary construction failure retains public precedence.
            if not isinstance(primary, Exception):
                try:
                    primary.lease_scope = self._lease_scope
                except BaseException:
                    # Rare custom BaseException refusing attributes: retain the
                    # original scope on a fixed fallback, never in stored graphs.
                    raise InferenceStoreConstructionError(self._lease_scope) from None
                raise
            failure = (
                binding.construction_error(primary)
                if binding is not None
                else InferenceStoreConstructionError(self._lease_scope)
            )
            raise failure from None

    @property
    def lease_scope(self):
        self._owner_guard()
        return self._lease_scope

    def _owner_guard(self):
        if os.getpid() != self._pid or threading.current_thread() is not self._thread:
            raise StorageError("inference store requires its original PID and Thread object")

    def _close_admission(self):
        self._owner_guard()
        self.closed = True

    def _guard(self):
        self._owner_guard()
        self._lease_scope.binding_for(self).guard_admission(self)
        if self.closed:
            raise StorageError("inference store is retired or used outside its owner thread")
        try:
            validate_artifact_path(self.path, allow_missing=True)
            validate_artifact_path(self.anchor_path, allow_missing=True)
        except (OSError, ValueError):
            raise StorageError("inference state path is unavailable") from None

    def _anchor(self):
        try:
            anchor = read_status_json(self.anchor_path)
        except FileNotFoundError:
            return None
        except (OSError, ValueError, TypeError):
            raise StorageError(
                "inference origin anchor could not be read; original preserved"
            ) from None
        if (
            type(anchor) is not dict
            or set(anchor) != {"data", "sha256"}
            or type(anchor["data"]) is not dict
            or set(anchor["data"])
            != {"schema_version", "file_id", "initial_revision", "initial_sha256"}
            or type(anchor["data"]["schema_version"]) is not int
            or anchor["data"]["schema_version"] != 1
            or anchor["data"]["file_id"] != digest(self.path.name)
            or type(anchor["data"]["initial_revision"]) is not int
            or anchor["data"]["initial_revision"] < 0
            or anchor["sha256"] != digest(anchor["data"])
        ):
            raise StorageError("inference origin anchor is invalid; original preserved")
        try:
            checksum(anchor["data"]["initial_sha256"])
        except ValidationError:
            raise StorageError("inference origin anchor identity is invalid") from None
        return anchor["data"]

    def read(self):
        self._guard()
        anchor = self._anchor()
        try:
            envelope = read_status_json(self.path)
        except FileNotFoundError:
            if anchor is not None:
                raise StorageError(
                    "registered inference state is missing; explicit recovery required"
                ) from None
            return None
        except (OSError, ValueError, TypeError):
            raise StorageError("inference snapshot could not be read; original preserved") from None
        if (
            anchor is None
            or type(envelope) is not dict
            or set(envelope) != {"schema_version", "revision", "snapshot_sha256", "snapshot"}
            or type(envelope["schema_version"]) is not int
            or envelope["schema_version"] != 1
            or type(envelope["revision"]) is not int
            or envelope["revision"] < 0
            or type(envelope["snapshot"]) is not dict
            or type(envelope["snapshot"].get("revision")) is not int
            or envelope["revision"] < anchor["initial_revision"]
            or envelope["revision"] == anchor["initial_revision"]
            and envelope["snapshot_sha256"] != anchor["initial_sha256"]
            or envelope["snapshot"].get("revision") != envelope["revision"]
            or digest(envelope["snapshot"]) != envelope["snapshot_sha256"]
        ):
            raise StorageError("inference snapshot integrity is unavailable; original preserved")
        return encoded(envelope["snapshot"])

    def save_snapshot(self, snapshot_json, *, expected_revision):
        self._guard()
        if self.failed:
            raise StorageError("inference write outcome is unknown; reopen and read actual state")
        if expected_revision is not None and (
            type(expected_revision) is not int or expected_revision < 0
        ):
            raise ValidationError("invalid expected inference revision")
        snapshot = decoded(snapshot_json)
        if (
            type(snapshot) is not dict
            or snapshot.get("scope") != CAUSAL_SCOPE
            or type(snapshot.get("revision")) is not int
            or snapshot["revision"] < 0
        ):
            raise ValidationError("invalid inference snapshot identity")
        actual = self.read()
        current = None if actual is None else decoded(actual)
        revision, checksum = snapshot["revision"], digest(snapshot)
        if actual == encoded(snapshot):
            ack = InferenceCommitAck(revision, checksum, True)
            self._last_ack = ack
            return ack
        if (None if current is None else current["revision"]) != expected_revision:
            raise ProtocolError("inference state changed outside its sole owner")
        if current is not None and revision <= current["revision"]:
            raise ProtocolError("inference state revision cannot move backwards")
        try:
            if current is None:
                anchor = {
                    "schema_version": 1,
                    "file_id": digest(self.path.name),
                    "initial_revision": revision,
                    "initial_sha256": checksum,
                }
                atomic_json(self.anchor_path, {"data": anchor, "sha256": digest(anchor)})
            atomic_json(
                self.path,
                {
                    "schema_version": 1,
                    "revision": revision,
                    "snapshot_sha256": checksum,
                    "snapshot": snapshot,
                },
            )
            self._guard()
            if self.read() != encoded(snapshot):
                raise StorageError("inference snapshot changed before its acknowledgement")
        except Exception:
            self.failed = True
            raise StorageError("inference snapshot commit is unconfirmed") from None
        ack = InferenceCommitAck(revision, checksum, True)
        self._last_ack = ack
        return ack

    def commit(self, transition):
        if not isinstance(transition, InferenceTransition):
            raise ValidationError("typed inference transition required")
        return self.save_snapshot(
            transition.snapshot_json, expected_revision=transition.base_revision
        )

    def close(self):
        self._owner_guard()
        return self._lease_scope.binding_for(self).close()

    def retirement_status(self):
        self._owner_guard()
        return self._lease_scope.binding_for(self).snapshot()


class CausalNNFeatureOwner:
    def __init__(
        self,
        root,
        model,
        bindings,
        *,
        worker,
        evidence,
        core_epoch,
        deployment_epoch,
        namespace="default",
        clock_ms=None,
        store=None,
        on_incident=None,
        leases=None,
    ):
        identity(namespace)
        if any(
            not callable(getattr(worker, key, None))
            for key in ("submit", "receipt", "cancel", "close")
        ) or not callable(getattr(evidence, "verify", None)):
            raise ValidationError("causal inference requires explicit worker and evidence ports")
        self._thread, self.closed = threading.get_ident(), False
        self.worker, self.evidence = worker, evidence
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.on_incident, self.error = on_incident, None
        self.incident_delivery_error = None
        self._dispatched, self._stored_revision = set(), None
        self.gate = CausalInferenceGate(
            model, bindings, core_epoch=core_epoch, deployment_epoch=deployment_epoch
        )
        try:
            self.store = store or FileInferenceStateStore(
                Path(root).absolute() / "state/causal-nn" / (digest(namespace) + ".json"),
                leases=leases,
            )
        except Exception:
            self.worker.close()
            raise
        try:
            saved = self.store.read()
            if saved is not None:
                self._stored_revision = decoded(saved)["revision"]
                self.gate = CausalInferenceGate.restore(
                    saved,
                    core_epoch=core_epoch,
                    deployment_epoch=deployment_epoch,
                    expected_model=model,
                )
                if self.gate.bindings != bindings:
                    raise ProtocolError(
                        "restored inference output mapping differs from configured bindings"
                    )
            self._save()
        except Exception:
            try:
                self.worker.close()
            finally:
                self.store.close()
            raise

    def _guard(self):
        if self.closed or threading.get_ident() != self._thread:
            raise StorageError("causal inference owner is retired or called outside its thread")

    def _now(self):
        return timestamp(self.clock_ms())

    def _recording_failure(self):
        self.error = "recording_unconfirmed"
        self.gate.recording_uncertain()
        if self.on_incident is not None:
            try:
                self.on_incident("CAUSAL_NN_RECORDING_UNCONFIRMED")
            except Exception as error:
                self.incident_delivery_error = type(error).__name__

    def _save(self):
        snapshot = self.gate.snapshot()
        expected = decoded(snapshot)
        try:
            ack = self.store.save_snapshot(snapshot, expected_revision=self._stored_revision)
            if (
                not isinstance(ack, InferenceCommitAck)
                or not ack.durable
                or ack.revision != expected["revision"]
                or ack.snapshot_sha256 != digest(expected)
            ):
                raise StorageError("inference request intent was not durably acknowledged")
        except Exception:
            self._recording_failure()
            raise
        self._stored_revision = ack.revision

    def snapshot(self):
        self._guard()
        return {
            "scope": CAUSAL_SCOPE,
            "closed": self.closed,
            "error": self.error,
            "state": decoded(self.gate.snapshot()),
            "outstanding_dispatches": len(self._dispatched),
            "incident_delivery_error": self.incident_delivery_error,
        }

    def open_stream(
        self, stream, instrument, *, stream_epoch, initial_state_json="null", after_open_ms=None
    ):
        self._guard()
        pending = decoded(self.gate.snapshot())["pending"]
        self.gate.open_stream(
            stream,
            instrument,
            stream_epoch=stream_epoch,
            initial_state_json=initial_state_json,
            after_open_ms=after_open_ms,
        )
        self._save()
        for request_id, request in pending.items():
            if request["stream"] == stream:
                self._dispatched.discard(request_id)
                self.worker.cancel(request_id)

    def request(self, stream, frame, *, decision_at_ms, deadline_ms):
        self._guard()
        before = self.gate.snapshot()
        request = self.gate.request(
            stream,
            frame,
            decision_at_ms=decision_at_ms,
            now_ms=self._now(),
            deadline_ms=deadline_ms,
        )
        if request.request_id in self._dispatched:
            return request
        try:
            proof = self.evidence.verify(request)
            now_ms = self._now()
            if (
                not isinstance(proof, InferenceEvidenceReceipt)
                or proof.request_sha256 != request.request_id
                or proof.source_commit_sha256 != frame.input_sha256
                or not request.issued_at_ms <= proof.verified_at_ms <= now_ms < deadline_ms
            ):
                raise ProtocolError("causal source/model evidence is missing, stale or mismatched")
        except Exception:
            self.gate.fail(request.request_id, "source_evidence_unconfirmed")
            self._save()
            raise
        if self.gate.snapshot() != before:
            self._save()  # No child receives a request before its real durable intent ACK.
        try:
            result = self.worker.submit(request, self.gate.previous_state_json(stream))
            if result is not None:
                raise ProtocolError("causal worker submit did not match its port contract")
        except Exception:
            self.gate.fail(request.request_id, "worker_dispatch_unknown")
            self._save()
            self.worker.cancel(request.request_id)
            raise
        self._dispatched.add(request.request_id)
        return request

    def poll(self, request_id):
        self._guard()
        if request_id not in decoded(self.gate.snapshot())["pending"]:
            self._dispatched.discard(request_id)
            return None  # Old receipts cannot mutate a reset or restarted stream.
        try:
            receipt = self.worker.receipt(request_id)
        except Exception:
            self._dispatched.discard(request_id)
            self.gate.fail(request_id, "worker_outcome_unknown")
            self._save()
            raise
        if receipt is None:
            self.expire()
            return None
        if not isinstance(receipt, InferenceReceipt) or receipt.request_id != request_id:
            self._dispatched.discard(request_id)
            self.gate.fail(request_id, "worker_receipt_mismatch")
            self._save()
            raise ProtocolError("causal worker returned another request or an offline result")
        before = self.gate.snapshot()
        try:
            transition = self.gate.stage(receipt, now_ms=self._now())
        except Exception:
            self._dispatched.discard(request_id)
            if self.gate.snapshot() == before:
                self.gate.fail(request_id, "inference_receipt_invalid")
            self._save()
            raise
        try:
            acknowledgement = self.store.commit(transition)
            feature = self.gate.commit(transition, acknowledgement, now_ms=self._now())
        except Exception:
            self._recording_failure()
            raise
        self._stored_revision = transition.revision
        self._dispatched.discard(request_id)
        return feature

    def expire(self):
        self._guard()
        expired = self.gate.expire(now_ms=self._now())
        if expired:
            try:
                self._save()
            finally:
                for request_id in expired:
                    self._dispatched.discard(request_id)
                    self.worker.cancel(request_id)
        return expired

    def lookup(self, stream, frame):
        self._guard()
        return self.gate.lookup(stream, frame, now_ms=self._now())

    def close(self):
        if threading.get_ident() != self._thread:
            raise StorageError("close causal inference owner on its constructing thread")
        self.closed = True
        errors = []
        for resource in (self.worker, self.store):
            try:
                resource.close()
            except Exception as error:
                errors.append(type(error).__name__)
        if errors or self.error == "recording_unconfirmed":
            raise StorageError("causal inference worker/recording cleanup is unconfirmed")
