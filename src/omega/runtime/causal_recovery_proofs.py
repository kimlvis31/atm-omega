"""Exact original retirement checks and bounded immutable lifecycle records.

These records bind observed evidence. They never assert normal model cleanup,
all-kernel cleanup, financial resume, or authority from a newly opened store.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path

from omega.domain.errors import StorageError, ValidationError
from omega.domain.external_features import ExternalFeaturePin
from omega.domain.inference import InferenceCommitAck, checksum, digest, encoded
from omega.research.artifacts import read_status_json, validate_artifact_path
from omega.runtime.causal_actor_retirement import (
    ActorJoinObservation,
    ActorRetirementSnapshot,
    validate_retirement_value,
)
from omega.runtime.causal_generation_outcomes import (
    GenerationSettlementSnapshot,
    covered_rejected_outcome,
)
from omega.runtime.causal_historical_carry import CarryAcceptedAck, CarryManifest
from omega.runtime.causal_inference_registry import validate_journal
from omega.runtime.causal_startup_policy import ManualStartupCertificate
from omega.runtime.settings import atomic_json

_ARCHIVE_KEYS = frozenset(
    {
        "proof_artifact_sha256",
        "proof_record_count",
        "proof_record_bytes",
        "proof_attachment_sha256",
    }
)
_ATTACHED_RECORD_BYTES = 2_097_152


def _proof_digest(value):
    return sha256(encoded(value, maximum=_ATTACHED_RECORD_BYTES).encode("utf-8")).hexdigest()


class RecoveryProofArchive:
    """Bounded immutable payloads; the original journal owns every reservation.

    A durable proof_pending checkpoint MUST precede write(). A crash or unknown
    return therefore cannot leave a clean reusable head with uncharged evidence.
    No long-lived writer lease or physical-close claim is added here, and no
    lifetime directory/history scan is needed.
    """

    def __init__(self, root, limits):
        self._path = validate_artifact_path(
            Path(root) / "state" / "causal-recovery-proofs", allow_missing=True
        )
        self._path.mkdir(exist_ok=True)
        self._limits = limits

    def prepare(self, document, previous, *, attachment=None):
        payload = {key: value for key, value in document.items() if key not in _ARCHIVE_KEYS}
        count, charged = (
            previous.get("proof_record_count", 0),
            previous.get("proof_record_bytes", 0),
        )
        head = previous.get("proof_artifact_sha256")
        if (
            type(count) is not int
            or type(charged) is not int
            or count < 0
            or charged < 0
            or count >= self._limits.max_records
        ):
            raise StorageError("recovery proof evidence capacity is unavailable")
        if head is not None:
            checksum(head)
        record = {
            "schema_version": 1,
            "sequence": count + 1,
            "previous_sha256": head,
            "document": payload,
        }
        if attachment is not None:
            record["attachment"] = attachment
        sha = _proof_digest(record)
        maximum = 262144 if attachment is None else _ATTACHED_RECORD_BYTES
        total = charged + len(encoded(record, maximum=maximum).encode("utf-8"))
        if total > self._limits.max_bytes:
            raise StorageError("recovery proof byte capacity is unavailable")
        complete = {
            **payload,
            "proof_artifact_sha256": sha,
            "proof_record_count": count + 1,
            "proof_record_bytes": total,
        }
        if attachment is not None:
            complete["proof_attachment_sha256"] = _proof_digest(attachment)
        pending = {
            "phase": "proof_pending",
            "core_epoch": payload.get("core_epoch"),
            "pending_proof_sha256": sha,
            "previous_sha256": head,
            "proof_record_count": count + 1,
            "proof_record_bytes": total,
            "manual_required": True,
        }
        return record, complete, pending

    def write(self, record):
        sha = _proof_digest(record)
        path = validate_artifact_path(self._path / (sha + ".json"), allow_missing=True)
        try:
            existing = read_status_json(path)
        except FileNotFoundError:
            atomic_json(path, record)
        else:
            if encoded(existing, maximum=_ATTACHED_RECORD_BYTES) != encoded(
                record, maximum=_ATTACHED_RECORD_BYTES
            ):
                raise StorageError("immutable recovery proof record changed")
        if encoded(read_status_json(path), maximum=_ATTACHED_RECORD_BYTES) != encoded(
            record, maximum=_ATTACHED_RECORD_BYTES
        ):
            raise StorageError("recovery proof readback is unconfirmed")
        return sha

    def verify(self, document):
        sha = document.get("proof_artifact_sha256")
        checksum(sha)
        path = validate_artifact_path(self._path / (sha + ".json"))
        record = read_status_json(path)
        payload = {key: value for key, value in document.items() if key not in _ARCHIVE_KEYS}
        if (
            type(record) is not dict
            or set(record)
            not in (
                {"schema_version", "sequence", "previous_sha256", "document"},
                {"schema_version", "sequence", "previous_sha256", "document", "attachment"},
            )
            or type(record.get("schema_version")) is not int
            or record["schema_version"] != 1
            or type(document.get("proof_record_count")) is not int
            or _proof_digest(record) != sha
            or encoded(record.get("document"), maximum=262144) != encoded(payload, maximum=262144)
            or record.get("sequence") != document.get("proof_record_count")
            or type(document.get("proof_record_bytes")) is not int
            or document["proof_record_bytes"]
            < len(encoded(record, maximum=_ATTACHED_RECORD_BYTES).encode("utf-8"))
            or document["proof_record_bytes"] > self._limits.max_bytes
            or not 0 < document["proof_record_count"] <= self._limits.max_records
        ):
            raise StorageError("current recovery proof checkpoint binding is unavailable")
        attachment_sha = document.get("proof_attachment_sha256")
        if ("attachment" in record) != (attachment_sha is not None):
            raise StorageError("recovery attachment checkpoint binding is unavailable")
        if attachment_sha is not None and _proof_digest(record["attachment"]) != checksum(
            attachment_sha
        ):
            raise StorageError("recovery attachment checksum differs from its checkpoint")
        return True

    def read_attachment(self, artifact_sha256):
        """Read one checkpoint-bound historical observation, never a capability."""
        checksum(artifact_sha256)
        path = validate_artifact_path(self._path / (artifact_sha256 + ".json"))
        record = read_status_json(path)
        if (
            type(record) is not dict
            or set(record)
            != {"schema_version", "sequence", "previous_sha256", "document", "attachment"}
            or type(record["schema_version"]) is not int
            or record["schema_version"] != 1
            or type(record["sequence"]) is not int
            or record["sequence"] <= 0
            or _proof_digest(record) != artifact_sha256
            or type(record["attachment"]) is not dict
        ):
            raise StorageError("historical recovery observation attachment is unavailable")
        return record["attachment"]


@dataclass(frozen=True, slots=True)
class RetiredGenerationProof:
    core_epoch: str
    generation: int
    actor_token: str
    retirement_sha256: str
    join_sha256: str
    settlement_sha256: str
    journal_revision: int
    journal_sha256: str
    store_heads: tuple
    original_errors: tuple
    terminal_coverage: tuple = ()
    normal_model_cleanup: str = "unconfirmed"
    all_kernel: bool = False

    def document(self):
        return asdict(self)

    @property
    def sha256(self):
        return digest(self.document())


@dataclass(frozen=True, slots=True)
class FaultPauseBarrierAck:
    operation_sha256: str
    old_proof_sha256: str
    mode: str
    old_journal_revision: int
    old_journal_sha256: str
    old_pause_ack_sha256: str | None
    settlement_sha256: str
    pending_unknown_sha256: str
    fault_codes: tuple
    host_revision: int
    host_snapshot_sha256: str
    durable: bool = True

    def document(self):
        return asdict(self)

    @property
    def sha256(self):
        return digest(self.document())


@dataclass(frozen=True, slots=True)
class RestorationReceipt:
    operation_sha256: str
    core_epoch: str
    old_generation: int
    new_generation: int
    old_actor_token: str
    new_actor_token: str
    intent_ack_sha256: str
    barrier_ack_sha256: str
    startup_ack_sha256: str
    namespace_sha256: str
    model_contracts_sha256: str
    control_revision: int
    settlement_policy: str = "verified_pins0_reservations0"
    automatic_inference_started: bool = False
    financial_resumed: bool = False
    old_error_cleared: bool = False

    def document(self):
        return asdict(self)

    @property
    def sha256(self):
        return digest(self.document())


def exact_ack(value):
    if (
        type(value) is not InferenceCommitAck
        or value.durable is not True
        or type(value.revision) is not int
        or value.revision < 0
    ):
        raise StorageError("exact current durable acknowledgement required")
    checksum(value.snapshot_sha256)
    return value


def verified_journal_head(root, ack):
    """Verify known original ACK after retirement, without opening a writer."""
    exact_ack(ack)
    path = validate_artifact_path(Path(root) / "state" / "causal-inference.json")
    envelope = read_status_json(path)
    if (
        type(envelope) is not dict
        or set(envelope) != {"schema_version", "revision", "snapshot_sha256", "snapshot"}
        or type(envelope["schema_version"]) is not int
        or envelope["schema_version"] != 1
        or type(envelope["revision"]) is not int
        or envelope["revision"] != ack.revision
        or envelope["snapshot_sha256"] != ack.snapshot_sha256
        or type(envelope["snapshot"]) is not dict
        or envelope["snapshot"].get("revision") != ack.revision
        or digest(envelope["snapshot"]) != ack.snapshot_sha256
    ):
        raise StorageError("retired original journal differs from its exact ACK")
    return validate_journal(envelope["snapshot"])


def pending_unknown_evidence(head):
    """Bind actual old inference uncertainty without claiming it was settled."""
    validate_journal(head)
    operations, streams = [], []
    for key, row in sorted(head["operations"].items()):
        if row["state"] not in {"queued", "running", "unknown"}:
            continue
        parameters, result = row["parameters"], row["result"] or {}
        operations.append(
            {
                "id": key,
                "kind": row["kind"],
                "state": row["state"],
                "request_digest": row["request_digest"],
                "deployment_id": parameters.get("deployment_id") or result.get("deployment_id"),
                "stream": parameters.get("stream") or result.get("stream"),
                "instrument": parameters.get("instrument"),
                "request_id": result.get("request_id"),
                "error_code": None if row["error"] is None else row["error"]["code"],
            }
        )
    for deployment_id, deployment in sorted(head["deployments"].items()):
        uncertain = frozenset(deployment["uncertain_streams"])
        for stream, row in sorted(deployment["streams"].items()):
            if row["pending_operation_id"] is not None or stream in uncertain:
                streams.append(
                    {
                        "deployment_id": deployment_id,
                        "stream": stream,
                        "stream_epoch": row["stream_epoch"],
                        "instrument": row["instrument"],
                        "pending_operation_id": row["pending_operation_id"],
                        "uncertain": stream in uncertain,
                    }
                )
    return {"operations": operations, "streams": streams}


def _retired_generation_evidence(context, retirement, join, settlement, terminal_coverage):
    """All physical/current-write checks precede any candidate factory call."""
    if type(retirement) is not ActorRetirementSnapshot or type(join) is not ActorJoinObservation:
        raise StorageError("exact original actor retirement and join evidence required")
    validate_retirement_value(retirement)
    validate_retirement_value(join)
    if (
        retirement.actor_token != context.actor_token
        or retirement.core_epoch != context.core_epoch
        or join.actor_token != retirement.actor_token
        or join.owner_thread_token != retirement.owner_thread_token
        or not join.join_attempted
        or not join.join_returned
        or not join.original_thread_dead
        or not join.identity_intact
        or not retirement.sealed
        or not retirement.admission_closed
        or not retirement.physical_confirmed
        or not retirement.factory_coverage_complete
        or not retirement.identity_intact
        or retirement.capture_failed
        or retirement.journal_write_pending
        or retirement.recording_unknown
        or retirement.stores is None
        or not retirement.stores.all_confirmed
    ):
        raise StorageError("original physical or recording retirement remains unconfirmed")
    ack = exact_ack(retirement.last_journal_ack)
    heads = []
    for store in retirement.stores.bindings:
        if (
            not store.identity_intact
            or not store.file_lease_confirmed
            or store.recording_failed
            or store.file_acquisition_unknown
            or store.construction_failed
            or not store.construction_complete
        ):
            raise StorageError("original file-store coverage is incomplete")
        last = store.last_ack
        if last is not None:
            exact_ack(last)
        heads.append(
            (
                store.scope_token,
                store.binding_token,
                store.role,
                None if last is None else last.revision,
                None if last is None else last.snapshot_sha256,
            )
        )
    if not any(row.role == "journal" and row.last_ack is ack for row in retirement.stores.bindings):
        raise StorageError("current original journal acknowledgement is uncovered")
    if type(settlement) is not GenerationSettlementSnapshot:
        raise StorageError("exact original feature settlement required")
    if (
        (settlement.core_epoch, settlement.generation, settlement.actor_token)
        != (context.core_epoch, context.generation, context.actor_token)
        or settlement.context_sha256 != context.binding_sha256
        or not settlement.sealed
        or settlement.capture_failed
        or settlement.recording_unknown
        or not settlement.current_write_confirmed
    ):
        raise StorageError("known original generation settlement required")
    covered = dict(terminal_coverage)
    if len(covered) != len(terminal_coverage) or len(covered) > 128:
        raise StorageError("bounded exact original terminal coverage required")
    for outcome in settlement.outcomes:
        if (
            outcome.recording_unknown
            or outcome.capture_failed
            or not outcome.current_write_confirmed
            or not (
                (outcome.phase == "complete" and outcome.result_json is not None)
                or covered_rejected_outcome(context, settlement, outcome, terminal_coverage)
            )
            or not context.matches(outcome.ref)
            or outcome.context_sha256 != context.binding_sha256
            or outcome.provider_token != settlement.provider_token
            or outcome.store_scope_token != settlement.store_scope_token
            or outcome.store_binding_token != settlement.store_binding_token
            or outcome.head_revision != settlement.head_revision
            or outcome.head_sha256 != settlement.head_sha256
            or outcome.last_exact_ack is not settlement.last_exact_ack
            or covered.get(outcome.ref.sha256) != outcome.sha256
        ):
            raise StorageError("dead original outcome lacks durable host terminal coverage")
    last = exact_ack(settlement.last_exact_ack)
    if last.revision != settlement.head_revision or last.snapshot_sha256 != settlement.head_sha256:
        raise ValidationError("original delivery current write differs from exact ACK")
    if not any(
        row.role == "delivery"
        and row.scope_token == settlement.store_scope_token
        and row.binding_token == settlement.store_binding_token
        and row.last_ack is last
        for row in retirement.stores.bindings
    ):
        raise StorageError("original delivery acknowledgement is uncovered")
    errors = tuple(
        sorted(
            set(
                retirement.errors
                + (() if retirement.actor_error_code is None else (retirement.actor_error_code,))
            )
        )
    )
    return RetiredGenerationProof(
        context.core_epoch,
        context.generation,
        context.actor_token,
        digest(asdict(retirement)),
        digest(asdict(join)),
        settlement.sha256,
        ack.revision,
        ack.snapshot_sha256,
        tuple(heads),
        errors,
        tuple(terminal_coverage),
    )


def retired_generation_proof(context, retirement, join, settlement, terminal_coverage=()):
    """Strict original pins0/reservations0 path remains separate from carry."""
    proof = _retired_generation_evidence(context, retirement, join, settlement, terminal_coverage)
    if settlement.pins or settlement.reservations:
        raise StorageError("verified pins0 reservations0 and known original outcomes required")
    return proof


def retired_generation_carry_proof(context, retirement, join, settlement, terminal_coverage=()):
    """Sibling proof: identical physical/current-write gates; typed obligations."""
    proof = _retired_generation_evidence(context, retirement, join, settlement, terminal_coverage)
    if (
        type(settlement.pins) is not tuple
        or type(settlement.reservations) is not tuple
        or any(type(pin) is not ExternalFeaturePin for pin in settlement.pins)
        or len({digest(pin.document()) for pin in settlement.pins}) != len(settlement.pins)
    ):
        raise StorageError("exact immutable historical obligations required")
    return proof


def original_observation_document(retirement, join, settlement, proof):
    """Persist original bodies while their live typed identities still exist."""
    validate_retirement_value(retirement)
    validate_retirement_value(join)
    if (
        type(settlement) is not GenerationSettlementSnapshot
        or digest(asdict(retirement)) != proof.retirement_sha256
        or digest(asdict(join)) != proof.join_sha256
        or settlement.sha256 != proof.settlement_sha256
    ):
        raise StorageError("original observation bodies differ from their physical proof")
    return {
        "original_retirement": asdict(retirement),
        "original_join": asdict(join),
        "original_settlement": settlement.document(),
    }


def verify_carry_acceptance(binding, manifest, acceptance):
    """Accept only the actual successor constructor's cached original ACK."""
    if type(manifest) is not CarryManifest or type(acceptance) is not CarryAcceptedAck:
        raise StorageError("exact historical carry manifest and acceptance required")
    context = binding.context
    if (
        binding.actor.carry_acceptance_snapshot() is not acceptance
        or acceptance.manifest_sha256 != manifest.sha256
        or (acceptance.new_core_epoch, acceptance.new_generation, acceptance.new_actor_token)
        != (context.core_epoch, context.generation, context.actor_token)
        or acceptance.observed_revision != manifest.delivery_revision
        or acceptance.observed_sha256 != manifest.delivery_sha256
    ):
        raise StorageError("successor carry acceptance binding changed")
    ack = exact_ack(acceptance.commit_ack)
    current = binding.actor.generation_settlement_snapshot()
    if (
        type(current) is not GenerationSettlementSnapshot
        or (current.core_epoch, current.generation, current.actor_token)
        != (context.core_epoch, context.generation, context.actor_token)
        or current.context_sha256 != context.binding_sha256
        or current.store_scope_token != acceptance.store_scope_token
        or current.store_binding_token != acceptance.store_binding_token
        or current.last_exact_ack is not ack
        or current.head_revision != ack.revision
        or current.head_sha256 != ack.snapshot_sha256
        or ack.revision != manifest.delivery_revision + 1
        or current.capture_failed
        or current.recording_unknown
        or not current.current_write_confirmed
        or tuple(sorted(encoded(pin.document()) for pin in current.pins))
        != tuple(sorted(encoded(pin.document()) for pin in manifest.pins))
        or tuple(sorted(current.reservations)) != tuple(sorted(manifest.reservations))
    ):
        raise StorageError("successor current carry write is unconfirmed")
    return ack


def startup_certificate_document(binding, journal_ack):
    """Persist an original optional observation; absence never grants adoption."""
    getter = getattr(binding.actor, "manual_startup_certificate", None)
    certificate = None if not callable(getter) else getter()
    if certificate is None:
        return None
    context = binding.context
    current = binding.actor.generation_settlement_snapshot()
    if (
        type(certificate) is not ManualStartupCertificate
        or getter() is not certificate
        or (certificate.core_epoch, certificate.generation, certificate.actor_token)
        != (context.core_epoch, context.generation, context.actor_token)
        or certificate.context_sha256 != context.binding_sha256
        or certificate.journal_revision != journal_ack.revision
        or certificate.journal_sha256 != journal_ack.snapshot_sha256
        or type(current) is not GenerationSettlementSnapshot
        or certificate.delivery_revision != current.head_revision
        or certificate.delivery_sha256 != current.head_sha256
        or certificate.provider_token != current.provider_token
        or certificate.store_scope_token != current.store_scope_token
        or certificate.store_binding_token != current.store_binding_token
        or certificate.settlement_sha256 != current.sha256
    ):
        raise StorageError("original candidate startup certificate binding changed")
    return certificate.document()
