"""Bounded historical data classification, not a live resource/ACK capability.

The coordinator verifies the immutable host artifact before calling this pure
function. It separately checks its actual original Core lease before any factory.
Only an untouched, certified default manual startup is eligible for explicit new
owner adoption. Unknown phases/resources remain held.
"""

from dataclasses import asdict, dataclass, fields
from hashlib import sha256

from omega.domain.errors import ValidationError
from omega.domain.inference import CAUSAL_SCOPE, checksum, digest, encoded, identity
from omega.runtime.causal_actor_retirement import ActorJoinObservation, ActorRetirementSnapshot
from omega.runtime.causal_inference_registry import validate_journal
from omega.runtime.causal_startup_policy import ManualStartupCertificate

MAX_OBSERVATION_BYTES = 2 * 1024 * 1024
SUPPORTED_PHASES = frozenset({"candidate_prepared", "restored_manual"})


def _observation_sha(document):
    return sha256(encoded(document, maximum=MAX_OBSERVATION_BYTES).encode("utf-8")).hexdigest()


def _no_live_authority(document):
    remaining = [200_000]
    forbidden = {
        "actor_recovery_authorized",
        "all_kernel_resources_confirmed",
        "durable_state_confirmed",
        "durable_write_confirmed",
    }

    def visit(value, depth=0):
        remaining[0] -= 1
        if depth > 32 or remaining[0] < 0:
            raise ValidationError("historical observation exceeds bounds")
        if type(value) is dict:
            for key, item in value.items():
                if key in forbidden and item is not False:
                    raise ValidationError("historical data cannot grant live authority")
                visit(item, depth + 1)
        elif type(value) in {list, tuple}:
            for item in value:
                visit(item, depth + 1)

    _observation_sha(document)
    visit(document)


def _head(envelope, revision, sha):
    if (
        type(envelope) is not dict
        or set(envelope) != {"schema_version", "revision", "snapshot_sha256", "snapshot"}
        or type(envelope["schema_version"]) is not int
        or envelope["schema_version"] != 1
        or type(envelope["revision"]) is not int
        or envelope["revision"] != revision
        or envelope["snapshot_sha256"] != sha
        or type(envelope["snapshot"]) is not dict
        or envelope["snapshot"].get("revision") != revision
        or _observation_sha(envelope["snapshot"]) != sha
        or envelope["snapshot"].get("scope") != CAUSAL_SCOPE
    ):
        raise ValidationError("cold startup head changed or is malformed")
    return envelope["snapshot"]


def _original_observations(document, observations):
    if type(observations) is not dict:
        raise ValidationError("original retirement observations are unavailable")
    retired = observations["original_retirement"]
    joined = observations["original_join"]
    settlement = observations["original_settlement"]
    proof = document["proof"]
    _no_live_authority(observations)
    if (
        type(retired) is not dict
        or set(retired) != {field.name for field in fields(ActorRetirementSnapshot)}
        or type(joined) is not dict
        or set(joined) != {field.name for field in fields(ActorJoinObservation)}
        or type(settlement) is not dict
        or type(proof) is not dict
        or _observation_sha(retired) != proof["retirement_sha256"]
        or _observation_sha(joined) != proof["join_sha256"]
        or _observation_sha(settlement) != proof["settlement_sha256"]
        or (proof["core_epoch"], proof["generation"], proof["actor_token"])
        != (document["core_epoch"], document["generation"], document["actor_token"])
        or (retired["core_epoch"], retired["actor_token"])
        != (proof["core_epoch"], proof["actor_token"])
        or joined["actor_token"] != retired["actor_token"]
        or joined["owner_thread_token"] != retired["owner_thread_token"]
        or any(
            retired.get(key) is not True
            for key in (
                "sealed",
                "admission_closed",
                "physical_confirmed",
                "factory_coverage_complete",
                "identity_intact",
            )
        )
        or any(
            retired.get(key) is not False
            for key in (
                "capture_failed",
                "recording_unknown",
                "journal_write_pending",
            )
        )
        or any(
            joined.get(key) is not True
            for key in (
                "join_attempted",
                "join_returned",
                "original_thread_dead",
                "identity_intact",
            )
        )
        or type(retired.get("stores")) is not dict
        or retired["stores"].get("all_confirmed") is not True
        or any(settlement.get(key) is not True for key in ("sealed", "current_write_confirmed"))
        or any(settlement.get(key) is not False for key in ("capture_failed", "recording_unknown"))
        or (settlement["core_epoch"], settlement["generation"], settlement["actor_token"])
        != (proof["core_epoch"], proof["generation"], proof["actor_token"])
        or proof.get("all_kernel") is not False
    ):
        raise ValidationError("original observed retirement is unconfirmed")


def validate_original_cold_observations(host_document, original_observations):
    """Validate historical JSON observations without constructing live capabilities."""
    _original_observations(host_document, original_observations)


@dataclass(frozen=True, slots=True)
class ColdAdoptionObservation:
    core_epoch: str
    prior_phase: str
    registry_sha256: str | None
    candidate_generation: int | None
    candidate_actor_token: str | None
    startup_certificate_sha256: str | None
    heads_sha256: str | None
    blockers: tuple[str, ...]
    factory_authority: bool = False
    physical_close_claimed: bool = False
    automatic_inference_started: bool = False
    financial_resumed: bool = False
    old_uncertainty_cleared: bool = False

    def __post_init__(self):
        identity(self.core_epoch)
        if any(
            value is not False
            for value in (
                self.factory_authority,
                self.physical_close_claimed,
                self.automatic_inference_started,
                self.financial_resumed,
                self.old_uncertainty_cleared,
            )
        ):
            raise ValidationError("cold data cannot grant live authority")

    @property
    def eligible(self):
        return not self.blockers

    def document(self):
        return {
            **asdict(self),
            "schema_version": 1,
            "scope": "cold_startup_observation",
            "eligible": self.eligible,
            "requires_confirmation": True,
        }

    @property
    def sha256(self):
        return digest(self.document())


def classify_interrupted_cold_adoption(
    host_document,
    journal_envelope,
    delivery_envelope,
    *,
    current_core_epoch,
    expected_policy_sha256,
    current_root_owned,
    original_observations=None,
):
    """No IO, mutation, factories, typed ACK reconstruction or model execution."""
    identity(current_core_epoch)
    document = host_document if type(host_document) is dict else {}
    phase = document.get("phase")
    phase = phase if type(phase) is str and phase in SUPPORTED_PHASES else "unsupported"
    blockers, registry_sha, certificate, heads_sha = [], None, None, None
    try:
        registry_sha = digest(document)
    except ValidationError:
        blockers.append("COLD_REGISTRY_INVALID")
    if phase not in SUPPORTED_PHASES:
        blockers.append("COLD_PHASE_REQUIRES_MANUAL_RECONCILIATION")
    if current_root_owned is not True:
        blockers.append("CURRENT_ROOT_NOT_OWNED")
    try:
        checksum(expected_policy_sha256)
        certificate = ManualStartupCertificate.parse(document["candidate_startup_certificate"])
        generation = document["generation"]
        if (
            type(generation) is not int
            or not 0 <= generation < 2**63 - 1
            or certificate.generation != generation + 1
            or certificate.actor_token != document["candidate_actor_token"]
            or certificate.core_epoch != document["core_epoch"]
            or certificate.core_epoch == current_core_epoch
            or certificate.policy_sha256 != expected_policy_sha256
        ):
            raise ValidationError("candidate startup policy or original identity differs")
        ack = document["candidate_startup_ack"]
        if (
            type(ack) is not dict
            or set(ack) != {"revision", "snapshot_sha256", "durable"}
            or type(ack["revision"]) is not int
            or ack["durable"] is not True
            or (ack["revision"], ack["snapshot_sha256"])
            != (certificate.journal_revision, certificate.journal_sha256)
        ):
            raise ValidationError("recorded startup observation differs from its head")
        journal = _head(journal_envelope, certificate.journal_revision, certificate.journal_sha256)
        validate_journal(journal)
        if journal["core_epoch"] != certificate.core_epoch or any(
            row["state"] not in {"manual_required", "retired", "failed"}
            or any(
                stream["manual_required"] is not True or stream["pending_operation_id"] is not None
                for stream in row["streams"].values()
            )
            for row in journal["deployments"].values()
        ):
            raise ValidationError("startup manual state changed")
        _head(delivery_envelope, certificate.delivery_revision, certificate.delivery_sha256)
        _original_observations(document, original_observations)
        heads_sha = digest(
            {
                "journal": [certificate.journal_revision, certificate.journal_sha256],
                "delivery": [certificate.delivery_revision, certificate.delivery_sha256],
            }
        )
    except (ValidationError, KeyError, TypeError, ValueError):
        blockers.append("COLD_STARTUP_OR_ORIGINAL_EVIDENCE_UNCONFIRMED")
    return ColdAdoptionObservation(
        current_core_epoch,
        phase,
        registry_sha,
        None if certificate is None else certificate.generation,
        None if certificate is None else certificate.actor_token,
        None if certificate is None else certificate.sha256,
        heads_sha,
        tuple(sorted(set(blockers))),
    )
