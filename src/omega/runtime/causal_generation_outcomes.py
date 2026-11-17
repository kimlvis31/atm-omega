"""Immutable, non-authorizing exports of one original actor's feature effects.

Only the original provider issues these snapshots. Hashes identify their bytes;
they are not durable acknowledgements or permission to replace an actor.
"""

import inspect
from dataclasses import asdict, dataclass, field
from hashlib import sha256

from omega.domain.errors import ValidationError
from omega.domain.external_features import ExternalFeaturePin
from omega.domain.inference import InferenceCommitAck, decoded, digest, identity
from omega.runtime.causal_recovery_records import FeatureMutationRef, MutationAckAuthority

_ACK_VERIFY_SHAPE = MutationAckAuthority.verify
MAX_SETTLEMENT_BYTES = 128 * 1024 * 1024


def _settlement_sha256(document):
    # Finite128 hot argument records plus the real store's finite pin membership
    # can exceed the generic one-document limit; no lifetime history is included.
    from omega.domain.inference import encoded

    return sha256(encoded(document, maximum=MAX_SETTLEMENT_BYTES).encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class GenerationContext:
    core_epoch: str
    generation: int
    actor_token: str
    host_token: str
    ack_authority: MutationAckAuthority | None = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        for value in (self.core_epoch, self.actor_token, self.host_token):
            identity(value)
        if type(self.generation) is not int or not 0 <= self.generation <= 2**63 - 1:
            raise ValidationError("bounded original generation required")
        if self.ack_authority is not None and type(self.ack_authority) is not MutationAckAuthority:
            raise ValidationError("exact original mutation acknowledgement authority required")

    @property
    def binding_sha256(self):
        return digest(
            {
                name: getattr(self, name)
                for name in ("core_epoch", "generation", "actor_token", "host_token")
            }
        )

    def matches(self, ref):
        return type(ref) is FeatureMutationRef and (
            ref.core_epoch,
            ref.generation,
            ref.actor_token,
        ) == (self.core_epoch, self.generation, self.actor_token)

    def verifies_terminal_ack(self, ref, ack, result_json):
        authority = self.ack_authority
        return (
            type(authority) is MutationAckAuthority
            and inspect.getattr_static(authority, "verify") is _ACK_VERIFY_SHAPE
            and _ACK_VERIFY_SHAPE(authority, ref, ack, result_json) is True
        )


@dataclass(frozen=True, slots=True)
class FeatureSettlementSnapshot:
    ref: FeatureMutationRef
    arguments_json: str
    context_sha256: str
    provider_token: str
    store_scope_token: str | None
    store_binding_token: str | None
    phase: str
    result_json: str | None
    error_code: str | None
    recording_unknown: bool
    head_revision: int | None
    head_sha256: str | None
    last_exact_ack: InferenceCommitAck | None
    current_write_confirmed: bool
    capture_failed: bool

    def document(self):
        return asdict(self)

    @property
    def sha256(self):
        return _settlement_sha256(self.document())


@dataclass(frozen=True, slots=True)
class GenerationSettlementSnapshot:
    core_epoch: str
    generation: int
    actor_token: str
    context_sha256: str
    provider_token: str
    store_scope_token: str | None
    store_binding_token: str | None
    pins: tuple[ExternalFeaturePin, ...]
    reservations: tuple[tuple[str, int], ...]
    outcomes: tuple[FeatureSettlementSnapshot, ...]
    head_revision: int | None
    head_sha256: str | None
    last_exact_ack: InferenceCommitAck | None
    current_write_confirmed: bool
    recording_unknown: bool
    capture_failed: bool
    sealed: bool

    def document(self):
        result = asdict(self)
        result["pins"] = [pin.document() for pin in self.pins]
        result["outcomes"] = [outcome.document() for outcome in self.outcomes]
        return result

    @property
    def sha256(self):
        return _settlement_sha256(self.document())


def covered_rejected_outcome(context, settlement, outcome, terminal_coverage):
    """Classify an original rejection, never turn it into a completed effect.

    The Host supplies coverage only after its exact issued terminal ACK. These
    immutable exports retain every original outcome and financial obligation;
    physical retirement and ACK issuer checks belong to the caller's proof.
    """
    if (
        type(context) is not GenerationContext
        or type(settlement) is not GenerationSettlementSnapshot
        or type(outcome) is not FeatureSettlementSnapshot
        or type(terminal_coverage) is not tuple
        or len(terminal_coverage) > 128
        or any(type(row) is not tuple or len(row) != 2 for row in terminal_coverage)
    ):
        return False
    try:
        covered = dict(terminal_coverage)
        return (
            len(covered) == len(terminal_coverage)
            and outcome.phase == "rejected"
            and outcome.result_json is None
            and outcome.error_code == "original_mutation_rejected"
            and not outcome.recording_unknown
            and not outcome.capture_failed
            and outcome.current_write_confirmed is True
            and settlement.sealed is True
            and not settlement.recording_unknown
            and not settlement.capture_failed
            and settlement.current_write_confirmed is True
            and context.matches(outcome.ref)
            and (settlement.core_epoch, settlement.generation, settlement.actor_token)
            == (context.core_epoch, context.generation, context.actor_token)
            and outcome.context_sha256 == settlement.context_sha256 == context.binding_sha256
            and outcome.provider_token == settlement.provider_token
            and outcome.store_scope_token == settlement.store_scope_token
            and outcome.store_binding_token == settlement.store_binding_token
            and outcome.head_revision == settlement.head_revision
            and outcome.head_sha256 == settlement.head_sha256
            and outcome.last_exact_ack is settlement.last_exact_ack
            and type(settlement.last_exact_ack) is InferenceCommitAck
            and settlement.last_exact_ack.durable is True
            and settlement.last_exact_ack.revision == settlement.head_revision
            and settlement.last_exact_ack.snapshot_sha256 == settlement.head_sha256
            and digest(decoded(outcome.arguments_json)) == outcome.ref.arguments_sha256
            and covered.get(outcome.ref.sha256) == outcome.sha256
        )
    except (TypeError, ValueError, ValidationError):
        return False
