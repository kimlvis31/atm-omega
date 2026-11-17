"""Pure TS decision-to-request preparation; no dispatch, clock, keys or durable recovery.

Before dispatch, the caller MUST durably bind the cycle, source digest, original
intents and reconciled account state. IDs alone are not an idempotency ledger.
Changed requests/source metadata deliberately keep the same order ID within a
cycle: supply the original prepared decision to detect a conflicting replacement.
"""

# Imports
import json
import re
from dataclasses import dataclass, replace
from decimal import localcontext
from hashlib import sha256

from omega.application.execution_rules import validate_execution_order
from omega.application.trading import validate_decision
from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.execution_rules import (
    MAX_TIME,
    ExecutionRulesSnapshot,
    NotionalReference,
    bounded_decimal,
)
from omega.domain.models import Candle, Instrument, Market, OrderRequest
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
)


def _identity(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValidationError("execution cycle identities must be bounded opaque strings")


@dataclass(frozen=True, slots=True)
class ExecutionCycle:
    account_id: str
    strategy_id: str
    cycle_id: str
    source_digest: str

    def __post_init__(self):
        for value in (self.account_id, self.strategy_id, self.cycle_id):
            _identity(value)
        if (
            type(self.source_digest) is not str
            or re.fullmatch(r"[a-f0-9]{64}", self.source_digest) is None
        ):
            raise ValidationError("execution source digest must be a canonical SHA256")


def _cycle(value):
    if type(value) is not ExecutionCycle:
        raise ValidationError("a typed immutable execution cycle is required")
    return replace(value)


def _instrument(value):
    if type(value) is not Instrument:
        raise ValidationError("a typed execution instrument is required")
    try:
        return replace(value)
    except (TypeError, ValueError):
        raise ValidationError("invalid execution instrument identity") from None


def _proposal(value):
    if type(value) is not OrderProposal:
        raise ValidationError("a typed immutable order proposal is required")
    bounded_decimal(value.quantity)
    if value.price is not None:
        bounded_decimal(value.price)
    return replace(value)


@dataclass(frozen=True, slots=True)
class ExecutionIntent:
    cycle: ExecutionCycle
    instrument: Instrument
    ordinal: int
    proposal: OrderProposal

    def __post_init__(self):
        _ = self.request  # Validate without caching a separately mutable request.

    @property
    def request(self) -> OrderRequest:
        cycle, instrument, proposal = (
            _cycle(self.cycle),
            _instrument(self.instrument),
            _proposal(self.proposal),
        )
        if type(self.ordinal) is not int or not 0 <= self.ordinal < 100:
            raise ValidationError("execution intent ordinal must be within 0..99")
        identity = (
            "omega.execution-intent.v1",
            cycle.account_id,
            cycle.strategy_id,
            cycle.cycle_id,
            instrument.key,
            self.ordinal,
        )
        digest = sha256(json.dumps(identity, separators=(",", ":"), ensure_ascii=False).encode())
        return OrderRequest(
            instrument,
            "om_" + digest.hexdigest()[:32],
            proposal.side,
            proposal.kind,
            proposal.quantity,
            proposal.price,
            reduce_only=proposal.reducing and instrument.market == Market.USDM,
        )

    @property
    def reducing(self) -> bool:
        return _proposal(self.proposal).reducing


@dataclass(frozen=True, slots=True)
class ExecutionDecision:
    cycle: ExecutionCycle
    intents: tuple[ExecutionIntent, ...]
    cancel_intent: ExecutionIntent | None
    reason: str

    def __post_init__(self):
        _cycle(self.cycle)
        if (
            type(self.intents) is not tuple
            or len(self.intents) > 1
            or type(self.reason) is not str
            or not 1 <= len(self.reason) <= 128
            or self.cancel_intent is not None
            and self.intents
        ):
            raise ValidationError("invalid serial execution decision")
        for ordinal, intent in enumerate(self.intents):
            if (
                type(intent) is not ExecutionIntent
                or intent.cycle != self.cycle
                or intent.ordinal != ordinal
            ):
                raise ValidationError("execution decision intent differs from its cycle")
            _ = intent.request
        if self.cancel_intent is not None:
            if type(self.cancel_intent) is not ExecutionIntent:
                raise ValidationError("cancellation requires the original typed intent")
            _ = self.cancel_intent.request
            if (
                self.cancel_intent.cycle.account_id != self.cycle.account_id
                or self.cancel_intent.cycle.strategy_id != self.cycle.strategy_id
            ):
                raise ValidationError("cancellation cannot cross account or strategy ownership")

    @property
    def cancel_client_order_id(self) -> str | None:
        return None if self.cancel_intent is None else self.cancel_intent.request.client_order_id


def _context(value):
    if (
        type(value) is not TradeContext
        or type(value.position) is not PositionView
        or type(value.budget) is not BudgetView
        or type(value.constraints) is not ExecutionConstraints
        or type(value.constraints.can_short) is not bool
        or type(value.observed_at_ms) is not int
        or not 0 <= value.observed_at_ms <= MAX_TIME
    ):
        raise ValidationError("typed bounded execution context is required")
    _instrument(value.instrument)
    for amount in (
        value.price,
        value.position.quantity,
        value.position.entry_price,
        value.budget.allocated,
        value.budget.available,
    ):
        bounded_decimal(amount)
    if value.closed_candle is not None:
        candle = value.closed_candle
        if (
            type(candle) is not Candle
            or type(candle.open_time_ms) is not int
            or type(candle.close_time_ms) is not int
        ):
            raise ValidationError("execution closed candle must have typed minute boundaries")
        replace(candle)
    # Revalidate existing domain contracts; features remain caller-owned, untouched.
    return replace(
        value,
        position=replace(value.position),
        budget=replace(value.budget),
        constraints=replace(value.constraints),
    )


def prepare_execution_decision(
    cycle,
    context,
    decision,
    *,
    pending_intent=None,
    original=None,
    rules=None,
    rule_references=(),
) -> ExecutionDecision:
    """Prepare a deterministic serial request, never permission to dispatch it.

    ``rules=None`` performs generic TS/account-constraint checks ONLY, not venue
    validation. Supplied normalized rules are checked strictly using the caller's
    observation clock; provider-specific blind spots and actual account authority
    remain the caller's responsibility. This function never samples an actual clock.
    Cancellation refers to the original pending intent, not the new cycle's ID.
    ``original`` adds exact decision conflict detection, not durable reconciliation.
    """
    cycle, context = _cycle(cycle), _context(context)
    if type(decision) is not TradeDecision:
        raise ValidationError("a typed immutable TS decision is required")
    decision = replace(decision)
    for proposal in decision.orders:
        _proposal(proposal)
    if type(rule_references) is not tuple or len(rule_references) > 8:
        raise ValidationError("execution rule references must be a bounded immutable tuple")
    if rules is None:
        if rule_references:
            raise ValidationError("rule references need an explicit execution rules snapshot")
    elif type(rules) is not ExecutionRulesSnapshot or rules.instrument != context.instrument:
        raise ValidationError("execution rules do not match the context instrument")
    else:
        rules = ExecutionRulesSnapshot.from_document(rules.document())
    for reference in rule_references:
        if type(reference) is not NotionalReference:
            raise ValidationError("typed execution rule references are required")
        replace(reference)
    if pending_intent is not None:
        if type(pending_intent) is not ExecutionIntent:
            raise ValidationError("a typed original pending execution intent is required")
        _ = pending_intent.request
        if (
            pending_intent.cycle.account_id != cycle.account_id
            or pending_intent.cycle.strategy_id != cycle.strategy_id
            or pending_intent.instrument != context.instrument
        ):
            raise ValidationError("pending intent differs from the execution owner or instrument")
    with localcontext() as calculation:
        calculation.prec = 256
        validate_decision(context, decision)
    if decision.cancel_pending and pending_intent is None:
        raise ValidationError("cancellation needs its original pending execution intent")
    if (
        decision.orders
        and context.instrument.market == Market.SPOT
        and context.position.quantity < 0
    ):
        raise TradingBlocked(
            "spot cash inventory is inconsistent; reconcile before preparing orders"
        )
    if rules is not None:
        for proposal in decision.orders:
            validate_execution_order(
                context.instrument,
                proposal,
                rules,
                now_ms=context.observed_at_ms,
                strict=True,
                references=rule_references,
            )
    result = ExecutionDecision(
        cycle,
        tuple(
            ExecutionIntent(cycle, context.instrument, ordinal, proposal)
            for ordinal, proposal in enumerate(decision.orders)
        ),
        pending_intent if decision.cancel_pending else None,
        decision.reason,
    )
    if original is not None:
        if type(original) is not ExecutionDecision:
            raise ValidationError("the original prepared decision must be typed")
        replace(original)
        if result != original:
            raise ValidationError("execution cycle already belongs to another source or decision")
    return result
