"""TS-independent proposal validation. No module can bypass common execution controls."""

from copy import deepcopy
from dataclasses import replace

from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.models import Market, Side
from omega.domain.trading import TradeContext, TradeDecision
from omega.ports.trade_strategy import TradeStrategy


def _protection_proposal(module, context, *, preview=False):
    callback = getattr(module, "preview_protection", None) if preview else None
    if not callable(callback):
        callback = getattr(module, "evaluate_protection", None)
    if not callable(callback):
        raise ValidationError("strategy has no independent protection capability")
    decision = callback(context)
    if not isinstance(decision, TradeDecision) or any(
        not order.reducing for order in decision.orders
    ):
        raise ValidationError("protection capability may only cancel or reduce exposure")
    return decision


def validate_decision(context: TradeContext, decision: TradeDecision) -> None:
    if not isinstance(decision, TradeDecision):
        raise ValidationError("TS module returned an invalid decision")
    if not decision.orders and not decision.cancel_pending:
        return
    if context.instrument.market == Market.SPOT and context.constraints.leverage != 1:
        raise ValidationError("spot cash execution requires leverage one")
    if not context.state_certain or not context.control_owned:
        raise TradingBlocked("position state or control is uncertain")
    if decision.cancel_pending:
        if not context.has_pending_order:
            raise TradingBlocked("no pending order to cancel")
        return
    if context.has_pending_order:
        raise TradingBlocked("existing order must settle before another proposal is accepted")
    # Serial planning is explicit; multi-order grids require a reservation-aware batch executor.
    if len(decision.orders) > 1:
        raise TradingBlocked("this executor requires serial order planning")
    for proposal in decision.orders:
        rules, position = context.constraints, context.position.quantity
        if rules.floor_quantity(proposal.quantity, proposal.kind) != proposal.quantity:
            raise ValidationError("proposed quantity violates instrument step or minimum")
        if (
            proposal.price is not None
            and rules.limit_price(proposal.price, proposal.side) != proposal.price
        ):
            raise ValidationError("proposed limit price violates the instrument tick")
        opposite = (position > 0 and proposal.side == Side.SELL) or (
            position < 0 and proposal.side == Side.BUY
        )
        if proposal.reducing:
            if not opposite or proposal.quantity > abs(position):
                raise TradingBlocked("reduction would exceed or reverse the observed position")
        else:
            if opposite:
                raise TradingBlocked("close the current direction before opening the other side")
            if not context.entries_allowed:
                raise TradingBlocked("new entries are paused")
            if proposal.side == Side.SELL and (
                not rules.can_short or context.instrument.market == Market.SPOT
            ):
                raise TradingBlocked("instrument does not support opening short exposure")
            price = proposal.price or context.price
            cost = proposal.quantity * price / rules.leverage
            committed = abs(position) * context.position.entry_price / rules.leverage
            if cost > context.budget.available or committed + cost > context.budget.allocated:
                raise TradingBlocked("proposal exceeds available or allocated linear-market budget")
        # Dust reductions are not silently converted into a new position or extra quantity.
        if proposal.quantity * (proposal.price or context.price) < rules.min_notional:
            raise TradingBlocked("proposal notional is below the instrument minimum")


class TradingEngine:
    def __init__(self, module: TradeStrategy):
        self.module = module
        self._last_key = None
        self._last_decision = None

    def peek_protection(self, context: TradeContext) -> TradeDecision:
        """Read a detached protective decision without changing this engine/module.

        A preview-only capability need not advance a durable strategy cursor.
        The committed protection path below still requires evaluate_protection.
        """
        if not isinstance(context, TradeContext):
            raise ValidationError("protection preview requires a typed context")
        sample = replace(context, entries_allowed=False)
        decision = _protection_proposal(deepcopy(self.module), sample, preview=True)
        try:
            validate_decision(sample, decision)
        except TradingBlocked as error:
            return TradeDecision(reason=f"blocked: {error}"[:128])
        return decision

    def evaluate(self, context: TradeContext, *, protection_only=False) -> TradeDecision:
        if type(protection_only) is not bool:
            raise ValidationError("explicit protection evaluation mode required")
        if protection_only:
            if not callable(getattr(self.module, "evaluate_protection", None)):
                raise ValidationError("strategy has no independent protection capability")
            context = replace(context, entries_allowed=False)
        # Position revisions permit a close-fill → new-direction cycle at the same market time.
        key = (
            context.observed_at_ms,
            context.instrument,
            context.position,
            context.price,
            context.closed_candle,
            dict(context.features),
            context.entries_allowed,
            context.state_certain,
            context.control_owned,
            context.has_pending_order,
            context.budget,
            context.constraints,
        )
        if context.protection_observation is not None:
            if not protection_only:
                raise ValidationError("partial observation only permits independent protection")
            key += (context.protection_observation,)
        if protection_only:
            key += ("protection_only",)
        if self._last_key == key:
            return self._last_decision
        if self._last_key is not None and context.observed_at_ms < self._last_key[0]:
            raise ValidationError("TS engine cannot consume a late observation without replay")
        candidate = deepcopy(self.module)
        decision = (
            _protection_proposal(candidate, context)
            if protection_only
            else candidate.evaluate(context)
        )
        try:
            validate_decision(context, decision)
        except TradingBlocked as exc:
            # A normal execution restriction must not freeze rolling analysis state.
            # Invalid plugin output still raises ValidationError and is not committed.
            rejected = getattr(candidate, "on_order_rejected", None)
            if callable(rejected):
                rejected(decision)
            decision = TradeDecision(reason=f"blocked: {exc}"[:128])
        self.module = candidate
        self._last_key, self._last_decision = key, decision
        return decision

    def reject_execution(self, decision: TradeDecision, reason: str) -> TradeDecision:
        """Reject an unsent decision after venue checks without rolling its model back."""
        if (
            decision != self._last_decision
            or type(reason) is not str
            or not 1 <= len(reason) <= 128
        ):
            raise ValidationError("execution rejection does not match the latest TS decision")
        candidate = deepcopy(self.module)
        callback = getattr(candidate, "on_order_rejected", None)
        if callable(callback):
            callback(decision)
        rejected = TradeDecision(reason=f"blocked: {reason}"[:128])
        self.module, self._last_decision = candidate, rejected
        return rejected
