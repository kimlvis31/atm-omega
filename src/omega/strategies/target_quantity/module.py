"""A fixed signed-quantity target TS; no TEF calculation, normalized exposure or leverage sizing."""

from dataclasses import replace
from decimal import localcontext

from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Side, decimal
from omega.domain.trading import (
    OrderProposal,
    TradeContext,
    TradeDecision,
    close_position,
)
from omega.ports.modules import ModuleDescriptor


class TargetQuantityModule:
    descriptor = ModuleDescriptor("target_quantity", "1", frozenset(), False)

    def __init__(self, instrument: Instrument, configuration: dict | None = None):
        config = dict(configuration or {})
        if config.keys() - {"quantity"}:
            raise ValidationError("unknown quantity-target configuration")
        self.instrument = instrument
        self.target = decimal(config.get("quantity", "0"))

    def snapshot(self):
        return {"module": "target_quantity", "quantity": str(self.target)}

    def resume(self):
        pass

    def checkpoint(self):
        return self.snapshot() | {"version": 1, "instrument": self.instrument.key}

    def restore(self, state):
        if state != self.checkpoint():
            raise ValidationError("quantity target checkpoint mismatch")

    def preview_protection(self, context: TradeContext) -> TradeDecision:
        """Reuse the pure target policy with expansion explicitly disabled."""
        return self.evaluate(replace(context, entries_allowed=False))

    def evaluate(self, context: TradeContext) -> TradeDecision:
        if context.instrument != self.instrument:
            raise ValidationError("quantity module instrument mismatch")
        position = context.position.quantity
        if position * self.target < 0:
            return close_position(context, "direction_change_close")
        with localcontext() as calculation:
            calculation.prec = 256
            delta = self.target - position
        reducing = position != 0 and self.target.copy_abs() < position.copy_abs()
        if delta == 0:
            return TradeDecision(reason="target_reached")
        if not reducing and not context.entries_allowed:
            return TradeDecision(reason="new_entries_paused")
        quantity = context.constraints.floor_quantity(delta.copy_abs())
        if quantity <= 0:
            return TradeDecision(reason="below_quantity_step")
        return TradeDecision(
            (OrderProposal(Side.BUY if delta > 0 else Side.SELL, quantity, reducing=reducing),),
            "quantity_target",
        )


def create(instrument: Instrument, configuration: dict | None = None) -> TargetQuantityModule:
    return TargetQuantityModule(instrument, configuration)
