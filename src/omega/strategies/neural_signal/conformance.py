"""Synthetic family safety probes; no NN execution or source authority claims."""

import json
from dataclasses import replace
from decimal import Decimal as D

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Side
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext


def validate(factory, instrument):
    def sample(module, *, index=0, close="100", signal="1", **changes):
        price = D(close)
        candle = Candle(
            instrument, index * 60000, (index + 1) * 60000, price, price, price, price, D(1)
        )
        return replace(
            TradeContext(
                instrument,
                price,
                candle.close_time_ms,
                PositionView(),
                BudgetView(D(1000), D(1000)),
                ExecutionConstraints(can_short=True),
                candle,
                {module.signal_field: D(signal)},
            ),
            **changes,
        )

    def entry_and_reverse():
        module = factory({"quantity": "2"})
        first = sample(module)
        assert module.evaluate(first).orders[0].quantity == 2
        held = PositionView(D(1), D(100), 1)
        next_context = sample(module, index=1, signal="-1", position=held)
        closing = module.evaluate(next_context).orders[0]
        assert closing.reducing and closing.quantity == 1 and closing.side == Side.SELL
        opening = module.evaluate(replace(next_context, position=PositionView(revision=2))).orders[
            0
        ]
        assert not opening.reducing and opening.quantity == 2 and opening.side == Side.SELL

    def deadband():
        module = factory({"threshold": "0.1"})
        decision = module.evaluate(
            sample(module, signal="0.1", position=PositionView(D(1), D(100), 1))
        )
        assert not decision.orders and decision.reason == "neural_deadband_hold"

    def strict_feature():
        module = factory()
        for features in ({}, {module.signal_field: 1.0}, {module.signal_field: D("NaN")}):
            try:
                module.evaluate(replace(sample(module), features=features))
            except ValidationError:
                continue
            raise AssertionError("missing or untyped neural feature was accepted")

    def no_signal_protection():
        module = factory()
        stopped = replace(
            sample(
                module, close="95", position=PositionView(D(1), D(100), 1), entries_allowed=False
            ),
            features={},
        )
        decision = module.evaluate_protection(stopped)
        assert decision.orders[0].reducing
        later = replace(sample(module, index=1), features={}, position=PositionView(revision=2))
        assert not module.evaluate_protection(later).orders
        assert not module.evaluate(
            sample(module, index=1, position=PositionView(revision=2))
        ).orders
        module.resume()
        assert module.evaluate(sample(module, index=1, position=PositionView(revision=2))).orders

    def cancel_before_close():
        module = factory()
        pending = replace(
            sample(
                module, close="95", position=PositionView(D(1), D(100), 1), has_pending_order=True
            ),
            features={},
        )
        decision = module.evaluate_protection(pending)
        assert decision.cancel_pending and not decision.orders
        duplicate = module.evaluate_protection(pending)
        assert not duplicate.orders and not duplicate.cancel_pending
        assert (
            module.evaluate_protection(replace(pending, has_pending_order=False)).orders[0].reducing
        )

    def checkpoint():
        module = factory()
        stopped = replace(
            sample(module, close="95", position=PositionView(D(1), D(100), 1)), features={}
        )
        module.evaluate_protection(stopped)
        restored = factory()
        restored.restore(json.loads(json.dumps(module.checkpoint(), allow_nan=False)))
        partial = replace(
            sample(module, index=1, position=PositionView(D("0.5"), D(100), 2)), features={}
        )
        assert restored.evaluate_protection(partial) == module.evaluate_protection(partial)
        assert restored.checkpoint() == module.checkpoint()

    cases = []
    for identity, action in (
        ("neural_fixed_quantity_close_before_reverse", entry_and_reverse),
        ("neural_deadband_holds_position", deadband),
        ("neural_requires_finite_decimal", strict_feature),
        ("neural_protection_without_signal_and_latch", no_signal_protection),
        ("neural_cancel_before_protective_replacement", cancel_before_close),
        ("neural_checkpoint_residual_stop", checkpoint),
    ):
        try:
            action()
            cases.append({"id": identity, "status": "passed"})
        except Exception as error:
            cases.append(
                {
                    "id": identity,
                    "status": "failed",
                    "error": type(error).__name__,
                    "detail": str(error)[:300],
                }
            )
    return cases
