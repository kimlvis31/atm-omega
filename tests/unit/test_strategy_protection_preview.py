"""Legacy strategy previews share protection rules without consuming normal history."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256

import pytest
from test_tef_module import INSTRUMENT, context

from omega.application.trading import TradingEngine
from omega.domain.models import Side
from omega.domain.trading import PositionView
from omega.strategies.target_quantity.module import TargetQuantityModule
from omega.strategies.tef.module import TefModule


def test_tef_normal_decisions_and_checkpoint_bytes_preserve_pre_preview_trajectory():
    engine = TradingEngine(
        TefModule(
            INSTRUMENT,
            {
                "fullStopLossImmediate": ".05",
                "fullStopLossClose": ".05",
                "order_type": "ADAPTIVE",
                "postStopLossReentry": True,
            },
        )
    )
    rows = []
    for index in range(24):
        quantity = ("0", "1", "1", "0", "-1", "-.5")[index % 6]
        price = ("100", "100", "95", "100", "105", "100")[index % 6]
        if index == 12:
            engine.module.resume()
        sample = context(
            index,
            close=price,
            feature=(0.25, -0.5, 0.8)[index % 3],
            position=PositionView(D(quantity), D(100) if D(quantity) else D(0), index + 1),
            entries_allowed=index % 4 != 0,
            has_pending_order=index % 7 == 0,
        )
        decision = engine.evaluate(sample)
        rows.append(
            {
                "checkpoint": engine.module.checkpoint(),
                "decision": [
                    decision.reason,
                    decision.cancel_pending,
                    [
                        [
                            order.side.value,
                            str(order.quantity),
                            order.kind.value,
                            None if order.price is None else str(order.price),
                            order.reducing,
                            order.reason,
                        ]
                        for order in decision.orders
                    ],
                ],
            }
        )
    # Captured from the accepted normal implementation before extracting _protection.
    assert (
        sha256(
            json.dumps(rows, sort_keys=True, allow_nan=False, separators=(",", ":")).encode()
        ).hexdigest()
        == "dccd912a9f7518111e8b076ea2fd8541434d735b2eb3f9121d05653f42549996"
    )


@pytest.mark.parametrize("quantity,price,side", [("1", "95", Side.SELL), ("-1", "105", Side.BUY)])
@pytest.mark.parametrize("pending", [False, True])
def test_tef_peek_stop_and_cancel_never_evaluate_model_or_commit_module(
    tmp_path, monkeypatch, quantity, price, side, pending
):
    module = TefModule(INSTRUMENT, {"fullStopLossImmediate": ".05"})
    monkeypatch.setattr(
        module.model,
        "evaluate",
        lambda *a, **k: pytest.fail("preview must not consume model history"),
    )
    engine = TradingEngine(module)
    sample = context(
        close=price,
        features={},
        position=PositionView(D(quantity), D(100), 1),
        has_pending_order=pending,
    )
    before = deepcopy(module.checkpoint())
    decision = engine.peek_protection(sample)
    assert decision.reason == "FSLIMMED"
    if pending:
        assert decision.cancel_pending and not decision.orders
    else:
        assert decision.orders[0].reducing and decision.orders[0].side == side
    assert module.checkpoint() == before and engine.module is module
    assert engine._last_key is None and engine._last_decision is None


def test_tef_safe_preview_cannot_turn_missing_indicator_into_model_failure(monkeypatch):
    module = TefModule(INSTRUMENT, {"fullStopLossClose": ".05"})
    monkeypatch.setattr(module.model, "evaluate", lambda *a, **k: pytest.fail("must not evaluate"))
    before = deepcopy(module.checkpoint())
    sample = context(features={}, position=PositionView(D(1), D(100), 1))
    assert not TradingEngine(module).peek_protection(sample).orders
    assert module.checkpoint() == before and module.snapshot()["model_failure"] is None


def test_tef_pending_stop_and_flat_confirmation_are_not_consumed_by_preview():
    engine = TradingEngine(TefModule(INSTRUMENT, {"fullStopLossClose": ".05"}))
    engine.evaluate(context(close="95", position=PositionView(D(1), D(100), 1)))
    before = deepcopy(engine.module.checkpoint())
    flat = context(1, features={}, position=PositionView(revision=2))
    assert not engine.peek_protection(flat).orders
    assert engine.module.checkpoint() == before
    assert engine.module.snapshot()["pending_stop"] is not None
    partial = replace(flat, position=PositionView(D(".4"), D(100), 2))
    assert engine.peek_protection(partial).orders[0].reducing
    assert engine.module.checkpoint() == before


@pytest.mark.parametrize(
    "target,position,expected",
    [("1", "1", False), ("2", "1", False), (".5", "1", True), ("-1", "1", True), ("1", "0", False)],
)
def test_target_preview_reuses_normal_grid_only_for_reductions(target, position, expected):
    module = TargetQuantityModule(INSTRUMENT, {"quantity": target})
    sample = context(position=PositionView(D(position), D(100) if D(position) else D(0), 1))
    before = module.checkpoint()
    decision = TradingEngine(module).peek_protection(sample)
    assert bool(decision.orders) is expected
    assert all(order.reducing for order in decision.orders)
    assert module.checkpoint() == before
