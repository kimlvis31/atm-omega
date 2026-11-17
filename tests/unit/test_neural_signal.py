"""Typed demo feature interpretation and signal-independent local close protection."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import ROUND_DOWN, localcontext
from decimal import Decimal as D
from hashlib import sha256

import pytest

from omega.application.trading import TradingEngine, validate_decision
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market, Side
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext
from omega.strategies.neural_signal.module import NeuralSignalModule
from omega.strategies.target_quantity.module import TargetQuantityModule

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def context(index=0, close="100", signal="1", **changes):
    value = D(close)
    candle = Candle(BTC, index * 60000, (index + 1) * 60000, value, value, value, value, D(10))
    return replace(
        TradeContext(
            BTC,
            value,
            candle.close_time_ms,
            PositionView(),
            BudgetView(D(1000), D(1000)),
            ExecutionConstraints(can_short=True),
            candle,
            {"NN.score": D(signal)},
        ),
        **changes,
    )


@pytest.mark.parametrize(
    "configuration",
    [
        [],
        {"unknown": "1"},
        {"signal_field": "score"},
        {"signal_field": "NN."},
        {"signal_field": True},
        {"quantity": "0"},
        {"quantity": "-1"},
        {"threshold": "-0.1"},
        {"threshold": float("nan")},
        {"quantity": True},
        {"local_stop_rate": "0"},
        {"local_stop_rate": "1"},
        {"quantity": "1E61"},
    ],
)
def test_invalid_configuration_never_creates_an_active_strategy(configuration):
    with pytest.raises(ValidationError):
        NeuralSignalModule(BTC, configuration)


def test_custom_required_field_is_exact_and_configuration_and_snapshot_are_detached():
    configuration = {"signal_field": "NN.custom", "quantity": "1.25"}
    module = NeuralSignalModule(BTC, configuration)
    configuration["quantity"] = "999"
    assert module.descriptor.required_fields == frozenset({"NN.custom"})
    assert module.validation_features() == {"NN.custom": D(1)}
    assert isinstance(module._long, TargetQuantityModule) and isinstance(
        module._short, TargetQuantityModule
    )
    detached = module.snapshot()
    detached["configuration"]["quantity"] = "500"
    assert module.snapshot()["configuration"]["quantity"] == "1.25"
    decision = module.evaluate(context(features={"NN.custom": D(1)}))
    assert decision.orders[0].quantity == D("1.25")


@pytest.mark.parametrize(
    "feature", [None, True, 1, 1.0, "1", D("NaN"), D("sNaN"), D("Infinity"), D("-Infinity")]
)
def test_normal_evaluation_requires_a_finite_decimal_and_does_not_consume_bad_input(feature):
    module = NeuralSignalModule(BTC)
    before = module.checkpoint()
    with pytest.raises(ValidationError, match="finite Decimal"):
        module.evaluate(context(features={} if feature is None else {"NN.score": feature}))
    assert module.checkpoint() == before


@pytest.mark.parametrize(
    "signal,side", [("1", Side.BUY), ("-1", Side.SELL), ("1E100000", Side.BUY)]
)
def test_fixed_quantity_uses_common_grid_without_signal_or_leverage_scaling(signal, side):
    engine = TradingEngine(NeuralSignalModule(BTC, {"quantity": "1.239"}))
    sample = context(
        signal=signal,
        constraints=ExecutionConstraints(quantity_step=D(".01"), leverage=D(10), can_short=True),
    )
    order = engine.evaluate(sample).orders[0]
    assert order.side == side and order.quantity == D("1.23") and not order.reducing
    assert engine.evaluate(sample).orders == (order,)


@pytest.mark.parametrize("signal", ["-.1", "0", ".1"])
def test_threshold_deadband_holds_position_and_never_means_flat(signal):
    module = NeuralSignalModule(BTC, {"threshold": ".1", "quantity": "1"})
    decision = module.evaluate(context(signal=signal, position=PositionView(D("1.7"), D(100), 1)))
    assert not decision.orders and not decision.cancel_pending
    assert decision.reason == "neural_deadband_hold"


def test_close_before_reverse_and_paused_reductions_reuse_target_policy():
    engine = TradingEngine(NeuralSignalModule(BTC, {"quantity": "2"}))
    held = context(signal="-1", position=PositionView(D(".5"), D(100), 1), entries_allowed=False)
    closing = engine.evaluate(held).orders[0]
    assert closing.reducing and closing.quantity == D(".5") and closing.side == Side.SELL
    assert not engine.evaluate(replace(held, position=PositionView(revision=2))).orders
    resumed = replace(held, position=PositionView(revision=2), entries_allowed=True)
    assert engine.evaluate(resumed).orders[0].quantity == 2


@pytest.mark.parametrize("quantity,close,side", [("1", "95", Side.SELL), ("-1", "105", Side.BUY)])
def test_protection_without_nn_works_at_closed_stop_boundary_during_pause(quantity, close, side):
    engine = TradingEngine(NeuralSignalModule(BTC))
    sample = context(
        close=close,
        features={},
        entries_allowed=False,
        position=PositionView(D(quantity), D(100), 1),
        budget=BudgetView(D(0), D(0)),
    )
    decision = engine.evaluate(sample, protection_only=True)
    assert decision.orders[0].reducing and decision.orders[0].side == side
    assert decision.orders[0].quantity == 1
    assert engine.module.snapshot()["halted"] and engine.module.last_open_time_ms == 0
    assert engine.evaluate(sample, protection_only=True) is decision


def test_protection_only_never_enters_and_does_not_consume_the_normal_signal_slot():
    engine = TradingEngine(NeuralSignalModule(BTC))
    sample = context()
    assert not engine.evaluate(sample, protection_only=True).orders
    assert engine.module.last_open_time_ms == 0 and engine.module.snapshot()["last_signal"] is None
    assert engine.evaluate(sample).orders[0].side == Side.BUY


def test_closed_stop_ignores_recovered_wicks_and_pre_entry_closes():
    module = NeuralSignalModule(BTC)
    base = context(position=PositionView(D(1), D(100), 1), features={})
    wick = replace(base.closed_candle, high=D(110), low=D(90))
    assert not module.evaluate_protection(replace(base, closed_candle=wick)).orders
    later = context(
        index=1,
        close="90",
        features={},
        observed_at_ms=120002,
        position=PositionView(D(1), D(100), 2, 120001),
    )
    assert not module.evaluate_protection(later).orders
    assert not module.snapshot()["halted"]


@pytest.mark.parametrize("gate", [{"state_certain": False}, {"control_owned": False}])
def test_uncertain_or_foreign_position_cannot_be_cancelled_or_reduced(gate):
    module = NeuralSignalModule(BTC)
    decision = module.evaluate_protection(
        context(
            close="95",
            features={},
            has_pending_order=True,
            position=PositionView(D(1), D(100), 1),
            **gate,
        )
    )
    assert not decision.orders and not decision.cancel_pending
    assert not module.snapshot()["halted"]


def test_cancel_ack_protection_pending_partial_fill_and_flat_latch_are_separate():
    module = NeuralSignalModule(BTC)
    pending = context(
        close="95", features={}, position=PositionView(D(1), D(100), 1), has_pending_order=True
    )
    first = module.evaluate_protection(pending)
    validate_decision(pending, first)
    assert first.cancel_pending and not first.orders
    duplicate = module.evaluate_protection(pending)
    assert not duplicate.cancel_pending and not duplicate.orders
    closing = module.evaluate_protection(replace(pending, has_pending_order=False))
    assert closing.orders[0].reducing
    assert not module.evaluate_protection(pending).cancel_pending
    assert not module.evaluate_protection(pending).orders
    recovered = context(index=1, features={}, position=PositionView(D(".4"), D(100), 2))
    assert module.evaluate_protection(recovered).orders[0].quantity == D(".4")
    flat = context(index=2, position=PositionView(revision=3))
    assert not module.evaluate(flat).orders
    assert module.snapshot()["pending_stop"] is None and module.snapshot()["halted"]
    module.resume()
    assert module.evaluate(flat).orders[0].side == Side.BUY


def test_resume_does_not_erase_pending_stop_or_allow_reexpansion_before_flat():
    module = NeuralSignalModule(BTC)
    module.evaluate_protection(
        context(close="95", features={}, position=PositionView(D(1), D(100), 1))
    )
    module.resume()
    assert module.snapshot()["pending_stop"] is not None
    partial = context(index=1, position=PositionView(D(".5"), D(100), 2))
    decision = module.evaluate(partial)
    assert decision.orders[0].reducing and decision.orders[0].quantity == D(".5")


def test_execution_rejection_keeps_stop_latch_and_retries_only_a_reduction():
    engine = TradingEngine(NeuralSignalModule(BTC))
    sample = context(
        close="95",
        features={},
        position=PositionView(D(1), D(100), 1),
        constraints=ExecutionConstraints(can_short=True, min_notional=D(200)),
    )
    assert not engine.evaluate(sample, protection_only=True).orders
    assert engine.module.snapshot()["halted"]
    assert engine.module.snapshot()["pending_stop"]["stage"] == "ready"
    result = engine.evaluate(context(index=1, position=PositionView(D(1), D(100), 1)))
    assert result.orders[0].reducing and result.orders[0].quantity == 1


def test_actual_fill_revision_is_required_before_clearing_stop_progress():
    module = NeuralSignalModule(BTC)
    module.evaluate_protection(
        context(close="95", features={}, position=PositionView(D(1), D(100), 1))
    )
    uncertain_flat = context(index=1, features={}, position=PositionView(revision=1))
    assert module.evaluate_protection(uncertain_flat).reason == "await_local_stop_confirmation"
    confirmed_flat = replace(
        uncertain_flat, position=PositionView(revision=2), has_pending_order=True
    )
    assert module.evaluate_protection(confirmed_flat).cancel_pending
    assert not module.evaluate_protection(confirmed_flat).cancel_pending
    module.evaluate_protection(replace(confirmed_flat, has_pending_order=False))
    assert module.snapshot()["pending_stop"] is None


def test_checkpoint_json_roundtrip_restores_cancel_progress_cursor_and_manual_resume():
    module = NeuralSignalModule(BTC, {"signal_field": "NN.custom"})
    stopped = context(
        close="95", features={}, position=PositionView(D(1), D(100), 1), has_pending_order=True
    )
    module.evaluate_protection(stopped)
    checkpoint = json.loads(json.dumps(module.checkpoint(), allow_nan=False))
    restored = NeuralSignalModule(BTC, {"signal_field": "NN.custom"})
    restored.restore(checkpoint)
    assert restored.checkpoint() == checkpoint and restored.last_open_time_ms == 0
    assert restored.evaluate_protection(stopped) == module.evaluate_protection(stopped)
    settled = replace(stopped, has_pending_order=False)
    assert restored.evaluate_protection(settled) == module.evaluate_protection(settled)
    restored.resume()
    assert restored.snapshot()["pending_stop"] is not None
    assert restored.last_open_time_ms == 0


@pytest.mark.parametrize(
    "change", ["checksum", "instrument", "configuration", "bool_cursor", "future_stop", "stage"]
)
def test_malformed_or_foreign_checkpoint_never_partially_mutates_module(change):
    module = NeuralSignalModule(BTC)
    module.evaluate_protection(
        context(close="95", features={}, position=PositionView(D(1), D(100), 1))
    )
    before = module.checkpoint()
    candidate = deepcopy(before)
    payload = candidate["payload"]
    if change == "checksum":
        candidate["sha256"] = "0" * 64
    else:
        if change == "instrument":
            payload["instrument"] = "binance:usdm:ETHUSDT"
        elif change == "configuration":
            payload["configuration"]["quantity"] = "2"
        elif change == "bool_cursor":
            payload["last_open_time_ms"] = False
        elif change == "future_stop":
            payload["pending_stop"]["open_time_ms"] = 60000
        else:
            payload["pending_stop"]["stage"] = "assumed_filled"
        candidate["sha256"] = sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    with pytest.raises(ValidationError):
        module.restore(candidate)
    assert module.checkpoint() == before


def test_late_or_conflicting_candles_and_same_bar_feature_changes_are_rejected():
    module = NeuralSignalModule(BTC)
    latest = context(index=1)
    module.evaluate(latest)
    before = module.checkpoint()
    for sample in (context(), context(index=1, close="99"), context(index=1, signal="2")):
        with pytest.raises(ValidationError, match="late|conflict"):
            module.evaluate(sample)
        assert module.checkpoint() == before


def test_equal_decimal_feature_spellings_keep_the_exact_same_bar_checkpoint():
    module = NeuralSignalModule(BTC)
    decision = module.evaluate(context(signal="1"))
    before = module.checkpoint()
    assert module.evaluate(context(signal="1.0")) == decision
    assert module.checkpoint() == before


def test_stop_math_and_fixed_quantity_are_independent_of_ambient_decimal_context():
    held = context(close="95", features={}, position=PositionView(D(1), D(100), 1))
    target = context(signal="-1")
    with localcontext() as calculation:
        calculation.prec, calculation.rounding, calculation.Emax, calculation.Emin = (
            3,
            ROUND_DOWN,
            3,
            -3,
        )
        assert NeuralSignalModule(BTC).evaluate_protection(held).orders[0].reducing
        assert NeuralSignalModule(BTC, {"quantity": "1.234"}).evaluate(target).orders[
            0
        ].quantity == D("1.234")
