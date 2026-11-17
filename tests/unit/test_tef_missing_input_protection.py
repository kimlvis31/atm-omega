"""Missing native inputs consume TEF time but never calculate a trading signal."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D

import pytest
from test_tef_module import INSTRUMENT, context

from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market, OrderKind, Side
from omega.domain.trading import PositionView
from omega.strategies.tef.module import TefModule
from omega.strategies.tef.strategy import TefModel


def explode(*_args):
    raise AssertionError("a protection-only minute must not invoke the calculator")


def roundtrip(module):
    document = json.loads(json.dumps(module.checkpoint()))
    restored = TefModule(INSTRUMENT, module.config)
    restored.restore(document)
    assert json.loads(json.dumps(restored.checkpoint())) == document
    return restored


@pytest.mark.parametrize("name", ["MMACDDEFAULT", "PATHFINDER", "CGALPHA"])
def test_missing_minutes_never_calculate_or_prime_even_cached_path_models(monkeypatch, name):
    module = TefModule(INSTRUMENT, {"model": name})
    monkeypatch.setattr(module.model, "_calculate", explode)
    original = module.model.checkpoint()
    for index in range(2):
        decision = module.evaluate_protection(context(index, feature=1.0))
        assert decision.reason == "tef_inputs_unavailable"
        assert not decision.orders and not decision.cancel_pending
    after = module.model.checkpoint()
    assert after["tracker"] == original["tracker"] == {}
    assert after["last_direction"] is None and after["signal"] is None
    assert after["closed_intervals"] == [] and not module.history_priming_ready
    assert module.model.last_open_time_ms == 60000
    assert module.snapshot()["model_failure"] is None
    roundtrip(module)


def test_identical_already_known_bar_keeps_original_signal_and_exact_checkpoint(monkeypatch):
    module = TefModule(INSTRUMENT)
    sample = context()
    assert module.evaluate(sample).orders
    original = deepcopy(module.checkpoint())
    monkeypatch.setattr(module.model, "_calculate", explode)
    decision = module.evaluate_protection(sample)
    assert not decision.orders and not decision.cancel_pending
    assert module.checkpoint() == original
    assert module.model.checkpoint()["signal"] is not None


def test_unavailable_minute_keeps_trackers_and_anchors_without_reauthorizing_late_features():
    model = TefModel("BSCALPHA", INSTRUMENT)
    fields = {
        "CLOSED_11": True,
        "CLOSED_5": True,
        "11_EMA_0_EMA": 101.0,
        "5_MMACD_MSDELTAABSMAREL": -0.6,
    }
    model.evaluate(context(0).closed_candle, fields)
    original = model.checkpoint()
    assert set(original["closed_intervals"]) == {"CLOSED_11", "CLOSED_5"}
    model.advance_unavailable(context(1).closed_candle)
    frozen = model.checkpoint()
    assert frozen["tracker"] == original["tracker"]
    assert frozen["last_direction"] == original["last_direction"]
    assert frozen["closed_intervals"] == original["closed_intervals"]
    assert frozen["signal"] is None and not model.history_priming_ready
    assert frozen["features_digest"] != original["features_digest"]
    model.advance_unavailable(context(1).closed_candle)
    assert model.checkpoint() == frozen
    restored = TefModel("BSCALPHA", INSTRUMENT)
    restored.restore(json.loads(json.dumps(frozen)))
    with pytest.raises(ValidationError, match="same minute"):
        restored.evaluate(context(1).closed_candle, fields)
    assert restored.checkpoint() == frozen
    assert restored.evaluate(context(2).closed_candle, fields) is not None
    assert restored.history_priming_ready


def test_restored_missing_clock_allows_the_healthy_next_minute_without_sticky_failure():
    module = TefModule(INSTRUMENT)
    assert not module.evaluate_protection(context(0)).orders
    restored = roundtrip(module)
    decision = restored.evaluate(context(1, feature=-1.0))
    assert decision.orders and decision.orders[0].side == Side.SELL
    assert restored.model.last_open_time_ms == 60000
    assert restored.snapshot()["model_failure"] is None


@pytest.mark.parametrize(
    "fault", ["foreign", "late", "gap", "changed", "float", "bool", "unaligned", "ohlc", "future"]
)
def test_rejected_unavailable_candle_never_mutates_the_existing_model(fault):
    model = TefModel("MMACDDEFAULT", INSTRUMENT)
    model.advance_unavailable(context(1).closed_candle)
    original = model.checkpoint()
    candidate = context(2).closed_candle
    if fault == "foreign":
        candidate = replace(candidate, instrument=Instrument("binance", Market.USDM, "ETHUSDT"))
    elif fault == "late":
        candidate = context(0).closed_candle
    elif fault == "gap":
        candidate = context(3).closed_candle
    elif fault == "changed":
        candidate = context(1, close="101").closed_candle
    elif fault == "float":
        object.__setattr__(candidate, "open_time_ms", float(candidate.open_time_ms))
    elif fault == "bool":
        object.__setattr__(candidate, "open_time_ms", True)
    elif fault == "unaligned":
        object.__setattr__(candidate, "open_time_ms", 120001)
        object.__setattr__(candidate, "close_time_ms", 180001)
    elif fault == "ohlc":
        object.__setattr__(candidate, "low", D(150))
    else:
        object.__setattr__(candidate, "close_time_ms", 253_402_300_800_000)
    with pytest.raises(ValidationError):
        model.advance_unavailable(candidate)
    assert model.checkpoint() == original


@pytest.mark.parametrize(
    "configuration,quantity,price,side,reason",
    [
        ({"fullStopLossImmediate": "0.05"}, "1", "95", Side.SELL, "FSLIMMED"),
        ({"fullStopLossImmediate": "0.05"}, "-1", "105", Side.BUY, "FSLIMMED"),
        ({"fullStopLossClose": "0.05"}, "1", "95", Side.SELL, "FSLCLOSE"),
    ],
)
def test_missing_inputs_keep_existing_local_market_reductions(
    monkeypatch, configuration, quantity, price, side, reason
):
    module = TefModule(INSTRUMENT, {**configuration, "order_type": "LIMIT"})
    monkeypatch.setattr(module.model, "_calculate", explode)
    decision = module.evaluate_protection(
        context(
            close=price,
            position=PositionView(D(quantity), D(100), 1),
            entries_allowed=False,
        )
    )
    assert decision.reason == reason and len(decision.orders) == 1
    proposal = decision.orders[0]
    assert proposal.reducing and proposal.side == side and proposal.quantity == 1
    assert proposal.kind == OrderKind.MARKET
    assert module.snapshot()["pending_stop"] is not None
    assert not module.snapshot()["halted"]


def test_pending_cancel_and_confirmed_stop_survive_checkpoint_without_duplicate_reduction():
    module = TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"})
    sample = context(close="95", position=PositionView(D(1), D(100), 1))
    cancellation = module.evaluate_protection(replace(sample, has_pending_order=True))
    assert cancellation.cancel_pending and not cancellation.orders
    assert module.snapshot()["pending_stop"] is None
    reduction = module.evaluate_protection(sample)
    assert reduction.orders[0].reducing and not reduction.cancel_pending
    restored = roundtrip(module)
    waiting = restored.evaluate_protection(replace(sample, has_pending_order=True))
    assert waiting.reason == "await_pending_protection_order" and not waiting.orders
    unconfirmed = restored.evaluate_protection(replace(sample, position=PositionView(revision=1)))
    assert unconfirmed.reason == "await_local_stop_confirmation" and not unconfirmed.orders
    confirmed = restored.evaluate_protection(context(1, position=PositionView(revision=2)))
    assert confirmed.reason == "stopped_after_local_stop" and not confirmed.orders
    assert restored.snapshot()["pending_stop"] is None and restored.snapshot()["halted"]


@pytest.mark.parametrize("gate", [{"state_certain": False}, {"control_owned": False}])
def test_uncertain_financial_state_still_blocks_local_protection_proposals(gate):
    module = TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"})
    result = module.evaluate_protection(
        context(
            close="95",
            position=PositionView(D(1), D(100), 1),
            **gate,
        )
    )
    assert result.reason == "position_or_control_uncertain"
    assert not result.orders and not result.cancel_pending
    assert module.snapshot()["pending_stop"] is None


def test_existing_model_failure_uses_only_original_protection_clock_and_keeps_the_fault(
    monkeypatch,
):
    module = TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"})
    monkeypatch.setattr(module.model, "_calculate", explode)
    assert module.evaluate(context(0)).reason == "model_failure_requires_recovery"
    frozen_model, failure = module.model.checkpoint(), module.snapshot()["model_failure"]
    monkeypatch.setattr(module.model, "advance_unavailable", explode)
    decision = module.evaluate_protection(
        context(
            1,
            close="95",
            position=PositionView(D(1), D(100), 1),
            entries_allowed=False,
        )
    )
    assert decision.orders[0].reducing and decision.reason == "FSLIMMED"
    assert module.model.checkpoint() == frozen_model
    assert module.snapshot()["model_failure"] == failure
    assert module.last_open_time_ms == 60000 and module.model.last_open_time_ms is None
    restored = roundtrip(module)
    assert restored.snapshot()["model_failure"] == failure
    assert not restored.history_priming_ready


def test_foreign_context_and_missing_closed_candle_never_create_a_signal():
    module = TefModule(INSTRUMENT)
    sample = context(0)
    foreign = Instrument("binance", Market.USDM, "ETHUSDT")
    foreign_context = replace(
        sample, instrument=foreign, closed_candle=replace(sample.closed_candle, instrument=foreign)
    )
    before = module.checkpoint()
    with pytest.raises(ValidationError, match="instrument"):
        module.evaluate_protection(foreign_context)
    assert module.checkpoint() == before
    waiting = module.evaluate_protection(replace(sample, closed_candle=None))
    assert waiting.reason == "await_closed_minute" and not waiting.orders
    assert module.checkpoint() == before
