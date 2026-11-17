"""Historical tracker preparation never creates orders or invents closed anchors."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext
from omega.strategies.tef.module import TefModule
from omega.strategies.tef.strategy import TefModel

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def candle(index, price="100"):
    value = Decimal(price)
    return Candle(
        INSTRUMENT, index * 60000, (index + 1) * 60000, value, value, value, value, Decimal(1)
    )


def features(**changes):
    return {
        "CLOSED_11": False,
        "CLOSED_5": False,
        "11_EMA_0_EMA": 101.0,
        "5_MMACD_MSDELTAABSMAREL": -0.6,
        **changes,
    }


def context(index, fields):
    bar = candle(index)
    return TradeContext(
        INSTRUMENT,
        bar.close,
        bar.close_time_ms,
        PositionView(),
        BudgetView(Decimal(100), Decimal(100)),
        ExecutionConstraints(can_short=True),
        bar,
        fields,
    )


def test_preview_is_not_evidence_that_a_closed_anchor_was_consumed():
    module = TefModule(INSTRUMENT, {"model": "BSCALPHA"})
    assert module.can_prime_history() and not module.history_priming_ready
    module.prime_history(candle(0), features())
    assert not module.can_prime_history() and not module.history_priming_ready
    assert module.model.state["EMA_EMA_last"] == 0  # Eta compatibility until a closed daily bar.
    module.prime_history(candle(1), features(CLOSED_5=True))
    assert not module.history_priming_ready
    module.prime_history(candle(2), features(CLOSED_11=True))
    assert module.history_priming_ready
    assert module.model.state["EMA_EMA_last"] == 101.0
    assert module.last_open_time_ms == 120000
    assert module.snapshot()["pending_stop"] is None
    assert not module.snapshot()["halted"]


def test_incomplete_inputs_cannot_mark_closed_anchors_ready_or_resize_positions():
    module = TefModule(INSTRUMENT, {"model": "BSCALPHA"})
    row = features(CLOSED_11=True, CLOSED_5=True)
    row["11_EMA_0_EMA"] = None
    module.prime_history(candle(0), row)
    assert module.model.checkpoint()["closed_intervals"] == []
    decision = module.evaluate(context(1, features()))
    assert not decision.orders and decision.reason == "indicator_warmup"
    assert not module.history_priming_ready


def test_local_protection_continues_before_model_anchors_are_ready():
    module = TefModule(INSTRUMENT, {"model": "BSCALPHA", "fullStopLossImmediate": "0.05"})
    row = replace(
        context(0, features()),
        price=Decimal(90),
        position=PositionView(Decimal(1), Decimal(100), 1),
        entries_allowed=False,
    )
    result = module.evaluate(row)
    assert result.reason == "FSLIMMED" and result.orders[0].reducing
    before = module.checkpoint()
    with pytest.raises(ValidationError, match="active local protection"):
        module.prime_history(candle(1), features(CLOSED_11=True, CLOSED_5=True))
    assert module.checkpoint() == before


def test_json_checkpoint_keeps_partially_primed_anchors_and_continues_exactly():
    original = TefModule(INSTRUMENT, {"model": "BSCALPHA"})
    original.prime_history(candle(0), features(CLOSED_11=True))
    restored = TefModule(INSTRUMENT, {"model": "BSCALPHA"})
    restored.restore(json.loads(json.dumps(original.checkpoint())))
    assert not restored.history_priming_ready
    for module in (original, restored):
        module.prime_history(candle(1), features(CLOSED_5=True))
        assert module.history_priming_ready
    assert restored.checkpoint() == original.checkpoint()


def test_legacy_model_checkpoint_does_not_claim_unknown_anchor_history():
    model = TefModel("BSCALPHA", INSTRUMENT)
    model.evaluate(candle(0), features(CLOSED_11=True, CLOSED_5=True))
    checkpoint = model.checkpoint()
    checkpoint.pop("closed_intervals")
    checkpoint["version"] = 1
    restored = TefModel("BSCALPHA", INSTRUMENT)
    restored.restore(checkpoint)
    assert restored.state == model.state
    assert not restored.history_priming_ready
    restored.evaluate(candle(1), features(CLOSED_11=True, CLOSED_5=True))
    assert restored.history_priming_ready


@pytest.mark.parametrize("protect", [False, True])
def test_legacy_held_position_exposes_unproven_anchors_and_keeps_configured_local_protection(
    protect,
):
    configuration = {"model": "BSCALPHA"}
    if protect:
        configuration["fullStopLossImmediate"] = "0.05"
    original = TefModule(INSTRUMENT, configuration)
    original.prime_history(candle(0), features(CLOSED_11=True, CLOSED_5=True))
    legacy = original.checkpoint()
    legacy["payload"]["model"].pop("closed_intervals")
    legacy["payload"]["model"]["version"] = 1
    legacy["sha256"] = sha256(
        json.dumps(legacy["payload"], sort_keys=True, allow_nan=False).encode()
    ).hexdigest()
    restored = TefModule(INSTRUMENT, configuration)
    restored.restore(legacy)
    state = restored.snapshot()["history_priming"]
    assert not state["ready"] and state["reason"] == "confirmed_anchors_missing"
    assert state["pending_closed_intervals"] == ["CLOSED_11", "CLOSED_5"]
    bar = candle(1, "80")
    result = restored.evaluate(
        replace(
            context(1, features(CLOSED_5=True)),
            price=bar.close,
            closed_candle=bar,
            position=PositionView(Decimal(1), Decimal(100), 1),
        )
    )
    assert restored.snapshot()["history_priming"]["pending_closed_intervals"] == ["CLOSED_11"]
    if protect:
        assert result.reason == "FSLIMMED" and result.orders[0].reducing
    else:
        assert not result.orders and result.reason == "indicator_warmup"


@pytest.mark.parametrize("anchors", [["CLOSED_0"], ["CLOSED_5", "CLOSED_5"], [True], "CLOSED_5"])
def test_invalid_anchor_checkpoint_does_not_mutate_existing_model(anchors):
    model = TefModel("BSCALPHA", INSTRUMENT)
    before = model.checkpoint()
    invalid = deepcopy(before)
    invalid["closed_intervals"] = anchors
    with pytest.raises(ValidationError):
        model.restore(invalid)
    assert model.checkpoint() == before


@pytest.mark.parametrize(
    "case",
    json.loads(Path("tests/fixtures/tef-legacy.json").read_text(encoding="utf-8")),
    ids=lambda row: row["model"],
)
def test_priming_all_six_models_matches_sequential_legacy_tracker_without_proposals(case):
    module = TefModule(INSTRUMENT, {"model": case["model"]})
    reference = TefModel(case["model"], INSTRUMENT)
    for index, step in enumerate(case["steps"]):
        bar = candle(index, str(step["features"]["KLINE_CLOSEPRICE"]))
        assert module.prime_history(bar, step["features"]) is None
        reference.evaluate(bar, step["features"])
        assert module.model.checkpoint() == reference.checkpoint()
        assert module.snapshot()["pending_stop"] is None
    assert module.history_priming_ready
