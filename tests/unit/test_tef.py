"""Frozen legacy trajectories plus independently specified trading semantics."""

import json
from pathlib import Path

import pytest

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.strategies.tef.strategy import MODELS, TefModel

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")
REFERENCE = json.loads(Path("tests/fixtures/tef-legacy.json").read_text())


def candle(index=0, close="100"):
    price = decimal(close)
    return Candle(
        INSTRUMENT, index * 60_000, (index + 1) * 60_000, price, price, price, price, decimal("1")
    )


@pytest.mark.parametrize("reference", REFERENCE, ids=lambda r: r["model"])
def test_240_step_legacy_reference_per_completed_model(reference):
    strategy = TefModel(reference["model"], INSTRUMENT)
    for index, step in enumerate(reference["steps"]):
        signal = strategy.evaluate(
            candle(index, str(step["features"]["KLINE_CLOSEPRICE"])), step["features"]
        )
        assert signal.target_exposure == decimal(str(step["target"]))
        assert strategy.last_direction == step["direction"]
        assert signal.available_at_ms == (index + 1) * 60_000


def test_pathfinder_quantized_entry_neutral_hold_profit_take_and_swap():
    strategy = TefModel("PATHFINDER", INSTRUMENT, {"Swap_Ratio_S": 0.5})

    def run(index, mst, sts, price="100"):
        return strategy.evaluate(
            candle(index, price),
            {
                "CLOSED_5": True,
                "CLOSED_0": True,
                "5_MST_0_MST": mst,
                "0_STS_S": sts,
            },
        ).target_exposure

    assert run(0, 0.8, -0.3) == decimal("0.05")  # 0.2 * minimum 1/4 impulse
    assert run(1, 0.8, -0.6) == decimal("0.2")
    assert run(2, 0.0, -1.0) == decimal("0.2")  # Neutral holds, blocks accumulation.
    assert run(3, 0.0, 0.6, "101") == decimal("0")
    assert run(4, 0.8, -0.6) == decimal("0.2")  # TP cycle resets accumulation counter.
    assert run(5, -0.8, 0.0) == decimal("-0.1")


def test_bscalpha_direction_separate_from_held_raw_exposure():
    strategy = TefModel("BSCALPHA", INSTRUMENT)
    features = {
        "CLOSED_11": True,
        "CLOSED_5": True,
        "11_EMA_0_EMA": 100.0,
        "5_MMACD_MSDELTAABSMAREL": -2.0,
    }
    strategy.evaluate(candle(0), features)
    assert (
        strategy.evaluate(candle(1), {**features, "5_MMACD_MSDELTAABSMAREL": -1.0}).target_exposure
        == 1
    )
    # Legacy flips direction but holds positive raw exposure on this edge.
    signal = strategy.evaluate(candle(2, "90"), features)
    assert strategy.state["tefVal_prev"] == 1
    assert strategy.last_direction == "SHORT"
    assert signal.target_exposure == -1  # Matches direction + abs sizing in legacy account.


@pytest.mark.parametrize("model", MODELS)
def test_missing_features_do_not_emit_liquidation_or_invent_model_inputs(model):
    strategy = TefModel(model, INSTRUMENT)
    assert strategy.evaluate(candle(), {}) is None
    if model == "PATHFINDER":
        # The original initializes its independent caches before checking missing inputs.
        assert strategy.state["MST_last"] is None and strategy.state["STS_last"] is None
        assert strategy.state["tefVal_prev"] == 0
    else:
        assert strategy.state == {}


@pytest.mark.parametrize(
    "parameters",
    [
        {"alpha": 0},
        {"alpha": float("nan")},
        {"alpha": True},
        {"beta": 20},
        {"unknown": 1},
    ],
)
def test_bad_parameters_rejected(parameters):
    with pytest.raises(ValidationError):
        TefModel("MMACDDEFAULT", INSTRUMENT, parameters)


def test_unfinished_strategy_and_beta_zero_denominator_excluded():
    with pytest.raises(ValidationError):
        TefModel("BSCDELTA", INSTRUMENT)
    with pytest.raises(ValidationError, match="alpha must be positive"):
        TefModel("BSCBETA", INSTRUMENT, {"MTF_Alpha_Entry_S": 0})


def test_idempotency_conflict_gap_and_state_isolation():
    strategy, other = (
        TefModel("MMACDDEFAULT", INSTRUMENT),
        TefModel("MMACDDEFAULT", INSTRUMENT),
    )
    fields = {"0_MMACD_MSDELTAABSMAREL": 1.0}
    signal = strategy.evaluate(candle(), fields)
    before = strategy.state
    assert strategy.evaluate(candle(), fields) == signal
    for bar, values in [
        (candle(), {**fields, "extra": 1}),
        (candle(2), fields),
        (candle(1), {"0_MMACD_MSDELTAABSMAREL": float("nan")}),
    ]:
        with pytest.raises(ValidationError):
            strategy.evaluate(bar, values)
        assert strategy.state == before
    copy = strategy.state
    copy.clear()
    assert strategy.state == before
    assert other.state == {}


def test_spd_zero_swing_price_rejected_before_division():
    with pytest.raises(ValidationError, match="price reference"):
        TefModel("SPDDEFAULT", INSTRUMENT).evaluate(
            candle(),
            {
                "0_SWING_0_LSTYPE": 1,
                "0_SWING_0_LSPRICE": 0,
            },
        )
