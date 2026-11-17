"""Signed derived features are distinct from positive TEF price references."""

from decimal import Decimal

import pytest

from omega.application.trading import TradingEngine
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


def path_fields():
    return {"CLOSED_5": True, "CLOSED_0": True, "5_MST_0_MST": 0.8, "0_STS_S": -0.3}


@pytest.mark.parametrize("model_name", ["MMACDDEFAULT", "PATHFINDER"])
def test_real_models_accept_signed_and_zero_mst_extras_without_changing_formula(model_name):
    original = TefModel(model_name, INSTRUMENT)
    with_extras = TefModel(model_name, INSTRUMENT)
    for index, strength in enumerate((0.0, -0.5, 0.25)):
        fields = path_fields() if model_name == "PATHFINDER" else {"0_MMACD_MSDELTAABSMAREL": 1.0}
        extra = {
            "0_MST_0_PRICESTR": strength,
            "5_MST_3_PRICESTR": strength,
            "PRICE_DREL": -0.25,
            "PRICE_STR": strength,
            "PRICE_DABSMA": 0.0,
            "5_BOL_2_BOLLOW": -1.0,
        }
        expected = original.evaluate(candle(index), fields)
        actual = with_extras.evaluate(candle(index), {**fields, **extra})
        assert actual == expected
        assert with_extras.state == original.state
        assert with_extras.last_direction == original.last_direction
        assert with_extras.last_open_time_ms == index * 60000
        assert with_extras.history_priming_ready == original.history_priming_ready
    # Signed extras remain part of exact input identity; accepting them is not dropping them.
    assert with_extras.checkpoint()["features_digest"] != original.checkpoint()["features_digest"]


def test_real_pathfinder_module_stays_healthy_across_zero_and_negative_mst_price_strength():
    engine = TradingEngine(TefModule(INSTRUMENT, {"model": "PATHFINDER"}))
    for index, strength in enumerate((0.0, -0.5, 0.25)):
        bar = candle(index)
        fields = {**path_fields(), "5_MST_0_PRICESTR": strength}
        decision = engine.evaluate(
            TradeContext(
                INSTRUMENT,
                bar.close,
                bar.close_time_ms,
                PositionView(),
                BudgetView(Decimal(100), Decimal(100)),
                ExecutionConstraints(),
                bar,
                fields,
                entries_allowed=False,
            )
        )
        assert not decision.orders
        assert decision.reason == "new_entries_paused"
        assert engine.module.snapshot()["model_failure"] is None
        assert engine.module.model.last_open_time_ms == index * 60000
        assert engine.module.history_priming_ready


@pytest.mark.parametrize(
    "key", ["0_SWING_0_LSPRICE", "7_SWING_3_LSPRICE", "11_EMA_0_EMA", "5_PSAR_2_PSAR"]
)
@pytest.mark.parametrize("value", [0.0, -1.0])
def test_true_reference_prices_remain_positive_and_invalid_updates_are_transactional(key, value):
    model = TefModel("MMACDDEFAULT", INSTRUMENT)
    fields = {"0_MMACD_MSDELTAABSMAREL": 1.0}
    model.evaluate(candle(0), fields)
    before = model.checkpoint()
    with pytest.raises(ValidationError, match="price reference must be positive"):
        model.evaluate(candle(1), {**fields, key: value})
    assert model.checkpoint() == before
    assert model.evaluate(candle(1), {**fields, key: 100.0}) is not None


def test_injected_close_reference_still_rejects_float_underflow_before_state_commit():
    model = TefModel("MMACDDEFAULT", INSTRUMENT)
    before = model.checkpoint()
    with pytest.raises(ValidationError, match="price reference must be positive"):
        model.evaluate(candle(0, "1E-1000"), {"0_MMACD_MSDELTAABSMAREL": 1.0})
    assert model.checkpoint() == before


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), float("-inf"), "0.5"])
def test_signed_strength_still_requires_finite_numeric_values_transactionally(value):
    model = TefModel("MMACDDEFAULT", INSTRUMENT)
    fields = {"0_MMACD_MSDELTAABSMAREL": 1.0}
    model.evaluate(candle(0), fields)
    before = model.checkpoint()
    with pytest.raises(ValidationError, match="finite numbers or unavailable"):
        model.evaluate(candle(1), {**fields, "5_MST_0_PRICESTR": value})
    assert model.checkpoint() == before


@pytest.mark.parametrize("value", [0, 1, None, 0.0, "false"])
def test_closed_flags_still_require_exact_bools_transactionally(value):
    model = TefModel("PATHFINDER", INSTRUMENT)
    model.evaluate(candle(0), path_fields())
    before = model.checkpoint()
    with pytest.raises(ValidationError, match="interval closure must be boolean"):
        model.evaluate(candle(1), {**path_fields(), "CLOSED_5": value})
    assert model.checkpoint() == before


def test_unavailable_optional_signed_metric_is_not_invented_as_a_zero_price():
    model = TefModel("MMACDDEFAULT", INSTRUMENT)
    signal = model.evaluate(candle(0), {"0_MMACD_MSDELTAABSMAREL": 1.0, "5_MST_0_PRICESTR": None})
    assert signal.target_exposure == Decimal("0.761594")
    assert model.history_priming_ready
