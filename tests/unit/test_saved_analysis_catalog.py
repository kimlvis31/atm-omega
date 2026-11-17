"""Saved structured analysis is a real feature graph, not only a GUI preset."""

from decimal import Decimal

import pytest

from omega.analysis.catalog import EmptyFeatures, create_features, validate_configuration
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def candle(index):
    close = Decimal("10.123456789") + Decimal(index) / 7
    return Candle(
        INSTRUMENT,
        index * 60000,
        (index + 1) * 60000,
        close - 1,
        close + 2,
        close - 2,
        close,
        Decimal(100 + index),
    )


def test_legacy_configuration_still_matches_equivalent_saved_configuration():
    required = frozenset({"0_EMA_0_EMA", "0_MMACD_MSDELTAABSMAREL", "2_MFI_0_MFI"})
    legacy = create_features(
        INSTRUMENT,
        required,
        {"ema_period": 3, "mmacd_periods": [2, 4], "signal_period": 2, "momentum_samples": 3},
    )
    saved = create_features(
        INSTRUMENT,
        required,
        {
            "moving_averages": [
                {"interval_id": 0, "ema_period": 3, "mmacd_periods": [2, 4], "signal_period": 2}
            ],
            "momentum": [{"interval_id": 2, "analysis_code": "MFI", "nSamples": 3}],
        },
    )
    for index in range(50):
        assert saved.update(candle(index)) == legacy.update(candle(index))


def test_saved_line_index_and_price_precision_are_used_in_real_calculation():
    graph = create_features(
        INSTRUMENT,
        {"0_EMA_0_EMA", "0_MFI_2_MFI"},
        {
            "moving_averages": [{"interval_id": 0, "ema_period": 2, "price_precision": 2}],
            "momentum": [
                {"interval_id": 0, "analysis_code": "MFI", "line_index": 2, "nSamples": 2}
            ],
        },
    )
    for index in range(6):
        result = graph.update(candle(index))
    assert result["0_EMA_0_EMA"] == round(result["0_EMA_0_EMA"], 2)
    assert result["0_MFI_2_MFI"] is not None
    assert "0_MFI_0_MFI" not in result


def test_saved_configuration_checks_coverage_before_trading_can_start():
    with pytest.raises(ValidationError, match="does not provide"):
        create_features(
            INSTRUMENT,
            {"5_MMACD_MSDELTAABSMAREL"},
            {"moving_averages": [{"interval_id": 0, "ema_period": 20}]},
        )
    with pytest.raises(ValidationError, match="no installed"):
        create_features(INSTRUMENT, {"0_MFI_0_FAKE"}, {})


def test_empty_strategy_still_validates_saved_parameters_without_emitting_features():
    config = {"moving_averages": [{"interval_id": 0, "ema_period": 20}]}
    graph = create_features(INSTRUMENT, frozenset(), config)
    assert isinstance(graph, EmptyFeatures)
    assert graph.update(candle(0)) == {}
    with pytest.raises(ValidationError):
        create_features(
            INSTRUMENT, frozenset(), {"moving_averages": [{"interval_id": 13, "ema_period": 20}]}
        )
    with pytest.raises(ValidationError):
        create_features(INSTRUMENT, frozenset(), {"ema_period": True})


def test_configuration_canonicalizes_defaults_and_preserves_caller():
    config = {"moving_averages": [{"interval_id": 0, "mmacd_periods": [26, 12]}]}
    normalized = validate_configuration(config)
    assert normalized["moving_averages"][0]["mmacd_periods"] == [12, 26]
    assert normalized["moving_averages"][0]["price_precision"] == 8
    assert config["moving_averages"][0]["mmacd_periods"] == [26, 12]
