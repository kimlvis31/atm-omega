"""Legacy differentials and independent causal aggregation/warmup contracts."""

import json
from pathlib import Path

import pytest

from omega.analysis.aggregation import CandleAggregation
from omega.analysis.graph import FeatureGraph, FeatureNode
from omega.analysis.momentum import LegacyMomentumFeatures, MomentumConfiguration
from omega.analysis.moving_averages import LegacyMaFeatures, MaConfiguration
from omega.application.strategies import StrategyRunner
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext
from omega.strategies.tef.module import TefModule

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")
REFERENCE = json.loads(Path("tests/fixtures/momentum-legacy.json").read_text(encoding="utf-8"))


def bar(index, row=None):
    values = row or ["100", "100", "100", "100", "0"]
    return Candle(INSTRUMENT, index * 60000, (index + 1) * 60000, *(decimal(v) for v in values))


@pytest.mark.parametrize(
    "case", REFERENCE["cases"], ids=lambda c: f"{c['analysis_code']}-{c['interval_id']}"
)
def test_legacy_mfi_dmi_arithmetic_and_higher_interval_previews(case):
    feature = LegacyMomentumFeatures(
        INSTRUMENT,
        (MomentumConfiguration(case["interval_id"], case["analysis_code"], case["nSamples"]),),
    )
    prefix = f"{case['interval_id']}_{case['analysis_code']}_0_"
    for index, (row, expected) in enumerate(
        zip(REFERENCE["inputs"], case["expected"], strict=True)
    ):
        actual = feature.update(bar(index, row))
        for old, value in expected.items():
            suffix = old.replace("_", "")
            assert actual[prefix + suffix] == value, (index, old, actual, expected)


def test_ohlcv_aggregation_has_exact_volume_extremes_and_duplicate_safety():
    aggregate = CandleAggregation(INSTRUMENT, (1,))
    rows = [
        ["10", "13", "9", "12", "0.1"],
        ["12", "14", "10", "11", "0.2"],
        ["11", "15", "8", "14", "0.3"],
    ]
    for index, row in enumerate(rows):
        result = aggregate.update(bar(index, row))
    frame = result["BAR_1"]
    assert frame.complete and frame.valid_prefix
    assert (frame.open, frame.high, frame.low, frame.close, frame.volume) == tuple(
        decimal(v) for v in ["10", "15", "8", "14", "0.6"]
    )
    assert aggregate.update(bar(2, rows[2])) == result
    with pytest.raises(ValidationError, match="contiguous"):
        aggregate.update(bar(2))
    assert aggregate._buckets[1] == frame


def test_legacy_flat_zero_volume_semantics_and_bounded_history():
    feature = LegacyMomentumFeatures(
        INSTRUMENT, (MomentumConfiguration(0, "MFI", 2), MomentumConfiguration(0, "DMIxADX", 2))
    )
    values = [feature.update(bar(index)) for index in range(300)]
    assert values[1]["0_MFI_0_MFI"] is None
    assert values[2]["0_MFI_0_MFI"] == 1.0  # Preserve Eta compatibility, not standard redefinition.
    assert values[2]["0_MFI_0_MFIDEVABSMAREL"] is None
    assert values[3]["0_MFI_0_MFIDEVABSMAREL"] == 1.0
    assert values[3]["0_DMIxADX_0_DMIxADX"] == 0.0
    assert values[4]["0_DMIxADX_0_DMIxADXABSMAREL"] == 0.0
    assert all(len(kernel.history) == 2 for kernel in feature._kernels.values())


def test_incomplete_initial_bar_cannot_seed_history_and_preview_does_not_commit():
    feature = LegacyMomentumFeatures(INSTRUMENT, (MomentumConfiguration(2, "MFI", 2),))
    for index in range(1, 5):
        result = feature.update(bar(index))
        assert not result["CLOSED_2"]
        assert result["2_MFI_0_MFI"] is None
    assert next(iter(feature._kernels.values())).analysisCount == 0
    for index in range(5, 10):
        feature.update(bar(index))
    assert next(iter(feature._kernels.values())).analysisCount == 1
    result = feature.update(bar(10))
    assert feature.update(bar(10)) == result
    assert next(iter(feature._kernels.values())).analysisCount == 1
    with pytest.raises(ValidationError):
        feature.update(bar(12))
    feature.update(bar(11))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"interval_id": True, "analysis_code": "MFI"},
        {"interval_id": 0, "analysis_code": "OTHER"},
        {"interval_id": 0, "analysis_code": "MFI", "nSamples": True},
        {"interval_id": 0, "analysis_code": "MFI", "nSamples": 1},
        {"interval_id": 0, "analysis_code": "MFI", "line_index": -1},
    ],
)
def test_invalid_momentum_configuration(kwargs):
    with pytest.raises(ValidationError):
        MomentumConfiguration(**kwargs)


def test_bscbeta_uses_one_shared_aggregation_without_leaking_internal_frames():
    aggregate = CandleAggregation(INSTRUMENT, (2, 3, 5))
    graph = FeatureGraph(
        (
            FeatureNode("bars", aggregate, public_fields=aggregate.public_fields),
            FeatureNode(
                "ma",
                LegacyMaFeatures(
                    INSTRUMENT,
                    (
                        MaConfiguration(5, ema_period=3),
                        MaConfiguration(3, mmacd_periods=(2, 4), signal_period=2),
                    ),
                ),
                ("bars",),
            ),
            FeatureNode(
                "momentum",
                LegacyMomentumFeatures(
                    INSTRUMENT,
                    (MomentumConfiguration(2, "MFI", 3), MomentumConfiguration(2, "DMIxADX", 3)),
                ),
                ("bars",),
            ),
        )
    )
    runner = StrategyRunner(
        graph,
        {"bscbeta": TefModule(INSTRUMENT, {"model": "BSCBETA"})},
        lambda _name, candle, features: TradeContext(
            INSTRUMENT,
            candle.close,
            candle.close_time_ms,
            PositionView(),
            BudgetView(decimal("1000"), decimal("1000")),
            ExecutionConstraints(can_short=True),
            candle,
            features,
        ),
    )
    for index in range(360):
        runner.update(bar(index, REFERENCE["inputs"][index % len(REFERENCE["inputs"])]))
    assert "bscbeta" in runner.decisions and not runner.errors
    assert runner.engines["bscbeta"].module.model._last_candle.close_time_ms == 360 * 60000
    assert not any(key.startswith("BAR_") for key in graph._output)
    assert graph._nodes[0].feature._last.open_time_ms == 359 * 60000


def test_missing_internal_dependency_and_public_field_are_rejected():
    feature = LegacyMomentumFeatures(INSTRUMENT, (MomentumConfiguration(0, "MFI", 2),))
    with pytest.raises(ValidationError, match="aggregation dependency"):
        feature.update_with(bar(0), {})
    aggregate = CandleAggregation(INSTRUMENT, (0,))
    graph = FeatureGraph((FeatureNode("bars", aggregate, public_fields=("MISSING",)),))
    with pytest.raises(ValidationError, match="missing fields"):
        graph.update(bar(0))
    assert graph._last is None
