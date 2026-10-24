import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from omega.analysis.graph import FeatureGraph, FeatureNode
from omega.analysis.moving_averages import (
    LegacyMaFeatures,
    MaConfiguration,
)
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market, decimal

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def bar(index, price):
    p = decimal(str(price))
    return Candle(INSTRUMENT, index * 60_000, (index + 1) * 60_000, p, p, p, p, decimal("1"))


def test_ema_independent_seed_rounding_recurrence_and_duplicate():
    f = LegacyMaFeatures(INSTRUMENT, (MaConfiguration(0, ema_period=3, price_precision=2),))
    assert f.update(bar(0, 10))["0_EMA_0_EMA"] is None
    assert f.update(bar(1, 12))["0_EMA_0_EMA"] is None
    assert f.update(bar(2, 14))["0_EMA_0_EMA"] == 12
    result = f.update(bar(3, 16))
    assert result["0_EMA_0_EMA"] == 14
    assert f.update(bar(3, 16)) == result
    assert f.update(bar(4, 18))["0_EMA_0_EMA"] == 16


def test_higher_interval_preview_not_committed_and_partial_start_not_full_bar():
    f = LegacyMaFeatures(INSTRUMENT, (MaConfiguration(1, ema_period=2),))
    results = [f.update(bar(n, n + 10)) for n in range(9)]
    assert [r["CLOSED_1"] for r in results] == [False, False, True] * 3
    assert results[2]["1_EMA_0_EMA"] is None
    assert results[3]["1_EMA_0_EMA"] == 12.5  # Previous close 12 + partial close 13.
    assert results[5]["1_EMA_0_EMA"] == 13.5  # Seed uses closes 12 and 15 only.
    assert results[8]["1_EMA_0_EMA"] == 16.5
    partial = LegacyMaFeatures(INSTRUMENT, (MaConfiguration(1, ema_period=2),))
    assert partial.update(bar(1, 11))["1_EMA_0_EMA"] is None
    assert not partial.update(bar(2, 12))["CLOSED_1"]
    with pytest.raises(ValidationError):
        partial.update(bar(4, 14))


def test_mmacd_constant_input_warmup_and_bounded_state():
    f = LegacyMaFeatures(INSTRUMENT, (MaConfiguration(0, mmacd_periods=(2, 4), signal_period=2),))
    results = [f.update(bar(n, 100)) for n in range(300)]
    assert all(r["0_MMACD_MSDELTAABSMAREL"] is None for r in results[:6])
    assert results[6]["0_MMACD_MSDELTAABSMAREL"] == 0
    assert results[-1]["0_MMACD_MSDELTA"] == 0
    assert len(f._kernels[0].closes) == 4


@pytest.mark.parametrize(
    "reference",
    json.loads(Path("tests/fixtures/analysis-legacy.json").read_text()),
    ids=lambda r: r["source"],
)
def test_legacy_arithmetic_sequence(reference):
    is_ema = reference["source"] == "moving_average_exponential.py"
    configuration = (
        MaConfiguration(0, ema_period=4, price_precision=4)
        if is_ema
        else MaConfiguration(
            0,
            mmacd_periods=(2, 4, 7),
            signal_period=3,
            price_precision=4,
        )
    )
    feature = LegacyMaFeatures(INSTRUMENT, (configuration,))
    for index, step in enumerate(reference["steps"]):
        actual = feature.update(bar(index, step["price"]))
        keys = (
            {"EMA": "0_EMA_0_EMA"}
            if is_ema
            else {
                "MSDELTA": "0_MMACD_MSDELTA",
                "MSDELTA_ABSMA": "0_MMACD_MSDELTAABSMA",
                "MSDELTA_ABSMAREL": "0_MMACD_MSDELTAABSMAREL",
            }
        )
        for old, new in keys.items():
            assert actual[new] == step["expected"][old]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"interval_id": 14, "ema_period": 3},
        {"interval_id": 0, "ema_period": True},
        {"interval_id": 0, "mmacd_periods": (2, 2)},
        {"interval_id": 0},
    ],
)
def test_invalid_kernel_configuration(kwargs):
    with pytest.raises(ValidationError):
        MaConfiguration(**kwargs)


@dataclass
class Counter:
    calls: int = 0

    def update(self, candle):
        self.calls += 1
        return {"base": self.calls}


@dataclass
class Dependent:
    fail: bool = False

    def update_with(self, candle, inputs):
        if self.fail:
            raise ValidationError("feature error")
        return {"derived": inputs["source"]["base"] * 2}


def test_graph_dependency_order_shared_cache_and_failure_atomicity():
    graph = FeatureGraph(
        (FeatureNode("dependent", Dependent(), ("source",)), FeatureNode("source", Counter()))
    )
    assert graph.update(bar(0, 10)) == {"base": 1, "derived": 2}
    assert graph.update(bar(0, 10)) == {"base": 1, "derived": 2}
    graph._nodes[1].feature.fail = True
    with pytest.raises(ValidationError):
        graph.update(bar(1, 11))
    assert graph._nodes[0].feature.calls == 1
    graph._nodes[1].feature.fail = False
    assert graph.update(bar(1, 11)) == {"base": 2, "derived": 4}


def test_graph_cycle_missing_dependency_and_output_collision():
    for nodes in [
        (FeatureNode("a", Counter(), ("b",)),),
        (FeatureNode("a", Counter(), ("b",)), FeatureNode("b", Counter(), ("a",))),
    ]:
        with pytest.raises(ValidationError):
            FeatureGraph(nodes)
    graph = FeatureGraph((FeatureNode("a", Counter()), FeatureNode("b", Counter())))
    with pytest.raises(ValidationError, match="collide"):
        graph.update(bar(0, 10))
    assert graph._nodes[0].feature.calls == 0
