"""Nullable result contracts distinguish source quality, seeding and no detection."""

import json
from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from omega.analysis.graph import MAX_NULLABLE_FIELDS, FeatureGraph, FeatureNode
from omega.analysis.market_graph import MarketFeatureGraph
from omega.analysis.order_flow import LegacyOrderFlowFeatures, OrderFlowConfiguration
from omega.analysis.volume_profile import LegacyVolumeProfileFeatures, ProfileConfiguration
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def candle(index, volume=0, price="100"):
    return Candle(
        INSTRUMENT,
        index * 60000,
        (index + 1) * 60000,
        *(Decimal(price) for _ in range(4)),
        Decimal(volume),
    )


def ivp_graph(*, market=False, nSamples=2, gammaFactor=0.01):
    node = FeatureNode(
        "ivp",
        LegacyVolumeProfileFeatures(
            INSTRUMENT,
            (
                ProfileConfiguration(
                    0, nSamples=nSamples, gammaFactor=gammaFactor, price_precision=2
                ),
            ),
        ),
    )
    return MarketFeatureGraph(INSTRUMENT, (node,)) if market else FeatureGraph((node,))


class FakeFeature:
    def __init__(self, nullable=frozenset(), ready=True):
        self.nullable_fields, self.calculation_ready = nullable, ready
        self.count = 0

    def update(self, candle):
        self.count += 1
        return {"visible": None, "private": None, "nested": {"counter": [self.count]}}


def test_empty_graph_has_no_calculation_cursor_or_public_outputs():
    graph = FeatureGraph((FeatureNode("feature", FakeFeature()),))
    assert graph.last_candle is None and graph.output_values == {}
    assert graph.nullable_fields == frozenset() and not graph.calculation_ready


def test_nullable_metadata_is_scoped_to_the_nodes_published_fields():
    feature = FakeFeature(frozenset({"visible", "private"}))
    graph = FeatureGraph((FeatureNode("feature", feature, public_fields=("visible",)),))
    assert graph.nullable_fields == frozenset({"visible"})
    graph.update(candle(0))
    assert graph.output_values == {"visible": None}
    assert graph.calculation_ready  # Engine still checks nonnullable None separately.


def test_default_nullable_contract_does_not_make_unannounced_none_optional():
    feature = FakeFeature()
    del feature.nullable_fields
    del feature.calculation_ready
    graph = FeatureGraph((FeatureNode("feature", feature, public_fields=("visible",)),))
    graph.update(candle(0))
    assert graph.nullable_fields == frozenset()
    assert graph.output_values["visible"] is None
    assert graph.calculation_ready


def test_last_candle_is_immutable_and_output_snapshot_is_deeply_independent():
    graph = FeatureGraph((FeatureNode("feature", FakeFeature()),))
    value = candle(0)
    graph.update(value)
    assert graph.last_candle is value
    with pytest.raises(FrozenInstanceError):
        graph.last_candle.volume = Decimal(3)
    output = graph.output_values
    output["nested"]["counter"].append(999)
    assert graph.output_values["nested"]["counter"] == [1]


@pytest.mark.parametrize(
    "metadata",
    [
        set(),
        [],
        None,
        {"visible": True},
        frozenset({True}),
        frozenset({""}),
        frozenset({" bad"}),
        frozenset({"x\n"}),
        frozenset({"x" * 129}),
        frozenset(f"field{i}" for i in range(MAX_NULLABLE_FIELDS + 1)),
    ],
)
def test_invalid_nullable_contract_is_rejected_even_when_unpublished(metadata):
    with pytest.raises(ValidationError):
        FeatureGraph((FeatureNode("feature", FakeFeature(metadata), public_fields=("visible",)),))


@pytest.mark.parametrize("metadata", [None, 0, 1, "false", []])
def test_calculation_ready_is_explicitly_boolean(metadata):
    with pytest.raises(ValidationError):
        FeatureGraph((FeatureNode("feature", FakeFeature(ready=metadata)),))


def test_nullable_union_has_its_own_global_bound():
    left = FakeFeature(frozenset(f"left{i}" for i in range(600)))
    right = FakeFeature(frozenset(f"right{i}" for i in range(600)))
    with pytest.raises(ValidationError):
        FeatureGraph((FeatureNode("left", left), FeatureNode("right", right)))


def test_dynamic_invalid_metadata_fails_before_transaction_commit():
    class ChangingFeature(FakeFeature):
        def update(self, candle):
            value = super().update(candle)
            self.nullable_fields = {"visible"}
            return value

    graph = FeatureGraph((FeatureNode("feature", ChangingFeature()),))
    with pytest.raises(ValidationError):
        graph.update(candle(0))
    assert graph.last_candle is None and graph.output_values == {}
    assert graph._nodes[0].feature.count == 0


def test_false_first_node_cannot_hide_invalid_second_node_readiness():
    graph = FeatureGraph(
        (
            FeatureNode("left", FakeFeature(ready=False), public_fields=("visible",)),
            FeatureNode("right", FakeFeature(), public_fields=("private",)),
        )
    )
    graph._nodes[1].feature.calculation_ready = "true"
    with pytest.raises(ValidationError):
        _ = graph.calculation_ready


@pytest.mark.parametrize("market", [False, True])
def test_zero_volume_seeded_profile_is_ready_with_normal_nullable_none(market):
    pytest.importorskip("scipy")
    graph = ivp_graph(market=market)
    assert not graph.calculation_ready
    first = graph.update(candle(0))
    assert all(value is None for field, value in first.items() if "IVP_NB" in field)
    assert not graph.calculation_ready
    assert graph._nodes[0].feature.input_ready
    second = graph.update(candle(1))
    assert graph.calculation_ready and len(graph.nullable_fields) == 10
    assert all(second[field] is None for field in graph.nullable_fields)


def test_nonzero_endpoint_profile_with_no_detected_peaks_is_normally_ready():
    pytest.importorskip("scipy")
    graph = ivp_graph(gammaFactor=0.1)
    graph.update(candle(0, volume=1, price="199.99"))
    result = graph.update(candle(1, volume=1, price="199.99"))
    feature = graph._nodes[0].feature
    assert sum(feature._raw[0]["volumePriceLevelProfile"]) > 0
    assert feature._raw[0]["volumePriceLevelProfile_Boundaries"] == []
    assert graph.calculation_ready
    assert all(result[field] is None for field in graph.nullable_fields)


def test_seeded_ivp_cannot_make_a_missing_auxiliary_node_ready():
    pytest.importorskip("scipy")
    ivp = LegacyVolumeProfileFeatures(INSTRUMENT, (ProfileConfiguration(0, nSamples=2),))
    woi = LegacyOrderFlowFeatures(INSTRUMENT, (OrderFlowConfiguration(0, "WOI", 2),))
    graph = MarketFeatureGraph(
        INSTRUMENT,
        (
            FeatureNode("ivp", ivp, public_fields=tuple(ivp.nullable_fields)),
            FeatureNode("woi", woi, public_fields=("0_WOI_0_WOI",)),
        ),
    )
    for index in range(3):
        bar = candle(index)
        graph.update_frame(MarketFeatureFrame(bar, bar.close_time_ms))
    assert graph._nodes[0].feature.calculation_ready
    assert not graph.input_ready and not graph.calculation_ready
    assert "0_WOI_0_WOI" not in graph.nullable_fields
    assert graph.output_values["0_WOI_0_WOI"] is None


@pytest.mark.parametrize("market", [False, True])
def test_ready_contract_and_public_restore_access_do_not_change_checkpoint_schema(market):
    pytest.importorskip("scipy")
    graph = ivp_graph(market=market)
    for index in range(3):
        graph.update(candle(index))
    saved = graph.checkpoint()
    expected = {"_nodes", "_last", "_output"} | (
        {"_last_frame", "_node_outputs"} if market else set()
    )
    assert set(saved["payload"]["state"]) == expected
    assert not any(
        name in saved["payload"]["fixed"]
        for name in ("nullable_fields", "calculation_ready", "last_candle", "output_values")
    )
    restored = ivp_graph(market=market).restored(json.loads(json.dumps(saved)), INSTRUMENT)
    assert restored.last_candle == graph.last_candle
    assert restored.output_values == graph.output_values
    assert restored.nullable_fields == graph.nullable_fields
    assert restored.calculation_ready is graph.calculation_ready is True
    assert (
        restored.checkpoint() == saved
    )  # Reading these properties adds no state or recomputation.
