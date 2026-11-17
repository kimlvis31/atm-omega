"""OHLCV and auxiliary providers compose without exposing stale held numbers."""

import json
from dataclasses import replace
from decimal import Decimal

import pytest

from omega.analysis.aggregation import CandleAggregation
from omega.analysis.graph import FeatureNode
from omega.analysis.market_graph import MarketFeatureGraph
from omega.analysis.moving_averages import LegacyMaFeatures, MaConfiguration
from omega.analysis.order_flow import LegacyOrderFlowFeatures, OrderFlowConfiguration
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
NES = "0_NES_0_NES"


def frame(index, *, available=True):
    candle = Candle(
        INSTRUMENT,
        index * 60000,
        (index + 1) * 60000,
        *(Decimal(x) for x in ("10", "12", "8", "11", "4")),
    )
    inputs = (
        (
            AuxiliaryInput(
                "trade_flow",
                (
                    ("AGGTRADE_NOTIONALBUY", Decimal(100 + index)),
                    ("AGGTRADE_NOTIONALSELL", Decimal(10)),
                ),
                candle.open_time_ms,
                candle.close_time_ms,
                candle.close_time_ms,
                candle.close_time_ms,
                source="test_observation",
            ),
        )
        if available
        else ()
    )
    return MarketFeatureFrame(candle, candle.close_time_ms, inputs)


def graph():
    bars = CandleAggregation(INSTRUMENT, (0, 1))
    return MarketFeatureGraph(
        INSTRUMENT,
        (
            FeatureNode("bars", bars, public_fields=bars.public_fields),
            FeatureNode("ma", LegacyMaFeatures(INSTRUMENT, (MaConfiguration(0, 2),)), ("bars",)),
            FeatureNode(
                "nes",
                LegacyOrderFlowFeatures(
                    INSTRUMENT, (OrderFlowConfiguration(0, "NES", nSamples=2),)
                ),
                public_fields=(NES,),
            ),
        ),
    )


def test_shared_candle_features_and_auxiliary_values_keep_independent_readiness():
    current = graph()
    for index in range(4):
        result = current.update_frame(frame(index))
    assert current.input_ready and result[NES] is not None
    missing = current.update_frame(frame(4, available=False))
    assert missing[NES] is None
    assert missing["0_EMA_0_EMA"] is not None and missing["CLOSED_0"] is True
    provider = next(node.feature for node in current._nodes if node.name == "nes")
    assert provider._result[NES] == result[NES]  # Diagnostic hold only.
    assert not current.input_ready and not current.input_quality["nes"]["ready"]
    current.input_quality["nes"]["ready"] = True
    assert not current.input_quality["nes"]["ready"]
    assert current.update_frame(frame(5))[NES] is not None and current.input_ready


@pytest.mark.parametrize("split", [0, 1, 2, 5])
def test_json_checkpoint_roundtrip_preserves_partial_bars_and_quality_gate(split):
    original = graph()
    frames = [frame(i, available=i != 4) for i in range(10)]
    for value in frames[:split]:
        original.update_frame(value)
    restored = graph().restored(json.loads(json.dumps(original.checkpoint())), INSTRUMENT)
    assert restored.checkpoint() == original.checkpoint()
    for value in frames[split:]:
        assert restored.update_frame(value) == original.update_frame(value)
        assert restored.input_quality == original.input_quality
        restored.validate_checkpoint()


def test_duplicate_is_idempotent_changed_input_or_backward_availability_is_rejected():
    current = graph()
    value = frame(0)
    first = current.update_frame(value)
    assert current.update_frame(value) == first
    later = replace(value, as_of_ms=value.as_of_ms + 10)
    assert current.update_frame(later) == first
    before = current.checkpoint()
    with pytest.raises(ValidationError, match="backwards"):
        current.update_frame(value)
    with pytest.raises(ValidationError, match="immutable"):
        current.update_frame(replace(later, inputs=()))
    assert current.checkpoint() == before


def test_a_provider_failure_rolls_back_every_node_and_graph_cursor():
    current = graph()
    current.update_frame(frame(0))
    before = current.checkpoint()
    invalid = frame(1)
    invalid = replace(
        invalid,
        inputs=(
            replace(
                invalid.inputs[0],
                values=(
                    ("AGGTRADE_NOTIONALBUY", Decimal(-1)),
                    ("AGGTRADE_NOTIONALSELL", Decimal(0)),
                ),
            ),
        ),
    )
    with pytest.raises(ValidationError):
        current.update_frame(invalid)
    assert current.checkpoint() == before
    assert current.update_frame(frame(1))[NES] is not None


def test_candle_only_call_does_not_invent_an_auxiliary_channel():
    current = graph()
    for index in range(4):
        assert current.update(frame(index).candle)[NES] is None
    assert not current.input_ready
    assert current.descriptor.required_fields == frozenset({"OHLCV", "trade_flow"})


def test_quality_failure_propagates_through_dependent_nodes():
    class Derived:
        def update_with(self, candle, dependencies):
            value = dependencies["nes"][NES]
            return {"signal": None if value is None else value * 2}

    current = graph()
    current._nodes.append(FeatureNode("derived", Derived(), ("nes",)))
    for index in range(3):
        result = current.update_frame(frame(index))
        assert result["signal"] == (None if result[NES] is None else result[NES] * 2)
    missing = current.update_frame(frame(3, available=False))
    assert missing[NES] is None and missing["signal"] is None
    assert not current.input_quality["derived"]["ready"]
