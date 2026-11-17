"""Exchange-schema fixtures to exact minute flow and quality-gated graph; offline."""

import json
from dataclasses import replace
from decimal import Decimal

from omega.analysis.aggregation import CandleAggregation
from omega.analysis.checkpoint import checkpoint
from omega.analysis.graph import FeatureNode
from omega.analysis.market_graph import MarketFeatureGraph
from omega.analysis.market_sentiment import LegacyMarketSentimentFeatures, SentimentConfiguration
from omega.analysis.moving_averages import LegacyMaFeatures, MaConfiguration
from omega.analysis.order_flow import LegacyOrderFlowFeatures, OrderFlowConfiguration
from omega.application.trade_flow import TradeFlowAggregator, TradeFlowCoverage
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.binance.auxiliary import agg_trade

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def trade(identifier, timestamp, *, websocket=False):
    data = {
        "a": identifier,
        "f": identifier * 2,
        "l": identifier * 2 + 1,
        "p": "100.01",
        "q": ".03",
        "T": timestamp,
        "m": False,
        "nq": ".02",
    }
    if websocket:
        data.update(s="ETHUSDT", e="aggTrade", E=timestamp + 1)
    raw = json.dumps(data, separators=(",", ":")).encode()
    parsed = agg_trade(
        INSTRUMENT, raw, available_at_ms=timestamp + 2, source="websocket" if websocket else "rest"
    )
    assert parsed.raw == raw and parsed.non_rpi_quantity == Decimal(".02")
    return parsed


def candle(index):
    return Candle(
        INSTRUMENT,
        index * 60000,
        (index + 1) * 60000,
        *(Decimal(v) for v in ("100", "102", "98", "101", "4")),
    )


def test_raw_rest_ws_duplicates_to_flow_and_json_restored_nes_graph():
    parsed = [trade(i, timestamp) for i, timestamp in enumerate((59999, 60001, 120001, 180001))]
    events = [row.as_flow_event() for row in parsed]
    flow = TradeFlowAggregator(INSTRUMENT, 60000, source=events[0].source, generation=1)
    for event in events[1:]:
        assert flow.accept(event, generation=1)
    duplicate = trade(1, 60001, websocket=True)
    assert duplicate.source != parsed[1].source
    assert not flow.accept(duplicate.as_flow_event(), generation=1)
    assert duplicate.raw != parsed[1].raw  # Both source payloads can be kept unchanged.

    def graph():
        return MarketFeatureGraph(
            INSTRUMENT,
            (
                FeatureNode(
                    "nes",
                    LegacyOrderFlowFeatures(
                        INSTRUMENT, (OrderFlowConfiguration(0, "NES", nSamples=2),)
                    ),
                ),
            ),
        )

    current = graph()
    for index in (1, 2):
        minute = flow.finalize(
            index * 60000,
            as_of_ms=(index + 1) * 60000 + 3,
            generation=1,
            coverage=TradeFlowCoverage(events[index - 1], events[index + 1], 1),
        )
        result = current.update_frame(
            MarketFeatureFrame(candle(index), minute.available_at_ms, (minute,))
        )
        if index == 1:
            assert result["0_NES_0_NES"] is None
            current = graph().restored(json.loads(json.dumps(current.checkpoint())), INSTRUMENT)
    assert result["0_NES_0_NES"] == 3.0003 and current.input_ready


def test_mst_later_asof_expires_quality_without_advancing_candle_nodes_and_restores():
    def graph():
        bars = CandleAggregation(INSTRUMENT, (0,))
        return MarketFeatureGraph(
            INSTRUMENT,
            (
                FeatureNode("bars", bars, public_fields=bars.public_fields),
                FeatureNode(
                    "ma", LegacyMaFeatures(INSTRUMENT, (MaConfiguration(0, 2),)), ("bars",)
                ),
                FeatureNode(
                    "mst",
                    LegacyMarketSentimentFeatures(
                        INSTRUMENT, (SentimentConfiguration(0, nSamples=2),)
                    ),
                    public_fields=("0_MST_0_MST",),
                ),
            ),
        )

    current = graph()
    for index in range(5):
        bar = candle(index)
        shared = dict(
            window_start_ms=bar.open_time_ms,
            window_end_ms=bar.close_time_ms,
            event_time_ms=bar.close_time_ms,
            available_at_ms=bar.close_time_ms,
            source="fixture",
        )
        frame = MarketFeatureFrame(
            bar,
            bar.close_time_ms,
            (
                AuxiliaryInput(
                    "trade_flow",
                    (
                        ("AGGTRADE_NOTIONALBUY", Decimal(100)),
                        ("AGGTRADE_NOTIONALSELL", Decimal(10)),
                    ),
                    **shared,
                ),
                AuxiliaryInput(
                    "metric",
                    (("METRIC_OPENINTEREST", Decimal(1000)), ("METRIC_LONGSHORTRATIO", Decimal(1))),
                    **shared,
                ),
            ),
        )
        result = current.update_frame(frame)
    assert current.input_ready and result["0_MST_0_MST"] is not None
    ma_before = checkpoint(next(node.feature for node in current._nodes if node.name == "ma"))
    later = replace(frame, as_of_ms=frame.as_of_ms + 330001)
    expired = current.update_frame(later)
    assert expired["0_MST_0_MST"] is None and not current.input_ready
    assert expired["0_EMA_0_EMA"] == result["0_EMA_0_EMA"]
    assert (
        checkpoint(next(node.feature for node in current._nodes if node.name == "ma")) == ma_before
    )
    restored = graph().restored(json.loads(json.dumps(current.checkpoint())), INSTRUMENT)
    assert restored.update_frame(later) == expired
    assert restored.input_quality == current.input_quality
