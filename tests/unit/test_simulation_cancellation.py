"""The replay executor acknowledges cancellation before a TS can replace it."""

from decimal import Decimal as D

import pytest

from omega.domain.models import Instrument, Market, OrderKind, Side
from omega.domain.trading import OrderProposal, TradeDecision
from omega.ports.modules import ModuleDescriptor
from omega.research.datasets import MarketRow
from omega.research.simulation import CancelPendingRequest, SimulationSpec, simulate
from omega.research.strategy_binding import TradeStrategyBinding, build_features

KEY = "binance:usdm:BTCUSDT"


class ProtectiveModule:
    descriptor = ModuleDescriptor("protective_probe", "1", frozenset(), True)

    def evaluate(self, context):
        minute = context.closed_candle.open_time_ms // 60000
        if minute == 0:
            return TradeDecision((OrderProposal(Side.BUY, D(1)),), "ENTRY")
        if minute == 1:
            return TradeDecision((OrderProposal(Side.BUY, D(1), OrderKind.LIMIT, D(50)),), "ENTRY")
        if minute == 2:
            if context.has_pending_order:
                return TradeDecision(reason="LOCAL_STOP", cancel_pending=True)
            return TradeDecision((OrderProposal(Side.SELL, D(1), reducing=True),), "LOCAL_STOP")
        return TradeDecision()


def rows():
    return [MarketRow(KEY, i * 60000, D(100), D(101), D(99), D(100), D(10)) for i in range(4)]


def test_replay_cancel_ack_then_local_reduction_keeps_causal_next_minute_fill():
    binding = TradeStrategyBinding(lambda instrument, configuration: ProtectiveModule())
    result = simulate(rows(), binding, SimulationSpec(fee_rate=D(0)))
    assert [fill["quantity"] for fill in result["fills"]] == ["1", "-1"]
    assert result["fills"][-1]["tag"] == "strategy"
    assert D(result["fills"][-1]["timestamp_ms"]) == 180000
    assert result["positions"][KEY] == "0"
    assert result["pending_orders"] == 0
    assert [event["count"] for event in result["events"] if event["kind"] == "orders_canceled"] == [
        1
    ]


def test_unknown_or_nonexistent_cancellation_cannot_be_acknowledged():
    for key in (KEY, "unknown"):
        with pytest.raises(ValueError):
            simulate(rows(), lambda view, key=key: [CancelPendingRequest(key)], SimulationSpec())


def test_research_and_paper_use_the_same_swing_provider():
    graph = build_features(
        Instrument("binance", Market.USDM, "BTCUSDT"),
        {"swing": [{"interval_id": 0, "swing_range": 0.01}]},
    )
    assert "legacy.swing" in repr(graph._nodes[-1].feature.descriptor)
