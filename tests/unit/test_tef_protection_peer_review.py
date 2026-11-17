"""Independent peer regressions for incomplete and unsubmitted local protection."""

from decimal import Decimal

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.application.trading import TradingEngine
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext
from omega.strategies.registry import StrategyRegistry
from omega.strategies.tef.module import TefModule

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def context(index, quantity, price, revision):
    value = Decimal(price)
    candle = Candle(
        INSTRUMENT, index * 60000, (index + 1) * 60000, value, value, value, value, Decimal(1)
    )
    return TradeContext(
        INSTRUMENT,
        value,
        candle.close_time_ms,
        PositionView(
            Decimal(quantity), Decimal(100) if Decimal(quantity) else Decimal(0), revision
        ),
        BudgetView(Decimal(100), Decimal(100)),
        ExecutionConstraints(can_short=True),
        candle,
        {"0_MMACD_MSDELTAABSMAREL": 1.0},
    )


def test_partial_local_stop_cannot_increase_risk_after_price_recovers():
    engine = TradingEngine(TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"}))
    first = engine.evaluate(context(0, "1", "90", 1))
    assert first.reason == "FSLIMMED" and first.orders[0].reducing
    partial = engine.evaluate(context(1, "0.5", "100", 2))
    assert all(order.reducing for order in partial.orders)


def test_unsubmitted_dust_protection_cannot_be_mistaken_for_a_confirmed_stop():
    engine = TradingEngine(TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"}))
    first = engine.evaluate(context(0, "0.0005", "90", 1))
    assert not first.orders and first.reason == "below_quantity_step"
    assert engine.module.snapshot()["pending_stop"] is None
    later = engine.evaluate(context(1, "0", "100", 2))
    assert later.reason != "stopped_after_local_stop"


def test_exact_recovery_cannot_claim_ready_when_durable_history_advanced_without_the_module():
    factory = StrategyRegistry().create
    paper = PaperTradingService(factory, create_features)
    paper.command("paper.account.create", {"account_id": "a", "balance": "10000"})
    paper.command(
        "paper.strategy.add",
        {
            "instance": "tef",
            "account_id": "a",
            "instrument": INSTRUMENT.key,
            "module_id": "tef",
            "budget": "1000",
            "analysis": {"mmacd_periods": [2, 4], "signal_period": 2},
        },
    )

    def step(service, index):
        price = str(100 + index % 3)
        return service.command(
            "paper.step",
            {
                "instrument": INSTRUMENT.key,
                "candle": {
                    "open_time_ms": index * 60000,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": "10",
                },
            },
        )

    for index in range(10):
        step(paper, index)
    paper.command("paper.order.cancel", {"instance": "tef"})
    restarted = PaperTradingService(factory, create_features, state=paper.data)
    step(restarted, 10)  # Allowed manual data input while strategy recovery is pending.
    with pytest.raises(ValidationError):
        restarted.command("paper.strategy.recover", {"instance": "tef"})
    assert restarted.detail("tef")["status"] == "recovery_required"
    assert restarted.snapshot()["paused"]
