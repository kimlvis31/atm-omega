"""Exact paper accounting, next-minute fills, recovery and shared-account limits."""

import json
from copy import deepcopy
from decimal import Decimal
from threading import Lock

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.application.paper_state import validate_state
from omega.domain.errors import ValidationError
from omega.ports.modules import ModuleDescriptor
from omega.strategies.registry import StrategyRegistry
from omega.strategies.target_quantity.module import TargetQuantityModule


class RoundTripModule(TargetQuantityModule):
    descriptor = ModuleDescriptor("round_trip", "1", frozenset(), True)

    def evaluate(self, context):
        if context.closed_candle.open_time_ms >= 120_000:
            self.target = Decimal(0)
        return super().evaluate(context)


def factory(identity, instrument, configuration):
    if identity == "round_trip":
        return RoundTripModule(instrument, configuration)
    return StrategyRegistry().create(identity, instrument, configuration)


def create_service(
    *,
    market="spot",
    quantity="1",
    balance="1000",
    fee="0.001",
    budget="500",
    module="target_quantity",
    persist=lambda _state: None,
):
    service = PaperTradingService(factory, create_features, persist=persist)
    service.command(
        "paper.account.create", {"account_id": "a", "balance": balance, "fee_rate": fee}
    )
    service.command(
        "paper.strategy.add",
        {
            "instance": "s",
            "account_id": "a",
            "instrument": f"binance:{market}:BTCUSDT",
            "module_id": module,
            "budget": budget,
            "configuration": {"quantity": quantity},
        },
    )
    service.command("resume", {})
    return service


def step(service, minute, price="100", *, instrument="binance:spot:BTCUSDT"):
    return service.command(
        "paper.step",
        {
            "instrument": instrument,
            "candle": {
                "open_time_ms": minute * 60_000,
                "open": price,
                "high": str(Decimal(price) + 1),
                "low": str(Decimal(price) - 1),
                "close": price,
                "volume": "10",
            },
        },
    )


def row(service):
    return service.data["strategies"]["s"]


def test_orders_fill_only_in_next_minute_and_pause_keeps_submitted_order():
    service = create_service()
    step(service, 0)
    assert row(service)["quantity"] == "0"
    assert row(service)["pending"]["eligible_at_ms"] == 60_000
    assert service.data["accounts"]["a"]["balance"] == "1000"
    service.command("pause", {})
    step(service, 1, "110")
    assert Decimal(row(service)["quantity"]) == 1
    assert Decimal(row(service)["entry_price"]) == 110
    assert Decimal(service.data["accounts"]["a"]["balance"]) == Decimal("889.890")
    assert row(service)["pending"] is None


@pytest.mark.parametrize(
    "market,quantity,prices,expected",
    [
        ("spot", "1", ["100", "110", "120", "130"], "1019.760"),
        ("usdm", "-1", ["100", "110", "100", "90"], "1019.800"),
    ],
)
def test_round_trip_cash_and_linear_short_quote_fees_are_exact(market, quantity, prices, expected):
    service = create_service(market=market, quantity=quantity, module="round_trip")
    for minute, price in enumerate(prices):
        step(service, minute, price, instrument=f"binance:{market}:BTCUSDT")
    assert Decimal(row(service)["quantity"]) == 0
    assert Decimal(row(service)["entry_price"]) == 0
    assert len(row(service)["fills"]) == 2
    assert Decimal(service.data["accounts"]["a"]["balance"]) == Decimal(expected)
    validate_state(service.data)


def test_same_minute_replay_does_not_refill_and_conflict_gap_roll_back():
    service = create_service()
    step(service, 0)
    step(service, 1)
    before = deepcopy(service.data)
    assert step(service, 1)["duplicate"]
    assert service.data == before
    for minute, price in ((1, "101"), (3, "100")):
        with pytest.raises(ValidationError):
            step(service, minute, price)
        assert row(service)["fills"] == before["strategies"]["s"]["fills"]
        assert service.data["candles"] == before["candles"]
        assert service.data["paused"]


def test_persistence_failure_rolls_back_fill_balance_and_analysis_state():
    history = []
    service = create_service(persist=lambda state: history.append(deepcopy(state)))
    step(service, 0)
    before = deepcopy(service.data)

    def fail(_state):
        raise OSError("disk full")

    service.persist = fail
    with pytest.raises(OSError):
        step(service, 1)
    assert row(service)["quantity"] == "0"
    assert row(service)["pending"] == before["strategies"]["s"]["pending"]
    assert service.data["accounts"] == before["accounts"]
    assert service.data["candles"] == before["candles"]
    assert service.data["paused"]
    assert history[-1] == before


def test_restart_requires_explicit_strategy_recovery_and_resume():
    service = create_service()
    step(service, 0)
    step(service, 1)
    restarted = PaperTradingService(factory, create_features, state=service.data)
    assert restarted.data["paused"]
    assert row(restarted)["status"] == "recovery_required"
    step(restarted, 2)
    assert row(restarted)["status"] == "recovery_required"
    restarted.command("paper.strategy.recover", {"instance": "s"})
    assert row(restarted)["status"] == "ready"
    assert restarted.data["paused"]
    restarted.command("resume", {})
    step(restarted, 3)
    assert Decimal(row(restarted)["quantity"]) == 1


def test_pending_restart_requires_explicit_cancel_before_strategy_recovery():
    service = create_service()
    step(service, 0)
    restarted = PaperTradingService(factory, create_features, state=service.data)
    with pytest.raises(ValidationError, match="pending"):
        restarted.command("paper.strategy.recover", {"instance": "s"})
    restarted.command("paper.order.cancel", {"instance": "s"})
    restarted.command("paper.strategy.recover", {"instance": "s"})
    assert row(restarted)["pending"] is None


def test_gap_fill_cannot_exceed_allocated_strategy_budget():
    service = create_service(budget="100", fee="0")
    step(service, 0)
    service.command("pause", {})
    step(service, 1, "200")
    assert row(service)["quantity"] == "0"
    assert row(service)["pending"] is None
    assert row(service)["fills"] == []
    assert service.data["accounts"]["a"]["balance"] == "1000"


def test_one_strategy_owner_per_account_instrument_and_market_partition():
    service = create_service()
    base = {
        "instance": "other",
        "account_id": "a",
        "module_id": "target_quantity",
        "budget": "100",
        "configuration": {"quantity": "1"},
    }
    with pytest.raises(ValidationError, match="owner"):
        service.command("paper.strategy.add", {**base, "instrument": "binance:spot:BTCUSDT"})
    with pytest.raises(ValidationError, match="mix"):
        service.command("paper.strategy.add", {**base, "instrument": "binance:usdm:ETHUSDT"})
    assert set(service.data["strategies"]) == {"s"}


def test_ten_accounts_and_one_hundred_strategy_configuration_limit():
    service = PaperTradingService(factory, create_features)
    for index in range(10):
        service.command(
            "paper.account.create", {"account_id": f"a{index}", "balance": "10000", "fee_rate": "0"}
        )
    with pytest.raises(ValidationError):
        service.command("paper.account.create", {"account_id": "overflow"})
    for index in range(100):
        service.command(
            "paper.strategy.add",
            {
                "instance": f"s{index}",
                "account_id": f"a{index // 10}",
                "instrument": f"binance:spot:COIN{index}USDT",
                "module_id": "target_quantity",
                "budget": "100",
                "configuration": {"quantity": "0"},
            },
        )
    assert len(service.snapshot()["strategies"]) == 100
    validate_state(service.data)
    summary = service.snapshot()
    assert all(
        "fills" not in strategy and "configuration" not in strategy and "analysis" not in strategy
        for strategy in summary["strategies"]
    )
    assert len(json.dumps(summary).encode("utf-8")) < 200_000
    with pytest.raises(ValidationError):
        service.command(
            "paper.strategy.add",
            {
                "instance": "overflow",
                "account_id": "a0",
                "instrument": "binance:spot:OVERFLOWUSDT",
                "module_id": "target_quantity",
                "budget": "1",
                "configuration": {"quantity": "0"},
            },
        )


def test_unsupported_analysis_provider_rolls_back_the_strategy_addition():
    service = PaperTradingService(factory, create_features)
    service.command("paper.account.create", {"account_id": "a", "balance": "1000"})
    with pytest.raises(ValidationError):
        service.command(
            "paper.strategy.add",
            {
                "instance": "bad",
                "account_id": "a",
                "instrument": "binance:usdm:BTCUSDT",
                "module_id": "tef",
                "budget": "100",
                "configuration": {"model": "CGALPHA"},
            },
        )
    assert not service.data["strategies"]
    assert service.data["accounts"]["a"]["market"] is None


def test_global_chronology_blocks_using_another_instrument_future_mark():
    service = create_service()
    step(service, 5)
    before = deepcopy(service.data)
    with pytest.raises(ValidationError, match="global chronological"):
        step(service, 4, instrument="binance:spot:ETHUSDT")
    assert service.data["candles"] == before["candles"]


def test_invalid_persisted_position_is_rejected_before_module_construction():
    service = create_service()
    corrupted = deepcopy(service.data)
    corrupted["strategies"]["s"].update(quantity="-1", entry_price="100")
    calls = []
    with pytest.raises(ValidationError):
        PaperTradingService(lambda *args: calls.append(args), create_features, state=corrupted)
    assert calls == []


def test_uncopyable_module_is_rejected_on_add_and_emergency_pause_bypasses_copy():
    def uncopyable(identity, instrument, configuration):
        module = factory(identity, instrument, configuration)
        module.lock = Lock()
        return module

    service = PaperTradingService(uncopyable, create_features)
    service.command("paper.account.create", {"account_id": "a"})
    with pytest.raises(TypeError):
        service.command(
            "paper.strategy.add",
            {
                "instance": "bad",
                "account_id": "a",
                "instrument": "binance:spot:BTCUSDT",
                "module_id": "target_quantity",
                "budget": "100",
                "configuration": {"quantity": "1"},
            },
        )
    assert service.data["strategies"] == {}
    healthy = create_service()
    healthy.engines["s"].module.lock = Lock()
    assert healthy.command("pause", {}) == {"paused": True}
    assert healthy.data["paused"]


def test_individual_ts_resume_clears_local_stop_without_resetting_model_state():
    service = PaperTradingService(factory, create_features)
    service.command("paper.account.create", {"account_id": "a"})
    service.command(
        "paper.strategy.add",
        {
            "instance": "s",
            "account_id": "a",
            "instrument": "binance:usdm:BTCUSDT",
            "module_id": "tef",
            "budget": "100",
            "configuration": {"model": "MMACDDEFAULT"},
        },
    )
    service.engines["s"].module._halted = True
    service.engines["s"].module.model._tracker["test_marker"] = 7
    service.command("resume", {})
    assert service.engines["s"].module.snapshot()["halted"]
    service.command("paper.strategy.resume", {"instance": "s"})
    assert not service.engines["s"].module.snapshot()["halted"]
    assert service.engines["s"].module.model.state["test_marker"] == 7
