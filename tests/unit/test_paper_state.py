"""Corrupt recovery state must fail before any module is reconstructed."""

from copy import deepcopy

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.application.paper_state import validate_state
from omega.domain.errors import ValidationError
from omega.strategies.registry import StrategyRegistry


@pytest.fixture
def state():
    service = PaperTradingService(StrategyRegistry().create, create_features)
    service.command(
        "paper.account.create", {"account_id": "a", "balance": "1000", "fee_rate": "0.001"}
    )
    service.command(
        "paper.strategy.add",
        {
            "instance": "s",
            "account_id": "a",
            "module_id": "target_quantity",
            "instrument": "binance:spot:BTCUSDT",
            "budget": "500",
            "configuration": {"quantity": "1"},
        },
    )
    service.command("resume", {})
    service.command(
        "paper.step",
        {
            "instrument": "binance:spot:BTCUSDT",
            "candle": {
                "open_time_ms": 0,
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100",
                "volume": "1",
            },
        },
    )
    return deepcopy(service.data)


def test_generated_state_validates_without_mutation(state):
    original = deepcopy(state)
    validate_state(state)
    assert state == original


@pytest.mark.parametrize(
    "mutate",
    [
        lambda data: data["accounts"]["a"].update(balance="NaN"),
        lambda data: data["accounts"]["a"].update(balance="-1"),
        lambda data: data["accounts"]["a"].update(fee_rate="1"),
        lambda data: data["accounts"]["a"].update(market="usdm"),
        lambda data: data["strategies"]["s"].update(quantity="-1", entry_price="100"),
        lambda data: data["strategies"]["s"].update(quantity="1", entry_price="0"),
        lambda data: data["strategies"]["s"].update(quantity="0", entry_price="100"),
        lambda data: data["strategies"]["s"].update(account_id="missing"),
        lambda data: data["strategies"]["s"].update(budget="-1"),
        lambda data: data["strategies"]["s"].update(revision=True),
        lambda data: data["strategies"]["s"].update(last_candle=1),
        lambda data: data["strategies"]["s"]["pending"].update(reserved="0"),
        lambda data: data["strategies"]["s"]["pending"].update(reducing=True, reserved="0"),
        lambda data: data["strategies"]["s"]["pending"].update(side="SELL"),
        lambda data: data["strategies"]["s"]["pending"].update(price="100"),
        lambda data: data["strategies"].update(duplicate=deepcopy(data["strategies"]["s"])),
        lambda data: data.update(paused="yes"),
        lambda data: data.update(schema_version=True),
        lambda data: data["accounts"]["a"].update(market=[]),
        lambda data: data["strategies"]["s"].update(configuration={"bad": float("inf")}),
    ],
)
def test_invalid_recovery_state_is_rejected(state, mutate):
    mutate(state)
    with pytest.raises(ValidationError):
        validate_state(state)


def test_history_limits_and_candle_contiguity_are_enforced(state):
    history = state["candles"]["binance:spot:BTCUSDT"]
    history.append({**history[0], "open_time_ms": 120_000})
    with pytest.raises(ValidationError):
        validate_state(state)
    state["candles"]["binance:spot:BTCUSDT"] = [history[0]] * 2001
    with pytest.raises(ValidationError):
        validate_state(state)


def test_unleveraged_gap_insolvency_is_preserved_for_usdm_monitoring(state):
    account = state["accounts"]["a"]
    account.update(balance="-10", market="usdm")
    row = state["strategies"]["s"]
    row.update(instrument="binance:usdm:BTCUSDT", pending=None, quantity="-1", entry_price="100")
    state["candles"] = {}
    validate_state(state)
