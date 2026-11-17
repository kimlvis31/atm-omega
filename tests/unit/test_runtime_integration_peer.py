"""Focused restart/alert contract checks; synthetic paper only, no external I/O."""

from copy import deepcopy

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.runtime.paper_feed import PaperFeedSupervisor
from omega.strategies.registry import StrategyRegistry


class UnusedSource:
    def read(self, *args, **kwargs):
        raise AssertionError("restart health must not require a market query")


def restored_open_position(saved_health):
    paper = PaperTradingService(StrategyRegistry().create, create_features)
    paper.command("paper.account.create", {"account_id": "a", "balance": "1000"})
    paper.command(
        "paper.strategy.add",
        {
            "instance": "s",
            "account_id": "a",
            "instrument": "binance:spot:BTCUSDT",
            "module_id": "target_quantity",
            "budget": "500",
            "configuration": {"quantity": "1"},
        },
    )
    paper.command("resume", {})
    for minute in (0, 1):
        paper.command(
            "paper.step",
            {
                "instrument": "binance:spot:BTCUSDT",
                "candle": {
                    "open_time_ms": minute * 60_000,
                    "open": "100",
                    "high": "101",
                    "low": "99",
                    "close": "100",
                    "volume": "10",
                },
            },
        )
    state = deepcopy(paper.data)
    if not saved_health:
        state["strategies"]["s"].pop("execution_health", None)
    restored = PaperTradingService(
        StrategyRegistry().create,
        create_features,
        state=state,
        rules_provider=lambda *args, **kwargs: None,
    )
    assert restored.data["strategies"]["s"]["quantity"] == "1"
    assert restored.data["strategies"]["s"]["status"] == "recovery_required"
    return restored


@pytest.mark.parametrize(
    "saved_health", [False, True], ids=["legacy-no-health", "synthetic-health"]
)
def test_restart_open_position_exposes_unavailable_protection_before_new_market_input(saved_health):
    paper = restored_open_position(saved_health)
    before = deepcopy(paper.data)
    health = paper.execution_health()
    assert len(health["states"]) == 1
    assert health["states"][0]["state"] == "protection_unavailable"
    assert health["states"][0]["protection_blocked"] is True
    assert health["incidents"] == [
        {
            "account_id": "a",
            "instance": "s",
            "instrument": "binance:spot:BTCUSDT",
            "code": "POSITION_PROTECTION_UNAVAILABLE",
            "severity": "critical",
        }
    ]
    assert paper.data == before  # Health reporting cannot resume or rewrite the position.


@pytest.mark.parametrize(
    "saved_health", [False, True], ids=["legacy-no-health", "synthetic-health"]
)
def test_restart_open_position_delivers_critical_alert_even_with_feed_disabled(saved_health):
    paper = restored_open_position(saved_health)
    incidents = []
    feed = PaperFeedSupervisor(paper, UnusedSource(), on_execution_incident=incidents.append)
    feed.run_once()
    feed.run_once()
    assert len(incidents) == 1
    assert incidents[0]["code"] == "POSITION_PROTECTION_UNAVAILABLE"
    assert incidents[0]["severity"] == "critical"
