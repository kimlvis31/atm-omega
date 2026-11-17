"""Keep the persisted market display cache bounded across a portfolio."""

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.domain.errors import StorageError
from omega.runtime.app import build_core
from omega.strategies.registry import StrategyRegistry


def test_global_history_budget_preserves_recent_causal_marks_and_duplicate_detection():
    service = PaperTradingService(StrategyRegistry().create, create_features)
    service.history_limit = 6

    def step(symbol, minute):
        return service.command(
            "paper.step",
            {
                "instrument": f"binance:spot:{symbol}",
                "candle": {
                    "open_time_ms": minute * 60000,
                    "open": "100",
                    "high": "101",
                    "low": "99",
                    "close": "100",
                    "volume": "1",
                },
            },
        )

    for minute in range(8):
        step("AAAUSDT", minute)
        step("BBBUSDT", minute)
        assert sum(len(r) for r in service.data["candles"].values()) <= 6
    assert all(
        len(rows) >= 2 and rows[-1]["open_time_ms"] == 7 * 60000
        for rows in service.data["candles"].values()
    )
    assert step("BBBUSDT", 7)["duplicate"]


def test_snapshot_capacity_cannot_publish_a_file_that_restart_will_refuse(tmp_path):
    core = build_core(tmp_path)
    path = tmp_path / "state" / "paper.json"
    previous = path.read_bytes()
    try:
        with pytest.raises(StorageError, match="capacity"):
            core.paper.persist({"oversized": "x" * 16_000_001})
        assert path.read_bytes() == previous
    finally:
        core.close()
