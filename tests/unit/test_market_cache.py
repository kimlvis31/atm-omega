"""Switching charts must not retain unbounded market snapshots in the core."""

from threading import RLock
from types import SimpleNamespace

from omega.runtime import app


def test_chart_cache_has_capacity_and_discards_expired_snapshots(tmp_path, monkeypatch):
    now = [0.0]
    monkeypatch.setattr(app.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(app, "load_database_settings", lambda path: None)
    calls = []

    class Store:
        def __init__(self, settings):
            pass

        def latest(self, instrument, limit):
            calls.append((instrument, limit))
            return ()

        def close(self):
            pass

    monkeypatch.setattr(app, "PostgresCandleStore", Store)
    paper = SimpleNamespace(lock=RLock(), data={"candles": {}})
    view = app.MarketView(tmp_path, paper)
    for limit in range(1, 31):
        view("binance:usdm:BTCUSDT", limit)
    assert len(view.cache) == 12
    view("binance:usdm:BTCUSDT", 30)
    assert len(calls) == 30
    now[0] = 3
    view("binance:usdm:BTCUSDT", 1)
    assert len(view.cache) == 1 and len(calls) == 31
