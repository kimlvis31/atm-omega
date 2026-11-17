"""Slow display DB requests leave core HTTP capacity available for control."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError
from omega.infrastructure.calendar_candles import CalendarCandleReader
from omega.infrastructure.postgres.config import DatabaseSettings
from omega.runtime import calendar_view
from omega.runtime.calendar_view import CalendarView

KEY = "binance:usdm:ETHUSDT"
END = 1709251200000


def test_display_connection_timeout_is_capped_without_mutating_collector_settings():
    for original, expected in ((1, 1), (5, 3), (60, 3)):
        settings = DatabaseSettings("127.0.0.1", 55433, "omega_test", "test", "secret", original)
        assert CalendarCandleReader(settings).settings.connect_timeout_s == expected
        assert settings.connect_timeout_s == original


def test_cache_age_starts_after_slow_query_completion(monkeypatch, tmp_path):
    clock, calls = [0.0], []
    monkeypatch.setattr(calendar_view, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(calendar_view, "load_database_settings", lambda _: None)

    class Reader:
        def __init__(self, _settings):
            pass

        def read(self, *args, **kwargs):
            calls.append(1)
            clock[0] += 5
            return {"candles": [1]}

    view = CalendarView(tmp_path, clock_ms=lambda: END, reader_factory=Reader)
    assert view(KEY, "1M", 1) == {"candles": [1]}
    clock[0] = 6.999
    assert view(KEY, "1M", 1) == {"candles": [1]}
    assert len(calls) == 1
    clock[0] = 7
    view(KEY, "1M", 1)
    assert len(calls) == 2


def test_duplicate_and_excess_queries_return_busy_without_waiting_for_database(
    monkeypatch, tmp_path
):
    entered = [Event(), Event()]
    release, lock, calls = Event(), Lock(), []
    monkeypatch.setattr(calendar_view, "load_database_settings", lambda _: None)

    class Reader:
        def __init__(self, _settings):
            pass

        def read(self, *args, **kwargs):
            with lock:
                index = len(calls)
                calls.append(args)
            entered[index].set()
            assert release.wait(4), "test must release blocked database"
            return {"candles": [index]}

    view = CalendarView(tmp_path, clock_ms=lambda: END, reader_factory=Reader)
    with ThreadPoolExecutor(2) as pool:
        first = pool.submit(view, KEY, "1M", 1)
        second = pool.submit(view, KEY, "1w", 1)
        try:
            assert all(event.wait(2) for event in entered)
            # These synchronous calls must complete while both DB reads remain blocked.
            assert view(KEY, "1M", 1)["quality"]["reason"] == "calendar_database_busy"
            assert view(KEY, "1M", 2)["quality"]["reason"] == "calendar_database_busy"
            assert len(calls) == 2
        finally:
            release.set()
        assert first.result() == {"candles": [0]}
        assert second.result() == {"candles": [1]}
    assert view.in_flight == set()
    assert view(KEY, "1M", 1) == {"candles": [0]}


@pytest.mark.parametrize("failure", [StorageError, RuntimeError])
def test_failure_always_releases_admission_slot_and_inflight_identity(
    monkeypatch, tmp_path, failure
):
    monkeypatch.setattr(calendar_view, "load_database_settings", lambda _: None)

    class Reader:
        def __init__(self, _settings):
            pass

        def read(self, *args, **kwargs):
            raise failure("test error")

    view = CalendarView(tmp_path, clock_ms=lambda: END, reader_factory=Reader)
    for period in ("1M", "1w"):
        if failure is StorageError:
            assert view(KEY, period, 1)["quality"]["reason"] == "calendar_database_unavailable"
        else:
            with pytest.raises(RuntimeError):
                view(KEY, period, 1)
        assert not view.in_flight
    assert view.query_slots.acquire(blocking=False)
    assert view.query_slots.acquire(blocking=False)
    assert not view.query_slots.acquire(blocking=False)
