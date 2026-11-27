import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import closing
from threading import Event
from types import SimpleNamespace

import pytest

from omega.infrastructure.postgres.batching import COMMIT_BATCH_ROWS
from omega.runtime.collect_aux_live import LiveAuxiliary
from omega.runtime.collector_data_manager import DatabaseAuxiliarySink, DatabaseBarSink
from omega.runtime.collector_history import HistoryWorker
from omega.runtime.collector_write_queue import CollectorWriteQueue
from omega.runtime.market_history_fetch import MarketHistoryFetch
from omega.runtime.market_ipc import BackfillPaused


def test_stream_flushes_on_size_and_oldest_row_deadline():
    async def scenario():
        stop = asyncio.Event()
        collector = LiveAuxiliary(None, {})
        committed = asyncio.Queue()
        loop = asyncio.get_running_loop()

        async def save(batch, _stop):
            await committed.put((loop.time(), [row.window_start_ms for _, row in batch]))

        collector._commit = save
        draining = asyncio.create_task(collector._save_stream(stop))
        try:
            began = loop.time()
            for index in range(1_800):
                await collector.write(
                    "market", SimpleNamespace(channel="trade", source="test", window_start_ms=index)
                )
            head_at, head = await asyncio.wait_for(committed.get(), 0.9)
            tail_at, tail = await asyncio.wait_for(committed.get(), 2.0)
            assert head == list(range(1_440))
            assert tail == list(range(1_440, 1_800))
            assert head_at - began < 1.0 <= tail_at - began
            assert tail_at - began < 1.4
            await collector.pending.put(None)
            await draining
        finally:
            if not draining.done():
                draining.cancel()
                await asyncio.gather(draining, return_exceptions=True)
            collector.storage.shutdown()

    asyncio.run(scenario())


def test_ipc_sink_splits_overflow_at_transaction_boundary(monkeypatch):
    import omega.runtime.collector_data_manager as manager

    monkeypatch.setattr(manager, "auxiliary_document", lambda instrument, row: row)
    monkeypatch.setattr(manager, "minute_document", lambda row: row.candle)
    monkeypatch.setattr(manager, "candle_from_document", lambda row: row)
    bar_sink = DatabaseBarSink.__new__(DatabaseBarSink)
    aux_sink = DatabaseAuxiliarySink.__new__(DatabaseAuxiliarySink)

    def write(operation, *, rows, **_):
        sizes.append((operation, len(rows)))
        return rows if operation == "bars" else len(rows)

    sizes = []
    bar_sink.connection = aux_sink.connection = SimpleNamespace(call=write)
    bars = [SimpleNamespace(instrument="market", open_time_ms=index) for index in range(1_800)]
    assert bar_sink.put_batch([SimpleNamespace(candle=bar) for bar in bars]) == bars
    assert aux_sink.put_live_batch([("market", bar) for bar in bars]) == 1_800
    assert sizes == [("bars", 1_440), ("bars", 360), ("auxiliary", 1_440), ("auxiliary", 360)]


def test_ready_write_lanes_rotate_between_history_batches():
    with closing(CollectorWriteQueue()) as queue, ThreadPoolExecutor(max_workers=5) as owners:
        first_started, release = Event(), Event()
        committed = []

        def commit(value):
            committed.append(value)
            if value == "history-1":
                first_started.set()
                assert release.wait(3)
            return value

        first = owners.submit(queue.run, "history", commit, "history-1")
        try:
            assert first_started.wait(1)
            queued = [
                owners.submit(queue.run, lane, commit, name)
                for lane, name in (
                    ("history", "history-2"),
                    ("stream_bars", "bars"),
                    ("stream_auxiliary", "auxiliary"),
                    ("repair", "repair"),
                )
            ]
            with queue.changed:
                assert queue.changed.wait_for(lambda: queue.pending == 4, timeout=1)
        finally:
            release.set()
        assert first.result(timeout=1) == "history-1"
        for job in queued:
            job.result(timeout=1)
        assert committed == ["history-1", "bars", "auxiliary", "repair", "history-2"]


def test_backfill_reserves_pages_and_signals_peer_in_both_directions(monkeypatch):
    worker = HistoryWorker.__new__(HistoryWorker)
    worker.fetch = SimpleNamespace(set_backfill_paused=lambda paused: signals.append(paused))
    worker.workers, worker.stop, worker.throttled = 12, Event(), {}
    tasks = [(index, "depth", None) for index in range(19)]
    signals, committed = [], []
    buffered = 0

    def prepare(_db, _store, _pool, task):
        nonlocal buffered
        buffered += COMMIT_BATCH_ROWS
        assert buffered <= 12 * COMMIT_BATCH_ROWS
        done = Future()
        done.set_result(task)
        return ((), done)

    def persist(_db, _bars, _auxiliary, task, _prepared):
        nonlocal buffered
        committed.append(task)
        buffered -= COMMIT_BATCH_ROWS
        return True

    monkeypatch.setattr(worker, "_tasks", lambda: iter(tasks))
    monkeypatch.setattr(worker, "_prepare", prepare)
    monkeypatch.setattr(worker, "_persist", persist)
    monkeypatch.setattr(worker, "_repair_if_due", lambda *_: None)
    assert worker._fetch_round(None, None, None, None)
    assert committed == tasks and buffered == 0
    assert signals and signals == [True, False] * (len(signals) // 2)


def test_market_peer_pause_preserves_repair_and_can_resume(tmp_path, monkeypatch):
    collector = MarketHistoryFetch(tmp_path, (), Event(), workers=12)
    monkeypatch.setattr(collector, "_fetch", lambda operation, _: operation)
    assert collector.dispatch("backfill_pause", {"paused": True}) is True
    with pytest.raises(BackfillPaused):
        collector.dispatch("klines", {})
    with pytest.raises(BackfillPaused):
        collector.dispatch("aux_day", {})
    assert collector.dispatch("repair_trade", {}) == "repair_trade"
    assert collector.dispatch("backfill_pause", {"paused": False}) is False
    assert collector.dispatch("aux_day", {}) == "aux_day"
