import asyncio

import pytest

from omega.analysis.graph import FeatureGraph, FeatureNode
from omega.analysis.moving_averages import (
    LegacyMaFeatures,
    MaConfiguration,
)
from omega.application.collection import MinuteCollector
from omega.application.strategies import StrategyRunner
from omega.domain.bar_schedule import BarSchedule
from omega.domain.errors import ProtocolError, RateLimited, ValidationError
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.domain.safety import BlockReason, EntryGate
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext
from omega.infrastructure.binance.streams import StreamPlan
from omega.ports.collection import MinuteObservation
from omega.runtime.collection import CollectionWorker
from omega.strategies.tef.module import TefModule

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def bar(n, price="100"):
    p = decimal(price)
    return Candle(INSTRUMENT, n * 60_000, (n + 1) * 60_000, p, p, p, p, decimal("1"))


def obs(n, price="100"):
    return MinuteObservation(bar(n, price), "websocket", 600_000)


class MemoryCandleStore:
    """A collection-service test double; real durability is tested against PostgreSQL."""

    def __init__(self):
        self.rows = {}

    def get(self, instrument, open_time_ms):
        return self.rows.get((instrument, open_time_ms))

    def put(self, observation):
        candle = observation.candle
        key = candle.instrument, candle.open_time_ms
        prior = self.rows.get(key)
        if prior is not None:
            if prior != candle:
                raise ProtocolError("conflicting closed minute")
            return False
        self.rows[key] = candle
        return True


@pytest.fixture
def store():
    return MemoryCandleStore()


def test_reorder_dedup_gap_and_conflict_retain_original(store):
    received = []
    collector = MinuteCollector({INSTRUMENT: 0}, store, received.append)
    collector.connected = True
    assert collector.accept(obs(2), now_ms=600_000) == 0
    assert not collector.ready(INSTRUMENT, 180_000)
    assert collector.accept(obs(0), now_ms=600_000) == 1
    assert collector.accept(obs(0), now_ms=600_000) == 0
    assert collector.accept(obs(1), now_ms=600_000) == 2
    assert received == [bar(0), bar(1), bar(2)]
    assert collector.ready(INSTRUMENT, 180_000)
    with pytest.raises(ProtocolError, match="conflicting"):
        collector.accept(obs(1, "99"), now_ms=600_000)
    assert store.get(INSTRUMENT, 60_000) == bar(1)
    assert not collector.ready(INSTRUMENT, 180_000)


def test_storage_commit_precedes_delivery_and_failures_keep_cursor(store):
    def failed(candle):
        assert store.get(INSTRUMENT, candle.open_time_ms) == candle
        raise RuntimeError("downstream failed")

    collector = MinuteCollector({INSTRUMENT: 0}, store, failed)
    with pytest.raises(RuntimeError):
        collector.accept(obs(0), now_ms=600_000)
    assert collector.cursor[INSTRUMENT] == 0
    collector.on_candle = lambda c: None
    assert collector.accept(obs(0), now_ms=600_000) == 1


def test_reorder_bound_and_open_minute(store):
    collector = MinuteCollector({INSTRUMENT: 0}, store, lambda c: None, max_pending=1)
    collector.accept(obs(1), now_ms=600_000)
    with pytest.raises(ProtocolError, match="capacity"):
        collector.accept(obs(2), now_ms=600_000)
    with pytest.raises(ProtocolError, match="future"):
        collector.accept(obs(0), now_ms=30_000)


class History:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    def closed_minutes(self, instrument, start_ms, end_ms, *, now_ms):
        self.calls.append((start_ms, end_ms))
        return tuple(c for c in self.rows if start_ms <= c.open_time_ms < end_ms)


def test_gap_repair_auto_data_recovery_preserves_manual_pause(store):
    gate = EntryGate()
    gate.recovered(BlockReason.RESTART)
    gate.resume(reconciled=True, durable=True)
    gate.block(BlockReason.PAUSE)

    def health(ready, reason):
        gate.recovered(BlockReason.DATA) if ready else gate.block(BlockReason.DATA)

    received = []
    collector = MinuteCollector({INSTRUMENT: 0}, store, received.append)
    collector.connected = True
    history = History([bar(0), bar(2)])
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)),
        history,
        collector,
        clock_ms=lambda: 180_002,
        on_health=health,
    )
    asyncio.run(worker.repair())
    worker.health("gap")
    assert received == [bar(0)] and BlockReason.DATA in gate.reasons
    history.rows.append(bar(1))
    asyncio.run(worker.repair())
    worker.health("gap")
    assert received == [bar(0), bar(1), bar(2)]
    assert gate.reasons == {BlockReason.PAUSE}


def test_no_data_no_spin_and_strategy_pipeline_shared_for_100_instances(store):
    graph = FeatureGraph(
        (
            FeatureNode(
                "ma",
                LegacyMaFeatures(
                    INSTRUMENT, (MaConfiguration(0, mmacd_periods=(2, 4), signal_period=2),)
                ),
            ),
        )
    )
    runner = StrategyRunner(
        graph,
        {str(n): TefModule(INSTRUMENT) for n in range(100)},
        lambda _name, candle, features: TradeContext(
            INSTRUMENT,
            candle.close,
            candle.close_time_ms,
            PositionView(),
            BudgetView(decimal("1000"), decimal("1000")),
            ExecutionConstraints(can_short=True),
            candle,
            features,
        ),
    )
    collector = MinuteCollector({INSTRUMENT: 0}, store, runner.update)
    history = History([bar(n, str(100 + (n % 7))) for n in range(100)])
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)), history, collector, clock_ms=lambda: 6_000_002
    )
    asyncio.run(worker.repair())
    assert collector.cursor[INSTRUMENT] == 6_000_000
    assert len(runner.decisions) == 100 and not runner.errors
    assert len(set(runner.decisions.values())) == 1
    assert all(
        e.module.model._last_candle.close_time_ms == 6_000_000 for e in runner.engines.values()
    )
    history.rows.clear()
    worker.clock_ms = lambda: 6_120_002
    calls = len(history.calls)
    asyncio.run(worker.repair())
    assert len(history.calls) == calls + 1
    assert collector.cursor[INSTRUMENT] == 6_000_000


def test_worker_stop_interrupts_idle_receive_and_marks_disconnected(store):
    async def scenario():
        entered = asyncio.Event()

        class Socket:
            closed = False

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                self.closed = True

            async def recv(self):
                entered.set()
                await asyncio.Event().wait()

        socket = Socket()
        stop = asyncio.Event()
        collector = MinuteCollector({INSTRUMENT: 0}, store, lambda c: None)
        worker = CollectionWorker(
            StreamPlan(Market.USDM, (INSTRUMENT,)),
            History([]),
            collector,
            connect=lambda p: socket,
            clock_ms=lambda: 0,
        )
        task = asyncio.create_task(worker.run(stop))
        await asyncio.wait_for(entered.wait(), timeout=1)
        stop.set()
        await asyncio.wait_for(task, timeout=1)
        assert socket.closed and not collector.connected

    asyncio.run(scenario())


def test_store_failure_does_not_publish_or_advance():
    received = []

    class FailedStore:
        def put(self, observation):
            raise OSError("disk full")

    collector = MinuteCollector({INSTRUMENT: 0}, FailedStore(), received.append)
    with pytest.raises(OSError, match="disk full"):
        collector.accept(obs(0), now_ms=600_000)
    assert received == [] and collector.cursor[INSTRUMENT] == 0
    assert not collector.ready(INSTRUMENT, 60_000)


def test_worker_reconnects_read_only_transport_failure_and_stops_backoff(store):
    async def scenario():
        from omega.domain.errors import TransportError

        entered = asyncio.Event()

        class Disconnected:
            async def __aenter__(self):
                entered.set()
                raise TransportError("connection lost")

            async def __aexit__(self, *args):
                pass

        stop = asyncio.Event()
        collector = MinuteCollector({INSTRUMENT: 0}, store, lambda c: None)
        worker = CollectionWorker(
            StreamPlan(Market.USDM, (INSTRUMENT,)),
            History([]),
            collector,
            connect=lambda p: Disconnected(),
            clock_ms=lambda: 0,
        )
        task = asyncio.create_task(worker.run(stop))
        await asyncio.wait_for(entered.wait(), timeout=1)
        await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(task, timeout=1)
        assert not collector.connected

    asyncio.run(scenario())


def test_partial_frames_do_not_trigger_a_rest_request_for_every_message(store, monkeypatch):
    from omega.domain.errors import TransportError

    class Socket:
        count = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def recv(self):
            self.count += 1
            if self.count == 101:
                raise TransportError("end fixture")
            return "partial frame"

    monkeypatch.setattr(StreamPlan, "decode", lambda *_args, **_kwargs: None)
    history = History([])
    collector = MinuteCollector({INSTRUMENT: 0}, store, lambda _c: None)
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)),
        history,
        collector,
        clock_ms=lambda: 180_000,
        monotonic=lambda: 0,
        connect=lambda _plan: Socket(),
    )
    with pytest.raises(TransportError):
        asyncio.run(worker.session())
    assert history.calls == [(0, 180_000)]
    assert not collector.connected and not collector.ready(INSTRUMENT, 180_000)


def test_stop_waits_for_owned_history_thread_without_publishing_its_late_result(store):
    import threading

    entered, release = threading.Event(), threading.Event()
    received = []

    class BlockingHistory:
        def closed_minutes(self, *_args, **_kwargs):
            entered.set()
            assert release.wait(3)
            return (bar(0),)

    class Socket:
        closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            self.closed = True

        async def recv(self):
            await asyncio.Event().wait()

    async def scenario():
        socket = Socket()
        collector = MinuteCollector({INSTRUMENT: 0}, store, received.append)
        worker = CollectionWorker(
            StreamPlan(Market.USDM, (INSTRUMENT,)),
            BlockingHistory(),
            collector,
            clock_ms=lambda: 60_001,
            connect=lambda _plan: socket,
        )
        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(stop))
        assert await asyncio.to_thread(entered.wait, 1)
        stop.set()
        await asyncio.sleep(0.02)
        assert not task.done()
        release.set()
        await asyncio.wait_for(task, 1)
        assert socket.closed and not collector.connected
        assert received == [] and store.rows == {}

    try:
        asyncio.run(scenario())
    finally:
        release.set()


def test_stream_receives_while_history_is_blocked_and_commits_in_order(store):
    import json
    import threading

    entered, release = threading.Event(), threading.Event()
    received = []
    accepted = asyncio.Event()

    class BlockingHistory:
        def closed_minutes(self, *_args, **_kwargs):
            entered.set()
            assert release.wait(3)
            return (bar(0), bar(1))

    class Socket:
        closed = False
        calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            self.closed = True

        async def recv(self):
            self.calls += 1
            if self.calls == 1:
                return json.dumps(
                    {
                        "stream": "btcusdt@kline_1m",
                        "data": {
                            "e": "kline",
                            "E": 180000,
                            "s": "BTCUSDT",
                            "k": {
                                "t": 120000,
                                "T": 179999,
                                "s": "BTCUSDT",
                                "i": "1m",
                                "o": "100",
                                "h": "100",
                                "l": "100",
                                "c": "100",
                                "v": "1",
                                "x": True,
                            },
                        },
                    }
                )
            accepted.set()
            await asyncio.Event().wait()

    async def scenario():
        socket = Socket()
        collector = MinuteCollector({INSTRUMENT: 0}, store, received.append)
        worker = CollectionWorker(
            StreamPlan(Market.USDM, (INSTRUMENT,)),
            BlockingHistory(),
            collector,
            clock_ms=lambda: 180001,
            connect=lambda _plan: socket,
        )
        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(stop))
        assert await asyncio.to_thread(entered.wait, 1)
        await asyncio.wait_for(accepted.wait(), 1)
        assert received == [] and collector.cursor[INSTRUMENT] == 0
        release.set()
        for _ in range(100):
            if len(received) == 3:
                break
            await asyncio.sleep(0.005)
        assert received == [bar(0), bar(1), bar(2)]
        stop.set()
        await asyncio.wait_for(task, 1)
        assert socket.closed and worker._repair_task is None

    try:
        asyncio.run(scenario())
    finally:
        release.set()


def test_bar_schedule_validates_minute_intervals_and_finite_sessions():
    continuous = BarSchedule()
    unresolved = BarSchedule(sessions=())
    assert continuous.coverage_known(60_000)
    assert continuous.contains_open(60_000)
    assert continuous.next_expected(60_000) == 120_000
    assert continuous.contiguous_missing_range(60_000, 180_000) == (60_000, 180_000)
    assert not unresolved.coverage_known(60_000)
    assert unresolved.aligned_start(60_000) is None
    assert not unresolved.contains_open(60_000)
    assert unresolved.next_expected(60_000) is None
    assert unresolved.contiguous_missing_range(60_000, 180_000) is None

    with pytest.raises(ValidationError, match="whole-minute"):
        BarSchedule(90_000)
    with pytest.raises(ValidationError, match="ordered"):
        BarSchedule(60_000, ((0, 120_000), (60_000, 180_000)))

    schedule = BarSchedule(
        300_000, ((300_000, 900_000), (1_500_000, 1_800_000), (2_100_000, 2_400_000))
    )
    assert schedule.aligned_start(599_999) == 300_000
    offset_schedule = BarSchedule(300_000, ((360_000, 960_000),), receipt_grace_ms=20_000)
    assert offset_schedule.aligned_start(629_999) == 360_000
    assert offset_schedule.contiguous_missing_range(360_000, 679_999) is None
    assert offset_schedule.contiguous_missing_range(360_000, 680_000) == (360_000, 660_000)
    assert offset_schedule.contiguous_missing_range(660_000, 1_260_000, chunk=1) == (
        660_000,
        960_000,
    )
    assert schedule.next_expected(600_000) == 1_500_000
    assert schedule.next_expected(1_500_000) == 2_100_000
    assert schedule.next_expected(2_100_000) is None
    assert schedule.contiguous_missing_range(600_000, 1_700_000) == (600_000, 900_000)
    assert schedule.contiguous_missing_range(1_800_000, 2_000_000) is None
    assert schedule.coverage_known(1_200_000)
    assert schedule.coverage_known(2_400_000)
    assert not schedule.contains_open(1_200_000)
    assert schedule.aligned_start(1_200_000) is None
    assert not schedule.contains_open(2_400_000)
    assert not schedule.coverage_known(2_400_001)

    adjacent = BarSchedule(300_000, ((360_000, 660_000), (660_000, 960_000)))
    assert adjacent.next_expected(360_000) == 660_000
    assert adjacent.aligned_start(660_000) == 660_000
    assert adjacent.contains_open(660_000)


def test_collector_applies_per_instrument_schedule_and_receipt_grace(store):
    schedule = BarSchedule(60_000, ((0, 180_000), (300_000, 480_000)), 20_000)
    collector = MinuteCollector(
        {INSTRUMENT: 0}, store, lambda _c: None, schedules={INSTRUMENT: schedule}
    )
    collector.connected = True
    candle = Candle(INSTRUMENT, 0, 60_000, *(decimal("100") for _ in range(4)), decimal("1"))
    too_early = MinuteObservation(candle, "websocket", 79_999)
    with pytest.raises(ProtocolError, match="future"):
        collector.accept(too_early, now_ms=59_999)
    accepted = MinuteObservation(candle, "websocket", 60_000)
    assert collector.accept(accepted, now_ms=100_000) == 1
    assert collector.cursor[INSTRUMENT] == 60_000
    assert collector.missing_range(INSTRUMENT, 240_000) == (60_000, 180_000)
    assert collector.ready(INSTRUMENT, 60_000)
    for minute in (1, 2):
        candle = bar(minute)
        observation = MinuteObservation(candle, "websocket", candle.close_time_ms)
        assert collector.accept(observation, now_ms=candle.close_time_ms) == 1
    assert collector.cursor[INSTRUMENT] == 300_000
    closed_bar = MinuteObservation(bar(3), "websocket", 240_000)
    with pytest.raises(ProtocolError, match="schedule"):
        collector.accept(closed_bar, now_ms=240_000)
    assert store.get(INSTRUMENT, 180_000) is None
    assert not collector.ready(INSTRUMENT, 480_001)

    resumed = MinuteCollector(
        {INSTRUMENT: 480_000}, store, lambda _c: None, schedules={INSTRUMENT: schedule}
    )
    resumed.connected = True
    assert resumed.missing_range(INSTRUMENT, 480_000) is None
    assert resumed.ready(INSTRUMENT, 480_000)
    assert not resumed.ready(INSTRUMENT, 480_001)
    assert resumed.accept(obs(0), now_ms=480_000) == 0
    with pytest.raises(ValidationError, match="collection start"):
        MinuteCollector(
            {INSTRUMENT: 180_000}, store, lambda _c: None, schedules={INSTRUMENT: schedule}
        )


def test_native_repair_waits_for_websocket_and_keeps_committed_candle(store):
    received = []
    now = [60_001]
    collector = MinuteCollector({INSTRUMENT: 0}, store, received.append)
    history = History([bar(0, "99"), bar(1, "101")])
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)),
        history,
        collector,
        clock_ms=lambda: now[0],
        repair_grace_ms=5_000,
    )
    asyncio.run(worker.repair())
    assert history.calls == []
    # A previous live session committed this candle, but the worker still needs
    # to advance its in-memory cursor from the original stored value.
    store.put(obs(0))
    now[0] = 125_000
    asyncio.run(worker.repair())
    assert history.calls == [(0, 120_000)]
    assert received == [bar(0), bar(1, "101")]
    assert store.get(INSTRUMENT, 0) == bar(0)
    assert collector.cursor[INSTRUMENT] == 120_000


def test_native_rest_throttle_leaves_live_socket_connected(store):
    async def scenario():
        pause = asyncio.Event()
        stop = asyncio.Event()
        closed = False
        delays = []

        class Socket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                nonlocal closed
                closed = True

            async def recv(self):
                await asyncio.Event().wait()

        class Limited:
            def closed_minutes(self, *_args, **_kwargs):
                raise RateLimited(12)

        def throttled(seconds):
            delays.append(seconds)
            pause.set()

        collector = MinuteCollector({INSTRUMENT: 0}, store, lambda _: None)
        worker = CollectionWorker(
            StreamPlan(Market.USDM, (INSTRUMENT,)),
            Limited(),
            collector,
            connect=lambda _: Socket(),
            clock_ms=lambda: 120_000,
            repair_grace_ms=5_000,
            on_rest_throttle=throttled,
        )
        task = asyncio.create_task(worker.run(stop))
        try:
            await asyncio.wait_for(pause.wait(), timeout=2)
            assert collector.connected and not closed
            assert delays == [12]
            assert worker._next_repair - worker.monotonic() > 11
        finally:
            stop.set()
            await asyncio.wait_for(task, timeout=2)
        assert closed

    asyncio.run(scenario())
