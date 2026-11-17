import asyncio

import pytest

from omega.analysis.graph import FeatureGraph, FeatureNode
from omega.analysis.moving_averages import (
    LegacyMaFeatures,
    MaConfiguration,
)
from omega.application.collection import MinuteCollector
from omega.application.strategies import StrategyRunner
from omega.domain.errors import ProtocolError
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
