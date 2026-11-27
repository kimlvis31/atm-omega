"""A real early closed frame and controlled local clocks: no future publication."""

import asyncio
from pathlib import Path

import pytest

from omega.application.collection import MinuteCollector
from omega.domain.bar_schedule import BarSchedule
from omega.domain.errors import ProtocolError, TransportError
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.infrastructure.binance.streams import StreamPlan
from omega.runtime import collection

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
OPEN = 1793017980000
CLOSE = OPEN + 60_000
RAW = (Path(__file__).parents[1] / "fixtures/binance-usdm-early-closed.json").read_text()


class Store:
    def __init__(self):
        self.rows = []

    def put(self, observation):
        assert observation.received_at_ms >= observation.candle.close_time_ms
        self.rows.append(observation)
        return True


class History:
    def closed_minutes(self, *args, **kwargs):
        return ()


def worker(clock, monotonic):
    store = Store()
    owner = MinuteCollector({INSTRUMENT: OPEN}, store, lambda _c: None)
    return collection.CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)),
        History(),
        owner,
        clock_ms=clock,
        monotonic=monotonic,
    ), store


@pytest.mark.parametrize("offset", [-401, 400])
def test_small_clock_skew_waits_for_actual_acceptance_and_retains_original_receipt(
    monkeypatch, offset
):
    wall, elapsed = [CLOSE + offset], [0.0]
    instance, store = worker(lambda: wall[0], lambda: elapsed[0])
    receipt = wall[0]

    async def sleep(seconds):
        assert store.rows == []
        elapsed[0] += seconds
        wall[0] += int(round(seconds * 1000)) + 1

    monkeypatch.setattr(collection.asyncio, "sleep", sleep)
    accepted = asyncio.run(instance.accept_stream(RAW, received_at_ms=receipt))
    assert len(store.rows) == 1 and accepted == store.rows[0]
    assert accepted.received_at_ms == wall[0] >= CLOSE
    assert accepted.raw_received_at_ms == receipt
    assert accepted.raw_source == "websocket" and accepted.raw == RAW
    if offset < 0:
        assert elapsed[0] > 0 and accepted.received_at_ms > CLOSE
    else:
        assert elapsed[0] == 0


@pytest.mark.parametrize("backwards", [False, True])
def test_stuck_or_backward_clock_hits_monotonic_deadline_without_publishing(monkeypatch, backwards):
    wall, elapsed = [CLOSE - 400], [0.0]
    instance, store = worker(lambda: wall[0], lambda: elapsed[0])

    async def sleep(seconds):
        elapsed[0] += seconds
        if backwards:
            wall[0] -= 100

    monkeypatch.setattr(collection.asyncio, "sleep", sleep)
    with pytest.raises(TransportError, match="clock"):
        asyncio.run(instance.accept_stream(RAW, received_at_ms=wall[0]))
    assert elapsed[0] >= 2 and store.rows == []


def test_more_than_two_seconds_ahead_remains_a_rejected_future_frame():
    instance, store = worker(lambda: CLOSE - 2001, lambda: 0)
    with pytest.raises(ProtocolError):
        asyncio.run(instance.accept_stream(RAW, received_at_ms=CLOSE - 2001))
    assert not store.rows


def test_stop_interrupts_early_frame_wait_and_no_future_row_is_saved():
    async def scenario():
        delivered = asyncio.Event()

        class Socket:
            closed = False

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                self.closed = True

            async def recv(self):
                delivered.set()
                return RAW

        instance, store = worker(lambda: CLOSE - 400, collection.time.monotonic)
        # Bootstrap starts at the current still-open local minute, so no REST is
        # needed before consuming this slightly early exchange-close event.
        instance.collector.cursor[INSTRUMENT] = OPEN
        socket, stop = Socket(), asyncio.Event()
        instance.connect = lambda _plan: socket
        task = asyncio.create_task(instance.run(stop))
        await asyncio.wait_for(delivered.wait(), 1)
        await asyncio.sleep(0)
        stop.set()
        await asyncio.wait_for(task, 1)
        assert socket.closed and not store.rows and not instance.collector.connected

    asyncio.run(scenario())


def test_repair_only_requests_due_bars_from_resolved_trading_sessions():
    minute = 60_000
    grace = 2_000
    next_open = OPEN + 3 * minute
    following_open = next_open + 3 * minute
    next_close = next_open + minute
    schedule = BarSchedule(
        minute,
        ((OPEN, CLOSE), (next_open, next_close), (following_open, following_open + minute)),
        receipt_grace_ms=grace,
    )

    def bar(opened):
        price = decimal("100")
        return Candle(INSTRUMENT, opened, opened + minute, price, price, price, price, price)

    first, second = bar(OPEN), bar(next_open)
    rows, requests = [first], []

    class AvailableHistory:
        def closed_minutes(self, instrument, begin, end, *, now_ms):
            assert instrument == INSTRUMENT and end <= now_ms
            requests.append((begin, end))
            return tuple(row for row in rows if begin <= row.open_time_ms < end)

    store, received, reports = Store(), [], []
    collector = MinuteCollector(
        {INSTRUMENT: OPEN}, store, received.append, schedules={INSTRUMENT: schedule}
    )
    collector.connected = True
    now = [CLOSE + grace - 1]
    instance = collection.CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)),
        AvailableHistory(),
        collector,
        clock_ms=lambda: now[0],
        on_health=lambda ready, reason: reports.append((ready, reason)),
    )

    asyncio.run(instance.repair())
    assert requests == []
    now[0] += 1
    asyncio.run(instance.repair())
    assert requests == [(OPEN, CLOSE)]
    assert collector.cursor[INSTRUMENT] == next_open
    assert collector.ready(INSTRUMENT, now[0])

    now[0] = next_close + grace
    asyncio.run(instance.repair())
    assert requests[-1] == (next_open, next_close)
    assert collector.cursor[INSTRUMENT] == next_open
    assert not collector.ready(INSTRUMENT, now[0])
    instance.health("gap")
    assert reports[-1] == (False, "gap")

    rows.append(second)
    asyncio.run(instance.repair())
    assert requests == [(OPEN, CLOSE), (next_open, next_close), (next_open, next_close)]
    assert received == [first, second]
    assert collector.cursor[INSTRUMENT] == following_open
    assert collector.ready(INSTRUMENT, now[0])

    now[0] = following_open + minute + grace
    instance.health("gap")
    assert reports[-1] == (False, "gap")

    rows.append(bar(following_open))
    asyncio.run(instance.repair())
    assert collector.missing_range(INSTRUMENT, now[0]) is None
    instance.health("gap")
    assert reports[-1] == (False, "calendar_unavailable")
    instance.health("rate_limited")
    assert reports[-1] == (False, "rate_limited")
