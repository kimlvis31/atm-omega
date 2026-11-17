"""Portable captured-public clock regressions and bounded offline scheduling."""

import asyncio
import hashlib
import json
import time
from dataclasses import replace
from decimal import Decimal as D

import pytest
from test_margin_intraminute_protection import protect
from test_margin_strategy import BTC, setup
from test_protection_observation import context, observation

from omega.application.collection import MinuteCollector
from omega.application.margin_strategy import MarginStrategyState, _decode, _encode
from omega.domain.errors import ProtocolError, TransportError, ValidationError
from omega.domain.models import Instrument, Market
from omega.domain.protection import ProtectionObservation
from omega.infrastructure.binance.streams import StreamPlan
from omega.runtime.collection import MAX_PROTECTION_PENDING_BYTES, CollectionWorker
from omega.strategies.tef.module import TefModule

# Public Binance kline packets captured Nov3; no account data/absolute source paths.
PUBLIC_PACKETS = (
    (
        "spot",
        1793709230137,
        1793709232023,
        (
            '{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1793709232023,"s"'
            ':"BTCUSDT","k":{"t":1793709180000,"T":1793709239999,"s":"BTCUSDT","i":'
            '"1m","f":6814777750,"L":6814779613,"o":"77465.91000000","c":"77479.790'
            '00000","h":"77479.80000000","l":"77457.19000000","v":"7.51878000","n":'
            '1864,"x":false,"q":"582464.08373630","V":"4.97743000","Q":"385582.1304'
            '0980","B":"0"}}}'
        ),
        "b0f421c7f65b3d9df63625d25af7daabe4f3bbad1067248d7a6cc9bd55a4c020",
    ),
    (
        "spot",
        1793709232167,
        1793709234053,
        (
            '{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1793709234053,"s"'
            ':"BTCUSDT","k":{"t":1793709180000,"T":1793709239999,"s":"BTCUSDT","i":'
            '"1m","f":6814777750,"L":6814779627,"o":"77465.91000000","c":"77479.800'
            '00000","h":"77479.80000000","l":"77457.19000000","v":"7.67567000","n":'
            '1878,"x":false,"q":"594619.88822950","V":"5.00144000","Q":"387442.4204'
            '0780","B":"0"}}}'
        ),
        "9f3bf630073d64100f6f3edaf496f810397dbdf821ac8076186de2484955d861",
    ),
    (
        "spot",
        1793709234138,
        1793709236025,
        (
            '{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1793709236025,"s"'
            ':"BTCUSDT","k":{"t":1793709180000,"T":1793709239999,"s":"BTCUSDT","i":'
            '"1m","f":6814777750,"L":6814779960,"o":"77465.91000000","c":"77461.730'
            '00000","h":"77479.80000000","l":"77457.19000000","v":"9.84055000","n":'
            '2211,"x":false,"q":"762323.52730910","V":"6.55416000","Q":"507718.7978'
            '1340","B":"0"}}}'
        ),
        "0410e7e0f9d1efc1462adae1eed94a03191a24e7ac34571fa9d8e3f00b9e62d2",
    ),
    (
        "usdm",
        1793709239926,
        1793709241809,
        (
            '{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1793709241809,"s"'
            ':"BTCUSDT","k":{"t":1793709240000, "T":1793709299999, "s":"BTCUSDT", "'
            'i":"1m", "f":8229441262, "L":8229441333, "o":"77430.40", "c":"77426.20'
            '", "h":"77430.40", "l":"77426.10", "v":"1.189", "n":72, "x":false, "q"'
            ':"92063.90980", "V":"0.078", "Q":"6039.48720", "B":"0"}}}'
        ),
        "2e863c04b7375bb826b2486d6d4b3145a8924d04d2758e4cd728dbed82828f90",
    ),
    (
        "usdm",
        1793709240202,
        1793709242064,
        (
            '{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1793709242064,"s"'
            ':"BTCUSDT","k":{"t":1793709240000, "T":1793709299999, "s":"BTCUSDT", "'
            'i":"1m", "f":8229441262, "L":8229441334, "o":"77430.40", "c":"77426.10'
            '", "h":"77430.40", "l":"77426.10", "v":"1.648", "n":73, "x":false, "q"'
            ':"127602.48970", "V":"0.078", "Q":"6039.48720", "B":"0"}}}'
        ),
        "c6d240b162dd567db13248ac15ac1e94e4bc1d7566e577de178f436b8bfb11bd",
    ),
    (
        "usdm",
        1793709240551,
        1793709242436,
        (
            '{"stream":"btcusdt@kline_1m","data":{"e":"kline","E":1793709242436,"s"'
            ':"BTCUSDT","k":{"t":1793709240000, "T":1793709299999, "s":"BTCUSDT", "'
            'i":"1m", "f":8229441262, "L":8229441795, "o":"77430.40", "c":"77405.50'
            '", "h":"77430.40", "l":"77405.40", "v":"58.939", "n":534, "x":false, "'
            'q":"4562605.13510", "V":"0.140", "Q":"10838.64810", "B":"0"}}}'
        ),
        "e1468c0ead0ac95a27cd01fd44a5d21fd2dc2066325490dbf6219c6c9131504a",
    ),
)


class Clock:
    def __init__(self, now=10000):
        self.now, self.elapsed = now, 0.0

    def wall(self):
        return self.now

    def monotonic(self):
        return self.elapsed


class Store:
    def __init__(self):
        self.rows = []

    def get(self, *_):
        return None

    def put(self, row):
        self.rows.append(row)
        return True


def payload(event=11800, *, symbol="BTCUSDT", padding=""):
    return json.dumps(
        {
            "stream": f"{symbol.lower()}@kline_1m",
            "data": {
                "e": "kline",
                "E": event,
                "s": symbol,
                "padding": padding,
                "k": {
                    "s": symbol,
                    "i": "1m",
                    "t": 0,
                    "T": 59999,
                    "o": "100",
                    "h": "100",
                    "l": "94",
                    "c": "94",
                    "v": "1",
                    "x": False,
                },
            },
        }
    )


def worker(clock, *, instruments=(BTC,), callback=None, **options):
    stored, published = Store(), []
    collector = MinuteCollector({item: 0 for item in instruments}, stored, lambda _: None)
    result = CollectionWorker(
        StreamPlan(instruments[0].market, instruments),
        object(),
        collector,
        clock_ms=clock.wall,
        monotonic=clock.monotonic,
        on_protection=published.append if callback is None else callback,
        protection_generation="a" * 32,
        **options,
    )
    result._protection_connection = 1
    return result, stored, published


@pytest.mark.parametrize("market,receipt,event,raw,sha", PUBLIC_PACKETS)
def test_original_six_public_packets_keep_receipt_and_wait_for_actual_clock(
    market, receipt, event, raw, sha
):
    assert hashlib.sha256(raw.encode()).hexdigest() == sha
    instrument = Instrument("binance", Market(market), "BTCUSDT")
    clock = Clock(receipt)
    service, stored, published = worker(clock, instruments=(instrument,))
    assert (
        service.plan.decode_protection(
            raw, received_at_ms=receipt, generation="a" * 32, connection=1, sequence=1
        )
        is None
    )
    assert asyncio.run(service.accept_stream(raw, received_at_ms=receipt)) is None
    assert not published and not stored.rows and service.protection_pending_status()["records"] == 1
    clock.now, clock.elapsed = event, (event - receipt) / 1000
    service._drain_protection()
    assert len(published) == 1 and not stored.rows
    accepted = published[0]
    assert accepted.event_time_ms == event <= accepted.received_at_ms == clock.now
    assert accepted.raw_received_at_ms == receipt and 0 < event - receipt <= 2000
    assert accepted.original_received_at_ms == receipt
    assert ProtectionObservation.from_document(accepted.document()) == accepted
    assert service.protection_pending_status()["records"] == 0


@pytest.mark.parametrize("mode", ["frozen", "backward"])
def test_frozen_or_backward_wall_clock_expires_without_future_or_revived_publication(mode):
    clock = Clock()
    service, _, published = worker(clock)
    asyncio.run(service.accept_stream(payload(), received_at_ms=clock.now))
    clock.now, clock.elapsed = (10000 if mode == "frozen" else 9000), 2.01
    service._drain_protection()
    assert not published and service.protection_pending_status()["records"] == 0
    clock.now, clock.elapsed = 11800, 2.02
    service._drain_protection()
    assert not published and service.protection_pending_status()["expired"] == 1


@pytest.mark.parametrize("field,value", [("c", "NaN"), ("h", "0"), ("l", "101"), ("t", True)])
def test_future_callback_only_receives_fully_validated_partial(field, value):
    raw = json.loads(payload())
    raw["data"]["k"][field] = value
    deferred = []
    with pytest.raises(ProtocolError):
        StreamPlan(Market.USDM, (BTC,)).decode_protection(
            json.dumps(raw),
            received_at_ms=10000,
            generation="a" * 32,
            connection=1,
            sequence=1,
            on_future=lambda *args: deferred.append(args),
        )
    assert not deferred


def test_future_ids_and_more_than_two_seconds_are_rejected_before_queueing():
    plan = StreamPlan(Market.USDM, (BTC,))
    for raw, changed in (
        (payload(12001), {}),
        (payload(), {"generation": "bad"}),
        (payload(), {"sequence": True}),
    ):
        values = dict(received_at_ms=10000, generation="a" * 32, connection=1, sequence=1)
        values.update(changed)
        deferred = []
        with pytest.raises(ProtocolError):
            plan.decode_protection(
                raw, **values, on_future=lambda *args, target=deferred: target.append(args)
            )
        assert not deferred


def test_oldest_matures_before_latest_coalescing_without_starvation_or_receipt_renewal():
    clock = Clock()
    service, _, published = worker(clock)
    for index in range(6):
        clock.now, clock.elapsed = 10000 + index * 300, index * 0.3
        asyncio.run(service.accept_stream(payload(clock.now + 1800), received_at_ms=clock.now))
    assert service.protection_pending_status()["records"] == 2
    assert service.protection_pending_status()["coalesced"] == 4
    clock.now, clock.elapsed = 11800, 1.8
    service._drain_protection()
    assert len(published) == 1 and published[0].sequence == 1
    assert published[0].raw_received_at_ms == 10000
    clock.now, clock.elapsed = 13300, 3.3
    service._drain_protection()
    assert [item.sequence for item in published] == [1, 6]
    assert published[1].raw_received_at_ms == 11500


def test_global_bytes_are_bounded_and_one_instrument_cannot_consume_all_slots():
    instruments = tuple(
        Instrument("binance", Market.USDM, f"A{index:02}USDT") for index in range(24)
    )
    clock = Clock()
    service, _, published = worker(clock, instruments=instruments)
    for instrument in instruments:
        asyncio.run(
            service.accept_stream(
                payload(symbol=instrument.symbol, padding="x" * 60000), received_at_ms=10000
            )
        )
    status = service.protection_pending_status()
    assert status["bytes"] <= MAX_PROTECTION_PENDING_BYTES and status["records"] <= 256
    assert status["capacity"] > 0 and not published
    service._clear_protection_pending()
    for _ in range(10):
        asyncio.run(
            service.accept_stream(payload(symbol=instruments[0].symbol), received_at_ms=10000)
        )
    asyncio.run(service.accept_stream(payload(symbol=instruments[1].symbol), received_at_ms=10000))
    assert len(service._protection_pending[instruments[0]]) == 2
    assert len(service._protection_pending[instruments[1]]) == 1
    clock.now, clock.elapsed = 11800, 1.8
    service._drain_protection()
    assert {item.instrument for item in published} == set(instruments[:2])


def test_delayed_raw_receipt_cannot_be_promoted_past_position_entry_and_marks_use_acceptance():
    delayed = observation(
        event_time_ms=11800, received_at_ms=11800, raw_received_at_ms=10000, last=D(94)
    )
    sample = context(observation(last=D(94)), changed=0)
    with pytest.raises(ValidationError):
        replace(
            sample,
            price=delayed.last,
            observed_at_ms=11800,
            position=replace(sample.position, last_changed_ms=11000),
            protection_observation=delayed,
        )
    *_, store, execution, adapter = setup(
        TefModule(BTC, {"fullStopLossImmediate": ".05"}), quantity="1"
    )
    result = protect(adapter, delayed)
    assert result.orders[0].reducing
    action = adapter.snapshot().checkpoint.last_protection
    own_mark = next(item for item in action.marks if item.instrument == BTC)
    assert own_mark.event_time_ms == 11800 and own_mark.available_at_ms == 11800
    assert action.observation.raw_received_at_ms == 10000
    assert MarginStrategyState.parse(json.loads(store.document)).document() == json.loads(
        store.document
    )
    assert execution.snapshot().pending[0].eligible_at_ms == 60000


def test_normal_dto_and_schema3_codec_omit_optional_none_but_preserve_delayed_original():
    normal = observation()
    assert "raw_received_at_ms" not in normal.document()
    encoded = _encode(normal)
    assert "raw_received_at_ms" not in encoded["fields"]
    assert _encode(_decode(encoded)) == encoded
    assert ProtectionObservation.from_document(normal.document()) == normal
    with pytest.raises(ValidationError):
        ProtectionObservation.from_document(normal.document() | {"raw_received_at_ms": None})
    delayed = replace(normal, event_time_ms=11800, received_at_ms=11800, raw_received_at_ms=10000)
    encoded_delayed = _encode(delayed)
    assert encoded_delayed["fields"]["raw_received_at_ms"] == 10000
    assert _encode(_decode(encoded_delayed)) == encoded_delayed
    delayed.current(11800)
    with pytest.raises(ValidationError):
        delayed.current(15001)


class Socket:
    def __init__(self):
        self.rows = asyncio.Queue()
        self.active = self.maximum = 0

    async def recv(self):
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        try:
            row = await self.rows.get()
            if isinstance(row, Exception):
                raise row
            return row
        finally:
            self.active -= 1


class Connection:
    def __init__(self, socket):
        self.socket = socket

    async def __aenter__(self):
        return self.socket

    async def __aexit__(self, *_):
        return False


async def queued(service):
    async with asyncio.timeout(1):
        while service.protection_pending_status()["records"] != 1:
            await asyncio.sleep(0.001)


@pytest.mark.parametrize("exit_kind", ["stop", "disconnect"])
def test_stop_or_disconnect_reaps_recv_and_discards_pending_without_late_publication(exit_kind):
    async def scenario():
        clock, socket = Clock(), Socket()
        service, _, published = worker(clock, connect=lambda _: Connection(socket))
        socket.rows.put_nowait(payload())
        stop = asyncio.Event()
        task = asyncio.create_task(service.run(stop) if exit_kind == "stop" else service.session())
        try:
            await queued(service)
            if exit_kind == "stop":
                stop.set()
                await asyncio.wait_for(task, timeout=1)
            else:
                socket.rows.put_nowait(TransportError("synthetic disconnect"))
                with pytest.raises(TransportError):
                    await asyncio.wait_for(task, timeout=1)
            assert socket.maximum == 1 and socket.active == 0
            assert service.protection_pending_status()["records"] == 0
            assert service.protection_pending_status()["bytes"] == 0
            clock.now, clock.elapsed = 11800, 1.8
            service._drain_protection()
            assert not [item for item in published if item is not None]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())


def test_timer_matures_without_more_packets_and_does_not_renew_network_idle_deadline():
    async def scenario():
        started = time.monotonic()

        class RunningClock:
            @staticmethod
            def wall():
                return 10000 + int((time.monotonic() - started) * 1000)

            @staticmethod
            def monotonic():
                return time.monotonic()

        socket, accepted, health = Socket(), [], []
        ready = asyncio.Event()

        def delivered(item):
            if item is not None:
                accepted.append(item)
                ready.set()

        service, _, _ = worker(
            RunningClock(),
            callback=delivered,
            connect=lambda _: Connection(socket),
            idle_seconds=0.05,
        )
        original_health = service.health

        def observe_health(reason):
            health.append(reason)
            return original_health(reason)

        service.health = observe_health
        socket.rows.put_nowait(payload(10200))
        task = asyncio.create_task(service.session())
        try:
            await asyncio.wait_for(ready.wait(), timeout=1)
            assert len(accepted) == 1 and accepted[0].event_time_ms == 10200
            assert accepted[0].raw_received_at_ms < accepted[0].event_time_ms
            assert accepted[0].received_at_ms >= accepted[0].event_time_ms
            assert health.count("stale") >= 1
            assert socket.maximum == 1
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert socket.active == 0 and service.protection_pending_status()["records"] == 0

    asyncio.run(scenario())
