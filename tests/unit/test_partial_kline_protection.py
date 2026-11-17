"""The same public socket keeps partial protection outside the candle store."""

import asyncio
import json
from decimal import Decimal as D

import pytest

from omega.application.collection import MinuteCollector
from omega.domain.errors import ProtocolError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.streams import StreamPlan
from omega.runtime.collection import CollectionWorker

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def payload(*, closed=False, event=10000):
    return json.dumps(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": event,
                "s": "BTCUSDT",
                "k": {
                    "s": "BTCUSDT",
                    "i": "1m",
                    "t": 0,
                    "T": 59999,
                    "o": "100",
                    "h": "101",
                    "l": "94",
                    "c": "100",
                    "v": "10",
                    "x": closed,
                },
            },
        }
    )


class Store:
    def __init__(self):
        self.rows = []

    def get(self, *_):
        return None

    def put(self, value):
        self.rows.append(value)
        return True


def test_same_socket_partial_callback_never_writes_canonical_db_then_closed_still_commits():
    store, received, partial = Store(), [], []
    collector = MinuteCollector({BTC: 0}, store, received.append)
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (BTC,)),
        object(),
        collector,
        clock_ms=lambda: 60001,
        on_protection=partial.append,
        protection_generation="a" * 32,
    )
    worker._protection_connection = 1  # The original socket-open lifecycle sets this.
    assert asyncio.run(worker.accept_stream(payload(), received_at_ms=10001)) is None
    assert partial[0].low == D(94) and not store.rows and not received
    assert worker.plan.decode(payload(), received_at_ms=10001) is None
    closed = asyncio.run(
        worker.accept_stream(payload(closed=True, event=60000), received_at_ms=60001)
    )
    assert closed.candle.close_time_ms == 60000
    assert len(store.rows) == len(received) == 1 and len(partial) == 1


@pytest.mark.parametrize("field,value", [("x", "false"), ("i", "5m"), ("T", True), ("l", "NaN")])
def test_partial_decoder_rejects_malformed_native_fields(field, value):
    raw = json.loads(payload())
    raw["data"]["k"][field] = value
    with pytest.raises(ProtocolError):
        StreamPlan(Market.USDM, (BTC,)).decode_protection(
            json.dumps(raw),
            received_at_ms=10001,
            generation="a" * 32,
            connection=1,
            sequence=1,
        )


@pytest.mark.parametrize("lead", [1, 2000])
def test_clock_leading_partial_is_unavailable_without_stopping_canonical_collection(lead):
    store, delivered, partial = Store(), [], []
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (BTC,)),
        object(),
        MinuteCollector({BTC: 0}, store, delivered.append),
        clock_ms=lambda: 60000,
        on_protection=partial.append,
        protection_generation="a" * 32,
    )
    worker._protection_connection = 1
    assert (
        asyncio.run(worker.accept_stream(payload(event=10000 + lead), received_at_ms=10000)) is None
    )
    assert not partial and not delivered and not store.rows
    # Existing closed-minute acceptance retains the real early receipt separately.
    result = asyncio.run(
        worker.accept_stream(payload(closed=True, event=60000), received_at_ms=59999)
    )
    assert result.received_at_ms == 60000 and result.raw_received_at_ms == 59999
    assert len(store.rows) == len(delivered) == 1
    assert not partial


def test_more_than_two_second_future_partial_is_not_granted_authority():
    with pytest.raises(ProtocolError):
        StreamPlan(Market.USDM, (BTC,)).decode_protection(
            payload(event=12001),
            received_at_ms=10000,
            generation="a" * 32,
            connection=1,
            sequence=1,
        )
