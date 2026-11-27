from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.ports.collection import MinuteObservation
from omega.runtime import collector_api
from omega.runtime.collector_stream import read_committed_page

KEY = "binance:spot:BTCUSDT"
INSTRUMENT = Instrument("binance", Market.SPOT, "BTCUSDT")
MINUTE = 60_000


def observation(open_ms):
    candle = Candle(
        INSTRUMENT,
        open_ms,
        open_ms + MINUTE,
        Decimal("1"),
        Decimal("1"),
        Decimal("1"),
        Decimal("1"),
        Decimal("0"),
    )
    return MinuteObservation(candle, "rest", open_ms + MINUTE)


class Source:
    def __init__(self, rows):
        self.rows = tuple(rows)
        self.calls = []

    def read_observations(self, keys, start, end, *, limit, as_of_ms):
        self.calls.append((keys, start, end, limit, as_of_ms))
        return tuple(row for row in self.rows if start <= row.candle.open_time_ms < end)[:limit]


def test_gap_returns_only_contiguous_prefix_without_advancing_across_gap():
    source = Source([observation(0), observation(MINUTE), observation(3 * MINUTE)])

    page = read_committed_page(source, KEY, 0, as_of_ms=5 * MINUTE + 123)

    assert [row.candle.open_time_ms for row in page.observations] == [0, MINUTE]
    assert page.next_ms == 2 * MINUTE
    assert page.state == "waiting_for_gap"


def test_resume_from_returned_cursor_starts_at_missing_minute():
    source = Source([observation(0), observation(2 * MINUTE)])
    first = read_committed_page(source, KEY, 0, as_of_ms=4 * MINUTE)
    assert first.next_ms == MINUTE
    source.rows = (observation(MINUTE), observation(2 * MINUTE))

    resumed = read_committed_page(source, KEY, first.next_ms, as_of_ms=4 * MINUTE)

    assert [row.candle.open_time_ms for row in resumed.observations] == [MINUTE, 2 * MINUTE]
    assert resumed.next_ms == 3 * MINUTE
    assert resumed.state == "waiting_for_gap"


def test_out_of_order_source_contract_is_rejected():
    source = Source([observation(0), observation(2 * MINUTE), observation(MINUTE)])
    with pytest.raises(ProtocolError):
        read_committed_page(source, KEY, 0, as_of_ms=4 * MINUTE)


def test_page_window_is_capped_and_full_page_leaves_cursor_for_next_call():
    source = Source(observation(index * MINUTE) for index in range(140))
    page = read_committed_page(source, KEY, 0, as_of_ms=140 * MINUTE, limit=128)

    assert len(page.observations) == 128
    assert page.next_ms == 128 * MINUTE
    assert page.state == "page_full"
    assert source.calls[0][2] == 128 * MINUTE
    resumed = read_committed_page(source, KEY, page.next_ms, as_of_ms=140 * MINUTE, limit=128)
    assert [row.candle.open_time_ms for row in resumed.observations] == [
        index * MINUTE for index in range(128, 140)
    ]


def test_frontier_is_floored_to_completed_minute_and_same_cursor_repeats():
    source = Source([observation(0), observation(MINUTE)])
    first = read_committed_page(source, KEY, 0, as_of_ms=2 * MINUTE - 1)
    repeat = read_committed_page(source, KEY, 0, as_of_ms=2 * MINUTE - 1)
    assert first == repeat
    assert first.next_ms == MINUTE
    assert first.state == "caught_up"


@pytest.mark.parametrize(
    "instrument,next_ms,as_of_ms,limit",
    [
        ("BINANCE:spot:BTCUSDT", 0, MINUTE, 1),
        (KEY, -MINUTE, MINUTE, 1),
        (KEY, 1, MINUTE, 1),
        (KEY, 0, True, 1),
        (KEY, 0, MINUTE, 129),
    ],
)
def test_invalid_arguments_are_rejected(instrument, next_ms, as_of_ms, limit):
    with pytest.raises(ValidationError):
        read_committed_page(Source([]), instrument, next_ms, as_of_ms=as_of_ms, limit=limit)


def test_live_poll_checks_again_until_a_committed_minute_arrives(monkeypatch):
    source = Source([])
    now = 0.0

    def sleep(seconds):
        nonlocal now
        now += seconds
        source.rows = (observation(0),)

    monkeypatch.setattr(
        collector_api,
        "time",
        SimpleNamespace(
            monotonic=lambda: now,
            time_ns=lambda: (MINUTE + int(now * 1000)) * 1_000_000,
            sleep=sleep,
        ),
    )
    response = collector_api.read_collector_stream(
        source, urlencode({"instrument": KEY, "next_ms": 0, "wait_ms": 500})
    )

    assert len(source.calls) == 2
    assert response["next_ms"] == MINUTE
    assert response["as_of_ms"] == MINUTE + 250
    assert [row["open_ms"] for row in response["rows"]] == [0]
    assert response["state"] == "caught_up"


def test_wait_with_fixed_cutoff_is_rejected_without_accessing_database():
    source = Source([])
    query = urlencode({"instrument": KEY, "next_ms": 0, "wait_ms": 500, "as_of_ms": MINUTE})
    with pytest.raises(ValidationError):
        collector_api.read_collector_stream(source, query)
    assert not source.calls
