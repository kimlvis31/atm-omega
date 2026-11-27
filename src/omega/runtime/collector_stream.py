"""Bounded, contiguous pages over committed one-minute observations."""

from dataclasses import dataclass

from omega.domain.bar_schedule import BarSchedule
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Candle
from omega.infrastructure.market_data import instrument_key
from omega.ports.collection import MinuteObservation

MINUTE_MS = 60_000
MAX_TIME_MS = 253_402_300_799_000
MAX_PAGE_SIZE = 128
_MINUTE_SCHEDULE = BarSchedule(MINUTE_MS)


@dataclass(frozen=True)
class CommittedPage:
    observations: tuple[MinuteObservation, ...]
    next_ms: int
    state: str


def read_committed_page(
    source, instrument: str, next_ms: int, *, as_of_ms: int, limit: int = 64
) -> CommittedPage:
    """Read a bounded page, advancing only over an uninterrupted minute prefix.

    ``next_ms`` is a consumer cursor. A retry starts at that same minute.
    An arrived backfill can extend the prefix; advance only after processing.
    """
    try:
        canonical = instrument_key(instrument)
    except (AttributeError, TypeError, ValueError, ValidationError):
        raise ValidationError("instrument must be a canonical Binance instrument") from None
    if (
        canonical.key != instrument
        or type(next_ms) is not int
        or not 0 <= next_ms <= MAX_TIME_MS
        or _MINUTE_SCHEDULE.aligned_start(next_ms) != next_ms
        or type(as_of_ms) is not int
        or not 0 <= as_of_ms <= MAX_TIME_MS
        or type(limit) is not int
        or not 1 <= limit <= MAX_PAGE_SIZE
    ):
        raise ValidationError("invalid committed stream cursor, cutoff or page limit")

    window = _MINUTE_SCHEDULE.contiguous_missing_range(next_ms, as_of_ms, chunk=limit)
    if window is None:
        return CommittedPage((), next_ms, "caught_up")
    _, end_ms = window

    rows = source.read_observations(
        [canonical.key], next_ms, end_ms, limit=limit, as_of_ms=as_of_ms
    )
    if type(rows) is not tuple or len(rows) > limit:
        raise ProtocolError("committed source must return a bounded tuple")

    prior = None
    for row in rows:
        if (
            type(row) is not MinuteObservation
            or not isinstance(row.candle, Candle)
            or row.candle.instrument != canonical
            or type(row.candle.open_time_ms) is not int
            or not next_ms <= row.candle.open_time_ms < end_ms
            or _MINUTE_SCHEDULE.aligned_start(row.candle.open_time_ms)
            != row.candle.open_time_ms
            or row.candle.close_time_ms != _MINUTE_SCHEDULE.next_expected(row.candle.open_time_ms)
            or prior is not None
            and row.candle.open_time_ms <= prior
            or row.source not in ("rest", "websocket", "vision")
            or type(row.received_at_ms) is not int
            or not row.candle.close_time_ms <= row.received_at_ms <= as_of_ms
        ):
            raise ProtocolError("committed source returned an invalid or unsorted observation")
        prior = row.candle.open_time_ms

    contiguous = []
    cursor = next_ms
    for row in rows:
        opened = row.candle.open_time_ms
        if opened != cursor:
            break
        contiguous.append(row)
        cursor = row.candle.close_time_ms

    unread = _MINUTE_SCHEDULE.contiguous_missing_range(cursor, as_of_ms, chunk=1)
    state = "caught_up"
    if unread is not None:
        state = "page_full" if len(contiguous) == limit else "waiting_for_gap"
    return CommittedPage(tuple(contiguous), cursor, state)
