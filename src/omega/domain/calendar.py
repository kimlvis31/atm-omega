"""UTC calendar boundaries shared by storage queries and display aggregation."""

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

MINUTE_MS = 60000
CALENDAR_PERIODS = frozenset({"1w", "1M"})
CALENDAR_QUERY_LIMITS = {"1w": 52, "1M": 12}
MAX_CALENDAR_BUCKETS = 2048
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def utc_date(timestamp_ms: int) -> datetime:
    if type(timestamp_ms) is not int:
        raise ValueError("UTC timestamp must be integer milliseconds")
    try:
        return _EPOCH + timedelta(milliseconds=timestamp_ms)
    except OverflowError as exc:
        raise ValueError("UTC timestamp outside supported calendar") from exc


def calendar_boundary(timestamp_ms: int, period: str, nTicks: int = 0) -> int:
    if not isinstance(period, str) or period not in CALENDAR_PERIODS or type(nTicks) is not int:
        raise ValueError("unsupported calendar display interval")
    date = utc_date(timestamp_ms)
    try:
        if period == "1w":
            opening = date.replace(hour=0, minute=0, second=0, microsecond=0)
            opening += timedelta(days=7 * nTicks - date.weekday())
        else:
            year, month0 = divmod(date.year * 12 + date.month - 1 + nTicks, 12)
            opening = datetime(year, month0 + 1, 1, tzinfo=UTC)
    except (ValueError, OverflowError) as exc:
        raise ValueError("calendar boundary outside supported dates") from exc
    return (opening - _EPOCH) // timedelta(milliseconds=1)


def calendar_bounds(timestamp_ms: int, period: str) -> tuple[int, int]:
    return calendar_boundary(timestamp_ms, period), calendar_boundary(timestamp_ms, period, 1)


def calendar_ticks(begin: int, end: int, period: str, target: int = 5) -> tuple[int, ...]:
    if end <= begin:
        return ()
    start = calendar_boundary(begin, period)
    if start < begin:
        start = calendar_boundary(start, period, 1)
    if period == "1w":
        count = (end - start) // (7 * 24 * 60 * MINUTE_MS) + 1
    else:
        first, last = utc_date(start), utc_date(end)
        count = (last.year - first.year) * 12 + last.month - first.month + 1
    stride = max(1, math.ceil(count / max(2, min(10, target))))
    result = []
    while start <= end and len(result) < 12:
        result.append(start)
        try:
            start = calendar_boundary(start, period, stride)
        except ValueError:
            break
    return tuple(result)


def calendar_query_window(end_ms: int, period: str, limit: int) -> tuple[int, int]:
    """At most 52 weeks / 12 months including the current partial interval."""
    if (
        not isinstance(period, str)
        or period not in CALENDAR_PERIODS
        or type(limit) is not int
        or not 1 <= limit <= CALENDAR_QUERY_LIMITS[period]
        or type(end_ms) is not int
        or end_ms <= 0
        or end_ms % MINUTE_MS
    ):
        raise ValueError("invalid bounded calendar query")
    utc_date(end_ms)
    return max(0, calendar_boundary(end_ms - 1, period, 1 - limit)), end_ms


@dataclass(frozen=True, slots=True)
class CalendarBarQuality:
    start_ms: int
    end_ms: int
    expected_minutes: int
    requested_minutes: int
    closed_minutes: int
    observed_minutes: int
    missing_minutes: int
    prefix_partial: bool
    suffix_partial: bool
    is_closed: bool
    complete: bool
