"""UTC calendar boundaries and complete display bars; no execution-analysis imports."""

# Imports
import math
from dataclasses import dataclass

from omega.domain.calendar import (
    CALENDAR_PERIODS,
    MAX_CALENDAR_BUCKETS,
    MINUTE_MS,
    CalendarBarQuality,
    calendar_boundary,
    calendar_bounds,
    utc_date,
)
from omega.domain.calendar import (
    calendar_ticks as calendar_ticks,
)
from omega.gui.models import Candle


def aggregate_calendar(candles: tuple[Candle, ...], period: str) -> tuple[Candle, ...]:
    """A calendar bar exists only after every constituent closed 1m observation exists."""
    if period not in CALENDAR_PERIODS:
        raise ValueError("unsupported calendar display interval")
    if any(type(row.open_time_ms) is not int or row.open_time_ms % MINUTE_MS for row in candles):
        raise ValueError("calendar display requires UTC-aligned one-minute candles")
    if any(a.open_time_ms >= b.open_time_ms for a, b in zip(candles, candles[1:], strict=False)):
        raise ValueError("calendar input must be sorted with unique timestamps")
    result, rows = [], []
    opening = ending = None

    def complete():
        if (
            rows
            and len(rows) == (ending - opening) // MINUTE_MS
            and all(
                row.open_time_ms == opening + index * MINUTE_MS for index, row in enumerate(rows)
            )
        ):
            result.append(
                Candle(
                    opening,
                    rows[0].open,
                    max(row.high for row in rows),
                    min(row.low for row in rows),
                    rows[-1].close,
                    math.fsum(row.volume for row in rows),
                )
            )

    for row in candles:
        if ending is None or row.open_time_ms >= ending:
            complete()
            opening, ending = calendar_bounds(row.open_time_ms, period)
            rows = []
        rows.append(row)
    complete()
    return tuple(result)


@dataclass(frozen=True, slots=True)
class CalendarAggregation:
    candles: tuple[Candle, ...]
    quality: tuple[CalendarBarQuality, ...]


def aggregate_calendar_with_quality(
    candles: tuple[Candle, ...],
    period: str,
    *,
    begin_ms: int,
    end_ms: int,
    closed_until_ms: int,
) -> CalendarAggregation:
    """Aggregate closed UTC 1m observations in explicit half-open request bounds.

    ``closed_until_ms`` is the exclusive confirmed-minute boundary, not an inferred
    wall clock. Input must be sorted, unique and within the requested closed range.
    Every intersecting calendar interval gets quality, including wholly absent
    intervals. Only complete intervals enter ``candles``; no interpolation occurs.
    The existing complete-only helper and fixed-minute display contracts are intact.
    """
    if period not in CALENDAR_PERIODS:
        raise ValueError("unsupported calendar display interval")
    for timestamp in (begin_ms, end_ms, closed_until_ms):
        if type(timestamp) is not int or timestamp % MINUTE_MS:
            raise ValueError("calendar request bounds must be UTC-aligned integer minutes")
        utc_date(timestamp)
    if end_ms < begin_ms:
        raise ValueError("calendar request end precedes begin")
    for row in candles:
        if not begin_ms <= row.open_time_ms < min(end_ms, closed_until_ms):
            raise ValueError("calendar observation is outside the requested closed range")
        numbers = (row.open, row.high, row.low, row.close, row.volume)
        if (
            any(isinstance(value, bool) or not math.isfinite(value) for value in numbers)
            or row.low > min(row.open, row.close)
            or row.high < max(row.open, row.close)
            or row.volume < 0
        ):
            raise ValueError("calendar observation OHLCV is inconsistent or non-finite")
    complete_bars = aggregate_calendar(candles, period)
    if begin_ms == end_ms:
        return CalendarAggregation((), ())

    # [1]: Index observed counts once; absent periods still receive quality below.
    counts = {}
    for row in candles:
        opening = calendar_boundary(row.open_time_ms, period)
        counts[opening] = counts.get(opening, 0) + 1
    quality = []
    opening = calendar_boundary(begin_ms, period)
    while opening < end_ms:
        if len(quality) >= MAX_CALENDAR_BUCKETS:
            raise ValueError("calendar quality exceeds its bounded period count")
        ending = calendar_boundary(opening, period, 1)
        requested_start, requested_end = max(begin_ms, opening), min(end_ms, ending)
        closed_minutes = max(0, min(requested_end, closed_until_ms) - requested_start) // MINUTE_MS
        observed = counts.get(opening, 0)
        expected = (ending - opening) // MINUTE_MS
        quality.append(
            CalendarBarQuality(
                start_ms=opening,
                end_ms=ending,
                expected_minutes=expected,
                requested_minutes=(requested_end - requested_start) // MINUTE_MS,
                closed_minutes=closed_minutes,
                observed_minutes=observed,
                missing_minutes=closed_minutes - observed,
                prefix_partial=begin_ms > opening,
                suffix_partial=end_ms < ending,
                is_closed=ending <= closed_until_ms,
                complete=ending <= closed_until_ms and observed == expected,
            )
        )
        opening = ending
    return CalendarAggregation(complete_bars, tuple(quality))


def covered_minutes(candles: tuple[Candle, ...], period: int | str) -> int:
    if period in CALENDAR_PERIODS:
        return sum(
            (calendar_bounds(row.open_time_ms, period)[1] - row.open_time_ms) // MINUTE_MS
            for row in candles
        )
    return len(candles) * period
