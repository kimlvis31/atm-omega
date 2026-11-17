"""Bounded read-only SQL calendar aggregation; original one-minute rows stay unchanged."""

from dataclasses import asdict, replace
from datetime import datetime, timedelta
from decimal import Decimal

from omega.domain.calendar import (
    MINUTE_MS,
    CalendarBarQuality,
    calendar_bounds,
    calendar_query_window,
)
from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument
from omega.infrastructure.candles import EPOCH, connect_database
from omega.infrastructure.postgres.config import DatabaseSettings
from omega.infrastructure.postgres.schema import verify_schema

# No raw month/year minute arrays cross the process/API boundary.
_QUERY = """
WITH bucketed AS (
    SELECT instrument,
           date_trunc(%s, open_time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS bucket,
           open_time,h,l,v
    FROM market.candles_1m
    WHERE instrument=%s AND open_time >= %s AND open_time < %s AND close_time <= %s
), grouped AS (
    SELECT instrument,bucket,count(*) AS n,min(open_time) AS first_time,
           max(open_time) AS last_time,max(h) AS high,min(l) AS low,sum(v) AS volume
    FROM bucketed GROUP BY instrument,bucket
)
SELECT g.bucket,g.n,g.first_time,g.last_time,f.o,g.high,g.low,z.c,g.volume
FROM grouped g
JOIN market.candles_1m f ON f.instrument=g.instrument AND f.open_time=g.first_time
JOIN market.candles_1m z ON z.instrument=g.instrument AND z.open_time=g.last_time
ORDER BY g.bucket
"""


def _ms(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise StorageError("calendar query returned an invalid UTC timestamp")
    delta = value - EPOCH
    result = (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000
    if delta.microseconds % 1000 or result % MINUTE_MS:
        raise StorageError("calendar query returned an unaligned timestamp")
    return result


def format_calendar(instrument, period, begin_ms, end_ms, closed_until_ms, rows):
    """Validate grouped DB values and retain explicit missing/partial period evidence."""
    groups = {}
    for row in rows:
        if len(row) != 9:
            raise StorageError("invalid calendar aggregation row")
        start, n, first, last = _ms(row[0]), row[1], _ms(row[2]), _ms(row[3])
        opening, ending = calendar_bounds(start, period)
        values = row[4:]
        if (
            start != opening
            or opening in groups
            or type(n) is not int
            or n <= 0
            or not max(begin_ms, opening) <= first <= last < min(end_ms, ending, closed_until_ms)
            or n > (last - first) // MINUTE_MS + 1
            or any(not isinstance(value, Decimal) or not value.is_finite() for value in values)
        ):
            raise StorageError("calendar query returned inconsistent observations")
        opening_price, high, low, closing_price, volume = values
        if (
            min(opening_price, high, low, closing_price) <= 0
            or low > min(opening_price, closing_price)
            or high < max(opening_price, closing_price)
            or volume < 0
        ):
            raise StorageError("calendar query returned invalid OHLCV")
        groups[opening] = (n, first, last, opening_price, high, low, closing_price, volume)
    candles, quality = [], []
    opening = calendar_bounds(begin_ms, period)[0]
    while opening < end_ms:
        _, ending = calendar_bounds(opening, period)
        start, end = max(begin_ms, opening), min(end_ms, ending)
        requested = (end - start) // MINUTE_MS
        closed = max(0, min(end, closed_until_ms) - start) // MINUTE_MS
        group = groups.get(opening)
        observed = group[0] if group else 0
        if observed > closed:
            raise StorageError("calendar source exceeds its closed range")
        expected = (ending - opening) // MINUTE_MS
        complete = (
            group is not None
            and ending <= closed_until_ms
            and observed == expected
            and group[1] == opening
            and group[2] == ending - MINUTE_MS
        )
        quality.append(
            asdict(
                CalendarBarQuality(
                    opening,
                    ending,
                    expected,
                    requested,
                    closed,
                    observed,
                    closed - observed,
                    begin_ms > opening,
                    end_ms < ending,
                    ending <= closed_until_ms,
                    complete,
                )
            )
        )
        if complete:
            candles.append(
                dict(
                    open_time_ms=opening,
                    **{
                        key: str(value)
                        for key, value in zip(
                            ("open", "high", "low", "close", "volume"), group[3:], strict=True
                        )
                    },
                )
            )
        opening = ending
    if any(key < quality[0]["start_ms"] or key >= end_ms for key in groups):
        raise StorageError("calendar query returned an out-of-range bucket")
    missing = sum(row["missing_minutes"] for row in quality)
    partial = sum(
        row["prefix_partial"] or row["suffix_partial"] or not row["is_closed"] for row in quality
    )
    return {
        "schema_version": 1,
        "instrument": instrument.key,
        "period": period,
        "source": "database",
        "begin_ms": begin_ms,
        "end_ms": end_ms,
        "closed_until_ms": closed_until_ms,
        "candles": candles,
        "quality": {
            "status": "gapped" if missing else "partial" if partial else "observed",
            "source_minutes": sum(row["observed_minutes"] for row in quality),
            "missing_minutes": missing,
            "partial_periods": partial,
            "bars": quality,
        },
    }


class CalendarCandleReader:
    def __init__(self, settings, *, connect=None):
        self.settings = (
            replace(settings, connect_timeout_s=min(settings.connect_timeout_s, 3))
            if isinstance(settings, DatabaseSettings)
            else settings
        )
        self.connect = connect or connect_database

    def read(
        self, instrument: Instrument, period: str, limit: int, *, end_ms: int, closed_until_ms: int
    ):
        try:
            begin, end = calendar_query_window(end_ms, period, limit)
            if (
                type(closed_until_ms) is not int
                or closed_until_ms % MINUTE_MS
                or end > closed_until_ms
            ):
                raise ValueError("calendar target is not yet closed")
        except ValueError as exc:
            raise ValidationError(str(exc)) from None
        connection = self.connect(self.settings)
        try:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                connection.execute("SET LOCAL statement_timeout = '2000ms'")
                verify_schema(connection)
                rows = connection.execute(
                    _QUERY,
                    (
                        "week" if period == "1w" else "month",
                        instrument.key,
                        EPOCH + timedelta(milliseconds=begin),
                        EPOCH + timedelta(milliseconds=end),
                        EPOCH + timedelta(milliseconds=closed_until_ms),
                    ),
                ).fetchmany(limit + 1)
                if len(rows) > limit:
                    raise StorageError("calendar result exceeds its bounded window")
                return format_calendar(instrument, period, begin, end, closed_until_ms, rows)
        except (StorageError, ValidationError):
            raise
        except Exception:
            raise StorageError("calendar database query unavailable") from None
        finally:
            connection.close()
