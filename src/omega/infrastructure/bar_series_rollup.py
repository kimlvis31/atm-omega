"""Complete, receipt-bounded OHLCV rollups of a collector base-bar series."""

from datetime import timedelta

from omega.domain.bar_series import bar_time
from omega.domain.errors import StorageError, ValidationError
from omega.infrastructure.candles import EPOCH

_QUERY = """
WITH bucketed AS (
    SELECT date_bin(%s,open_time,%s::timestamptz) AS bucket, open_time, o,h,l,c,v,received_ms
    FROM market.series_candles
    WHERE series_id=%s AND open_time >= %s AND open_time < %s AND received_ms <= %s
), grouped AS (
    SELECT bucket, count(*) AS n, min(open_time) AS first_time, max(open_time) AS last_time,
           (array_agg(o ORDER BY open_time))[1] AS opening, max(h) AS high, min(l) AS low,
           (array_agg(c ORDER BY open_time DESC))[1] AS closing, sum(v) AS volume,
           max(received_ms) AS received_ms
    FROM bucketed GROUP BY bucket
)
SELECT bucket,opening,high,low,closing,volume,received_ms FROM grouped
WHERE n=%s AND first_time=bucket AND last_time=bucket+%s
ORDER BY bucket
"""


def rollup_series(store, series, start_ms, end_ms, interval_ms, as_of_ms):
    """Return whole buckets in one known session, including explicit unavailable openings."""
    base = series.schedule.interval_ms
    if (
        not all(bar_time(value) for value in (start_ms, end_ms, interval_ms, as_of_ms))
        or not series.effective_from_ms <= start_ms < end_ms
        or not interval_ms
        or interval_ms % base
        or (end_ms - start_ms) % interval_ms
        or (end_ms - start_ms) // interval_ms > 128
        or (end_ms - start_ms) // base > 4096
    ):
        raise ValidationError("invalid bounds or size for a base-bar aggregation")
    sessions = series.schedule.sessions
    anchor = 0
    if sessions is not None:
        session = next((window for window in sessions if window[0] <= start_ms < window[1]), None)
        if session is None or end_ms > session[1]:
            raise ValidationError("base-bar aggregation must fit in one known trading session")
        anchor = session[0]
    if (start_ms - anchor) % interval_ms:
        raise ValidationError("aggregation does not align to the trading session")

    ready = max(start_ms, min(end_ms, as_of_ms - series.schedule.receipt_grace_ms))
    ready_end = start_ms + (ready - start_ms) // interval_ms * interval_ms
    import psycopg

    try:
        rows = (
            store.connection.execute(
                _QUERY,
                (
                    timedelta(milliseconds=interval_ms),
                    EPOCH + timedelta(milliseconds=anchor),
                    series.series_id,
                    EPOCH + timedelta(milliseconds=start_ms),
                    EPOCH + timedelta(milliseconds=ready_end),
                    as_of_ms,
                    interval_ms // base,
                    timedelta(milliseconds=interval_ms - base),
                ),
            ).fetchall()
            if ready_end > start_ms
            else ()
        )
    except psycopg.Error:
        raise StorageError("base-bar aggregation query failed") from None

    period = range(start_ms, ready_end, interval_ms)
    values = {}
    for bucket, *fields, received_at_ms in rows:
        opened = (bucket - EPOCH) // timedelta(milliseconds=1)
        values[opened] = {
            "open_ms": opened,
            "close_ms": opened + interval_ms,
            "received_at_ms": received_at_ms,
            **dict(zip(("open", "high", "low", "close", "volume"), map(str, fields), strict=True)),
        }
    return {
        "version": 2,
        "series_id": series.series_id,
        "instrument": series.instrument.key,
        "base_interval_ms": base,
        "interval_ms": interval_ms,
        "as_of_ms": as_of_ms,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "bars": list(values.values()),
        "missing_open_ms": [opened for opened in period if opened not in values],
        "pending_open_ms": list(range(ready_end, end_ms, interval_ms)),
    }
