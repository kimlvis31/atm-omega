"""Read collector coverage without scanning stored market observations."""

from collections import defaultdict
from datetime import timedelta
from heapq import merge
from itertools import islice

from omega.infrastructure.candles import EPOCH

MINUTE = 60_000
DAY = 86_400_000


def first_covered(db, scope, channel, datasets):
    """First stored slot; all requested datasets must have been seen."""
    values = db.execute(
        """SELECT first.opening
           FROM unnest(%s::text[]) AS requested(dataset)
           CROSS JOIN LATERAL (
               SELECT lower(range_merge(observed+incomplete)) AS opening
               FROM market.collector_coverage
               WHERE scope_key=%s AND channel=%s AND dataset=requested.dataset
               ORDER BY day_ms LIMIT 1
           ) AS first""",
        (list(datasets), scope, channel),
    ).fetchall()
    if len(values) == len(datasets):
        return max((row[0] for row in values), default=None)
    return None


def note_source_check(db, instrument, channel, source, start, end, result, now):
    db.execute(
        """INSERT INTO market.collector_history_checks
               (instrument,channel,source,start_ms,end_ms,result,checked_ms)
           VALUES (%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (instrument,channel,source,start_ms,end_ms) DO UPDATE SET
               result=EXCLUDED.result,checked_ms=EXCLUDED.checked_ms
           WHERE market.collector_history_checks.checked_ms<=EXCLUDED.checked_ms""",
        (instrument.key, channel, source, start, end, result, now),
    )


def clear_unlisted_check(db, instrument, channel, start, end):
    db.execute(
        """DELETE FROM market.collector_history_checks
           WHERE instrument=%s AND channel=%s AND source='vision'
             AND start_ms=%s AND end_ms=%s AND result='unlisted'""",
        (instrument.key, channel, start, end),
    )


def recent_unconfirmed_trades(db, by_series, now, retry, *, limit=60):
    """Intersect saved candles with unverified trade slots before reading candle volumes."""
    low = (max(0, now - DAY) + MINUTE - 1) // MINUTE * MINUTE
    high = (max(0, now - 3 * MINUTE) + MINUTE - 1) // MINUTE * MINUTE
    if low >= high:
        return []
    ranges = db.execute(
        """SELECT pair.series_id,
                  (bars.observed-COALESCE(trades.observed,'{}'::int8multirange))
                  * int8multirange(int8range(%s,%s,'[)'))
           FROM unnest(%s::text[],%s::text[]) AS pair(series_id,instrument)
           JOIN market.collector_coverage AS bars
             ON bars.scope_key=pair.series_id AND bars.channel='kline' AND bars.dataset=''
           LEFT JOIN market.collector_coverage AS trades
             ON trades.scope_key=pair.instrument AND trades.channel='trade_flow'
            AND trades.dataset=pair.instrument||':aggTrade' AND trades.day_ms=bars.day_ms
           WHERE bars.day_ms BETWEEN %s AND %s
           ORDER BY bars.day_ms DESC""",
        (
            low,
            high,
            list(by_series),
            [i.key for i in by_series.values()],
            low // DAY * DAY,
            (high - 1) // DAY * DAY,
        ),
    ).fetchall()

    per_series = defaultdict(list)
    for series_id, incomplete in ranges:
        per_series[series_id].extend(reversed(incomplete))

    def openings(series_id, intervals):
        instrument = by_series[series_id]
        for interval in intervals:
            for opening in range(interval.upper - MINUTE, interval.lower - 1, -MINUTE):
                if retry.get((instrument.key, opening), 0) <= now:
                    yield opening, series_id

    series_order = {series_id: index for index, series_id in enumerate(sorted(by_series))}
    selected = list(
        islice(
            merge(
                *(openings(series_id, gaps) for series_id, gaps in per_series.items()),
                key=lambda slot: (slot[0], -series_order[slot[1]]),
                reverse=True,
            ),
            limit,
        )
    )
    if not selected:
        return []
    times = [EPOCH + timedelta(milliseconds=opening) for opening, _ in selected]
    return db.execute(
        """SELECT bar.series_id,(extract(epoch FROM bar.open_time)*1000)::bigint,bar.v
           FROM unnest(%s::text[],%s::timestamptz[]) AS requested(series_id,open_time)
           JOIN market.series_candles AS bar USING (series_id,open_time)
           ORDER BY bar.open_time DESC,bar.series_id""",
        ([series_id for _, series_id in selected], times),
    ).fetchall()
