"""Bounded verification and transfer of v1 history into a staged 1m series."""

from datetime import timedelta

from omega.domain.bar_series import BarSeries, bar_time
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.infrastructure.bar_series_store import MAX_SERIES_PAGE, OBSERVATION_COLUMNS
from omega.infrastructure.candles import EPOCH

LEGACY_FEED = "legacy-1m"
MINUTE_MS = 60_000


def transfer_legacy_window(
    store, series: BarSeries, start_ms: int, end_ms: int, *, verify_only: bool = False
) -> dict:
    """Copy or compare the entire window in a consistent database snapshot."""
    if (
        not isinstance(series, BarSeries)
        or series.feed != LEGACY_FEED
        or series.native_intervals_ms != (MINUTE_MS,)
        or series.schedule.sessions is not None
        or not all(bar_time(value) and value % MINUTE_MS == 0 for value in (start_ms, end_ms))
        or not series.effective_from_ms <= start_ms < end_ms
        or end_ms - start_ms > MAX_SERIES_PAGE * MINUTE_MS
        or type(verify_only) is not bool
    ):
        raise ValidationError("legacy transfer requires a staged 1m series and an aligned window")
    window = tuple(EPOCH + timedelta(milliseconds=value) for value in (start_ms, end_ms))
    original = (series.instrument.key, *window)
    staged = (series.series_id, *window)
    within = "open_time >= %s AND open_time < %s"
    original_query = (
        f"SELECT {OBSERVATION_COLUMNS} FROM market.candles_1m WHERE instrument=%s AND {within}"
    )
    staged_query = (
        f"SELECT {OBSERVATION_COLUMNS} FROM market.series_candles WHERE series_id=%s AND {within}"
    )
    connection = store.connection
    import psycopg

    try:
        with connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            if verify_only:
                if store.load(series.series_id) != series:
                    raise ProtocolError("staged base-bar definition changed")
            else:
                store.register(series)
                # The legacy store can attach raw data after its canonical observation arrived.
                # A later transfer can fill that same empty field. The comparison below rolls
                # back all changes if any nonempty original field differs.
                connection.execute(
                    "INSERT INTO market.series_candles AS target "
                    f"(series_id,{OBSERVATION_COLUMNS}) "
                    f"SELECT %s, legacy.* FROM ({original_query}) AS legacy ORDER BY open_time "
                    "ON CONFLICT (series_id,open_time) DO UPDATE SET "
                    "raw=EXCLUDED.raw,raw_source=EXCLUDED.raw_source,"
                    "raw_received_ms=EXCLUDED.raw_received_ms "
                    "WHERE target.raw IS NULL AND EXCLUDED.raw IS NOT NULL",
                    (series.series_id, *original),
                )
            source_count, mismatch = connection.execute(
                f"WITH original AS ({original_query}), staged AS ({staged_query}) "
                "SELECT (SELECT COUNT(*) FROM original), EXISTS ("
                "(SELECT * FROM original EXCEPT ALL SELECT * FROM staged) UNION ALL "
                "(SELECT * FROM staged EXCEPT ALL SELECT * FROM original))",
                (*original, *staged),
            ).fetchone()
            if mismatch:
                raise ProtocolError("staged bars differ from the current legacy snapshot")
    except psycopg.Error:
        raise StorageError("legacy transfer failed; retry the same bounded window") from None
    return {
        "series_id": series.series_id,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "verified_rows": source_count,
    }


def check_base_bar_transition(store, previous_id: str, target_id: str, as_of_ms: int) -> dict:
    """Verify the handoff between two immutable series in one read-only snapshot."""
    if not bar_time(as_of_ms):
        raise ValidationError("transition requires a bounded UTC as-of timestamp")
    import psycopg

    try:
        with store.connection.transaction():
            store.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            previous = store.load(previous_id)
            target = store.load(target_id)
            boundary = target.effective_from_ms
            if previous.instrument != target.instrument or previous.effective_from_ms >= boundary:
                raise ValidationError(
                    "transition must move one instrument to a later base-bar series"
                )
            previous_open = _previous_bar_at_boundary(previous.schedule, boundary)
            if previous_open is None or previous_open < previous.effective_from_ms:
                raise ValidationError(
                    "the previous series has no verified calendar at the boundary"
                )
            previous_end = previous_open + previous.schedule.interval_ms
            target_end = boundary + target.schedule.interval_ms
            if not bar_time(target_end):
                raise ValidationError("the target bar falls beyond the supported UTC range")
            preceding, following = (
                bool(
                    store.read_observations(
                        series_id, start, end, as_of_ms=as_of_ms, limit=1, include_raw=False
                    )
                )
                for series_id, start, end in (
                    (previous_id, previous_open, previous_end),
                    (target_id, boundary, target_end),
                )
            )
    except psycopg.Error:
        raise StorageError("base-bar transition could not read a consistent snapshot") from None
    return {
        "instrument": target.instrument.key,
        "previous_series_id": previous_id,
        "target_series_id": target_id,
        "previous_interval_ms": previous.schedule.interval_ms,
        "target_interval_ms": target.schedule.interval_ms,
        "previous_open_ms": previous_open,
        "target_open_ms": boundary,
        "as_of_ms": as_of_ms,
        "previous_bar_available": preceding,
        "target_bar_available": following,
        "verified": preceding and following,
    }


def _previous_bar_at_boundary(schedule, boundary: int) -> int | None:
    if schedule.sessions is None:
        start, end = 0, boundary
    else:
        if not schedule.coverage_known(boundary):
            return None
        for start, close in reversed(schedule.sessions):
            if start < boundary:
                end = min(boundary, close)
                break
        else:
            return None
    interval = schedule.interval_ms
    if (end - start) % interval:
        raise ValidationError("requested transition would split a bar in the previous series")
    return end - interval if end - interval >= start else None
