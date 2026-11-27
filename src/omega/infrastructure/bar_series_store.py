"""Receipt-preserving PostgreSQL storage for immutable collector bar series."""

from datetime import timedelta

from omega.domain.bar_series import (
    BarSeries,
    SeriesCandle,
    SeriesObservation,
    bar_time,
    validate_series_id,
)
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.infrastructure.candles import (
    CANDLE_COLUMNS,
    EPOCH,
    DatabaseUnavailable,
    connect_database,
)
from omega.infrastructure.postgres.batching import COMMIT_BATCH_ROWS
from omega.infrastructure.postgres.config import DatabaseSettings
from omega.infrastructure.postgres.schema import verify_schema

OBSERVATION_COLUMNS = CANDLE_COLUMNS + ",source,received_ms,raw,raw_source,raw_received_ms"
MAX_SERIES_PAGE = 4096
BAR_WRITE_BATCH_SIZE = COMMIT_BATCH_ROWS


def series_observation(series, row) -> SeriesObservation:
    try:
        opened, closed = ((value - EPOCH) // timedelta(milliseconds=1) for value in row[:2])
        return SeriesObservation(SeriesCandle(series, opened, closed, *row[2:7]), *row[7:])
    except (TypeError, ValueError, OverflowError, ValidationError):
        raise ProtocolError("stored bar does not match its series contract") from None


class PostgresBarSeriesStore:
    def __init__(self, settings: DatabaseSettings, *, read_only: bool = False):
        self.connection = connect_database(settings)
        import psycopg

        self._database_error = psycopg.Error
        self._live_series: set[str] = set()
        try:
            if read_only:
                self.connection.execute("SET default_transaction_read_only=on")
            verify_schema(self.connection)
            row = self.connection.execute(
                "SELECT to_regclass('market.bar_series') IS NOT NULL, "
                "to_regclass('market.series_live_cursor') IS NOT NULL, EXISTS ("
                "SELECT 1 FROM timescaledb_information.hypertables "
                "WHERE hypertable_schema='market' AND hypertable_name='series_candles')"
            ).fetchone()
            if row != (True, True, True):
                raise StorageError("base-bar series schema is incomplete")
        except (psycopg.Error, StorageError) as error:
            failure = self._storage_error(
                error, "base-bar schema is not ready; run omega-db migrate"
            )
            self.connection.close()
            raise failure from None

    def _storage_error(self, error, message):
        state = getattr(error, "sqlstate", None) or ""
        if (
            self.connection.broken
            or state.startswith("08")
            or state
            in {
                "40001",  # Serialization failure
                "40P01",  # Deadlock
                "55P03",  # Lock timeout
                "57P01",  # Server terminated the session
                "57P02",  # Server shut down after a crash
                "57P03",  # Server is starting
            }
        ):
            return DatabaseUnavailable("base-bar database temporarily unavailable")
        return StorageError(message)

    def register(self, series: BarSeries) -> bool:
        if not isinstance(series, BarSeries):
            raise ValidationError("typed base-bar series required")
        from psycopg.types.json import Jsonb

        definition = series.document()
        try:
            with self.connection.transaction():
                row = self.connection.execute(
                    "INSERT INTO market.bar_series(series_id,definition) VALUES (%s,%s) "
                    "ON CONFLICT (series_id) DO NOTHING RETURNING series_id",
                    (series.series_id, Jsonb(definition)),
                ).fetchone()
                if row is None:
                    existing = self.connection.execute(
                        "SELECT definition FROM market.bar_series WHERE series_id=%s",
                        (series.series_id,),
                    ).fetchone()
                    if existing != (definition,):
                        raise ProtocolError("conflicting bar-series definition")
            return row is not None
        except self._database_error as error:
            raise self._storage_error(error, "bar-series registration failed") from None

    def load(self, series_id: str) -> BarSeries:
        validate_series_id(series_id)
        try:
            row = self.connection.execute(
                "SELECT definition FROM market.bar_series WHERE series_id=%s", (series_id,)
            ).fetchone()
        except self._database_error as error:
            raise self._storage_error(error, "bar-series metadata query failed") from None
        if row is None:
            raise LookupError("bar series not found")
        try:
            series = BarSeries.from_document(row[0])
        except ValidationError:
            raise ProtocolError("stored bar-series definition is invalid") from None
        if series.series_id != series_id:
            raise ProtocolError("stored bar-series metadata hash differs from its identity")
        return series

    def put(self, observation: SeriesObservation) -> bool:
        if not isinstance(observation, SeriesObservation):
            raise ValidationError("typed base-bar observation required")
        candle = observation.candle
        series = candle.series
        if self.load(series.series_id) != series:
            raise ProtocolError("bar does not match the registered series metadata")
        try:
            with self.connection.transaction():
                return self._insert_batch((observation,))[0][0]
        except self._database_error:
            raise StorageError("bar commit failed; retry with the same series and time") from None

    def import_batch(self, series: BarSeries, observations: tuple[SeriesObservation, ...]) -> int:
        if (
            not isinstance(series, BarSeries)
            or type(observations) is not tuple
            or not 1 <= len(observations) <= BAR_WRITE_BATCH_SIZE
            or any(
                not isinstance(row, SeriesObservation) or row.candle.series != series
                for row in observations
            )
            or any(
                left.candle.open_time_ms >= right.candle.open_time_ms
                for left, right in zip(observations, observations[1:], strict=False)
            )
        ):
            raise ValidationError(
                f"import requires 1..{BAR_WRITE_BATCH_SIZE} ordered observations of one series"
            )
        try:
            with self.connection.transaction():
                self.register(series)
                return sum(inserted for inserted, _ in self._insert_batch(observations))
        except self._database_error:
            raise StorageError("base-bar batch failed; retry the same ordered batch") from None

    def claim_live_series(self, series: BarSeries, now_ms: int) -> int:
        """Own one series writer and recover its first due position from committed bars."""
        if (
            not isinstance(series, BarSeries)
            or not bar_time(now_ms)
            or now_ms < series.effective_from_ms
            or series.series_id in self._live_series
        ):
            raise ValidationError("invalid live series, start boundary or duplicate writer claim")
        try:
            claimed = self.connection.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 201926501))",
                (series.series_id,),
            ).fetchone()[0]
            if not claimed:
                raise StorageError("base-bar series already has a live writer")
            self.register(series)
            self.connection.execute(
                "INSERT INTO market.series_live_cursor(series_id,next_ms) VALUES (%s,%s) "
                "ON CONFLICT (series_id) DO NOTHING",
                (series.series_id, series.effective_from_ms),
            )
            self._live_series.add(series.series_id)
            while True:
                with self.connection.transaction():
                    owned = {series.series_id: series}
                    cursors = self._lock_live_cursors(owned)
                    ends = self._advance_committed_prefixes(
                        owned, cursors, {series.series_id: now_ms}
                    )
                    start, end = cursors[series.series_id], ends[series.series_id]
                if (
                    end == start
                    or series.schedule.contiguous_missing_range(end, now_ms, chunk=1) is None
                ):
                    return end
        except self._database_error as error:
            raise self._storage_error(error, "live base-bar resume query failed") from None

    def put_live_observation(self, observation: SeriesObservation) -> bool:
        return self.put_live_batch((observation,))[0][0]

    def put_live_batch(self, observations, *, missing_only=False):
        if not 1 <= len(observations) <= BAR_WRITE_BATCH_SIZE or any(
            not isinstance(row, SeriesObservation) for row in observations
        ):
            raise ValidationError("live write requires a bounded batch of typed bars")
        series_by_id, received_by_id = {}, {}
        for row in observations:
            series = row.candle.series
            series_id = series.series_id
            if series_id not in self._live_series:
                raise StorageError("this connection does not own the live base-bar series")
            series_by_id[series_id] = series
            received_by_id[series_id] = max(received_by_id.get(series_id, 0), row.received_at_ms)
        try:
            with self.connection.transaction():
                cursors = self._lock_live_cursors(series_by_id)
                result = self._insert_batch(observations, missing_only=missing_only)
                self._advance_committed_prefixes(series_by_id, cursors, received_by_id)
            return result
        except self._database_error as error:
            raise self._storage_error(error, "live base-bar batch commit failed") from None

    def _lock_live_cursors(self, series_by_id):
        rows = self.connection.execute(
            "SELECT series_id,next_ms FROM market.series_live_cursor "
            "WHERE series_id=ANY(%s::text[]) ORDER BY series_id FOR UPDATE",
            (list(series_by_id),),
        ).fetchall()
        cursors = dict(rows)
        if cursors.keys() != series_by_id.keys() or any(
            not bar_time(cursor)
            or not (
                series_by_id[series_id].schedule.contains_open(cursor)
                or series_by_id[series_id].schedule.is_terminal_cursor(cursor)
            )
            for series_id, cursor in rows
        ):
            raise ProtocolError("live base-bar cursor is missing or invalid")
        return cursors

    def _advance_committed_prefixes(self, series_by_id, cursors, received_by_id):
        # Bound each series scan even if a large amount of future history is already staged.
        windows = []
        for series_id, cursor in cursors.items():
            as_of_ms = received_by_id[series_id]
            window = series_by_id[series_id].schedule.contiguous_missing_range(
                cursor, as_of_ms, chunk=BAR_WRITE_BATCH_SIZE
            )
            if window is not None:
                lower, upper = (EPOCH + timedelta(milliseconds=value) for value in window)
                windows.append((series_id, lower, upper, as_of_ms))
        positions = dict(cursors)
        if not windows:
            return positions

        rows = self.connection.execute(
            """
            SELECT target.series_id,bar.open_time FROM unnest(
                %s::text[], %s::timestamptz[], %s::timestamptz[], %s::bigint[]
            ) AS target(series_id,low,high,as_of_ms)
            CROSS JOIN LATERAL (
                SELECT open_time FROM market.series_candles
                WHERE series_id=target.series_id AND open_time>=target.low AND open_time<target.high
                  AND received_ms<=target.as_of_ms
                ORDER BY open_time LIMIT %s
            ) AS bar
            ORDER BY target.series_id,bar.open_time
            """,
            (*map(list, zip(*windows, strict=True)), BAR_WRITE_BATCH_SIZE),
        ).fetchall()
        for series_id, opened in rows:
            cursor = positions[series_id]
            if opened == EPOCH + timedelta(milliseconds=cursor):
                schedule = series_by_id[series_id].schedule
                positions[series_id] = (
                    schedule.next_expected(cursor) or cursor + schedule.interval_ms
                )

        updates = [
            (series_id, stamp)
            for series_id, stamp in positions.items()
            if stamp != cursors[series_id]
        ]
        if updates:
            self.connection.execute(
                """
                UPDATE market.series_live_cursor AS cursor SET next_ms=batch.next_ms
                FROM unnest(%s::text[], %s::bigint[]) AS batch(series_id,next_ms)
                WHERE cursor.series_id=batch.series_id
                """,
                tuple(map(list, zip(*updates, strict=True))),
            )
        return positions

    @staticmethod
    def _insert_values(observation):
        candle = observation.candle
        return (
            candle.series.series_id,
            EPOCH + timedelta(milliseconds=candle.open_time_ms),
            EPOCH + timedelta(milliseconds=candle.close_time_ms),
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
            observation.source,
            observation.received_at_ms,
            observation.raw,
            observation.raw_source,
            observation.raw_received_at_ms,
        )

    def _insert_batch(self, observations, *, missing_only=False):
        values = [self._insert_values(row) for row in observations]
        # Group identical keys before INSERT. Keep the first transport receipt.
        first_by_key = {}
        for row in values:
            first_by_key.setdefault(row[:2], row)
        ordered_rows = [first_by_key[key] for key in sorted(first_by_key)]
        inserted = set(
            self.connection.execute(
                f"""
                INSERT INTO market.series_candles (series_id,{OBSERVATION_COLUMNS})
                SELECT * FROM unnest(
                    %s::text[], %s::timestamptz[], %s::timestamptz[],
                    %s::numeric[], %s::numeric[], %s::numeric[], %s::numeric[], %s::numeric[],
                    %s::text[], %s::bigint[], %s::bytea[], %s::text[], %s::bigint[]
                )
                ON CONFLICT (series_id,open_time) DO NOTHING RETURNING series_id,open_time
                """,
                tuple(map(list, zip(*ordered_rows, strict=True))),
            ).fetchall()
        )
        existing = {}
        conflicts = sorted(first_by_key.keys() - inserted)
        if conflicts:
            # A fresh statement sees concurrent commits that caused ON CONFLICT to skip.
            columns = ",".join(f"bar.{name}" for name in OBSERVATION_COLUMNS.split(","))
            rows = self.connection.execute(
                f"""
                SELECT bar.series_id,{columns}
                FROM unnest(%s::text[], %s::timestamptz[]) AS keys(series_id,open_time)
                JOIN market.series_candles AS bar USING (series_id,open_time)
                ORDER BY bar.series_id,bar.open_time FOR UPDATE OF bar
                """,
                tuple(map(list, zip(*conflicts, strict=True))),
            ).fetchall()
            existing = {row[:2]: row[1:] for row in rows}
            if len(existing) != len(conflicts):
                raise ProtocolError("existing bar disappeared during its commit")
        result, committed = [], {}
        for observation, row in zip(observations, values, strict=True):
            key = row[:2]
            is_new = key in inserted and key not in committed
            if is_new:
                committed[key] = observation.candle
            elif key not in committed:
                committed[key] = series_observation(observation.candle.series, existing[key]).candle
            accepted = committed[key]
            if accepted != observation.candle and not missing_only:
                raise ProtocolError("conflicting base-bar observation; original retained")
            result.append((is_new, accepted))
        return result

    def read_observations(
        self,
        series_id: str,
        start_ms: int,
        end_ms: int,
        *,
        as_of_ms: int,
        limit: int = 256,
        include_raw: bool = True,
    ) -> tuple[SeriesObservation, ...]:
        if (
            not all(bar_time(value) for value in (start_ms, end_ms, as_of_ms))
            or start_ms >= end_ms
            or type(include_raw) is not bool
            or type(limit) is not int
            or not 1 <= limit <= MAX_SERIES_PAGE
        ):
            raise ValidationError("invalid bounded base-bar query")
        series = self.load(series_id)
        columns = OBSERVATION_COLUMNS if include_raw else CANDLE_COLUMNS + ",source,received_ms"
        try:
            rows = self.connection.execute(
                f"SELECT {columns} FROM market.series_candles "
                "WHERE series_id=%s AND open_time>=%s AND open_time<%s AND received_ms<=%s "
                "ORDER BY open_time LIMIT %s",
                (
                    series_id,
                    EPOCH + timedelta(milliseconds=start_ms),
                    EPOCH + timedelta(milliseconds=end_ms),
                    as_of_ms,
                    limit + 1,
                ),
            ).fetchall()
        except self._database_error as error:
            raise self._storage_error(error, "base-bar history query failed") from None
        if len(rows) > limit:
            raise StorageError("base-bar query exceeds bounded page capacity")
        return tuple(series_observation(series, row) for row in rows)

    def close(self) -> None:
        self.connection.close()
