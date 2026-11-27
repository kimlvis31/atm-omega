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
from omega.infrastructure.postgres.config import DatabaseSettings
from omega.infrastructure.postgres.schema import verify_schema

OBSERVATION_COLUMNS = CANDLE_COLUMNS + ",source,received_ms,raw,raw_source,raw_received_ms"
MAX_SERIES_PAGE = 4096


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
                return self._insert(observation)
        except self._database_error:
            raise StorageError("bar commit failed; retry with the same series and time") from None

    def import_batch(self, series: BarSeries, observations: tuple[SeriesObservation, ...]) -> int:
        if (
            not isinstance(series, BarSeries)
            or type(observations) is not tuple
            or not 1 <= len(observations) <= 256
            or any(
                not isinstance(row, SeriesObservation) or row.candle.series != series
                for row in observations
            )
            or any(
                left.candle.open_time_ms >= right.candle.open_time_ms
                for left, right in zip(observations, observations[1:], strict=False)
            )
        ):
            raise ValidationError("import requires 1..256 ordered observations of one series")
        try:
            with self.connection.transaction():
                self.register(series)
                return sum(self._insert(row) for row in observations)
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
                    start = self._lock_live_cursor(series)
                    end = self._advance_committed_prefix(series, start, now_ms)
                if (
                    end == start
                    or series.schedule.contiguous_missing_range(end, now_ms, chunk=1) is None
                ):
                    return end
        except self._database_error as error:
            raise self._storage_error(error, "live base-bar resume query failed") from None

    def put_live_observation(self, observation: SeriesObservation) -> bool:
        if not isinstance(observation, SeriesObservation):
            raise ValidationError("typed base-bar observation required")
        series = observation.candle.series
        if series.series_id not in self._live_series:
            raise StorageError("this connection does not own the live base-bar series")
        try:
            with self.connection.transaction():
                cursor = self._lock_live_cursor(series)
                inserted = self._insert(observation)
                self._advance_committed_prefix(series, cursor, observation.received_at_ms)
                return inserted
        except self._database_error as error:
            raise self._storage_error(error, "live base-bar commit failed") from None

    def _lock_live_cursor(self, series: BarSeries) -> int:
        row = self.connection.execute(
            "SELECT next_ms FROM market.series_live_cursor WHERE series_id=%s FOR UPDATE",
            (series.series_id,),
        ).fetchone()
        if (
            row is None
            or row[0] < series.effective_from_ms
            or not (
                series.schedule.contains_open(row[0]) or series.schedule.is_terminal_cursor(row[0])
            )
        ):
            raise ProtocolError("live base-bar cursor is missing or invalid")
        return row[0]

    def _advance_committed_prefix(self, series: BarSeries, cursor: int, as_of_ms: int) -> int:
        # Limit each scan and transaction, including when a staged history is already present.
        window = series.schedule.contiguous_missing_range(cursor, as_of_ms, chunk=256)
        if window is None:
            return cursor
        rows = self.connection.execute(
            "SELECT open_time FROM market.series_candles "
            "WHERE series_id=%s AND open_time >= %s AND open_time < %s AND received_ms <= %s "
            "ORDER BY open_time LIMIT 256",
            (
                series.series_id,
                *(EPOCH + timedelta(milliseconds=value) for value in window),
                as_of_ms,
            ),
        )
        next_ms = cursor
        for (opened,) in rows:
            if opened != EPOCH + timedelta(milliseconds=next_ms):
                break
            next_ms = (
                series.schedule.next_expected(next_ms) or next_ms + series.schedule.interval_ms
            )
        if next_ms != cursor:
            self.connection.execute(
                "UPDATE market.series_live_cursor SET next_ms=%s WHERE series_id=%s",
                (next_ms, series.series_id),
            )
        return next_ms

    def _insert(self, observation: SeriesObservation) -> bool:
        candle = observation.candle
        opened = EPOCH + timedelta(milliseconds=candle.open_time_ms)
        key = (candle.series.series_id, opened)
        inserted = self.connection.execute(
            "INSERT INTO market.series_candles "
            "(series_id," + OBSERVATION_COLUMNS + ") "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (series_id,open_time) DO NOTHING RETURNING series_id",
            (
                *key,
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
            ),
        ).fetchone()
        if inserted is not None:
            return True
        row = self.connection.execute(
            f"SELECT {OBSERVATION_COLUMNS} FROM market.series_candles "
            "WHERE series_id=%s AND open_time=%s FOR UPDATE",
            key,
        ).fetchone()
        if row is None:
            raise ProtocolError("existing bar disappeared during its commit")
        previous = series_observation(candle.series, row)
        if previous.candle != candle:
            raise ProtocolError("conflicting base-bar observation; original retained")
        return False

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
