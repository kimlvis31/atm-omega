"""PostgreSQL/TimescaleDB minute storage; one connection per collection owner."""

# Imports
from datetime import UTC, datetime, timedelta

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Candle, Instrument
from omega.infrastructure.postgres.config import DatabaseSettings
from omega.infrastructure.postgres.schema import verify_schema
from omega.ports.collection import MinuteObservation

# Constants
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
CANDLE_COLUMNS = "open_time,close_time,o,h,l,c,v"


def connect_database(settings: DatabaseSettings):
    try:
        import psycopg
    except ImportError:
        raise StorageError("install the postgres extra before using the database") from None
    try:
        return psycopg.connect(
            host=settings.host,
            port=settings.port,
            dbname=settings.dbname,
            user=settings.user,
            password=settings.password,
            connect_timeout=settings.connect_timeout_s,
            autocommit=True,
            application_name="atm-omega",
            options="-c statement_timeout=5000 -c lock_timeout=2000 "
            "-c idle_in_transaction_session_timeout=10000",
        )
    except psycopg.Error:
        raise StorageError("database connection failed") from None


def candle_from_row(instrument: Instrument, row) -> Candle:
    times = []
    for value in row[:2]:
        delta = value - EPOCH
        times.append((delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000)
    return Candle(instrument, *times, *row[2:])


class PostgresCandleStore:
    def __init__(self, settings: DatabaseSettings):
        self.connection = connect_database(settings)
        import psycopg

        self._database_error = psycopg.Error
        try:
            verify_schema(self.connection)
        except (psycopg.Error, StorageError):
            self.connection.close()
            raise StorageError("database schema is not ready; run omega-db migrate") from None

    def get(self, instrument: Instrument, open_time_ms: int) -> Candle | None:
        try:
            row = self.connection.execute(
                f"SELECT {CANDLE_COLUMNS} FROM market.candles_1m "
                "WHERE instrument=%s AND open_time=%s",
                (instrument.key, EPOCH + timedelta(milliseconds=open_time_ms)),
            ).fetchone()
            return None if row is None else candle_from_row(instrument, row)
        except self._database_error:
            raise StorageError("database candle query failed") from None

    def latest(self, instrument: Instrument, limit: int = 500) -> tuple[Candle, ...]:
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValidationError("candle query limit must be 1..10000")
        try:
            rows = self.connection.execute(
                f"SELECT {CANDLE_COLUMNS} FROM market.candles_1m "
                "WHERE instrument=%s ORDER BY open_time DESC LIMIT %s",
                (instrument.key, limit),
            ).fetchall()
            return tuple(candle_from_row(instrument, row) for row in reversed(rows))
        except self._database_error:
            raise StorageError("database candle query failed") from None

    def latest_observations(self, instrument: Instrument, limit: int = 500):
        if type(limit) is not int or not 1 <= limit <= 10_000:
            raise ValidationError("candle query limit must be 1..10000")
        try:
            rows = self.connection.execute(
                f"SELECT {CANDLE_COLUMNS},source,received_ms,raw,raw_source,raw_received_ms "
                "FROM market.candles_1m "
                "WHERE instrument=%s ORDER BY open_time DESC LIMIT %s",
                (instrument.key, limit),
            ).fetchall()
            return tuple(
                MinuteObservation(candle_from_row(instrument, row[:7]), *row[7:])
                for row in reversed(rows)
            )
        except self._database_error:
            raise StorageError("database observation query failed") from None

    def put(self, observation: MinuteObservation) -> bool:
        # [1]: Observation Validation
        c = observation.candle
        if observation.source not in {"rest", "websocket"}:
            raise ValidationError("unknown market observation source")
        if (
            type(observation.received_at_ms) is not int
            or observation.received_at_ms < c.close_time_ms
        ):
            raise ValidationError("observation received before candle closed")
        if observation.raw is not None and (
            not isinstance(observation.raw, str) or len(observation.raw.encode("utf-8")) > 65536
        ):
            raise ValidationError("raw candle payload must be bounded text")
        raw_source, raw_received = observation.raw_source, observation.raw_received_at_ms
        if (raw_source is None) != (raw_received is None):
            raise ValidationError("raw reception metadata must be a complete pair")
        if observation.raw is None and raw_source is not None:
            raise ValidationError("raw reception metadata requires a payload")
        if raw_source is not None and (
            raw_source not in {"rest", "websocket"}
            or type(raw_received) is not int
            or raw_received < 0
        ):
            raise ValidationError("invalid raw reception metadata")
        if observation.raw is not None and raw_source is None:
            raw_source, raw_received = observation.source, observation.received_at_ms
        opened = EPOCH + timedelta(milliseconds=c.open_time_ms)
        closed = EPOCH + timedelta(milliseconds=c.close_time_ms)
        key = (c.instrument.key, opened)

        # [2]: Atomic Insert, Duplicate Verification & Optional Raw Capture
        try:
            with self.connection.transaction():
                inserted = self.connection.execute(
                    "INSERT INTO market.candles_1m "
                    "(instrument,open_time,close_time,o,h,l,c,v,source,received_ms,"
                    "raw,raw_source,raw_received_ms) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (instrument,open_time) DO NOTHING RETURNING instrument",
                    (
                        *key,
                        closed,
                        c.open,
                        c.high,
                        c.low,
                        c.close,
                        c.volume,
                        observation.source,
                        observation.received_at_ms,
                        observation.raw,
                        raw_source,
                        raw_received,
                    ),
                ).fetchone()
                if inserted is None:
                    row = self.connection.execute(
                        f"SELECT {CANDLE_COLUMNS} FROM market.candles_1m "
                        "WHERE instrument=%s AND open_time=%s FOR UPDATE",
                        key,
                    ).fetchone()
                    if row is None or candle_from_row(c.instrument, row) != c:
                        raise ProtocolError("conflicting closed minute; original data retained")
                    if observation.raw is not None:
                        self.connection.execute(
                            "UPDATE market.candles_1m SET raw=COALESCE(raw,%s), "
                            "raw_source=CASE WHEN raw IS NULL THEN %s ELSE raw_source END, "
                            "raw_received_ms=CASE WHEN raw IS NULL THEN %s "
                            "ELSE raw_received_ms END "
                            "WHERE instrument=%s AND open_time=%s",
                            (observation.raw, raw_source, raw_received, *key),
                        )
            # A failed/unknown commit raises; downstream publication cannot advance.
            return inserted is not None
        except self._database_error:
            raise StorageError(
                "database candle commit failed; retry with the same candle key"
            ) from None

    def close(self) -> None:
        self.connection.close()
