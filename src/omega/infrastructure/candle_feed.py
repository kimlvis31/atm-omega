"""Read-only committed Omega candles. No exchange client or ingestion writes."""

from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.infrastructure.candles import EPOCH, candle_from_row
from omega.infrastructure.market_data import instrument_key
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import verify_schema


class CommittedCandleSource:
    def __init__(self, root: Path, *, connect=None):
        self.root, self.connect = Path(root), connect

    def _connect(self):
        settings = load_database_settings(self.root / "database.config.toml")
        return self._connect_settings(settings)

    def _connect_settings(self, settings):
        """Allow a worker-owned adapter to bind a checked immutable destination."""
        if settings.host not in {"127.0.0.1", "::1", "localhost"}:
            raise ValidationError("paper feed database must use explicit local Omega configuration")
        settings = replace(
            settings, host="127.0.0.1" if settings.host == "localhost" else settings.host
        )
        if self.connect:
            return self.connect(settings)
        try:
            import psycopg

            return psycopg.connect(
                host=settings.host,
                port=settings.port,
                dbname=settings.dbname,
                user=settings.user,
                password=settings.password,
                connect_timeout=min(5, settings.connect_timeout_s),
                autocommit=True,
                application_name="omega-committed-paper-feed",
                options="-c default_transaction_read_only=on "
                "-c statement_timeout=5000 -c lock_timeout=2000",
            )
        except ImportError:
            raise StorageError("PostgreSQL support is not installed") from None
        except Exception:
            raise StorageError("paper feed database connection unavailable") from None

    def read(self, keys, start_ms, end_ms, *, limit=4096, as_of_ms=None):
        if (
            not keys
            or len(keys) > 100
            or len(set(keys)) != len(keys)
            or type(start_ms) is not int
            or type(end_ms) is not int
            or start_ms < 0
            or start_ms % 60000
            or end_ms % 60000
            or start_ms >= end_ms
            or type(limit) is not int
            or not 1 <= limit <= 4096
            or as_of_ms is not None
            and (type(as_of_ms) is not int or not 0 <= as_of_ms <= 253_402_300_799_000)
        ):
            raise ValidationError("invalid bounded committed candle query")
        instruments = {key: instrument_key(key) for key in keys}
        connection = self._connect()
        try:
            verify_schema(connection)
            cutoff = "" if as_of_ms is None else " AND received_ms<=%s"
            parameters = (
                list(keys),
                EPOCH + timedelta(milliseconds=start_ms),
                EPOCH + timedelta(milliseconds=end_ms),
            )
            if as_of_ms is not None:
                parameters += (as_of_ms,)
            rows = connection.execute(
                "SELECT instrument,open_time,close_time,o,h,l,c,v,source,received_ms "
                "FROM market.candles_1m WHERE instrument=ANY(%s) AND open_time>=%s "
                "AND open_time<%s" + cutoff + " ORDER BY open_time,instrument LIMIT %s",
                (*parameters, limit + 1),
            ).fetchall()
            if len(rows) > limit:
                raise StorageError("committed candle query exceeds bounded feed capacity")
            candles = []
            for key, *values in rows:
                if key not in instruments:
                    raise ProtocolError("feed query returned an unsubscribed instrument")
                candle = candle_from_row(instruments[key], values[:7])
                if (
                    not start_ms <= candle.open_time_ms < end_ms
                    or values[7] not in {"rest", "websocket"}
                    or type(values[8]) is not int
                    or values[8] < candle.close_time_ms
                    or as_of_ms is not None
                    and values[8] > as_of_ms
                ):
                    raise ProtocolError("feed requires a valid committed real observation")
                candles.append(candle)
            return tuple(candles)
        finally:
            connection.close()

    def close(self):
        # Connections are bounded per read; no thread-owned handle escapes a query.
        pass
