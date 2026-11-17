"""Read-only one-statement snapshots of actual committed Omega candle rows."""

from datetime import timedelta

from omega.domain.errors import StorageError, ValidationError
from omega.domain.inference import timestamp
from omega.domain.models import Instrument
from omega.infrastructure.candles import EPOCH, PostgresCandleStore


def validate_window(instrument, start_ms, end_ms):
    if not isinstance(instrument, Instrument):
        raise ValidationError("typed inference instrument required")
    timestamp(start_ms)
    timestamp(end_ms)
    if start_ms % 60000 or end_ms % 60000 or not start_ms < end_ms <= start_ms + 4096 * 60000:
        raise ValidationError("inference market window must contain 1..4096 aligned minutes")
    return (end_ms - start_ms) // 60000


def select_committed_rows(connection, instrument, start_ms, end_ms, count):
    """One original SELECT on an owned connection; caller validates the window."""
    if (
        type(count) is not int
        or not 1 <= count <= 4096
        or type(start_ms) is not int
        or type(end_ms) is not int
        or start_ms < 0
        or start_ms % 60000
        or end_ms % 60000
        or end_ms - start_ms != count * 60000
    ):
        raise ValidationError("committed row count must match an aligned bounded window")
    return connection.execute(
        "SELECT instrument,open_time,close_time,o,h,l,c,v,source,received_ms "
        "FROM market.candles_1m WHERE instrument=%s AND open_time>=%s "
        "AND open_time<%s ORDER BY open_time LIMIT %s",
        (
            instrument.key,
            EPOCH + timedelta(milliseconds=start_ms),
            EPOCH + timedelta(milliseconds=end_ms),
            count + 1,
        ),
    ).fetchall()


def read_committed_window(settings, instrument, start_ms, end_ms, *, store_factory=None):
    """No repair/export/MC path; a single SELECT sees one committed MVCC snapshot.

    PostgresCandleStore verifies the operational schema. Its fresh connection is
    made read-only before the data query and closed before any result escapes.
    Missing rows remain missing. No SQL/configuration exception text is exposed.
    """
    count = validate_window(instrument, start_ms, end_ms)
    store = None
    try:
        store = (store_factory or PostgresCandleStore)(settings)
        connection = store.connection
        connection.execute("SET default_transaction_read_only=on")
        return tuple(select_committed_rows(connection, instrument, start_ms, end_ms, count))
    except Exception:
        raise StorageError("committed inference market query unavailable") from None
    finally:
        if store is not None:
            try:
                store.close()
            except Exception:
                raise StorageError(
                    "committed inference market connection cleanup unconfirmed"
                ) from None
