"""Opt-in durability checks on omega_test at 127.0.0.1:55433, never an operational DB."""

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.application.collection import MinuteCollector
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.market_data import MarketDataBackend
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import migrate
from omega.ports.collection import MinuteObservation

pytestmark = pytest.mark.database


@pytest.fixture(scope="module")
def settings():
    path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not path:
        pytest.fail("OMEGA_TEST_DB_CONFIG must explicitly select the isolated test DB")
    value = load_database_settings(Path(path))
    if (value.host, value.port, value.dbname, value.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing DB tests outside the designated local test database")
    with connect_database(value) as connection:
        assert connection.execute("SELECT current_database()").fetchone()[0] == "omega_test"
        migrate(connection)
    return value


@pytest.fixture
def instrument():
    return Instrument("binance", Market.USDM, "TEST_" + uuid4().hex.upper())


@pytest.fixture
def store(settings):
    value = PostgresCandleStore(settings)
    yield value
    value.close()


def observation(instrument, price="100", raw=None, source="rest"):
    timestamp = 1_700_000_040_000
    p = decimal(price)
    candle = Candle(
        instrument, timestamp, timestamp + 60000, p, p, p, p, decimal("0.123456789012345678901")
    )
    return MinuteObservation(candle, source, timestamp + 60002, raw)


def test_exact_values_raw_duplicate_reconnect_and_market_identity(store, instrument, settings):
    original = observation(instrument, "100.123456789012345678901")
    assert store.put(original)
    assert not store.put(
        observation(instrument, str(original.candle.close), '{"original":true}', "websocket")
    )
    spot = Instrument(instrument.provider, Market.SPOT, instrument.symbol)
    assert store.put(observation(spot, "77"))
    with connect_database(settings) as connection:
        raw = connection.execute(
            "SELECT raw FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
        ).fetchone()[0]
        assert raw == '{"original":true}'
    reopened = PostgresCandleStore(settings)
    try:
        assert reopened.get(instrument, original.candle.open_time_ms) == original.candle
        assert reopened.get(spot, original.candle.open_time_ms).close == decimal("77")
    finally:
        reopened.close()


def test_conflict_rolls_back_and_original_remains_immutable(store, instrument):
    original = observation(instrument)
    store.put(original)
    with pytest.raises(ProtocolError, match="conflicting"):
        store.put(observation(instrument, "99", '{"must_not_replace":true}'))
    assert store.get(instrument, original.candle.open_time_ms) == original.candle
    assert not store.put(observation(instrument, raw='{"late":true}', source="websocket"))


def test_commit_is_visible_to_another_connection_before_publication(store, instrument, settings):
    original = observation(instrument)
    reader = PostgresCandleStore(settings)
    received = []

    def publish(candle):
        assert reader.get(instrument, candle.open_time_ms) == candle
        received.append(candle)

    try:
        collector = MinuteCollector({instrument: original.candle.open_time_ms}, store, publish)
        assert collector.accept(original, now_ms=original.received_at_ms) == 1
        assert received == [original.candle]
    finally:
        reader.close()


def test_sql_failure_rolls_back_and_never_advances_delivery(store, instrument):
    original_connection = store.connection

    class FailingConnection:
        def transaction(self):
            return original_connection.transaction()

        def execute(self, sql, params=None):
            result = original_connection.execute(sql, params)
            if sql.startswith("INSERT INTO market"):
                original_connection.execute("SELECT 1/0")
            return result

    original = observation(instrument)
    received = []
    collector = MinuteCollector({instrument: original.candle.open_time_ms}, store, received.append)
    store.connection = FailingConnection()
    try:
        with pytest.raises(StorageError, match="commit failed"):
            collector.accept(original, now_ms=original.received_at_ms)
    finally:
        store.connection = original_connection
    assert received == []
    assert collector.cursor[instrument] == original.candle.open_time_ms
    assert store.get(instrument, original.candle.open_time_ms) is None
    assert collector.accept(original, now_ms=original.received_at_ms) == 1


def test_commit_acknowledgement_loss_is_retryable_without_duplicate_delivery(store, instrument):
    import psycopg

    original_connection = store.connection

    class LostAcknowledgement:
        @contextmanager
        def transaction(self):
            with original_connection.transaction():
                yield
            raise psycopg.OperationalError("commit acknowledged by server but reply was lost")

        def execute(self, *args, **kwargs):
            return original_connection.execute(*args, **kwargs)

    original = observation(instrument)
    received = []
    collector = MinuteCollector({instrument: original.candle.open_time_ms}, store, received.append)
    store.connection = LostAcknowledgement()
    try:
        with pytest.raises(StorageError):
            collector.accept(original, now_ms=original.received_at_ms)
    finally:
        store.connection = original_connection
    assert received == [] and collector.cursor[instrument] == original.candle.open_time_ms
    assert store.get(instrument, original.candle.open_time_ms) == original.candle
    assert collector.accept(original, now_ms=original.received_at_ms) == 1
    assert received == [original.candle]


def test_failed_migration_rolls_back_ddl_and_version_record(settings, monkeypatch):
    import psycopg

    from omega.infrastructure.postgres import schema

    with connect_database(settings) as connection:
        connection.execute("BEGIN")
        try:
            connection.execute("DELETE FROM omega_meta.schema_migrations")
            with monkeypatch.context() as patch:
                patch.setattr(
                    schema,
                    "migration_sql",
                    lambda version=1: "CREATE TABLE market.failed_probe(id INTEGER); SELECT 1/0;",
                )
                with pytest.raises(psycopg.errors.DivisionByZero):
                    migrate(connection)
            assert connection.execute("SELECT to_regclass('market.failed_probe')").fetchone() == (
                None,
            )
            assert connection.execute(
                "SELECT count(*) FROM omega_meta.schema_migrations"
            ).fetchone() == (0,)
        finally:
            connection.rollback()
        schema.verify_schema(connection)


@pytest.mark.parametrize("prices", [("100", "100"), ("100", "99")])
def test_concurrent_writers_deduplicate_or_reject_conflicting_value(settings, instrument, prices):
    def save(price):
        value = PostgresCandleStore(settings)
        try:
            return value.put(observation(instrument, price))
        except ProtocolError:
            return "conflict"
        finally:
            value.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(save, prices))
    assert results.count(True) == 1
    assert results.count(False if prices[0] == prices[1] else "conflict") == 1


def test_migration_checksum_and_runtime_role(settings):
    with connect_database(settings) as connection:
        migrate(connection)
        migrate(connection)
        assert connection.execute(
            "SELECT rolsuper FROM pg_roles WHERE rolname=current_user"
        ).fetchone() == (False,)
        assert connection.execute(
            "SELECT count(*) FROM timescaledb_information.hypertables "
            "WHERE hypertable_schema='market' AND hypertable_name='candles_1m'"
        ).fetchone() == (1,)
        connection.execute("BEGIN")
        try:
            connection.execute("UPDATE omega_meta.schema_migrations SET checksum='mismatch'")
            with pytest.raises(StorageError, match="checksum mismatch"):
                migrate(connection)
        finally:
            connection.rollback()
        migrate(connection)


def test_latest_export_preserves_raw_capture_origin_separately(store, instrument):
    from dataclasses import replace

    original = observation(instrument)
    store.put(original)
    captured = replace(
        original,
        source="websocket",
        received_at_ms=original.received_at_ms + 999,
        raw='{"received":"later websocket"}',
    )
    assert not store.put(captured)
    newer = replace(
        captured,
        candle=replace(
            captured.candle,
            open_time_ms=captured.candle.open_time_ms + 60000,
            close_time_ms=captured.candle.close_time_ms + 60000,
        ),
        received_at_ms=captured.received_at_ms + 60000,
    )
    store.put(newer)
    result = store.latest_observations(instrument, 2)
    assert result[0].source == "rest"
    assert result[0].received_at_ms == original.received_at_ms
    assert result[0].raw_source == "websocket"
    assert result[0].raw_received_at_ms == captured.received_at_ms
    assert result[0].raw == captured.raw
    assert store.latest(instrument, 1) == (newer.candle,)
    assert store.latest_observations(instrument, 1) == (result[1],)


def test_clock_deferred_acceptance_preserves_actual_early_raw_receipt(store, instrument):
    original = observation(instrument)
    captured = replace(
        original,
        source="websocket",
        raw='{"x":true}',
        raw_source="websocket",
        raw_received_at_ms=original.candle.close_time_ms - 401,
    )
    assert store.put(captured)
    exported = store.latest_observations(instrument, 1)[0]
    assert exported == captured
    # A later duplicate cannot replace the original payload/receipt history.
    assert not store.put(
        replace(
            captured,
            raw='{"different":"duplicate"}',
            raw_received_at_ms=captured.received_at_ms + 1000,
        )
    )
    assert store.latest_observations(instrument, 1)[0] == captured


@pytest.mark.parametrize(
    "changes",
    [
        {"raw_source": "websocket"},
        {"raw_received_at_ms": 1},
        {"raw_source": "unknown", "raw_received_at_ms": 1},
        {"raw_source": "websocket", "raw_received_at_ms": True},
        {"raw_source": "websocket", "raw_received_at_ms": -1},
    ],
)
def test_invalid_raw_receipt_metadata_never_commits(store, instrument, changes):
    candidate = replace(observation(instrument, raw="{}"), **changes)
    with pytest.raises(ValidationError):
        store.put(candidate)
    assert store.get(instrument, candidate.candle.open_time_ms) is None


def test_database_page_reports_real_internal_gaps_and_observation_scope(
    settings, store, instrument, tmp_path, monkeypatch
):
    original = observation(instrument)
    store.put(original)
    last = replace(
        original,
        candle=replace(
            original.candle,
            open_time_ms=original.candle.open_time_ms + 120000,
            close_time_ms=original.candle.close_time_ms + 120000,
        ),
        received_at_ms=original.received_at_ms + 120000,
    )
    store.put(last)
    backend = MarketDataBackend(tmp_path)
    monkeypatch.setattr(backend, "_store", lambda: PostgresCandleStore(settings))
    view = backend.refresh("usdm", instrument.key)
    assert view["database"]["state"] == "ready"
    assert view["database"]["name"] == "omega_test"
    assert view["database"]["size_bytes"] > 0
    row = next(row for row in view["instruments"] if row["instrument"] == instrument.key)
    assert row["rows"] == 2 and row["gap_rows"] == 1
    assert row["availability"] == pytest.approx(2 / 3)
    assert row["gap_rate"] == pytest.approx(1 / 3)
    assert row["dummy_rate"] is None
    channel = view["selected"]["channels"]["ohlcv"]
    assert channel["gap_ranges"] == [
        [original.candle.close_time_ms, original.candle.close_time_ms + 60000]
    ]
    assert channel["range_window"] == {
        "start_ms": original.candle.open_time_ms,
        "end_ms": last.candle.close_time_ms,
    }
    assert not view["selected"]["channels"]["depth"]["supported"]


def test_private_peer_import_reads_only_real_gaps_and_preserves_raw_provenance(
    settings, store, instrument, tmp_path, monkeypatch
):
    if os.environ.get("OMEGA_TEST_PEER_DB") != "1":
        pytest.skip("requires explicitly enabled peer DB omega_test_peer at 127.0.0.1:55434")
    import psycopg

    from omega.infrastructure import market_data

    peer_settings = replace(settings, port=isolated_test_port(peer=True), dbname="omega_test_peer")
    with connect_database(peer_settings) as connection:
        assert connection.execute("SELECT current_database()").fetchone() == ("omega_test_peer",)
        migrate(connection)
    first = observation(instrument)
    middle = replace(
        first,
        candle=replace(
            first.candle,
            open_time_ms=first.candle.open_time_ms + 60000,
            close_time_ms=first.candle.close_time_ms + 60000,
            open=decimal("107"),
            high=decimal("107"),
            low=decimal("107"),
            close=decimal("107"),
        ),
        received_at_ms=first.received_at_ms + 60000,
        raw='{"peer_actual_capture":true}',
    )
    last = replace(
        first,
        candle=replace(
            first.candle,
            open_time_ms=first.candle.open_time_ms + 120000,
            close_time_ms=first.candle.close_time_ms + 120000,
        ),
        received_at_ms=first.received_at_ms + 120000,
    )
    store.put(first)
    store.put(last)
    peer = PostgresCandleStore(peer_settings)
    try:
        for item in (first, middle, last):
            peer.put(item)
    finally:
        peer.close()
    real_connect = psycopg.connect
    readonly_checks = []

    def checked_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        if kwargs.get("application_name") == "omega-readonly-peer":
            readonly_checks.append(
                connection.execute("SHOW default_transaction_read_only").fetchone()[0]
            )
        return connection

    monkeypatch.setattr(psycopg, "connect", checked_connect)
    monkeypatch.setattr(market_data.time, "time_ns", lambda: last.candle.close_time_ms * 1000000)
    backend = MarketDataBackend(tmp_path)
    monkeypatch.setattr(backend, "_store", lambda: PostgresCandleStore(settings))
    result = backend.import_peer(
        instrument.key,
        {
            "host": "127.0.0.1",
            "port": isolated_test_port(peer=True),
            "database": "omega_test_peer",
            "user": peer_settings.user,
            "password": peer_settings.password,
        },
        3,
        lambda _progress: None,
        threading.Event(),
    )
    assert result["inserted_rows"] == 1
    assert result["remaining_minutes"] == 0 and result["observed_only"]
    assert readonly_checks == ["on"]
    assert store.get(instrument, first.candle.open_time_ms) == first.candle
    assert store.get(instrument, middle.candle.open_time_ms) == middle.candle
    copied = store.latest_observations(instrument, 3)[1]
    assert copied.raw == middle.raw
    assert copied.raw_source == "rest"
    assert copied.raw_received_at_ms == middle.received_at_ms
