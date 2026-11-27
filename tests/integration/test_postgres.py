"""Opt-in durability checks on omega_test at 127.0.0.1:55433, never an operational DB."""

import _socket
import asyncio
import http.client
import json
import os
import socket
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from queue import Queue
from urllib.parse import urlencode
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.application.collection import MinuteCollector
from omega.domain.bar_schedule import BarSchedule
from omega.domain.bar_series import BarSeries, SeriesCandle, SeriesObservation
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.bar_series_store import PostgresBarSeriesStore
from omega.infrastructure.bar_series_transfer import LEGACY_FEED, transfer_legacy_window
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.collector_coverage import (
    first_covered,
    note_source_check,
    recent_unconfirmed_trades,
)
from omega.infrastructure.market_data import MarketDataBackend
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import migrate
from omega.ports.collection import MinuteObservation
from omega.runtime.collector_series_api import (
    SERIES_AGGREGATE_PATH,
    SERIES_DEFINITION_PATH,
    SERIES_STREAM_PATH,
)

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


def test_base_bar_storage_separates_intervals_and_preserves_first_receipt(settings, instrument):
    minute = 60_000
    opened = 1_700_000_040_000
    fine = BarSeries(instrument, "native-minute", (minute, 5 * minute), BarSchedule(), opened)
    coarse = BarSeries(
        instrument,
        "native-five-minute",
        (5 * minute,),
        BarSchedule(5 * minute, ((opened, opened + 10 * minute),)),
        opened,
        "calendar-1",
    )
    price = decimal("100.123456789012345678901")
    volume = decimal("0.000000000000000000123")
    first = SeriesObservation(
        SeriesCandle(fine, opened, opened + minute, price, price, price, price, volume),
        "websocket",
        opened + minute + 5,
        '{"one":1}',
        "rest",
        opened + minute - 1,
    )
    second = SeriesObservation(
        SeriesCandle(coarse, opened, opened + 5 * minute, price, price, price, price, volume),
        "rest",
        opened + 5 * minute + 6,
    )
    writer = PostgresBarSeriesStore(settings)
    reader = PostgresBarSeriesStore(settings)
    try:
        assert writer.register(fine)
        assert writer.register(coarse)
        assert not reader.register(fine)
        assert reader.load(coarse.series_id) == coarse
        assert writer.put(first) and writer.put(second)
        assert not reader.put(replace(first, source="vision", received_at_ms=opened + 2 * minute))
        with pytest.raises(ProtocolError, match="conflicting"):
            writer.put(replace(first, candle=replace(first.candle, volume=decimal("1"))))
        end = opened + 10 * minute
        assert not reader.read_observations(fine.series_id, opened, end, as_of_ms=opened + minute)
        assert reader.read_observations(
            fine.series_id, opened, end, as_of_ms=first.received_at_ms
        ) == (first,)
        assert reader.read_observations(
            coarse.series_id, opened, end, as_of_ms=second.received_at_ms
        ) == (second,)
    finally:
        writer.close()
        reader.close()


def test_legacy_bar_transfer_checks_full_window_and_late_provenance(store, instrument, settings):
    original = observation(instrument)
    opened = original.candle.open_time_ms
    minute = 60_000
    end = opened + 2 * minute
    series = BarSeries(instrument, LEGACY_FEED, (minute,), BarSchedule(), opened)
    stage = PostgresBarSeriesStore(settings)
    try:
        assert store.put(original)
        assert transfer_legacy_window(stage, series, opened, end)["verified_rows"] == 1
        assert transfer_legacy_window(stage, series, opened, end)["verified_rows"] == 1
        assert not store.put(observation(instrument, raw='{"late":true}', source="websocket"))
        with pytest.raises(ProtocolError, match="staged bars differ"):
            transfer_legacy_window(stage, series, opened, end, verify_only=True)
        assert transfer_legacy_window(stage, series, opened, end)["verified_rows"] == 1
        rows = stage.read_observations(series.series_id, opened, end, as_of_ms=end)
        row = rows[0]
        assert (row.source, row.received_at_ms) == (original.source, original.received_at_ms)
        assert (row.raw, row.raw_source, row.raw_received_at_ms) == (
            '{"late":true}',
            "websocket",
            original.received_at_ms,
        )

        extra = SeriesObservation(
            replace(row.candle, open_time_ms=opened + minute, close_time_ms=end), "rest", end
        )
        assert stage.put(extra)
        with pytest.raises(ProtocolError, match="staged bars differ"):
            transfer_legacy_window(stage, series, opened, end)
        assert transfer_legacy_window(stage, series, opened, opened + minute)["verified_rows"] == 1
        assert store.get(instrument, opened) == original.candle
        assert store.get(instrument, opened + minute) is None
    finally:
        stage.close()


def test_base_bar_cli_import_restart_and_direct_session_cursor(settings, instrument, tmp_path):
    token = "B" * 43
    token_path = tmp_path / "token"
    token_path.write_text(token, encoding="ascii")
    source = tmp_path / "bars.json"
    five = 300_000
    opened = 360_000
    next_session = opened + 5 * five + 60_000
    precision = "100.000000000000000000123"
    series = BarSeries(
        instrument,
        "equity-native",
        (five,),
        BarSchedule(five, ((opened, opened + 3 * five), (next_session, next_session + five))),
        opened,
        "exchange-1",
    )

    def bar(stamp, receipt=None):
        return {
            "open_ms": stamp,
            "close_ms": stamp + five,
            "received_at_ms": receipt or stamp + five,
            "source": "rest",
            "open": precision,
            "high": precision,
            "low": precision,
            "close": precision,
            "volume": "2.50",
        }

    def stage(*bars):
        source.write_text(json.dumps({"series": series.document(), "bars": bars}), encoding="utf-8")
        return subprocess.run(
            [*command, "import", str(source)], capture_output=True, text=True, timeout=10
        )

    command = [
        sys.executable,
        "-I",
        "-B",
        "-m",
        "omega.runtime.collect_cli",
        "bars",
        "--db-config",
        os.environ["OMEGA_TEST_DB_CONFIG"],
    ]
    first, late, final, next_day = (
        bar(opened),
        bar(opened + five, opened + 4 * five),
        bar(opened + 2 * five),
        bar(next_session),
    )
    initial = stage(first, final, next_day)
    assert initial.returncode == 0, initial.stderr
    assert json.loads(initial.stdout)["inserted"] == 3
    assert json.loads(stage(first, final, next_day).stdout)["inserted"] == 0
    assert stage(late, {**final, "volume": "5.00"}).returncode == 1

    # The child executes the packaged CLI with stdin as a deterministic stop signal.
    child_script = """
import signal
import sys
from threading import Thread
from omega.runtime.collect_cli import main

def stop():
    sys.stdin.readline()
    signal.raise_signal(signal.SIGTERM)

Thread(target=stop, daemon=True).start()
main()
"""
    child_command = [
        *command[:3],
        "-c",
        child_script,
        *command[5:],
        "serve",
        "--token-file",
        str(token_path),
    ]
    ready = Queue()
    with subprocess.Popen(
        child_command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    ) as child:
        reader = threading.Thread(target=lambda: ready.put(child.stdout.readline()), daemon=True)
        reader.start()
        try:
            port = json.loads(ready.get(timeout=8))["port"]

            def connect_owned(address, timeout, source_address=None):
                assert address == ("127.0.0.1", port) and source_address is None
                owned = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                owned.settimeout(timeout)
                try:
                    _socket.socket.connect(owned, address)
                except BaseException:
                    owned.close()
                    raise
                return owned

            def request(path, query, supplied):
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
                connection._create_connection = connect_owned
                try:
                    connection.request(
                        "GET",
                        path + "?" + query,
                        headers={"Authorization": f"Bearer {supplied}"},
                    )
                    response = connection.getresponse()
                    payload = response.read()
                    return response.status, json.loads(payload) if payload else None
                finally:
                    connection.close()

            def get(cursor, cutoff, *, series_id=series.series_id, limit=64, supplied=token):
                query = urlencode(
                    dict(series_id=series_id, next_ms=cursor, as_of_ms=cutoff, limit=limit)
                )
                return request(SERIES_STREAM_PATH, query, supplied)

            base_query = urlencode({"series_id": series.series_id, "session_limit": 1})
            status, page = request(SERIES_DEFINITION_PATH, base_query, token)
            assert status == 200 and page["session_count"] == 2
            assert page["definition"]["native_intervals_ms"] == [five]
            assert page["definition"]["sessions"] == [[opened, opened + 3 * five]]
            remaining = base_query + "&session_offset=" + str(page["next_session_offset"])
            status, page = request(SERIES_DEFINITION_PATH, remaining, token)
            assert status == 200 and page["next_session_offset"] is None
            assert page["definition"]["sessions"] == [[next_session, next_session + five]]

            cutoff = next_session + five
            assert get(opened, cutoff, supplied="unauthorized")[0] == 401
            assert get(opened, cutoff, series_id="0" * 64)[0] == 404
            assert get(opened + 60_000, cutoff)[0] == 400
            status, page = get(opened, cutoff)
            assert status == 200
            assert page["version"] == 2 and page["interval_ms"] == five
            assert page["state"] == "waiting_for_gap" and page["next_ms"] == late["open_ms"]
            assert page["rows"][0]["close"] == precision and page["rows"][0]["volume"] == "2.50"
            repaired = stage(late, final)
            assert repaired.returncode == 0, repaired.stderr
            assert json.loads(repaired.stdout)["inserted"] == 1

            def rollup(cutoff_ms, end_ms=opened + 3 * five):
                query = urlencode(
                    dict(
                        series_id=series.series_id,
                        start_ms=opened,
                        end_ms=end_ms,
                        interval_ms=3 * five,
                        as_of_ms=cutoff_ms,
                    )
                )
                return request(SERIES_AGGREGATE_PATH, query, token)

            pending = rollup(opened + 3 * five - 1)[1]
            assert pending["pending_open_ms"] == [opened] and not pending["bars"]
            unavailable = rollup(late["received_at_ms"] - 1)[1]
            assert unavailable["missing_open_ms"] == [opened] and not unavailable["bars"]
            status, summary = rollup(late["received_at_ms"])
            assert status == 200 and summary["missing_open_ms"] == summary["pending_open_ms"] == []
            assert summary["bars"] == [
                {
                    "open_ms": opened,
                    "close_ms": opened + 3 * five,
                    "open": precision,
                    "high": precision,
                    "low": precision,
                    "close": precision,
                    "volume": "7.50",
                    "received_at_ms": late["received_at_ms"],
                }
            ]
            assert rollup(cutoff, opened + 6 * five)[0] == 400
            early_cutoff = late["received_at_ms"] - 1
            assert get(page["next_ms"], early_cutoff)[1]["state"] == "waiting_for_gap"
            step = get(page["next_ms"], cutoff, limit=1)[1]
            assert step["state"] == "page_full" and step["next_ms"] == final["open_ms"]
            resumed = get(step["next_ms"], cutoff)[1]
            assert [row["open_ms"] for row in resumed["rows"]] == [final["open_ms"]]
            assert resumed["next_ms"] == next_session and resumed["state"] == "session_closed"
            terminal = get(resumed["next_ms"], cutoff)[1]
            assert terminal["state"] == "calendar_unavailable" and len(terminal["rows"]) == 1
            after = get(terminal["next_ms"], cutoff)[1]
            assert after["state"] == "calendar_unavailable" and not after["rows"]
        finally:
            if child.poll() is None:
                child.stdin.close()
                child.stdin = None
            try:
                _, stderr = child.communicate(timeout=8)
            except subprocess.TimeoutExpired:
                child.kill()
                child.communicate(timeout=4)
                raise
            reader.join(timeout=2)
        assert child.returncode == 0, stderr


def test_native_minute_writer_restarts_from_first_uncommitted_minute(settings, instrument):
    config = Path(os.environ["OMEGA_TEST_DB_CONFIG"])
    opened = 1_700_000_040_000
    minute = 60_000
    command = """
import asyncio
import sys

from omega.application.collection import MINUTE
from omega.domain.models import Candle, decimal
from omega.ports.collection import MinuteObservation
from omega.runtime import collect_cli

configuration, symbol, first_open, expected, indices = sys.argv[1:]

class Worker:
    def __init__(self, plan, history, collector, **kwargs):
        self.instrument, = plan.instruments
        self.collector = collector
        assert collector.cursor == {self.instrument: int(expected)}

    async def run(self, stop):
        price = decimal("100.1234567890123456789")
        for index in map(int, indices.split(",") if indices else []):
            opened = int(first_open) + index * MINUTE
            candle = Candle(self.instrument, opened, opened + MINUTE,
                            price, price, price, price, decimal("2.50"))
            receipt = opened + MINUTE + 2 if index != 1 else opened + 3 * MINUTE + 9
            obs = MinuteObservation(candle, "websocket", receipt)
            self.collector.accept(obs, now_ms=obs.received_at_ms)

collect_cli.CollectionWorker = Worker
args = collect_cli.parser().parse_args([
    "--market", "usdm", "--symbols", symbol, "--db-config", configuration,
    "--series-effective-from-ms", first_open,
])
asyncio.run(collect_cli.collect(args))
"""

    def run(start, stamps):
        result = subprocess.run(
            [
                sys.executable,
                "-I",
                "-B",
                "-c",
                command,
                str(config),
                instrument.symbol,
                str(opened),
                str(start),
                stamps,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)["series"][instrument.key]

    series_id = run(opened, "-1,0,2")
    assert run(opened + minute, "1") == series_id
    with connect_database(settings) as connection:
        assert connection.execute(
            "SELECT next_ms FROM market.series_live_cursor WHERE series_id=%s", (series_id,)
        ).fetchone() == (opened + 3 * minute,)
    assert run(opened + 3 * minute, "") == series_id
    read = PostgresBarSeriesStore(settings, read_only=True)
    try:
        rows = read.read_observations(
            series_id, opened, opened + 3 * minute, as_of_ms=opened + 5 * minute
        )
        assert [row.received_at_ms for row in rows] == [
            opened + minute + 2,
            opened + 4 * minute + 9,
            opened + 3 * minute + 2,
        ]
        assert all(row.source == "websocket" for row in rows)
        assert read.connection.execute(
            "SELECT count(*) FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
        ).fetchone() == (0,)
    finally:
        read.close()


def test_native_minute_cursor_never_advances_past_rolled_back_write(settings, instrument):
    opened = 1_700_000_040_000
    minute = 60_000
    series = BarSeries(instrument, "binance-public", (minute,), BarSchedule(), opened)
    owner = PostgresBarSeriesStore(settings)
    contender = PostgresBarSeriesStore(settings)
    price = decimal("100")
    row = SeriesObservation(
        SeriesCandle(series, opened, opened + minute, price, price, price, price, price),
        "rest",
        opened + minute + 1,
    )
    try:
        assert owner.claim_live_series(series, row.received_at_ms) == opened
        with pytest.raises(StorageError, match="already has a live writer"):
            contender.claim_live_series(series, row.received_at_ms)
        with pytest.raises(RuntimeError, match="simulated abort"):
            with owner.connection.transaction():
                assert owner.put_live_observation(row)
                raise RuntimeError("simulated abort")
        owner.close()
        assert contender.claim_live_series(series, row.received_at_ms) == opened
        assert contender.put_live_observation(row)
        assert not contender.put_live_observation(row)
    finally:
        owner.close()
        contender.close()


def test_native_batch_conflict_rolls_back_insert_and_cursor(settings, instrument):
    opened = 1_700_000_040_000
    minute = 60_000
    series = BarSeries(instrument, "binance-public", (minute,), BarSchedule(), opened)
    store = PostgresBarSeriesStore(settings)
    price = decimal("100")
    first = SeriesObservation(
        SeriesCandle(series, opened, opened + minute, price, price, price, price, price),
        "websocket",
        opened + minute + 1,
    )
    second = replace(
        first,
        candle=replace(
            first.candle, open_time_ms=opened + minute, close_time_ms=opened + 2 * minute
        ),
        received_at_ms=opened + 2 * minute + 1,
    )
    conflict = replace(first, candle=replace(first.candle, volume=decimal("99")))
    try:
        assert store.claim_live_series(series, first.received_at_ms) == opened
        assert store.put_live_observation(first)
        with pytest.raises(ProtocolError, match="conflicting base-bar"):
            store.put_live_batch((second, conflict))
        assert store.connection.execute(
            "SELECT next_ms FROM market.series_live_cursor WHERE series_id=%s", (series.series_id,)
        ).fetchone() == (opened + minute,)
        assert (
            store.read_observations(
                series.series_id,
                opened + minute,
                opened + 2 * minute,
                as_of_ms=second.received_at_ms,
                limit=1,
                include_raw=False,
            )
            == ()
        )
        assert store.put_live_batch((second, conflict), missing_only=True) == [
            (True, second.candle),
            (False, first.candle),
        ]
        assert store.put_live_batch((first, second)) == [
            (False, first.candle),
            (False, second.candle),
        ]
    finally:
        store.close()


def test_native_collector_recovers_a_lost_db_session_without_rebinding_api(
    settings, instrument, tmp_path, monkeypatch, capsys
):
    from omega.runtime import collect_cli

    opened = 1_700_000_040_000
    minute = 60_000
    token_file = tmp_path / "collector-token"
    token_file.write_text("D" * 43, encoding="ascii")
    started = []

    class Worker:
        def __init__(self, plan, history, collector, **kwargs):
            assert plan.instruments == (instrument,)
            self.collector = collector
            started.append(collector.cursor[instrument])

        async def run(self, _stop):
            startup = self.collector.cursor[instrument]
            candle = replace(
                observation(instrument).candle, open_time_ms=startup, close_time_ms=startup + minute
            )
            received = startup + minute + 1
            self.collector.accept(MinuteObservation(candle, "websocket", received), now_ms=received)
            if startup == opened:
                owned_pid = self.collector.store.store.connection.info.backend_pid
                with connect_database(settings) as other:
                    assert other.execute(
                        "SELECT pg_terminate_backend(%s,1000)", (owned_pid,)
                    ).fetchone() == (True,)
                following = replace(
                    candle, open_time_ms=startup + minute, close_time_ms=startup + 2 * minute
                )
                self.collector.accept(
                    MinuteObservation(following, "websocket", received + minute),
                    now_ms=received + minute,
                )

    monkeypatch.setattr(collect_cli, "CollectionWorker", Worker)
    arguments = collect_cli.parser().parse_args(
        [
            "--market",
            "usdm",
            "--symbols",
            instrument.symbol,
            "--db-config",
            os.environ["OMEGA_TEST_DB_CONFIG"],
            "--series-effective-from-ms",
            str(opened),
            "--api-port",
            "0",
            "--api-token-file",
            str(token_file),
        ]
    )
    asyncio.run(collect_cli.collect(arguments))
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len([event for event in events if event.get("api_ready")]) == 1
    assert {"ready": False, "reason": "database_unavailable"} in events
    assert started == [opened, opened + minute]
    reader = PostgresBarSeriesStore(settings, read_only=True)
    try:
        series_id = events[0]["series"][instrument.key]
        rows = reader.read_observations(
            series_id, opened, opened + 2 * minute, as_of_ms=opened + 3 * minute
        )
        assert [row.candle.open_time_ms for row in rows] == started
    finally:
        reader.close()


def test_bar_transition_checks_both_sides_at_the_fixed_receipt_cutoff(settings, instrument):
    minute = 60_000
    opened = 360_000
    boundary = opened + 2 * minute
    previous = BarSeries(instrument, "native-one", (minute,), BarSchedule(), opened)
    target = BarSeries(
        instrument,
        "native-five",
        (5 * minute,),
        BarSchedule(5 * minute, ((boundary, boundary + 10 * minute),)),
        boundary,
        "revision-2",
    )
    split = BarSeries(instrument, "native-one", (minute,), BarSchedule(), boundary + minute)
    price = decimal("123.456")
    before = SeriesObservation(
        SeriesCandle(previous, boundary - minute, boundary, price, price, price, price, price),
        "rest",
        boundary + 1,
    )
    after = SeriesObservation(
        SeriesCandle(target, boundary, boundary + 5 * minute, price, price, price, price, price),
        "rest",
        boundary + 5 * minute + 2,
    )
    staging = PostgresBarSeriesStore(settings)
    try:
        assert staging.import_batch(previous, (before,)) == 1
        assert staging.import_batch(target, (after,)) == 1
        staging.register(split)
    finally:
        staging.close()

    command = [
        sys.executable,
        "-I",
        "-B",
        "-m",
        "omega.runtime.collect_cli",
        "bars",
        "--db-config",
        os.environ["OMEGA_TEST_DB_CONFIG"],
        "check-transition",
    ]

    def verify(left, right, cutoff):
        return subprocess.run(
            [
                *command,
                "--previous-series-id",
                left.series_id,
                "--target-series-id",
                right.series_id,
                "--as-of-ms",
                str(cutoff),
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

    early = verify(previous, target, after.received_at_ms - 1)
    assert early.returncode == 2, early.stderr
    assert json.loads(early.stdout)["target_bar_available"] is False
    complete = verify(previous, target, after.received_at_ms)
    assert complete.returncode == 0, complete.stderr
    result = json.loads(complete.stdout)
    assert result["verified"] and result["previous_open_ms"] + minute == result["target_open_ms"]
    wrong = verify(target, split, after.received_at_ms)
    assert wrong.returncode == 1 and "would split a bar" in wrong.stderr


def test_five_minute_writer_restores_session_aware_checkpoint(settings, instrument):
    five = 300_000
    opened = 360_000
    following = opened + 5 * five + 60_000
    final = following + five
    series = BarSeries(
        instrument,
        "session-feed",
        (five,),
        BarSchedule(five, ((opened, opened + 2 * five), (following, final))),
        opened,
        "calendar-1",
    )
    price = decimal("1.250")
    cutoff = final + 2

    def observed(stamp, receipt):
        return SeriesObservation(
            SeriesCandle(series, stamp, stamp + five, price, price, price, price, price),
            "rest",
            receipt,
        )

    writer = PostgresBarSeriesStore(settings)
    try:
        assert writer.claim_live_series(series, cutoff) == opened
        assert writer.put_live_observation(observed(opened, opened + five))
        assert writer.put_live_observation(observed(following, final))
    finally:
        writer.close()
    recovered = PostgresBarSeriesStore(settings)
    try:
        assert recovered.claim_live_series(series, cutoff) == opened + five
        assert recovered.put_live_observation(observed(opened + five, cutoff))
    finally:
        recovered.close()
    again = PostgresBarSeriesStore(settings)
    try:
        assert again.claim_live_series(series, cutoff) == final
        assert not again.put_live_observation(observed(following, cutoff))
    finally:
        again.close()


def test_one_native_collector_runs_spot_and_futures_together(
    settings, instrument, monkeypatch, capsys
):
    from omega.runtime import collect_cli

    opened = 1_700_000_040_000
    minute = 60_000
    started = set()
    spot = Instrument("binance", Market.SPOT, instrument.symbol)
    both_started = asyncio.Event()
    store_ids = set()

    class Worker:
        def __init__(self, plan, history, collector, **kwargs):
            (self.symbol,) = plan.instruments
            assert history.market == self.symbol.market
            self.collector = collector
            assert collector.cursor == {self.symbol: opened}
            store_ids.add(id(collector.store))

        async def run(self, _stop):
            started.add(self.symbol)
            if len(started) == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), timeout=3)
            price = "1.001" if self.symbol.market == Market.SPOT else "9.999"
            row = observation(self.symbol, price)
            self.collector.accept(row, now_ms=row.received_at_ms)

    monkeypatch.setattr(collect_cli, "CollectionWorker", Worker)
    arguments = collect_cli.parser().parse_args(
        [
            "--subscription",
            f"spot:{instrument.symbol}",
            "--subscription",
            f"usdm:{instrument.symbol}",
            "--subscription",
            f"usdm:{instrument.symbol}",
            "--db-config",
            os.environ["OMEGA_TEST_DB_CONFIG"],
            "--series-effective-from-ms",
            str(opened),
        ]
    )
    asyncio.run(collect_cli.collect(arguments))
    assert started == {spot, instrument}
    assert len(store_ids) == 1
    result = json.loads(capsys.readouterr().out.strip())["series"]
    assert set(result) == {spot.key, instrument.key}
    assert result[spot.key] != result[instrument.key]
    reader = PostgresBarSeriesStore(settings, read_only=True)
    try:

        def close_price(key):
            (row,) = reader.read_observations(
                result[key], opened, opened + minute, as_of_ms=opened + 2 * minute
            )
            return row.candle.close

        assert close_price(spot.key) == decimal("1.001")
        assert close_price(instrument.key) == decimal("9.999")
    finally:
        reader.close()


def test_native_coverage_tracks_commits_revisions_and_candidate_gaps(settings, instrument):
    minute = 60_000
    opening = 1_700_000_040_000
    now = opening + 10 * minute
    series = BarSeries(instrument, "binance-public", (minute,), BarSchedule(), opening)
    source = instrument.key + ":aggTrade"
    price = decimal("10")

    def bar(index):
        start = opening + index * minute
        return SeriesObservation(
            SeriesCandle(series, start, start + minute, price, price, price, price, price),
            "rest",
            now,
        )

    def trade(index, quality="observed", *, empty=False):
        start = opening + index * minute
        return AuxiliaryInput(
            "trade_flow",
            (("AGGTRADE_KLINE_ZERO", 1), ("AGGTRADE_EVENTCOUNT", 0))
            if empty
            else (("AGGTRADE_EVENTCOUNT", 4),),
            start,
            start + minute,
            start + minute,
            now,
            quality,
            decimal("1" if quality == "observed" else "0.5"),
            source,
        )

    bars, aux = PostgresBarSeriesStore(settings), PostgresAuxiliaryStore(settings)
    try:
        bars.import_batch(series, (bar(0), bar(1), bar(2), bar(4)))
        aux.put_live_batch([(instrument, trade(0, empty=True)), (instrument, trade(1, "partial"))])
        with aux.connection.transaction(force_rollback=True):
            aux.put_live_batch([(instrument, trade(2))])
        with bars.connection.transaction(force_rollback=True):
            bars.import_batch(series, (bar(3),))
        db = bars.connection
        assert first_covered(db, instrument.key, "trade_flow", (source,)) == opening
        row = db.execute(
            "SELECT observed,incomplete,confirmed_empty FROM market.collector_coverage "
            "WHERE scope_key=%s AND channel='trade_flow' AND dataset=%s",
            (instrument.key, source),
        ).fetchone()
        assert [[(slot.lower, slot.upper) for slot in span] for span in row] == [
            [(opening, opening + minute)],
            [(opening + minute, opening + 2 * minute)],
            [(opening, opening + minute)],
        ]
        targets = {series.series_id: instrument}
        redo = {(instrument.key, opening + 4 * minute): now + 1}
        missing = recent_unconfirmed_trades(db, targets, now, redo)
        assert [row[1] for row in missing] == [opening + 2 * minute, opening + minute]

        repair = replace(trade(1), available_at_ms=now + 1)
        assert aux.put_history_batch(instrument, [repair]) == 1
        remaining = recent_unconfirmed_trades(db, targets, now, redo)
        assert [row[1] for row in remaining] == [opening + 2 * minute]
        note_source_check(
            db, instrument, "agg_trade", "vision", opening, opening + minute, "unlisted", now
        )
        assert db.execute(
            "SELECT result,checked_ms FROM market.collector_history_checks WHERE instrument=%s",
            (instrument.key,),
        ).fetchone() == ("unlisted", now)
    finally:
        bars.close()
        aux.close()
