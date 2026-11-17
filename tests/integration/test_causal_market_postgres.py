"""Explicit isolated real PostgreSQL commit visibility, not exchange collection.

Only TEST_CAUSAL_* rows created here are removed. No migrations, Docker lifecycle,
real account, keys or trading operations are involved.
"""

import json
import os
import time
import tomllib
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.errors import ProtocolError
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.collection import MinuteObservation
from omega.runtime.causal_market_source import CommittedInferenceMarketSource

pytestmark = pytest.mark.database
MINUTE = 60000
START = 1_700_000_040_000


@pytest.fixture(scope="module")
def configuration(request):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated database opt-in")
    name = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not name:
        pytest.fail("OMEGA_TEST_DB_CONFIG must explicitly select the isolated test DB")
    path = Path(name).resolve()
    settings = load_database_settings(path)
    if (settings.host, settings.port, settings.dbname, settings.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing causal NN DB tests outside 127.0.0.1:55433/omega_test/omega_app")
    with connect_database(settings) as connection:
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
    return path, settings


@pytest.fixture
def database(configuration, tmp_path):
    path, settings = configuration
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    secret = Path(raw["password_file"])
    if not secret.is_absolute():
        secret = path.parent / secret
    values = {
        "host": settings.host,
        "port": settings.port,
        "dbname": settings.dbname,
        "user": settings.user,
        "password_file": secret.resolve().as_posix(),
    }
    (tmp_path / "database.config.toml").write_text(
        "\n".join(f"{key} = {json.dumps(value)}" for key, value in values.items()), encoding="utf-8"
    )
    instruments, handles = [], []

    def instrument(market=Market.USDM):
        value = Instrument("binance", market, "TEST_CAUSAL_" + uuid4().hex[:20].upper())
        instruments.append(value)
        return value

    def store():
        value = PostgresCandleStore(settings)
        handles.append(value)
        return value

    try:
        yield settings, instrument, store, CommittedInferenceMarketSource(tmp_path)
    finally:
        for handle in handles:
            handle.close()
        with connect_database(settings) as connection:
            for value in instruments:
                assert value.symbol.startswith("TEST_CAUSAL_")
                connection.execute(
                    "DELETE FROM market.candles_1m WHERE instrument=%s", (value.key,)
                )


def observation(instrument, minute=0, *, start=START, price="100.123456789012345678901"):
    opened, p = start + minute * MINUTE, D(price)
    return MinuteObservation(
        Candle(
            instrument, opened, opened + MINUTE, p, p + 1, p - 1, p, D("2.123456789012345678901")
        ),
        "websocket",
        opened + MINUTE + 5,
    )


def test_actual_committed_exact_ohlcv_has_read_completion_availability_and_stable_identity(
    database,
):
    _, instrument, store, source = database
    key = instrument()
    writer = store()
    writer.put(observation(key))
    writer.put(observation(key, 1))
    before = time.time_ns() // 1_000_000
    result = source.read_window(key, START, START + 2 * MINUTE)
    after = time.time_ns() // 1_000_000
    assert len(result) == 2
    assert result[0].row.open == D("100.123456789012345678901")
    assert result[0].row.volume == D("2.123456789012345678901")
    assert all(before <= row.available_at_ms <= after for row in result)
    repeated = source.read_window(key, START, START + 2 * MINUTE)
    assert [row.commit_sha256 for row in repeated] == [row.commit_sha256 for row in result]
    assert repeated[0].available_at_ms >= result[0].available_at_ms


def test_actual_gap_and_other_instrument_are_not_filled_or_substituted(database):
    _, instrument, store, source = database
    key, other = instrument(), instrument(Market.SPOT)
    writer = store()
    for value in (observation(key, 0), observation(key, 2), observation(other, 1)):
        writer.put(value)
    result = source.read_window(key, START, START + 3 * MINUTE)
    assert [row.row.open_time_ms for row in result] == [START, START + 2 * MINUTE]
    assert all(row.row.instrument == key.key for row in result)
    assert source.read_window(key, START + 3 * MINUTE, START + 4 * MINUTE) == ()


def test_actual_uncommitted_and_rolled_back_rows_are_invisible(database):
    _, instrument, store, source = database
    key = instrument()
    writer = store()
    with writer.connection.transaction():
        assert writer.put(observation(key))
        assert source.read_window(key, START, START + MINUTE) == ()
    assert len(source.read_window(key, START, START + MINUTE)) == 1
    with pytest.raises(RuntimeError, match="explicit rollback"):
        with writer.connection.transaction():
            assert writer.put(observation(key, 1))
            assert source.read_window(key, START + MINUTE, START + 2 * MINUTE) == ()
            raise RuntimeError("explicit rollback")
    assert source.read_window(key, START + MINUTE, START + 2 * MINUTE) == ()


def test_actual_future_candle_or_received_time_cannot_become_current_input(database):
    _, instrument, store, source = database
    key = instrument()
    future = (time.time_ns() // 1_000_000 // MINUTE + 2) * MINUTE
    writer = store()
    writer.put(observation(key, start=future))
    with pytest.raises(ProtocolError, match="closed observations"):
        source.read_window(key, future, future + MINUTE)
    past_key = instrument()
    writer.put(replace(observation(past_key), received_at_ms=future))
    with pytest.raises(ProtocolError, match="closed observations"):
        source.read_window(past_key, START, START + MINUTE)


def test_actual_schema_permits_old_received_but_adapter_explicitly_rejects_it(database):
    settings, instrument, store, source = database
    key = instrument()
    store().put(observation(key))
    with connect_database(settings) as connection:
        connection.execute(
            "UPDATE market.candles_1m SET received_ms=%s WHERE instrument=%s",
            (START + MINUTE - 1, key.key),
        )
    with pytest.raises(ProtocolError, match="closed observations"):
        source.read_window(key, START, START + MINUTE)


def test_actual_reader_sets_read_only_and_closes_its_store(database):
    _, instrument, store, source = database
    key = instrument()
    store().put(observation(key))
    opened = []

    def factory(settings):
        reader = PostgresCandleStore(settings)
        original = reader.close

        def close():
            assert reader.connection.execute("SHOW default_transaction_read_only").fetchone() == (
                "on",
            )
            assert reader.connection.execute("SHOW transaction_read_only").fetchone() == ("on",)
            original()

        reader.close = close
        opened.append(reader)
        return reader

    source.store_factory = factory
    assert not opened
    assert len(source.read_window(key, START, START + MINUTE)) == 1
    assert opened[0].connection.closed


def test_one_actual_select_keeps_its_snapshot_when_another_commit_arrives_before_fetch(database):
    _, instrument, store, source = database
    key = instrument()
    writer = store()
    writer.put(observation(key, 0))
    writer.put(observation(key, 1))
    data_queries = []
    inserted = False

    def factory(settings):
        reader = PostgresCandleStore(settings)
        connection = reader.connection

        class CommitAfterSelect:
            def execute(self, sql, parameters=None):
                nonlocal inserted
                cursor = connection.execute(sql, parameters)
                if sql.startswith("SELECT instrument,open_time,close_time"):
                    data_queries.append(sql)
                    if not inserted:
                        writer.put(observation(key, 2))
                        inserted = True
                return cursor

            def close(self):
                connection.close()

        reader.connection = CommitAfterSelect()
        return reader

    source.store_factory = factory
    first = source.read_window(key, START, START + 3 * MINUTE)
    assert [row.row.open_time_ms for row in first] == [START, START + MINUTE]
    assert len(data_queries) == 1
    second = source.read_window(key, START, START + 3 * MINUTE)
    assert [row.row.open_time_ms for row in second] == [START, START + MINUTE, START + 2 * MINUTE]
    assert len(data_queries) == 2
