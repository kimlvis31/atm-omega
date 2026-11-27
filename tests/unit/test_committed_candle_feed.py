"""The feed adapter only reads the explicit local checksum-verified Omega schema."""

import sys
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.infrastructure import candle_feed as module
from omega.infrastructure.candles import EPOCH
from omega.infrastructure.postgres.config import DatabaseSettings, database_destination_id
from omega.ports.collection import MinuteObservation

KEY = "binance:spot:BTCUSDT"


def row(source="rest", received=60000):
    return (
        KEY,
        EPOCH,
        EPOCH + timedelta(minutes=1),
        Decimal(100),
        Decimal(101),
        Decimal(99),
        Decimal(100),
        Decimal(10),
        source,
        received,
    )


class Connection:
    def __init__(self, rows):
        self.rows, self.closed, self.queries = rows, False, []

    def execute(self, sql, parameters):
        self.queries.append((sql, parameters))
        assert sql.startswith("SELECT ")
        return SimpleNamespace(fetchall=lambda: self.rows)

    def close(self):
        self.closed = True


def test_checksum_is_verified_before_only_bounded_read_and_connection_always_closes(
    tmp_path, monkeypatch
):
    connection, calls = Connection([row()]), []
    source = module.CommittedCandleSource(tmp_path)
    monkeypatch.setattr(source, "_connect", lambda: connection)
    monkeypatch.setattr(module, "verify_schema", lambda selected: calls.append(selected))
    rows = source.read([KEY], 0, 60000)
    assert calls == [connection] and connection.closed
    assert rows[0].open_time_ms == 0 and rows[0].instrument.key == KEY
    assert connection.queries[0][1][-1] == 4097


def test_read_observations_preserves_committed_source_and_received_time(tmp_path, monkeypatch):
    connection = Connection([row("websocket", 63000)])
    adapter = module.CommittedCandleSource(tmp_path)
    monkeypatch.setattr(adapter, "_connect", lambda: connection)
    monkeypatch.setattr(module, "verify_schema", lambda _: None)

    observations = adapter.read_observations([KEY], 0, 60000)

    assert observations == (MinuteObservation(observations[0].candle, "websocket", 63000),)
    assert observations[0].raw is None
    assert observations[0].raw_source is None
    assert observations[0].raw_received_at_ms is None
    assert connection.closed


@pytest.mark.parametrize("source,received", [("synthetic", 60000), ("rest", 1), ("rest", True)])
def test_unknown_or_impossible_observation_is_never_delivered(
    tmp_path, monkeypatch, source, received
):
    connection = Connection([row(source, received)])
    adapter = module.CommittedCandleSource(tmp_path)
    monkeypatch.setattr(adapter, "_connect", lambda: connection)
    monkeypatch.setattr(module, "verify_schema", lambda _: None)
    with pytest.raises(ProtocolError):
        adapter.read([KEY], 0, 60000)
    assert connection.closed


def test_mismatching_schema_closes_without_query(tmp_path, monkeypatch):
    connection = Connection([row()])
    adapter = module.CommittedCandleSource(tmp_path)
    monkeypatch.setattr(adapter, "_connect", lambda: connection)
    monkeypatch.setattr(
        module, "verify_schema", lambda _: (_ for _ in ()).throw(StorageError("bad schema"))
    )
    with pytest.raises(StorageError):
        adapter.read([KEY], 0, 60000)
    assert connection.closed and not connection.queries


def test_connection_readonly_and_local_literal_policy(tmp_path, monkeypatch):
    calls = []
    settings = DatabaseSettings("localhost", 55432, "omega", "reader", "never-log")
    monkeypatch.setattr(module, "load_database_settings", lambda _: settings)
    monkeypatch.setitem(
        sys.modules, "psycopg", SimpleNamespace(connect=lambda **kw: calls.append(kw) or object())
    )
    module.CommittedCandleSource(tmp_path)._connect()
    assert calls[0]["host"] == "127.0.0.1"
    assert "default_transaction_read_only=on" in calls[0]["options"]
    settings = DatabaseSettings("example.com", 5432, "legacy", "reader", "never-log")
    with pytest.raises(ValidationError):
        module.CommittedCandleSource(tmp_path)._connect()
    assert len(calls) == 1


def test_source_capacity_is_enforced_before_any_unbounded_delivery(tmp_path, monkeypatch):
    connection = Connection([row(), row()])
    adapter = module.CommittedCandleSource(tmp_path)
    monkeypatch.setattr(adapter, "_connect", lambda: connection)
    monkeypatch.setattr(module, "verify_schema", lambda _: None)
    with pytest.raises(StorageError, match="capacity"):
        adapter.read([KEY], 0, 60000, limit=1)
    assert connection.closed


def test_optional_actual_knowledge_cutoff_is_in_sql_and_legacy_query_is_unchanged(
    tmp_path, monkeypatch
):
    source = module.CommittedCandleSource(tmp_path)
    connections = [Connection([row()]), Connection([row()])]
    monkeypatch.setattr(source, "_connect", lambda: connections.pop(0))
    monkeypatch.setattr(module, "verify_schema", lambda _: None)
    old, filtered = tuple(connections)
    assert source.read([KEY], 0, 60000) == source.read([KEY], 0, 60000, as_of_ms=60000)
    assert "received_ms<=%s" not in old.queries[0][0]
    assert "received_ms<=%s" in filtered.queries[0][0]
    assert old.queries[0][1][-1] == filtered.queries[0][1][-1] == 4097
    assert filtered.queries[0][1][-2] == 60000


def test_broken_connection_cannot_return_a_candle_received_after_the_cutoff(tmp_path, monkeypatch):
    source, connection = module.CommittedCandleSource(tmp_path), Connection([row(received=60001)])
    monkeypatch.setattr(source, "_connect", lambda: connection)
    monkeypatch.setattr(module, "verify_schema", lambda _: None)
    with pytest.raises(ProtocolError):
        source.read([KEY], 0, 60000, as_of_ms=60000)
    assert connection.closed


@pytest.mark.parametrize("cutoff", [True, -1, 60000.0, 253402300799001])
def test_invalid_cutoff_is_rejected_before_connecting(tmp_path, monkeypatch, cutoff):
    source = module.CommittedCandleSource(tmp_path)
    monkeypatch.setattr(source, "_connect", lambda: pytest.fail("invalid cutoff reached DB"))
    with pytest.raises(ValidationError):
        source.read([KEY], 0, 60000, as_of_ms=cutoff)


def test_destination_fence_uses_same_loaded_settings_before_connection(tmp_path, monkeypatch):
    source = module.CommittedCandleSource(tmp_path)
    connection = Connection([row()])
    settings = DatabaseSettings("127.0.0.1", 55432, "omega", "reader", "secret")
    loaded, connected = [], []
    monkeypatch.setattr(
        module,
        "load_database_settings",
        lambda _path: loaded.append(settings) or settings,
    )
    monkeypatch.setattr(
        source,
        "_connect_settings",
        lambda selected: connected.append(selected) or connection,
    )
    monkeypatch.setattr(module, "verify_schema", lambda _: None)

    observations = source.read_observations(
        [KEY], 0, 60_000, destination_id=database_destination_id(settings)
    )

    assert len(observations) == 1
    assert loaded == [settings] and connected == [settings]
    assert connection.closed


def test_destination_mismatch_or_bad_digest_is_rejected_before_connect(tmp_path, monkeypatch):
    source = module.CommittedCandleSource(tmp_path)
    settings = DatabaseSettings("127.0.0.1", 55432, "omega", "reader", "secret")
    monkeypatch.setattr(module, "load_database_settings", lambda _path: settings)
    monkeypatch.setattr(
        source,
        "_connect_settings",
        lambda _settings: pytest.fail("destination mismatch reached database connect"),
    )
    with pytest.raises(ValidationError, match="destination mismatch"):
        source.read_observations([KEY], 0, 60_000, destination_id="0" * 64)

    monkeypatch.setattr(
        module,
        "load_database_settings",
        lambda _path: pytest.fail("malformed destination should be rejected before config load"),
    )
    with pytest.raises(ValidationError, match="invalid bounded"):
        source.read_observations([KEY], 0, 60_000, destination_id="A" * 64)
