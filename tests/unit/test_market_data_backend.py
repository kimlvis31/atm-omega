"""Observed repair and read-only peer validation without sockets or a database."""

from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure import market_data
from omega.infrastructure.candles import EPOCH
from omega.ports.collection import MinuteObservation

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def test_public_catalog_repair_and_collector_history_share_one_project_budget(tmp_path):
    from omega.domain.errors import RateLimited
    from omega.infrastructure.binance.shared_budget import SharedRequestBudget

    backend = market_data.MarketDataBackend(tmp_path)
    spot = backend.history("spot").client.budget
    usdm = backend.history("usdm").client.budget
    child = market_data.MarketDataBackend._history("usdm", root=tmp_path).client.budget
    assert all(isinstance(budget, SharedRequestBudget) for budget in (spot, usdm, child))
    assert spot.path == usdm.path == child.path
    spot.reserve({"binance-ip": 600})
    with pytest.raises(RateLimited):
        usdm.reserve({"binance-ip": 1})
    with pytest.raises(RateLimited):
        child.reserve({"binance-ip": 1})


def candle(stamp, price=100):
    value = Decimal(price)
    return Candle(INSTRUMENT, stamp, stamp + 60_000, value, value, value, value, Decimal(1))


def test_ranges_are_half_open_disjoint_and_never_generate_a_candle():
    assert market_data.ranges([0, 120_000, 120_000], 0, 240_000) == (
        [[0, 60_000], [120_000, 180_000]],
        [[60_000, 120_000], [180_000, 240_000]],
    )


@pytest.mark.parametrize(
    "host", ["localhost", "8.8.8.8", "169.254.1.1", "224.0.0.1", "example.com", 3232235777]
)
def test_peer_requires_a_private_literal_not_dns_or_public_ip(host):
    with pytest.raises(ValidationError):
        market_data.peer_parameters(
            {
                "host": host,
                "port": 5432,
                "database": "omega",
                "user": "reader",
                "password": "secret",
            }
        )


@pytest.mark.parametrize(
    "host", ["127.0.0.1", "192.168.1.2", "10.0.0.2", "172.16.0.1", "::1", "fd00::1"]
)
def test_private_and_loopback_literals_are_accepted(host):
    assert (
        market_data.peer_parameters(
            {"host": host, "database": "omega_peer", "user": "reader", "password": "secret"}
        )["database"]
        == "omega_peer"
    )


class Store:
    def __init__(self):
        self.data = {60_000: candle(60_000)}
        self.writes = []
        self.connection = self

    def execute(self, _sql, parameters):
        low = market_data.milliseconds(parameters[1])
        high = market_data.milliseconds(parameters[2])
        return SimpleNamespace(
            fetchall=lambda: [
                (EPOCH + timedelta(milliseconds=stamp),)
                for stamp in sorted(self.data)
                if low <= stamp < high
            ]
        )

    def get(self, instrument, stamp):
        return self.data.get(stamp)

    def put(self, observation):
        self.writes.append(observation)
        stamp = observation.candle.open_time_ms
        inserted = stamp not in self.data
        self.data[stamp] = observation.candle
        return inserted

    def close(self):
        pass


def test_repair_fills_only_confirmed_gaps_and_preserves_late_raw_provenance(tmp_path, monkeypatch):
    backend, store = market_data.MarketDataBackend(tmp_path), Store()
    monkeypatch.setattr(backend, "_store", lambda: store)
    monkeypatch.setattr(market_data.time, "time_ns", lambda: 180_000 * 1_000_000)

    def fetch(instrument, low, high, now):
        assert (low, high) in {(0, 60_000), (120_000, 180_000)}
        return [
            MinuteObservation(candle(low), "rest", 180_000, "actual-payload", "websocket", 180_001)
        ]

    stop = SimpleNamespace(is_set=lambda: False)
    result = backend._repair(INSTRUMENT, 3, fetch, lambda _value: None, stop)
    assert result["inserted_rows"] == 2 and result["remaining_minutes"] == 0
    assert len(store.writes) == 4
    assert store.writes[0].source == "rest" and store.writes[0].raw is None
    assert store.writes[1].source == "rest" and store.writes[1].received_at_ms == 180_000
    assert store.writes[1].raw_source == "websocket"
    assert store.writes[1].raw_received_at_ms == 180_001
    assert store.writes[1].raw == "actual-payload"
    assert all(observation.candle.open_time_ms != 60_000 for observation in store.writes)
    result = backend._repair(
        INSTRUMENT, 3, lambda *_: pytest.fail("duplicate refetch"), lambda _: None, stop
    )
    assert result["inserted_rows"] == 0


def test_peer_repair_preserves_early_raw_receipt_separately_from_accepted_candle(
    tmp_path, monkeypatch
):
    backend, store = market_data.MarketDataBackend(tmp_path), Store()
    monkeypatch.setattr(backend, "_store", lambda: store)
    monkeypatch.setattr(market_data.time, "time_ns", lambda: 180_000 * 1_000_000)

    def fetch(instrument, low, high, now):
        return [
            MinuteObservation(
                candle(low),
                "websocket",
                low + 60_010,
                "early real bytes",
                "websocket",
                low + 59_600,
            )
        ]

    result = backend._repair(
        INSTRUMENT, 3, fetch, lambda _: None, SimpleNamespace(is_set=lambda: False)
    )
    assert result["inserted_rows"] == 2
    enriched = [row for row in store.writes if row.raw is not None]
    assert len(enriched) == 2
    for row in enriched:
        assert row.received_at_ms == row.candle.close_time_ms + 10
        assert row.raw_received_at_ms == row.candle.close_time_ms - 400
        assert row.raw_source == "websocket" and row.raw == "early real bytes"


def test_a_concurrent_conflicting_observation_is_never_overwritten(tmp_path, monkeypatch):
    backend, store = market_data.MarketDataBackend(tmp_path), Store()
    monkeypatch.setattr(backend, "_store", lambda: store)
    monkeypatch.setattr(market_data.time, "time_ns", lambda: 180_000 * 1_000_000)
    monkeypatch.setattr(store, "get", lambda *_: candle(0, 999))
    with pytest.raises(ProtocolError, match="conflicts"):
        backend._repair(
            INSTRUMENT,
            3,
            lambda *_: [MinuteObservation(candle(0), "rest", 180_000)],
            lambda _: None,
            SimpleNamespace(is_set=lambda: False),
        )
    assert store.writes == []


def test_unknown_raw_provenance_is_not_invented(tmp_path, monkeypatch):
    backend, store = market_data.MarketDataBackend(tmp_path), Store()
    monkeypatch.setattr(backend, "_store", lambda: store)
    monkeypatch.setattr(market_data.time, "time_ns", lambda: 180_000 * 1_000_000)
    result = backend._repair(
        INSTRUMENT,
        3,
        lambda instrument, low, high, now: [
            MinuteObservation(candle(low), "rest", 180_000, "unknown-origin-payload")
        ],
        lambda _: None,
        SimpleNamespace(is_set=lambda: False),
    )
    assert result["raw_payloads_skipped_unknown_provenance"] == 2
    assert result["inserted_rows"] == 2
    assert all(observation.raw is None for observation in store.writes)


def test_database_summary_distinguishes_real_gaps_from_eta_placeholder_rate(tmp_path, monkeypatch):
    backend = market_data.MarketDataBackend(tmp_path)

    class Connection:
        info = SimpleNamespace(dbname="omega")

        def execute(self, sql, parameters=None):
            if sql.startswith("SELECT instrument,"):
                return SimpleNamespace(
                    fetchall=lambda: [
                        (INSTRUMENT.key, EPOCH, EPOCH + timedelta(milliseconds=120_000), 2, 180_000)
                    ]
                )
            if "pg_database_size" in sql:
                return SimpleNamespace(fetchone=lambda: (2048,))
            return SimpleNamespace(
                fetchall=lambda: [(EPOCH,), (EPOCH + timedelta(milliseconds=120_000),)]
            )

    monkeypatch.setattr(
        backend, "_store", lambda: SimpleNamespace(connection=Connection(), close=lambda: None)
    )
    result = backend.refresh("usdm", INSTRUMENT.key)
    row = result["instruments"][0]
    assert row["availability"] == 2 / 3 and row["gap_rate"] == 1 / 3
    assert row["dummy_rate"] is None
    assert result["selected"]["channels"]["ohlcv"]["gap_ranges"] == [[60_000, 120_000]]
    assert result["database"]["size_bytes"] == 2048
    assert result["database"]["free_bytes"] is None


def test_peer_connection_is_read_only_and_schema_is_checked_before_reading(tmp_path, monkeypatch):
    import sys

    calls = []
    connection = SimpleNamespace(close=lambda: calls.append("close"))

    def connect(**parameters):
        assert "default_transaction_read_only=on" in parameters["options"]
        assert parameters["host"] == "192.168.1.2"
        return connection

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=connect))
    monkeypatch.setattr(market_data, "verify_schema", lambda selected: calls.append("schema"))
    backend = market_data.MarketDataBackend(tmp_path)
    monkeypatch.setattr(
        backend, "_repair", lambda *_: calls.append("local_gap_repair") or {"inserted_rows": 0}
    )
    result = backend.import_peer(
        INSTRUMENT.key,
        {
            "host": "192.168.1.2",
            "database": "omega_peer",
            "user": "reader",
            "password": "never-log",
        },
        500,
        lambda _: None,
        SimpleNamespace(is_set=lambda: False),
    )
    assert result["inserted_rows"] == 0
    assert calls == ["schema", "local_gap_repair", "close"]
    monkeypatch.setattr(
        market_data,
        "verify_schema",
        lambda _: (_ for _ in ()).throw(StorageError("checksum mismatch")),
    )
    with pytest.raises(StorageError):
        backend.import_peer(
            INSTRUMENT.key,
            {
                "host": "192.168.1.2",
                "database": "omega_peer",
                "user": "reader",
                "password": "never-log",
            },
            500,
            lambda _: None,
            SimpleNamespace(is_set=lambda: False),
        )
    assert calls[-1] == "close"
