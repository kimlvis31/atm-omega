"""Bounded real-source adapter with injected SQL handles, not actual DB evidence."""

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal as D

import pytest

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.candles import EPOCH
from omega.infrastructure.postgres.config import DatabaseSettings
from omega.runtime import causal_market_source as module
from omega.runtime.causal_market_source import CommittedInferenceMarketSource

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
START = 1_700_000_040_000
MINUTE = 60000


def record(minute=0, **changes):
    opened = START + minute * MINUTE
    values = {
        "key": BTC.key,
        "opened": EPOCH + timedelta(milliseconds=opened),
        "closed": EPOCH + timedelta(milliseconds=opened + MINUTE),
        "open": D("100.12000"),
        "high": D("102"),
        "low": D("99"),
        "close": D("101"),
        "volume": D("2.123456789012345678901"),
        "source": "websocket",
        "received": opened + MINUTE + 1,
    }
    values.update(changes)
    return tuple(values.values())


class Store:
    def __init__(self, rows, *, fail=False, fail_close=False):
        self.rows, self.fail, self.fail_close = rows, fail, fail_close
        self.calls, self.closed = [], False
        self.connection = self

    def execute(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        if self.fail and sql.startswith("SELECT"):
            raise RuntimeError("secret-password-that-must-not-escape")
        return self

    def fetchall(self):
        return self.rows

    def close(self):
        self.closed = True
        if self.fail_close:
            raise RuntimeError("close-secret")


def source(tmp_path, monkeypatch, rows, *, clocks=None, **store_options):
    settings = DatabaseSettings("127.0.0.1", 55433, "omega_test", "omega_app", "secret")
    state = {"settings": settings, "opened": []}
    monkeypatch.setattr(module, "load_database_settings", lambda _: state["settings"])

    def factory(settings):
        store = Store(rows, **store_options)
        state["opened"].append(store)
        return store

    clock = iter(clocks or (START + 3 * MINUTE, START + 3 * MINUTE + 20))
    value = CommittedInferenceMarketSource(
        tmp_path, store_factory=factory, clock_ms=lambda: next(clock)
    )
    assert not state["opened"]
    return value, state


def test_one_consistent_read_only_query_exact_values_and_real_read_availability(
    tmp_path, monkeypatch
):
    value, state = source(tmp_path, monkeypatch, (record(0), record(1)))
    result = value.read_window(BTC, START, START + 2 * MINUTE)
    store = state["opened"][0]
    assert store.closed
    assert len(store.calls) == 2
    assert store.calls[0] == ("SET default_transaction_read_only=on", None)
    sql, params = store.calls[1]
    assert sql.count("SELECT") == 1
    assert "market.candles_1m" in sql
    assert "JOIN" not in sql and "UPDATE" not in sql
    assert params == (
        BTC.key,
        EPOCH + timedelta(milliseconds=START),
        EPOCH + timedelta(milliseconds=START + 2 * MINUTE),
        3,
    )
    assert result[0].row.volume == D("2.123456789012345678901")
    assert {row.available_at_ms for row in result} == {START + 3 * MINUTE + 20}
    assert result[0].origin == "committed_market"
    assert result[0].quality == "observed"
    assert len(result[0].commit_sha256) == 64


def test_missing_rows_stay_missing_no_fill_or_other_data_fallback(tmp_path, monkeypatch):
    value, state = source(tmp_path, monkeypatch, (record(0), record(2)))
    result = value.read_window(BTC, START, START + 3 * MINUTE)
    assert [v.row.open_time_ms for v in result] == [START, START + 2 * MINUTE]
    assert state["opened"][0].closed


@pytest.mark.parametrize(
    "changes",
    [
        {"source": "interpolated"},
        {"source": "monte_carlo"},
        {"received": START + MINUTE - 1},
        {"received": START + 4 * MINUTE},
        {"key": "binance:usdm:ETHUSDT"},
        {"volume": D("-1")},
        {"closed": EPOCH + timedelta(milliseconds=START + 2 * MINUTE)},
        {"opened": EPOCH + timedelta(milliseconds=START, microseconds=1)},
        {"opened": (EPOCH + timedelta(milliseconds=START)).replace(tzinfo=None)},
        {"received": True},
    ],
)
def test_invalid_original_observation_is_rejected_after_connection_close(
    tmp_path, monkeypatch, changes
):
    value, state = source(tmp_path, monkeypatch, (record(**changes),))
    with pytest.raises((ProtocolError, ValidationError)):
        value.read_window(BTC, START, START + MINUTE)
    assert state["opened"][0].closed


@pytest.mark.parametrize("rows", [(record(), record()), (record(1), record(0)), (record(2),)])
def test_duplicate_unordered_and_out_of_range_rows_are_not_repaired(tmp_path, monkeypatch, rows):
    value, state = source(tmp_path, monkeypatch, rows)
    with pytest.raises(ProtocolError):
        value.read_window(BTC, START, START + 2 * MINUTE)
    assert state["opened"][0].closed


@pytest.mark.parametrize(
    "start,end",
    [
        (True, START),
        (START, START),
        (START + 1, START + MINUTE),
        (START, START + 4097 * MINUTE),
        (-MINUTE, START),
        (START, float(START + MINUTE)),
    ],
)
def test_invalid_window_never_opens_database(tmp_path, monkeypatch, start, end):
    value, state = source(tmp_path, monkeypatch, ())
    with pytest.raises(ValidationError):
        value.read_window(BTC, start, end)
    assert not state["opened"]


def test_clock_regression_is_not_hidden_by_clamping(tmp_path, monkeypatch):
    value, state = source(
        tmp_path, monkeypatch, (record(),), clocks=(START + 3 * MINUTE, START + 2 * MINUTE)
    )
    with pytest.raises(ProtocolError, match="regressed"):
        value.read_window(BTC, START, START + MINUTE)
    assert state["opened"][0].closed


@pytest.mark.parametrize("options", [{"fail": True}, {"fail_close": True}])
def test_query_and_close_failures_produce_no_result_or_secret(tmp_path, monkeypatch, options):
    value, state = source(tmp_path, monkeypatch, (record(),), **options)
    with pytest.raises(StorageError) as error:
        value.read_window(BTC, START, START + MINUTE)
    assert "secret" not in str(error.value)
    assert state["opened"][0].closed


def test_commit_identity_excludes_password_read_clock_and_decimal_spelling(tmp_path, monkeypatch):
    value, state = source(
        tmp_path, monkeypatch, [record()], clocks=[START + 3 * MINUTE + n for n in range(8)]
    )
    first = value.read_window(BTC, START, START + MINUTE)[0]
    state["settings"] = replace(state["settings"], password="different-password")
    # Same logical NUMERIC, distinct redundant scale spelling.
    state["opened"][0].rows[:] = [record(open=D("100.12"))]
    second = value.read_window(BTC, START, START + MINUTE)[0]
    assert first.commit_sha256 == second.commit_sha256
    assert first.available_at_ms < second.available_at_ms
    state["settings"] = replace(state["settings"], dbname="another_namespace")
    third = value.read_window(BTC, START, START + MINUTE)[0]
    assert third.commit_sha256 != second.commit_sha256
    value.close()
