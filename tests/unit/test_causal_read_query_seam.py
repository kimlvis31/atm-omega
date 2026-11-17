"""Shared SQL and unchanged legacy ownership; fake handles, no database proof."""

from datetime import timedelta
from decimal import Decimal

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure import causal_market as module
from omega.infrastructure.candles import EPOCH

INSTRUMENT = Instrument("binance", Market.USDM, "TESTUSDT")
START = 1_700_000_040_000
SQL = (
    "SELECT instrument,open_time,close_time,o,h,l,c,v,source,received_ms "
    "FROM market.candles_1m WHERE instrument=%s AND open_time>=%s "
    "AND open_time<%s ORDER BY open_time LIMIT %s"
)


class Connection:
    def __init__(self, rows, events, failure=None):
        self.rows, self.events, self.failure = rows, events, failure
        self.calls = []

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters))
        stage = "readonly" if statement.startswith("SET") else "select"
        self.events.append(stage)
        if self.failure == stage or self.failure == "select_close" and stage == "select":
            raise RuntimeError("deliberate query secret")
        return self

    def fetchall(self):
        self.events.append("fetchall")
        if self.failure == "fetchall":
            raise RuntimeError("deliberate fetch secret")
        return self.rows


@pytest.mark.parametrize("count", [1, 2, 4096])
def test_shared_select_exact_sql_binds_single_execution_and_original_result(count):
    decimal = Decimal("100.000000000000000001")
    rows, events = [(INSTRUMENT.key, decimal)], []
    connection = Connection(rows, events)
    end = START + count * 60000
    result = module.select_committed_rows(connection, INSTRUMENT, START, end, count)
    assert result is rows and result[0][1] is decimal
    assert connection.calls == [
        (
            SQL,
            (
                INSTRUMENT.key,
                EPOCH + timedelta(milliseconds=START),
                EPOCH + timedelta(milliseconds=end),
                count + 1,
            ),
        )
    ]
    assert events == ["select", "fetchall"]  # No SET/factory/schema/close in the seam.


@pytest.mark.parametrize("count", [None, True, 0, -1, 4097, 1.0, "1"])
def test_shared_select_count_guard_precedes_execute(count):
    connection = Connection([], [])
    with pytest.raises(ValidationError):
        module.select_committed_rows(connection, INSTRUMENT, START, START + 60000, count)
    assert connection.calls == [] and connection.events == []


@pytest.mark.parametrize(
    ("start", "end", "count"),
    [
        (START, START + 60000, 2),
        (START + 1, START + 60000, 1),
        (START, START + 60001, 1),
        (False, 60000, 1),
        (0, True, 1),
        ("0", 60000, 1),
        (-60000, 0, 1),
    ],
)
def test_shared_select_alignment_duration_and_native_time_guard_precedes_execute(start, end, count):
    connection = Connection([], [])
    with pytest.raises(ValidationError):
        module.select_committed_rows(connection, INSTRUMENT, start, end, count)
    assert not connection.calls


def test_legacy_public_factory_signature_order_tuple_and_close_remain_exact(monkeypatch):
    settings, events = object(), []
    decimal = Decimal("0.000000000000000001")
    rows = [(INSTRUMENT.key, decimal)]
    connection = Connection(rows, events)
    closed, validations = [], []

    class Store:
        def __init__(self):
            self.connection = connection

        def close(self):
            closed.append(True)
            events.append("close")

    def factory(actual_settings):
        assert actual_settings is settings
        events.append("factory")
        return Store()

    original_validate = module.validate_window

    def validate(*args):
        validations.append(args)
        return original_validate(*args)

    monkeypatch.setattr(module, "validate_window", validate)
    result = module.read_committed_window(
        settings, INSTRUMENT, START, START + 60000, store_factory=factory
    )
    assert result == tuple(rows) and type(result) is tuple and result[0][1] is decimal
    assert validations == [(INSTRUMENT, START, START + 60000)]
    assert events == ["factory", "readonly", "select", "fetchall", "close"]
    assert closed == [True]
    assert connection.calls[0] == ("SET default_transaction_read_only=on", None)
    assert connection.calls[1] == (
        SQL,
        (
            INSTRUMENT.key,
            EPOCH + timedelta(milliseconds=START),
            EPOCH + timedelta(milliseconds=START + 60000),
            2,
        ),
    )


@pytest.mark.parametrize(
    "failure", ["factory", "readonly", "select", "fetchall", "close", "select_close"]
)
def test_legacy_public_query_and_finally_error_precedence_is_preserved(failure):
    events, closed = [], []
    connection = Connection([], events, failure)

    class Store:
        def __init__(self):
            self.connection = connection

        def close(self):
            closed.append(True)
            if failure in {"close", "select_close"}:
                raise RuntimeError("deliberate cleanup secret")

    def factory(settings):
        if failure == "factory":
            raise RuntimeError("deliberate factory secret")
        return Store()

    expected = (
        "committed inference market connection cleanup unconfirmed"
        if failure in {"close", "select_close"}
        else "committed inference market query unavailable"
    )
    with pytest.raises(StorageError) as caught:
        module.read_committed_window(None, INSTRUMENT, START, START + 60000, store_factory=factory)
    assert str(caught.value) == expected and "secret" not in str(caught.value)
    assert closed == ([] if failure == "factory" else [True])


def test_legacy_invalid_window_still_rejects_before_factory():
    called = []
    with pytest.raises(ValidationError):
        module.read_committed_window(
            None, INSTRUMENT, START + 1, START + 60000, store_factory=lambda _: called.append(True)
        )
    assert called == []
