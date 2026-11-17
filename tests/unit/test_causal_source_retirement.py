"""Default source lifecycle with deliberate fake connections; no driver/DB proof."""

import os
import threading
from dataclasses import FrozenInstanceError
from datetime import timedelta
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import test_causal_market_source as source_fixture

from omega.domain.errors import ProtocolError, StorageError
from omega.infrastructure import causal_market_read as read_module
from omega.infrastructure.candles import EPOCH
from omega.infrastructure.postgres.config import DatabaseSettings
from omega.runtime import causal_market_source as source_module
from omega.runtime.causal_market_source import (
    CommittedInferenceMarketSource,
    SourceReadRetirementSnapshot,
)

BTC, START, MINUTE = source_fixture.BTC, source_fixture.START, source_fixture.MINUTE


class _Connection:
    def __init__(self, rows, *, fail=None, close_failure=False):
        self.rows, self.fail, self.close_failure = rows, fail, close_failure
        self.closed, self.readonly, self.close_calls = False, False, 0
        self.calls, self.events = [], []

    def execute(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        if sql == "SET default_transaction_read_only=on":
            self.events.append("readonly")
            if self.fail == "readonly":
                raise OSError("private-readonly-fixture-secret")
            self.readonly = True
        else:
            self.events.append("select")
            if self.fail == "query":
                raise OSError("private-query-fixture-secret")
        return self

    def fetchall(self):
        return self.rows

    def close(self):
        self.close_calls += 1
        self.events.append("close")
        self.closed = True
        if self.close_failure:
            raise OSError("private-close-fixture-secret")


def _default_source(tmp_path, monkeypatch, factory, *, clock=None):
    settings = DatabaseSettings("127.0.0.1", 55433, "omega_test", "omega_app", "fake-only")
    attempts, opened, holder = [], [], {}

    def connect(actual_settings):
        assert actual_settings is settings
        attempts.append(threading.current_thread())
        connection = factory()
        opened.append(connection)
        connection.events.append("connect_returned")
        return connection

    def verify(connection):
        assert holder["source"]._original_read_scope.active.original_connection is connection
        assert connection.readonly  # Original connection captured before schema work.
        connection.events.append("verify")
        if connection.fail == "schema":
            raise OSError("private-schema-fixture-secret")

    monkeypatch.setattr(source_module, "load_database_settings", lambda _: settings)
    monkeypatch.setattr(read_module, "_connect", connect)
    monkeypatch.setattr(read_module, "_verify", verify)
    source = CommittedInferenceMarketSource(
        tmp_path, clock_ms=clock or (lambda: START + 5 * MINUTE)
    )
    holder["source"] = source
    assert not attempts and not opened
    return source, attempts, opened


def test_default_source_orders_readonly_schema_one_select_close_then_availability(
    tmp_path, monkeypatch
):
    connection = _Connection((source_fixture.record(), source_fixture.record(1)))
    clocks = []

    def clock():
        if clocks:
            assert connection.closed and connection.close_calls == 1
            connection.events.append("availability")
        clocks.append(True)
        return START + 5 * MINUTE + len(clocks)

    source, attempts, opened = _default_source(
        tmp_path, monkeypatch, lambda: connection, clock=clock
    )
    try:
        rows = source.read_window(BTC, START, START + 2 * MINUTE)
        assert opened == [connection] and len(attempts) == 1
        assert connection.events == [
            "connect_returned",
            "readonly",
            "verify",
            "select",
            "close",
            "availability",
        ]
        assert len(connection.calls) == 2
        sql, parameters = connection.calls[1]
        assert sql == (
            "SELECT instrument,open_time,close_time,o,h,l,c,v,source,received_ms "
            "FROM market.candles_1m WHERE instrument=%s AND open_time>=%s "
            "AND open_time<%s ORDER BY open_time LIMIT %s"
        )
        assert parameters == (
            BTC.key,
            EPOCH + timedelta(milliseconds=START),
            EPOCH + timedelta(milliseconds=START + 2 * MINUTE),
            3,
        )
        assert rows[0].row.open == D("100.12000")
        assert rows[0].row.volume == D("2.123456789012345678901")
        assert {row.available_at_ms for row in rows} == {START + 5 * MINUTE + 2}
        before = source.retirement_status()
        assert type(before) is SourceReadRetirementSnapshot
        assert type(before.read_scope) is read_module.ReadScopeReceipt
        assert before.identity_intact and before.complete_default_factory
        assert not before.source_closed and not before.physical_confirmed
        assert before.read_scope.retired == before.read_scope.reads == 1
        with pytest.raises(FrozenInstanceError):
            before.physical_confirmed = True
    finally:
        retired = source.close()
    assert retired.physical_confirmed and retired.source_closed
    assert retired.generation == before.generation and retired.scope_token == before.scope_token
    assert not retired.actor_recovery_authorized
    assert not retired.read_scope.durable_state_confirmed
    assert not retired.read_scope.all_kernel_resources_confirmed
    assert source.close() == retired and connection.close_calls == 1
    with pytest.raises(StorageError):
        source.read_window(BTC, START, START + MINUTE)
    assert len(attempts) == 1


@pytest.mark.parametrize("stage", ["readonly", "schema", "query"])
def test_confirmed_cleanup_after_operation_failure_allows_only_a_new_explicit_read(
    tmp_path, monkeypatch, stage
):
    connections = iter(
        (
            _Connection((source_fixture.record(),), fail=stage),
            _Connection((source_fixture.record(),)),
        )
    )
    source, attempts, opened = _default_source(tmp_path, monkeypatch, lambda: next(connections))
    try:
        with pytest.raises(StorageError) as error:
            source.read_window(BTC, START, START + MINUTE)
        assert "secret" not in str(error.value)
        assert len(attempts) == 1 and opened[0].closed and opened[0].close_calls == 1
        first = source.retirement_status().read_scope
        assert first.failed_reads == first.retired == first.reads == 1
        assert first.active is None and not first.admission_held
        assert first.history[0].primary_failure is not None
        assert first.history[0].physical_status == "confirmed"
        assert len(source.read_window(BTC, START, START + MINUTE)) == 1
        assert len(attempts) == 2 and opened[1].closed and opened[1].close_calls == 1
    finally:
        source.close()


@pytest.mark.parametrize("primary", [None, "schema"])
def test_destructive_close_failure_keeps_original_and_blocks_every_new_connection(
    tmp_path, monkeypatch, primary
):
    connection = _Connection((source_fixture.record(),), fail=primary, close_failure=True)
    source, attempts, opened = _default_source(tmp_path, monkeypatch, lambda: connection)
    with pytest.raises(StorageError) as error:
        source.read_window(BTC, START, START + MINUTE)
    assert "secret" not in str(error.value)
    status = source.retirement_status()
    original = source._original_read_scope.active.original_connection
    assert original is connection and opened == [connection]
    assert connection.closed and connection.close_calls == 1
    assert status.read_scope.admission_held and status.read_scope.active.destructive_unknown
    assert status.read_scope.active.cleanup_failure is not None
    assert (status.read_scope.active.primary_failure is not None) is (primary is not None)
    with pytest.raises(StorageError):
        source.read_window(BTC, START, START + MINUTE)
    for _ in range(2):
        with pytest.raises(StorageError):
            source.close()
        with pytest.raises(StorageError):
            source.read_window(BTC, START, START + MINUTE)
    assert source.retirement_status().source_closed
    assert not source.retirement_status().physical_confirmed
    assert len(attempts) == connection.close_calls == 1
    assert source._original_read_scope.active.original_connection is original


def test_connect_without_returned_original_is_held_without_invented_close(tmp_path, monkeypatch):
    def failed_connect():
        raise OSError("private-connect-fixture-secret")

    source, attempts, opened = _default_source(tmp_path, monkeypatch, failed_connect)
    with pytest.raises(StorageError):
        source.read_window(BTC, START, START + MINUTE)
    receipt = source.retirement_status().read_scope.active
    assert receipt.acquisition_unknown and receipt.connect_entered and not receipt.connect_returned
    assert (
        not receipt.close_attempted
        and source._original_read_scope.active.original_connection is None
    )
    with pytest.raises(StorageError):
        source.read_window(BTC, START, START + MINUTE)
    with pytest.raises(StorageError):
        source.close()
    assert len(attempts) == 1 and not opened
    assert not source.retirement_status().physical_confirmed


@pytest.mark.parametrize("foreign", ["thread", "pid"])
def test_source_owner_fences_precede_io_and_state_mutation(tmp_path, monkeypatch, foreign):
    source, attempts, _ = _default_source(tmp_path, monkeypatch, lambda: pytest.fail("foreign IO"))
    before = source.retirement_status()
    errors = []

    def call_foreign():
        for action in (
            lambda: source.read_window(BTC, START, START + MINUTE),
            source.close,
            source.retirement_status,
        ):
            try:
                action()
            except StorageError as error:
                errors.append(str(error))

    if foreign == "pid":
        with patch.object(source_module.os, "getpid", return_value=os.getpid() + 1):
            call_foreign()
    else:
        other = threading.Thread(target=call_foreign, daemon=True)
        other.start()
        other.join(timeout=2)
        assert not other.is_alive()
    assert len(errors) == 3 and not attempts
    assert source.retirement_status() == before
    assert source.close().physical_confirmed


@pytest.mark.parametrize("mode", ["never", "success", "failed"])
def test_legacy_factory_always_reports_incomplete_ownership_coverage(tmp_path, monkeypatch, mode):
    settings = DatabaseSettings("127.0.0.1", 55433, "omega_test", "omega_app", "fake-only")
    monkeypatch.setattr(source_module, "load_database_settings", lambda _: settings)
    opened = []

    def factory(_):
        if mode == "failed":
            raise OSError("deliberate legacy construction failure")
        store = source_fixture.Store((source_fixture.record(),))
        opened.append(store)
        return store

    source = CommittedInferenceMarketSource(
        tmp_path, store_factory=factory, clock_ms=lambda: START + 5 * MINUTE
    )
    if mode == "success":
        assert len(source.read_window(BTC, START, START + MINUTE)) == 1
        assert opened[0].closed and len(opened[0].calls) == 2
    elif mode == "failed":
        with pytest.raises(StorageError):
            source.read_window(BTC, START, START + MINUTE)
    status = source.close()
    assert not status.complete_default_factory and not status.physical_confirmed
    assert status.legacy_read_attempted is (mode != "never")
    assert status.read_scope.reads == 0 and status.source_closed
    assert not status.actor_recovery_authorized


def test_pre_first_read_legacy_selection_preserves_public_path_without_later_upgrade(
    tmp_path, monkeypatch
):
    source, attempts, _ = _default_source(
        tmp_path,
        monkeypatch,
        lambda: pytest.fail("legacy selection must not open a default connection"),
    )
    legacy = source_fixture.Store((source_fixture.record(),))
    source.store_factory = lambda _: legacy
    assert len(source.read_window(BTC, START, START + MINUTE)) == 1
    assert legacy.closed and len(legacy.calls) == 2
    status = source.retirement_status()
    assert status.identity_intact and not status.complete_default_factory
    assert status.legacy_read_attempted and not status.physical_confirmed
    assert status.read_scope.reads == 0 and not attempts
    source.store_factory = None  # Never turn an unregistered legacy read into default proof.
    with pytest.raises(StorageError):
        source.read_window(BTC, START, START + MINUTE)
    with pytest.raises(StorageError):
        source.close()
    assert not source.retirement_status().identity_intact
    assert not source.retirement_status().physical_confirmed
    assert source._original_read_scope.snapshot().physical_confirmed
    assert len(legacy.calls) == 2 and not attempts


@pytest.mark.parametrize("replacement", ["scope", "factory"])
def test_post_read_substitution_cleans_original_without_replacement_ack(
    tmp_path, monkeypatch, replacement
):
    connection = _Connection((source_fixture.record(),))
    source, attempts, _ = _default_source(tmp_path, monkeypatch, lambda: connection)
    assert len(source.read_window(BTC, START, START + MINUTE)) == 1
    original_scope = source._original_read_scope
    forbidden = SimpleNamespace(close=lambda: pytest.fail("replacement cleanup"))
    if replacement == "scope":
        source._read_scope = forbidden
    else:
        source.store_factory = lambda _: pytest.fail("replacement connect")
    with pytest.raises(StorageError):
        source.read_window(BTC, START, START + MINUTE)
    with pytest.raises(StorageError):
        source.close()
    assert original_scope.snapshot().physical_confirmed
    assert not source.retirement_status().identity_intact
    assert not source.retirement_status().physical_confirmed
    assert len(attempts) == connection.close_calls == 1


def test_invalid_observation_never_escapes_even_after_confirmed_default_cleanup(
    tmp_path, monkeypatch
):
    connection = _Connection((source_fixture.record(received=START + 6 * MINUTE),))
    source, _, _ = _default_source(tmp_path, monkeypatch, lambda: connection)
    try:
        with pytest.raises(ProtocolError):
            source.read_window(BTC, START, START + MINUTE)
        receipt = source.retirement_status().read_scope.history[0]
        assert connection.closed and connection.close_calls == 1
        assert receipt.physical_status == "confirmed"
    finally:
        assert source.close().physical_confirmed
