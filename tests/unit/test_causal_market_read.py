"""Prepared fake-connection resource tests; no actual DB or OS failure proof."""

import json
import threading
from dataclasses import FrozenInstanceError, asdict
from types import SimpleNamespace

import pytest

from omega.infrastructure import causal_market_read as module
from omega.infrastructure.causal_market_read import (
    InferenceReadScope,
    ReadCleanupError,
    ReadOperationError,
)


class Connection:
    def __init__(self):
        self.events = []
        self._closed = False
        self.close_calls = self.closed_faults = 0
        self.fail_close = self.post_fault = False
        self.close_hook = None

    def execute(self, statement):
        assert statement == "SET default_transaction_read_only=on"
        self.events.append("read_only")

    @property
    def closed(self):
        if self.closed_faults:
            self.closed_faults -= 1
            raise OSError("injected observation secret")
        return self._closed

    def close(self):
        self.close_calls += 1
        self.events.append("close")
        if self.close_hook:
            self.close_hook()
        self._closed = True
        if self.post_fault:
            self.closed_faults = 1
        if self.fail_close:
            raise OSError("injected native-return wrapper secret")


def setup_scope(*, history_limit=16):
    connections, state = [], {}

    def connect(settings):
        assert settings is state
        scope = state["scope"]
        assert scope.active is not None and scope.active.original_connection is None
        connection = Connection()
        connections.append(connection)
        return connection

    def verify(connection):
        assert state["scope"].active.original_connection is connection
        assert connection.events == ["read_only"]
        connection.events.append("verify")

    scope = InferenceReadScope(
        "read-generation", connect=connect, verify=verify, history_limit=history_limit
    )
    state["scope"] = scope
    return scope, state, connections


def query(connection):
    connection.events.append("query")
    return [("exact", "1.230000000000000001")]


def test_original_registered_readonly_before_verify_query_and_publish_after_close():
    scope, settings, connections = setup_scope()
    result = scope.read(settings, query)
    assert result == (("exact", "1.230000000000000001"),)
    assert connections[0].events == ["read_only", "verify", "query", "close"]
    receipt = scope.snapshot().history[0]
    assert receipt.connect_entered and receipt.connect_returned
    assert receipt.readonly_set_returned and receipt.schema_verified
    assert receipt.close_returned and receipt.post_closed is True
    assert scope.active is None and connections[0].close_calls == 1
    closed = scope.close()
    assert closed.physical_confirmed and not closed.actor_recovery_authorized
    assert not closed.durable_state_confirmed and not closed.all_kernel_resources_confirmed
    with pytest.raises(FrozenInstanceError):
        closed.physical_confirmed = False


@pytest.mark.parametrize("stage", ["read_only", "verify", "query"])
@pytest.mark.parametrize("cleanup_failure", [False, True])
def test_primary_and_secondary_failures_retained_sanitized(stage, cleanup_failure):
    scope, settings, connections = setup_scope()
    original_connect, original_verify = scope._connect, scope._verify

    def connect(value):
        connection = original_connect(value)
        connection.fail_close = cleanup_failure
        if stage == "read_only":

            def fail_set(_):
                raise RuntimeError("primary read-only secret")

            connection.execute = fail_set
        return connection

    def verify(connection):
        if stage == "verify":
            raise RuntimeError("primary verify secret")
        original_verify(connection)

    def operation(connection):
        if stage == "query":
            raise RuntimeError("primary query secret")
        return query(connection)

    scope._connect, scope._verify = connect, verify
    published = None
    with pytest.raises(ReadOperationError) as caught:
        published = scope.read(settings, operation)
    assert published is None and "secret" not in str(caught.value)
    snapshot = scope.snapshot()
    receipt = snapshot.active if cleanup_failure else snapshot.history[-1]
    assert receipt.primary_failure == "read_" + stage + "_failed"
    assert (
        receipt.cleanup_failure is not None if cleanup_failure else receipt.cleanup_failure is None
    )
    assert "secret" not in json.dumps(asdict(snapshot))
    assert connections[0].close_calls == 1
    if cleanup_failure:
        assert scope.active.original_connection is connections[0]
        for _ in range(2):
            with pytest.raises(ReadCleanupError):
                scope.close()
        assert connections[0].close_calls == 1
    else:
        assert scope.active is None
        # An explicit next read after known cleanup is allowed; there was no retry.
        scope._connect, scope._verify = original_connect, original_verify
        assert scope.read(settings, query)
        assert len(connections) == 2 and scope.close().physical_confirmed


@pytest.mark.parametrize("fault", ["native", "pre_observation", "post_observation"])
def test_cleanup_unknown_never_publishes_and_retry_only_original(fault):
    scope, settings, connections = setup_scope()

    def operation(connection):
        connection.fail_close = fault == "native"
        connection.closed_faults = 1 if fault == "pre_observation" else 0
        connection.post_fault = fault == "post_observation"
        return query(connection)

    published = None
    with pytest.raises(ReadCleanupError):
        published = scope.read(settings, operation)
    assert published is None and scope.active.original_connection is connections[0]
    first = scope.active.snapshot()
    assert first.operation_returned and first.candidate_count == 1
    with pytest.raises(ReadOperationError):
        scope.read(settings, query)
    assert len(connections) == 1
    if fault == "native":
        assert first.destructive_unknown
        connections[0].fail_close = False
        with pytest.raises(ReadCleanupError):
            scope.close()
        assert connections[0].close_calls == 1 and not scope.snapshot().physical_confirmed
    else:
        final = scope.close()
        assert final.physical_confirmed and final.admission_held
        assert connections[0].close_calls == 1
        assert final.history[-1].ever_uncertain


def test_hidden_constructor_connection_is_not_invented_or_recovered_by_reopen():
    hidden = []

    def hidden_factory(settings):
        hidden.append(Connection())
        raise RuntimeError("hidden constructor verification secret")

    scope = InferenceReadScope("hidden", connect=hidden_factory, verify=lambda _: None)
    with pytest.raises(ReadOperationError):
        scope.read(None, query)
    assert scope.active.original_connection is None
    assert scope.active.snapshot().acquisition_unknown
    with pytest.raises(ReadOperationError):
        scope.read(None, query)
    with pytest.raises(ReadCleanupError):
        scope.close()
    assert len(hidden) == 1 and not scope.snapshot().physical_confirmed
    hidden[0].close()  # Fault-fixture-only cleanup, not original-scope evidence.
    assert not scope.snapshot().physical_confirmed


@pytest.mark.parametrize("fault", ["method", "identity", "external_close"])
def test_original_identity_and_external_close_cannot_gain_ack(fault):
    scope, settings, connections = setup_scope()
    replacement, saved = Connection(), []

    def operation(connection):
        saved.append(connection.close)
        if fault == "method":
            connection.close = lambda: pytest.fail("replacement must not close")
        elif fault == "identity":
            scope.active._connection = replacement
        else:
            connection._closed = True
        return query(connection)

    with pytest.raises(ReadCleanupError):
        scope.read(settings, operation)
    assert scope.active.original_connection is connections[0]
    assert not scope.snapshot().physical_confirmed and replacement.close_calls == 0
    if not connections[0]._closed:
        saved[0]()  # Exact fake original, separate from the scope's unconfirmed ACK.
    assert connections[0]._closed
    with pytest.raises(ReadCleanupError):
        scope.close()


def test_deleted_class_close_remains_unknown_after_exact_original_method_restored(monkeypatch):
    scope, settings, connections = setup_scope()
    original_method = Connection.close
    with monkeypatch.context() as patch:

        def operation(connection):
            patch.delattr(Connection, "close")
            return query(connection)

        with pytest.raises(ReadCleanupError):
            scope.read(settings, operation)
        first = scope.active.snapshot()
        assert first.destructive_unknown and not first.close_attempted
        assert "original_close_method_missing" in first.history
        assert scope.active.original_connection is connections[0]
        assert connections[0].close_calls == 0
    assert Connection.close is original_method
    for _ in range(2):
        with pytest.raises(ReadCleanupError):
            scope.close()
    assert scope.active.original_connection is connections[0]
    assert scope.active.snapshot().destructive_unknown
    assert not scope.snapshot().physical_confirmed and connections[0].close_calls == 0
    assert connections[0]._closed is False  # Held fake has no OS resource to clean.


@pytest.mark.parametrize("fault", ["static_capture", "getter_capture"])
def test_capture_failure_retains_original_and_never_recaptures_close(monkeypatch, fault):
    capture_calls, connections = [], []

    class GetterConnection(Connection):
        @property
        def close(self):
            capture_calls.append("getter")
            raise RuntimeError("deliberate original close getter failure")

    connection = Connection() if fault == "static_capture" else GetterConnection()
    connections.append(connection)
    scope = InferenceReadScope(
        "capture-failure",
        connect=lambda _: connection,
        verify=lambda _: pytest.fail("capture must precede verify"),
    )
    original_static = module.inspect.getattr_static

    def failed_static(value, name, *args):
        if value is connection and name == "close":
            capture_calls.append("static")
            raise AttributeError("deliberate static capture failure")
        return original_static(value, name, *args)

    with monkeypatch.context() as patch:
        if fault == "static_capture":
            patch.setattr(module.inspect, "getattr_static", failed_static)
        with pytest.raises(ReadOperationError):
            scope.read(None, query)
    assert len(capture_calls) == 1
    assert scope.active.original_connection is connection and scope.active._close is None
    assert scope.active.snapshot().destructive_unknown
    if fault == "getter_capture":
        # Supplying a working method later is not trusted initial capture.
        monkeypatch.setattr(GetterConnection, "close", Connection.close)
    for _ in range(2):
        with pytest.raises(ReadCleanupError):
            scope.close()
    assert len(capture_calls) == 1 and connection.close_calls == 0
    assert scope.active.original_connection is connections[0]
    assert scope.active._close is None and not scope.snapshot().physical_confirmed


def test_transient_identity_observer_outage_allows_only_explicit_original_retry(monkeypatch):
    scope, settings, connections = setup_scope()
    original_static = module.inspect.getattr_static
    state = {"armed": False, "failures": 0}

    def transient_static(value, name, *args):
        if connections and value is connections[0] and name == "close" and state["armed"]:
            state["armed"] = False
            state["failures"] += 1
            raise OSError("deliberate identity observer outage, not method absence")
        return original_static(value, name, *args)

    def operation(connection):
        state["armed"] = True
        return query(connection)

    monkeypatch.setattr(module.inspect, "getattr_static", transient_static)
    with pytest.raises(ReadCleanupError):
        scope.read(settings, operation)
    first = scope.active.snapshot()
    assert not first.close_attempted and not first.destructive_unknown
    assert first.ever_uncertain and connections[0].close_calls == 0
    assert state["failures"] == 1 and scope.active.original_connection is connections[0]
    final = scope.close()
    assert final.physical_confirmed and final.admission_held
    assert final.history[-1].ever_uncertain and connections[0].close_calls == 1


def test_materialization_consumes_only_max_rows_plus_one_and_no_automatic_retry():
    scope, settings, connections = setup_scope()
    consumed = []

    def unbounded(connection):
        index = 0
        while True:
            consumed.append(index)
            yield (index,)
            index += 1

    with pytest.raises(ReadOperationError):
        scope.read(settings, unbounded, max_rows=3)
    assert consumed == [0, 1, 2, 3] and len(connections) == 1
    assert scope.active is None and scope.snapshot().failed_reads == 1
    assert scope.read(settings, query) and len(connections) == 2
    assert scope.close().physical_confirmed


@pytest.mark.parametrize("limit", [True, 0, -1, 4098, 1.0])
def test_row_limits_rejected_before_registration_connect(limit):
    scope, settings, connections = setup_scope()
    before = scope.snapshot()
    with pytest.raises(ValueError):
        scope.read(settings, query, max_rows=limit)
    assert scope.snapshot() == before and not connections


def test_actual_foreign_thread_and_same_numeric_ident_cannot_mutate(monkeypatch):
    scope, settings, connections = setup_scope()
    before, errors = scope.snapshot(), []

    def foreign():
        for operation in (lambda: scope.read(settings, query), scope.close, scope.snapshot):
            try:
                operation()
            except BaseException as error:
                errors.append(error)

    thread = threading.Thread(target=foreign)
    thread.start()
    thread.join(1)
    assert not thread.is_alive() and len(errors) == 3
    assert all(type(error) is RuntimeError for error in errors)
    assert scope.snapshot() == before and not connections
    for fault in ("pid", "thread"):
        with monkeypatch.context() as patch:
            if fault == "pid":
                patch.setattr(module.os, "getpid", lambda: scope._pid + 1)
            else:
                fake = SimpleNamespace(ident=threading.get_ident())
                patch.setattr(module.threading, "current_thread", lambda value=fake: value)
            with pytest.raises(RuntimeError):
                scope.close()
        assert scope.snapshot() == before
    assert scope.read(settings, query) and scope.close().physical_confirmed


@pytest.mark.parametrize("kind", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("stage", ["query", "close"])
def test_interrupt_retains_exact_object_family_scope_and_original(kind, stage):
    scope, settings, connections = setup_scope()
    primary = kind("deliberate interruption")

    def interrupt():
        raise primary

    def operation(connection):
        if stage == "query":
            interrupt()
        connection.close_hook = interrupt
        return query(connection)

    with pytest.raises(kind) as caught:
        scope.read(settings, operation)
    assert caught.value is primary and primary.read_scope is scope
    if stage == "close":
        assert scope.active.original_connection is connections[0]
        assert scope.active.snapshot().destructive_unknown
        with pytest.raises(ReadCleanupError):
            scope.close()
        assert connections[0].close_calls == 1
        # This deliberately held fake owns no OS handle. Do not alter its flag
        # to manufacture cleanup; actual driver containment is a later gate.
        assert connections[0]._closed is False
        assert not scope.snapshot().physical_confirmed
    else:
        assert scope.active is None and scope.close().physical_confirmed


def test_reentrant_native_close_cannot_become_confirmed_after_outer_return():
    scope, settings, connections = setup_scope()

    def operation(connection):
        def reenter():
            with pytest.raises(ReadCleanupError):
                scope.active.close()

        connection.close_hook = reenter
        return query(connection)

    with pytest.raises(ReadCleanupError):
        scope.read(settings, operation)
    receipt = scope.active.snapshot()
    assert receipt.close_returned and receipt.destructive_unknown
    assert receipt.physical_status == "unknown" and connections[0].close_calls == 1
    with pytest.raises(ReadCleanupError):
        scope.close()
    assert connections[0].close_calls == 1


def test_close_during_operation_does_not_close_active_connection_or_publish():
    scope, settings, connections = setup_scope()

    def operation(connection):
        with pytest.raises(ReadCleanupError, match="in progress"):
            scope.close()
        assert connection.close_calls == 0
        return query(connection)

    with pytest.raises(ReadOperationError):
        scope.read(settings, operation)
    assert connections[0].close_calls == 1
    assert scope.snapshot().physical_confirmed and scope.snapshot().admission_held


def test_confirmed_only_history_bounded_and_empty_close_is_positive_absence():
    scope, settings, connections = setup_scope(history_limit=2)
    for _ in range(5):
        assert scope.read(settings, query)
    receipt = scope.close()
    assert receipt.reads == receipt.retired == 5 and len(receipt.history) == 2
    assert receipt.active is None and receipt.physical_confirmed
    assert all(connection.close_calls == 1 for connection in connections)
    empty = InferenceReadScope("empty", connect=lambda _: pytest.fail("no connect"))
    assert not empty.snapshot().physical_confirmed
    assert empty.close().physical_confirmed and empty.snapshot().reads == 0
