"""Explicit isolated PG/default-source driver boundary; only owned UUID rows.

The imported fixture refuses every destination except127.0.0.1:55433/omega_test
as omega_app. Its existing TEST_CAUSAL_UUID cleanup remains unchanged. Deliberate
wrapper faults below are not claims of actual psycopg native-close failures.
"""

from decimal import Decimal as D

import pytest
from database_safety import isolated_test_port
from test_causal_market_postgres import (
    MINUTE,
    START,
    observation,
)
from test_causal_market_postgres import (
    configuration as configuration,
)
from test_causal_market_postgres import (
    database as database,
)

from omega.domain.errors import StorageError
from omega.infrastructure import causal_market_read as read_module
from omega.runtime.causal_market_source import (
    CommittedInferenceMarketSource,
    SourceReadRetirementSnapshot,
)
from omega.runtime.causal_nn_proof import wall_ms

pytestmark = pytest.mark.database


class _DriverBoundary:
    def __init__(self, native, *, close_failure=False):
        self.native, self.close_failure = native, close_failure
        self.calls, self.close_calls, self.closed_at_ms = [], 0, None
        self.readonly_observations, self.schema_returned = [], False

    @property
    def closed(self):
        return self.native.closed

    def execute(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        return self.native.execute(sql, parameters)

    def close(self):
        self.close_calls += 1
        self.native.close()
        self.closed_at_ms = wall_ms()
        if self.close_failure:
            raise OSError("deliberate wrapper exception after actual driver close")


def _default_driver_source(monkeypatch, template, *, close_failure=False, deny_write_key=None):
    original_connect, original_verify = read_module._connect, read_module._verify
    attempts, connections, denied = [], [], []
    template.close()  # Unused fixture source has no open DB resource.

    def connect(settings):
        assert (settings.host, settings.port, settings.dbname, settings.user) == (
            "127.0.0.1",
            isolated_test_port(),
            "omega_test",
            "omega_app",
        )
        attempts.append(True)
        connection = _DriverBoundary(original_connect(settings), close_failure=close_failure)
        connections.append(connection)
        return connection

    def verify(connection):
        connection.readonly_observations = [
            connection.execute("SHOW default_transaction_read_only").fetchone(),
            connection.execute("SHOW transaction_read_only").fetchone(),
        ]
        assert connection.readonly_observations == [("on",), ("on",)]
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
        original_verify(connection)  # Actual migration checksums/Timescale schema.
        connection.schema_returned = True
        if deny_write_key is not None:
            assert deny_write_key.symbol.startswith("TEST_CAUSAL_")
            try:
                # Only our fixture's UUID row; even a broken readonly boundary
                # would leave the value unchanged. No old/test-shared row is used.
                connection.execute(
                    "UPDATE market.candles_1m SET c=c WHERE instrument=%s", (deny_write_key.key,)
                )
            except Exception as error:
                denied.append(getattr(error, "sqlstate", None))
                assert denied[-1] == "25006"  # PostgreSQL read_only_sql_transaction.
                raise
            pytest.fail("actual PostgreSQL did not enforce the source read-only boundary")

    def clock():
        # The initial clock has no connection yet. Publication time must follow
        # every actual driver close, including repeated explicit source reads.
        if connections:
            assert all(item.native.closed for item in connections)
            assert all(item.close_calls == 1 for item in connections)
        return wall_ms()

    monkeypatch.setattr(read_module, "_connect", connect)
    monkeypatch.setattr(read_module, "_verify", verify)
    source = CommittedInferenceMarketSource(template.root, clock_ms=clock)
    assert not attempts
    return source, attempts, connections, denied


def test_actual_default_source_readonly_schema_exact_decimal_single_select_and_driver_close(
    database, monkeypatch
):
    _, instrument, store, template = database
    key = instrument()
    writer = store()
    writer.put(observation(key))
    writer.put(observation(key, 1))
    source, attempts, connections, _ = _default_driver_source(monkeypatch, template)
    try:
        first = source.read_window(key, START, START + 2 * MINUTE)
        after = wall_ms()
        assert len(first) == 2 and len(attempts) == len(connections) == 1
        assert first[0].row.open == D("100.123456789012345678901")
        assert first[0].row.volume == D("2.123456789012345678901")
        assert all(connections[0].closed_at_ms <= row.available_at_ms <= after for row in first)
        second = source.read_window(key, START, START + 2 * MINUTE)
        assert [row.commit_sha256 for row in first] == [row.commit_sha256 for row in second]
        assert len(attempts) == len(connections) == 2
        for connection in connections:
            assert connection.native.closed and connection.close_calls == 1
            assert connection.schema_returned
            assert connection.calls[0] == ("SET default_transaction_read_only=on", None)
            data_queries = [
                (sql, parameters)
                for sql, parameters in connection.calls
                if sql.startswith("SELECT instrument,open_time,close_time")
            ]
            assert len(data_queries) == 1
            assert data_queries[0][1][0] == key.key and data_queries[0][1][-1] == 3
        status = source.retirement_status()
        assert type(status) is SourceReadRetirementSnapshot
        assert not status.physical_confirmed  # Source admission is still open.
        assert status.read_scope.reads == status.read_scope.retired == 2
        assert all(item.physical_status == "confirmed" for item in status.read_scope.history)
    finally:
        retired = source.close()
    assert retired.physical_confirmed and retired.identity_intact
    assert retired.complete_default_factory and not retired.actor_recovery_authorized
    assert not retired.read_scope.durable_state_confirmed
    assert not retired.read_scope.all_kernel_resources_confirmed


def test_actual_server_readonly_rejects_write_to_only_owned_uuid_row_and_closes_driver(
    database, monkeypatch
):
    _, instrument, store, template = database
    key = instrument()
    store().put(observation(key))
    source, attempts, connections, denied = _default_driver_source(
        monkeypatch, template, deny_write_key=key
    )
    try:
        with pytest.raises(StorageError):
            source.read_window(key, START, START + MINUTE)
        assert denied == ["25006"]
        assert len(attempts) == len(connections) == 1
        assert connections[0].schema_returned and connections[0].native.closed
        assert connections[0].close_calls == 1
        receipt = source.retirement_status().read_scope
        assert receipt.failed_reads == receipt.retired == 1
        assert receipt.active is None and not receipt.admission_held
        assert receipt.history[0].physical_status == "confirmed"
    finally:
        assert source.close().physical_confirmed


def test_actual_driver_closed_then_deliberate_wrapper_fault_stays_unknown_without_next_connect(
    database, monkeypatch
):
    _, instrument, store, template = database
    key = instrument()
    store().put(observation(key))
    source, attempts, connections, _ = _default_driver_source(
        monkeypatch, template, close_failure=True
    )
    with pytest.raises(StorageError):
        source.read_window(key, START, START + MINUTE)
    original = connections[0]
    assert original.native.closed and original.close_calls == 1
    status = source.retirement_status()
    assert status.read_scope.admission_held and not status.physical_confirmed
    assert status.read_scope.active.destructive_unknown
    with pytest.raises(StorageError):
        source.read_window(key, START, START + MINUTE)
    for _ in range(2):
        with pytest.raises(StorageError):
            source.close()
    assert len(attempts) == len(connections) == original.close_calls == 1
    assert source._original_read_scope.active.original_connection is original
    assert not source.retirement_status().physical_confirmed
    # Only real driver-close return was observed. The wrapper's throwing close
    # and retained source evidence remain unknown; no no-op/reset grants ACK.
