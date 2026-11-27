from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError
from omega.infrastructure.candles import DatabaseUnavailable, database_storage_error
from omega.runtime import collector_data_manager as manager


class FakeConnection:
    def __init__(self, *, closed=False, broken=False):
        self.closed = closed
        self.broken = broken


@pytest.mark.parametrize(
    "sqlstate, connection",
    [
        ("55P03", FakeConnection()),  # lock timeout
        ("57014", FakeConnection()),  # statement timeout/cancel
        ("08006", FakeConnection()),  # disconnected session
    ],
)
def test_database_storage_error_classifies_transient_failures(sqlstate, connection):
    error = SimpleNamespace(sqlstate=sqlstate)

    assert isinstance(
        database_storage_error(connection, error, "query failed"), DatabaseUnavailable
    )


def test_database_storage_error_keeps_other_sql_errors_as_storage_error():
    error = SimpleNamespace(sqlstate="23505")

    failure = database_storage_error(FakeConnection(), error, "query failed")

    assert type(failure) is StorageError


def test_auxiliary_route_discards_only_broken_auxiliary_and_reopens_next_call(monkeypatch):
    instrument = object()
    monkeypatch.setattr(manager, "auxiliary_from_document", lambda _document: (instrument, "row"))

    class FakeStore:
        def __init__(self, *, fails):
            self.connection = FakeConnection(closed=fails)
            self.closed = False
            self.fails = fails

        def put_live_batch(self, rows):
            assert rows == [(instrument, "row")]
            if self.fails:
                raise DatabaseUnavailable("synthetic transient failure")
            return "committed"

        def close(self):
            self.closed = True

    stores = []

    def make_store(*_args, **_kwargs):
        store = FakeStore(fails=not stores)
        stores.append(store)
        return store

    monkeypatch.setattr(manager, "PostgresAuxiliaryStore", make_store)
    ingestion = manager.DatabaseIngestion(settings=None, series={instrument: object()}, stop=None)

    live_bar_connection = FakeConnection()
    bar_store = SimpleNamespace(
        connection=live_bar_connection,
        close=lambda: pytest.fail("live bar store must remain open"),
    )
    ingestion.bars = SimpleNamespace(store=bar_store)
    args = {"rows": ["wire"]}

    with pytest.raises(DatabaseUnavailable):
        ingestion._handle("auxiliary", args)

    assert stores[0].closed
    assert ingestion.auxiliary is None
    assert ingestion.bars.store.connection is live_bar_connection
    assert not live_bar_connection.closed and not live_bar_connection.broken

    assert ingestion._handle("auxiliary", args) == "committed"
    assert len(stores) == 2
    assert ingestion.auxiliary is stores[1]
    assert not stores[1].closed
