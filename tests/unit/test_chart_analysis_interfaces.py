"""Cancellation cannot publish a partial native snapshot or gain device authority."""

from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal

import pytest

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure import analysis_frames
from omega.infrastructure.auxiliary_store import EPOCH, PostgresAuxiliaryStore
from omega.infrastructure.postgres.config import (
    DatabaseSettings,
    database_destination_id,
)
from omega.ports.auxiliary import AuxiliaryReadRange
from omega.runtime.service import CoreService

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
SPAN = AuxiliaryReadRange(BTC, "open_interest", BTC.key + ":open_interest_history", 0, 60000)


class Connection:
    def __init__(self, rows):
        self.rows = rows
        self.phase = "initial"
        self.rollback = self.committed = self.cursor_closed = False

    @contextmanager
    def transaction(self):
        try:
            yield
        except BaseException:
            self.rollback = True
            raise
        else:
            self.committed = True

    def execute(self, query, *args):
        self.phase = "query" if query.startswith("WITH selected") else "transaction"

    @contextmanager
    def cursor(self, *, name):
        try:
            yield self
        finally:
            self.cursor_closed = True

    def fetchmany(self, size):
        assert size == 64
        self.phase = "fetched"
        result, self.rows = self.rows, []
        return result


def native_store():
    observation = AuxiliaryInput(
        "open_interest",
        (("METRIC_OPENINTEREST", Decimal(100)),),
        0,
        60000,
        0,
        1,
        source=SPAN.source,
    )
    raw = encode_observation(BTC, observation)
    checksum = observation_checksum(raw)
    owner = object.__new__(PostgresAuxiliaryStore)
    owner.connection = Connection([(EPOCH, 1, checksum, None, raw, 1, 0, None, None)])
    owner._database_error = RuntimeError
    return owner


@pytest.mark.parametrize("phase", ["query", "fetched", "decoded"])
def test_cancel_during_native_batch_rolls_back_without_returning_a_partial_snapshot(
    phase,
):
    owner = native_store()
    original = owner._stored

    def decode(*args, **kwargs):
        result = original(*args, **kwargs)
        owner.connection.phase = "decoded"
        return result

    owner._stored = decode

    def stop():
        if owner.connection.phase == phase:
            raise InterruptedError("cancelled")

    with pytest.raises(InterruptedError, match="cancelled"):
        owner.read_revisions((SPAN,), as_of_ms=10, check_stop=stop)
    assert owner.connection.rollback and not owner.connection.committed
    assert owner.connection.cursor_closed


def test_stop_before_native_transaction_and_invalid_callback_never_query():
    owner = native_store()

    def stop():
        raise InterruptedError("before transaction")

    with pytest.raises(InterruptedError):
        owner.read_revisions((SPAN,), as_of_ms=10, check_stop=stop)
    with pytest.raises(ValidationError):
        owner.read_revisions((SPAN,), as_of_ms=10, check_stop=True)
    assert owner.connection.phase == "initial"
    assert not owner.connection.committed and not owner.connection.rollback


def source_owner(tmp_path, monkeypatch, *, timeout=60):
    settings = DatabaseSettings("127.0.0.1", 54329, "omega_test", "omega_app", "unused", timeout)
    current = [settings]
    monkeypatch.setattr(analysis_frames, "load_database_settings", lambda _: current[0])
    calls = []

    class Store:
        destination_id = database_destination_id(settings)

        def read_revisions(self, ranges, *, as_of_ms, limit, check_stop):
            calls.append((ranges, as_of_ms, limit))
            if check_stop:
                check_stop()
            return ()

        def close(self):
            calls.append("closed")

    store = Store()

    def factory(bound):
        calls.append(bound)
        return store

    return (
        analysis_frames.CommittedAnalysisFrameSource(tmp_path, store_factory=factory),
        current,
        calls,
        store,
    )


def test_chart_batch_caps_fresh_connection_without_changing_destination_or_settings(
    tmp_path, monkeypatch
):
    source, current, calls, _ = source_owner(tmp_path, monkeypatch)
    original = current[0]
    try:
        assert source.read_revisions((SPAN,), as_of_ms=10) == ()
        assert calls[0] == replace(original, connect_timeout_s=5)
        assert current[0] is original and original.connect_timeout_s == 60
        assert source.destination_id == database_destination_id(original)
    finally:
        source.close()


def test_changed_destination_during_batch_cannot_publish_selected_snapshot(tmp_path, monkeypatch):
    source, current, _, store = source_owner(tmp_path, monkeypatch)

    def changed(*args, **kwargs):
        current[0] = replace(current[0], dbname="changed")
        return ()

    store.read_revisions = changed
    try:
        with pytest.raises(ValidationError, match="destination changed"):
            source.read_revisions((SPAN,), as_of_ms=10)
    finally:
        source.close()


@pytest.mark.parametrize("ranges", [(), (SPAN, SPAN), (True,)])
def test_bad_chart_batch_never_constructs_storage(tmp_path, monkeypatch, ranges):
    source, _, calls, _ = source_owner(tmp_path, monkeypatch)
    try:
        with pytest.raises(ValidationError):
            source.read_revisions(ranges, as_of_ms=10)
        assert calls == []
    finally:
        source.close()


def test_stop_before_chart_source_connection(tmp_path, monkeypatch):
    source, _, calls, _ = source_owner(tmp_path, monkeypatch)

    def stop():
        raise InterruptedError("before connect")

    try:
        with pytest.raises(InterruptedError):
            source.read_revisions((SPAN,), as_of_ms=10, check_stop=stop)
        assert calls == []
    finally:
        source.close()


class Jobs:
    analysis_kinds = ("WOI", "MST", "STS")

    def __init__(self):
        self.cancelled = []

    def cancel(self, identity):
        if identity != "original":
            raise ValidationError("unknown original job")
        self.cancelled.append(identity)
        return {"id": identity, "state": "cancelling", "result": None}

    def close(self):
        pass


def test_cancel_api_is_owner_only_and_keeps_original_epoch_identity(tmp_path):
    jobs = Jobs()
    core = CoreService(tmp_path, chart_series=jobs)
    try:
        assert core.state()["core"]["capabilities"]["chart_analysis_kinds"] == list(
            jobs.analysis_kinds
        )
        row = core.request("POST", "/api/chart-series/original/cancel", core.token, {})
        assert row["id"] == "original" and row["state"] == "cancelling" and row["result"] is None
        assert row["core_epoch"] == core.control.snapshot()["epoch"]
        for body in (None, [], {"force": True}):
            with pytest.raises(ValidationError):
                core.request("POST", "/api/chart-series/original/cancel", core.token, body)
        with pytest.raises(ValidationError):
            core.request("POST", "/api/chart-series/original/cancel?force=1", core.token, {})
        pairing = core.control.create_pairing("phone")
        device = core.request("POST", "/api/pair", "", {"code": pairing["code"], "name": "phone"})
        with pytest.raises(PermissionError):
            core.request("POST", "/api/chart-series/original/cancel", device["token"], {})
        assert jobs.cancelled == ["original"]
    finally:
        core.close()
