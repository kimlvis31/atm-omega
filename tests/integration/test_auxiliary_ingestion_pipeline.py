"""Opt-in real archive/journal/PostgreSQL pipeline with public synthetic GET bytes.

No exchange transport is constructed. Only the explicitly isolated Omega test DB
may be used; every cleanup key belongs to this test's unique instrument/source.
"""

import json
import os
from contextlib import contextmanager
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from threading import Event
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.binance.auxiliary_client import BinanceAuxiliaryClient
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.transport import HttpResponse
from omega.infrastructure.candles import connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import migrate
from omega.infrastructure.raw_archive import FileRawArchive
from omega.ports.raw_data import PublicRawResponse
from omega.runtime.auxiliary_collect_cli import destination_id
from omega.runtime.auxiliary_collection import AuxiliaryCollection
from omega.runtime.auxiliary_journal import AuxiliaryJobJournal

pytestmark = pytest.mark.database
START = 1_700_000_040_000
END = START + 60_000
NOW = END + 120_000


@pytest.fixture(scope="module")
def settings(request):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated database opt-in")
    path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not path:
        pytest.fail("OMEGA_TEST_DB_CONFIG must select the isolated Omega test database")
    value = load_database_settings(Path(path).resolve())
    if (value.host, value.port, value.dbname, value.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing pipeline DB tests outside 127.0.0.1:55433/omega_test/omega_app")
    with connect_database(value) as connection:
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
        migrate(connection)
    return value


@pytest.fixture
def case(settings, tmp_path):
    instrument = Instrument("binance", Market.USDM, "TEST_AUXPIPE_" + uuid4().hex[:24].upper())
    source = instrument.key + ":aggTrade"
    store = PostgresAuxiliaryStore(settings)
    plan = {
        "instrument": instrument.key,
        "source": source,
        "start_ms": START,
        "end_ms": END,
        "limit": 500,
        "max_pages": 20,
        "destination_id": destination_id(settings),
    }
    value = {
        "instrument": instrument,
        "store": store,
        "plan": plan,
        "archive": FileRawArchive(tmp_path / "raw"),
        "jobs": tmp_path / "jobs",
        "journal": AuxiliaryJobJournal(tmp_path / "jobs", plan),
    }
    yield value
    store.close()
    with connect_database(settings) as connection:
        connection.execute(
            "DELETE FROM market.auxiliary_minutes "
            "WHERE instrument=%s AND channel=%s AND dataset=%s",
            (instrument.key, "trade_flow", source),
        )


class FakeTransport:
    def __init__(self, *bodies):
        self.bodies, self.calls = list(bodies), []

    def send(self, request):
        self.calls.append(request)
        assert request.method == "GET" and request.body is None
        assert not request.headers.get("X-MBX-APIKEY")
        assert self.bodies, "unexpected public HTTP after durable source replay"
        return HttpResponse(
            200,
            self.bodies.pop(0),
            {
                "Content-Type": "application/json",
                "X-MBX-USED-WEIGHT-1M": "20",
                "Set-Cookie": "synthetic-cookie-must-not-be-archived",
                "Authorization": "synthetic-header-must-not-be-archived",
            },
        )


def trade(identifier, when):
    return {
        "a": identifier,
        "f": identifier * 3,
        "l": identifier * 3 + 2,
        "p": "100.01",
        "q": ".03",
        "T": when,
        "m": False,
    }


def wire(rows=None):
    if rows is None:
        rows = [trade(1, START - 1), trade(2, START + 10_000), trade(3, END + 1)]
    # Whitespace survives the real client/archive; this is not a reserialized body.
    return (" \n" + json.dumps(rows, indent=1) + "\n ").encode()


def collection(case, transport, *, now=NOW, journal=None, stop_requested=lambda: False):
    budget = RequestBudget((Window("binance-ip", 1000, 60),), clock=lambda: now / 1000)
    client = BinanceAuxiliaryClient(
        Market.USDM,
        BinanceClient("https://fapi.binance.com", transport, budget=budget, clock_ms=lambda: now),
    )
    return AuxiliaryCollection(
        case["plan"],
        client,
        case["archive"],
        case["store"],
        case["journal"] if journal is None else journal,
        clock_ms=lambda: now,
        stop_requested=stop_requested,
    )


def stored(case, cutoff=NOW):
    return case["store"].get(
        case["instrument"], "trade_flow", case["plan"]["source"], START, as_of_ms=cutoff
    )


def row_count(case):
    return (
        case["store"]
        .connection.execute(
            "SELECT count(*) FROM market.auxiliary_minutes "
            "WHERE instrument=%s AND channel=%s AND dataset=%s",
            (case["instrument"].key, "trade_flow", case["plan"]["source"]),
        )
        .fetchone()[0]
    )


def assert_pinned(case, journal, expected):
    state = journal.snapshot()
    assert state["pending"] is None and state["next_ms"] == END
    assert len(state["deliveries"]) == 1
    pinned = state["deliveries"][0]
    assert pinned["batch_id"] == f"{journal.job_id}:{START}"
    assert (
        pinned["payload"] == encode_observation(case["instrument"], expected.observation).decode()
    )
    assert pinned["receipt"]["revision"] == expected.revision
    assert pinned["receipt"]["checksum"] == expected.checksum
    assert sha256(pinned["payload"].encode()).hexdigest() == expected.checksum
    assert pinned["committed_previous"] == expected.previous_checksum


def resume_without_http(case, *, now=NOW + 60_000):
    journal = AuxiliaryJobJournal(case["jobs"], case["plan"])
    transport = FakeTransport()
    result = collection(case, transport, now=now, journal=journal).run()
    assert transport.calls == []
    return result, journal


def test_wire_to_real_database_pin_and_completed_restart(case, settings):
    body = wire()
    transport = FakeTransport(body)
    result = collection(case, transport).run()
    assert result["complete"] and result["reason"] == "complete"
    assert result["quality_minutes"] == {"observed": 1, "partial": 0, "missing": 0}
    assert result["pages"] == len(transport.calls) == 1
    request = transport.calls[0]
    assert urlsplit(request.url).path == "/fapi/v1/aggTrades"
    assert parse_qs(urlsplit(request.url).query)["symbol"] == [case["instrument"].symbol]
    identity = case["journal"].snapshot()["batches"][0]
    archived = case["archive"].get(identity)
    assert archived.record.body == body
    assert archived.receipt.body_sha256 == sha256(body).hexdigest()
    assert archived.record.received_at_ms == NOW
    assert dict(archived.record.headers) == {
        "content-type": "application/json",
        "x-mbx-used-weight-1m": "20",
    }
    assert archived.record.instrument == case["instrument"]
    assert archived.record.channel == "agg_trades"
    value = stored(case)
    assert value.revision == 1 and value.previous_checksum is None
    assert value.observation.available_at_ms == NOW
    assert value.observation.event_time_ms == END
    assert value.observation.mapping() == {
        "AGGTRADE_QUANTITYBUY": Decimal(".03"),
        "AGGTRADE_QUANTITYSELL": Decimal(0),
        "AGGTRADE_NOTIONALBUY": Decimal("3.0003"),
        "AGGTRADE_NOTIONALSELL": Decimal(0),
        "AGGTRADE_NTRADESBUY": 3,
        "AGGTRADE_NTRADESSELL": 0,
        "AGGTRADE_EVENTCOUNT": 1,
    }
    assert stored(case, NOW - 1) is None
    assert_pinned(case, case["journal"], value)
    case["store"].close()
    case["store"] = PostgresAuxiliaryStore(settings)
    try:
        repeated, journal = resume_without_http(case)
        assert repeated["complete"] and row_count(case) == 1
        assert stored(case) == value
        assert_pinned(case, journal, value)
        assert case["archive"].get(identity).record.body == body
    finally:
        case["store"].close()


@pytest.mark.parametrize("mode", ["page_cap", "id_gap", "empty"])
def test_incomplete_public_ranges_remain_partial_or_missing(case, mode):
    if mode == "page_cap":
        case["plan"].update(limit=2, max_pages=1)
        case["journal"] = AuxiliaryJobJournal(case["jobs"], case["plan"])
        body = wire([trade(1, START - 1), trade(2, START + 10_000)])
    elif mode == "id_gap":
        body = wire([trade(1, START - 1), trade(3, START + 10_000), trade(4, END + 1)])
    else:
        body = b"[ ]"
    result = collection(case, FakeTransport(body)).run()
    assert result["target_committed"] and not result["complete"]
    assert result["reason"].startswith("target_committed_incomplete_coverage:")
    value = stored(case)
    assert value.observation.quality == ("missing" if mode == "empty" else "partial")
    assert value.observation.coverage == 0
    if mode == "empty":
        assert value.observation.mapping() == {}
    else:
        assert value.observation.mapping()["AGGTRADE_NOTIONALBUY"] == Decimal("3.0003")
        assert value.observation.mapping()["AGGTRADE_EVENTCOUNT"] == 1
    assert_pinned(case, case["journal"], value)


def test_adjacent_real_before_after_ids_prove_an_observed_zero_minute(case):
    body = wire([trade(1, START - 1), trade(2, END + 1)])
    result = collection(case, FakeTransport(body)).run()
    assert result["complete"]
    value = stored(case)
    assert value.observation.quality == "observed" and value.observation.coverage == 1
    assert len(value.observation.values) == 7
    assert all(number == 0 for number in value.observation.mapping().values())
    assert_pinned(case, case["journal"], value)


def test_real_commit_then_synthetic_ack_loss_resumes_the_frozen_write(case):
    import psycopg

    connection = case["store"].connection

    class LostCommitReply:
        @contextmanager
        def transaction(self):
            with connection.transaction():
                yield
            raise psycopg.OperationalError("synthetic ACK loss after actual DB commit")

        def __getattr__(self, name):
            return getattr(connection, name)

    case["store"].connection = LostCommitReply()
    try:
        first = collection(case, FakeTransport(wire())).run()
    finally:
        case["store"].connection = connection
    assert first["pending"] and first["commit_outcome"] == "unconfirmed"
    assert not first["target_committed"]
    pending = case["journal"].pending()
    assert pending.receipt is None and pending.observation.available_at_ms == NOW
    value = stored(case)
    assert value is not None and row_count(case) == 1
    second, journal = resume_without_http(case)
    assert second["complete"] and row_count(case) == 1
    assert stored(case) == value
    assert journal.snapshot()["deliveries"][0]["receipt"]["inserted"] is False
    assert_pinned(case, journal, value)


def test_journal_pin_failure_preserves_confirmed_pending_and_original_bytes(case, monkeypatch):
    original = case["journal"]._save
    failed = False

    def lose_first_delivery(candidate):
        nonlocal failed
        if candidate["deliveries"] and not failed:
            failed = True
            raise StorageError("synthetic delivery fsync failure")
        return original(candidate)

    monkeypatch.setattr(case["journal"], "_save", lose_first_delivery)
    body = wire()
    first = collection(case, FakeTransport(body)).run()
    assert failed and first["pending"]
    assert first["commit_outcome"] == "confirmed_pending_delivery"
    state = case["journal"].snapshot()
    pending = case["journal"].pending()
    assert pending.receipt is not None and pending.observation.available_at_ms == NOW
    value = stored(case)
    second, journal = resume_without_http(case)
    assert second["complete"] and row_count(case) == 1 and stored(case) == value
    assert case["archive"].get(state["batches"][0]).record.body == body
    assert_pinned(case, journal, value)


def test_stop_after_durable_stage_restarts_without_resampling_availability(case, monkeypatch):
    stopped = Event()
    original = case["journal"].stage

    def stage_then_stop(pending):
        original(pending)
        stopped.set()

    monkeypatch.setattr(case["journal"], "stage", stage_then_stop)
    first = collection(case, FakeTransport(wire()), stop_requested=stopped.is_set).run()
    assert first["reason"] == "stopped" and first["commit_outcome"] == "not_attempted"
    assert stored(case) is None and row_count(case) == 0
    pending = case["journal"].pending()
    assert pending.observation.available_at_ms == NOW
    second, journal = resume_without_http(case)
    assert second["complete"] and row_count(case) == 1
    value = stored(case)
    assert value.observation == pending.observation
    assert_pinned(case, journal, value)


def test_archived_foreign_symbol_is_rejected_before_any_database_write(case):
    foreign = Instrument("binance", Market.USDM, "FOREIGN_TEST")
    body = wire()
    receipt = case["archive"].put(
        PublicRawResponse(
            foreign,
            "agg_trades",
            (("limit", "500"), ("symbol", foreign.symbol)),
            NOW,
            body,
        )
    )
    case["journal"].record_batch(receipt)
    result = collection(case, FakeTransport()).run()
    assert result["reason"] == "collection_failed" and not result["target_committed"]
    assert result["error"] == "ProtocolError"
    assert row_count(case) == 0 and case["journal"].snapshot()["next_ms"] == START
    assert case["archive"].get(receipt.archive_id).record.body == body


def test_job_destination_binding_rejects_a_plan_for_another_database(case):
    changed = {**case["plan"], "destination_id": "0" * 64}
    transport = FakeTransport()
    case["plan"] = changed
    with pytest.raises(ValidationError, match="plan"):
        collection(case, transport)
    assert transport.calls == [] and row_count(case) == 0
