"""Mixed financial facts, stable prefixes and bounded observational read tickets."""

import threading
import time
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from test_margin_session import BTC, bars, configuration, tables
from test_margin_strategy import setup
from test_margin_trading import Session, Source, await_value, configure

from omega.application.margin_funding import FundingPoint, prepare_funding_batch
from omega.domain.errors import ProtocolError, TradingBlocked, ValidationError
from omega.domain.margin import (
    CollateralTransfer,
    FillEvent,
    MarginAccount,
    MarginMode,
    MarginPosition,
    apply_fill,
    apply_funding,
    apply_transfer,
)
from omega.runtime import margin_ledger as module
from omega.runtime.margin_ledger import LedgerPages, LedgerQuery, MarginLedgerReads
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration
from omega.runtime.margin_store import (
    account_document,
    account_from_document,
    ledger_entry_document,
    ledger_entry_from_document,
)
from omega.runtime.margin_trading import MarginTradingService


def initial():
    return MarginAccount(
        "demo",
        "USDT",
        D(10000),
        (MarginPosition(BTC, "s", MarginMode.ISOLATED, 10, D(1000), isolated_wallet=D(1000)),),
    )


def append_fact(account, index):
    when = index * 60000
    if index % 3 == 0:
        quantity = D(1) if account.positions[0].quantity == 0 else D(-1)
        fact = FillEvent(
            f"fill{index}",
            f"order{index}",
            BTC,
            "s",
            quantity,
            D(100),
            D(".1"),
            "USDT",
            when,
            when + 60000,
        )
        return apply_fill(account, fact).account
    if index % 3 == 1:
        fact = CollateralTransfer(f"transfer{index}", BTC, "s", D("-.01"), when, when + 60000)
        return apply_transfer(account, fact).account
    return prepare_funding_batch(
        account,
        (FundingPoint(BTC, when, when + 1, when, D(".001"), D(100)),),
        as_of_ms=when + 60000,
    ).candidate_account


def mixed(count=150):
    account = initial()
    for index in range(count):
        account = append_fact(account, index)
    return account


def page(account, query=None, *, pages=None, anchor="a" * 64):
    return (pages or LedgerPages("b" * 64)).page(anchor, account, query or LedgerQuery())


def test_more_than128_mixed_typed_facts_exact_rows_and_stable_append_prefix():
    account = mixed()
    pages = LedgerPages("b" * 64)
    first = page(account, pages=pages)
    assert first["ledger_total"] == first["total"] == 150
    assert first["ledger_capacity"] == account.event_capacity
    assert len(first["rows"]) == first["next_offset"] == 128
    assert len(first["snapshot"]) <= 256
    assert {row["fact"]["kind"] for row in first["rows"]} == {"fill", "transfer", "funding"}
    grown = append_fact(account, 150)
    second = page(grown, LedgerQuery(offset=128, snapshot=first["snapshot"]), pages=pages)
    assert second["ledger_total"] == 150 and second["total"] == 150
    assert second["snapshot"] == first["snapshot"] and second["next_offset"] is None
    rows = first["rows"] + second["rows"]
    assert [row["sequence"] for row in rows] == list(range(1, 151))
    assert rows == account_document(account)["account"]["ledger"]
    assert [ledger_entry_from_document(row) for row in rows] == list(account.ledger)
    assert any(row["fact"].get("amount", "").startswith("-") for row in rows)
    assert page(grown, pages=pages)["ledger_total"] == 151


def test_filter_is_inside_frozen_prefix_and_offsets_do_not_change_selection():
    account = mixed(30)
    first = page(account, LedgerQuery(limit=4, start_ms=120000, end_ms=900000))
    assert first["ledger_total"] == 30 and first["total"] == 13
    assert [row["sequence"] for row in first["rows"]] == [3, 4, 5, 6]
    next_page = page(
        account,
        LedgerQuery(
            offset=4, limit=1000, snapshot=first["snapshot"], start_ms=120000, end_ms=900000
        ),
    )
    assert [row["sequence"] for row in next_page["rows"]] == list(range(7, 16))
    with pytest.raises(ProtocolError, match="selection changed"):
        page(account, LedgerQuery(snapshot=first["snapshot"], start_ms=120001, end_ms=900000))


@pytest.mark.parametrize("change", ["namespace", "anchor", "shorter", "interior"])
def test_another_lineage_shorter_or_changed_prefix_rejects_snapshot(change):
    account = mixed(12)
    original = page(account)
    chosen, anchor, namespace = account, "a" * 64, "b" * 64
    if change == "namespace":
        namespace = "c" * 64
    elif change == "anchor":
        anchor = "c" * 64
    elif change == "shorter":
        chosen = mixed(11)
    else:
        chosen = initial()
        for entry in account.ledger:
            fact = entry.fact
            if isinstance(fact, CollateralTransfer) and entry.sequence in {2, 5}:
                fact = replace(fact, amount=fact.amount + (D(1) if entry.sequence == 2 else D(-1)))
            apply = (
                apply_fill
                if isinstance(fact, FillEvent)
                else apply_transfer
                if isinstance(fact, CollateralTransfer)
                else apply_funding
            )
            chosen = apply(chosen, fact).account
        assert chosen.ledger[-1] == account.ledger[-1]
        assert chosen.cross_wallet == account.cross_wallet
    with pytest.raises(ProtocolError, match="prefix, lineage"):
        page(
            chosen,
            LedgerQuery(snapshot=original["snapshot"]),
            pages=LedgerPages(namespace),
            anchor=anchor,
        )


def test_append_hash_cache_does_not_serialize_whole_ledger_for_each_page(monkeypatch):
    account, pages = mixed(), LedgerPages("b" * 64)
    original = page(account, pages=pages)
    serializer = module.ledger_entry_document
    calls = []

    def observed(entry):
        calls.append(entry.sequence)
        return serializer(entry)

    monkeypatch.setattr(module, "ledger_entry_document", observed)
    page(account, LedgerQuery(offset=128, limit=2, snapshot=original["snapshot"]), pages=pages)
    assert calls == [129, 130]


@pytest.mark.parametrize(
    "arguments",
    [
        {"offset": True},
        {"offset": -1},
        {"offset": 10001},
        {"limit": 0},
        {"limit": 1001},
        {"limit": 1.0},
        {"start_ms": True},
        {"end_ms": -1},
        {"end_ms": 253402300799001},
        {"start_ms": 2, "end_ms": 2},
        {"snapshot": "a" * 257},
        {"snapshot": "bad"},
    ],
)
def test_invalid_native_queries_rejected_before_owner_access(arguments):
    with pytest.raises(ValidationError):
        LedgerQuery(**arguments)


def test_strict_row_decoder_and_account_versions_preserve_existing_bytes():
    account = mixed(9)
    document = account_document(account)
    assert account_document(account_from_document(document)) == document
    for row in document["account"]["ledger"]:
        assert ledger_entry_document(ledger_entry_from_document(row)) == row
    wrong = {**document, "schema_version": 1}
    with pytest.raises(ValidationError, match="funding"):
        account_from_document(wrong)


def test_engine_observation_is_original_verified_anchor_and_immutable_memory_only(monkeypatch):
    _, account_store, journal, _, execution, _ = setup()
    account = execution.snapshot().account

    def forbidden(*args, **kwargs):
        pytest.fail("ledger observation touched source IO or reconciliation")

    monkeypatch.setattr(journal, "read", forbidden)
    monkeypatch.setattr(account_store, "load", forbidden)
    monkeypatch.setattr(execution, "reconcile", forbidden)
    anchor, observed = execution.ledger_observation()
    assert observed is account
    assert anchor == execution._history[0].record.checksum
    execution._state = "unknown"
    with pytest.raises(TradingBlocked):
        execution.ledger_observation()


class Alive:
    def is_alive(self):
        return True


def ticket_owner(session):
    return SimpleNamespace(
        condition=threading.Condition(),
        closed=False,
        ready=True,
        error=None,
        thread=Alive(),
        data={"accounts": {"demo": {"creation_state": "created"}}},
        sessions={"demo": session},
        blocked_accounts=set(),
    )


def test_eight_ticket_bound_expiration_and_close_never_execute_abandoned_queries():
    calls = []
    owner = ticket_owner(SimpleNamespace(ledger_page=lambda **query: calls.append(query)))
    reader = MarginLedgerReads(owner)
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(reader.request("demo", timeout=0.3)))
        for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    await_value(lambda: len(reader.waiters), lambda value: value == 8)
    assert reader.request("demo", timeout=0.01)["reason_code"] == "read_queue_full"
    for thread in threads:
        thread.join(1)
    reader.serve_one()
    assert not calls and all(row["state"] == "unavailable" for row in results)
    assert not reader.queue and not reader.waiters
    thread = threading.Thread(target=lambda: results.append(reader.request("demo", timeout=1)))
    thread.start()
    await_value(lambda: len(reader.waiters), lambda value: value == 1)
    with owner.condition:
        owner.closed = True
        reader.fail("owner_closed")
    thread.join(1)
    reader.serve_one()
    assert not calls and results[-1]["reason_code"] == "owner_closed"


class NativeSession(MarginTradingSession):
    prepare_configuration = staticmethod(prepare_margin_configuration)

    def __init__(self, *args, create=False, **kwargs):
        super().__init__(*args, create=create, **kwargs)
        if create:
            self.resume("fixture-start")
            self.run_once(bars(0), as_of_ms=60000, brackets=tables())
            self.pause("fixture-pause")
            self.run_once(bars(1), as_of_ms=120000, brackets=tables())


def start_native(root):
    service = MarginTradingService(
        root, None, Source, core_epoch="ledger", session_factory=NativeSession
    )
    service.start()
    await_value(service.snapshot, lambda value: value["state"] == "ready")
    if not service.sessions:
        account, strategies = configuration()
        service.submit(
            "configure",
            "configure",
            {"account": account, "strategies": strategies, "start_ms": 0, "brackets": []},
        )
        await_value(
            lambda: service.status("configure"), lambda value: value["state"] == "completed"
        )
    return service


def test_actual_owner_worker_pages_without_fence_file_or_model_access_and_restart(
    tmp_path, monkeypatch
):
    service = start_native(tmp_path)
    try:
        with service.mutation_lock:
            with monkeypatch.context() as patch:

                def forbidden(*args, **kwargs):
                    pytest.fail("ledger GET executed a file or source callback")

                patch.setattr("builtins.open", forbidden)
                patch.setattr("pathlib.Path.open", forbidden)
                patch.setattr(service, "_guard", forbidden)
                first = service.read_ledger("session-test", limit=1)
        assert first["state"] == "ready" and first["ledger_total"] == 2
        assert first["rows"][0]["sequence"] == 1
        snapshot = first["snapshot"]
        assert service.read_ledger("missing")["state"] == "unavailable"
        before = service.operations.copy()
        second = service.read_ledger("session-test", offset=1, snapshot=snapshot)
        assert second["rows"][0]["sequence"] == 2
        assert service.operations == before
    finally:
        service.close()
    restored = start_native(tmp_path)
    try:
        resumed = restored.read_ledger("session-test", offset=1, snapshot=snapshot)
        assert resumed == second
    finally:
        restored.close()


@pytest.mark.parametrize("fault", ["unknown", "missing_registry", "wrong_account", "malformed"])
def test_worker_unknown_registry_or_invalid_result_is_not_confirmed_empty(fault):
    result = page(initial())
    calls = []
    if fault == "wrong_account":
        result["account_id"] = "other"
    elif fault == "malformed":
        result = {"private": "SECRET"}
    owner = ticket_owner(SimpleNamespace(ledger_page=lambda **query: calls.append(query) or result))
    if fault == "unknown":
        owner.blocked_accounts.add("demo")
    elif fault == "missing_registry":
        owner.data["accounts"].clear()
    reader = MarginLedgerReads(owner)
    observed = []
    thread = threading.Thread(target=lambda: observed.append(reader.request("demo", timeout=1)))
    thread.start()
    await_value(lambda: len(reader.waiters), lambda value: value == 1)
    reader.serve_one()
    thread.join(1)
    assert observed[0]["state"] == "unavailable" and observed[0]["ledger_total"] is None
    assert observed[0]["snapshot"] is None and "SECRET" not in str(observed)
    assert bool(calls) is (fault in {"wrong_account", "malformed"})


def test_worker_read_flood_does_not_starve_due_financial_tick_or_shutdown_observation(tmp_path):
    source, page_calls = Source(), []
    second_read_started = threading.Event()
    release_second_read = threading.Event()
    second_minute_consumed = threading.Event()

    class FloodSession(Session):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.book, self.pages = mixed(3), LedgerPages("b" * 64)

        def ledger_page(self, **query):
            page_calls.append(threading.get_ident())
            time.sleep(0.01)  # Bounded work in a fake read port; no financial IO.
            return page(self.book, LedgerQuery(**query), pages=self.pages)

        def run_once(self, candles, **parameters):
            super().run_once(candles, **parameters)
            if len(self.calls) >= 2:
                second_minute_consumed.set()

    def read(instruments, start, end, **kwargs):
        source.calls.append((instruments, start, end))
        if len(source.calls) == 2:
            second_read_started.set()
            assert release_second_read.wait(5)
        return (replace(bars(0)[0], open_time_ms=start, close_time_ms=end),)

    source.read = read
    runtime = MarginTradingService(
        tmp_path,
        None,
        lambda: source,
        core_epoch="flood",
        clock_ms=lambda: 180000,
        session_factory=FloodSession,
    )
    runtime.start()
    await_value(runtime.snapshot, lambda value: value["state"] == "ready")
    stop, responses = threading.Event(), []

    def reader():
        while not stop.is_set():
            responses.append(runtime.read_ledger("demo", timeout=0.5))

    threads = [threading.Thread(target=reader) for _ in range(8)]
    try:
        assert configure(runtime)["state"] == "completed"
        runtime.submit("resume", "resume", {"account_id": "demo"})
        await_value(lambda: runtime.status("resume"), lambda value: value["state"] == "completed")
        for thread in threads:
            thread.start()
        assert second_read_started.wait(5)
        # Observing a source read does not mean its minute reached the consumer.
        assert len(source.calls) == 2 and len(runtime.sessions["demo"].calls) == 1
        await_value(lambda: len(runtime._ledger_reads.waiters), lambda value: value == 8)
        release_second_read.set()
        assert second_minute_consumed.wait(5)
        await_value(lambda: len(page_calls), lambda value: value >= 10)
        observation = runtime.inspect_shutdown(timeout=0.5)
        assert "inspection_timeout" not in observation["unknown"]
        assert observation["ready"] is False  # Fake sessions have no financial authority.
        assert len(runtime.sessions["demo"].calls) >= 2
        assert runtime.sessions["demo"].next == 180000
        assert all(identity == runtime.thread.ident for identity in page_calls)
    finally:
        release_second_read.set()
        stop.set()
        runtime.close()
        for thread in threads:
            if thread.ident is not None:
                thread.join(2)
    assert not any(thread.is_alive() for thread in threads)
    assert not runtime._ledger_reads.waiters and not runtime._ledger_reads.queue
    assert responses and source.closed
