"""Recovery-only actual Spot/USD-M adapters over synthetic memory HTTP; no writes."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
from urllib.parse import urlsplit

import pytest
import test_execution_intent_binance_bridge as bridge

from omega.application.execution_account import KIND as ACCOUNT_EVENT
from omega.application.execution_account import ExecutionAccountRecovery
from omega.application.execution_bindings import ExecutionDecisionBindings
from omega.application.orders import OrderCoordinator
from omega.domain.errors import JournalError, TradingBlocked
from omega.domain.execution_account import (
    ExecutionAccountOpening,
    apply_execution_fills,
    initial_execution_book,
)
from omega.domain.models import AccountSnapshot, Balance, Market
from omega.domain.safety import BlockReason, EntryGate
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient, Credentials
from omega.infrastructure.binance.spot import SpotAdapter
from omega.infrastructure.binance.transport import HttpResponse
from omega.infrastructure.binance.usdm import UsdmAdapter
from omega.infrastructure.journal import FailoverJournal, JsonlJournal
from omega.ports.journal import Durability


class AccountGateway(bridge.MemoryGateway):
    def __init__(self, sink, account_id, market):
        super().__init__(sink, account_id)
        self.market, self.now_ms = market, 120010
        self.fills, self.wallets, self.quantity = [], {}, "0"

    def send(self, request):
        assert request.method == "GET", "financial recovery must never dispatch/cancel"
        path = urlsplit(request.url).path
        if path.endswith(("/myTrades", "/userTrades", "/account")):
            self.requests.append(request)
            if path.endswith(("/myTrades", "/userTrades")):
                boundary = int(self.parameters(request).get("fromId", ["0"])[0])
                payload = [row for row in self.fills if row["id"] >= boundary]
            elif self.market == Market.SPOT:
                payload = {
                    "canTrade": True,
                    "balances": [
                        {"asset": asset, "free": amount, "locked": "0"}
                        for asset, amount in self.wallets.items()
                    ],
                }
            else:
                payload = {
                    "canTrade": True,
                    "assets": [
                        {
                            "asset": asset,
                            "walletBalance": amount,
                            "availableBalance": "0",
                            "marginBalance": amount,
                        }
                        for asset, amount in self.wallets.items()
                    ],
                    "positions": [
                        {
                            "symbol": "BTCUSDT",
                            "positionSide": "BOTH",
                            "positionAmt": self.quantity,
                            "entryPrice": "10000",
                            "unrealizedProfit": "123",
                        }
                    ],
                }
            return HttpResponse(200, json.dumps(payload).encode())
        return super().send(request)


def trade(identity, market):
    quantity, quote, fee, asset, pnl = (
        (".002", "20.0001", ".000002", "BTC", "1.25"),
        (".003", "30.0002", ".03", "USDT", "-.25"),
        (".002", "20.0003", ".00001", "BNB", "0"),
        (".003", "30.0004", "-.000002", "BNB", "0"),
    )[identity - 1]
    row = dict(
        symbol="BTCUSDT",
        id=identity,
        orderId=7,
        side="BUY",
        isBuyer=True,
        qty=quantity,
        price="10000",
        quoteQty=quote,
        commission=fee,
        commissionAsset=asset,
        time=120000 + identity,
    )
    if market == Market.USDM:
        row.update(realizedPnl=pnl, marginAsset="FDUSD")
    return row


def journals(root):
    return FailoverJournal(
        JsonlJournal(root / "primary.jsonl"),
        JsonlJournal(root / "fallback.jsonl"),
        lambda _code: pytest.fail("both synthetic journals unavailable"),
    )


def fixture(
    root,
    market,
    sink_factory=lambda sink: sink,
    *,
    bound=True,
    reader_factory=lambda reader: reader,
):
    sink = sink_factory(journals(root))
    account = "synthetic-bridge-owner"
    gateway = AccountGateway(sink, account, market)
    client = BinanceClient(
        "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com",
        gateway,
        credentials=Credentials("memory-test-key", "memory-test-secret"),
        budget=RequestBudget((Window("ip", 10000, 60), Window("orders", 100, 60))),
        ip_scope="ip",
        order_scope="orders",
        clock_ms=lambda: gateway.now_ms,
    )
    adapter = (
        SpotAdapter(account, client) if market == Market.SPOT else UsdmAdapter(account, client)
    )
    _cycle, _decision, compiled = bridge.compiled(account, bridge.context(market))
    if bound:
        assert (
            ExecutionDecisionBindings(account, sink)
            .bind(compiled, account_state_digest="b" * 64)
            .admitted
        )
    request = compiled.intents[0].request
    sink.record(
        {
            "kind": "OrderIntent",
            "account_id": account,
            "client_order_id": request.client_order_id,
            "instrument": request.instrument.key,
            "side": request.side.value,
            "order_kind": request.kind.value,
            "quantity": str(request.quantity),
            "price": None,
            "reducing": False,
            "reduce_only": False,
        }
    )
    gateway.orders[request.client_order_id] = dict(
        symbol="BTCUSDT",
        clientOrderId=request.client_order_id,
        orderId=7,
        status="PARTIALLY_FILLED",
        executedQty=".005",
        cummulativeQuoteQty="50.0003",
        cumQuote="50.0003",
    )
    initial = {
        "BTC": "0" if market == Market.SPOT else "1",
        "USDT": "1000",
        "BNB": "1",
        "FDUSD": "0",
    }
    anchor = ExecutionAccountOpening(
        AccountSnapshot(
            account,
            market,
            True,
            0,
            tuple(Balance(asset, D(amount), D(amount)) for asset, amount in initial.items()),
        ),
        request.instrument,
        "BTC",
        "USDT",
        None if market == Market.SPOT else "FDUSD",
        0,
    )
    gateway.fills = [trade(2, market), trade(1, market)]
    gateway.wallets = (
        {"BTC": ".004998", "USDT": "949.9697", "BNB": "1", "FDUSD": "0"}
        if market == Market.SPOT
        else {"BTC": ".999998", "USDT": "999.97", "BNB": "1", "FDUSD": "1"}
    )
    gateway.quantity = ".005"
    gate = EntryGate()
    gate.block(BlockReason.PAUSE)
    coordinator = OrderCoordinator(account, adapter, sink, gate)
    incidents = []
    recovery = ExecutionAccountRecovery(
        coordinator, reader_factory(adapter), sink, opening=anchor, on_incident=incidents.append
    )
    return recovery, coordinator, gateway, sink, anchor, incidents


def still_held(coordinator, gateway):
    operations = coordinator.restore_history()
    assert len(operations) == 1
    assert operations[0].state == "UNKNOWN" and operations[0].observation is None
    assert coordinator._remaining(operations[0]) == D(".01")
    assert {
        BlockReason.RESTART,
        BlockReason.PAUSE,
        BlockReason.EXECUTION,
    } <= coordinator.gate.reasons
    assert not coordinator.gate.allows_new_risk
    assert all(request.method == "GET" for request in gateway.requests)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_actual_adapter_partial_late_fills_and_reordered_restart_commit_assets_without_release(
    tmp_path, market
):
    recovery, coordinator, gateway, sink, anchor, _incidents = fixture(tmp_path, market)
    first = recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert first.matched and first.state == "primary_acknowledged"
    assert first.recovery_required and not first.manual_resume_ready
    assert first.book.revision == 2 and len(first.book.applied_fills) == 2
    still_held(coordinator, gateway)

    gateway.now_ms = 120020
    gateway.fills = [trade(4, market), trade(2, market), trade(3, market), trade(4, market)]
    gateway.orders[next(iter(gateway.orders))].update(
        status="FILLED", executedQty=".01", cummulativeQuoteQty="100.0010", cumQuote="100.0010"
    )
    gateway.wallets.update(BNB=".999992")
    if market == Market.SPOT:
        gateway.wallets.update(BTC=".009998", USDT="899.9690")
    gateway.quantity = ".01"
    final = recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert final.matched and final.book.revision == 4
    assert final.book.wallet("BNB") == D(".999992")
    assert final.book.wallet("USDT") == D("899.9690" if market == Market.SPOT else "999.97")
    assert dict(final.book.fees)["BNB"] == D(".000008")
    if market == Market.USDM:
        assert final.book.wallet("FDUSD") == D(1) and final.book.quantity == D(".01")
        assert final.book.book_cost is None
    still_held(coordinator, gateway)

    class ReorderedHistory:
        def records(self):
            rows = sink.records()
            return tuple(reversed(rows)) + rows

        def record(self, event):
            return sink.record(event)

    reopened_sink = ReorderedHistory()
    reopened_coordinator = OrderCoordinator(
        coordinator.account_id, coordinator.broker, reopened_sink, coordinator.gate
    )
    before = len(gateway.requests)
    reopened = ExecutionAccountRecovery(
        reopened_coordinator, coordinator.broker, reopened_sink, opening=anchor
    )
    assert len(gateway.requests) == before
    assert reopened.snapshot() == final.book
    assert reopened.status()["state"] == "historical"
    assert reopened.status()["pending_phase"] == "account"
    with pytest.raises(TradingBlocked):
        reopened.recover_once(observed_at_ms=gateway.now_ms)
    reack = reopened.retry_pending()
    assert len(gateway.requests) == before
    assert reack.book == final.book and reack.record_sha256 == final.record_sha256
    assert reack.state == "primary_acknowledged" and not reack.manual_resume_ready
    again = reopened.recover_once(observed_at_ms=gateway.now_ms)
    assert again.book == final.book and again.recovery_required and not again.manual_resume_ready
    assert len(again.book.applied_fills) == 4
    still_held(reopened_coordinator, gateway)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_unexplained_account_wallet_keeps_valid_owned_fills_but_never_authorizes_recovery(
    tmp_path, market
):
    recovery, coordinator, gateway, _sink, _anchor, incidents = fixture(tmp_path, market)
    gateway.wallets["USDT"] = "777"
    result = recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert not result.matched and result.recovery_required and not result.manual_resume_ready
    assert len(result.book.applied_fills) == 2 and result.book.wallet("USDT") != D(777)
    assert incidents
    still_held(coordinator, gateway)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_legacy_unbound_original_cannot_gain_financial_authority_from_matching_quantity(
    tmp_path, market
):
    recovery, coordinator, gateway, _sink, _anchor, _incidents = fixture(
        tmp_path, market, bound=False
    )
    with pytest.raises(TradingBlocked):
        recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert recovery.snapshot() is None
    still_held(coordinator, gateway)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_unowned_provider_fill_is_held_even_when_total_quantity_fits_original(tmp_path, market):
    recovery, coordinator, gateway, _sink, _anchor, _incidents = fixture(tmp_path, market)
    gateway.fills = [row | {"orderId": 999} for row in gateway.fills]
    with pytest.raises(TradingBlocked):
        recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert recovery.snapshot() is None
    assert BlockReason.EXTERNAL in coordinator.gate.reasons
    still_held(coordinator, gateway)


def uncertain_sink(failure):
    class UncertainAccountSink:
        def __init__(self, base):
            self.base, self.failing, self.attempts = base, True, []

        def records(self):
            return self.base.records()

        def record(self, event):
            if event.get("kind") == ACCOUNT_EVENT:
                self.attempts.append(deepcopy(event))
                if self.failing:
                    if failure == "after-append":
                        self.base.record(event)
                    elif failure == "fallback-only":
                        self.base.fallback.append(deepcopy(event))
                        return Durability.FALLBACK
                    raise OSError("synthetic primary account acknowledgement unavailable")
            return self.base.record(event)

    return UncertainAccountSink


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
@pytest.mark.parametrize("failure", ["before-append", "after-append", "fallback-only"])
def test_account_primary_ack_loss_retries_exact_saved_candidate_without_gateway_reads(
    tmp_path, market, failure
):
    recovery, coordinator, gateway, sink, _anchor, _incidents = fixture(
        tmp_path, market, uncertain_sink(failure)
    )
    with pytest.raises(TradingBlocked):
        recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert recovery.snapshot() is None
    assert recovery.status()["pending"] is True
    assert recovery.status()["pending_phase"] == "account"
    pending = deepcopy(sink.attempts[0])
    calls = len(gateway.requests)
    # Even a changed external view cannot replace the saved financial candidate.
    gateway.wallets["USDT"] = "777"
    gateway.now_ms += 100
    with pytest.raises(TradingBlocked):
        recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert len(gateway.requests) == calls and recovery.snapshot() is None
    sink.failing = False
    committed = recovery.retry_pending()
    assert committed.state == "primary_acknowledged" and committed.matched
    assert committed.book.revision == len(committed.book.applied_fills) == 2
    assert recovery.snapshot() == committed.book
    assert len(sink.attempts) == 2 and sink.attempts == [pending, pending]
    assert len(gateway.requests) == calls
    assert recovery.status()["pending"] is False and not committed.manual_resume_ready
    assert BlockReason.STORAGE in coordinator.gate.reasons
    still_held(coordinator, gateway)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
@pytest.mark.parametrize("failure", ["after-append", "fallback-only"])
def test_restart_reacknowledges_exact_last_candidate_before_any_fresh_query(
    tmp_path, market, failure
):
    recovery, coordinator, gateway, sink, anchor, _incidents = fixture(
        tmp_path, market, uncertain_sink(failure)
    )
    with pytest.raises(TradingBlocked):
        recovery.recover_once(observed_at_ms=gateway.now_ms)
    pending = deepcopy(sink.attempts[0])
    calls = len(gateway.requests)
    gate = EntryGate()
    gate.block(BlockReason.PAUSE)
    restarted_coordinator = OrderCoordinator(coordinator.account_id, coordinator.broker, sink, gate)
    restarted = ExecutionAccountRecovery(
        restarted_coordinator, coordinator.broker, sink, opening=anchor
    )
    historical = restarted.snapshot()
    assert historical.revision == 2
    assert restarted.status()["state"] == "historical" and restarted.status()["pending"]
    assert restarted.status()["pending_phase"] == "account"
    with pytest.raises(TradingBlocked):
        restarted.recover_once(observed_at_ms=gateway.now_ms)
    with pytest.raises(TradingBlocked):
        restarted.retry_pending()
    assert len(gateway.requests) == calls and restarted.snapshot() == historical
    sink.failing = False
    acknowledged = restarted.retry_pending()
    assert acknowledged.state == "primary_acknowledged" and acknowledged.book == historical
    assert sink.attempts == [pending, pending, pending]
    assert len(gateway.requests) == calls and not restarted.status()["pending"]
    assert acknowledged.recovery_required and not acknowledged.manual_resume_ready
    assert restarted.recover_once(observed_at_ms=gateway.now_ms).book == historical
    # Reack duplicates the existing durable event, not its in-memory chain sequence.
    assert sorted({row["sequence"] for row in sink.records() if row["kind"] == ACCOUNT_EVENT}) == [
        1,
        2,
    ]
    still_held(restarted_coordinator, gateway)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_numeric_match_with_account_observed_before_fills_commits_facts_but_stays_held(
    tmp_path, market
):
    def older(reader):
        class HistoricalAccountReader:
            def account(self):
                return replace(reader.account(), observed_at_ms=120000)

        return HistoricalAccountReader()

    recovery, coordinator, gateway, sink, _anchor, incidents = fixture(
        tmp_path, market, reader_factory=older
    )
    result = recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert len(result.book.applied_fills) == 2 and not result.matched
    assert recovery.snapshot() == result.book and recovery.status()["state"] == "held"
    assert result.recovery_required and not result.manual_resume_ready and incidents
    event = next(row for row in sink.records() if row["kind"] == ACCOUNT_EVENT)
    assert event["account_snapshot"]["observed_at_ms"] == 120000
    assert min(fill.timestamp_ms for fill in result.book.applied_fills) == 120001
    still_held(coordinator, gateway)


def test_replay_cannot_select_later_page_and_omit_known_financial_predecessors(tmp_path):
    recovery, coordinator, gateway, sink, anchor, _incidents = fixture(tmp_path, Market.SPOT)
    recovery.recover_once(observed_at_ms=gateway.now_ms)
    gateway.now_ms = 120020
    gateway.fills = [trade(index, Market.SPOT) for index in range(1, 5)]
    gateway.orders[next(iter(gateway.orders))].update(
        status="FILLED", executedQty=".01", cummulativeQuoteQty="100.0010", cumQuote="100.0010"
    )
    gateway.wallets.update(BTC=".009998", USDT="899.9690", BNB=".999992")
    recovery.recover_once(observed_at_ms=gateway.now_ms)

    class History:
        def __init__(self, rows):
            self.rows = deepcopy(rows)

        def records(self):
            return tuple(deepcopy(self.rows))

        def record(self, event):
            self.rows.append(deepcopy(event))
            return Durability.PRIMARY

    # A legitimate fill-only prefix can predate the first financial owner.
    history = History([row for row in sink.records() if row["kind"] != ACCOUNT_EVENT])
    fresh = OrderCoordinator(coordinator.account_id, coordinator.broker, history, EntryGate())
    owner = ExecutionAccountRecovery(fresh, coordinator.broker, history, opening=anchor)
    receipt = owner.recover_once(observed_at_ms=gateway.now_ms)
    event = deepcopy(next(row for row in history.rows if row["kind"] == ACCOUNT_EVENT))
    assert event["sequence"] == 1 and event["previous_sha256"] is None
    pages = [row for row in history.rows if row["kind"] == "ExecutionFillPage"]
    assert len(pages) == 3 and pages[-1]["trade_ids"] == ["4"]
    only = receipt.book.applied_fills[-1]
    subset = apply_execution_fills(initial_execution_book(anchor), anchor, (only,))

    def number(value):
        return None if value is None else format(value.normalize(), "f")

    # Keep every original source fact/page untouched; counterfeit ONLY the account
    # receipt's chosen references/book/snapshot to hide the first three facts.
    excluded = {
        sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        for row in history.rows
        if row["kind"] == "ExecutionFillPage"
        and row["revision"] < 3
        or row["kind"] == "ExecutionFillFact"
        and row["fill"]["trade_id"] != "4"
    }
    event["source_index"] = [key for key in event["source_index"] if key not in excluded]
    for field in ("quantity", "book_cost", "realized_gross_pnl", "base_fee_cost_in_quote"):
        event["book"][field] = number(getattr(subset, field))
    for field in ("wallets", "fees", "realized_pnl"):
        event["book"][field] = [[asset, number(value)] for asset, value in getattr(subset, field)]
    event["book"]["revision"] = 1
    event["book"]["applied_fills"] = [event["book"]["applied_fills"][-1]]
    for balance in event["account_snapshot"]["balances"]:
        balance["wallet"] = balance["available"] = number(subset.wallet(balance["asset"]))
    corrupt = History([row for row in history.rows if row["kind"] != ACCOUNT_EVENT] + [event])
    calls = len(gateway.requests)
    unknown = OrderCoordinator(coordinator.account_id, coordinator.broker, corrupt, EntryGate())
    with pytest.raises(JournalError):
        ExecutionAccountRecovery(unknown, coordinator.broker, corrupt, opening=anchor)
    assert len(gateway.requests) == calls


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_restart_keeps_durable_later_fill_tail_while_reacking_last_confirmed_book(tmp_path, market):
    recovery, coordinator, gateway, sink, anchor, _incidents = fixture(
        tmp_path, market, uncertain_sink("before-append")
    )
    sink.failing = False
    first = recovery.recover_once(observed_at_ms=gateway.now_ms)
    original_event = deepcopy(sink.attempts[-1])
    assert first.book.revision == 2 and original_event["sequence"] == 1

    gateway.now_ms = 120020
    gateway.fills = [trade(4, market), trade(2, market), trade(3, market)]
    gateway.orders[next(iter(gateway.orders))].update(
        status="FILLED", executedQty=".01", cummulativeQuoteQty="100.0010", cumQuote="100.0010"
    )
    gateway.quantity = ".01"
    gateway.wallets["BNB"] = ".999992"
    if market == Market.SPOT:
        gateway.wallets.update(BTC=".009998", USDT="899.9690")
    sink.failing = True
    with pytest.raises(TradingBlocked):
        recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert recovery.snapshot() == first.book
    assert sink.attempts[-1]["sequence"] == 2
    assert {row["sequence"] for row in sink.records() if row["kind"] == ACCOUNT_EVENT} == {1}
    durable_tail = tuple(row for row in sink.records() if row["kind"] != ACCOUNT_EVENT)
    assert any(row["kind"] == "ExecutionFillPage" and row["revision"] == 2 for row in durable_tail)
    assert any(
        row["kind"] == "ExecutionFillFact" and row["fill"]["trade_id"] == "4"
        for row in durable_tail
    )

    # Crash before account2 append leaves the original query/page/fill tail intact.
    calls = len(gateway.requests)
    gate = EntryGate()
    gate.block(BlockReason.PAUSE)
    restarted_coordinator = OrderCoordinator(coordinator.account_id, coordinator.broker, sink, gate)
    restarted = ExecutionAccountRecovery(
        restarted_coordinator, coordinator.broker, sink, opening=anchor
    )
    assert restarted.snapshot() == first.book and restarted.status()["pending_phase"] == "account"
    sink.failing = False
    reack = restarted.retry_pending()
    assert reack.book == first.book and reack.record_sha256 == first.record_sha256
    assert sink.attempts[-1] == original_event and len(gateway.requests) == calls
    assert tuple(row for row in sink.records() if row["kind"] != ACCOUNT_EVENT) == durable_tail

    final = restarted.recover_once(observed_at_ms=gateway.now_ms)
    assert final.matched and final.book.revision == len(final.book.applied_fills) == 4
    assert final.book.wallet("BNB") == D(".999992")
    assert final.book.wallet("USDT") == D("899.9690" if market == Market.SPOT else "999.97")
    assert sorted({row["sequence"] for row in sink.records() if row["kind"] == ACCOUNT_EVENT}) == [
        1,
        2,
    ]
    still_held(restarted_coordinator, gateway)
    still_held(coordinator, gateway)
