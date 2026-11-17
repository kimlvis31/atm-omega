"""Offline known-scope join: real adapters/financial files, memory HTTP only.

The fixture explicitly owns its fixed opening ledger and original synthetic
intent. Its endpoint evidence never proves real-venue completeness or new risk.
"""

import json
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D
from urllib.parse import parse_qs, urlsplit

import pytest
import test_binance_open_order_inventory as inventory_fixture
import test_execution_account_adoption as adoption_fixture
import test_execution_account_binance as account_fixture

from omega.application.execution_account import ExecutionAccountRecovery
from omega.application.execution_control import KIND, ExecutionControlOwner
from omega.application.orders import OrderCoordinator
from omega.domain.errors import JournalError, TradingBlocked, ValidationError
from omega.domain.models import Market
from omega.domain.safety import BlockReason, EntryGate
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient, Credentials
from omega.infrastructure.binance.spot import SpotAdapter
from omega.infrastructure.binance.transport import HttpResponse
from omega.infrastructure.binance.usdm import UsdmAdapter
from omega.ports.journal import Durability


class InventoryTransport:
    def __init__(self, gateway):
        self.gateway = gateway
        self.ordinary, self.conditional = [], []

    def send(self, request):
        assert request.method == "GET" and request.body is None
        path, parameters = urlsplit(request.url).path, parse_qs(urlsplit(request.url).query)
        assert "symbol" not in parameters and "limit" not in parameters
        assert path.endswith(("/openOrders", "/openAlgoOrders"))
        self.gateway.requests.append(request)
        payload = self.conditional if path.endswith("/openAlgoOrders") else self.ordinary
        return HttpResponse(200, json.dumps(payload).encode(), received_at_ms=self.gateway.now_ms)


class ControlSink:
    """Faults only the new control event; financial/source receipts stay genuine."""

    def __init__(self, base):
        self.base, self.failure, self.attempts = base, None, []
        self.before_control = None

    def records(self):
        return self.base.records()

    def record(self, event):
        if event.get("kind") == KIND:
            if self.before_control is not None:
                self.before_control()
            self.attempts.append(deepcopy(event))
            if self.failure == "after-append":
                self.base.record(event)
            elif self.failure == "fallback-only":
                self.base.fallback.append(deepcopy(event))
                return Durability.FALLBACK
            if self.failure in ("before-append", "after-append"):
                raise OSError("synthetic control primary ACK unavailable")
        return self.base.record(event)


def make_control(
    recovery, gateway, *, incidents=None, clock=None, reader_wrapper=lambda value: value
):
    market = recovery.opening.instrument.market
    transport = InventoryTransport(gateway)
    origin = "https://api.binance.com" if market is Market.SPOT else "https://fapi.binance.com"
    client = BinanceClient(
        origin,
        transport,
        credentials=Credentials("memory-inventory-key", "memory-inventory-secret"),
        budget=RequestBudget((Window("ip", 10000, 60),)),
        ip_scope="ip",
        clock_ms=lambda: gateway.now_ms,
    )
    adapter = (
        SpotAdapter(recovery.account_id, client)
        if market is Market.SPOT
        else UsdmAdapter(recovery.account_id, client)
    )
    reader = reader_wrapper(adapter)
    owner = ExecutionControlOwner(
        recovery,
        ordinary_reader=reader,
        conditional_reader=reader if market is Market.USDM else None,
        required_inventory_kinds=("ordinary",)
        if market is Market.SPOT
        else ("ordinary", "conditional"),
        origin=origin,
        clock_ms=clock or (lambda: gateway.now_ms),
        on_incident=None if incidents is None else incidents.append,
    )
    return owner, transport, reader


def fixture(tmp_path, market, **kwargs):
    values = account_fixture.fixture(tmp_path, market, ControlSink)
    recovery, coordinator, gateway, sink, anchor, incidents = values
    owner, transport, reader = make_control(recovery, gateway, incidents=incidents, **kwargs)
    return owner, transport, reader, recovery, coordinator, gateway, sink, anchor, incidents


def unknown(coordinator):
    rows = coordinator.restore_history()
    assert rows and all(row.state == "UNKNOWN" for row in rows)
    assert all(coordinator._remaining(row) == row.request.quantity for row in rows)


def held(owner, coordinator):
    status = owner.status()
    assert status["paused"] and status["recovery_required"]
    assert not status["manual_resume_ready"] and not status["history_complete"]
    assert status["coverage"] == "endpoint_only" and not coordinator.gate.allows_new_risk
    assert {
        BlockReason.RESTART,
        BlockReason.PAUSE,
        BlockReason.EXECUTION,
    } <= coordinator.gate.reasons


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_actual_partial_late_facts_and_all_endpoint_inventories_join_only_while_paused(
    tmp_path, market
):
    owner, transport, _reader, recovery, coordinator, gateway, sink, _anchor, _incidents = fixture(
        tmp_path, market
    )
    first = owner.reconcile_once()
    assert first["state"] == "held" and first["revision"] == 1
    assert recovery.snapshot().revision == 2 and len(recovery.snapshot().applied_fills) == 2
    unknown(coordinator)
    assert "NONTERMINAL_ORIGINALS" in sink.attempts[-1]["issues"]
    adoption_fixture.terminal(gateway, market)
    sink.before_control = lambda: unknown(coordinator)  # PRIMARY must precede reservation release.
    second = owner.reconcile_once()
    sink.before_control = None
    assert second["state"] == "known_terminal_adopted_paused" and second["revision"] == 2
    book = recovery.snapshot()
    assert len(book.applied_fills) == 4
    assert dict(book.wallets) == {asset: D(value) for asset, value in gateway.wallets.items()}
    (operation,) = coordinator.restore_history()
    assert operation.state == "OBSERVED" and coordinator._remaining(operation) == 0
    assert operation.request.quantity == D(".01")
    assert operation.observation.executed_quantity == D(".01")
    record = sink.attempts[-1]
    assert len(record["inventories"]) == (1 if market is Market.SPOT else 2)
    assert all(row["orders"] == [] for row in record["inventories"])
    assert {row["provenance"]["endpoint_kind"] for row in record["inventories"]} == (
        {"ordinary"} if market is Market.SPOT else {"ordinary", "conditional"}
    )
    requests = len(gateway.requests)
    refused = owner.request_manual_resume("explicit-request", 2)
    assert refused["state"] == "manual_resume_refused" and refused["revision"] == 3
    assert sink.attempts[-1]["issues"] == ["UNSUPPORTED_ACCOUNT_COVERAGE"]
    assert sink.attempts[-1]["inventories"] == record["inventories"]
    assert owner.request_manual_resume("explicit-request", 2) == refused
    assert len(gateway.requests) == requests
    with pytest.raises(TradingBlocked):
        owner.request_manual_resume("explicit-request", 1)
    assert all(request.method == "GET" for request in gateway.requests)
    assert transport.ordinary == transport.conditional == []
    held(owner, coordinator)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
@pytest.mark.parametrize("failure", ["before-append", "after-append", "fallback-only"])
def test_control_ack_loss_preserves_exact_candidate_and_reservations_without_requery(
    tmp_path, market, failure
):
    owner, transport, _reader, recovery, coordinator, gateway, sink, _anchor, _incidents = fixture(
        tmp_path, market
    )
    adoption_fixture.terminal(gateway, market)
    sink.failure = failure
    with pytest.raises(TradingBlocked):
        owner.reconcile_once()
    original = deepcopy(sink.attempts[0])
    assert recovery.snapshot() is not None and len(recovery.snapshot().applied_fills) == 4
    unknown(coordinator)
    calls = len(gateway.requests)
    # Later venue differences must not replace or refresh the acknowledged round.
    transport.ordinary = [inventory_fixture.ordinary(market, symbol="ETHBTC")]
    gateway.wallets["USDT"] = "777"
    with pytest.raises(TradingBlocked):
        owner.reconcile_once()
    assert len(gateway.requests) == calls
    sink.failure = None
    result = owner.retry_pending()
    assert result["state"] == "known_terminal_adopted_paused" and result["revision"] == 1
    assert sink.attempts == [original, original] and len(gateway.requests) == calls
    assert len([row for row in sink.records() if row["kind"] == KIND]) >= 1
    (operation,) = coordinator.restore_history()
    assert operation.state == "OBSERVED" and coordinator._remaining(operation) == 0
    assert BlockReason.STORAGE in coordinator.gate.reasons
    held(owner, coordinator)


@pytest.mark.parametrize("scope", ["ordinary", "conditional"])
def test_unowned_other_symbol_inventory_blocks_external_before_terminal_adoption(tmp_path, scope):
    owner, transport, _reader, recovery, coordinator, gateway, sink, _anchor, incidents = fixture(
        tmp_path, Market.USDM
    )
    adoption_fixture.terminal(gateway, Market.USDM)
    if scope == "ordinary":
        transport.ordinary = [inventory_fixture.ordinary(Market.USDM, symbol="ETHBTC")]
    else:
        transport.conditional = [inventory_fixture.conditional()]
    sink.before_control = lambda: (
        unknown(coordinator),
        pytest.fail("EXTERNAL must precede control commit")
        if BlockReason.EXTERNAL not in coordinator.gate.reasons
        else None,
    )
    result = owner.reconcile_once()
    assert result["state"] == "external" and BlockReason.EXTERNAL in coordinator.gate.reasons
    assert recovery.snapshot() is not None and len(recovery.snapshot().applied_fills) == 4
    unknown(coordinator)
    assert any(event["code"] == "EXECUTION_CONTROL_EXTERNAL_INVENTORY" for event in incidents)
    assert sink.attempts[-1]["inventories"][scope == "conditional"]["orders"]
    held(owner, coordinator)


@pytest.mark.parametrize(
    "defect", ["old", "future", "wrong-account", "wrong-origin", "missing-conditional"]
)
def test_missing_stale_or_foreign_required_inventory_never_looks_empty_or_releases(
    tmp_path, defect
):
    class Reader:
        def __init__(self, base):
            self.base = base

        def open_orders(self):
            value = self.base.open_orders()
            source = value.provenance
            if defect == "old":
                source = replace(source, requested_at_ms=0, received_at_ms=0, completed_at_ms=0)
            elif defect == "future":
                source = replace(
                    source, requested_at_ms=999999, received_at_ms=999999, completed_at_ms=999999
                )
            elif defect == "wrong-account":
                source = replace(source, account_id="foreign")
            elif defect == "wrong-origin":
                source = replace(source, origin="https://elsewhere.invalid")
            return replace(value, provenance=source)

        def open_conditional_orders(self):
            if defect == "missing-conditional":
                raise TradingBlocked("required endpoint is unavailable, not empty")
            return self.base.open_conditional_orders()

    owner, _transport, _reader, recovery, coordinator, gateway, sink, _anchor, _incidents = fixture(
        tmp_path, Market.USDM, reader_wrapper=Reader
    )
    adoption_fixture.terminal(gateway, Market.USDM)
    with pytest.raises(TradingBlocked):
        owner.reconcile_once()
    assert recovery.snapshot() is not None and sink.attempts == []
    unknown(coordinator)
    held(owner, coordinator)


def test_expired_exact_retry_records_original_but_does_not_release_stale_reservations(tmp_path):
    owner, _transport, _reader, _recovery, coordinator, gateway, sink, _anchor, _incidents = (
        fixture(tmp_path, Market.SPOT)
    )
    adoption_fixture.terminal(gateway, Market.SPOT)
    sink.failure = "after-append"
    with pytest.raises(TradingBlocked):
        owner.reconcile_once()
    original, calls = deepcopy(sink.attempts[0]), len(gateway.requests)
    gateway.now_ms += 6000
    sink.failure = None
    assert owner.retry_pending()["state"] == "held_stale"
    assert sink.attempts == [original, original] and len(gateway.requests) == calls
    unknown(coordinator)


def test_restart_reack_is_historical_and_no_input_or_manual_ready_authority(tmp_path):
    owner, _transport, _reader, _recovery, coordinator, gateway, sink, anchor, _incidents = fixture(
        tmp_path, Market.USDM
    )
    adoption_fixture.terminal(gateway, Market.USDM)
    assert owner.reconcile_once()["state"] == "known_terminal_adopted_paused"
    calls, event = len(gateway.requests), deepcopy(sink.attempts[-1])
    gate = EntryGate()
    restarted_coordinator = OrderCoordinator(coordinator.account_id, coordinator.broker, sink, gate)
    restarted_recovery = ExecutionAccountRecovery(
        restarted_coordinator, coordinator.broker, sink, opening=anchor
    )
    restarted, _transport, _reader = make_control(restarted_recovery, gateway)
    assert restarted.status()["state"] == "historical" and restarted.status()["pending"]
    with pytest.raises(TradingBlocked):
        restarted_recovery.confirmed_inputs()
    assert restarted.retry_pending()["state"] == "historical"
    assert sink.attempts[-1] == event and len(gateway.requests) == calls
    unknown(restarted_coordinator)
    with pytest.raises(TradingBlocked):
        restarted.request_manual_resume("restart-request", 1)
    assert len(gateway.requests) == calls
    held(restarted, restarted_coordinator)


@pytest.mark.parametrize("change", ["reader", "sink", "source"])
def test_pending_candidate_original_source_cannot_be_replaced_before_reack(tmp_path, change):
    owner, _transport, _reader, recovery, coordinator, gateway, sink, _anchor, _incidents = fixture(
        tmp_path, Market.SPOT
    )
    adoption_fixture.terminal(gateway, Market.SPOT)
    sink.failure = "after-append"
    with pytest.raises(TradingBlocked):
        owner.reconcile_once()
    calls, attempts = len(gateway.requests), len(sink.attempts)
    if change == "reader":
        recovery.account_reader = object()
    elif change == "sink":
        recovery.sink = account_fixture.journals(tmp_path / "foreign")
    else:
        original = next(row for row in sink.records() if row["kind"] == "OrderIntent")
        changed = deepcopy(original)
        changed["quantity"] = ".02"
        sink.base.record(changed)
    sink.failure = None
    with pytest.raises((TradingBlocked, ValidationError, JournalError)):
        owner.retry_pending()
    assert len(gateway.requests) == calls and len(sink.attempts) == attempts
    assert owner.status()["pending"]
    assert not coordinator.gate.allows_new_risk


def test_confirmed_accessor_is_detached_and_callback_failure_cannot_undo_financial_ack(tmp_path):
    owner, transport, _reader, recovery, coordinator, gateway, sink, _anchor, _incidents = fixture(
        tmp_path, Market.SPOT
    )
    adoption_fixture.terminal(gateway, Market.SPOT)
    transport.ordinary = [inventory_fixture.ordinary(Market.SPOT, symbol="ETHBTC")]

    def unavailable(_event):
        raise OSError("synthetic notification unavailable")

    owner._on_incident = unavailable
    result = owner.reconcile_once()
    assert result["state"] == "external" and owner.status()["notification_failed"]
    first, second = recovery.confirmed_inputs(), recovery.confirmed_inputs()
    assert first == second and first is not second and first.account is not second.account
    with pytest.raises(FrozenInstanceError):
        first.observed_at_ms = 0
    assert first.receipt.book == recovery.snapshot() and first.receipt.matched
    assert first.receipt.record_sha256 == sink.attempts[-1]["account_sha256"]
    assert recovery.status()["pending"] is False
    unknown(coordinator)


@pytest.mark.parametrize("change", ["removed-original", "conflicting-pending"])
def test_control_retry_validates_original_chain_and_exact_partial_write(tmp_path, change):
    owner, _transport, _reader, _recovery, coordinator, gateway, sink, _anchor, _incidents = (
        fixture(tmp_path, Market.SPOT)
    )
    assert owner.reconcile_once()["state"] == "held"  # Original partial round, reservations intact.
    adoption_fixture.terminal(gateway, Market.SPOT)
    sink.failure = "after-append"
    with pytest.raises(TradingBlocked):
        owner.reconcile_once()
    saved_records = deepcopy(sink.records())
    if change == "removed-original":
        changed = tuple(
            row for row in saved_records if not (row["kind"] == KIND and row["revision"] == 1)
        )
    else:
        changed = list(saved_records)
        for row in changed:
            if row["kind"] == KIND and row["revision"] == 2:
                row["issues"] = ["NONTERMINAL_ORIGINALS"]
                row["state"] = "held"
    sink.records = lambda: deepcopy(changed)
    calls, attempts = len(gateway.requests), len(sink.attempts)
    sink.failure = None
    with pytest.raises(JournalError):
        owner.retry_pending()
    assert len(gateway.requests) == calls and len(sink.attempts) == attempts
    unknown(coordinator)
    assert owner.status()["pending"]


def test_unowned_evidence_is_not_lost_when_another_required_endpoint_is_unavailable(tmp_path):
    class Reader:
        def __init__(self, base):
            self.base = base

        def open_orders(self):
            return self.base.open_orders()

        def open_conditional_orders(self):
            raise TradingBlocked("synthetic required endpoint unavailable")

    owner, transport, _reader, recovery, coordinator, gateway, sink, _anchor, incidents = fixture(
        tmp_path, Market.USDM, reader_wrapper=Reader
    )
    adoption_fixture.terminal(gateway, Market.USDM)
    transport.ordinary = [inventory_fixture.ordinary(Market.USDM, symbol="ETHBTC")]
    with pytest.raises(TradingBlocked):
        owner.reconcile_once()
    assert BlockReason.EXTERNAL in coordinator.gate.reasons
    assert any(event["code"] == "EXECUTION_CONTROL_EXTERNAL_INVENTORY" for event in incidents)
    assert recovery.snapshot() is not None and sink.attempts == []
    unknown(coordinator)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_terminal_query_ahead_of_retained_fills_is_neutral_until_actual_adoption(tmp_path, market):
    owner, _transport, _reader, recovery, coordinator, gateway, sink, _anchor, _incidents = fixture(
        tmp_path, market
    )
    # The native order is terminal, but the fill endpoint and wallet still expose
    # exactly the first two original facts. Numeric book match is not coverage.
    gateway.orders[next(iter(gateway.orders))].update(
        status="FILLED", executedQty=".01", cummulativeQuoteQty="100.0010", cumQuote="100.0010"
    )
    with pytest.raises(TradingBlocked):
        owner.reconcile_once()
    assert len(recovery.snapshot().applied_fills) == 2
    assert dict(recovery.snapshot().wallets) == {
        asset: D(value) for asset, value in gateway.wallets.items()
    }
    assert sink.attempts[-1]["state"] == "observed_known_scope_paused"
    assert sink.attempts[-1]["issues"] == []
    assert any(row["kind"] == KIND for row in sink.records())  # Original control PRIMARY observed.
    assert owner.status()["state"] == "held" and owner.status()["revision"] == 1
    unknown(coordinator)
    calls, attempts = len(gateway.requests), len(sink.attempts)
    with pytest.raises(TradingBlocked):
        owner.request_manual_resume("incomplete-terminal", 1)
    assert len(gateway.requests) == calls and len(sink.attempts) == attempts
    held(owner, coordinator)


def test_backwards_clock_cannot_start_new_round_inside_prior_completed_interval(tmp_path):
    class CompletingReader:
        def __init__(self, base):
            self.base, self.calls = base, 0
            self.completions = iter((14000, 12000))

        def open_orders(self):
            self.calls += 1
            self.base.client.transport.gateway.now_ms = next(self.completions)
            return self.base.open_orders()

    owner, _transport, reader, recovery, coordinator, gateway, sink, _anchor, _incidents = fixture(
        tmp_path, Market.SPOT, reader_wrapper=CompletingReader
    )
    gateway.now_ms = 10000
    for row in gateway.fills:
        row["time"] = 9000 + row["id"]  # Explicit synthetic opening-compatible facts.
    assert owner.reconcile_once()["state"] == "held"  # Original partial order stays reserved.
    first = sink.attempts[-1]
    assert (first["started_at_ms"], first["completed_at_ms"]) == (10000, 14000)
    assert recovery.confirmed_inputs().account.observed_at_ms == 10000
    calls, attempts = len(gateway.requests), len(sink.attempts)
    gateway.now_ms = 11000  # Even >= previous account observation, it predates completion.
    with pytest.raises(TradingBlocked, match="predates prior completion"):
        owner.reconcile_once()
    assert len(gateway.requests) == calls and len(sink.attempts) == attempts
    assert reader.calls == 1  # The hypothetical next inventory's 12000 completion is never read.
    assert owner.status()["state"] == "held" and owner.status()["pending"] is False
    unknown(coordinator)
    held(owner, coordinator)
