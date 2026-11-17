"""Known-original terminal reservation adoption, real adapters/memory HTTP only."""

from copy import deepcopy
from decimal import Decimal as D

import pytest
import test_execution_account_binance as account_fixture
import test_execution_intent_binance_bridge as bridge

from omega.application.execution_account import ExecutionAccountRecovery
from omega.application.execution_bindings import ExecutionDecisionBindings
from omega.application.orders import OrderCoordinator
from omega.domain.errors import JournalError, TradingBlocked, ValidationError
from omega.domain.models import Market
from omega.domain.safety import BlockReason, EntryGate

HELD = (TradingBlocked, ValidationError, JournalError)


def terminal(gateway, market):
    gateway.now_ms = 120020
    gateway.fills = [account_fixture.trade(index, market) for index in range(1, 5)]
    gateway.orders[next(iter(gateway.orders))].update(
        status="FILLED", executedQty=".01", cummulativeQuoteQty="100.0010", cumQuote="100.0010"
    )
    gateway.wallets.update(BNB=".999992")
    if market == Market.SPOT:
        gateway.wallets.update(BTC=".009998", USDT="899.9690")
    gateway.quantity = ".01"


def operations(coordinator):
    return {
        operation.request.client_order_id: (
            operation.request,
            operation.state,
            operation.observation,
            operation.terminal_review_required,
            coordinator._remaining(operation),
        )
        for operation in coordinator.restore_history()
    }


def assert_unknown(coordinator):
    rows = operations(coordinator)
    assert rows
    assert all(
        state == "UNKNOWN" and observation is None and not review and remaining == request.quantity
        for request, state, observation, review, remaining in rows.values()
    )
    assert not coordinator.gate.allows_new_risk
    return rows


def no_io_adopt(recovery, coordinator, gateway, sink):
    before_calls, before_records = len(gateway.requests), deepcopy(sink.records())
    before_gate = coordinator.gate.reasons
    recovery.adopt_confirmed_originals()
    assert len(gateway.requests) == before_calls
    assert sink.records() == before_records  # No additional durable authority/event.
    assert coordinator.gate.reasons == before_gate
    assert all(request.method == "GET" for request in gateway.requests)
    assert not coordinator.gate.allows_new_risk


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_terminal_exact_financial_facts_adopt_originals_once_without_io_or_resume(tmp_path, market):
    recovery, coordinator, gateway, sink, _anchor, _incidents = account_fixture.fixture(
        tmp_path, market
    )
    original = assert_unknown(coordinator)
    terminal(gateway, market)
    receipt = recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert receipt.matched and len(receipt.book.applied_fills) == 4
    assert receipt.recovery_required and not receipt.manual_resume_ready
    assert_unknown(coordinator)
    no_io_adopt(recovery, coordinator, gateway, sink)
    adopted = operations(coordinator)
    assert adopted.keys() == original.keys()
    for identity, row in adopted.items():
        assert row[0] == original[identity][0]
        assert row[1] == "OBSERVED" and row[2].status.value == "FILLED"
        assert row[2].executed_quantity == D(".01") and row[2].cumulative_quote == D("100.0010")
        assert row[3] is False and row[4] == 0
    no_io_adopt(recovery, coordinator, gateway, sink)
    assert operations(coordinator) == adopted
    assert recovery.status()["recovery_required"] is True
    assert recovery.status()["manual_resume_ready"] is False
    assert {
        BlockReason.RESTART,
        BlockReason.PAUSE,
        BlockReason.EXECUTION,
    } <= coordinator.gate.reasons


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_cancelled_partial_releases_only_after_original_facts_are_committed(tmp_path, market):
    recovery, coordinator, gateway, sink, _anchor, _incidents = account_fixture.fixture(
        tmp_path, market
    )
    gateway.orders[next(iter(gateway.orders))]["status"] = "CANCELED"
    receipt = recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert receipt.matched and len(receipt.book.applied_fills) == 2
    assert_unknown(coordinator)
    no_io_adopt(recovery, coordinator, gateway, sink)
    (row,) = operations(coordinator).values()
    assert row[2].status.value == "CANCELED" and row[2].executed_quantity == D(".005")
    assert row[2].cumulative_quote == D("50.0003") and row[4] == 0


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_zero_execution_cancel_does_not_invent_facts_or_cash_delta(tmp_path, market):
    recovery, coordinator, gateway, sink, anchor, _incidents = account_fixture.fixture(
        tmp_path, market
    )
    gateway.fills = []
    gateway.wallets = {row.asset: str(row.wallet) for row in anchor.account.balances}
    gateway.quantity = "0"
    gateway.orders[next(iter(gateway.orders))].update(
        status="CANCELED", executedQty="0", cummulativeQuoteQty="0", cumQuote="0"
    )
    receipt = recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert receipt.matched and receipt.book.applied_fills == ()
    no_io_adopt(recovery, coordinator, gateway, sink)
    assert all(row[4] == 0 for row in operations(coordinator).values())


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
@pytest.mark.parametrize(
    "defect", ["still-partial", "quantity-gap", "quote-gap", "account-mismatch"]
)
def test_primary_matched_or_recorded_book_is_not_sufficient_adoption_authority(
    tmp_path, market, defect
):
    recovery, coordinator, gateway, sink, _anchor, _incidents = account_fixture.fixture(
        tmp_path, market
    )
    order = gateway.orders[next(iter(gateway.orders))]
    if defect == "quantity-gap":
        order.update(
            status="FILLED", executedQty=".01", cummulativeQuoteQty="100.0010", cumQuote="100.0010"
        )
    elif defect == "quote-gap":
        order.update(status="CANCELED", cummulativeQuoteQty="50.0013", cumQuote="50.0013")
    elif defect == "account-mismatch":
        terminal(gateway, market)
        gateway.wallets["USDT"] = "777"
    receipt = recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert receipt.state == "primary_acknowledged"
    assert receipt.matched is (defect != "account-mismatch")
    before, calls, records = (
        assert_unknown(coordinator),
        len(gateway.requests),
        deepcopy(sink.records()),
    )
    with pytest.raises(HELD):
        recovery.adopt_confirmed_originals()
    assert operations(coordinator) == before and recovery.snapshot() == receipt.book
    assert len(gateway.requests) == calls and sink.records() == records


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
@pytest.mark.parametrize("failure", ["before-append", "after-append", "fallback-only"])
def test_pending_account_candidate_cannot_release_before_fresh_primary_ack(
    tmp_path, market, failure
):
    recovery, coordinator, gateway, sink, _anchor, _incidents = account_fixture.fixture(
        tmp_path, market, account_fixture.uncertain_sink(failure)
    )
    terminal(gateway, market)
    with pytest.raises(TradingBlocked):
        recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert recovery.status()["pending_phase"] == "account"
    pending = deepcopy(sink.attempts[-1])
    before, calls, records = (
        assert_unknown(coordinator),
        len(gateway.requests),
        deepcopy(sink.records()),
    )
    with pytest.raises(HELD):
        recovery.adopt_confirmed_originals()
    assert operations(coordinator) == before
    assert len(gateway.requests) == calls and sink.records() == records
    sink.failing = False
    receipt = recovery.retry_pending()
    assert receipt.matched and receipt.state == "primary_acknowledged"
    assert sink.attempts == [pending, pending] and len(gateway.requests) == calls
    assert BlockReason.STORAGE in coordinator.gate.reasons
    no_io_adopt(recovery, coordinator, gateway, sink)
    assert all(row[4] == 0 for row in operations(coordinator).values())
    assert BlockReason.STORAGE in coordinator.gate.reasons


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_restart_and_account_only_reack_require_fresh_order_queries_before_adoption(
    tmp_path, market
):
    recovery, coordinator, gateway, sink, anchor, _incidents = account_fixture.fixture(
        tmp_path, market
    )
    terminal(gateway, market)
    original = recovery.recover_once(observed_at_ms=gateway.now_ms)
    no_io_adopt(recovery, coordinator, gateway, sink)
    fresh_gate = EntryGate()
    fresh_gate.block(BlockReason.PAUSE)
    restarted_coordinator = OrderCoordinator(
        coordinator.account_id, coordinator.broker, sink, fresh_gate
    )
    calls = len(gateway.requests)
    restarted = ExecutionAccountRecovery(
        restarted_coordinator, coordinator.broker, sink, opening=anchor
    )
    assert restarted.snapshot() == original.book and restarted.status()["state"] == "historical"
    assert_unknown(restarted_coordinator)
    with pytest.raises(HELD):
        restarted.adopt_confirmed_originals()
    reack = restarted.retry_pending()
    assert reack.book == original.book and reack.state == "primary_acknowledged"
    assert len(gateway.requests) == calls
    with pytest.raises(HELD):
        restarted.adopt_confirmed_originals()
    assert_unknown(restarted_coordinator)
    assert len(gateway.requests) == calls
    current = restarted.recover_once(observed_at_ms=gateway.now_ms)
    assert current.book == original.book and len(gateway.requests) > calls
    no_io_adopt(restarted, restarted_coordinator, gateway, sink)
    assert all(row[4] == 0 for row in operations(restarted_coordinator).values())


def test_foreign_second_owner_cannot_replace_once_bound_owner_on_same_coordinator(tmp_path):
    recovery, coordinator, gateway, sink, anchor, _incidents = account_fixture.fixture(
        tmp_path, Market.SPOT
    )
    before, calls, records = (
        assert_unknown(coordinator),
        len(gateway.requests),
        deepcopy(sink.records()),
    )
    with pytest.raises(HELD):
        ExecutionAccountRecovery(coordinator, coordinator.broker, sink, opening=anchor)
    assert operations(coordinator) == before
    assert len(gateway.requests) == calls and sink.records() == records
    terminal(gateway, Market.SPOT)
    recovery.recover_once(observed_at_ms=gateway.now_ms)
    no_io_adopt(recovery, coordinator, gateway, sink)


def test_caller_receipt_boolean_or_digest_cannot_supply_adoption_authority(tmp_path):
    recovery, coordinator, gateway, sink, _anchor, _incidents = account_fixture.fixture(
        tmp_path, Market.SPOT
    )
    terminal(gateway, Market.SPOT)
    receipt = recovery.recover_once(observed_at_ms=gateway.now_ms)
    before = assert_unknown(coordinator)
    with pytest.raises(TypeError):
        recovery.adopt_confirmed_originals(receipt)
    with pytest.raises(TypeError):
        recovery.adopt_confirmed_originals(matched=True, record_sha256=receipt.record_sha256)
    assert operations(coordinator) == before
    no_io_adopt(recovery, coordinator, gateway, sink)


def test_external_hold_preserves_all_reservations(tmp_path):
    recovery, coordinator, gateway, sink, _anchor, _incidents = account_fixture.fixture(
        tmp_path, Market.SPOT
    )
    terminal(gateway, Market.SPOT)
    recovery.recover_once(observed_at_ms=gateway.now_ms)
    before, calls = assert_unknown(coordinator), len(gateway.requests)
    coordinator.gate.block(BlockReason.EXTERNAL)
    with pytest.raises(HELD):
        recovery.adopt_confirmed_originals()
    assert operations(coordinator) == before and BlockReason.EXTERNAL in coordinator.gate.reasons
    assert len(gateway.requests) == calls


def test_changed_original_history_cannot_replace_current_financial_evidence(tmp_path):
    recovery, coordinator, gateway, sink, _anchor, _incidents = account_fixture.fixture(
        tmp_path, Market.SPOT
    )
    terminal(gateway, Market.SPOT)
    recovery.recover_once(observed_at_ms=gateway.now_ms)
    before, calls = deepcopy(coordinator._operations), len(gateway.requests)
    intent = deepcopy(next(row for row in sink.records() if row["kind"] == "OrderIntent"))
    sink.record(intent | {"quantity": ".02"})
    records = deepcopy(sink.records())
    with pytest.raises(HELD):
        recovery.adopt_confirmed_originals()
    assert coordinator._operations == before
    assert len(gateway.requests) == calls and sink.records() == records


def test_registered_owner_cannot_adopt_into_coordinator_with_swapped_sink(tmp_path):
    recovery, coordinator, gateway, sink, _anchor, _incidents = account_fixture.fixture(
        tmp_path, Market.SPOT
    )
    terminal(gateway, Market.SPOT)
    recovery.recover_once(observed_at_ms=gateway.now_ms)
    before, calls, records = (
        deepcopy(coordinator._operations),
        len(gateway.requests),
        deepcopy(sink.records()),
    )
    replacement = account_fixture.journals(tmp_path / "different-owner-store")
    coordinator.sink = replacement
    with pytest.raises(HELD):
        recovery.adopt_confirmed_originals()
    assert coordinator._operations == before
    assert len(gateway.requests) == calls and sink.records() == records
    assert not replacement.records()


def test_unresolved_terminal_discrepancy_is_not_cleared_by_financial_commit(tmp_path):
    recovery, coordinator, gateway, sink, _anchor, _incidents = account_fixture.fixture(
        tmp_path, Market.SPOT
    )
    terminal(gateway, Market.SPOT)
    (original,) = coordinator.restore_history()
    coordinator.reconcile(original.request)
    gateway.orders[original.request.client_order_id].update(
        cummulativeQuoteQty="100.0020", cumQuote="100.0020"
    )
    with pytest.raises(ValidationError):
        coordinator.reconcile(original.request)
    gateway.orders[original.request.client_order_id].update(
        cummulativeQuoteQty="100.0010", cumQuote="100.0010"
    )
    receipt = recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert receipt.matched
    before, calls, records = (
        deepcopy(coordinator._operations),
        len(gateway.requests),
        deepcopy(sink.records()),
    )
    assert original.state == "UNKNOWN" and original.terminal_review_required is True
    with pytest.raises(HELD):
        recovery.adopt_confirmed_originals()
    assert coordinator._operations == before and original.terminal_review_required is True
    assert len(gateway.requests) == calls and sink.records() == records


def test_one_nonterminal_original_prevents_partial_set_adoption(tmp_path):
    _old_owner, coordinator, gateway, sink, anchor, _incidents = account_fixture.fixture(
        tmp_path, Market.SPOT
    )
    terminal(gateway, Market.SPOT)
    _cycle, _decision, compiled = bridge.compiled(
        coordinator.account_id, bridge.context(Market.SPOT), cycle_id="second-original"
    )
    assert (
        ExecutionDecisionBindings(coordinator.account_id, sink)
        .bind(compiled, account_state_digest="b" * 64)
        .admitted
    )
    request = compiled.intents[0].request
    sink.record(
        {
            "kind": "OrderIntent",
            "account_id": coordinator.account_id,
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
        orderId=8,
        status="NEW",
        executedQty="0",
        cummulativeQuoteQty="0",
        cumQuote="0",
    )
    fresh = OrderCoordinator(coordinator.account_id, coordinator.broker, sink, EntryGate())
    fresh.gate.block(BlockReason.PAUSE)
    recovery = ExecutionAccountRecovery(fresh, coordinator.broker, sink, opening=anchor)
    receipt = recovery.recover_once(observed_at_ms=gateway.now_ms)
    assert receipt.matched and len(receipt.book.applied_fills) == 4
    before, calls, records = assert_unknown(fresh), len(gateway.requests), deepcopy(sink.records())
    assert len(before) == 2
    with pytest.raises(HELD):
        recovery.adopt_confirmed_originals()
    assert (
        operations(fresh) == before and len(gateway.requests) == calls and sink.records() == records
    )
    gateway.orders[request.client_order_id]["status"] = "CANCELED"
    recovery.recover_once(observed_at_ms=gateway.now_ms)
    no_io_adopt(recovery, fresh, gateway, sink)
    assert len(operations(fresh)) == 2 and all(row[4] == 0 for row in operations(fresh).values())
