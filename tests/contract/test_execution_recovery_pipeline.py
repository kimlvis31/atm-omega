"""One real-adapter/memory-HTTP journal pipeline; no socket, real key or account."""

import json
from decimal import Decimal as D
from urllib.parse import urlsplit

import pytest
import test_execution_intent_binance_bridge as bridge

from omega.application.execution_bindings import ExecutionDecisionBindings
from omega.application.execution_fills import ExecutionFillRecovery
from omega.application.execution_recovery import ExecutionObservationRecovery
from omega.application.orders import OrderCoordinator
from omega.domain.errors import OrderOutcomeUnknown
from omega.domain.ledger import CashLedger
from omega.domain.models import Market
from omega.domain.safety import BlockReason, EntryGate
from omega.infrastructure.binance.transport import HttpResponse
from omega.infrastructure.journal import FailoverJournal, JsonlJournal


class FillGateway(bridge.MemoryGateway):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fills = []

    def send(self, request):
        if urlsplit(request.url).path.endswith(("/myTrades", "/userTrades")):
            assert request.method == "GET"
            self.requests.append(request)
            boundary = int(self.parameters(request).get("fromId", ["0"])[0])
            rows = [row for row in self.fills if row["id"] >= boundary]
            return HttpResponse(200, json.dumps(rows).encode())
        return super().send(request)


def fill(identity, quantity, fee):
    return dict(
        symbol="BTCUSDT",
        id=identity,
        orderId=7,
        side="BUY",
        isBuyer=True,
        qty=quantity,
        price="10000",
        commission=fee,
        commissionAsset="USDT",
        time=120001,
    )


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_original_intent_partial_and_late_actual_adapter_facts_survive_restarts(
    tmp_path, monkeypatch, market
):
    monkeypatch.setattr(bridge, "MemoryGateway", FillGateway)
    account, adapter, original, transport, sink, _gate = bridge.owner(
        tmp_path, market, lose_post_ack=True
    )
    ctx = bridge.context(market)
    cycle, _decision, compiled = bridge.compiled(account, ctx)
    binding = ExecutionDecisionBindings(account, sink).bind(compiled, account_state_digest="b" * 64)
    assert binding.admitted
    intent = compiled.intents[0]
    with pytest.raises(OrderOutcomeUnknown):
        original.submit(intent.request, position=D(0), state_certain=True)
    original_prefix = (tmp_path / "primary.jsonl").read_bytes()
    assert len(transport.order_calls("POST")) == 1

    def reopened_sink():
        return FailoverJournal(
            JsonlJournal(tmp_path / "primary.jsonl"),
            JsonlJournal(tmp_path / "fallback.jsonl"),
            lambda _: pytest.fail("synthetic journal unavailable"),
        )

    recovered_sink = reopened_sink()
    gate = EntryGate()  # Restart is deliberately not manually resumed.
    gate.block(BlockReason.PAUSE)
    coordinator = OrderCoordinator(account, adapter, recovered_sink, gate)
    recovery = ExecutionObservationRecovery(account, coordinator, recovered_sink)
    provider = transport.orders[intent.request.client_order_id]
    provider.update(
        status="PARTIALLY_FILLED", executedQty="0.004", cummulativeQuoteQty="40", cumQuote="40"
    )
    (partial,) = recovery.recover()
    assert partial.source_bound and partial.state == "primary_acknowledged"
    transport.fills = [fill(1, "0.004", "0.04")]
    pages = ExecutionFillRecovery(
        account,
        ctx.instrument,
        adapter,
        recovered_sink,
        gate,
        initial_from_id=0,
        owned=((partial.request, partial.observation),),
    )
    first = pages.recover_page()
    assert len(first.new_facts) == 1 and first.through_id == 1

    # A paused entry still fills at the provider. Observe, record and deduplicate it;
    # never cancel/resubmit it or attribute cumulative differences as invented fills.
    provider.update(status="FILLED", executedQty="0.01", cummulativeQuoteQty="100", cumQuote="100")
    (completed,) = recovery.recover()
    transport.fills.append(fill(2, "0.006", "0.06"))
    next_sink = reopened_sink()
    restarted_pages = ExecutionFillRecovery(
        account,
        ctx.instrument,
        adapter,
        next_sink,
        gate,
        initial_from_id=0,
        owned=((completed.request, completed.observation),),
    )
    assert restarted_pages.history.recovery_required
    second = restarted_pages.recover_page()
    assert second.from_id == 1 and second.through_id == 2
    assert [fact.trade_id for fact in second.new_facts] == ["2"]
    assert second.recovery_required and not second.history_complete
    assert all(
        operation.state == "UNKNOWN" and operation.observation is None
        for operation in coordinator.restore_history()
    )
    assert {BlockReason.RESTART, BlockReason.PAUSE, BlockReason.EXECUTION} <= gate.reasons
    assert not gate.allows_new_risk
    assert len(transport.order_calls("POST")) == 1 and not transport.order_calls("DELETE")
    restored = ExecutionDecisionBindings(account, next_sink).lookup(cycle)
    assert restored.recovery_required and not restored.admitted
    assert (tmp_path / "primary.jsonl").read_bytes().startswith(original_prefix)
    assert sum(fact.quantity for fact in restarted_pages.history.facts) == D("0.01")
    assert sum(fact.fee for fact in restarted_pages.history.facts) == D("0.10")
    if market == Market.SPOT:
        # Explicit synthetic opening balance, chosen by this test, not inferred
        # account authority. Futures collateral/PnL reconstruction is out of scope.
        ledger = CashLedger(account, ctx.instrument, "USDT", D(1000), base_asset="BTC")
        for fact in restarted_pages.history.facts:
            assert ledger.apply(fact)
            assert not ledger.apply(fact)
        assert ledger.cash == D("899.90") and ledger.quantity == D("0.01")
