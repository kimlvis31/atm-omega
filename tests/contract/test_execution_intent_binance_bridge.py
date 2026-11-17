"""Offline compiler → coordinator → actual Binance adapters, using memory HTTP only.

Clients and synthetic credentials are created inside tests, never at import time.
The test-only client enables writes to its memory transport; no runtime live
composition, real account, socket transport or external service is enabled.
"""

import json
from dataclasses import replace
from decimal import Decimal as D
from urllib.parse import parse_qs, urlsplit

import pytest

from omega.application.execution_intents import ExecutionCycle, prepare_execution_decision
from omega.application.orders import OrderCoordinator
from omega.domain.errors import OrderOutcomeUnknown, TransportError, ValidationError
from omega.domain.models import Instrument, Market, OrderStatus, Side
from omega.domain.safety import BlockReason, EntryGate
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
)
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient, Credentials
from omega.infrastructure.binance.spot import SpotAdapter
from omega.infrastructure.binance.transport import HttpResponse
from omega.infrastructure.binance.usdm import UsdmAdapter
from omega.infrastructure.journal import FailoverJournal, JsonlJournal


class MemoryGateway:
    """Small synthetic HTTP transcript; no socket/opener/default transport exists."""

    def __init__(self, sink, account_id, *, lose_post_ack=False):
        self.sink, self.account_id = sink, account_id
        self.lose_post_ack = lose_post_ack
        self.requests, self.orders = [], {}

    @staticmethod
    def parameters(request):
        text = (
            request.body.decode("ascii")
            if request.body is not None
            else urlsplit(request.url).query
        )
        return parse_qs(text)

    def send(self, request):
        self.requests.append(request)
        path, parameters = urlsplit(request.url).path, self.parameters(request)
        if path.endswith("/exchangeInfo"):
            value = {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "baseAsset": "BTC",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                        "contractType": "PERPETUAL",
                        "filters": [
                            {
                                "filterType": "LOT_SIZE",
                                "minQty": "0.001",
                                "maxQty": "1000",
                                "stepSize": "0.001",
                            },
                            {
                                "filterType": "PRICE_FILTER",
                                "minPrice": "0.01",
                                "maxPrice": "1000000",
                                "tickSize": "0.01",
                            },
                        ],
                    }
                ]
            }
        elif path.endswith("/positionSide/dual"):
            assert request.method == "GET"
            value = {"dualSidePosition": False}
        elif path.endswith("/multiAssetsMargin"):
            assert request.method == "GET"
            value = {"multiAssetsMargin": False}
        elif path.endswith("/order"):
            if request.method == "POST":
                identity = parameters["newClientOrderId"][0]
                assert identity not in self.orders, "bridge submitted the same ID twice"
                # Real RecordSink primary intent must precede the adapter POST.
                intents = [event for event in self.sink.records() if event["kind"] == "OrderIntent"]
                assert len(intents) == 1 and intents[0]["client_order_id"] == identity
                assert intents[0]["account_id"] == self.account_id
                self.orders[identity] = {
                    "symbol": parameters["symbol"][0],
                    "clientOrderId": identity,
                    "orderId": 7,
                    "status": "NEW",
                    "executedQty": "0",
                    "cummulativeQuoteQty": "0",
                    "cumQuote": "0",
                }
                if self.lose_post_ack:
                    self.lose_post_ack = False
                    raise TransportError("synthetic ACK loss after memory provider accepted intent")
            else:
                assert request.method in {"GET", "DELETE"}
                identity = parameters["origClientOrderId"][0]
                assert identity in self.orders
                if request.method == "DELETE":
                    self.orders[identity]["status"] = "CANCELED"
            value = dict(self.orders[identity])
        else:
            raise AssertionError("unexpected route in offline bridge transcript")
        return HttpResponse(200, json.dumps(value).encode("utf-8"))

    def order_calls(self, method):
        return [
            request
            for request in self.requests
            if request.method == method and urlsplit(request.url).path.endswith("/order")
        ]


def owner(tmp_path, market, *, lose_post_ack=False):
    account_id = "synthetic-bridge-owner"
    sink = FailoverJournal(
        JsonlJournal(tmp_path / "primary.jsonl"),
        JsonlJournal(tmp_path / "fallback.jsonl"),
        lambda _code: pytest.fail("unexpected loss of both synthetic record stores"),
    )
    transport = MemoryGateway(sink, account_id, lose_post_ack=lose_post_ack)
    client = BinanceClient(
        "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com",
        transport,
        credentials=Credentials("bridge-test-key", "bridge-test-secret"),
        enable_trading=True,
        budget=RequestBudget((Window("ip", 10000, 60), Window("orders", 100, 60))),
        ip_scope="ip",
        order_scope="orders",
        clock_ms=lambda: 120000,
    )
    adapter = (
        SpotAdapter(account_id, client)
        if market == Market.SPOT
        else UsdmAdapter(account_id, client)
    )
    adapter.refresh_symbols()
    if market == Market.USDM:
        adapter.verify_one_way()
    gate = EntryGate()
    gate.recovered(BlockReason.RESTART)
    gate.resume(reconciled=True, durable=True)
    coordinator = OrderCoordinator(account_id, adapter, sink, gate)
    return account_id, adapter, coordinator, transport, sink, gate


def context(market, *, position="0", pending=False, entries=True):
    quantity = D(position)
    return TradeContext(
        Instrument("binance", market, "BTCUSDT"),
        D(10000),
        120000,
        PositionView(quantity, D(10000) if quantity else D(0)),
        BudgetView(D(1000), D(1000)),
        ExecutionConstraints(
            quantity_step=D(".001"),
            price_tick=D(".01"),
            leverage=D(1),
            can_short=market == Market.USDM,
        ),
        entries_allowed=entries,
        has_pending_order=pending,
    )


def compiled(account_id, ctx, *, cycle_id="entry", quantity=".01", reducing=False):
    cycle = ExecutionCycle(account_id, "synthetic-ts", cycle_id, "a" * 64)
    decision = TradeDecision(
        (
            OrderProposal(
                Side.SELL if reducing else Side.BUY,
                D(quantity),
                reducing=reducing,
            ),
        ),
        "synthetic-bridge-decision",
    )
    return cycle, decision, prepare_execution_decision(cycle, ctx, decision)


@pytest.mark.parametrize("reducing", [False, True], ids=["spot-entry", "spot-inventory-reduction"])
def test_compiled_spot_intent_reaches_real_adapter_with_inventory_semantics(tmp_path, reducing):
    account_id, _adapter, coordinator, transport, sink, _gate = owner(tmp_path, Market.SPOT)
    ctx = context(Market.SPOT, position=".02" if reducing else "0")
    _cycle, _decision, result = compiled(account_id, ctx, reducing=reducing)
    assert len(result.intents) == 1 and result.cancel_intent is None
    intent = result.intents[0]
    assert intent.reducing is reducing and intent.request.reduce_only is False
    operation = coordinator.submit(
        intent.request,
        position=ctx.position.quantity,
        state_certain=ctx.state_certain,
        reducing=intent.reducing,
    )
    assert operation.state == "OBSERVED" and operation.observation.status == OrderStatus.NEW
    post = transport.order_calls("POST")
    assert len(post) == 1
    parameters = transport.parameters(post[0])
    assert parameters["newClientOrderId"] == [intent.request.client_order_id]
    assert parameters["side"] == ["SELL" if reducing else "BUY"]
    assert D(parameters["quantity"][0]) == intent.request.quantity
    assert "reduceOnly" not in parameters and "positionSide" not in parameters
    assert [event["kind"] for event in sink.records()] == ["OrderIntent", "OrderObserved"]
    assert sink.records()[0]["reducing"] is reducing


def test_compiled_usdm_reduction_remains_exchange_reduce_only_while_entries_paused(tmp_path):
    account_id, _adapter, coordinator, transport, sink, gate = owner(tmp_path, Market.USDM)
    gate.block(BlockReason.PAUSE)
    ctx = context(Market.USDM, position=".02", entries=False)
    _cycle, _decision, result = compiled(account_id, ctx, reducing=True)
    intent = result.intents[0]
    assert intent.reducing is True and intent.request.reduce_only is True
    observed = coordinator.submit(
        intent.request,
        position=ctx.position.quantity,
        state_certain=True,
        reducing=intent.reducing,
    )
    assert observed.state == "OBSERVED" and not gate.allows_new_risk
    parameters = transport.parameters(transport.order_calls("POST")[0])
    assert parameters["reduceOnly"] == ["true"] and parameters["positionSide"] == ["BOTH"]
    assert parameters["newClientOrderId"] == [intent.request.client_order_id]
    assert sink.records()[0]["reducing"] is True and sink.records()[0]["reduce_only"] is True


def test_same_cycle_replay_is_idempotent_but_changed_request_keeps_id_and_is_rejected(tmp_path):
    account_id, _adapter, coordinator, transport, sink, _gate = owner(tmp_path, Market.SPOT)
    ctx = context(Market.SPOT)
    cycle, decision, original = compiled(account_id, ctx)
    first = original.intents[0]
    observed = coordinator.submit(
        first.request, position=D(0), state_certain=True, reducing=first.reducing
    )
    replay = prepare_execution_decision(cycle, ctx, decision, original=original)
    assert replay == original
    assert (
        coordinator.submit(
            replay.intents[0].request,
            position=D(0),
            state_certain=True,
            reducing=replay.intents[0].reducing,
        )
        is observed
    )
    changed = prepare_execution_decision(
        cycle,
        ctx,
        TradeDecision((OrderProposal(Side.BUY, D(".02")),), "changed-payload"),
    ).intents[0]
    assert changed.request.client_order_id == first.request.client_order_id
    with pytest.raises(ValidationError, match="another intent"):
        coordinator.submit(
            changed.request, position=D(0), state_certain=True, reducing=changed.reducing
        )
    changed_source = prepare_execution_decision(
        replace(cycle, source_digest="b" * 64), ctx, decision
    )
    assert changed_source.intents[0].request.client_order_id == first.request.client_order_id
    with pytest.raises(ValidationError, match="another source or decision"):
        prepare_execution_decision(
            replace(cycle, source_digest="b" * 64), ctx, decision, original=original
        )
    assert len(transport.order_calls("POST")) == 1
    assert len([event for event in sink.records() if event["kind"] == "OrderIntent"]) == 1


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_lost_post_ack_queries_compiled_original_id_without_resubmission(tmp_path, market):
    account_id, _adapter, coordinator, transport, sink, gate = owner(
        tmp_path, market, lose_post_ack=True
    )
    ctx = context(market)
    cycle, decision, original = compiled(account_id, ctx)
    intent = original.intents[0]
    with pytest.raises(OrderOutcomeUnknown):
        coordinator.submit(
            intent.request, position=D(0), state_certain=True, reducing=intent.reducing
        )
    replay = prepare_execution_decision(cycle, ctx, decision, original=original)
    unknown = coordinator.submit(
        replay.intents[0].request,
        position=D(0),
        state_certain=True,
        reducing=replay.intents[0].reducing,
    )
    assert unknown.state == "UNKNOWN" and BlockReason.EXECUTION in gate.reasons
    assert len(transport.order_calls("POST")) == 1
    assert [event["kind"] for event in sink.records()] == ["OrderIntent"]
    observation = coordinator.reconcile(intent.request)
    assert observation.client_order_id == intent.request.client_order_id
    assert observation.status == OrderStatus.NEW and unknown.state == "OBSERVED"
    query = transport.order_calls("GET")
    assert len(query) == 1
    assert transport.parameters(query[0])["origClientOrderId"] == [intent.request.client_order_id]
    assert len(transport.order_calls("POST")) == 1
    assert BlockReason.EXECUTION in gate.reasons and not gate.allows_new_risk


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_compiled_cancellation_uses_prior_intent_id_and_existing_adapter_cancel(tmp_path, market):
    account_id, adapter, coordinator, transport, _sink, gate = owner(tmp_path, market)
    ctx = context(market)
    _cycle, _decision, original = compiled(account_id, ctx)
    intent = original.intents[0]
    coordinator.submit(intent.request, position=D(0), state_certain=True, reducing=intent.reducing)
    gate.block(BlockReason.PAUSE)
    cancellation = prepare_execution_decision(
        ExecutionCycle(account_id, "synthetic-ts", "cancel-cycle", "c" * 64),
        replace(ctx, entries_allowed=False, has_pending_order=True),
        TradeDecision(reason="synthetic-cancel", cancel_pending=True),
        pending_intent=intent,
    )
    assert cancellation.intents == () and cancellation.cancel_intent == intent
    assert cancellation.cancel_client_order_id == intent.request.client_order_id
    # OrderCoordinator has no cancel API. Use its existing adapter, then reconcile
    # the immutable original request; no new coordinator behavior is synthesized.
    canceled = adapter.cancel(intent.request.instrument, cancellation.cancel_client_order_id)
    assert canceled.status == OrderStatus.CANCELED
    observed = coordinator.reconcile(intent.request)
    assert observed == canceled
    deletes = transport.order_calls("DELETE")
    assert len(deletes) == 1
    assert transport.parameters(deletes[0])["origClientOrderId"] == [intent.request.client_order_id]
    assert len(transport.order_calls("POST")) == 1
    assert len(transport.order_calls("GET")) == 1 and BlockReason.PAUSE in gate.reasons
