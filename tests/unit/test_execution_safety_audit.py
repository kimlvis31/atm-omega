"""Safety regressions from concrete read-only audit counterexamples.

No account/network execution. The broker is synthetic, while the coordinator,
entry gate, common proposal validator and paper command path are actual code.
"""

from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.analysis.catalog import create_features
from omega.application.orders import OrderCoordinator
from omega.application.paper import PaperTradingService
from omega.application.trading import validate_decision
from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.models import (
    Instrument,
    Market,
    OrderKind,
    OrderObservation,
    OrderRequest,
    OrderStatus,
    Side,
)
from omega.domain.safety import BlockReason, EntryGate
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
)
from omega.ports.journal import Durability
from omega.strategies.registry import StrategyRegistry

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


class Sink:
    def __init__(self):
        self.events = []

    def record(self, event):
        self.events.append(event)
        return Durability.PRIMARY

    def records(self):
        return tuple(self.events)


class Broker:
    account_id = "synthetic"

    def __init__(self):
        self.reply, self.failure, self.calls = None, None, 0

    def submit(self, request):
        self.calls += 1
        if self.failure:
            raise self.failure
        return self.reply or OrderObservation(
            request.instrument, request.client_order_id, "7", OrderStatus.NEW, D(0), D(0)
        )

    def query(self, instrument, client_order_id):
        return self.reply


def ready_owner():
    gate = EntryGate()
    gate.recovered(BlockReason.RESTART)
    gate.resume(reconciled=True, durable=True)
    broker = Broker()
    return OrderCoordinator("synthetic", broker, Sink(), gate), broker, gate


def request(identity="audit-1", *, reducing=False, quantity="10"):
    return OrderRequest(
        INSTRUMENT,
        identity,
        Side.SELL if reducing else Side.BUY,
        OrderKind.MARKET,
        D(quantity),
        reduce_only=reducing,
    )


@pytest.mark.parametrize("change", ["instrument", "client_order_id", "overfilled"])
def test_submit_rejects_observation_for_another_order_or_excess_quantity(change):
    owner, broker, _ = ready_owner()
    order = request()
    values = {
        "instrument": Instrument("binance", Market.USDM, "BTCUSDT"),
        "client_order_id": "different-id",
        "executed_quantity": D(11),
    }
    field = "executed_quantity" if change == "overfilled" else change
    observation = OrderObservation(
        INSTRUMENT, order.client_order_id, "7", OrderStatus.PARTIALLY_FILLED, D(1), D(100)
    )
    broker.reply = replace(observation, **{field: values[field]})
    with pytest.raises((ValidationError, TradingBlocked)):
        owner.submit(order, position=D(0), state_certain=True)


def test_reconcile_cannot_regress_partial_cumulative_execution_or_release_pending():
    owner, broker, _ = ready_owner()
    order = request(reducing=True)
    broker.reply = OrderObservation(
        INSTRUMENT, order.client_order_id, "7", OrderStatus.PARTIALLY_FILLED, D(4), D(400)
    )
    operation = owner.submit(order, position=D(10), reducing=True, state_certain=True)
    original = operation.observation
    # A stale or mismatched terminal message must not release six still-pending units.
    broker.reply = replace(
        original, status=OrderStatus.CANCELED, executed_quantity=D(0), cumulative_quote=D(0)
    )
    with pytest.raises((ValidationError, TradingBlocked)):
        owner.reconcile(order)
    assert operation.observation == original


def test_unclassified_gateway_failure_blocks_new_risk_until_reconciliation():
    owner, broker, gate = ready_owner()
    broker.failure = RuntimeError("synthetic unexpected adapter failure after possible submission")
    with pytest.raises(RuntimeError):
        owner.submit(request(), position=D(0), state_certain=True)
    broker.failure = None
    with pytest.raises(TradingBlocked):
        owner.submit(request("audit-2"), position=D(0), state_certain=True)
    assert BlockReason.EXECUTION in gate.reasons and broker.calls == 1


def test_spot_proposal_cannot_obtain_implicit_credit_from_futures_leverage_field():
    context = TradeContext(
        Instrument("binance", Market.SPOT, "ETHUSDT"),
        D(100),
        60000,
        PositionView(),
        BudgetView(D(10), D(10)),
        ExecutionConstraints(leverage=D(10)),
    )
    decision = TradeDecision((OrderProposal(Side.BUY, D(1)),), "synthetic_spot_credit")
    with pytest.raises((ValidationError, TradingBlocked)):
        validate_decision(context, decision)


def test_partial_reduction_keeps_remaining_reservation_and_external_halt_blocks_control():
    owner, broker, gate = ready_owner()
    order = request(reducing=True)
    broker.reply = OrderObservation(
        INSTRUMENT, order.client_order_id, "7", OrderStatus.PARTIALLY_FILLED, D(4), D(400)
    )
    operation = owner.submit(order, position=D(10), reducing=True, state_certain=True)
    assert owner._remaining(operation) == D(6)
    with pytest.raises(TradingBlocked):
        owner.submit(
            request("second-close", reducing=True, quantity="1"),
            position=D(6),
            reducing=True,
            state_certain=True,
        )
    gate.block(BlockReason.EXTERNAL)
    with pytest.raises(TradingBlocked):
        owner.submit(
            request("external-close", reducing=True, quantity="1"),
            position=D(6),
            reducing=True,
            state_certain=True,
        )
    assert broker.calls == 1


def test_paper_host_rejects_requested_isolated_leverage_instead_of_silent_one_x_execution():
    registry = StrategyRegistry()
    service = PaperTradingService(registry.create, create_features)
    service.command("paper.account.create", {"account_id": "synthetic"})
    with pytest.raises(ValidationError):
        service.command(
            "paper.strategy.add",
            {
                "instance": "synthetic",
                "account_id": "synthetic",
                "instrument": INSTRUMENT.key,
                "budget": "1000",
                "module_id": "target_quantity",
                "configuration": {"quantity": "1"},
                "leverage": 10,
                "margin_mode": "isolated",
            },
        )


@pytest.mark.parametrize(
    "change", ["provider_id", "wrong_instrument", "quote_regression", "new_again"]
)
def test_bad_reconcile_preserves_last_good_and_reserves_uncertain_remainder(change):
    owner, broker, gate = ready_owner()
    order = request(reducing=True)
    broker.reply = OrderObservation(
        INSTRUMENT, order.client_order_id, "7", OrderStatus.PARTIALLY_FILLED, D(4), D(400)
    )
    operation = owner.submit(order, position=D(10), reducing=True, state_certain=True)
    original = operation.observation
    changes = {
        "provider_id": {"provider_order_id": "another"},
        "wrong_instrument": {"instrument": Instrument("binance", Market.USDM, "BTCUSDT")},
        "quote_regression": {"executed_quantity": D(5), "cumulative_quote": D(399)},
        "new_again": {"status": OrderStatus.NEW},
    }
    broker.reply = replace(original, **changes[change])
    with pytest.raises((TradingBlocked, ValidationError)):
        owner.reconcile(order)
    assert operation.observation == original and operation.state == "UNKNOWN"
    assert BlockReason.EXECUTION in gate.reasons and owner._remaining(operation) == D(6)
    with pytest.raises(TradingBlocked):
        owner.submit(
            request("extra", reducing=True, quantity="1"),
            position=D(6),
            reducing=True,
            state_certain=True,
        )


def test_terminal_late_fill_correction_is_explicitly_unresolved_not_silently_false_or_accepted():
    owner, broker, gate = ready_owner()
    order = request(reducing=True)
    original = OrderObservation(
        INSTRUMENT, order.client_order_id, "7", OrderStatus.CANCELED, D(4), D(400)
    )
    broker.reply = original
    operation = owner.submit(order, position=D(10), reducing=True, state_certain=True)
    assert owner._remaining(operation) == 0
    # A real late correction is possible, but this owner cannot apply account/fill facts itself.
    broker.reply = replace(original, executed_quantity=D(8), cumulative_quote=D(800))
    with pytest.raises(TradingBlocked, match="terminal order changed"):
        owner.reconcile(order)
    assert operation.observation == original and operation.terminal_review_required
    assert owner._remaining(operation) == 6 and not gate.allows_new_risk
    broker.reply = original
    with pytest.raises(TradingBlocked, match="actual-fill"):
        owner.reconcile(order)
    assert owner._remaining(operation) == 6


def test_generic_failed_reduction_keeps_its_entire_uncertain_quantity_reserved():
    owner, broker, _ = ready_owner()
    order = request(reducing=True)
    broker.failure = RuntimeError("synthetic uncertain mutation")
    with pytest.raises(RuntimeError):
        owner.submit(order, position=D(10), reducing=True, state_certain=True)
    broker.failure = None
    # The EXECUTION gate blocks entries, and the prior reservation prevents duplicate exits.
    with pytest.raises(TradingBlocked):
        owner.submit(
            request("another-exit", reducing=True, quantity="1"),
            position=D(10),
            reducing=True,
            state_certain=True,
        )
    assert broker.calls == 1


def test_valid_monotonic_partial_then_cancel_releases_only_confirmed_unfilled_remainder():
    owner, broker, gate = ready_owner()
    order = request(reducing=True)
    broker.reply = OrderObservation(
        INSTRUMENT, order.client_order_id, "7", OrderStatus.PARTIALLY_FILLED, D(4), D(400)
    )
    operation = owner.submit(order, position=D(10), reducing=True, state_certain=True)
    broker.reply = replace(operation.observation, status=OrderStatus.CANCELED)
    owner.reconcile(order)
    assert owner._remaining(operation) == 0 and not operation.terminal_review_required
    assert gate.allows_new_risk


@pytest.mark.parametrize(
    ("position", "side", "reducing"),
    [
        ("0", Side.SELL, False),
        ("-1", Side.BUY, True),
        ("-1", Side.BUY, False),
    ],
)
def test_raw_spot_owner_cannot_open_short_or_trade_from_negative_cash_inventory(
    position, side, reducing
):
    owner, broker, _ = ready_owner()
    order = OrderRequest(
        Instrument("binance", Market.SPOT, "ETHUSDT"), "cash-audit", side, OrderKind.MARKET, D(1)
    )
    with pytest.raises(TradingBlocked, match="spot"):
        owner.submit(order, position=D(position), reducing=reducing, state_certain=True)
    assert broker.calls == 0


def test_spot_reduction_preserves_cash_exchange_flags_and_reserves_inventory():
    owner, broker, _ = ready_owner()
    order = OrderRequest(
        Instrument("binance", Market.SPOT, "ETHUSDT"),
        "cash-close",
        Side.SELL,
        OrderKind.MARKET,
        D(1),
    )
    operation = owner.submit(order, position=D(1), reducing=True, state_certain=True)
    assert not operation.request.reduce_only and owner._remaining(operation) == D(1)
    with pytest.raises(TradingBlocked):
        owner.submit(
            replace(order, client_order_id="cash-close-duplicate"),
            position=D(1),
            reducing=True,
            state_certain=True,
        )
    assert broker.calls == 1


def test_spot_buy_still_delegates_cash_allocation_to_reconciled_account_risk_owner():
    owner, broker, _ = ready_owner()
    order = OrderRequest(
        Instrument("binance", Market.SPOT, "ETHUSDT"), "cash-buy", Side.BUY, OrderKind.MARKET, D(1)
    )
    operation = owner.submit(order, position=D(0), state_certain=True)
    assert operation.state == "OBSERVED" and broker.calls == 1
