"""Reopened real JSONL restores immutable originals/reservations without dispatch."""

from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.application.orders import OrderCoordinator
from omega.domain.errors import (
    JournalError,
    OrderOutcomeUnknown,
    TradingBlocked,
    ValidationError,
)
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
from omega.infrastructure.journal import FailoverJournal, JsonlJournal


class Broker:
    account_id = "A"

    def __init__(self, *, unknown=False):
        self.submits = self.queries = 0
        self.unknown = unknown

    def submit(self, request):
        self.submits += 1
        if self.unknown:
            raise OrderOutcomeUnknown("synthetic ACK loss")
        return self.query(request.instrument, request.client_order_id)

    def query(self, instrument, client_order_id):
        self.queries += 1
        return OrderObservation(instrument, client_order_id, "7", OrderStatus.NEW, D(0), D(0))


def gate():
    result = EntryGate()
    result.recovered(BlockReason.RESTART)
    result.resume(reconciled=True, durable=True)
    return result


def sink(root):
    return FailoverJournal(
        JsonlJournal(root / "primary.jsonl"),
        JsonlJournal(root / "fallback.jsonl"),
        lambda _: None,
    )


def request(market=Market.USDM, identity="first"):
    return OrderRequest(
        Instrument("binance", market, "BTCUSDT"),
        identity,
        Side.SELL,
        OrderKind.LIMIT,
        D(1),
        D(100),
        market == Market.USDM,
    )


def saved(root, market=Market.USDM, *, unknown=False):
    broker = Broker(unknown=unknown)
    owner = OrderCoordinator("A", broker, sink(root), gate())
    if unknown:
        with pytest.raises(OrderOutcomeUnknown):
            owner.submit(request(market), position=D(2), state_certain=True, reducing=True)
    else:
        owner.submit(request(market), position=D(2), state_certain=True, reducing=True)
    return owner, broker


@pytest.mark.parametrize("market", list(Market))
@pytest.mark.parametrize("unknown", [False, True])
def test_reopened_original_reserves_quantity_and_never_resubmits(tmp_path, market, unknown):
    previous, broker = saved(tmp_path, market, unknown=unknown)
    original_events = previous.sink.records()
    before = broker.submits, broker.queries
    restarted = OrderCoordinator("A", broker, sink(tmp_path), gate())
    (restored,) = restarted.restore_history()
    assert restored.request == request(market) and restored.state == "UNKNOWN"
    assert restored.observation is None and restarted._remaining(restored) == D(1)
    assert (broker.submits, broker.queries) == before
    assert restarted.sink.records() == original_events
    assert BlockReason.EXECUTION in restarted.gate.reasons
    assert (
        restarted.submit(request(market), position=D(2), state_certain=True, reducing=True)
        is restored
    )
    with pytest.raises(ValidationError, match="original immutable"):
        restarted.reconcile(replace(request(market), quantity=D(2)))
    assert (broker.submits, broker.queries) == before
    assert restarted.reconcile(restored.request).status == OrderStatus.NEW
    assert broker.submits == before[0] and broker.queries == before[1] + 1
    assert BlockReason.EXECUTION in restarted.gate.reasons  # Query is not resume.


@pytest.mark.parametrize("market", list(Market))
def test_all_pending_reservations_restore_before_any_further_reduction(tmp_path, market):
    owner, broker = saved(tmp_path, market)
    owner.submit(request(market, "second"), position=D(2), state_certain=True, reducing=True)
    restarted = OrderCoordinator("A", broker, sink(tmp_path), gate())
    restored = restarted.restore_history()
    assert [op.request.client_order_id for op in restored] == ["first", "second"]
    before = broker.submits, broker.queries
    with pytest.raises(TradingBlocked, match="unreserved"):
        restarted.submit(request(market, "third"), position=D(2), state_certain=True, reducing=True)
    with pytest.raises(TradingBlocked):
        restarted.submit(
            replace(request(market, "entry"), side=Side.BUY, reduce_only=False),
            position=D(0),
            state_certain=True,
        )
    assert (broker.submits, broker.queries) == before


def test_old_terminal_snapshot_is_not_proof_of_current_position_or_fill_settlement(
    tmp_path,
):
    owner, broker = saved(tmp_path)
    owner.sink.record(
        {
            "kind": "OrderObserved",
            "account_id": "A",
            "client_order_id": "first",
            "provider_order_id": "7",
            "status": "FILLED",
        }
    )
    restarted = OrderCoordinator("A", broker, sink(tmp_path), gate())
    (restored,) = restarted.restore_history()
    assert restored.observation is None and restarted._remaining(restored) == D(1)
    assert restored.state == "UNKNOWN"


def test_fallback_duplicate_replay_does_not_duplicate_quantity_or_rewind_current_operation(
    tmp_path,
):
    owner, broker = saved(tmp_path)
    original = owner._operations["first"]
    intent = owner.sink.records()[0]
    owner.sink.fallback.append(intent)
    (restored,) = owner.restore_history()
    assert restored is original and restored.state == "OBSERVED"
    reopened = OrderCoordinator("A", broker, sink(tmp_path), gate())
    (first,) = reopened.restore_history()
    (again,) = reopened.restore_history()
    assert first is again and reopened._remaining(first) == D(1)
    assert broker.submits == 1


@pytest.mark.parametrize(
    "change",
    [
        {"quantity": "2"},
        {"side": "BUY"},
        {"price": "101"},
        {"instrument": "binance:usdm:ETHUSDT"},
        {"reducing": False},
        {"reduce_only": False},
    ],
)
def test_conflicting_duplicate_original_is_rejected_atomically_before_broker(tmp_path, change):
    owner, broker = saved(tmp_path)
    owner.sink.fallback.append(owner.sink.records()[0] | change)
    restarted = OrderCoordinator("A", broker, sink(tmp_path), gate())
    before = broker.submits, broker.queries
    with pytest.raises(ValidationError):
        restarted.restore_history()
    assert not restarted._operations
    assert BlockReason.EXECUTION in restarted.gate.reasons
    assert (broker.submits, broker.queries) == before


@pytest.mark.parametrize(
    "change",
    [
        {"quantity": 1},
        {"quantity": "NaN"},
        {"quantity": "0"},
        {"price": 100},
        {"instrument": 123},
        {"instrument": "bad"},
        {"side": "INVALID"},
        {"order_kind": "INVALID"},
        {"reducing": 1},
        {"reducing": "false"},
        {"reduce_only": 0},
        {"client_order_id": "invalid space"},
    ],
)
def test_malformed_original_never_publishes_partial_reservations(tmp_path, change):
    owner, broker = saved(tmp_path)
    bad = owner.sink.records()[0] | {"client_order_id": "second"} | change
    owner.sink.fallback.append(bad)
    restarted = OrderCoordinator("A", broker, sink(tmp_path), gate())
    with pytest.raises(ValidationError):
        restarted.restore_history()
    assert not restarted._operations and broker.submits == 1


def test_foreign_history_is_not_adopted_or_validated_for_this_account(tmp_path):
    owner, broker = saved(tmp_path)
    owner.sink.fallback.append(
        {
            "kind": "OrderIntent",
            "account_id": "B",
            "client_order_id": "first",
            "quantity": "bad",
        }
    )
    restarted = OrderCoordinator("A", broker, sink(tmp_path), gate())
    (restored,) = restarted.restore_history()
    assert restored.request == request() and broker.submits == 1


def test_replay_read_failure_retains_execution_gate_and_has_no_gateway_effect(tmp_path):
    owner, broker = saved(tmp_path)
    restarted = OrderCoordinator("A", broker, sink(tmp_path), gate())
    with (tmp_path / "primary.jsonl").open("ab") as stream:
        stream.write(b"torn")
    before = broker.submits, broker.queries
    with pytest.raises(JournalError):
        restarted.restore_history()
    assert not restarted._operations and BlockReason.EXECUTION in restarted.gate.reasons
    assert (broker.submits, broker.queries) == before
