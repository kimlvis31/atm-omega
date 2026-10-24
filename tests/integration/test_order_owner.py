from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.application.orders import OrderCoordinator
from omega.domain.errors import JournalError, OrderOutcomeUnknown, TradingBlocked, ValidationError
from omega.domain.ledger import CashLedger
from omega.domain.models import (
    Fill,
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


class Journal:
    def __init__(self, fail=False):
        self.events = []
        self.fail = fail

    def append(self, record):
        if self.fail:
            raise JournalError("disk unavailable")
        self.events.append(record)

    def records(self):
        return tuple(self.events)


class Broker:
    account_id = "A"

    def __init__(self, fail=False, before_submit=lambda: None):
        self.fail = fail
        self.submits = 0
        self.queries = 0
        self.before_submit = before_submit

    def submit(self, request):
        self.before_submit()
        self.submits += 1
        if self.fail:
            raise OrderOutcomeUnknown("lost acknowledgement")
        return OrderObservation(
            request.instrument, request.client_order_id, "7", OrderStatus.NEW, D(0), D(0)
        )

    def query(self, instrument, client_order_id):
        self.queries += 1
        return OrderObservation(instrument, client_order_id, "7", OrderStatus.NEW, D(0), D(0))


def request(side=Side.BUY):
    return OrderRequest(
        Instrument("binance", Market.SPOT, "BTCUSDT"),
        "omega-1",
        side,
        OrderKind.LIMIT,
        D(1),
        D(100),
    )


def ready_gate():
    gate = EntryGate()
    gate.recovered(BlockReason.RESTART)
    gate.resume(reconciled=True, durable=True)
    return gate


def test_intent_durable_before_gateway_and_duplicate_not_submitted(tmp_path):
    journal = JsonlJournal(tmp_path / "primary.jsonl")
    sink = FailoverJournal(journal, Journal(), lambda _: None)
    broker = Broker(before_submit=lambda: assert_intent(journal))
    owner = OrderCoordinator("A", broker, sink, ready_gate())
    first = owner.submit(request(), position=D(0), state_certain=True)
    second = owner.submit(request(), position=D(0), state_certain=True)
    assert first is second and broker.submits == 1
    assert len(journal.records()) == 2


def assert_intent(journal):
    assert journal.records()[0]["kind"] == "OrderIntent"


def test_unknown_ack_query_original_id_without_resubmission():
    primary = Journal()
    sink = FailoverJournal(primary, Journal(), lambda _: None)
    broker = Broker(fail=True)
    gate = ready_gate()
    owner = OrderCoordinator("A", broker, sink, gate)
    with pytest.raises(OrderOutcomeUnknown):
        owner.submit(request(), position=D(0), state_certain=True)
    assert owner.submit(request(), position=D(0), state_certain=True).state == "UNKNOWN"
    owner.reconcile(request())
    assert broker.submits == 1 and broker.queries == 1
    assert BlockReason.EXECUTION in gate.reasons


def test_restart_never_reuses_previously_journaled_id():
    sink = FailoverJournal(Journal(), Journal(), lambda _: None)
    broker = Broker()
    OrderCoordinator("A", broker, sink, ready_gate()).submit(
        request(), position=D(0), state_certain=True
    )
    restarted = OrderCoordinator("A", broker, sink, ready_gate())
    with pytest.raises(TradingBlocked):
        restarted.submit(request(), position=D(0), state_certain=True)
    assert broker.submits == 1


def test_file_fallback_blocks_new_risk_even_if_fallback_is_durable(tmp_path):
    fallback = JsonlJournal(tmp_path / "fallback.jsonl")
    broker = Broker()
    gate = ready_gate()
    owner = OrderCoordinator(
        "A", broker, FailoverJournal(Journal(True), fallback, lambda _: None), gate
    )
    with pytest.raises(TradingBlocked):
        owner.submit(request(), position=D(0), state_certain=True)
    assert broker.submits == 0
    assert fallback.records()[0]["kind"] == "OrderIntent"
    assert BlockReason.STORAGE in gate.reasons


def test_all_storage_failure_still_manages_certain_position_and_alarms():
    alarms = []
    broker = Broker()
    gate = ready_gate()
    owner = OrderCoordinator(
        "A", broker, FailoverJournal(Journal(True), Journal(True), alarms.append), gate
    )
    result = owner.submit(request(Side.SELL), position=D(1), state_certain=True, reducing=True)
    assert result.state == "OBSERVED" and broker.submits == 1
    assert alarms and BlockReason.STORAGE in gate.reasons
    assert not gate.allows_new_risk


def test_external_intervention_overrides_emergency_storage_management():
    broker = Broker()
    gate = ready_gate()
    gate.block(BlockReason.EXTERNAL)
    owner = OrderCoordinator(
        "A", broker, FailoverJournal(Journal(True), Journal(True), lambda _: None), gate
    )
    with pytest.raises(TradingBlocked):
        owner.submit(request(Side.SELL), position=D(1), state_certain=True, reducing=True)
    assert broker.submits == 0


def test_pending_reduction_cannot_exceed_position():
    broker = Broker()
    owner = OrderCoordinator(
        "A", broker, FailoverJournal(Journal(), Journal(), lambda _: None), ready_gate()
    )
    owner.submit(request(Side.SELL), position=D(1), state_certain=True, reducing=True)
    with pytest.raises(TradingBlocked):
        owner.submit(
            replace(request(Side.SELL), client_order_id="omega-2"),
            position=D(1),
            state_certain=True,
            reducing=True,
        )
    assert broker.submits == 1


def test_pause_does_not_cancel_submitted_order_or_discard_late_fill():
    broker = Broker()
    gate = ready_gate()
    owner = OrderCoordinator(
        "A", broker, FailoverJournal(Journal(), Journal(), lambda _: None), gate
    )
    owner.submit(request(), position=D(0), state_certain=True)
    gate.block(BlockReason.PAUSE)
    ledger = CashLedger("A", request().instrument, "USDT", D(1000))
    late = Fill(
        "A", request().instrument, "fill-1", "7", Side.BUY, D(1), D(100), D("0.1"), "USDT", 1
    )
    assert ledger.apply(late)
    assert ledger.cash == D("899.9") and ledger.quantity == 1
    with pytest.raises(TradingBlocked):
        owner.submit(
            replace(request(), client_order_id="omega-2"), position=D(1), state_certain=True
        )
    assert broker.submits == 1


def test_alarm_delivery_failure_cannot_block_emergency_reduction():
    def broken_alarm(_):
        raise OSError("audio unavailable")

    sink = FailoverJournal(Journal(True), Journal(True), broken_alarm)
    broker = Broker()
    owner = OrderCoordinator("A", broker, sink, ready_gate())
    owner.submit(request(Side.SELL), position=D(1), state_certain=True, reducing=True)
    assert broker.submits == 1
    assert sink.notification_failed and sink.last_incident == "ALL_RECORDING_UNAVAILABLE"


def test_position_reversal_requires_explicit_planning():
    broker = Broker()
    owner = OrderCoordinator(
        "A", broker, FailoverJournal(Journal(), Journal(), lambda _: None), ready_gate()
    )
    with pytest.raises(TradingBlocked):
        owner.submit(request(Side.SELL), position=D(1), state_certain=True)
    assert broker.submits == 0


def test_account_owner_cannot_accidentally_use_another_accounts_gateway():
    with pytest.raises(ValidationError, match="identity must match"):
        OrderCoordinator(
            "B", Broker(), FailoverJournal(Journal(), Journal(), lambda _: None), ready_gate()
        )
