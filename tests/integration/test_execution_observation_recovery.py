"""Real temporary JSONL plus query-only synthetic broker; no socket/account/order IO."""

from copy import deepcopy
from dataclasses import FrozenInstanceError
from decimal import Decimal as D

import pytest

from omega.application.execution_bindings import ExecutionDecisionBindings
from omega.application.execution_intents import ExecutionCycle, ExecutionDecision, ExecutionIntent
from omega.application.execution_recovery import ExecutionObservationRecovery
from omega.application.orders import OrderCoordinator
from omega.domain.errors import JournalError, TradingBlocked, ValidationError
from omega.domain.models import Instrument, Market, OrderObservation, OrderStatus, Side
from omega.domain.safety import BlockReason, EntryGate
from omega.domain.trading import OrderProposal
from omega.infrastructure.journal import FailoverJournal, JsonlJournal

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
KIND = "ExecutionRecoveryObserved"


def prepared(cycle="minute-1", instrument=BTC):
    identity = ExecutionCycle("A", "TEF", cycle, "a" * 64)
    intent = ExecutionIntent(identity, instrument, 0, OrderProposal(Side.BUY, D(1)))
    return ExecutionDecision(identity, (intent,), None, "entry")


def intent_event(request):
    return {
        "kind": "OrderIntent",
        "account_id": "A",
        "client_order_id": request.client_order_id,
        "instrument": request.instrument.key,
        "side": request.side.value,
        "order_kind": request.kind.value,
        "quantity": str(request.quantity),
        "price": None if request.price is None else str(request.price),
        "reducing": False,
        "reduce_only": request.reduce_only,
    }


def observation(request, *, status=OrderStatus.NEW, quantity="0", quote="0", provider="7"):
    return OrderObservation(
        request.instrument, request.client_order_id, provider, status, D(quantity), D(quote)
    )


class ReadBroker:
    account_id = "A"

    def __init__(self):
        self.replies, self.calls = {}, []
        self.before_query = lambda: None

    def query(self, instrument, client_order_id):
        self.before_query()
        self.calls.append((instrument, client_order_id))
        reply = self.replies[client_order_id]
        if isinstance(reply, Exception):
            raise reply
        return reply

    def submit(self, request):
        pytest.fail("query-only recovery must never submit")

    def cancel(self, instrument, client_order_id):
        pytest.fail("query-only recovery must never cancel")

    def trades(self, instrument, *, from_id=None):
        pytest.fail("actual fills/cursors are outside this slice")


def journals(tmp_path):
    primary, fallback = (
        JsonlJournal(tmp_path / "primary.jsonl"),
        JsonlJournal(tmp_path / "fallback.jsonl"),
    )
    return primary, fallback


def setup(tmp_path, *, bound=False, count=1, market=Market.USDM, sink_factory=None):
    primary, fallback = journals(tmp_path)
    sink = FailoverJournal(primary, fallback, lambda _code: None)
    if sink_factory is not None:
        sink = sink_factory(sink)
    decisions = [
        prepared(f"minute-{index}", Instrument("binance", market, "BTCUSDT"))
        for index in range(count)
    ]
    for decision in decisions:
        if bound:
            ExecutionDecisionBindings("A", sink).bind(decision, account_state_digest="b" * 64)
        primary.append(intent_event(decision.intents[0].request))
    requests = [decision.intents[0].request for decision in decisions]
    broker, gate = ReadBroker(), EntryGate()
    for request in requests:
        broker.replies[request.client_order_id] = observation(request)
    coordinator = OrderCoordinator("A", broker, sink, gate)
    owner = ExecutionObservationRecovery("A", coordinator, sink)
    return owner, coordinator, broker, sink, primary, fallback, requests


def recovery_rows(sink):
    return [row for row in sink.records() if row.get("kind") == KIND]


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_all_unknown_reservations_restored_before_queries_and_primary_view_is_not_settlement(
    tmp_path, market
):
    owner, coordinator, broker, sink, _primary, _fallback, requests = setup(
        tmp_path, bound=True, count=2, market=market
    )
    operations = coordinator.restore_history()

    def assert_all_reserved():
        assert len(operations) == 2
        assert all(op.state == "UNKNOWN" and op.observation is None for op in operations)
        assert all(coordinator._remaining(op) == op.request.quantity for op in operations)

    broker.before_query = assert_all_reserved
    first = requests[0]
    broker.replies[first.client_order_id] = observation(
        first, status=OrderStatus.FILLED, quantity="1", quote="100"
    )
    views = owner.recover()
    assert len(views) == 2 and len(broker.calls) == 2
    assert all(view.state == "primary_acknowledged" and view.source_bound for view in views)
    assert_all_reserved()  # Even queried FILLED has no account/fill settlement authority.
    assert BlockReason.EXECUTION in coordinator.gate.reasons
    assert BlockReason.RESTART in coordinator.gate.reasons
    assert not coordinator.gate.allows_new_risk
    event = recovery_rows(sink)[0]
    assert event["source_binding"]["cycle"]["source_digest"] == "a" * 64
    assert event["observation"]["executed_quantity"] in {"0", "1"}
    with pytest.raises(FrozenInstanceError):
        views[0].state = "historical"


def test_legacy_without_source_is_explicit_and_old_terminal_snapshot_is_not_fill_settlement(
    tmp_path,
):
    owner, coordinator, broker, sink, primary, _fallback, requests = setup(tmp_path)
    request = requests[0]
    primary.append(
        {
            "kind": "OrderObserved",
            "account_id": "A",
            "client_order_id": request.client_order_id,
            "provider_order_id": "7",
            "status": "FILLED",
        }
    )
    owner = ExecutionObservationRecovery("A", coordinator, sink)
    broker.replies[request.client_order_id] = observation(
        request, status=OrderStatus.FILLED, quantity="1", quote="100"
    )
    view = owner.recover()[0]
    assert view.source_bound is False
    assert recovery_rows(sink)[0]["source_binding"] is None
    operation = coordinator.restore_history()[0]
    assert operation.state == "UNKNOWN" and coordinator._remaining(operation) == 1


class AckLossSink:
    def __init__(self, sink):
        self.sink, self.lose = sink, True

    def records(self):
        return self.sink.records()

    def record(self, event):
        result = self.sink.record(event)
        if self.lose and event["kind"] == KIND:
            self.lose = False
            raise JournalError("synthetic ACK loss after primary bytes committed")
        return result


def test_primary_ack_loss_readback_never_publishes_pending_and_retry_never_requeries(tmp_path):
    owner, coordinator, broker, sink, _primary, _fallback, requests = setup(
        tmp_path, sink_factory=AckLossSink
    )
    with pytest.raises(TradingBlocked, match="acknowledgement"):
        owner.recover()
    original = deepcopy(recovery_rows(sink)[0])
    assert owner.snapshot() == () and len(broker.calls) == 1
    assert BlockReason.STORAGE in coordinator.gate.reasons
    broker.replies[requests[0].client_order_id] = AssertionError(
        "must not read another provider state"
    )
    view = owner.recover()[0]
    assert view.revision == 1 and view.state == "primary_acknowledged"
    assert len(broker.calls) == 1
    assert recovery_rows(sink) == [original, original]
    assert BlockReason.STORAGE in coordinator.gate.reasons  # No automatic gate clear.


class FailingJournal:
    def __init__(self, journal, *, write_before_error=False):
        self.journal, self.fail, self.write_before_error = journal, True, write_before_error

    def records(self):
        return self.journal.records()

    def append(self, event):
        if self.fail and event["kind"] == KIND:
            if self.write_before_error:
                self.journal.append(event)
            raise JournalError("synthetic primary storage failure")
        self.journal.append(event)


@pytest.mark.parametrize("both_fail", [False, True])
def test_fallback_or_volatile_write_keeps_candidate_and_requires_fresh_primary(tmp_path, both_fail):
    owner, coordinator, broker, _sink, primary, fallback, _requests = setup(tmp_path)
    failing_primary, failing_fallback = (
        FailingJournal(primary, write_before_error=True),
        FailingJournal(fallback),
    )
    failing_fallback.fail = both_fail
    sink = FailoverJournal(failing_primary, failing_fallback, lambda _code: None)
    coordinator = OrderCoordinator("A", broker, sink, coordinator.gate)
    owner = ExecutionObservationRecovery("A", coordinator, sink)
    with pytest.raises(TradingBlocked, match="fresh primary"):
        owner.recover()
    assert owner.snapshot() == () and len(broker.calls) == 1
    failing_primary.fail = False
    assert owner.recover()[0].state == "primary_acknowledged"
    assert len(broker.calls) == 1
    assert len({str(row) for row in recovery_rows(sink)}) == 1


def test_restart_exposes_only_historical_facts_until_query_and_new_primary(tmp_path):
    owner, coordinator, broker, sink, _primary, _fallback, _requests = setup(tmp_path)
    owner.recover()
    restarted_coordinator = OrderCoordinator("A", broker, sink, coordinator.gate)
    restarted = ExecutionObservationRecovery("A", restarted_coordinator, sink)
    assert restarted.snapshot()[0].state == "historical"
    assert len(broker.calls) == 1
    assert restarted.recover()[0].revision == 2
    assert len(broker.calls) == 2
    assert restarted_coordinator.restore_history()[0].observation is None


def test_restart_after_primary_ack_loss_exposes_history_without_fresh_receipt(tmp_path):
    owner, _coordinator, broker, sink, _primary, _fallback, _requests = setup(
        tmp_path, sink_factory=AckLossSink
    )
    with pytest.raises(TradingBlocked):
        owner.recover()
    restarted_coordinator = OrderCoordinator("A", broker, sink, EntryGate())
    restarted = ExecutionObservationRecovery("A", restarted_coordinator, sink)
    assert restarted.snapshot()[0].state == "historical"
    assert len(broker.calls) == 1 and not restarted_coordinator.gate.allows_new_risk


def test_history_chain_ignores_primary_fallback_concatenation_order_and_exact_duplicates(tmp_path):
    owner, coordinator, broker, sink, _primary, _fallback, requests = setup(tmp_path / "original")
    owner.recover()
    request = requests[0]
    broker.replies[request.client_order_id] = observation(
        request, status=OrderStatus.PARTIALLY_FILLED, quantity=".5", quote="50"
    )
    owner.recover()
    first, second = recovery_rows(sink)
    primary, fallback = journals(tmp_path / "shuffled")
    primary.append(intent_event(request))
    primary.append(second)
    fallback.append(first)
    fallback.append(second)
    shuffled = FailoverJournal(primary, fallback, lambda _code: None)
    restarted = ExecutionObservationRecovery(
        "A", OrderCoordinator("A", broker, shuffled, EntryGate()), shuffled
    )
    view = restarted.snapshot()[0]
    assert view.revision == 2 and view.observation.executed_quantity == D(".5")
    assert view.state == "historical" and len(broker.calls) == 2


@pytest.mark.parametrize("damage", ["gap", "previous", "duplicate", "untyped_request"])
def test_invalid_recovery_chains_are_rejected_before_any_new_query(tmp_path, damage):
    owner, _coordinator, broker, sink, _primary, _fallback, requests = setup(tmp_path / "original")
    owner.recover()
    event = deepcopy(recovery_rows(sink)[0])
    primary, fallback = journals(tmp_path / "damaged")
    primary.append(intent_event(requests[0]))
    if damage == "duplicate":
        primary.append(event)
        event["observation"]["provider_order_id"] = "8"
    elif damage == "gap":
        event["revision"] = 2
    elif damage == "previous":
        event["previous_sha256"] = "a" * 64
    else:
        event["request"]["reduce_only"] = 0
    primary.append(event)
    damaged = FailoverJournal(primary, fallback, lambda _code: None)
    with pytest.raises(JournalError):
        ExecutionObservationRecovery(
            "A", OrderCoordinator("A", broker, damaged, EntryGate()), damaged
        )
    assert len(broker.calls) == 1


def test_restart_rejects_a_later_terminal_discrepancy_even_with_valid_revision_chain(tmp_path):
    owner, _coordinator, broker, sink, primary, _fallback, requests = setup(tmp_path)
    request = requests[0]
    broker.replies[request.client_order_id] = observation(
        request, status=OrderStatus.FILLED, quantity="1", quote="100"
    )
    first = owner.recover()[0]
    changed = deepcopy(recovery_rows(sink)[0])
    changed["revision"], changed["previous_sha256"] = 2, first.record_sha256
    changed["observation"]["status"] = "CANCELED"
    primary.append(changed)
    with pytest.raises(TradingBlocked, match="terminal order changed"):
        ExecutionObservationRecovery("A", OrderCoordinator("A", broker, sink, EntryGate()), sink)
    assert len(broker.calls) == 1


@pytest.mark.parametrize("change", ["provider", "quantity", "quote", "terminal"])
def test_cumulative_or_terminal_conflict_is_sticky_without_mutating_reservations(tmp_path, change):
    owner, coordinator, broker, sink, _primary, _fallback, requests = setup(tmp_path)
    request = requests[0]
    if change == "terminal":
        prior = observation(request, status=OrderStatus.FILLED, quantity="1", quote="100")
        altered = observation(request, status=OrderStatus.CANCELED, quantity="1", quote="100")
    else:
        prior = observation(request, status=OrderStatus.PARTIALLY_FILLED, quantity=".5", quote="50")
        altered = observation(
            request,
            status=OrderStatus.PARTIALLY_FILLED,
            quantity=".4" if change == "quantity" else ".5",
            quote="49" if change == "quote" else "50",
            provider="8" if change == "provider" else "7",
        )
    broker.replies[request.client_order_id] = prior
    owner.recover()
    broker.replies[request.client_order_id] = altered
    with pytest.raises((ValidationError, TradingBlocked)):
        owner.recover()
    assert len(recovery_rows(sink)) == 1
    assert owner.snapshot()[0].observation == prior
    operation = coordinator.restore_history()[0]
    assert operation.observation is None and coordinator._remaining(operation) == 1
    with pytest.raises(TradingBlocked, match="explicit account review"):
        owner.recover()
    assert len(broker.calls) == 2


def test_conflicting_same_cycle_source_binding_keeps_same_request_id_but_blocks_recovery(tmp_path):
    _owner, coordinator, broker, sink, primary, _fallback, _requests = setup(tmp_path, bound=True)
    binding = deepcopy(
        next(row for row in sink.records() if row["kind"] == "ExecutionDecisionBound")
    )
    binding["cycle"]["source_digest"] = "c" * 64
    binding["decision"]["intents"][0]["cycle"]["source_digest"] = "c" * 64
    primary.append(binding)
    with pytest.raises(ValidationError, match="conflicting durable source"):
        ExecutionObservationRecovery("A", coordinator, sink)
    assert broker.calls == []


@pytest.mark.parametrize("change", ["provider", "terminal", "partial"])
def test_legacy_observation_lower_bound_is_enforced_without_inventing_quantities(tmp_path, change):
    _owner, coordinator, broker, sink, primary, _fallback, requests = setup(tmp_path)
    request = requests[0]
    primary.append(
        {
            "kind": "OrderObserved",
            "account_id": "A",
            "client_order_id": request.client_order_id,
            "provider_order_id": "8" if change == "provider" else "7",
            "status": "FILLED" if change == "terminal" else "PARTIALLY_FILLED",
        }
    )
    owner = ExecutionObservationRecovery("A", coordinator, sink)
    with pytest.raises((ValidationError, TradingBlocked)):
        owner.recover()
    assert recovery_rows(sink) == []


def test_changed_or_torn_history_never_silently_becomes_empty(tmp_path):
    owner, _coordinator, _broker, _sink, primary, _fallback, _requests = setup(tmp_path)
    owner.recover()
    primary.path.write_bytes(primary.path.read_bytes() + b'{"event":')
    with pytest.raises(JournalError):
        owner.snapshot()


def test_already_mutated_coordinator_is_not_relabelled_unknown_or_silently_reused(tmp_path):
    _owner, coordinator, broker, sink, _primary, _fallback, requests = setup(tmp_path)
    coordinator.reconcile(requests[0])  # Existing mutating API, synthetic query only.
    with pytest.raises(TradingBlocked, match="fresh restored coordinator"):
        ExecutionObservationRecovery("A", coordinator, sink)
    assert len(broker.calls) == 1
    assert coordinator.restore_history()[0].state == "OBSERVED"
