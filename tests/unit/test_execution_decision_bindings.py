"""Decision/source binding only: real temporary JSONL or memory sink, never brokers."""

from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D

import pytest

from omega.application.execution_bindings import ExecutionDecisionBindings
from omega.application.execution_intents import (
    ExecutionCycle,
    ExecutionDecision,
    ExecutionIntent,
)
from omega.domain.errors import JournalError, TradingBlocked, ValidationError
from omega.domain.models import Instrument, Market, OrderKind, Side
from omega.domain.trading import OrderProposal
from omega.infrastructure.journal import FailoverJournal, JsonlJournal
from omega.ports.journal import Durability

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


class Sink:
    def __init__(self, *, durability=Durability.PRIMARY, error=None, persist=True):
        self.events, self.calls = [], []
        self.durability, self.error, self.persist = durability, error, persist

    def records(self):
        return tuple(deepcopy(self.events))

    def record(self, event):
        self.calls.append(deepcopy(event))
        if self.persist:
            self.events.append(deepcopy(event))
        if self.error is not None:
            raise self.error
        return self.durability


def decision(
    *,
    cycle_id="minute-1",
    source="a" * 64,
    quantity=".125",
    account="A",
    strategy="TEF",
    instrument=BTC,
):
    cycle = ExecutionCycle(account, strategy, cycle_id, source)
    intent = ExecutionIntent(cycle, instrument, 0, OrderProposal(Side.BUY, D(quantity)))
    return ExecutionDecision(cycle, (intent,), None, "entry")


def bind(owner, prepared=None, *, state="b" * 64):
    return owner.bind(prepared or decision(), account_state_digest=state)


def test_primary_ack_exact_replay_and_immutable_receipt():
    sink = Sink()
    owner = ExecutionDecisionBindings("A", sink)
    result = bind(owner)
    assert result.admitted and not result.recovery_required
    assert owner.lookup(decision().cycle) == result
    assert bind(owner) is result and len(sink.calls) == 1
    with pytest.raises(FrozenInstanceError):
        result.state = "recovery_required"
    event = sink.events[0]
    assert event["cycle"]["source_digest"] == "a" * 64
    assert event["account_state_digest"] == "b" * 64
    assert event["decision"]["intents"][0]["proposal"]["quantity"] == "0.125"
    assert (
        event["decision"]["intents"][0]["client_order_id"]
        == result.decision.intents[0].request.client_order_id
    )


@pytest.mark.parametrize(
    "durability", [Durability.FALLBACK, Durability.VOLATILE, "PRIMARY_COMMITTED"]
)
def test_nonprimary_or_untyped_ack_cannot_admit_and_retry_requires_fresh_primary(
    durability,
):
    sink = Sink(durability=durability)
    owner = ExecutionDecisionBindings("A", sink)
    with pytest.raises(TradingBlocked, match="fresh primary"):
        bind(owner)
    assert owner.lookup(decision().cycle).state == "pending"
    candidate = deepcopy(sink.calls[0])
    sink.durability = Durability.PRIMARY
    assert bind(owner).admitted
    assert sink.calls == [candidate, candidate]


@pytest.mark.parametrize(
    "error", [JournalError("ACK lost"), OSError("write failed"), TimeoutError()]
)
def test_written_candidate_with_lost_ack_is_not_admitted_by_readback(error):
    sink = Sink(error=error)
    owner = ExecutionDecisionBindings("A", sink)
    with pytest.raises(TradingBlocked, match="acknowledgement"):
        bind(owner)
    assert len(sink.records()) == 1
    assert owner.lookup(decision().cycle).admitted is False
    sink.error = None
    assert bind(owner).admitted
    assert sink.calls[0] == sink.calls[1]


def test_unwritten_candidate_is_retained_and_can_only_retry_identical_contents():
    sink = Sink(error=JournalError("unavailable"), persist=False)
    owner = ExecutionDecisionBindings("A", sink)
    with pytest.raises(TradingBlocked):
        bind(owner)
    with pytest.raises(ValidationError, match="another source"):
        bind(owner, decision(quantity=".25"))
    assert len(sink.calls) == 1 and sink.events == []
    sink.error, sink.persist = None, True
    assert bind(owner).admitted


def test_restart_after_write_ack_loss_never_promotes_readback_to_admission():
    sink = Sink(error=JournalError("primary committed but ACK lost"))
    with pytest.raises(TradingBlocked):
        bind(ExecutionDecisionBindings("A", sink))
    sink.error = None
    restarted = ExecutionDecisionBindings("A", sink)
    assert bind(restarted).recovery_required
    assert not restarted.lookup(decision().cycle).admitted
    assert len(sink.calls) == 1


def test_restart_and_primary_fallback_duplicates_remain_recovery_required():
    sink = Sink()
    first = bind(ExecutionDecisionBindings("A", sink))
    sink.events.append(deepcopy(sink.events[0]))
    restarted = ExecutionDecisionBindings("A", sink)
    recovered = restarted.lookup(decision().cycle)
    assert recovered.decision == first.decision and recovered.recovery_required
    assert not bind(restarted).admitted and len(sink.calls) == 1


@pytest.mark.parametrize(
    "change",
    [
        {"source": "c" * 64},
        {"quantity": ".25"},
        {"instrument": Instrument("binance", Market.SPOT, "BTCUSDT")},
    ],
)
def test_source_or_request_change_conflicts_even_after_restart(change):
    sink = Sink()
    bind(ExecutionDecisionBindings("A", sink))
    restarted = ExecutionDecisionBindings("A", sink)
    with pytest.raises(ValidationError, match="another source"):
        bind(restarted, decision(**change))
    assert len(sink.calls) == 1


def test_account_state_and_decision_reason_are_bound_to_the_same_cycle():
    sink = Sink()
    owner = ExecutionDecisionBindings("A", sink)
    original = bind(owner)
    with pytest.raises(ValidationError, match="another source"):
        bind(owner, state="c" * 64)
    with pytest.raises(ValidationError, match="another source"):
        bind(owner, replace(original.decision, reason="changed"))
    assert len(sink.calls) == 1


def test_namespaces_are_distinct_and_foreign_account_cannot_bind_or_lookup():
    sink = Sink()
    owner = ExecutionDecisionBindings("A", sink)
    for prepared in (
        decision(),
        decision(cycle_id="minute-2"),
        decision(strategy="other"),
    ):
        assert bind(owner, prepared).admitted
    other = ExecutionDecisionBindings("B", sink)
    assert bind(other, decision(account="B")).admitted
    with pytest.raises(ValidationError, match="another account"):
        bind(owner, decision(account="B"))
    with pytest.raises(ValidationError, match="another account"):
        owner.lookup(decision(account="B").cycle)
    assert len(sink.calls) == 4


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_complete_limit_reduction_and_original_cancel_target_round_trip(market):
    sink = Sink()
    cycle = decision().cycle
    prior = ExecutionIntent(
        cycle,
        Instrument("binance", market, "BTCUSDT"),
        0,
        OrderProposal(
            Side.SELL,
            D(".1250"),
            OrderKind.LIMIT,
            D("100.00"),
            True,
            "local_protection",
        ),
    )
    reducing = ExecutionDecision(cycle, (prior,), None, "reduce")
    assert bind(ExecutionDecisionBindings("A", sink), reducing).admitted
    cancel_cycle = replace(cycle, cycle_id="minute-2", source_digest="d" * 64)
    cancel = ExecutionDecision(cancel_cycle, (), prior, "cancel_previous")
    owner = ExecutionDecisionBindings("A", sink)
    assert bind(owner, cancel).decision.cancel_client_order_id == prior.request.client_order_id
    restored = ExecutionDecisionBindings("A", sink).lookup(cancel_cycle)
    assert restored.recovery_required and restored.decision == cancel
    assert restored.decision.cancel_intent.cycle == cycle
    assert restored.decision.cancel_intent.proposal.reason == "local_protection"
    assert restored.decision.cancel_intent.request.reduce_only is (market == Market.USDM)


def test_hold_and_equivalent_decimal_representation_do_not_create_another_binding():
    sink = Sink()
    owner = ExecutionDecisionBindings("A", sink)
    first = bind(owner, decision(quantity=".1250"))
    assert bind(owner, decision(quantity=".125")) == first
    hold = ExecutionDecision(replace(decision().cycle, cycle_id="hold"), (), None, "hold")
    assert bind(owner, hold).decision == hold
    assert ExecutionDecisionBindings("A", sink).lookup(hold.cycle).recovery_required


@pytest.mark.parametrize("damage", ["schema", "decimal", "client_id", "reduce_only", "extra"])
def test_malformed_or_changed_history_is_never_ignored(damage):
    sink = Sink()
    bind(ExecutionDecisionBindings("A", sink))
    event = sink.events[0]
    intent = event["decision"]["intents"][0]
    if damage == "schema":
        event["schema_version"] = True
    elif damage == "decimal":
        intent["proposal"]["quantity"] = "0.1250"
    elif damage == "client_id":
        intent["client_order_id"] = "another"
    elif damage == "reduce_only":
        intent["reduce_only"] = 0
    else:
        event["extra"] = "unknown"
    with pytest.raises(JournalError, match="malformed"):
        ExecutionDecisionBindings("A", sink)


def test_conflicting_duplicate_history_and_missing_acknowledged_history_block():
    sink = Sink()
    owner = ExecutionDecisionBindings("A", sink)
    bind(owner)
    conflicting = deepcopy(sink.events[0])
    conflicting["account_state_digest"] = "c" * 64
    sink.events.append(conflicting)
    with pytest.raises(ValidationError, match="another source"):
        ExecutionDecisionBindings("A", sink)
    sink.events.clear()
    with pytest.raises(JournalError, match="missing"):
        owner.lookup(decision().cycle)


@pytest.mark.parametrize("state", [None, "B" * 64, "b" * 63, True])
def test_invalid_state_digest_is_rejected_before_any_write(state):
    sink = Sink()
    with pytest.raises(ValidationError, match="SHA256"):
        bind(ExecutionDecisionBindings("A", sink), state=state)
    assert sink.calls == []


def test_real_jsonl_reopen_and_torn_tail_are_not_repaired(tmp_path):
    primary, fallback = tmp_path / "primary.jsonl", tmp_path / "fallback.jsonl"
    sink = FailoverJournal(JsonlJournal(primary), JsonlJournal(fallback), lambda _code: None)
    assert bind(ExecutionDecisionBindings("A", sink)).admitted
    reopened = FailoverJournal(JsonlJournal(primary), JsonlJournal(fallback), lambda _code: None)
    assert bind(ExecutionDecisionBindings("A", reopened)).recovery_required
    primary.write_bytes(primary.read_bytes() + b'{"event":')
    with pytest.raises(JournalError):
        ExecutionDecisionBindings("A", reopened)
