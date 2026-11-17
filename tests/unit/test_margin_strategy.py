"""Actual TS modules and factual margin next-bar accounting; no external IO."""

import json
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from decimal import localcontext
from threading import Event, RLock, Thread

import pytest

from omega.application.margin_account import MarginAccountOwner
from omega.application.margin_execution import FrozenPendingOrder
from omega.application.margin_risk import approve_order
from omega.application.margin_strategy import (
    MarginStrategyAdapter,
    MarginStrategyState,
    margin_bar_digest,
)
from omega.application.virtual_margin import JournalReceipt, VirtualMarginEngine
from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.domain.margin import (
    MarginAccount,
    MarginBracket,
    MarginBracketSnapshot,
    MarginMode,
    MarginPosition,
    MarkSnapshot,
)
from omega.domain.models import Candle, Instrument, Market, Side
from omega.domain.trading import ExecutionConstraints, OrderProposal
from omega.ports.journal import Durability
from omega.strategies.neural_signal.module import NeuralSignalModule
from omega.strategies.target_quantity.module import TargetQuantityModule
from omega.strategies.tef.module import TefModule

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")
RULES = ExecutionConstraints(leverage=D(10), can_short=True)


def candle(index=0, *, price="100", low=None, instrument=BTC):
    value = D(price)
    return Candle(
        instrument, index * 60000, (index + 1) * 60000, value, value, D(low or price), value, D(10)
    )


def marks(time=60000, price="100"):
    return tuple(MarkSnapshot(item, D(price), time, time, "test_observed") for item in (BTC, ETH))


def brackets():
    return tuple(
        MarginBracketSnapshot(
            item,
            (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
            0,
            0,
            "test_bracket",
            "a" * 64,
        )
        for item in (BTC, ETH)
    )


class AccountStore:
    def __init__(self):
        self.value = None
        self.ack_loss = False

    def load(self, _):
        return self.value

    def save(self, account, *, expected_sequence):
        if self.value == account:
            return
        assert (None if self.value is None else len(self.value.ledger)) == expected_sequence
        self.value = account
        if self.ack_loss:
            self.ack_loss = False
            raise StorageError("synthetic factual account ACK loss")


class Journal:
    def __init__(self):
        self.rows, self.calls = [], []
        self.ack_loss = False

    def read(self, _):
        return tuple(self.rows) if self.rows else None

    def append(self, record):
        self.calls.append(record)
        previous = next(
            (row for row in self.rows if row.record.record_id == record.record_id), None
        )
        if previous:
            assert previous.record == record
        else:
            self.rows.append(JournalReceipt(record, Durability.PRIMARY))
        if self.ack_loss:
            self.ack_loss = False
            raise StorageError("synthetic intent ACK loss")
        return Durability.PRIMARY


class StrategyStore:
    """Every boundary really round-trips the strict typed JSON codec."""

    def __init__(self):
        self.document = None
        self.saved = []
        self.ack_at = self.fail_at = None

    def load(self, _):
        return (
            None if self.document is None else MarginStrategyState.parse(json.loads(self.document))
        )

    def save(self, state, *, expected_revision):
        previous = self.load(state.strategy_id)
        if previous == state:
            return
        if (None if previous is None else previous.revision) != expected_revision:
            raise ProtocolError("strategy CAS conflict")
        if state.revision == self.fail_at:
            self.fail_at = None
            raise StorageError("strategy save failed before commit")
        self.document = json.dumps(state.document(), sort_keys=True)
        self.saved.append(state)
        if state.revision == self.ack_at:
            self.ack_at = None
            raise StorageError("strategy save committed; ACK lost")


def setup(module=None, *, quantity="0", cash="1000", mode=MarginMode.CROSS, create=True):
    initial = MarginAccount(
        "account",
        "USDT",
        D(cash),
        (
            MarginPosition(
                BTC,
                "s1",
                mode,
                10,
                D(100),
                D(quantity),
                D(100) if D(quantity) else D(0),
                D(100) if mode == MarginMode.ISOLATED else D(0),
            ),
            MarginPosition(ETH, "s2", MarginMode.CROSS, 10, D(100)),
        ),
    )
    account_store, journal, strategy_store = AccountStore(), Journal(), StrategyStore()
    owner = MarginAccountOwner(initial, account_store, create_if_missing=True)
    execution = VirtualMarginEngine(owner, journal, create_if_missing=True)
    module = module or TargetQuantityModule(BTC, {"quantity": "1"})
    adapter = MarginStrategyAdapter(
        "s1", BTC, module, execution, strategy_store, owner_id="s1", create_if_missing=create
    )
    return initial, account_store, journal, strategy_store, execution, adapter


def evaluate(adapter, index=0, *, features=None, **changes):
    values = dict(
        as_of_ms=(index + 1) * 60000,
        marks=marks((index + 1) * 60000),
        brackets=brackets(),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
        input_reference="b" * 64,
    )
    values.update(changes)
    return adapter.evaluate_closed(
        candle(index), {} if features is None else features, RULES, **values
    )


def settle(execution, index=1, **changes):
    bars = (candle(index), candle(index, instrument=ETH))
    for phase in ("open", "close"):
        plan = execution.prepare_batch(
            bars, as_of_ms=(index + 1) * 60000, phase=phase, brackets=brackets()
        )
        if plan.facts or plan.rejections:
            execution.commit_batch(plan)
    return execution.snapshot()


def restore(initial, account_store, journal, strategy_store, module=None):
    execution = VirtualMarginEngine(MarginAccountOwner(initial, account_store), journal)
    adapter = MarginStrategyAdapter(
        "s1",
        BTC,
        module or TargetQuantityModule(BTC, {"quantity": "1"}),
        execution,
        strategy_store,
        owner_id="s1",
    )
    return execution, adapter


@pytest.mark.parametrize("mode", [MarginMode.CROSS, MarginMode.ISOLATED])
def test_target_quantity_durable_order_nextbar_accounting_restart_exactly_once(mode):
    initial, account_store, journal, store, execution, adapter = setup(mode=mode)
    execution.manual_resume("start")
    decision = evaluate(adapter)
    assert decision.orders[0].quantity == 1  # No hidden leverage multiplication.
    pending = execution.snapshot().pending[0]
    assert pending.eligible_at_ms == 60000 and pending.original.proposal == decision.orders[0]
    assert store.saved[-2].pending and store.saved[-2].last_open_time_ms is None
    assert store.saved[-1].pending is None and store.saved[-1].last_open_time_ms == 0
    execution.pause_entries("pause")
    result = settle(execution)
    assert len(result.account.ledger) == 1 and result.account.positions[0].quantity == 1
    assert result.account.ledger[0].fact.order_id == pending.order_id
    if mode == MarginMode.CROSS:
        assert result.account.cross_wallet == D("999.9")
    else:
        assert result.account.cross_wallet == 1000 and result.account.positions[
            0
        ].isolated_wallet == D("99.9")
    restarted, restored_adapter = restore(initial, account_store, journal, store)
    assert restarted.snapshot().paused and restarted.snapshot().pending == ()
    writes = len(journal.calls), len(store.saved)
    assert evaluate(restored_adapter) == decision
    assert (len(journal.calls), len(store.saved)) == writes
    assert not evaluate(restored_adapter, 1).orders
    assert len(restarted.snapshot().account.ledger) == 1


def test_actual_tef_margin_sizing_is_private_and_survives_checkpoint_restart():
    module = TefModule(BTC)
    initial, account_store, journal, store, execution, adapter = setup(module)
    execution.manual_resume("start")
    decision = evaluate(adapter, features={"0_MMACD_MSDELTAABSMAREL": 1.0})
    assert decision.reason == "ENTRY" and decision.orders[0].quantity == D("7.615")
    settle(execution)
    restarted, restored_adapter = restore(initial, account_store, journal, store, TefModule(BTC))
    assert restored_adapter.module.checkpoint() == adapter.module.checkpoint()
    stopped = evaluate(restored_adapter, 1, features={"0_MMACD_MSDELTAABSMAREL": 0.0})
    assert stopped.orders[0].reducing and stopped.orders[0].quantity == D("7.615")
    result = settle(restarted, 2)
    assert result.account.positions[0].quantity == 0 and len(result.account.ledger) == 2


def test_tef_fsl_with_missing_indicators_and_paused_entries_keeps_local_protection():
    *_, execution, adapter = setup(TefModule(BTC, {"fullStopLossImmediate": ".05"}), quantity="1")
    sample = candle(low="90")
    decision = adapter.evaluate_closed(
        sample,
        {},
        RULES,
        as_of_ms=60000,
        marks=(),
        brackets=(),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
    )
    assert decision.reason == "FSLIMMED" and decision.orders[0].reducing
    assert execution.snapshot().paused and execution.snapshot().pending
    assert settle(execution).account.positions[0].quantity == 0


@pytest.mark.parametrize(
    "fault", ["stage_ack", "stage_before", "intent_ack", "final_ack", "final_before"]
)
def test_fixed_staged_action_recovery_never_reevaluates_or_changes_order(fault, monkeypatch):
    initial, account_store, journal, store, execution, adapter = setup()
    execution.manual_resume("start")
    if fault == "stage_ack":
        store.ack_at = 1
    elif fault == "stage_before":
        store.fail_at = 1
    elif fault == "intent_ack":
        journal.ack_loss = True
    elif fault == "final_ack":
        store.ack_at = 2
    else:
        store.fail_at = 2
    with pytest.raises(StorageError):
        evaluate(adapter)
    staged = adapter._write_pending[1] if adapter._write_pending else adapter.snapshot().checkpoint
    fixed = staged.pending.order if staged.pending else execution.snapshot().pending[0]
    monkeypatch.setattr(
        TargetQuantityModule,
        "evaluate",
        lambda *_: pytest.fail("reconciliation must not reevaluate TS"),
    )
    adapter.reconcile()
    assert adapter.snapshot().state == "ready"
    assert execution.snapshot().pending == (fixed,)
    assert (
        len([row for row in journal.rows if isinstance(row.record.payload, FrozenPendingOrder)])
        == 1
    )
    restarted, restored_adapter = restore(initial, account_store, journal, store)
    assert restored_adapter.snapshot().state == "ready" and restarted.snapshot().paused
    assert restarted.snapshot().pending == (fixed,)
    result = settle(restarted)
    assert (
        len(result.account.ledger) == 1 and result.account.ledger[0].fact.order_id == fixed.order_id
    )


def test_restart_before_intent_requires_manual_resume_and_keeps_staged_input(monkeypatch):
    initial, account_store, journal, store, execution, adapter = setup()
    execution.manual_resume("start")
    original = execution.submit_intent
    monkeypatch.setattr(
        execution,
        "submit_intent",
        lambda *a, **kw: (_ for _ in ()).throw(StorageError("before intent")),
    )
    with pytest.raises(StorageError):
        evaluate(adapter)
    fixed = store.load("s1").pending
    assert not execution.snapshot().pending
    restarted, restored_adapter = restore(initial, account_store, journal, store)
    with pytest.raises(TradingBlocked):
        restored_adapter.reconcile()
    assert store.load("s1").pending == fixed and not restarted.snapshot().pending
    restarted.manual_resume("manual-new-start")
    restored_adapter.reconcile()
    assert restarted.snapshot().pending == (fixed.order,)
    assert len(account_store.value.ledger) == 0
    monkeypatch.setattr(execution, "submit_intent", original)


def test_duplicate_cycle_conflict_gap_late_and_no_concrete_change_are_rejected():
    *_, execution, adapter = setup()
    execution.manual_resume("start")
    first = evaluate(adapter)
    assert evaluate(adapter) == first
    with pytest.raises(ProtocolError, match="changed"):
        evaluate(adapter, fee_rate=D(".002"))
    with pytest.raises(ProtocolError, match="causal"):
        evaluate(adapter, 2)
    # The original pending creation is a concrete book change. A hold consumes it once.
    assert not evaluate(adapter, decision_cycle=1).orders
    with pytest.raises(ProtocolError, match="concrete"):
        evaluate(adapter, decision_cycle=2)
    evaluate(adapter, 1)
    with pytest.raises(ProtocolError, match="causal"):
        evaluate(adapter, 0)


def test_tef_cancel_then_confirmed_same_minute_cycle_submits_protection():
    module = TefModule(BTC, {"fullStopLossImmediate": ".05"})
    *_, execution, adapter = setup(module, quantity="1")
    execution.manual_resume("start")
    account = execution.snapshot().account
    existing = approve_order(
        account,
        "old-entry",
        OrderProposal(Side.BUY, D(1)),
        instrument=BTC,
        owner_id="s1",
        as_of_ms=0,
        marks=marks(0),
        brackets=brackets(),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
    )
    execution.submit_intent(
        FrozenPendingOrder(existing, 60000), marks=marks(0), brackets=brackets()
    )
    sample = candle(low="90")
    params = dict(
        as_of_ms=60000,
        marks=marks(),
        brackets=brackets(),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
    )
    cancel = adapter.evaluate_closed(sample, {}, RULES, **params)
    assert cancel.cancel_pending and execution.snapshot().pending == ()
    execution.pause_entries("pause-before-protection")
    close = adapter.evaluate_closed(sample, {}, RULES, decision_cycle=1, **params)
    assert close.orders[0].reducing and close.reason == "FSLIMMED"
    order_id = execution.snapshot().pending[0].order_id
    assert adapter.evaluate_closed(sample, {}, RULES, decision_cycle=1, **params) == close
    assert execution.snapshot().pending[0].order_id == order_id
    assert settle(execution).account.positions[0].quantity == 0


def test_available_wallet_collateral_and_other_cross_reservations_limit_tef_sizing():
    *_, execution, adapter = setup(TefModule(BTC), cash="15")
    execution.manual_resume("start")
    account = execution.snapshot().account
    other = approve_order(
        account,
        "eth",
        OrderProposal(Side.BUY, D(1)),
        instrument=ETH,
        owner_id="s2",
        as_of_ms=0,
        marks=marks(0),
        brackets=brackets(),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
    )
    execution.submit_intent(FrozenPendingOrder(other, 60000), marks=marks(0), brackets=brackets())
    # Only 4.9 margin remains. Entry fee makes the exact TEF budget-sized proposal
    # fail the common risk gate rather than borrow from the other reservation.
    decision = evaluate(adapter, features={"0_MMACD_MSDELTAABSMAREL": 1.0})
    assert not decision.orders and decision.reason.startswith("blocked:")
    assert (
        len(execution.snapshot().pending) == 1 and execution.snapshot().pending[0].order_id == "eth"
    )
    assert adapter.module.last_open_time_ms == 0


def test_unavailable_other_instrument_marks_blocks_entries_but_not_existing_pending():
    *_, execution, adapter = setup()
    execution.manual_resume("start")
    account = execution.snapshot().account
    other = approve_order(
        account,
        "eth",
        OrderProposal(Side.BUY, D(1)),
        instrument=ETH,
        owner_id="s2",
        as_of_ms=0,
        marks=marks(0),
        brackets=brackets(),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
    )
    execution.submit_intent(FrozenPendingOrder(other, 60000), marks=marks(0), brackets=brackets())
    decision = evaluate(adapter, marks=(marks()[0],))
    assert not decision.orders
    assert execution.snapshot().pending == (FrozenPendingOrder(other, 60000),)


def test_non_aligned_actual_decision_latency_never_uses_past_nextbar_open():
    *_, execution, adapter = setup()
    execution.manual_resume("start")
    evaluate(adapter, as_of_ms=60001)
    assert execution.snapshot().pending[0].eligible_at_ms == 120000
    assert len(settle(execution, 1).account.ledger) == 0
    assert len(settle(execution, 2).account.ledger) == 1


def test_missing_checkpoint_is_fail_closed_and_bootstrap_change_requires_explicit_migration():
    initial, account_store, journal, store, execution, adapter = setup(create=False)
    assert adapter.snapshot().state == "unavailable" and store.document is None
    with pytest.raises(StorageError):
        adapter.reconcile()
    assert store.document is None
    created = MarginStrategyAdapter(
        "s1",
        BTC,
        TargetQuantityModule(BTC, {"quantity": "1"}),
        execution,
        store,
        owner_id="s1",
        create_if_missing=True,
    )
    assert created.snapshot().state == "ready"
    changed = MarginStrategyAdapter(
        "s1", BTC, TargetQuantityModule(BTC, {"quantity": "2"}), execution, store, owner_id="s1"
    )
    assert changed.snapshot().state == "conflict"
    assert store.load("s1").bootstrap_digest == created.snapshot().checkpoint.bootstrap_digest


def test_json_codec_exact_roundtrip_staged_and_committed_and_rejects_unknown_tags():
    *_, store, execution, adapter = setup()
    execution.manual_resume("start")
    evaluate(adapter)
    for state in store.saved:
        assert MarginStrategyState.parse(json.loads(json.dumps(state.document()))) == state
    document = deepcopy(store.saved[-2].document())
    document["state"]["record"] = "ImportAndExecute"
    with pytest.raises(ValidationError):
        MarginStrategyState.parse(document)
    for key, value in (("revision", True), ("last_cycle", True)):
        document = deepcopy(store.saved[-1].document())
        document["state"]["fields"][key] = value
        with pytest.raises(ValidationError):
            MarginStrategyState.parse(document)


def test_low_ambient_precision_never_changes_quantities_or_reservation_fingerprint():
    outputs = []
    for precision in (6, 28):
        *_, execution, adapter = setup(TefModule(BTC))
        execution.manual_resume("start")
        with localcontext() as calculation:
            calculation.prec = precision
            decision = evaluate(adapter, features={"0_MMACD_MSDELTAABSMAREL": 1.0})
            outputs.append((decision, execution.snapshot().pending[0].original))
    assert outputs[0] == outputs[1]


def test_owner_leverage_and_strategy_codec_validation_do_not_mutate_execution():
    *_, execution, adapter = setup()
    execution.manual_resume("start")
    with pytest.raises(ProtocolError, match="leverage"):
        adapter.evaluate_closed(
            candle(),
            {},
            replace(RULES, leverage=D(5)),
            as_of_ms=60000,
            marks=marks(),
            brackets=brackets(),
            worst_execution_price=D(100),
            fee_rate=D(".001"),
        )
    assert (
        not execution.snapshot().pending and adapter.snapshot().checkpoint.last_open_time_ms is None
    )
    with pytest.raises(ValidationError, match="own"):
        MarginStrategyAdapter(
            "other", BTC, TargetQuantityModule(BTC), execution, StrategyStore(), owner_id="other"
        )


@pytest.mark.parametrize(
    "field,value",
    [("worst_execution_price", True), ("fee_rate", D("-0.1")), ("fee_rate", D("1e-19"))],
)
def test_invalid_numeric_policy_is_rejected_even_when_entries_are_paused(field, value):
    *_, execution, adapter = setup()
    with pytest.raises(ValidationError):
        evaluate(adapter, **{field: value})
    assert adapter.snapshot().checkpoint.last_open_time_ms is None
    assert not execution.snapshot().pending


def test_reconciliation_rejects_rollback_or_foreign_checkpoint_advance():
    *_, store, execution, adapter = setup()
    bootstrap = store.document
    execution.manual_resume("start")
    evaluate(adapter)
    current = adapter.snapshot().checkpoint
    store.document = bootstrap
    with pytest.raises(ProtocolError, match="outside"):
        adapter.reconcile()
    assert adapter.snapshot().checkpoint == current
    assert len(execution.snapshot().pending) == 1


def test_cancel_staged_payload_cannot_target_other_strategy_pending(monkeypatch):
    initial, account_store, journal, store, execution, adapter = setup(
        TefModule(BTC, {"fullStopLossImmediate": ".05"}), quantity="1"
    )
    execution.manual_resume("start")
    for instrument, owner in ((BTC, "s1"), (ETH, "s2")):
        account = execution.snapshot().account
        original = approve_order(
            account,
            instrument.symbol,
            OrderProposal(Side.BUY, D(1)),
            instrument=instrument,
            owner_id=owner,
            as_of_ms=0,
            marks=marks(0),
            brackets=brackets(),
            worst_execution_price=D(100),
            fee_rate=D(".001"),
            reservations=tuple(item.original for item in execution.snapshot().pending),
        )
        execution.submit_intent(
            FrozenPendingOrder(original, 60000), marks=marks(0), brackets=brackets()
        )
    monkeypatch.setattr(
        execution,
        "cancel_order",
        lambda *args: (_ for _ in ()).throw(StorageError("before cancel")),
    )
    with pytest.raises(StorageError):
        adapter.evaluate_closed(
            candle(low="90"),
            {},
            RULES,
            as_of_ms=60000,
            marks=marks(),
            brackets=brackets(),
            worst_execution_price=D(100),
            fee_rate=D(".001"),
        )
    staged = store.load("s1")
    forged = replace(
        staged,
        pending=replace(
            staged.pending, cancellations=((ETH.symbol, staged.pending.cancellations[0][1]),)
        ),
    )
    store.document = json.dumps(forged.document())
    restarted, restored_adapter = restore(
        initial, account_store, journal, store, TefModule(BTC, {"fullStopLossImmediate": ".05"})
    )
    with pytest.raises(ProtocolError, match="another owner"):
        restored_adapter.reconcile()
    assert {item.order_id for item in restarted.snapshot().pending} == {BTC.symbol, ETH.symbol}


def test_public_bar_digest_exactly_matches_consumed_checkpoint_and_rejects_invalid_metadata():
    *_, execution, adapter = setup()
    execution.manual_resume("start")
    evaluate(adapter)
    state = adapter.snapshot().checkpoint
    assert margin_bar_digest(candle(), {}, RULES, "b" * 64) == state.last_bar_digest
    assert margin_bar_digest(candle(), {}, RULES, "c" * 64) != state.last_bar_digest
    for features, reference in (({"x": float("nan")}, None), ({1: 1}, None), ({}, True)):
        with pytest.raises((ValidationError, ValueError)):
            margin_bar_digest(candle(), features, RULES, reference)


@pytest.mark.parametrize("authority", [False, None, 1, "yes"])
def test_external_entry_authority_requires_native_true_and_records_unsent_disposition(authority):
    *_, execution, adapter = setup()
    execution.manual_resume("start")
    actions = []
    adapter.entry_authorizer = lambda action: actions.append(action) or authority
    decision = evaluate(adapter)
    assert not decision.orders and decision.reason == "blocked: external_input_unavailable"
    assert not execution.snapshot().paused and not execution.snapshot().pending
    state = adapter.snapshot().checkpoint
    assert len(actions) == len(state.rejected_intents) == 1
    assert state.rejected_intents[0].action is actions[0]
    assert MarginStrategyState.parse(state.document()) == state
    assert evaluate(adapter) == decision and len(actions) == 1


def test_staged_ack_loss_rechecks_current_authority_before_first_financial_dispatch():
    *_, store, execution, adapter = setup()
    execution.manual_resume("start")
    store.ack_at = 1
    allowed = [True]
    calls = []
    adapter.entry_authorizer = lambda action: calls.append(action) or allowed[0]
    with pytest.raises(StorageError, match="ACK lost"):
        evaluate(adapter)
    assert calls == [] and not execution.snapshot().pending
    assert store.load("s1").pending is not None
    allowed[0] = False
    adapter.reconcile()
    assert len(calls) == 1 and not execution.snapshot().pending
    assert adapter.snapshot().checkpoint.rejected_intents[0].reason == "external_input_unavailable"


def test_external_authority_exception_is_unsent_hold_without_financial_pause_or_submission():
    *_, execution, adapter = setup()
    execution.manual_resume("start")
    adapter.entry_authorizer = lambda _: (_ for _ in ()).throw(StorageError("owner unavailable"))
    assert not evaluate(adapter).orders
    assert not execution.snapshot().pending and not execution.snapshot().paused
    assert adapter.snapshot().checkpoint.rejected_intents[0].reason == "external_input_unavailable"


def test_current_entry_lease_stays_held_until_financial_dispatch_ack(monkeypatch):
    *_, execution, adapter = setup()
    execution.manual_resume("start")
    lock, attempted, invalidated, steps = RLock(), Event(), Event(), []

    @contextmanager
    def lease(action):
        with lock:
            steps.append("enter")
            yield True
            steps.append("leave")

    def invalidate():
        attempted.set()
        with lock:
            steps.append("invalidate")
            invalidated.set()

    thread = Thread(target=invalidate)
    original = execution.submit_intent

    def submit(*args, **kwargs):
        steps.append("submit")
        thread.start()
        assert attempted.wait(2)
        assert not invalidated.is_set()
        result = original(*args, **kwargs)
        steps.append("financial_ack")
        assert not invalidated.is_set()
        return result

    adapter.entry_guard = lease
    monkeypatch.setattr(execution, "submit_intent", submit)
    evaluate(adapter)
    thread.join(2)
    assert not thread.is_alive() and invalidated.is_set()
    assert steps == ["enter", "submit", "financial_ack", "leave", "invalidate"]
    assert len(execution.snapshot().pending) == 1


@pytest.mark.parametrize("failure", ["false", "enter_error", "not_context_manager"])
def test_failed_entry_lease_never_reaches_financial_submission(failure):
    *_, execution, adapter = setup()
    execution.manual_resume("start")

    @contextmanager
    def lease(action):
        if failure == "enter_error":
            raise StorageError("cache authority retired")
        yield False

    adapter.entry_guard = (lambda action: True) if failure == "not_context_manager" else lease
    decision = evaluate(adapter)
    assert decision.reason == "blocked: external_input_unavailable"
    assert not execution.snapshot().pending and not execution.snapshot().paused


def test_financial_dispatch_error_exits_lease_and_preserves_original_unknown(monkeypatch):
    *_, journal, store, execution, adapter = setup()
    execution.manual_resume("start")
    exited = []

    @contextmanager
    def lease(action):
        try:
            yield True
        finally:
            exited.append(True)

    adapter.entry_guard = lease
    journal.ack_loss = True
    with pytest.raises(StorageError, match="ACK loss"):
        evaluate(adapter)
    assert exited == [True]
    original = store.load("s1").pending.order
    adapter.entry_guard = lambda _: (_ for _ in ()).throw(AssertionError("old accepted order"))
    adapter.reconcile()
    assert execution.original_intent(original.order_id) == original
    assert len(execution.snapshot().pending) == 1


def test_lease_exit_error_after_dispatch_never_discards_or_reauthorizes_accepted_intent():
    *_, store, execution, adapter = setup()
    execution.manual_resume("start")

    @contextmanager
    def lease(action):
        yield True
        raise StorageError("lease cleanup outcome unknown")

    adapter.entry_guard = lease
    with pytest.raises(StorageError, match="lease cleanup"):
        evaluate(adapter)
    original = store.load("s1").pending.order
    assert execution.original_intent(original.order_id) == original
    adapter.entry_guard = lambda _: (_ for _ in ()).throw(AssertionError("already accepted"))
    adapter.reconcile()
    assert execution.snapshot().pending == (original,)
    assert adapter.snapshot().checkpoint.pending is None


class ResumableQuantity(TargetQuantityModule):
    calls = []

    def __init__(self, instrument, *, change_cursor=False):
        super().__init__(instrument, {"quantity": "1"})
        self.resumes, self.change_cursor = 0, change_cursor

    def resume(self):
        type(self).calls.append(True)
        self.resumes += 1
        if self.change_cursor:
            self.last_open_time_ms = 0

    def checkpoint(self):
        return super().checkpoint() | {"resumes": self.resumes}

    def restore(self, state):
        self.resumes = state["resumes"]
        super().restore(state)


def test_module_resume_is_durable_and_cannot_itself_enable_financial_entries():
    ResumableQuantity.calls = []
    *_, store, execution, adapter = setup(ResumableQuantity(BTC))
    adapter.resume_module()
    assert execution.snapshot().paused
    assert adapter.module.resumes == 1 and len(ResumableQuantity.calls) == 1
    assert store.load("s1").module_checkpoint == adapter.snapshot().checkpoint.module_checkpoint
    assert adapter.snapshot().checkpoint.last_open_time_ms is None


def test_module_resume_ack_loss_reconciles_candidate_without_recalling_plugin():
    ResumableQuantity.calls = []
    *_, store, execution, adapter = setup(ResumableQuantity(BTC))
    store.ack_at = 1
    with pytest.raises(StorageError, match="ACK lost"):
        adapter.resume_module()
    with pytest.raises(TradingBlocked, match="reconciled"):
        adapter.resume_module()
    adapter.reconcile()
    assert adapter.module.resumes == 1 and ResumableQuantity.calls == [True]
    assert execution.snapshot().paused


def test_module_resume_preserves_accepted_order_and_rejects_cursor_rewrite():
    *_, execution, adapter = setup(ResumableQuantity(BTC))
    execution.manual_resume("start")
    evaluate(adapter)
    original = execution.snapshot().pending
    adapter.resume_module()
    assert execution.snapshot().pending == original
    assert adapter.snapshot().checkpoint.last_open_time_ms == 0
    *_, bad_execution, bad = setup(ResumableQuantity(BTC, change_cursor=True))
    checkpoint = bad.snapshot().checkpoint
    with pytest.raises(ProtocolError, match="candle cursor"):
        bad.resume_module()
    assert bad.snapshot().checkpoint == checkpoint and bad_execution.snapshot().paused


def test_readonly_protection_preview_uses_real_context_without_consuming_any_state():
    *_, journal, store, execution, adapter = setup(NeuralSignalModule(BTC), quantity="1")
    before = store.document, len(store.saved), tuple(journal.calls), adapter.module.checkpoint()
    result = adapter.preview_protection(
        candle(price="90"), RULES, as_of_ms=60000, marks=marks(price="90"), brackets=brackets()
    )
    assert result.orders[0].reducing and result.orders[0].quantity == 1
    assert (
        store.document,
        len(store.saved),
        tuple(journal.calls),
        adapter.module.checkpoint(),
    ) == before
    assert not execution.snapshot().pending and execution.snapshot().paused
    actual = adapter.evaluate_closed(
        candle(price="90"),
        {},
        RULES,
        as_of_ms=60000,
        marks=marks(price="90"),
        brackets=brackets(),
        worst_execution_price=D(90),
        fee_rate=D(".001"),
        protection_only=True,
    )
    assert actual == result and len(execution.snapshot().pending) == 1
    assert adapter.module.snapshot()["halted"] is True


def test_preview_refuses_missing_capability_future_input_and_uncertain_strategy():
    class NoProtection(TargetQuantityModule):
        preview_protection = None
        evaluate_protection = None

    *_, adapter = setup(NoProtection(BTC, {"quantity": "1"}))
    arguments = dict(as_of_ms=60000, marks=marks(), brackets=brackets())
    with pytest.raises(ValidationError, match="no independent protection"):
        adapter.preview_protection(candle(), RULES, **arguments)
    with pytest.raises(ValidationError, match="exact closed minute"):
        adapter.preview_protection(candle(1), RULES, **arguments)
    adapter._state = "unknown"
    with pytest.raises(TradingBlocked, match="reconciled strategy"):
        adapter.preview_protection(candle(), RULES, **arguments)


def test_accepted_original_intent_survives_expired_authority_and_ack_loss_without_redispatch():
    *_, journal, store, execution, adapter = setup()
    execution.manual_resume("start")
    calls = []
    adapter.entry_authorizer = lambda action: calls.append(action) or True
    journal.ack_loss = True
    with pytest.raises(StorageError, match="ACK loss"):
        evaluate(adapter)
    original = store.load("s1").pending.order
    assert len(calls) == 1
    adapter.entry_authorizer = lambda _: (_ for _ in ()).throw(AssertionError("expired"))
    adapter.reconcile()
    assert execution.original_intent(original.order_id) == original
    assert execution.snapshot().pending == (original,)
    assert not adapter.snapshot().checkpoint.rejected_intents
    result = settle(execution)
    assert result.account.positions[0].quantity == 1
    assert len(result.account.ledger) == 1


def test_external_authority_failure_does_not_block_observed_position_reduction():
    module = TargetQuantityModule(BTC, {"quantity": "0"})
    *_, execution, adapter = setup(module, quantity="1")
    adapter.entry_authorizer = lambda _: (_ for _ in ()).throw(AssertionError("no NN"))
    decision = evaluate(adapter)
    assert decision.orders[0].reducing and execution.snapshot().paused
    assert not adapter.snapshot().checkpoint.rejected_intents
    result = settle(execution)
    assert result.account.positions[0].quantity == 0
