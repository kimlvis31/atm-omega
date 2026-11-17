"""Durable original protection actions share the real virtual financial owner."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D

import pytest
from test_margin_session import bars, ledger, new, tables
from test_margin_strategy import BTC, RULES, brackets, marks, restore, settle, setup
from test_margin_trading import Session
from test_protection_observation import observation

from omega.application.margin_execution import FrozenPendingOrder
from omega.application.margin_risk import approve_order
from omega.application.margin_strategy import MarginStrategyAdapter, MarginStrategyState
from omega.application.virtual_margin import CancelIntent
from omega.domain.errors import ProtocolError, StorageError
from omega.domain.margin import MarkSnapshot
from omega.domain.models import Side
from omega.domain.trading import OrderProposal
from omega.runtime.margin_trading import MarginTradingService
from omega.strategies.tef.module import TefModule


def protect(adapter, sample, **changes):
    values = dict(
        as_of_ms=sample.received_at_ms,
        marks=tuple(
            item for item in marks(sample.received_at_ms) if item.instrument != sample.instrument
        )
        + (
            MarkSnapshot(
                sample.instrument,
                sample.last,
                sample.event_time_ms,
                sample.received_at_ms,
                sample.source,
            ),
        ),
        brackets=brackets(),
        fee_rate=D(".001"),
    )
    values.update(changes)
    return adapter.evaluate_observation(sample, RULES, **values)


def test_paused_partial_touch_freezes_original_id_and_never_fills_at_stop_or_prior_open():
    *_, store, execution, adapter = setup(
        TefModule(BTC, {"fullStopLossImmediate": ".05"}), quantity="1"
    )
    assert execution.snapshot().paused
    legacy = json.loads(store.document)
    assert legacy["schema_version"] == 2 and "last_protection" not in legacy["state"]["fields"]
    assert MarginStrategyState.parse(legacy).document() == legacy
    original = adapter.snapshot().checkpoint
    sample = observation()
    decision = protect(adapter, sample)
    assert decision.orders[0].reducing and decision.orders[0].price is None
    state = adapter.snapshot().checkpoint
    assert state.document()["schema_version"] == 3
    for key in (
        "last_open_time_ms",
        "last_cycle",
        "last_as_of_ms",
        "last_input_digest",
        "last_bar_digest",
    ):
        assert getattr(state, key) == getattr(original, key)
    pending = execution.snapshot().pending
    assert len(pending) == 1 and pending[0].eligible_at_ms == 60000
    before = store.document
    assert protect(adapter, sample) == decision and store.document == before
    later = replace(sample, sequence=2, event_time_ms=10001, received_at_ms=10002)
    assert protect(adapter, later) == decision and store.document == before
    assert adapter.snapshot().checkpoint.last_protection.observation == sample
    with pytest.raises(ProtocolError, match="changed payload"):
        protect(adapter, replace(sample, low=D(93)))
    assert not settle(execution, 0).account.ledger
    result = settle(execution, 1)
    assert result.account.positions[0].quantity == 0
    assert result.account.ledger[0].fact.price == 100
    assert result.account.ledger[0].fact.event_time_ms >= pending[0].eligible_at_ms
    assert result.paused


@pytest.mark.parametrize("boundary", ["checkpoint", "intent"])
def test_ack_loss_restart_replays_exact_protection_candidate_without_source_reselection(boundary):
    module = TefModule(BTC, {"fullStopLossImmediate": ".05"})
    initial, account_store, journal, store, execution, adapter = setup(module, quantity="1")
    if boundary == "checkpoint":
        store.ack_at = 1
    else:
        journal.ack_loss = True
    sample = observation()
    with pytest.raises(StorageError):
        protect(adapter, sample)
    original = store.load("s1").pending
    assert original.observation == sample and original.order.original.proposal.reducing
    restarted, recovered = restore(initial, account_store, journal, store, module)
    recovered.reconcile()
    assert restarted.snapshot().pending == (original.order,)
    assert recovered.snapshot().checkpoint.last_protection == original
    assert recovered.snapshot().checkpoint.last_open_time_ms is None
    assert protect(recovered, sample) == original.decision
    assert len(restarted.snapshot().pending) == 1


def test_actual_session_signal_graph_cursor_and_restart_are_independent_of_partial_stop(
    tmp_path, monkeypatch
):
    issued = []
    evaluate_closed = MarginStrategyAdapter.evaluate_closed

    def observe_closed(adapter, *args, **kwargs):
        result = evaluate_closed(adapter, *args, **kwargs)
        issued.append((adapter.snapshot().checkpoint.strategy_id, kwargs["as_of_ms"], result))
        return result

    monkeypatch.setattr(MarginStrategyAdapter, "evaluate_closed", observe_closed)
    session = new(tmp_path, tef=True)
    try:
        session.resume("start")
        for index in range(20):
            price = str(100 + index * index)
            opening = str(100 + max(0, index - 1) ** 2)
            # Preserve the original warmup entry bound; after an actual owned fill,
            # processing delay moves newly generated TS orders beyond this open.
            held_before = session.execution.snapshot().account.positions[0].quantity > 0
            session.run_once(
                bars(index, price, opening=opening),
                as_of_ms=(index + 1) * 60000 + int(held_before),
                brackets=tables(),
            )
            financial = session.execution.snapshot()
            if (
                financial.account.positions[0].quantity > 0
                and any(row.original.instrument == BTC for row in financial.pending)
                and all(row.eligible_at_ms > (index + 1) * 60000 for row in financial.pending)
            ):
                break
        assert session.execution.snapshot().account.positions[0].quantity > 0
        original_ts_orders = tuple(
            row for row in session.execution.snapshot().pending if row.original.instrument == BTC
        )
        assert len(original_ts_orders) == 1
        original_ts_order = original_ts_orders[0]
        registered = session.execution.original_intent(original_ts_order.order_id)
        assert registered == original_ts_order
        assert registered.original.instrument == BTC and registered.original.owner_id == "btc-ts"
        original_outputs = tuple(
            decision
            for strategy_id, actual_as_of_ms, decision in issued
            if strategy_id == registered.original.owner_id
            and actual_as_of_ms == registered.original.as_of_ms
            and decision.orders == (registered.original.proposal,)
        )
        assert len(original_outputs) == 1
        decision = original_outputs[0]
        assert decision.orders == (registered.original.proposal,)
        assert (
            registered.original.proposal.side,
            registered.original.proposal.quantity,
            registered.original.proposal.reducing,
        ) == (
            decision.orders[0].side,
            decision.orders[0].quantity,
            decision.orders[0].reducing,
        )
        assert registered.eligible_at_ms > (index + 1) * 60000
        cancel_calls = []
        original_cancel = session.execution.cancel_order

        def cancel(order_id, request_id):
            cancel_calls.append((order_id, request_id))
            return original_cancel(order_id, request_id)

        monkeypatch.setattr(session.execution, "cancel_order", cancel)
        session.pause("hold-new-risk")
        before = deepcopy(session._meta)
        graphs = {key: deepcopy(graph.checkpoint()) for key, graph in session._graphs.items()}
        signal_cursor = session._adapters["btc-ts"].module.last_open_time_ms
        monkeypatch.setattr(
            session._adapters["btc-ts"].module.model,
            "_calculate",
            lambda *_: pytest.fail("partial must not calculate"),
        )
        start = (index + 1) * 60000
        sample = observation(
            open_time_ms=start,
            close_time_ms=start + 60000,
            event_time_ms=start + 100,
            received_at_ms=start + 101,
            last=D(price),
            high=D(price),
            low=D(1),
        )
        count = len(ledger(session))
        session.observe_protection((sample,), as_of_ms=start + 102, brackets=tables())
        assert session._meta == before and len(ledger(session)) == count
        assert {key: graph.checkpoint() for key, graph in session._graphs.items()} == graphs
        assert session._adapters["btc-ts"].module.last_open_time_ms == signal_cursor
        first = session._adapters["btc-ts"].snapshot().checkpoint.last_protection
        assert first.decision.cancel_pending and not first.decision.orders and first.order is None
        assert (
            len(first.cancellations) == 1
            and first.cancellations[0][0] == original_ts_order.order_id
        )
        assert session.execution.original_intent(original_ts_order.order_id) == original_ts_order
        assert tuple(cancel_calls) == first.cancellations
        assert not tuple(
            row for row in session.execution.snapshot().pending if row.original.instrument == BTC
        )
        # A new actual receipt, not replay of the cancellation, authorizes the reduction.
        next_sample = replace(
            sample,
            sequence=sample.sequence + 1,
            event_time_ms=start + 103,
            received_at_ms=start + 104,
        )
        session.observe_protection((next_sample,), as_of_ms=start + 105, brackets=tables())
        assert session._meta == before and len(ledger(session)) == count
        assert {key: graph.checkpoint() for key, graph in session._graphs.items()} == graphs
        assert session._adapters["btc-ts"].module.last_open_time_ms == signal_cursor
        pending = tuple(
            row for row in session.execution.snapshot().pending if row.original.instrument == BTC
        )
        assert len(pending) == 1 and pending[0].original.proposal.reducing
        assert pending[0].eligible_at_ms == start + 60000
        original_reduction = session._adapters["btc-ts"].snapshot().checkpoint.last_protection
        assert original_reduction.observation == next_sample
        assert original_reduction.order == pending[0]
        assert tuple(cancel_calls) == first.cancellations
    finally:
        session.close()
    restored = new(tmp_path, create=False, tef=True)
    try:
        restored.observe_protection((), as_of_ms=start + 106, brackets=tables())
        assert (
            tuple(
                row
                for row in restored.execution.snapshot().pending
                if row.original.instrument == BTC
            )
            == pending
        )
        assert restored.snapshot()["paused"]
        assert restored._adapters["btc-ts"].module.last_open_time_ms == signal_cursor
    finally:
        restored.close()


def test_runtime_reads_partial_before_closed_minute_is_due_and_replays_before_source(tmp_path):
    events = []
    sample = observation()

    class ObservingSession(Session):
        def observe_protection(self, rows, **_options):
            events.append(("replay" if not rows else "observe", rows))

    def source(instruments, *, as_of_ms):
        assert instruments == (BTC.key,) and as_of_ms == 10002
        assert events == [("replay", ())]
        events.append(("source", ()))
        return (sample,)

    runtime = MarginTradingService(
        tmp_path,
        None,
        lambda: pytest.fail("closed-minute source must not be read yet"),
        core_epoch="partial-runtime",
        clock_ms=lambda: 10002,
        protection_source=source,
    )
    session = ObservingSession(tmp_path, {"account_id": "demo"}, [], 0)
    runtime.sessions["demo"] = session
    runtime.data = {"accounts": {"demo": {"feed_enabled": True, "parameters": {"brackets": []}}}}
    try:
        runtime._tick()
        assert events == [("replay", ()), ("source", ()), ("observe", (sample,))]
        assert session.paused and not session.calls
    finally:
        runtime.sessions.clear()
        runtime.close()


def seed_original_ts_order(execution, *, side=Side.BUY, quantity=D(1), reducing=False):
    execution.manual_resume("seed-original")
    reservation = approve_order(
        execution.snapshot().account,
        "original-ts-order",
        OrderProposal(side, quantity, reducing=reducing),
        instrument=BTC,
        owner_id="s1",
        as_of_ms=0,
        marks=marks(0),
        brackets=brackets(),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
    )
    original = FrozenPendingOrder(reservation, 60000)
    execution.submit_intent(original, marks=marks(0), brackets=brackets())
    execution.pause_entries("preserve-entry-pause")
    return original


@pytest.mark.parametrize(
    "boundary", ["checkpoint", "cancel_before_primary", "cancel_after_primary"]
)
def test_partial_cancel_recovery_replays_original_before_rebound_reduction(monkeypatch, boundary):
    module = TefModule(BTC, {"fullStopLossImmediate": ".05"})
    monkeypatch.setattr(module.model, "_calculate", lambda *_: pytest.fail("no signal calculation"))
    initial, account_store, journal, store, execution, adapter = setup(module, quantity="1")
    original = seed_original_ts_order(execution)
    append = journal.append
    armed = True

    def fail_before_cancel(record):
        nonlocal armed
        if (
            boundary == "cancel_before_primary"
            and armed
            and isinstance(record.payload, CancelIntent)
        ):
            armed = False
            raise StorageError("synthetic cancellation primary unavailable")
        return append(record)

    monkeypatch.setattr(journal, "append", fail_before_cancel)
    if boundary == "checkpoint":
        store.ack_at = 1
    elif boundary == "cancel_after_primary":
        journal.ack_loss = True
    sample = observation(last=D(94))
    with pytest.raises(StorageError):
        protect(adapter, sample)
    frozen = store.load("s1").pending
    assert frozen.observation == sample and frozen.decision.cancel_pending
    assert not frozen.decision.orders and frozen.order is None
    assert len(frozen.cancellations) == 1 and frozen.cancellations[0][0] == original.order_id
    assert json.loads(frozen.module_checkpoint)["payload"]["pending_stop"] is not None
    assert not execution.snapshot().account.ledger
    restarted, recovered = restore(initial, account_store, journal, store, module)
    recovered.reconcile()
    assert not restarted.snapshot().pending and restarted.snapshot().paused
    assert recovered.snapshot().checkpoint.last_protection == frozen
    cancellations = tuple(
        row.record.payload for row in journal.rows if isinstance(row.record.payload, CancelIntent)
    )
    assert len(cancellations) == 1 and cancellations[0].order_id == original.order_id
    assert cancellations[0].original_request_fingerprint == original.original.request_fingerprint
    assert restarted.original_intent(original.order_id) == original
    writes = len(journal.calls), len(store.saved)
    assert protect(recovered, sample) == frozen.decision
    assert (len(journal.calls), len(store.saved)) == writes
    later = observation(
        sequence=2,
        open_time_ms=60000,
        close_time_ms=120000,
        event_time_ms=70000,
        received_at_ms=70001,
        last=D(100),
        high=D(100),
        low=D(100),
    )
    reduction = protect(recovered, later)
    assert len(reduction.orders) == 1 and reduction.orders[0].reducing
    assert reduction.orders[0].quantity == 1 and reduction.orders[0].side == Side.SELL
    assert reduction.orders[0].price is None and reduction.reason == "FSLIMMED"
    pending = restarted.snapshot().pending
    assert len(pending) == 1 and pending[0].eligible_at_ms == 120000
    assert recovered.snapshot().checkpoint.last_protection.order == pending[0]
    assert recovered.module.snapshot()["pending_stop"][2] == frozen.as_of_ms
    assert recovered.module.last_open_time_ms is None and not restarted.snapshot().account.ledger


def test_partial_never_retroactively_cancels_already_eligible_original_open_fill():
    *_, store, execution, adapter = setup(
        TefModule(BTC, {"fullStopLossImmediate": ".05"}), quantity="1"
    )
    original = seed_original_ts_order(execution, side=Side.SELL, quantity=D(".5"), reducing=True)
    sample = observation(
        open_time_ms=60000,
        close_time_ms=120000,
        event_time_ms=70000,
        received_at_ms=70001,
        last=D(94),
        high=D(100),
        low=D(94),
    )
    before = store.document, deepcopy(adapter.module.checkpoint()), execution.snapshot()
    result = protect(adapter, sample)
    assert result.reason == "await_eligible_pending_settlement"
    assert not result.orders and not result.cancel_pending
    assert (store.document, adapter.module.checkpoint(), execution.snapshot()) == before
    assert execution.original_intent(original.order_id) == original
    assert execution.snapshot().pending == (original,)
    settled = settle(execution, 1)
    assert len(settled.account.ledger) == 1
    fact = settled.account.ledger[0].fact
    assert fact.order_id == original.order_id and fact.event_time_ms == 60000
    assert fact.price == 100 and fact.delta_qty == D("-.5")
    assert settled.account.positions[0].quantity == D(".5")
