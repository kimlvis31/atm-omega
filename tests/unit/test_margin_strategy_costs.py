"""Explicit cost stages keep original policy, issue and dispatch authority."""

from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal as D

import pytest
from test_margin_strategy import (
    BTC,
    AccountStore,
    Journal,
    StrategyStore,
    evaluate,
    setup,
)

from omega.application.margin_account import MarginAccountOwner
from omega.application.margin_cost import MarginCostPolicy
from omega.application.margin_strategy import MarginStrategyAdapter, MarginStrategyState
from omega.application.margin_strategy_costs import StagedCostMarginDecision
from omega.application.virtual_margin import VirtualMarginEngine
from omega.domain.errors import StorageError, ValidationError
from omega.strategies.target_quantity.module import TargetQuantityModule


def policy(**changes):
    values = dict(
        slippage_rate=D(".001"),
        execution_price_bound_rate=D(".02"),
        mark_drift_bound_rate=D(".02"),
        entry_delay_ms=1,
        reduction_delay_ms=0,
    )
    values.update(changes)
    return MarginCostPolicy(**values)


def build(*, selected=None, entry_guard=None):
    initial, *_ = setup()
    account_store, journal, strategy_store = AccountStore(), Journal(), StrategyStore()
    owner = MarginAccountOwner(initial, account_store, create_if_missing=True)
    execution = VirtualMarginEngine(owner, journal, create_if_missing=True)
    adapter = MarginStrategyAdapter(
        "s1",
        BTC,
        TargetQuantityModule(BTC, {"quantity": "1"}),
        execution,
        strategy_store,
        owner_id="s1",
        create_if_missing=True,
        cost_policy=selected or policy(),
        entry_guard=entry_guard,
    )
    execution.manual_resume("start")
    return execution, strategy_store, journal, adapter


@pytest.mark.parametrize(
    "delay,eligible", [(0, 60000), (1, 120000), (59999, 120000), (60000, 120000), (60001, 180000)]
)
def test_costed_stage_strict_roundtrip_keeps_one_time_delayed_eligibility(delay, eligible):
    execution, store, _, adapter = build(selected=policy(entry_delay_ms=delay))
    store.ack_at = 1
    with pytest.raises(StorageError):
        evaluate(adapter)
    state = store.load("s1")
    assert type(state.pending) is StagedCostMarginDecision
    assert state.pending.as_of_ms == state.pending.order.original.as_of_ms == 60000
    assert state.pending.order.eligible_at_ms == eligible
    assert state.pending.cost_policy_sha256 == policy(entry_delay_ms=delay).sha256
    assert MarginStrategyState.parse(state.document()) == state
    original = state.pending
    adapter.reconcile()
    assert execution.snapshot().pending == (original.order,)
    assert store.load("s1").last_as_of_ms == 60000


def test_accepted_cost_intent_ack_loss_does_not_reacquire_expired_entry_guard():
    entered = []

    @contextmanager
    def guard(action):
        entered.append(action)
        yield True

    execution, store, journal, adapter = build(entry_guard=guard)
    journal.ack_loss = True
    with pytest.raises(StorageError):
        evaluate(adapter)
    stage = store.load("s1").pending
    assert len(entered) == 1 and type(stage) is StagedCostMarginDecision

    def forbidden(action):
        pytest.fail("accepted original cost intent reacquired a new authorization")

    adapter.entry_guard = forbidden
    adapter.reconcile()
    assert execution.snapshot().pending == (stage.order,)
    assert len(entered) == 1 and store.load("s1").pending is None


def test_policy_drift_is_rejected_at_bootstrap_without_new_dispatch():
    execution, store, journal, adapter = build()
    evaluate(adapter)
    before = tuple(journal.rows)
    changed = MarginStrategyAdapter(
        "s1",
        BTC,
        TargetQuantityModule(BTC, {"quantity": "1"}),
        execution,
        store,
        owner_id="s1",
        cost_policy=policy(entry_delay_ms=2),
    )
    assert changed.snapshot().state == "conflict"
    assert tuple(journal.rows) == before


def test_costed_stage_cannot_lie_about_policy_or_original_eligibility():
    _, store, _, adapter = build()
    store.ack_at = 1
    with pytest.raises(StorageError):
        evaluate(adapter)
    stage = store.load("s1").pending
    with pytest.raises(ValidationError):
        replace(stage, cost_policy_sha256="a" * 64)
    document = store.load("s1").document()
    fields = document["state"]["fields"]["pending"]["fields"]
    fields["order"]["fields"]["eligible_at_ms"] = 60000
    with pytest.raises(ValidationError):
        MarginStrategyState.parse(document)


def test_pause_disposes_only_unsubmitted_cost_entry_with_exact_original_action():
    execution, store, _, adapter = build()
    store.ack_at = 1
    with pytest.raises(StorageError):
        evaluate(adapter)
    stage = store.load("s1").pending
    execution.pause_entries("paused")
    adapter.reconcile(reject_unsubmitted_entries=True)
    state = store.load("s1")
    assert not execution.snapshot().pending
    assert state.rejected_intents[0].action == stage
    assert state.rejected_intents[0].reason == "paused_before_intent"
    assert MarginStrategyState.parse(state.document()) == state


def test_cost_constructor_requires_parsed_policy_not_an_untrusted_mapping():
    execution, store, _, _ = build()
    with pytest.raises(ValidationError, match="parsed immutable"):
        MarginStrategyAdapter(
            "s1",
            BTC,
            TargetQuantityModule(BTC, {"quantity": "1"}),
            execution,
            store,
            owner_id="s1",
            cost_policy=policy().document(),
        )
