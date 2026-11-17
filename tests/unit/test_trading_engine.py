"""Execution boundaries and transactional state across TS observations."""

from dataclasses import replace
from decimal import Decimal

import pytest

from omega.application.trading import TradingEngine, validate_decision
from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.models import Instrument, Market, OrderKind, Side
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
)
from omega.strategies.target_quantity.module import TargetQuantityModule

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def context(**changes):
    return replace(
        TradeContext(
            INSTRUMENT,
            Decimal(100),
            60_000,
            PositionView(),
            BudgetView(Decimal(100), Decimal(100)),
            ExecutionConstraints(can_short=True),
        ),
        **changes,
    )


class CountingProposal:
    """A mutable plugin whose state must commit only after validation."""

    def __init__(self):
        self.calls = 0

    def evaluate(self, sample):
        self.calls += 1
        quantity = sample.features.get("quantity", Decimal(1))
        return TradeDecision((OrderProposal(Side.BUY, quantity),))


class IndependentProtection:
    def __init__(self, *, expanding=False):
        self.calls = 0
        self.expanding = expanding

    def evaluate(self, sample):
        raise AssertionError("normal path must not run with missing external inputs")

    def evaluate_protection(self, sample):
        assert sample.entries_allowed is False and "NN.score" not in sample.features
        self.calls += 1
        return TradeDecision(
            (OrderProposal(Side.SELL, Decimal(1), reducing=not self.expanding),),
            "independent_local_stop",
        )


def test_protection_uses_explicit_module_capability_and_normal_execution_validation():
    engine = TradingEngine(IndependentProtection())
    sample = context(position=PositionView(Decimal(1), Decimal(100), 1))
    result = engine.evaluate(sample, protection_only=True)
    assert result.orders[0].reducing and engine.module.calls == 1
    assert engine.evaluate(sample, protection_only=True) is result
    assert engine.module.calls == 1
    blocked = engine.evaluate(replace(sample, state_certain=False), protection_only=True)
    assert not blocked.orders and engine.module.calls == 2


def test_protection_cannot_expand_or_silently_fall_back_to_normal_evaluation():
    sample = context(position=PositionView(Decimal(1), Decimal(100), 1))
    original = IndependentProtection(expanding=True)
    engine = TradingEngine(original)
    with pytest.raises(ValidationError, match="only cancel or reduce"):
        engine.evaluate(sample, protection_only=True)
    assert engine.module is original and original.calls == 0
    with pytest.raises(ValidationError, match="no independent protection"):
        TradingEngine(CountingProposal()).evaluate(sample, protection_only=True)
    with pytest.raises(ValidationError, match="explicit protection"):
        engine.evaluate(sample, protection_only=1)


class PreviewOnly(CountingProposal):
    def preview_protection(self, sample):
        self.calls += 1
        assert not sample.entries_allowed
        return TradeDecision((OrderProposal(Side.SELL, Decimal(1), reducing=True),), "preview")


def test_preview_only_capability_never_changes_engine_cache_or_claims_durable_cursor():
    original = PreviewOnly()
    engine = TradingEngine(original)
    sample = context(position=PositionView(Decimal(1), Decimal(100), 1))
    assert engine.peek_protection(sample).orders[0].reducing
    assert engine.module is original and original.calls == 0
    assert engine._last_key is None and engine._last_decision is None
    with pytest.raises(ValidationError, match="no independent protection"):
        engine.evaluate(sample, protection_only=True)
    assert not engine.peek_protection(replace(sample, state_certain=False)).orders
    assert original.calls == 0


def test_preview_rejects_expansion_before_it_can_become_a_wait_permission():
    original = IndependentProtection(expanding=True)
    sample = context(position=PositionView(Decimal(1), Decimal(100), 1))
    with pytest.raises(ValidationError, match="only cancel or reduce"):
        TradingEngine(original).peek_protection(sample)
    assert original.calls == 0


def test_instrument_change_at_same_time_cannot_hit_previous_decision_cache():
    engine = TradingEngine(TargetQuantityModule(INSTRUMENT, {"quantity": "1"}))
    first = context()
    engine.evaluate(first)
    other = Instrument("binance", Market.USDM, "ETHUSDT")
    with pytest.raises(ValidationError, match="instrument mismatch"):
        engine.evaluate(replace(first, instrument=other))
    assert engine.evaluate(first).orders[0].quantity == 1


def test_duplicate_is_cached_but_fill_revision_and_budget_release_are_reconsidered():
    engine = TradingEngine(TargetQuantityModule(INSTRUMENT, {"quantity": "-2"}))
    sample = context(position=PositionView(Decimal(1), Decimal(100), 1))
    closing = engine.evaluate(sample)
    assert closing.orders[0].reducing
    assert closing.orders[0].quantity == 1
    assert engine.evaluate(replace(sample)) is closing

    # Fill confirmation can arrive at the same market timestamp.
    flat = replace(sample, position=PositionView(revision=2))
    assert not engine.evaluate(flat).orders  # Two units exceed the current budget.
    funded = replace(flat, budget=BudgetView(Decimal(200), Decimal(200)))
    opening = engine.evaluate(funded).orders[0]
    assert opening.side == Side.SELL
    assert opening.quantity == 2
    assert not opening.reducing


def test_invalid_proposal_rolls_back_state_and_does_not_consume_observation():
    original = CountingProposal()
    engine = TradingEngine(original)
    first = context()
    accepted = engine.evaluate(first)
    committed = engine.module
    bad = replace(first, observed_at_ms=120_000, features={"quantity": Decimal("1.0001")})
    with pytest.raises(ValidationError, match="step or minimum"):
        engine.evaluate(bad)
    assert engine.module is committed
    assert engine.module.calls == 1
    assert original.calls == 0
    assert engine.evaluate(first) is accepted
    repaired = engine.evaluate(replace(bad, features={"quantity": Decimal("0.5")}))
    assert repaired.orders[0].quantity == Decimal("0.5")
    assert engine.module.calls == 2


@pytest.mark.parametrize(
    "gate", [{"state_certain": False}, {"control_owned": False}, {"has_pending_order": True}]
)
def test_normal_execution_block_commits_analysis_once_and_rechecks_gate_change(gate):
    engine = TradingEngine(CountingProposal())
    blocked = context(**gate)
    result = engine.evaluate(blocked)
    assert not result.orders
    assert engine.module.calls == 1
    assert engine.evaluate(blocked) is result
    assert engine.module.calls == 1
    assert engine.evaluate(context()).orders
    assert engine.module.calls == 2


@pytest.mark.parametrize("quantity,target", [("1", "0.4"), ("-1", "-0.4")])
def test_paused_entries_allow_partial_reduction_without_available_capital(quantity, target):
    engine = TradingEngine(TargetQuantityModule(INSTRUMENT, {"quantity": target}))
    result = engine.evaluate(
        context(
            entries_allowed=False,
            position=PositionView(Decimal(quantity), Decimal(100), 1),
            budget=BudgetView(Decimal(0), Decimal(0)),
        )
    )
    proposal = result.orders[0]
    assert proposal.reducing
    assert proposal.quantity == Decimal("0.6")
    assert proposal.side == (Side.SELL if Decimal(quantity) > 0 else Side.BUY)


@pytest.mark.parametrize("quantity", ["0.999", "1.001"])
def test_target_quantity_floors_without_rounding_up_budget_or_position(quantity):
    engine = TradingEngine(TargetQuantityModule(INSTRUMENT, {"quantity": quantity}))
    result = engine.evaluate(
        context(constraints=ExecutionConstraints(quantity_step=Decimal("0.1")))
    )
    assert result.orders[0].quantity == (Decimal("0.9") if quantity == "0.999" else Decimal(1))


@pytest.mark.parametrize(
    "proposal,rules,error",
    [
        (
            OrderProposal(Side.BUY, Decimal("0.09")),
            ExecutionConstraints(min_quantity=Decimal("0.1")),
            "minimum",
        ),
        (
            OrderProposal(Side.BUY, Decimal("0.15")),
            ExecutionConstraints(quantity_step=Decimal("0.1")),
            "step",
        ),
        (
            OrderProposal(Side.BUY, Decimal(1), OrderKind.LIMIT, Decimal("99.99")),
            ExecutionConstraints(price_tick=Decimal("0.1")),
            "tick",
        ),
    ],
)
def test_invalid_exchange_precision_is_an_error_not_a_silent_hold(proposal, rules, error):
    with pytest.raises(ValidationError, match=error):
        validate_decision(context(constraints=rules), TradeDecision((proposal,)))


def test_limit_order_budget_uses_limit_price_and_entry_price_margin_commitment():
    sample = context(
        price=Decimal(120),
        position=PositionView(Decimal(2), Decimal(80), 1),
        budget=BudgetView(Decimal(130), Decimal(50)),
        constraints=ExecutionConstraints(leverage=Decimal(2)),
    )
    at_boundary = OrderProposal(Side.BUY, Decimal(1), OrderKind.LIMIT, Decimal(100))
    validate_decision(sample, TradeDecision((at_boundary,)))
    with pytest.raises(TradingBlocked, match="budget"):
        validate_decision(sample, TradeDecision((replace(at_boundary, price=Decimal("100.02")),)))
    with pytest.raises(TradingBlocked, match="budget"):
        validate_decision(
            replace(sample, budget=BudgetView(Decimal("129.99"), Decimal(50))),
            TradeDecision((at_boundary,)),
        )


@pytest.mark.parametrize("reducing", [False, True])
def test_minimum_notional_boundary_applies_to_entry_and_dust_reduction(reducing):
    sample = context(
        constraints=ExecutionConstraints(min_notional=Decimal(10)),
        position=PositionView(Decimal(1), Decimal(100), 1) if reducing else PositionView(),
    )
    proposal = OrderProposal(Side.SELL if reducing else Side.BUY, Decimal("0.1"), reducing=reducing)
    validate_decision(sample, TradeDecision((proposal,)))
    with pytest.raises(TradingBlocked, match="notional"):
        validate_decision(sample, TradeDecision((replace(proposal, quantity=Decimal("0.099")),)))


def test_spot_short_denied_even_when_capability_claims_short_support():
    spot = Instrument("binance", Market.SPOT, "BTCUSDT")
    engine = TradingEngine(TargetQuantityModule(spot, {"quantity": "-1"}))
    assert not engine.evaluate(context(instrument=spot)).orders


def test_multi_order_batch_cannot_spend_available_budget_twice():
    sample = context()
    buy = OrderProposal(Side.BUY, Decimal("0.75"))
    # Each leg fits alone; accepting both would overcommit the same capital.
    validate_decision(sample, TradeDecision((buy,)))
    with pytest.raises(TradingBlocked, match="serial"):
        validate_decision(sample, TradeDecision((buy, buy)))


def test_late_observation_cannot_mutate_committed_plugin_state():
    engine = TradingEngine(CountingProposal())
    latest = context(observed_at_ms=120_000)
    decision = engine.evaluate(latest)
    with pytest.raises(ValidationError, match="late observation"):
        engine.evaluate(context())
    assert engine.module.calls == 1
    assert engine.evaluate(latest) is decision


def test_reversal_while_paused_closes_but_cannot_open_after_flat_confirmation():
    engine = TradingEngine(TargetQuantityModule(INSTRUMENT, {"quantity": "-0.5"}))
    held = context(position=PositionView(Decimal("0.3"), Decimal(100), 1), entries_allowed=False)
    proposal = engine.evaluate(held).orders[0]
    assert proposal.reducing
    assert proposal.quantity == Decimal("0.3")
    assert not engine.evaluate(replace(held, position=PositionView(revision=2))).orders


@pytest.mark.parametrize(
    "side,quantity,reducing",
    [(Side.SELL, "2", True), (Side.BUY, "1", True), (Side.SELL, "1", False)],
)
def test_reducing_flag_cannot_disguise_increase_overclose_or_reversal(side, quantity, reducing):
    sample = context(position=PositionView(Decimal(1), Decimal(100), 1))
    with pytest.raises(TradingBlocked):
        validate_decision(
            sample, TradeDecision((OrderProposal(side, Decimal(quantity), reducing=reducing),))
        )
