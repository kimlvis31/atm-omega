"""Cost policy must survive actual common admission/quote/fee preparation."""

from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.application.margin_cost import (
    CostPendingFillAdmission,
    MarginCostPolicy,
    PreparedCostMarginBatch,
    approve_cost_order,
    freeze_cost_pending,
)
from omega.application.margin_execution import FrozenPendingOrder, prepare_pending_batch
from omega.application.margin_risk import approve_order
from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.margin import (
    MarginAccount,
    MarginBracket,
    MarginBracketSnapshot,
    MarginMode,
    MarginPosition,
    MarkSnapshot,
)
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side
from omega.domain.trading import OrderProposal

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")


def policy(**changes):
    return MarginCostPolicy(
        **{
            "slippage_rate": D(".01"),
            "execution_price_bound_rate": D(".1"),
            "mark_drift_bound_rate": D(".1"),
            "entry_delay_ms": 0,
            "reduction_delay_ms": 0,
            **changes,
        }
    )


def account(*, mode=MarginMode.CROSS, cash="1000", quantity="0", instruments=(BTC,)):
    return MarginAccount(
        "cost",
        "USDT",
        D(cash),
        tuple(
            MarginPosition(
                item,
                item.symbol,
                mode,
                10,
                D(100),
                D(quantity) if item == BTC else D(0),
                D(100) if item == BTC and D(quantity) else D(0),
                D(100) if mode == MarginMode.ISOLATED else D(0),
            )
            for item in instruments
        ),
    )


def tables(instruments=(BTC,)):
    return tuple(
        MarginBracketSnapshot(
            item,
            (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
            0,
            0,
            "explicit_cost_scenario",
            "a" * 64,
        )
        for item in instruments
    )


def marks(instruments=(BTC,), price="100"):
    return tuple(
        MarkSnapshot(item, D(price), 0, 0, "actual_test_observation") for item in instruments
    )


def bar(*, opening=60000, price="100", instrument=BTC):
    value = D(price)
    return Candle(instrument, opening, opening + 60000, value, value + 1, value - 1, value, D(10))


def order(
    state,
    *,
    side=Side.BUY,
    limit=None,
    instrument=BTC,
    identity="order",
    cost=None,
    reservations=(),
    reducing=False,
    issue=60000,
    refs=None,
    price="100",
):
    proposal = OrderProposal(
        side,
        D(1),
        OrderKind.LIMIT if limit else OrderKind.MARKET,
        D(limit) if limit else None,
        reducing,
    )
    original = approve_cost_order(
        state,
        identity,
        proposal,
        policy=cost or policy(),
        instrument=instrument,
        owner_id=instrument.symbol,
        as_of_ms=issue,
        marks=marks(tuple(p.instrument for p in state.positions), price) if refs is None else refs,
        brackets=tables(tuple(p.instrument for p in state.positions)),
        worst_execution_price=D(price),
        fee_rate=D(".001"),
        reservations=reservations,
    )
    return freeze_cost_pending(original)


@pytest.mark.parametrize("mode", [MarginMode.CROSS, MarginMode.ISOLATED])
@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
@pytest.mark.parametrize("limited", [False, True])
def test_final_price_fee_and_raw_observation_are_shared_not_double_debited(mode, side, limited):
    state = account(mode=mode)
    limit = ("100.5" if side == Side.BUY else "99.5") if limited else None
    pending = order(state, side=side, limit=limit)
    plan = prepare_pending_batch(state, (pending,), (bar(),), as_of_ms=120000, brackets=tables())
    assert type(plan) is PreparedCostMarginBatch
    admission, fact = plan.admissions[0], plan.facts[0]
    assert type(admission) is CostPendingFillAdmission
    price = D(limit) if limited else D(101) if side == Side.BUY else D(99)
    assert admission.quote.raw_price == D(100)
    assert admission.quote.price == fact.price == price
    assert admission.quote.limit_capped is limited
    assert fact.fee == price * D(".001")
    entry = plan.candidate_account.ledger[-1]
    assert entry.cross_delta + entry.isolated_delta == -fact.fee
    assert plan.candidate_account.positions[0].quantity == (1 if side == Side.BUY else -1)
    assert plan.candles[0].open == 100
    assert pending.original.envelope.anchor.price == 100


@pytest.mark.parametrize("side,limit", [(Side.BUY, "90"), (Side.SELL, "110")])
def test_slippage_cannot_manufacture_a_limit_touch(side, limit):
    state = account()
    pending = order(state, side=side, limit=limit)
    plan = prepare_pending_batch(
        state, (pending,), (bar(),), as_of_ms=120000, phase="close", brackets=tables()
    )
    assert not plan.facts and plan.remaining == (pending,)
    assert plan.admissions[0].outcome == "not_touched"


def test_favorable_101_to_100_requires_upfront_cost_mark_and_loss_envelope():
    state = account()
    pending = order(state, price="101")
    plan = prepare_pending_batch(state, (pending,), (bar(),), as_of_ms=120000, brackets=tables())
    assert len(plan.facts) == 1 and not plan.rejections
    assert pending.original.mark_price == D(101)
    assert pending.original.extra_entry_loss > 0


@pytest.mark.parametrize(
    "price,drift,reason",
    [
        ("120", ".1", "cost_mark_bound_exceeded"),
        ("111", ".2", "cost_execution_bound_exceeded"),
    ],
)
def test_original_entry_envelope_is_not_expanded_at_fill(price, drift, reason):
    state = account()
    pending = order(state, cost=policy(mark_drift_bound_rate=D(drift)))
    plan = prepare_pending_batch(
        state, (pending,), (bar(price=price),), as_of_ms=120000, brackets=tables()
    )
    assert not plan.facts and plan.rejections == ((pending.order_id, reason),)
    assert plan.candidate_account == state


@pytest.mark.parametrize(
    "delay,eligible", [(0, 60000), (1, 120000), (59999, 120000), (60000, 120000), (60001, 180000)]
)
def test_delay_fixes_original_eligibility_and_does_not_use_a_previous_bar(delay, eligible):
    state = account()
    pending = order(state, cost=policy(entry_delay_ms=delay))
    assert pending.eligible_at_ms == eligible
    early = prepare_pending_batch(state, (pending,), (bar(),), as_of_ms=120000, brackets=tables())
    assert bool(early.facts) is (eligible == 60000)
    if eligible > 60000:
        assert early.remaining == (pending,)
        later = prepare_pending_batch(
            state,
            (pending,),
            (bar(opening=eligible),),
            as_of_ms=eligible + 60000,
            brackets=tables(),
        )
        assert len(later.facts) == 1 and later.facts[0].event_time_ms == eligible
    with pytest.raises(ValidationError):
        replace(pending, eligible_at_ms=eligible + 60000)


@pytest.mark.parametrize("mode", [MarginMode.CROSS, MarginMode.ISOLATED])
def test_reduction_without_original_mark_keeps_policy_and_allows_outside_entry_band(mode):
    state = account(mode=mode, quantity="1")
    pending = order(state, side=Side.SELL, reducing=True, refs=())
    assert pending.original.envelope.anchor is None
    assert pending.original.envelope.execution_upper is None
    assert pending.original.extra_initial_margin == pending.original.extra_entry_loss == 0
    plan = prepare_pending_batch(
        state, (pending,), (bar(price="20"),), as_of_ms=120000, brackets=()
    )
    assert len(plan.facts) == 1 and plan.facts[0].price == D("19.8")
    assert plan.candidate_account.positions[0].quantity == 0


def test_shared_wallet_counts_cost_reserve_deltas_for_all_pending_claims():
    state = account(cash="45", instruments=(BTC, ETH))
    first = order(state, identity="btc")
    assert first.original.total_reserved > D(30)
    common = dict(
        instrument=ETH,
        owner_id=ETH.symbol,
        as_of_ms=60000,
        marks=marks((BTC, ETH)),
        brackets=tables((BTC, ETH)),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
        reservations=(first.original,),
    )
    legacy = approve_order(state, "legacy-eth", OrderProposal(Side.BUY, D(1)), **common)
    assert legacy.total_reserved == D("10.1")
    with pytest.raises(TradingBlocked, match="collateral"):
        order(state, identity="cost-eth", instrument=ETH, reservations=(first.original,))


def test_mixed_batch_keeps_legacy_fill_identity_and_cost_order_model():
    state = account(instruments=(BTC, ETH))
    original = approve_order(
        state,
        "legacy",
        OrderProposal(Side.BUY, D(1)),
        instrument=BTC,
        owner_id=BTC.symbol,
        as_of_ms=60000,
        marks=marks((BTC, ETH)),
        brackets=tables((BTC, ETH)),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
    )
    legacy = FrozenPendingOrder(original, 60000)
    costed = order(state, identity="cost", instrument=ETH, reservations=(original,))
    baseline = prepare_pending_batch(
        state, (legacy,), (bar(), bar(instrument=ETH)), as_of_ms=120000, brackets=tables((BTC, ETH))
    )
    mixed = prepare_pending_batch(
        state,
        (legacy, costed),
        (bar(), bar(instrument=ETH)),
        as_of_ms=120000,
        brackets=tables((BTC, ETH)),
    )
    assert type(mixed) is PreparedCostMarginBatch and len(mixed.facts) == 2
    assert mixed.facts[0].fill_id == baseline.facts[0].fill_id
    assert [fact.price for fact in mixed.facts] == [D(100), D(101)]


def test_legacy_wrapper_cannot_hide_a_delayed_cost_order():
    state = account()
    costed = order(state, cost=policy(entry_delay_ms=60001))
    forged = FrozenPendingOrder(costed.original, 60000)
    with pytest.raises(ValidationError):
        prepare_pending_batch(state, (forged,), (bar(),), as_of_ms=120000, brackets=tables())


@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_initial_cost_candidate_uses_effective_valuation_for_leverage_tier(side):
    state = account()
    bracket = (
        MarginBracketSnapshot(
            BTC,
            (
                MarginBracket(D(0), D(105), D(".01"), D(0), 20),
                MarginBracket(D(105), D(1000000), D(".02"), D("1.05"), 5),
            ),
            0,
            0,
            "explicit_tier_scenario",
            "a" * 64,
        ),
    )
    references = dict(
        instrument=BTC,
        owner_id=BTC.symbol,
        as_of_ms=60000,
        marks=marks(),
        brackets=bracket,
        worst_execution_price=D(100),
        fee_rate=D(".001"),
    )
    legacy = approve_order(state, "legacy", OrderProposal(side, D(1)), **references)
    assert legacy.valuation_price == 100 and legacy.leverage == 10
    with pytest.raises(TradingBlocked, match="leverage"):
        approve_cost_order(
            state, "cost-tier", OrderProposal(side, D(1)), policy=policy(), **references
        )
