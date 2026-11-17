"""Reservation values use factual state without mutating it or approving venue execution."""

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.application.margin_risk import MarginReservation, approve_order
from omega.domain.errors import ProtocolError, TradingBlocked, ValidationError
from omega.domain.margin import (
    FillEvent,
    MarginAccount,
    MarginBracket,
    MarginBracketSnapshot,
    MarginMode,
    MarginPosition,
    MarkSnapshot,
    apply_fill,
)
from omega.domain.models import Instrument, Market, OrderKind, Side
from omega.domain.trading import OrderProposal

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")
NOW = 1000


def position(instrument=BTC, *, mode=MarginMode.CROSS, **changes):
    return MarginPosition(instrument, instrument.symbol, mode, 10, D(100), **changes)


def account(*positions, cash="100"):
    return MarginAccount("a", "USDT", D(cash), positions or (position(), position(ETH)))


def mark(instrument=BTC, price="100"):
    return MarkSnapshot(instrument, D(price), 900, 950, "actual_mark_fixture")


def table(instrument=BTC):
    return MarginBracketSnapshot(
        instrument,
        (
            MarginBracket(D(0), D(1000), D(".01"), D(0), 20),
            MarginBracket(D(1000), D(2000), D(".02"), D(10), 5),
        ),
        900,
        950,
        "explicit_table_fixture",
        "a" * 64,
    )


def proposal(quantity="1", *, side=Side.BUY, reducing=False, price=None):
    return OrderProposal(
        side,
        D(quantity),
        OrderKind.LIMIT if price else OrderKind.MARKET,
        D(price) if price else None,
        reducing,
    )


def approve(state=None, order_id="order", value=None, *, instrument=BTC, **changes):
    arguments = {
        "instrument": instrument,
        "owner_id": instrument.symbol,
        "as_of_ms": NOW,
        "marks": (mark(), mark(ETH)),
        "brackets": (table(), table(ETH)),
        "worst_execution_price": D(100),
        "fee_rate": D(".001"),
        **changes,
    }
    return approve_order(state or account(), order_id, value or proposal(), **arguments)


def test_new_margin_and_fee_are_reserved_without_multiplying_strategy_quantity_or_mutating_facts():
    state = account()
    reservation = approve(state, worst_execution_price=D(110))
    assert isinstance(reservation, MarginReservation)
    assert reservation.proposal.quantity == 1 and reservation.valuation_price == 110
    assert reservation.initial_margin == 11 and reservation.fee_reserve == D(".11")
    assert reservation.mark_price == 100 and reservation.adverse_fill_price == 110
    assert reservation.entry_loss == 10 and reservation.total_reserved == D("21.11")
    assert not state.ledger and state.positions[0].quantity == 0 and state.cross_wallet == 100
    with pytest.raises(FrozenInstanceError):
        reservation.initial_margin = D(0)


@pytest.mark.parametrize(
    "price,worst,limit,expected",
    [
        ("100", "120", None, "120"),
        ("130", "120", None, "130"),
        ("100", "120", "140", "140"),
    ],
)
def test_conservative_notional_uses_larger_fill_mark_or_limit_price(price, worst, limit, expected):
    result = approve(
        value=proposal(price=limit),
        marks=(mark(price=price), mark(ETH)),
        worst_execution_price=D(worst),
    )
    assert result.valuation_price == D(expected)


@pytest.mark.parametrize(
    "changes",
    [
        {"marks": ()},
        {"brackets": ()},
        {"marks": (replace(mark(), available_at_ms=1001),)},
        {"brackets": (replace(table(), available_at_ms=1001),)},
        {"max_mark_age_ms": 99},
        {"max_bracket_age_ms": 99},
    ],
)
def test_flat_position_still_needs_fresh_actual_references_for_new_notional(changes):
    with pytest.raises(TradingBlocked):
        approve(**changes)


@pytest.mark.parametrize(
    "state,quantity,worst",
    [
        (account(), "10", "100"),  # exact cap selects lower-leverage second bracket
        (account(), "20", "100"),  # final cap has no supported bracket
        (account(position(quantity=D(9), entry_price=D(100))), "1", "100"),
        (account(), "9", "120"),  # worst execution crosses the tier despite mark 100
    ],
)
def test_proposed_total_notional_and_leverage_use_exclusive_caps(state, quantity, worst):
    with pytest.raises(TradingBlocked, match="planned notional"):
        approve(state, value=proposal(quantity), worst_execution_price=D(worst))


def test_two_cross_orders_share_collateral_but_isolated_reservation_cannot_take_cross_cash():
    crossed = account(cash="20")
    pending = approve(crossed, "eth", instrument=ETH)
    with pytest.raises(TradingBlocked, match="available collateral"):
        approve(crossed, "btc", reservations=(pending,))
    separate = account(
        position(), position(ETH, mode=MarginMode.ISOLATED, isolated_wallet=D(100)), cash="20"
    )
    isolated = approve(separate, "eth", proposal("5"), instrument=ETH)
    assert isolated.initial_margin == 50
    assert approve(separate, "btc", reservations=(isolated,)).initial_margin == 10


def test_isolated_wallet_cannot_borrow_cross_or_other_isolated_funds():
    state = account(position(mode=MarginMode.ISOLATED, isolated_wallet=D(10)), cash="1000")
    with pytest.raises(TradingBlocked, match="available collateral"):
        approve(state)
    state = account(
        position(), position(ETH, mode=MarginMode.ISOLATED, isolated_wallet=D(1000)), cash="5"
    )
    with pytest.raises(TradingBlocked, match="available collateral"):
        approve(state)


def test_budget_is_current_initial_margin_plus_new_initial_margin_and_fee():
    state = account(replace(position(), budget_margin=D(10)), cash="1000")
    with pytest.raises(TradingBlocked, match="allocated margin budget"):
        approve(state)
    state = account(
        replace(position(quantity=D(5), entry_price=D(100)), budget_margin=D(60)), cash="1000"
    )
    with pytest.raises(TradingBlocked, match="allocated margin budget"):
        approve(state)


@pytest.mark.parametrize("mode", [MarginMode.CROSS, MarginMode.ISOLATED])
def test_confirmed_reduction_is_not_denied_for_debt_overbudget_or_missing_brackets(mode):
    state = account(
        replace(position(mode=mode, quantity=D(2), entry_price=D(100)), budget_margin=D(0)),
        cash="-10",
    )
    result = approve(
        state,
        value=proposal("2", side=Side.SELL, reducing=True),
        marks=(),
        brackets=(),
        worst_execution_price=D(150),
    )
    assert result.initial_margin == 0 and result.fee_reserve == D(".3")
    assert result.entry_loss == 0 and result.mark_price is None
    assert state.positions[0].quantity == 2 and state.cross_wallet == -10


def test_reduction_fee_reservation_is_kept_and_subtracted_from_next_cross_entry_capacity():
    state = account(position(quantity=D(2), entry_price=D(100)), position(ETH))
    reduction = approve(
        state, "reduce", proposal("2", side=Side.SELL, reducing=True), fee_rate=D(".5")
    )
    assert reduction.initial_margin == 0 and reduction.fee_reserve == 100
    with pytest.raises(TradingBlocked, match="available collateral"):
        approve(state, "entry", instrument=ETH, reservations=(reduction,))


@pytest.mark.parametrize(
    "value",
    [
        proposal("2", side=Side.SELL, reducing=True),
        proposal("1", reducing=True),
        proposal("1", side=Side.SELL),
        proposal("3", side=Side.SELL),
    ],
)
def test_reductions_cannot_exceed_quantity_and_ordinary_opposite_orders_do_not_reverse(value):
    state = account(position(quantity=D(1), entry_price=D(100)))
    with pytest.raises(TradingBlocked):
        approve(state, value=value)


def test_serial_instrument_reservation_and_owner_identity_are_mandatory_even_for_reductions():
    state = account(position(quantity=D(1), entry_price=D(100)))
    pending = approve(state, "old", proposal("1", side=Side.SELL, reducing=True))
    with pytest.raises(TradingBlocked, match="existing reservation"):
        approve(
            state, "next", proposal("1", side=Side.SELL, reducing=True), reservations=(pending,)
        )
    with pytest.raises(TradingBlocked, match="owner"):
        approve(state, owner_id="foreign")
    with pytest.raises(TradingBlocked, match="owner"):
        approve(state, instrument=Instrument("binance", Market.USDM, "XRPUSDT"))


def test_identical_order_request_is_noop_but_any_new_clock_policy_price_or_pin_conflicts():
    state = account()
    pending = approve(state)
    assert approve(state, reservations=(pending,)) is pending
    assert (
        approve(
            state, fee_rate=D(".0010"), worst_execution_price=D("100.00"), reservations=(pending,)
        )
        is pending
    )
    for changes in (
        {"as_of_ms": 1001},
        {"fee_rate": D(".002")},
        {"worst_execution_price": D(101)},
        {"max_mark_age_ms": 100},
        {"brackets": (replace(table(), checksum="b" * 64), table(ETH))},
    ):
        with pytest.raises(ProtocolError, match="frozen reservation"):
            approve(state, reservations=(pending,), **changes)


def test_changed_reservation_book_is_part_of_duplicate_request_fingerprint():
    state = account()
    original = approve(state)
    other = approve(state, "eth", instrument=ETH)
    with pytest.raises(ProtocolError, match="frozen reservation"):
        approve(state, reservations=(original, other))


def test_new_facts_or_higher_pending_mark_require_revalidation_without_automatic_price_upgrade():
    state = account()
    pending = approve(state, "eth", instrument=ETH)
    original_price = pending.valuation_price
    updated = apply_fill(
        state, FillEvent("fill", "actual", ETH, ETH.symbol, D(".1"), D(100), D(".01"), "USDT", 1, 2)
    ).account
    with pytest.raises(TradingBlocked, match="revalidation"):
        approve(updated, "btc", reservations=(pending,))
    for marks in ((mark(), mark(ETH, "101")), (mark(),)):
        with pytest.raises(TradingBlocked, match="revalidation"):
            approve(state, "btc", marks=marks, reservations=(pending,))
    assert pending.valuation_price == original_price and pending.account_sequence == 0


def test_protective_reduction_can_continue_with_unrelated_stale_reservation():
    state = account(position(quantity=D(1), entry_price=D(100)), position(ETH))
    pending = approve(state, "eth", instrument=ETH)
    result = approve(
        state,
        "reduce",
        proposal("1", side=Side.SELL, reducing=True),
        marks=(),
        brackets=(),
        reservations=(pending,),
    )
    assert result.initial_margin == 0 and result.fee_reserve == D(".1")


@pytest.mark.parametrize(
    "changes",
    [
        {"fee_rate": True},
        {"fee_rate": D("NaN")},
        {"fee_rate": D(-1)},
        {"fee_rate": D(2)},
        {"worst_execution_price": 100.0},
        {"worst_execution_price": D(0)},
        {"as_of_ms": True},
        {"max_mark_age_ms": True},
        {"reservations": []},
    ],
)
def test_untyped_or_unbounded_risk_input_is_rejected(changes):
    with pytest.raises(ValidationError):
        approve(**changes)


def test_reservation_amount_tamper_wrong_account_duplicate_id_and_capacity_are_rejected():
    pending = approve()
    with pytest.raises(ValidationError, match="amounts"):
        replace(pending, initial_margin=D(0))
    with pytest.raises(ProtocolError, match="another account"):
        approve(order_id="other", reservations=(replace(pending, account_id="foreign"),))
    with pytest.raises(ValidationError):
        approve(reservations=(pending, pending))
    with pytest.raises(ValidationError):
        approve(reservations=tuple(replace(pending, order_id=f"p{i}") for i in range(101)))
    changed_output = replace(
        pending, valuation_price=D(110), initial_margin=D(11), fee_reserve=D(".11")
    )
    with pytest.raises(ProtocolError, match="frozen reservation"):
        approve(reservations=(changed_output,))


def test_math_and_fingerprints_are_independent_of_ambient_decimal_precision():
    expected = approve(worst_execution_price=D("123.456789123456789"))
    with localcontext() as context:
        context.prec = 6
        actual = approve(worst_execution_price=D("123.456789123456789"))
    assert actual == expected


@pytest.mark.parametrize(
    "side,current_mark,worst",
    [
        (Side.BUY, "1", "100"),
        (Side.SELL, "100", "1"),
    ],
)
@pytest.mark.parametrize("mode", [MarginMode.CROSS, MarginMode.ISOLATED])
def test_adverse_fill_instant_loss_is_reserved_before_approval_for_flat_positions(
    side,
    current_mark,
    worst,
    mode,
):
    pos = position(mode=mode, isolated_wallet=D(20) if mode == MarginMode.ISOLATED else D(0))
    pos = replace(pos, budget_margin=D(200))
    state = account(pos, cash="20")
    with pytest.raises(TradingBlocked, match="available collateral"):
        approve(
            state,
            value=proposal(side=side),
            marks=(mark(price=current_mark),),
            worst_execution_price=D(worst),
            fee_rate=D(0),
        )
    funded = account(
        replace(
            pos,
            budget_margin=D(200),
            isolated_wallet=D(120) if mode == MarginMode.ISOLATED else D(0),
        ),
        cash="120",
    )
    result = approve(
        funded,
        value=proposal(side=side),
        marks=(mark(price=current_mark),),
        worst_execution_price=D(worst),
        fee_rate=D(0),
    )
    assert result.initial_margin == 10 and result.entry_loss == 99
    assert result.total_reserved == 109


def test_entry_loss_also_consumes_strategy_margin_budget_with_an_ample_wallet():
    state = account(cash="1000")
    with pytest.raises(TradingBlocked, match="allocated margin budget"):
        approve(state, marks=(mark(price="1"),), worst_execution_price=D(100), fee_rate=D(0))


def test_existing_pending_entry_loss_is_subtracted_from_cross_pool():
    state = account(cash="40")
    pending = approve(state, "eth", instrument=ETH, worst_execution_price=D(110))
    assert pending.total_reserved == D("21.11")
    with pytest.raises(TradingBlocked, match="available collateral"):
        approve(state, "btc", proposal("2"), reservations=(pending,))


@pytest.mark.parametrize(
    "side,quantity,worst,loss",
    [
        (Side.BUY, "1", "150", "50"),
        (Side.SELL, "-1", "50", "50"),
    ],
)
def test_held_same_direction_increase_includes_new_entry_loss_in_budget(
    side, quantity, worst, loss
):
    state = account(position(quantity=D(quantity), entry_price=D(100)), cash="1000")
    result = approve(
        state, value=proposal(side=side), worst_execution_price=D(worst), fee_rate=D(0)
    )
    assert result.entry_loss == D(loss)
    # Same wallet is ample, but a smaller allocation cannot absorb both existing
    # IM and the new entry's modeled immediate loss.
    limited = account(replace(state.positions[0], budget_margin=D(60)), cash="1000")
    with pytest.raises(TradingBlocked, match="budget"):
        approve(limited, value=proposal(side=side), worst_execution_price=D(worst), fee_rate=D(0))


@pytest.mark.parametrize(
    "side,worst,limit,adverse,loss",
    [
        (Side.BUY, "110", "120", "120", "20"),
        (Side.SELL, "90", "80", "80", "20"),
        (Side.BUY, "90", None, "90", "0"),
        (Side.SELL, "110", None, "110", "0"),
    ],
)
def test_side_specific_adverse_bound_and_favorable_prices_never_credit_future_profit(
    side,
    worst,
    limit,
    adverse,
    loss,
):
    result = approve(
        value=proposal(side=side, price=limit), worst_execution_price=D(worst), fee_rate=D(0)
    )
    assert result.adverse_fill_price == D(adverse) and result.entry_loss == D(loss)


@pytest.mark.parametrize(
    "side,worst,limit,new_mark",
    [
        (Side.BUY, "110", None, "90"),
        (Side.SELL, "90", "150", "105"),
    ],
)
def test_existing_pending_loss_increase_requires_revalidation_without_reserve_upgrade(
    side,
    worst,
    limit,
    new_mark,
):
    state = account()
    pending = approve(
        state,
        "eth",
        proposal(side=side, price=limit),
        instrument=ETH,
        worst_execution_price=D(worst),
    )
    assert pending.entry_loss == 10
    # Both marks stay below the old notional valuation; loss must be checked
    # separately from the existing upper-price bound.
    assert D(new_mark) < pending.valuation_price
    with pytest.raises(TradingBlocked, match="revalidation"):
        approve(state, "btc", marks=(mark(), mark(ETH, new_mark)), reservations=(pending,))
    assert pending.entry_loss == 10 and pending.mark_price == 100


def test_opening_loss_and_fixed_mark_or_adverse_fields_cannot_be_tampered():
    pending = approve(worst_execution_price=D(110))
    for changes in (
        {"entry_loss": D(0)},
        {"mark_price": None},
        {"adverse_fill_price": D(120)},
        {"mark_price": D(90)},
    ):
        with pytest.raises(ValidationError):
            replace(pending, **changes)
    changed = replace(pending, mark_price=D(90), entry_loss=D(20))
    with pytest.raises(ProtocolError, match="frozen reservation"):
        approve(worst_execution_price=D(110), reservations=(changed,))


@pytest.mark.parametrize("changed", ["missing", "stale", "unavailable", "cap", "leverage"])
def test_flat_pending_entry_needs_current_bracket_revalidation_before_other_entry(changed):
    state = account()
    pending = approve(state, "eth", instrument=ETH)
    current = table(ETH)
    if changed == "missing":
        tables = (table(),)
    else:
        if changed == "stale":
            current = replace(current, observed_at_ms=0)
        elif changed == "unavailable":
            current = replace(current, available_at_ms=1001)
        elif changed == "cap":
            current = replace(current, rows=(replace(current.rows[0], notional_cap=D(100)),))
        else:
            current = replace(
                current, rows=(replace(current.rows[0], max_leverage=5), current.rows[1])
            )
        tables = (table(), current)
    with pytest.raises(TradingBlocked, match="revalidation"):
        approve(state, "btc", brackets=tables, reservations=(pending,), max_bracket_age_ms=100)


def test_future_pending_book_cannot_be_adopted_by_earlier_approval_clock():
    state = account()
    pending = approve(state, "eth", instrument=ETH, as_of_ms=1001)
    with pytest.raises(TradingBlocked, match="revalidation"):
        approve(state, "btc", reservations=(pending,))


def test_future_account_fact_cannot_supply_wallet_to_earlier_reservation():
    state = apply_fill(
        account(),
        FillEvent(
            "later_fill", "later_order", ETH, ETH.symbol, D(1), D(100), D(0), "USDT", 900, NOW + 1
        ),
    ).account
    with pytest.raises(ProtocolError, match="unavailable.*cutoff"):
        approve(state, "btc")
    assert approve(state, "btc", as_of_ms=NOW + 1).account_sequence == 1
