"""Pure, frozen next-bar proxy plans: no store, broker, source clock or real orders."""

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.application.margin_execution import (
    FEE_QUANTUM,
    FrozenPendingOrder,
    advance_prefix_checksum,
    prepare_pending_batch,
)
from omega.application.margin_risk import approve_order
from omega.domain.errors import ProtocolError, ValidationError
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
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side
from omega.domain.trading import OrderProposal

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")


def state(
    *, cash="1000", budget="100", isolated=False, quantity="0", entry="0", instruments=(BTC, ETH)
):
    return MarginAccount(
        "a",
        "USDT",
        D(cash),
        tuple(
            MarginPosition(
                value,
                value.symbol,
                MarginMode.ISOLATED if isolated else MarginMode.CROSS,
                10,
                D(budget),
                D(quantity) if value == BTC else D(0),
                D(entry) if value == BTC else D(0),
                D(cash) if isolated else D(0),
            )
            for value in instruments
        ),
    )


def tables(instruments=(BTC, ETH), cap="1000000"):
    return tuple(
        MarginBracketSnapshot(
            value,
            (MarginBracket(D(0), D(cap), D(".01"), D(0), 20),),
            0,
            0,
            "explicit_test_table",
            "a" * 64,
        )
        for value in instruments
    )


def bar(instrument=BTC, *, opening=60000, price="100", high="120", low="80", close="110"):
    return Candle(instrument, opening, opening + 60000, D(price), D(high), D(low), D(close), D(10))


def pending(
    account,
    identity="order",
    *,
    instrument=BTC,
    side=Side.BUY,
    limit=None,
    quantity="1",
    fee=".001",
    price="100",
    reducing=False,
    eligible=60000,
    cap="1000000",
):
    proposal = OrderProposal(
        side,
        D(quantity),
        OrderKind.LIMIT if limit else OrderKind.MARKET,
        D(limit) if limit else None,
        reducing,
    )
    reservation = approve_order(
        account,
        identity,
        proposal,
        instrument=instrument,
        owner_id=instrument.symbol,
        as_of_ms=0,
        marks=tuple(
            MarkSnapshot(value.instrument, D(price), 0, 0, "test_mark")
            for value in account.positions
        ),
        brackets=tables(tuple(value.instrument for value in account.positions), cap),
        worst_execution_price=D(price),
        fee_rate=D(fee),
    )
    return FrozenPendingOrder(reservation, eligible)


def prepare(account, orders, candles=None, **changes):
    return prepare_pending_batch(
        account,
        orders,
        candles or (bar(), bar(ETH)),
        **{"as_of_ms": 120000, "brackets": tables(), **changes},
    )


def test_two_instruments_re_admit_against_successive_prefixes_without_mutating_original_intents():
    account = state()
    orders = (pending(account, "eth", instrument=ETH), pending(account, "btc"))
    result = prepare(account, orders)
    assert [fact.order_id for fact in result.facts] == ["btc", "eth"]
    assert [len(value.ledger) for value in result.prefix_accounts] == [0, 1, 2]
    assert [value.reservation.account_sequence for value in result.admissions] == [0, 1]
    assert all(value.original.account_sequence == 0 for value in orders)
    assert result.remaining == () and result.rejections == () and result.ready
    assert result.candidate_account.cross_wallet == D("999.8")
    assert account.ledger == () and all(value.quantity == 0 for value in account.positions)
    for index, candidate in enumerate(result.prefix_accounts[1:]):
        assert (
            advance_prefix_checksum(result.prefix_checksums[index], candidate)
            == result.prefix_checksums[index + 1]
        )
    with pytest.raises(FrozenInstanceError):
        result.as_of_ms = 130000


def test_identical_preparation_is_deterministic_regardless_of_input_arrival_order():
    account = state()
    orders = (pending(account, "btc"), pending(account, "eth", instrument=ETH))
    first = prepare(account, orders)
    repeated = prepare(account, orders[::-1], (bar(ETH), bar()))
    assert first == repeated and first.checksum == repeated.checksum
    changed = prepare(account, orders, (replace(bar(), high=D(121)), bar(ETH)))
    assert changed.batch_id == first.batch_id and changed.checksum != first.checksum
    later = prepare(account, orders, as_of_ms=120001)
    assert later.batch_id == first.batch_id and later.checksum != first.checksum
    assert first.facts[0].available_at_ms == 120000


def test_ledger_fact_authority_consumes_duplicate_pending_without_another_fee_or_fact():
    account = state()
    order = pending(account)
    first = prepare(account, (order,))
    repeated = prepare(first.candidate_account, (order,))
    assert repeated.facts == () and repeated.remaining == ()
    assert repeated.admissions[0].outcome == "already_settled"
    assert repeated.candidate_account is first.candidate_account
    assert repeated.candidate_account.cross_wallet == D("999.9")


def test_same_fill_identity_with_a_different_fee_is_not_accepted_as_settled():
    account = state()
    order = pending(account)
    first = prepare(account, (order,))
    altered = apply_fill(account, replace(first.facts[0], fee=D(".2"))).account
    with pytest.raises(ProtocolError, match="conflicts"):
        prepare(altered, (order,))


def test_changed_account_snapshot_changes_checksum_even_with_same_bar_and_intent():
    account = state()
    order = pending(account)
    first = prepare(account, (order,))
    altered = replace(account, cross_wallet=D(1001), opening_balance=D(1001))
    changed = prepare(altered, (order,))
    assert changed.batch_id == first.batch_id and changed.checksum != first.checksum


def test_open_and_late_limit_close_phases_do_not_mix_wicks_or_fill_times():
    account = state()
    market = pending(account, "market")
    late = pending(account, "limit", instrument=ETH, limit="90")
    opened = prepare(account, (market, late))
    assert [fact.order_id for fact in opened.facts] == ["market"]
    assert opened.facts[0].price == 100 and opened.facts[0].event_time_ms == 60000
    assert opened.remaining == (late,) and opened.admissions[1].outcome == "await_close"
    closed = prepare(opened.candidate_account, opened.remaining, phase="close")
    assert closed.facts[0].order_id == "limit" and closed.facts[0].price == 90
    assert closed.facts[0].event_time_ms == 120000
    assert closed.admissions[0].reservation.mark_price == 110
    assert closed.admissions[0].reservation is not late.original
    assert late.original.mark_price == 100


def test_close_phase_cannot_backdate_an_unprocessed_open_fill():
    account = state()
    result = prepare(account, (pending(account),), phase="close")
    assert not result.ready and not result.facts
    assert result.admissions[0].reason == "open_phase_required"


def test_untouched_limit_and_subminute_latency_retain_original_pending():
    account = state()
    late = pending(account, eligible=60001)
    result = prepare(account, (late,))
    assert not result.facts and result.remaining == (late,)
    assert result.admissions[0].outcome == "waiting_eligible"
    later = prepare(
        account, (late,), (bar(opening=120000), bar(ETH, opening=120000)), as_of_ms=180000
    )
    assert later.facts[0].event_time_ms == 120000
    limit = pending(account, limit="70")
    untouched = prepare(account, (limit,), phase="close")
    assert untouched.remaining == (limit,) and untouched.admissions[0].outcome == "not_touched"


def test_gap_rejects_new_entry_for_insufficient_actual_margin_without_a_ledger_fact():
    account = state(cash="20")
    order = pending(account)
    result = prepare(account, (order,), (bar(price="1000", high="1000", low="1000", close="1000"),))
    assert result.rejections == (("order", "original_execution_bound_exceeded"),)
    assert not result.facts and not result.remaining and result.candidate_account is account


def test_missing_other_pending_claim_defers_entry_but_known_reduction_can_continue():
    account = state()
    orders = (pending(account, "btc"), pending(account, "eth", instrument=ETH))
    blocked = prepare(account, orders, (bar(),))
    assert not blocked.facts and not blocked.ready and len(blocked.remaining) == 2
    account = state(quantity="1", entry="100")
    reduced = pending(account, "reduce", side=Side.SELL, reducing=True)
    other = pending(account, "eth", instrument=ETH)
    allowed = prepare(account, (reduced, other), (bar(),))
    assert len(allowed.facts) == 1 and allowed.facts[0].order_id == "reduce"
    assert allowed.candidate_account.positions[0].quantity == 0 and allowed.remaining == (other,)


@pytest.mark.parametrize("limited", ["collateral", "budget"])
def test_positive_fee_rounding_difference_is_checked_in_addition_to_risk_estimate(limited):
    account = state(
        cash=".100000001" if limited == "collateral" else "1",
        budget=".100000001" if limited == "budget" else "1",
        instruments=(BTC,),
    )
    order = pending(account, price="1", fee=".000000001")
    result = prepare(account, (order,), (bar(price="1", high="1", low="1", close="1"),))
    assert result.rejections and not result.facts and result.candidate_account is account
    assert result.admissions[0].fee_adjustment == D(".000000009")


def test_reduction_records_rounded_estimated_fee_debt_even_if_rounding_room_is_absent():
    account = state(cash="-1", quantity="1", entry="1", budget="0", instruments=(BTC,))
    order = pending(account, price="1", fee=".000000001", side=Side.SELL, reducing=True)
    result = prepare(
        account, (order,), (bar(price="1", high="1", low="1", close="1"),), brackets=()
    )
    assert result.facts[0].fee == FEE_QUANTUM
    assert result.candidate_account.cross_wallet == D("-1.00000001")
    assert result.candidate_account.positions[0].quantity == 0


def test_fee_outside_factual_dto_bound_rejects_entire_prepare_without_partial_publication():
    account = state(cash="1e20", budget="1e20")
    orders = (
        pending(account, "small", cap="1e18"),
        pending(
            account, "huge", instrument=ETH, quantity="123456789", price="123456789", cap="1e18"
        ),
    )
    # The large original can be constructed with an exact fee, but 8 decimal
    # settlement places exceed FillEvent's 18 significant-digit bound.
    with pytest.raises(ValidationError):
        prepare(
            account,
            orders,
            (
                bar(),
                bar(ETH, price="123456789", high="123456789", low="123456789", close="123456789"),
            ),
            brackets=tables(cap="1e18"),
        )
    assert account.ledger == ()


def test_factual_direction_change_rejects_an_old_opposite_entry_instead_of_reversing():
    account = state()
    order = pending(account, side=Side.SELL)
    held = apply_fill(
        account,
        FillEvent(
            "external_fact", "different_order", BTC, BTC.symbol, D(1), D(100), D(0), "USDT", 1, 2
        ),
    ).account
    result = prepare(held, (order,))
    assert result.rejections and not result.facts
    assert result.candidate_account.positions[0].quantity == 1


def test_unsupported_or_conflicting_source_batch_and_future_snapshot_are_rejected():
    account = state()
    order = pending(account)
    for candles in ((bar(), bar()), (bar(), bar(ETH, opening=120000))):
        with pytest.raises(ValidationError):
            prepare(account, (order,), candles)
    with pytest.raises(ValidationError):
        prepare(account, (order,), as_of_ms=119999)
    future = apply_fill(
        account,
        FillEvent("future", "other", ETH, ETH.symbol, D(1), D(100), D(0), "USDT", 1, 120001),
    ).account
    with pytest.raises(ProtocolError, match="unavailable"):
        prepare(future, (order,))
    with pytest.raises(ValidationError):
        prepare(account, (order, order))


def test_fee_and_candidate_checksums_do_not_depend_on_caller_decimal_precision():
    account = state()
    order = pending(account, quantity=".123456789123456789")
    expected = prepare(account, (order,))
    with localcontext() as context:
        context.prec = 6
        actual = prepare(account, (order,))
    assert actual == expected


@pytest.mark.parametrize(
    "side,price,reason",
    [
        (Side.BUY, "110", "original_execution_bound_exceeded"),
        (Side.SELL, "90", "original_execution_bound_exceeded"),
        (Side.BUY, "90", "original_entry_loss_bound_exceeded"),
        (Side.SELL, "110", "original_entry_loss_bound_exceeded"),
    ],
)
def test_ample_cash_cannot_automatically_upgrade_original_price_or_loss_bounds(side, price, reason):
    account = state(cash="10000", budget="1000")
    order = pending(account, side=side)
    result = prepare(account, (order,), (bar(price=price),))
    assert not result.facts and result.rejections == (("order", reason),)
    assert order.original.worst_execution_price == 100 and order.original.entry_loss == 0
    assert result.candidate_account is account


def test_allowed_price_inside_original_bound_keeps_original_worst_in_new_admission():
    account = state()
    original = approve_order(
        account,
        "bounded",
        OrderProposal(Side.BUY, D(1)),
        instrument=BTC,
        owner_id=BTC.symbol,
        as_of_ms=0,
        marks=(MarkSnapshot(BTC, D(100), 0, 0, "mark"),),
        brackets=tables(),
        worst_execution_price=D(110),
        fee_rate=D(".001"),
    )
    order = FrozenPendingOrder(original, 60000)
    result = prepare(account, (order,), (bar(price="105"),))
    assert result.facts[0].price == 105
    assert result.admissions[0].reservation.worst_execution_price == 110
    assert result.admissions[0].reservation.entry_loss == 5 and original.entry_loss == 10


def test_known_reduction_is_not_blocked_only_because_its_original_price_bound_changed():
    account = state(quantity="1", entry="100")
    order = pending(account, reducing=True, side=Side.SELL)
    result = prepare(account, (order,), (bar(price="90"),))
    assert result.facts[0].price == 90 and result.candidate_account.positions[0].quantity == 0
    assert order.original.worst_execution_price == 100
