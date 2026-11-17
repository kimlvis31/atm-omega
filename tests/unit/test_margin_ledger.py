"""Pure v2 facts preserve wallets, signed fills and immutable/idempotent candidates."""

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.margin import (
    CollateralTransfer,
    FillEvent,
    LedgerEntry,
    MarginAccount,
    MarginMode,
    MarginPosition,
    apply_fill,
    apply_transfer,
)
from omega.domain.models import Instrument, Market

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def account(
    *, mode=MarginMode.CROSS, quantity="0", entry="0", cash="1000", isolated="0", capacity=10000
):
    return MarginAccount(
        "account",
        "USDT",
        D(cash),
        (MarginPosition(BTC, "strategy", mode, 10, D(100), D(quantity), D(entry), D(isolated)),),
        event_capacity=capacity,
    )


def fill(identity="f1", delta="1", price="100", fee="0.1", *, when=1, owner="strategy"):
    return FillEvent(
        identity, "order", BTC, owner, D(delta), D(price), D(fee), "USDT", when, when + 1
    )


def transfer(identity="t1", amount="100", *, when=1):
    return CollateralTransfer(identity, BTC, "strategy", D(amount), when, when + 1)


@pytest.mark.parametrize(
    "quantity,entry,delta,price,new_qty,new_entry,realized",
    [
        ("0", "0", "2", "100", "2", "100", "0"),
        ("2", "100", "1", "130", "3", "110", "0"),
        ("-2", "100", "-1", "130", "-3", "110", "0"),
        ("2", "100", "-1", "130", "1", "100", "30"),
        ("-2", "100", "1", "70", "-1", "100", "30"),
        ("1", "100", "-1", "110", "0", "0", "10"),
        ("1", "100", "-3", "110", "-2", "110", "10"),
        ("-1", "100", "3", "90", "2", "90", "10"),
    ],
)
def test_signed_fill_splits_reversal_instead_of_legacy_absolute_quantity_branch(
    quantity,
    entry,
    delta,
    price,
    new_qty,
    new_entry,
    realized,
):
    before = account(quantity=quantity, entry=entry)
    result = apply_fill(before, fill(delta=delta, price=price))
    position = result.account.positions[0]
    assert position.quantity == D(new_qty) and position.entry_price == D(new_entry)
    assert result.entry.realized_pnl == D(realized)
    assert result.account.cross_wallet == D(1000) + D(realized) - D(".1")
    assert before.positions[0].quantity == D(quantity)
    assert before.ledger == ()
    with pytest.raises(FrozenInstanceError):
        result.account.cross_wallet = D(0)


def test_partial_fills_charge_each_actual_fee_once_and_keep_order_id_separate_from_fill_id():
    state = account()
    for event in (fill("a", ".3", "100", ".03"), fill("b", ".7", "110", ".077", when=2)):
        state = apply_fill(state, event).account
    assert state.positions[0].quantity == 1 and state.positions[0].entry_price == 107
    assert state.cross_wallet == D("999.893") and len(state.ledger) == 2
    result = apply_fill(state, fill("c", "-1", "120", ".12", when=3))
    assert result.entry.realized_pnl == 13 and result.account.cross_wallet == D("1012.773")


def test_numeric_decimal_scale_is_idempotent_at_capacity_but_conflicting_fact_is_not():
    event = fill()
    state = apply_fill(account(capacity=1), event).account
    same = apply_fill(state, replace(event, delta_qty=D("1.00"), price=D("100.0")))
    assert same.account is state and same.entry is None
    with pytest.raises(ProtocolError, match="conflicts"):
        apply_fill(state, replace(event, price=D(101)))
    with pytest.raises(ProtocolError, match="capacity"):
        apply_fill(state, fill("new", when=2))


def test_isolated_realized_loss_and_fee_create_debt_without_drawing_cross_wallet():
    before = account(mode=MarginMode.ISOLATED, quantity="1", entry="100", isolated="5")
    result = apply_fill(before, fill(delta="-1", price="80", fee="1"))
    assert result.account.cross_wallet == 1000
    assert result.account.positions[0].isolated_wallet == -16
    assert result.entry.cross_delta == 0 and result.entry.isolated_delta == -21
    assert result.account.opening_balance == 1005


def test_actual_cross_fill_fee_larger_than_wallet_is_recorded_instead_of_rejected_or_clamped():
    state = apply_fill(account(cash="0"), fill(fee="3")).account
    assert state.cross_wallet == -3 and state.positions[0].quantity == 1


def test_explicit_transfer_is_balanced_idempotent_and_never_automatic():
    before = account(mode=MarginMode.ISOLATED)
    funded = apply_transfer(before, transfer()).account
    assert funded.cross_wallet == 900 and funded.positions[0].isolated_wallet == 100
    opened = apply_fill(funded, fill(when=2)).account
    assert opened.cross_wallet == 900 and opened.positions[0].isolated_wallet == D("99.9")
    withdrawn = apply_transfer(opened, transfer("t2", "-99.9", when=3)).account
    assert withdrawn.positions[0].quantity == 1 and withdrawn.positions[0].isolated_wallet == 0
    # This is a cash movement, not an approval that the remaining position is safe.
    assert apply_transfer(withdrawn, transfer("t2", "-99.90", when=3)).account is withdrawn
    for event in (transfer("too-much", "1001", when=4), transfer("negative", "-1", when=4)):
        with pytest.raises(ProtocolError, match="insufficient"):
            apply_transfer(withdrawn, event)
    with pytest.raises(ValidationError, match="isolated"):
        apply_transfer(account(), transfer())


def test_one_way_owner_and_late_fact_conflicts_do_not_change_original_candidate():
    state = apply_fill(account(), fill(when=10)).account
    with pytest.raises(ProtocolError, match="owner"):
        apply_fill(state, fill("foreign", owner="another", when=11))
    with pytest.raises(ProtocolError, match="ordered replay"):
        apply_fill(state, fill("late", when=9))
    with pytest.raises(ValidationError):
        replace(state, positions=state.positions + (replace(state.positions[0], owner_id="other"),))
    assert state.positions[0].quantity == 1 and len(state.ledger) == 1


def test_fixed_math_and_exact_cash_addition_do_not_depend_on_callers_decimal_context():
    def run():
        state = account()
        for index, event in enumerate(
            (
                fill("a", "1", "1", ".000000000000000001"),
                fill("b", "2", "2", "0", when=2),
                fill("c", "-1", "3", "0", when=3),
            )
        ):
            state = apply_fill(state, event).account
            assert len(state.ledger) == index + 1
        return state

    expected = run()
    with localcontext() as context:
        context.prec = 6
        actual = run()
    assert actual == expected
    with localcontext() as context:
        context.prec = 256
        assert actual.cross_wallet == actual.opening_balance + sum(
            (entry.realized_pnl - entry.fee for entry in actual.ledger), D(0)
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"delta_qty": D(0)},
        {"price": D(0)},
        {"fee": D(-1)},
        {"fee_asset": "BNB"},
        {"delta_qty": 1.0},
        {"price": D("NaN")},
        {"price": D("1e19")},
        {"event_time_ms": True},
        {"available_at_ms": 0},
    ],
)
def test_unbounded_or_untyped_fill_inputs_are_rejected(changes):
    with pytest.raises(ValidationError):
        replace(fill(), **changes)


def test_restored_cash_or_ledger_corruption_is_rejected():
    state = apply_fill(account(), fill()).account
    assert replace(state) == state
    with pytest.raises(ValidationError, match="conservation"):
        replace(state, cross_wallet=state.cross_wallet + 1)
    with pytest.raises(ValidationError, match="conservation"):
        LedgerEntry(1, fill(), D(0), D(".1"), D(0), D(0))
    with pytest.raises(ValidationError):
        replace(state, opening_balance=None)


def test_wallet_partition_and_position_realized_total_cannot_be_forged_on_restore():
    state = apply_fill(account(quantity="1", entry="100"), fill(delta="-1", price="110")).account
    original = state.ledger[0]
    diverted = replace(original, cross_delta=D(0), isolated_delta=original.cross_delta)
    with pytest.raises(ValidationError, match="wrong wallet"):
        replace(state, ledger=(diverted,))
    with pytest.raises(ValidationError, match="realized total"):
        replace(state, positions=(replace(state.positions[0], realized_pnl=D(0)),))


def test_small_fraction_is_retained_alongside_large_actual_fill_quantity():
    state = apply_fill(account(), fill("big", "999999999999999999e18", "1", "0")).account
    updated = apply_fill(state, fill("small", "1e-18", "1", "0", when=2)).account
    with localcontext() as context:
        context.prec = 256
        assert updated.positions[0].quantity - state.positions[0].quantity == D("1e-18")
    assert len(updated.ledger) == 2


def test_exact_cash_capacity_rejects_unrepresentable_sum_without_losing_existing_state():
    state = account(cash="1e256")
    with pytest.raises(ValidationError, match="exact bound"):
        apply_fill(state, fill(fee=".1"))
    assert state.cross_wallet == D("1e256") and not state.ledger


def test_position_capacity_is_explicit_and_dedup_facts_are_not_evicted():
    positions = tuple(
        replace(account().positions[0], instrument=Instrument("binance", Market.USDM, f"P{i}USDT"))
        for i in range(101)
    )
    assert len(MarginAccount("a", "USDT", D(0), positions[:100]).positions) == 100
    with pytest.raises(ValidationError, match="oversized"):
        MarginAccount("a", "USDT", D(0), positions)
