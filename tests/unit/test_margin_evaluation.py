"""Exact supplied-mark collateral evaluation; no liquidation/funding execution claims."""

from dataclasses import replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.domain.errors import ValidationError
from omega.domain.margin import (
    MarginAccount,
    MarginBracket,
    MarginBracketSnapshot,
    MarginMode,
    MarginPosition,
    MarkSnapshot,
    evaluate_margin,
)
from omega.domain.models import Instrument, Market

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")
NOW = 1000


def table(instrument=BTC, **changes):
    return MarginBracketSnapshot(
        instrument,
        (
            MarginBracket(D(0), D(1000), D(".01"), D(0), 20),
            MarginBracket(D(1000), D(2000), D(".02"), D(10), 10),
        ),
        900,
        950,
        "explicit_test_table",
        "a" * 64,
        **changes,
    )


def position(instrument=BTC, mode=MarginMode.CROSS, **changes):
    return MarginPosition(
        instrument,
        instrument.symbol,
        mode,
        10,
        D(100),
        **{"quantity": D(2), "entry_price": D(100), **changes},
    )


def mark(price="120", instrument=BTC):
    return MarkSnapshot(instrument, D(price), 900, 950, "explicit_mark")


def evaluate(pos=None, *, price="120", cash="1000", marks=None, tables=None, **kwargs):
    state = MarginAccount("a", "USDT", D(cash), (pos or position(),))
    return evaluate_margin(
        state,
        (mark(price),) if marks is None else marks,
        (table(),) if tables is None else tables,
        as_of_ms=NOW,
        **kwargs,
    )


def test_linear_initial_maintenance_unrealized_equity_and_free_collateral_are_distinct():
    result = evaluate()
    row = result.positions[0]
    assert result.ready and row.ready
    assert row.initial_margin == 24 and row.maintenance_margin == D("2.4")
    assert row.unrealized_pnl == 40 and row.budget_remaining == 76
    assert result.wallet_balance == 1000 and result.equity == 1040
    assert result.cross_free_collateral == 1016
    assert (
        row.equity is None and row.free_collateral is None
    )  # No per-position cross wallet copies.


def test_isolated_equity_and_losses_do_not_become_cross_collateral():
    result = evaluate(position(mode=MarginMode.ISOLATED, isolated_wallet=D(10)), price="80")
    row = result.positions[0]
    assert row.unrealized_pnl == -40 and row.equity == -30
    assert row.initial_margin == 16 and row.free_collateral == -46
    assert result.cross_free_collateral == 1000
    assert result.wallet_balance == 1010 and result.equity == 970


@pytest.mark.parametrize(
    "price,maintenance", [("499.99", "9.9998"), ("500", "10"), ("999.99", "29.9996")]
)
def test_bracket_cap_is_exclusive_and_cumulative_deduction_is_applied(price, maintenance):
    result = evaluate(price=price)
    assert result.ready and result.maintenance_margin == D(maintenance)


@pytest.mark.parametrize(
    "marks,tables,kwargs,reason",
    [
        ((), None, {}, "mark_missing"),
        (None, (), {}, "bracket_missing"),
        ((replace(mark(), available_at_ms=1001),), None, {}, "mark_unavailable_at_cutoff"),
        (None, (replace(table(), available_at_ms=1001),), {}, "bracket_unavailable_at_cutoff"),
        (None, None, {"max_mark_age_ms": 99}, "mark_stale"),
        (None, None, {"max_bracket_age_ms": 99}, "bracket_stale"),
    ],
)
def test_unavailable_inputs_remain_none_instead_of_zero(marks, tables, kwargs, reason):
    result = evaluate(marks=marks, tables=tables, **kwargs)
    assert not result.ready and result.positions[0].reason == reason
    assert result.initial_margin is None and result.equity is None
    assert result.cross_free_collateral is None and result.positions[0].maintenance_margin is None


def test_last_cap_and_leverage_excess_are_unavailable_without_fallback():
    at_cap = evaluate(price="1000")
    assert not at_cap.ready and at_cap.positions[0].reason == "bracket_range_unsupported"
    oversized = evaluate(replace(position(), leverage=11), price="500")
    assert not oversized.ready and oversized.positions[0].reason == "leverage_exceeds_bracket"


def test_flat_positions_require_no_mark_or_bracket_but_do_not_approve_new_risk():
    result = evaluate(position(quantity=D(0), entry_price=D(0)), marks=(), tables=())
    assert result.ready and result.initial_margin == result.maintenance_margin == 0
    assert result.cross_free_collateral == 1000


def test_multi_position_cross_loss_is_atomic_and_input_order_independent():
    first = position()
    second = position(ETH, quantity=D(-1), entry_price=D(200))
    account = MarginAccount("a", "USDT", D(1000), (first, second))
    marks, tables = (mark("80"), mark("220", ETH)), (table(), table(ETH))
    normal = evaluate_margin(account, marks, tables, as_of_ms=NOW)
    reverse = evaluate_margin(
        replace(account, positions=(second, first)), marks[::-1], tables[::-1], as_of_ms=NOW
    )
    assert normal.equity == reverse.equity == 940
    assert normal.initial_margin == reverse.initial_margin == 38
    assert normal.cross_free_collateral == reverse.cross_free_collateral == 902
    assert {row.instrument: row for row in normal.positions} == {
        row.instrument: row for row in reverse.positions
    }


def test_context_independence_and_exact_ttl_boundary():
    expected = evaluate(price="123.456789123456789", max_mark_age_ms=100, max_bracket_age_ms=100)
    with localcontext() as context:
        context.prec = 6
        actual = evaluate(price="123.456789123456789", max_mark_age_ms=100, max_bracket_age_ms=100)
    assert actual == expected and actual.ready


def test_bracket_gap_overlap_discontinuity_and_boolean_leverage_are_invalid():
    first, second = table().rows
    for row in (
        replace(second, notional_floor=D(1001)),
        replace(second, notional_floor=D(999)),
        replace(second, maintenance_amount=D(9)),
    ):
        with pytest.raises(ValidationError):
            replace(table(), rows=(first, row))
    with pytest.raises(ValidationError):
        replace(first, max_leverage=True)


def test_duplicate_snapshots_unsupported_settlement_and_mutable_input_are_rejected():
    with pytest.raises(ValidationError):
        evaluate(marks=(mark(), mark()))
    with pytest.raises(ValidationError):
        evaluate(tables=[table()])
    with pytest.raises(ValidationError):
        MarginAccount("a", "USDC", D(1000))
    with pytest.raises(ValidationError):
        replace(position(), instrument=Instrument("binance", Market.SPOT, "BTCUSDT"))
    with pytest.raises(ValidationError):
        replace(position(), leverage=True)
