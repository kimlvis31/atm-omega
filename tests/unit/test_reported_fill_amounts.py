"""Reported economics are facts; synthetic quote differences do not assert venue rounding."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from decimal import getcontext, localcontext
from fractions import Fraction

import pytest

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.ledger import CashLedger
from omega.domain.models import Fill, Instrument, Market, Side
from omega.infrastructure.binance.parsing import trade_fill

SPOT = Instrument("binance", Market.SPOT, "BTCUSDT")
USDM = Instrument("binance", Market.USDM, "BTCUSDT")


def fill(identity="1", **changes):
    return replace(
        Fill("A", SPOT, identity, "7", Side.BUY, D(1), D(100), D(0), "USDT", 60000), **changes
    )


def row(**changes):
    return {
        "symbol": "BTCUSDT",
        "id": 1,
        "orderId": 7,
        "isBuyer": True,
        "qty": "1",
        "price": "100",
        "commission": "0.1",
        "commissionAsset": "USDT",
        "time": 60000,
    } | changes


def ledger(**changes):
    return CashLedger("A", SPOT, "USDT", D(1000), base_asset="BTC", **changes)


def context_state():
    context = getcontext()
    return (
        context.prec,
        context.rounding,
        context.Emin,
        context.Emax,
        context.capitals,
        context.clamp,
        dict(context.traps),
        dict(context.flags),
    )


def test_appended_optional_fields_keep_original_positional_construction_and_missing_semantics():
    original = fill()
    assert original.quote_quantity is original.realized_pnl is original.realized_pnl_asset is None
    assert trade_fill("A", SPOT, row()).quote_quantity is None
    assert trade_fill("A", USDM, row(side="BUY")).realized_pnl is None
    observed_zero = replace(original, realized_pnl=D(0), realized_pnl_asset="USDT")
    assert observed_zero != original and observed_zero.realized_pnl == 0


@pytest.mark.parametrize(
    "quote", [0, False, 100.0, "100", D(0), D(-1), D("NaN"), D("Infinity"), D("1e61")]
)
def test_reported_quote_is_native_bounded_positive_decimal_or_explicitly_missing(quote):
    with pytest.raises(ValidationError):
        fill(quote_quantity=quote)


@pytest.mark.parametrize(
    "pnl,asset",
    [
        (D(0), None),
        (None, "USDT"),
        (False, "USDT"),
        (0.0, "USDT"),
        ("0", "USDT"),
        (D("NaN"), "USDT"),
        (D("1e61"), "USDT"),
        (D(0), ""),
        (D(0), "X" * 41),
        (D(0), "US DT"),
        (D(0), "USDT\n"),
        (D(0), 1),
    ],
)
def test_reported_pnl_requires_a_native_signed_amount_and_paired_bounded_asset(pnl, asset):
    with pytest.raises(ValidationError):
        fill(realized_pnl=pnl, realized_pnl_asset=asset)


def test_parser_preserves_reported_quote_and_usdm_pnl_without_deriving_commissions():
    spot = trade_fill("A", SPOT, row(quoteQty="100.001"))
    assert spot.quote_quantity == D("100.001") and spot.fee == D(".1")
    assert spot.realized_pnl is spot.realized_pnl_asset is None
    futures = trade_fill(
        "A", USDM, row(side="SELL", quoteQty="100.001", realizedPnl="-2.25", marginAsset="USDT")
    )
    assert futures.quote_quantity == D("100.001")
    assert futures.realized_pnl == D("-2.25") and futures.realized_pnl_asset == "USDT"
    zero = trade_fill("A", USDM, row(side="BUY", realizedPnl="0", marginAsset="USDC"))
    assert zero.realized_pnl == 0 and zero.realized_pnl_asset == "USDC"


@pytest.mark.parametrize(
    "changes",
    [
        {"quoteQty": None},
        {"quoteQty": 100.0},
        {"quoteQty": True},
        {"quoteQty": "0"},
        {"quoteQty": "NaN"},
        {"realizedPnl": None, "marginAsset": "USDT"},
        {"realizedPnl": 0.0, "marginAsset": "USDT"},
        {"realizedPnl": "0"},
        {"marginAsset": "USDT"},
        {"realizedPnl": "0", "marginAsset": None},
        {"realizedPnl": "0", "marginAsset": "US DT"},
    ],
)
def test_present_invalid_or_unpaired_provider_economics_never_become_missing(changes):
    with pytest.raises(ProtocolError):
        trade_fill("A", USDM, row(side="BUY", **changes))


def test_reported_quote_uses_provider_gross_value_not_a_recomputed_price_product():
    # Defensive synthetic difference; this fixture is not observed Binance behavior.
    owner = ledger()
    observed = fill(quote_quantity=D(101), fee=D(".1"))
    assert owner.apply(observed)
    assert owner.cash == D("898.9") and owner.book_cost == D(101)
    assert owner.quantity == D(1) and owner.fees == {"USDT": D(".1")}
    assert owner.apply(observed) is False


def test_reported_updates_are_exact_under_coarse_ambient_context_and_leave_it_unchanged():
    amount = D("123456789012345678901234567890.123456789")
    initial = D("223456789012345678901234567890.123456789")
    observed = fill(
        quantity=D("1.123456789012345678901234567890123456789"),
        quote_quantity=amount,
        fee=D(".000000000000000000000000000123456789"),
    )
    owner = CashLedger("A", SPOT, "USDT", initial, base_asset="BTC")
    global_before = context_state()
    with localcontext() as ambient:
        ambient.prec = 3
        ambient.clear_flags()
        ambient_before = context_state()
        assert owner.apply(observed)
        assert context_state() == ambient_before
        assert Fraction(owner.cash) == Fraction(initial) - Fraction(amount) - Fraction(observed.fee)
        assert owner.book_cost == amount and owner.quantity == observed.quantity
        assert owner.fees["USDT"] == observed.fee
    assert context_state() == global_before


def test_base_commission_buy_and_sell_share_reported_cost_basis_and_residual_policy():
    owner = ledger()
    owner.apply(
        fill(quantity=D(2), price=D(99), quote_quantity=D(200), fee=D(".01"), fee_asset="BTC")
    )
    assert (owner.cash, owner.quantity, owner.book_cost) == (D(800), D("1.99"), D(199))
    assert owner.base_fee_cost_in_quote == D(1)
    owner.apply(
        fill(
            "2", side=Side.SELL, price=D(109), quote_quantity=D(110), fee=D(".01"), fee_asset="BTC"
        )
    )
    assert (owner.cash, owner.quantity, owner.book_cost) == (D(910), D(".98"), D(98))
    assert owner.realized_gross_pnl == D(10) and owner.base_fee_cost_in_quote == D(2)
    assert owner.fees == {"BTC": D(".02")}


def test_quote_and_base_rebates_and_third_asset_fees_keep_their_original_units():
    owner = ledger()
    owner.apply(fill(quantity=D(2), quote_quantity=D(200), fee=D("-.1")))
    owner.apply(fill("2", side=Side.SELL, quote_quantity=D(110), fee=D("-.02")))
    assert owner.cash == D("910.12") and owner.realized_gross_pnl == D(10)
    assert owner.fees == {"USDT": D("-.12")}
    base = ledger()
    base.apply(fill(quote_quantity=D(100), fee=D("-.01"), fee_asset="BTC"))
    assert base.quantity == D("1.01") and base.book_cost == D(101)
    assert base.base_fee_cost_in_quote == D(-1) and base.cash == D(900)
    third = ledger()
    third.apply(fill(quote_quantity=D(100), fee=D(".03"), fee_asset="BNB"))
    assert third.cash == D(900) and third.book_cost == D(100)
    assert third.fees == {"BNB": D(".03")}


@pytest.mark.parametrize("fault", ["capacity", "inventory", "base_commission", "fees", "pnl"])
def test_reported_path_rejects_before_any_wallet_inventory_fee_or_dedup_mutation(fault):
    owner, observed = ledger(), fill(quote_quantity=D(100))
    if fault == "capacity":
        owner.cash, owner.quantity, owner.book_cost = D("1e512"), D(1), D(100)
        observed = replace(observed, side=Side.SELL, price=D("100.1"), quote_quantity=D("100.1"))
    elif fault == "inventory":
        observed = replace(observed, side=Side.SELL)
    elif fault == "base_commission":
        observed = replace(observed, fee=D(2), fee_asset="BTC")
    elif fault == "fees":
        owner.fees["USDT"] = D("1e512")
        observed = replace(observed, fee=D(1))
    else:
        observed = replace(observed, realized_pnl=D(0), realized_pnl_asset="USDT")
    original = deepcopy(owner.__dict__)
    global_before = context_state()
    with pytest.raises(ValidationError):
        owner.apply(observed)
    assert owner.__dict__ == original and context_state() == global_before


def test_legacy_none_branch_retains_original_math_and_enrichment_cannot_apply_twice():
    owner = ledger()
    original = fill(fee=D(".1"))
    assert owner.apply(original) and owner.cash == D("899.9") and owner.book_cost == D(100)
    before = deepcopy(owner.__dict__)
    with pytest.raises(ValidationError, match="conflicting"):
        owner.apply(replace(original, quote_quantity=D(100)))
    assert owner.__dict__ == before


def test_base_fee_valuation_uses_documented_fixed_division_and_conserves_linear_allocation():
    owner = ledger()
    observed = fill(quantity=D(3), quote_quantity=D(100), fee=D(1), fee_asset="BTC")
    owner.apply(observed)
    assert owner.cash == D(900) and owner.quantity == D(2)
    assert Fraction(owner.book_cost) + Fraction(owner.base_fee_cost_in_quote) == Fraction(100)
    assert len(owner.book_cost.as_tuple().digits) == 256


def test_ordinary_partial_sells_keep_finite_linear_updates_beyond_valuation_precision():
    owner = ledger()
    fills = (
        fill(quantity=D(1), price=D(100), quote_quantity=D(100)),
        fill("2", quantity=D(2), price=D(200), quote_quantity=D(400)),
        fill("3", side=Side.SELL, quantity=D(1), price=D(100), quote_quantity=D(100)),
        fill("4", side=Side.SELL, quantity=D(1), price=D(2000), quote_quantity=D(2000)),
    )
    before = context_state()
    with localcontext() as ambient:
        ambient.prec = 3
        ambient.clear_flags()
        original_ambient = context_state()
        for observed in fills:
            assert Fraction(observed.quote_quantity) == Fraction(observed.quantity) * Fraction(
                observed.price
            )
            assert owner.apply(observed)
        assert context_state() == original_ambient
    assert context_state() == before
    assert owner.cash == D(2600) and owner.quantity == D(1)
    assert owner.fees == {"USDT": D(0)}
    assert Fraction(owner.realized_gross_pnl) - Fraction(owner.book_cost) == Fraction(1600)
    assert 256 < len(owner.realized_gross_pnl.as_tuple().digits) <= 512
    assert len(owner._fills) == 4
    with localcontext() as ambient:
        ambient.prec = 3
        ambient.clear_flags()
        original_ambient = context_state()
        assert owner.apply(fill("5", side=Side.SELL, price=D(500), quote_quantity=D(500)))
        assert context_state() == original_ambient
    assert context_state() == before
    assert owner.cash == D(3100) and owner.quantity == owner.book_cost == 0
    assert owner.realized_gross_pnl == D(2100) and owner.base_fee_cost_in_quote == 0
    assert len(owner._fills) == 5


def test_full_close_transfers_all_recorded_basis_without_rounding_it_again():
    original_basis = D("1." + "1" * 256)
    owner = ledger(quantity=D(1), book_cost=original_basis)
    assert owner.apply(fill(side=Side.SELL, price=D(500), quote_quantity=D(500)))
    assert owner.quantity == owner.book_cost == 0 and owner.cash == D(1500)
    assert Fraction(owner.realized_gross_pnl) == Fraction(500) - Fraction(original_basis)
