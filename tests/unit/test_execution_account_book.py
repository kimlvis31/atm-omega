"""Factual asset accounting only; synthetic fills confer no trading permission."""

from dataclasses import replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.domain.errors import ValidationError
from omega.domain.execution_account import (
    ExecutionAccountOpening,
    apply_execution_fills,
    initial_execution_book,
)
from omega.domain.models import (
    AccountSnapshot,
    Balance,
    Fill,
    Instrument,
    Market,
    PositionSnapshot,
    Side,
)


def opening(market=Market.SPOT):
    instrument = Instrument("binance", market, "BTCUSDT")
    balances = tuple(
        Balance(asset, D(amount), D(amount))
        for asset, amount in (
            ("BTC", "0" if market == Market.SPOT else "1"),
            ("USDT", "1000"),
            ("BNB", "1"),
            ("FDUSD", "0"),
        )
    )
    return ExecutionAccountOpening(
        AccountSnapshot("synthetic-account", market, True, 0, balances),
        instrument,
        "BTC",
        "USDT",
        None if market == Market.SPOT else "FDUSD",
        0,
    )


def fills(anchor):
    # Reported quote deliberately differs from quantity*price. FDUSD is explicitly
    # synthetic settlement metadata, not inferred from the BTCUSDT symbol.
    return tuple(
        Fill(
            anchor.account.account_id,
            anchor.instrument,
            str(index),
            "7",
            Side.BUY,
            D(quantity),
            D(10000),
            D(fee),
            asset,
            120000 + index,
            quote_quantity=D(quote),
            realized_pnl=None if anchor.instrument.market == Market.SPOT else D(pnl),
            realized_pnl_asset=anchor.settlement_asset,
        )
        for index, (quantity, quote, fee, asset, pnl) in enumerate(
            (
                (".002", "20.0001", ".000002", "BTC", "1.25"),
                (".003", "30.0002", ".03", "USDT", "-.25"),
                (".002", "20.0003", ".00001", "BNB", "0"),
                (".003", "30.0004", "-.000002", "BNB", "0"),
            ),
            start=1,
        )
    )


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_reported_principal_asset_fees_rebate_and_pnl_are_exact_under_small_context(market):
    anchor = opening(market)
    observed = fills(anchor)
    results = []
    for precision in (6, 28, 80):
        with localcontext() as context:
            context.prec = precision
            results.append(apply_execution_fills(initial_execution_book(anchor), anchor, observed))
    assert results[0] == results[1] == results[2]
    book = results[0]
    assert book.revision == 4 and book.applied_fills == observed
    assert dict(book.fees) == {"BTC": D(".000002"), "USDT": D(".03"), "BNB": D(".000008")}
    assert book.wallet("BNB") == D(".999992") and book.wallet("ABSENT") is None
    if market == Market.SPOT:
        assert book.wallet("USDT") == D("899.9690")
        assert book.quantity == book.wallet("BTC") == D(".009998")
        assert book.book_cost == D("99.9809999")
        assert book.base_fee_cost_in_quote == D(".0200001")
        assert book.realized_gross_pnl == 0 and not book.realized_pnl
    else:
        assert book.wallet("USDT") == D("999.97")  # No futures gross-notional cash debit.
        assert book.wallet("BTC") == D(".999998") and book.quantity == D(".01")
        assert book.wallet("FDUSD") == D(1) and dict(book.realized_pnl) == {"FDUSD": D(1)}
        assert book.book_cost is book.realized_gross_pnl is book.base_fee_cost_in_quote is None


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_duplicates_are_noop_but_same_identity_changed_contents_cannot_rewrite_book(market):
    anchor = opening(market)
    first, second, *_ = fills(anchor)
    book = apply_execution_fills(initial_execution_book(anchor), anchor, (first, second))
    assert apply_execution_fills(book, anchor, (second, first, second)) == book
    with pytest.raises(ValidationError):
        apply_execution_fills(book, anchor, (replace(first, fee=D(".000003")),))
    assert book.revision == 2 and book.applied_fills == (first, second)


@pytest.mark.parametrize("missing", ["spot-quote", "futures-pnl", "fee-wallet", "pnl-wallet"])
def test_missing_reported_metadata_or_asset_is_not_silently_zero(missing):
    anchor = opening(Market.SPOT if missing == "spot-quote" else Market.USDM)
    fact = fills(anchor)[0]
    if missing == "spot-quote":
        fact = replace(fact, quote_quantity=None)
    elif missing == "futures-pnl":
        fact = replace(fact, realized_pnl=None, realized_pnl_asset=None)
    elif missing == "fee-wallet":
        fact = replace(fact, fee_asset="UNKNOWN", fee=D(".01"))
    else:
        fact = replace(fact, realized_pnl_asset="UNKNOWN", realized_pnl=D(0))
    book = initial_execution_book(anchor)
    with pytest.raises(ValidationError):
        apply_execution_fills(book, anchor, (fact,))
    assert book == initial_execution_book(anchor) and not book.applied_fills


@pytest.mark.parametrize("change", ["account", "instrument", "old-new-id"])
def test_original_account_instrument_and_financial_order_cannot_be_reinterpreted(change):
    anchor = opening()
    first, second, *_ = fills(anchor)
    book = apply_execution_fills(initial_execution_book(anchor), anchor, (second,))
    wrong = (
        replace(first, account_id="other")
        if change == "account"
        else replace(first, instrument=Instrument("binance", Market.SPOT, "ETHUSDT"))
        if change == "instrument"
        else first
    )
    with pytest.raises(ValidationError):
        apply_execution_fills(book, anchor, (wrong,))
    assert book.applied_fills == (second,) and book.revision == 1


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_opening_requires_explicit_flat_instrument_not_an_invented_old_cost_basis(market):
    anchor = opening(market)
    if market == Market.SPOT:
        balances = tuple(
            replace(balance, wallet=D(1), available=D(1)) if balance.asset == "BTC" else balance
            for balance in anchor.account.balances
        )
        account = replace(anchor.account, balances=balances)
    else:
        account = replace(
            anchor.account,
            positions=(PositionSnapshot(anchor.instrument, D(1), D(100), D(0)),),
        )
    with pytest.raises(ValidationError):
        initial_execution_book(replace(anchor, account=account))


def test_applied_identity_capacity_holds_without_truncating_already_committed_facts():
    anchor = opening()
    template = fills(anchor)[0]
    facts = tuple(
        replace(
            template,
            trade_id=str(index),
            timestamp_ms=120000 + index,
            quantity=D(".0001"),
            price=D(100),
            quote_quantity=D(".01"),
            fee=D(0),
            fee_asset="USDT",
        )
        for index in range(1, 130)
    )
    book = apply_execution_fills(initial_execution_book(anchor), anchor, facts[:128])
    with pytest.raises(ValidationError):
        apply_execution_fills(book, anchor, facts[128:])
    assert book.revision == len(book.applied_fills) == 128
    assert book.applied_fills == facts[:128] and book.wallet("USDT") == D("998.72")


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_owned_reduction_books_actual_proceeds_or_reported_pnl_without_mixing_asset_units(market):
    anchor = opening(market)
    template = fills(anchor)[0]
    entry = replace(
        template,
        quantity=D(2),
        price=D(100),
        quote_quantity=D(200),
        fee=D(0),
        fee_asset="USDT",
        realized_pnl=None if market == Market.SPOT else D(0),
    )
    reduction = replace(
        entry,
        trade_id="2",
        provider_order_id="8",
        side=Side.SELL,
        timestamp_ms=120002,
        quantity=D(1),
        price=D(110),
        quote_quantity=D("110.0001"),
        fee=D("-.001"),
        fee_asset="BNB",
        realized_pnl=None if market == Market.SPOT else D("10.0001"),
    )
    book = apply_execution_fills(initial_execution_book(anchor), anchor, (entry, reduction))
    assert book.quantity == 1 and book.wallet("BNB") == D("1.001")
    if market == Market.SPOT:
        assert book.wallet("USDT") == D("910.0001")
        assert book.book_cost == 100 and book.realized_gross_pnl == D("10.0001")
    else:
        assert book.wallet("USDT") == 1000 and book.wallet("FDUSD") == D("10.0001")
        assert book.book_cost is book.realized_gross_pnl is None


def test_pure_reducer_rejects_original_fill_older_than_explicit_flat_opening():
    anchor = opening()
    anchor = replace(anchor, account=replace(anchor.account, observed_at_ms=120002))
    original = fills(anchor)[0]
    assert original.timestamp_ms == 120001 < anchor.account.observed_at_ms
    book = initial_execution_book(anchor)
    with pytest.raises(ValidationError):
        apply_execution_fills(book, anchor, (original,))
    assert book.revision == 0 and book.applied_fills == ()
