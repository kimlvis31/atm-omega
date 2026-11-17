"""Legacy next-bar market/limit price and conservative timing reference."""

from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.application.paper_fill import resolve_bar_fill
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side
from omega.domain.trading import OrderProposal

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def candle():
    return Candle(BTC, 60000, 120000, D(100), D(120), D(80), D(110), D(10))


@pytest.mark.parametrize(
    "side,limit,price,phase",
    [
        (Side.BUY, None, "100", "open"),
        (Side.SELL, None, "100", "open"),
        (Side.BUY, "110", "100", "open"),
        (Side.SELL, "90", "100", "open"),
        (Side.BUY, "100", "100", "open"),
        (Side.SELL, "100", "100", "open"),
        (Side.BUY, "90", "90", "close"),
        (Side.SELL, "110", "110", "close"),
        (Side.BUY, "80", "80", "close"),
        (Side.SELL, "120", "120", "close"),
    ],
)
def test_market_and_limit_quote_price_phase_and_time_match_legacy_fill(side, limit, price, phase):
    proposal = OrderProposal(
        side, D(1), OrderKind.LIMIT if limit else OrderKind.MARKET, D(limit) if limit else None
    )
    quote = resolve_bar_fill(proposal, candle(), 60000)
    assert quote.price == D(price) and quote.phase == phase
    assert quote.event_time_ms == (60000 if phase == "open" else 120000)


@pytest.mark.parametrize("side,limit", [(Side.BUY, "79"), (Side.SELL, "121")])
def test_untouched_limit_has_no_quote(side, limit):
    assert (
        resolve_bar_fill(OrderProposal(side, D(1), OrderKind.LIMIT, D(limit)), candle(), 60000)
        is None
    )


def test_eligible_minute_prevents_fill_from_the_previous_bar_wick():
    order = OrderProposal(Side.BUY, D(1), OrderKind.LIMIT, D(90))
    assert resolve_bar_fill(order, candle(), 120000) is None
    later = replace(candle(), open_time_ms=120000, close_time_ms=180000)
    assert resolve_bar_fill(order, later, 120000).price == 90


def test_subminute_latency_waits_for_first_later_bar_open():
    order = OrderProposal(Side.BUY, D(1))
    assert resolve_bar_fill(order, candle(), 60001) is None
    later = replace(candle(), open_time_ms=120000, close_time_ms=180000)
    assert resolve_bar_fill(order, later, 60001).event_time_ms == 120000


@pytest.mark.parametrize("eligible", [True, -60000, 253402300800000])
def test_invalid_eligibility_is_rejected(eligible):
    with pytest.raises(ValidationError):
        resolve_bar_fill(OrderProposal(Side.BUY, D(1)), candle(), eligible)
