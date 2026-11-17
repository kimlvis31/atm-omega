"""Real TEF sizing keeps exact affordable lot limits across Decimal contexts."""

from dataclasses import replace
from decimal import Decimal, getcontext, localcontext
from fractions import Fraction

import pytest

from omega.application.trading import TradingEngine
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market, OrderKind
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext
from omega.strategies.tef.module import TefModule

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")
TARGET = Decimal("0.761594")  # The real default MMACD model's tanh(1) output.


def sample(*, position=None, available="10", constraints=None, price="114.49"):
    value = Decimal(price)
    candle = Candle(INSTRUMENT, 0, 60000, value, value, value, value, Decimal(1))
    return TradeContext(
        INSTRUMENT,
        value,
        60000,
        position or PositionView(),
        BudgetView(Decimal(100), Decimal(available)),
        constraints or ExecutionConstraints(leverage=Decimal(2), can_short=True),
        candle,
        {"0_MMACD_MSDELTAABSMAREL": 1.0},
    )


def affordable(sample, *, reducing):
    held = abs(Fraction(sample.position.quantity))
    leverage = Fraction(sample.constraints.leverage)
    entry = Fraction(sample.position.entry_price)
    difference = Fraction(sample.budget.allocated) * Fraction(TARGET) - held * entry / leverage
    if reducing:
        return min(held, -difference * leverage / entry)
    return min(difference, Fraction(sample.budget.available)) * leverage / Fraction(sample.price)


def assert_largest_affordable(sample, order):
    step, origin, minimum = sample.constraints.quantity_grid(order.kind)
    cap = affordable(sample, reducing=order.reducing)
    actual, unit, start = map(Fraction, (order.quantity, step, origin))
    assert actual <= cap < actual + unit
    assert (actual - start) % unit == 0
    assert order.quantity >= minimum
    if order.reducing:
        assert actual <= abs(Fraction(sample.position.quantity))
    else:
        cost = actual * Fraction(sample.price) / Fraction(sample.constraints.leverage)
        assert cost <= Fraction(sample.budget.available)
        assert cost <= Fraction(sample.budget.allocated) * Fraction(TARGET) - (
            abs(Fraction(sample.position.quantity))
            * Fraction(sample.position.entry_price)
            / Fraction(sample.constraints.leverage)
        )


@pytest.mark.parametrize("precision", [28, 80, 256, 512])
@pytest.mark.parametrize(
    "position,available,reason",
    [
        (PositionView(), "10", "ENTRY"),
        (PositionView(Decimal(1), Decimal(50), 1), "100", "ENTRY"),
        (PositionView(Decimal(2), Decimal("113.17"), 1), "0", "EXIT"),
    ],
)
def test_real_mmacd_nonterminating_entry_resize_and_downsize(
    precision, position, available, reason
):
    context = sample(position=position, available=available)
    outer = getcontext().copy()
    with localcontext() as calculation:
        calculation.prec = precision
        decision = TradingEngine(TefModule(INSTRUMENT)).evaluate(context)
        assert calculation.prec == precision
    assert str(getcontext()) == str(outer)
    assert decision.reason == reason
    assert len(decision.orders) == 1
    order = decision.orders[0]
    assert order.reducing == (reason == "EXIT")
    assert_largest_affordable(context, order)
    if not position.quantity:
        assert str(order.quantity) == "0.174"


@pytest.mark.parametrize("order_type", ["MARKET", "LIMIT", "ADAPTIVE"])
def test_nonzero_origin_and_combined_market_grid_use_the_actual_order_kind(order_type):
    context = sample(
        constraints=ExecutionConstraints(
            leverage=Decimal(2),
            can_short=True,
            quantity_step=Decimal("0.002"),
            quantity_origin=Decimal("0.001"),
            market_quantity_step=Decimal("0.003"),
            market_min_quantity=Decimal("0.009"),
        )
    )
    with localcontext() as calculation:
        calculation.prec = 256
        decision = TradingEngine(TefModule(INSTRUMENT, {"order_type": order_type})).evaluate(
            context
        )
    order = decision.orders[0]
    assert order.kind == (OrderKind.MARKET if order_type == "MARKET" else OrderKind.LIMIT)
    assert order.quantity == Decimal("0.171" if order_type == "MARKET" else "0.173")
    assert_largest_affordable(context, order)


def test_fractional_downsize_on_combined_nonzero_origin_never_exceeds_held_quantity():
    context = sample(
        position=PositionView(Decimal(2), Decimal("113.17"), 1),
        available="0",
        constraints=ExecutionConstraints(
            leverage=Decimal(2),
            quantity_step=Decimal("0.002"),
            quantity_origin=Decimal("0.001"),
            market_quantity_step=Decimal("0.003"),
        ),
    )
    with localcontext() as calculation:
        calculation.prec = 256
        decision = TradingEngine(TefModule(INSTRUMENT)).evaluate(context)
    assert decision.reason == "EXIT"
    assert decision.orders[0].reducing
    assert_largest_affordable(context, decision.orders[0])


@pytest.mark.parametrize("available,minimum", [("0.00001", "0.001"), ("10", "0.2")])
def test_fractional_dust_and_market_minimum_do_not_create_an_order(available, minimum):
    context = sample(
        available=available,
        constraints=ExecutionConstraints(leverage=Decimal(2), market_min_quantity=Decimal(minimum)),
    )
    with localcontext() as calculation:
        calculation.prec = 256
        decision = TradingEngine(TefModule(INSTRUMENT)).evaluate(context)
    assert not decision.orders
    assert decision.reason == "below_quantity_step"


def test_terminating_original_path_keeps_quantity_exponent_and_never_uses_fallback(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("bounded terminating quantity must retain its old path")

    monkeypatch.setattr(TefModule, "_exact_sizing", staticmethod(unexpected))
    for precision in (28, 80, 256, 512):
        with localcontext() as calculation:
            calculation.prec = precision
            decision = TradingEngine(TefModule(INSTRUMENT)).evaluate(sample(price="100"))
        assert str(decision.orders[0].quantity) == "0.2"
        assert decision.orders[0].quantity.as_tuple().exponent == -1


@pytest.mark.parametrize("field", ["budget", "position", "price"])
def test_over_bound_originals_fail_closed_without_committing_model(field):
    context = sample()
    if field == "budget":
        context = replace(context, budget=BudgetView(Decimal("1E100"), Decimal("1E100")))
    elif field == "position":
        context = replace(context, position=PositionView(Decimal("1E100"), Decimal(100), 1))
    else:
        context = sample(price="1E-100")
    engine = TradingEngine(TefModule(INSTRUMENT))
    original = engine.module.checkpoint()
    with localcontext() as calculation:
        calculation.prec = 256
        with pytest.raises(ValidationError, match="exact calculation bound"):
            engine.evaluate(context)
    assert engine.module.checkpoint() == original


@pytest.mark.parametrize(
    "target", [True, 1.0, "0.5", None, Fraction(1, 2), Decimal("NaN"), Decimal("Infinity")]
)
def test_rational_fallback_never_coerces_invalid_original_target(target):
    with pytest.raises(ValidationError):
        TefModule._exact_sizing(sample(), target, OrderKind.MARKET, reducing=False)


@pytest.mark.parametrize("target", [Decimal(2), Decimal(-2)])
def test_bounded_but_invalid_original_exposure_is_not_treated_as_additional_budget(target):
    with pytest.raises(ValidationError, match="invalid original TEF sizing inputs"):
        TefModule._exact_sizing(sample(), target, OrderKind.MARKET, reducing=False)


def test_over_bound_rational_result_is_not_admitted_by_widening_public_grid_limits():
    context = replace(
        sample(price="1E-60", available="1E60"),
        budget=BudgetView(Decimal("1E60"), Decimal("1E60")),
    )
    with pytest.raises(ValidationError, match="exact calculation bound"):
        TefModule._exact_sizing(context, TARGET, OrderKind.MARKET, reducing=False)
