"""Exact TS-neutral grids and builtin TS sizing; no provider I/O or account orders."""

from dataclasses import replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.application.execution_rules import sizing_constraints, validate_execution_order
from omega.application.trading import validate_decision
from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.execution_rules import DecimalRange, ExecutionRulesSnapshot, NotionalRule
from omega.domain.models import Instrument, Market, OrderKind, Side
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
    close_position,
    floor_to_step,
    limit_price,
)
from omega.strategies.target_quantity.module import TargetQuantityModule
from omega.strategies.tef.module import TefModule

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def rules(**changes):
    return replace(
        ExecutionRulesSnapshot(
            INSTRUMENT,
            900100,
            "public:synthetic-rules",
            "TRADING",
            DecimalRange(D(".001"), D(100), D(".004"), D(".001")),
            DecimalRange(D(".003"), D(100), D(".006"), D(".003")),
            DecimalRange(D(".01"), D(10000), D(".02"), D(".01")),
            reductions_allowed=True,
        ),
        **changes,
    )


def context(constraints=None, *, position=None, price="100"):
    return TradeContext(
        INSTRUMENT,
        D(price),
        900100,
        position or PositionView(),
        BudgetView(D(10000), D(10000)),
        constraints or sizing_constraints(rules()),
    )


def test_old_six_positional_fields_and_helper_signatures_keep_default_numeric_and_scale():
    default = ExecutionConstraints(D(".001"), D(".01"), D(".001"), D(0), D(1), True)
    assert default.quantity_grid() == (D(".001"), D(0), D(".001"))
    assert str(floor_to_step(D("1.234567890"), D(".001"))) == "1.234"
    assert str(default.floor_quantity(D("1.234567890"))) == "1.234"
    assert limit_price(D("1.234"), D(".01"), Side.BUY) == D("1.23")
    assert limit_price(D("1.234"), D(".01"), Side.SELL) == D("1.24")


def test_market_quantity_uses_exact_crt_intersection_without_risk_increasing_roundup():
    constraint = sizing_constraints(rules())
    assert constraint.quantity_grid() == (D(".012"), D(".009"), D(".003"))
    assert constraint.floor_quantity(D(".020")) == D(".009")
    assert constraint.floor_quantity(D(".020"), OrderKind.LIMIT) == D(".017")
    assert constraint.floor_quantity(D(".005")) == 0
    assert constraint.floor_quantity(D(".001")) == 0


@pytest.mark.parametrize(
    ("first", "a", "second", "b"), [(4, 1, 6, 3), (2, 1, 3, 0), (3, 0, 6, 3), (4, 0, 2, 0)]
)
def test_crt_floor_matches_independently_enumerated_valid_units(first, a, second, b):
    constraint = ExecutionConstraints(
        quantity_step=D(first),
        min_quantity=D(0),
        quantity_origin=D(a),
        market_quantity_step=D(second),
        market_quantity_origin=D(b),
    )
    for target in range(1, 61):
        valid = [
            value
            for value in range(1, target + 1)
            if (value - a) % first == 0 and (value - b) % second == 0
        ]
        assert constraint.floor_quantity(D(target)) == D(max(valid, default=0))


def test_incompatible_crt_and_representation_overflow_are_explicitly_unsupported():
    with pytest.raises(ValidationError, match="intersection"):
        ExecutionConstraints(
            quantity_step=D(".004"),
            market_quantity_step=D(".006"),
            market_quantity_origin=D(".001"),
        )
    with pytest.raises(ValidationError, match="bound"):
        ExecutionConstraints(
            quantity_step=D("1" + "0" * 78 + "1"), market_quantity_step=D("1" + "0" * 78 + "3")
        )


def test_market_zero_extra_grid_keeps_regular_grid_and_its_distinct_minimum():
    constraint = ExecutionConstraints(
        quantity_step=D(".005"),
        min_quantity=D(".005"),
        market_quantity_step=D(0),
        market_min_quantity=D(".02"),
    )
    assert constraint.floor_quantity(D(".019")) == 0
    assert constraint.floor_quantity(D(".023")) == D(".020")
    assert constraint.floor_quantity(D(".019"), OrderKind.LIMIT) == D(".015")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("quantity_origin", True),
        ("price_origin", D(-1)),
        ("market_quantity_step", 1),
        ("market_quantity_step", D(-1)),
        ("market_quantity_origin", 0.001),
        ("market_min_quantity", True),
    ],
)
def test_new_constraint_fields_reject_bool_float_and_negative_values_atomically(field, value):
    with pytest.raises(ValidationError):
        ExecutionConstraints(**{field: value})


def test_zero_regular_step_or_tick_never_becomes_an_invented_default():
    for snapshot in [rules(quantity=DecimalRange()), rules(price=DecimalRange())]:
        with pytest.raises(ValidationError, match="unsupported"):
            sizing_constraints(snapshot)


def test_price_origin_applies_to_limit_and_half_even_protection_rounding():
    constraint = sizing_constraints(rules())
    assert constraint.limit_price(D("1.02"), Side.BUY) == D("1.01")
    assert constraint.limit_price(D("1.02"), Side.SELL) == D("1.03")
    assert constraint.round_price(D("1.02")) == D("1.01")
    assert constraint.round_price(D("1.04")) == D("1.05")
    ctx = context(constraint, position=PositionView(D(1), D(100)))
    assert TefModule._stop_reached(ctx, "LONG", D(".05"), D("95.005"))
    assert not TefModule._stop_reached(ctx, "LONG", D(".05"), D("95.015"))


def test_tef_selects_final_adaptive_or_protective_kind_before_flooring_quantity():
    module = TefModule(INSTRUMENT, {"order_type": "ADAPTIVE"})
    ctx = context()
    limit = module._order(ctx, Side.BUY, D(".020"), reducing=False, reason="ENTRY").orders[0]
    clear = module._order(ctx, Side.SELL, D(".020"), reducing=True, reason="CLEAR").orders[0]
    stop = module._order(ctx, Side.SELL, D(".020"), reducing=True, reason="FSLIMMED").orders[0]
    assert limit.kind == OrderKind.LIMIT and limit.quantity == D(".017")
    assert clear.kind == stop.kind == OrderKind.MARKET and clear.quantity == stop.quantity == D(
        ".009"
    )
    assert ctx.constraints.limit_price(limit.price, Side.BUY) == limit.price


def test_target_and_common_close_use_market_grid_preserving_subgrid_dust():
    target = TargetQuantityModule(INSTRUMENT, {"quantity": ".020"})
    assert target.evaluate(context()).orders[0].quantity == D(".009")
    held = context(position=PositionView(D(".020"), D(100)))
    assert close_position(held, "synthetic_exit").orders[0].quantity == D(".009")
    dust = context(position=PositionView(D(".005"), D(100)))
    assert not close_position(dust, "synthetic_dust").orders


def test_common_validator_checks_order_kind_grid_and_price_origin():
    ctx = context()
    valid_limit = OrderProposal(Side.BUY, D(".017"), OrderKind.LIMIT, D("100.01"))
    validate_decision(ctx, TradeDecision((valid_limit,), "synthetic_limit"))
    with pytest.raises(ValidationError):
        validate_decision(
            ctx, TradeDecision((replace(valid_limit, price=D(100)),), "bad_price_grid")
        )
    with pytest.raises(ValidationError):
        validate_decision(
            ctx, TradeDecision((OrderProposal(Side.BUY, D(".017")),), "bad_market_grid")
        )


def test_sizing_does_not_clamp_maximum_or_substitute_close_for_market_notional():
    snapshot = rules(
        quantity=DecimalRange(D(".001"), D(".01"), D(".001")),
        market_quantity=None,
        notionals=(NotionalRule(D(10), apply_min_to_market=True, reference_kind="mark_price"),),
    )
    constraint = sizing_constraints(snapshot)
    assert constraint.floor_quantity(D(1)) == D(1) and constraint.min_notional == 0
    with pytest.raises(ValidationError, match="range"):
        validate_execution_order(INSTRUMENT, OrderProposal(Side.BUY, D(1)), snapshot, now_ms=900100)
    with pytest.raises(TradingBlocked, match="reference"):
        validate_execution_order(
            INSTRUMENT, OrderProposal(Side.BUY, D(".001")), snapshot, now_ms=900100
        )


def test_sizing_factory_is_not_a_status_freshness_or_spot_credit_permission():
    halted = rules(status="HALT", reductions_allowed=False)
    sizing_constraints(halted)
    with pytest.raises(TradingBlocked):
        validate_execution_order(
            INSTRUMENT, OrderProposal(Side.BUY, D(".009")), halted, now_ms=900100
        )
    spot = replace(rules(), instrument=Instrument("binance", Market.SPOT, "ETHUSDT"))
    with pytest.raises(ValidationError, match="leverage"):
        sizing_constraints(spot, D(2))


def test_low_ambient_decimal_context_cannot_round_grid_or_stop_price_inputs():
    ctx = context(position=PositionView(D(1), D(100)))
    with localcontext() as calculation:
        calculation.prec = 2
        constraint = sizing_constraints(rules())
        assert constraint.floor_quantity(D(".020123456789123456789")) == D(".009")
        assert floor_to_step(D("1.23456789123456789"), D(".001"), D(".0005")) == D("1.2345")
        assert limit_price(D("1.02456789123456789"), D(".02"), Side.SELL, D(".01")) == D("1.03")
        assert TefModule._stop_reached(ctx, "LONG", D(".050123"), D("94.985"))
