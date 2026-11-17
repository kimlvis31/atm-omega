"""Exact normalized static rules; synthetic exchangeInfo, without HTTP or orders."""

from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.application.execution_rules import validate_execution_order
from omega.application.trading import validate_decision
from omega.domain.errors import ProtocolError, TradingBlocked, ValidationError
from omega.domain.execution_rules import DecimalRange, ExecutionRulesSnapshot, NotionalReference
from omega.domain.models import Instrument, Market, OrderKind, Side
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
)
from omega.infrastructure.binance.execution_rules import rules_snapshot
from omega.infrastructure.binance.market import SymbolRules

NOW = 900100
SPOT = Instrument("binance", Market.SPOT, "ETHUSDT")
USDM = Instrument("binance", Market.USDM, "ETHUSDT")


def symbol(instrument=SPOT, *, extra=(), quantity=None, price=None, status="TRADING"):
    return SymbolRules(
        instrument,
        "ETH",
        "USDT",
        status,
        (
            quantity
            or {"filterType": "LOT_SIZE", "minQty": ".0005", "maxQty": "10", "stepSize": ".0005"},
            price
            or {
                "filterType": "PRICE_FILTER",
                "minPrice": "0",
                "maxPrice": "10000",
                "tickSize": ".005",
            },
            *extra,
        ),
    )


def snapshot(**kwargs):
    return rules_snapshot(symbol(**kwargs), observed_at_ms=NOW)


def check(proposal, rules=None, **kwargs):
    rules = rules or snapshot()
    return validate_execution_order(
        rules.instrument, proposal, rules, now_ms=kwargs.pop("now_ms", NOW), **kwargs
    )


def test_actual_symbol_step_is_not_the_paper_default_and_maximum_is_enforced():
    rules = snapshot()
    proposal = OrderProposal(Side.BUY, D(".0015"), OrderKind.LIMIT, D(".015"))
    assert check(proposal, rules).fresh
    # Reproduce the separate, still-unwired host gap: its generic default rejects this valid grid.
    context = TradeContext(
        SPOT, D(".015"), 60000, PositionView(), BudgetView(D(100), D(100)), ExecutionConstraints()
    )
    with pytest.raises(ValidationError):
        validate_decision(context, TradeDecision((proposal,), "synthetic_static_grid"))
    with pytest.raises(ValidationError, match="range"):
        check(replace(proposal, quantity=D(11)), rules)


def test_market_and_regular_quantity_rules_both_apply_with_zero_extra_step():
    rules = snapshot(
        extra=({"filterType": "MARKET_LOT_SIZE", "minQty": "0", "maxQty": "2", "stepSize": "0"},)
    )
    assert check(OrderProposal(Side.BUY, D(".0015")), rules).fresh
    with pytest.raises(ValidationError):
        check(OrderProposal(Side.BUY, D(".0014")), rules)
    with pytest.raises(ValidationError):
        check(OrderProposal(Side.BUY, D(3)), rules)


def test_market_extra_grid_does_not_replace_regular_lot_and_limit_ignores_extra_grid():
    rules = snapshot(
        quantity={"filterType": "LOT_SIZE", "minQty": "0", "maxQty": "10", "stepSize": ".002"},
        extra=(
            {"filterType": "MARKET_LOT_SIZE", "minQty": "0", "maxQty": "10", "stepSize": ".003"},
        ),
    )
    with pytest.raises(ValidationError):
        check(OrderProposal(Side.BUY, D(".004")), rules)
    assert check(OrderProposal(Side.BUY, D(".006")), rules).fresh
    assert check(OrderProposal(Side.BUY, D(".004"), OrderKind.LIMIT, D(100)), rules).fresh


def test_futures_grid_origin_matches_documented_minimum_offset():
    rules = snapshot(
        instrument=USDM,
        quantity={"filterType": "LOT_SIZE", "minQty": ".003", "maxQty": "10", "stepSize": ".002"},
        price={
            "filterType": "PRICE_FILTER",
            "minPrice": ".01",
            "maxPrice": "10000",
            "tickSize": ".02",
        },
    )
    assert check(OrderProposal(Side.BUY, D(".005"), OrderKind.LIMIT, D(".03")), rules).fresh
    with pytest.raises(ValidationError):
        check(OrderProposal(Side.BUY, D(".004"), OrderKind.LIMIT, D(".02")), rules)


def notional(*, apply=True, maximum=None):
    return (
        {
            "filterType": "MIN_NOTIONAL",
            "minNotional": "10",
            "applyToMarket": apply,
            "avgPriceMins": 5,
        }
        if maximum is None
        else {
            "filterType": "NOTIONAL",
            "minNotional": "10",
            "maxNotional": maximum,
            "applyMinToMarket": apply,
            "applyMaxToMarket": True,
            "avgPriceMins": 5,
        }
    )


def reference(
    price="6000", *, instrument=SPOT, kind="provider_average", window=300000, observed=NOW
):
    return NotionalReference(
        instrument, D(price), kind, window, observed, "public:provider-reference"
    )


def test_market_minimum_needs_matching_actual_reference_not_a_candle_close():
    rules = snapshot(extra=(notional(),))
    order = OrderProposal(Side.BUY, D(".002"))
    with pytest.raises(TradingBlocked, match="provider reference"):
        check(order, rules)
    with pytest.raises(TradingBlocked):
        check(order, rules, references=(reference(kind="last_trade", window=0),))
    assert check(order, rules, references=(reference(),)).unchecked == ()
    with pytest.raises(ValidationError, match="notional"):
        check(order, rules, references=(reference("4000"),))
    with pytest.raises(ValidationError):
        check(order, rules, references=(reference(instrument=USDM),))


def test_market_notional_applicability_and_maximum_preserve_limit_rules():
    rules = snapshot(extra=(notional(apply=False, maximum="100"),))
    order = OrderProposal(Side.BUY, D(".002"))
    assert check(order, rules, references=(reference("4000"),)).fresh  # market minimum disabled
    with pytest.raises(ValidationError):
        check(order, rules, references=(reference("60000"),))
    with pytest.raises(ValidationError):
        check(replace(order, kind=OrderKind.LIMIT, price=D(100)), rules)


def test_usdm_reduce_minimum_exemption_is_explicit_and_never_disables_maximum():
    rules = snapshot(instrument=USDM, extra=({"filterType": "MIN_NOTIONAL", "notional": "200"},))
    order = OrderProposal(Side.SELL, D(".001"), reducing=True)
    assert check(order, rules).unchecked == ()
    with pytest.raises(TradingBlocked):
        check(replace(order, reducing=False), rules)
    with pytest.raises(ValidationError):
        check(replace(order, quantity=D(11)), rules)


def test_unknown_filter_blocks_strict_entry_and_is_visible_for_static_mode_or_reduction():
    rules = snapshot(
        extra=(
            {"filterType": "PERCENT_PRICE", "multiplierUp": "1.2"},
            {"filterType": "FUTURE_RULE"},
        )
    )
    order = OrderProposal(Side.BUY, D(1))
    with pytest.raises(TradingBlocked, match="supported"):
        check(order, rules)
    approximate = check(order, rules, strict=False)
    assert approximate.unchecked == (
        "unsupported_filter:FUTURE_RULE",
        "unsupported_filter:PERCENT_PRICE",
    )
    reduced = check(replace(order, side=Side.SELL, reducing=True), rules)
    assert reduced.unchecked == approximate.unchecked


def test_freshness_uses_actual_clock_and_retrospective_does_not_claim_past_rules():
    rules = snapshot()
    order = OrderProposal(Side.BUY, D(1))
    result = check(order, rules, now_ms=NOW + 1, retrospective=True)
    assert result.retrospective and result.snapshot_hash == rules.content_hash
    with pytest.raises(TradingBlocked, match="ahead"):
        check(order, rules, now_ms=NOW - 1)
    with pytest.raises(TradingBlocked, match="fresh"):
        check(order, rules, now_ms=NOW + 60001)
    stale_exit = check(replace(order, side=Side.SELL, reducing=True), rules, now_ms=NOW + 60001)
    assert not stale_exit.fresh and stale_exit.unchecked == ("stale_rules_reduction",)
    with pytest.raises(TradingBlocked):
        check(replace(order, side=Side.SELL, reducing=True), rules, now_ms=NOW - 1)


def test_nontrading_reduction_requires_explicit_status_policy():
    order = OrderProposal(Side.SELL, D(1), reducing=True)
    halted = symbol(status="HALT")
    rules = rules_snapshot(halted, observed_at_ms=NOW)
    with pytest.raises(TradingBlocked, match="status"):
        check(order, rules)
    policy = rules_snapshot(halted, observed_at_ms=NOW, reduction_statuses=("HALT",))
    assert check(order, policy).fresh
    with pytest.raises(TradingBlocked):
        check(replace(order, side=Side.BUY, reducing=False), policy)


def test_reduction_missing_average_stays_explicitly_unchecked_without_fake_price():
    rules = snapshot(extra=(notional(),))
    result = check(OrderProposal(Side.SELL, D(1), reducing=True), rules)
    assert result.unchecked == ("missing_market_reference:provider_average:300000",)


@pytest.mark.parametrize(
    "damage", ["duplicate", "missing", "numeric_float", "negative", "bad_flag"]
)
def test_malformed_filters_do_not_become_a_fallback_snapshot(damage):
    rules = symbol(extra=(notional(),))
    rows = [dict(row) for row in rules.filters]
    if damage == "duplicate":
        rows.append(rows[0])
    elif damage == "missing":
        rows.pop(0)
    elif damage == "numeric_float":
        rows[0]["stepSize"] = 0.001
    elif damage == "negative":
        rows[0]["minQty"] = "-1"
    else:
        rows[-1]["applyToMarket"] = 1
    with pytest.raises(ProtocolError):
        rules_snapshot(replace(rules, filters=tuple(rows)), observed_at_ms=NOW)


def test_snapshot_is_detached_and_hash_is_content_not_wire_or_observation_time():
    raw = symbol()
    rules = rules_snapshot(raw, observed_at_ms=NOW)
    digest = rules.content_hash
    raw.filters[0]["stepSize"] = "1"
    assert rules.content_hash == digest and rules.quantity.step == D(".0005")
    assert replace(rules, observed_at_ms=NOW + 1).content_hash == digest
    # Explicit policy: Decimal scale is preserved, so 1 versus 1.0 hashes differ.
    assert (
        replace(rules, quantity=replace(rules.quantity, maximum=D("10.0"))).content_hash != digest
    )
    public = rules.document()
    public["quantity"]["step"] = "corrupted copy"
    assert rules.quantity.step == D(".0005") and rules.content_hash == digest
    with pytest.raises(FrozenInstanceError):
        rules.status = "HALT"


def test_exact_grid_and_product_ignore_ambient_low_decimal_precision():
    rules = snapshot(extra=(notional(apply=False),))
    order = OrderProposal(Side.BUY, D(".0015"), OrderKind.LIMIT, D(10000))
    with localcontext() as context:
        context.prec = 2
        assert check(order, rules).fresh
    with pytest.raises(ValidationError):
        DecimalRange(step=D("1e-1000"))


def test_wrong_or_future_reference_does_not_satisfy_market_notional():
    rules = snapshot(extra=(notional(),))
    order = OrderProposal(Side.BUY, D(".002"))
    with pytest.raises(TradingBlocked):
        check(order, rules, references=(reference(observed=NOW + 1),))
    with pytest.raises(TradingBlocked):
        check(order, rules, references=(reference(observed=NOW - 60001),))
    with pytest.raises(ValidationError):
        check(order, rules, references=(reference(), reference()))


def test_document_roundtrip_preserves_scaled_decimals_identity_and_optional_rules():
    rules = snapshot(
        extra=(
            notional(),
            {"filterType": "MARKET_LOT_SIZE", "minQty": "0", "maxQty": "2.00", "stepSize": "0"},
        )
    )
    document = rules.document()
    restored = ExecutionRulesSnapshot.from_document(document)
    assert restored == rules and restored.document() == document
    document["quantity"]["step"] = "1"
    assert restored.quantity.step == D(".0005")


@pytest.mark.parametrize(
    "damage",
    [
        "unknown",
        "schema_bool",
        "clock_bool",
        "decimal_int",
        "decimal_nonfinite",
        "hash",
        "extra_range",
        "notional_bool",
        "tuple_fields",
        "noncanonical",
    ],
)
def test_document_rejects_unknown_types_hash_mismatch_and_noncanonical_values(damage):
    document = deepcopy(snapshot(extra=(notional(),)).document())
    if damage == "unknown":
        document["provider_filter"] = {}
    elif damage == "schema_bool":
        document["schema_version"] = True
    elif damage == "clock_bool":
        document["observed_at_ms"] = True
    elif damage == "decimal_int":
        document["quantity"]["minimum"] = 1
    elif damage == "decimal_nonfinite":
        document["quantity"]["minimum"] = "NaN"
    elif damage == "hash":
        document["content_hash"] = "0" * 64
    elif damage == "extra_range":
        document["price"]["precision"] = 8
    elif damage == "notional_bool":
        document["notionals"][0]["apply_min_to_market"] = 1
    elif damage == "tuple_fields":
        document["unsupported_filters"] = ()
    else:
        document["quantity"]["minimum"] = "0_0.0005"
    with pytest.raises(ValidationError):
        ExecutionRulesSnapshot.from_document(document)
