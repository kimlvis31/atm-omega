"""Pure decision compilation, original identity and risk/rule boundaries; no brokers or IO."""

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D

import pytest

from omega.application.execution_intents import (
    ExecutionCycle,
    ExecutionDecision,
    ExecutionIntent,
    prepare_execution_decision,
)
from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.execution_rules import (
    MAX_TIME,
    DecimalRange,
    ExecutionRulesSnapshot,
    NotionalReference,
    NotionalRule,
)
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
)

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
SPOT = Instrument("binance", Market.SPOT, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")


def cycle(**changes):
    return replace(ExecutionCycle("account-1", "strategy-1", "closed-0", "a" * 64), **changes)


def context(**changes):
    return replace(
        TradeContext(
            BTC,
            D(100),
            60000,
            PositionView(),
            BudgetView(D(1000), D(1000)),
            ExecutionConstraints(can_short=True),
        ),
        **changes,
    )


def decision(**changes):
    return TradeDecision((OrderProposal(Side.BUY, D(".125"), **changes),), "entry")


def rules(**changes):
    return replace(
        ExecutionRulesSnapshot(
            BTC,
            60000,
            "synthetic_public_rules",
            "TRADING",
            DecimalRange(D(0), D(10), D(".001")),
            None,
            DecimalRange(D(0), D(10000), D(".01")),
            reductions_allowed=True,
        ),
        **changes,
    )


def test_stable_request_identity_is_namespaced_and_the_fresh_request_is_immutable():
    result = prepare_execution_decision(cycle(), context(), decision())
    assert result.intents[0].request.client_order_id == "om_d605e0f8b266cc81bed3ff37c75abd6d"
    assert result.intents[0].request == result.intents[0].request
    assert result.intents[0].request is not result.intents[0].request
    assert result.intents[0].reducing is False and result.intents[0].request.reduce_only is False
    assert prepare_execution_decision(cycle(), context(), decision(), original=result) == result
    with pytest.raises(FrozenInstanceError):
        result.reason = "changed"
    with pytest.raises(FrozenInstanceError):
        result.intents[0].request.quantity = D(2)


@pytest.mark.parametrize(
    "change",
    [
        {"account_id": "account-2"},
        {"strategy_id": "strategy-2"},
        {"cycle_id": "closed-1"},
    ],
)
def test_account_strategy_and_cycle_have_distinct_request_namespaces(change):
    baseline = prepare_execution_decision(cycle(), context(), decision())
    candidate = prepare_execution_decision(cycle(**change), context(), decision())
    assert (
        candidate.intents[0].request.client_order_id != baseline.intents[0].request.client_order_id
    )


def test_instrument_and_ordinal_are_distinct_without_random_or_clock_identity():
    proposal = decision().orders[0]
    ids = {
        ExecutionIntent(cycle(), instrument, ordinal, proposal).request.client_order_id
        for instrument, ordinal in ((BTC, 0), (BTC, 1), (ETH, 0))
    }
    assert len(ids) == 3


@pytest.mark.parametrize("change", ["quantity", "source", "reason"])
def test_changed_request_or_source_keeps_id_but_original_decision_rejects_replacement(change):
    original = prepare_execution_decision(cycle(), context(), decision())
    next_cycle, next_decision = cycle(), decision()
    if change == "quantity":
        next_decision = TradeDecision((replace(decision().orders[0], quantity=D(".25")),), "entry")
    elif change == "source":
        next_cycle = cycle(source_digest="b" * 64)
    else:
        next_decision = replace(next_decision, reason="changed_reason")
    replacement = prepare_execution_decision(next_cycle, context(), next_decision)
    assert (
        replacement.intents[0].request.client_order_id
        == original.intents[0].request.client_order_id
    )
    with pytest.raises(ValidationError, match="another source or decision"):
        prepare_execution_decision(next_cycle, context(), next_decision, original=original)
    assert original.intents[0].request.quantity == D(".125")


def test_an_original_cycle_cannot_be_reinterpreted_as_the_next_cycle():
    original = prepare_execution_decision(cycle(), context(), decision())
    with pytest.raises(ValidationError, match="another source or decision"):
        prepare_execution_decision(cycle(cycle_id="next"), context(), decision(), original=original)


def test_decimal_grids_are_validated_without_rounding_quantity_price_or_scale():
    constraints = ExecutionConstraints(
        quantity_step=D(".002"),
        quantity_origin=D(".001"),
        price_tick=D(".02"),
        price_origin=D(".01"),
    )
    proposal = OrderProposal(Side.BUY, D(".0050"), OrderKind.LIMIT, D("100.030"))
    result = prepare_execution_decision(
        cycle(),
        context(constraints=constraints),
        TradeDecision((proposal,)),
    )
    request = result.intents[0].request
    assert str(request.quantity) == "0.0050"
    assert str(request.price) == "100.030"
    with pytest.raises(ValidationError, match="step or minimum"):
        prepare_execution_decision(
            cycle(),
            context(constraints=constraints),
            TradeDecision((replace(proposal, quantity=D(".004")),)),
        )
    with pytest.raises(ValidationError, match="tick"):
        prepare_execution_decision(
            cycle(),
            context(constraints=constraints),
            TradeDecision((replace(proposal, price=D("100.02")),)),
        )


def test_hold_has_no_request_and_keeps_the_existing_uncertain_or_paused_context_inert():
    sample = context(entries_allowed=False, state_certain=False, control_owned=False)
    result = prepare_execution_decision(cycle(), sample, TradeDecision(reason="waiting"))
    assert result == ExecutionDecision(cycle(), (), None, "waiting")
    assert result.cancel_client_order_id is None and sample.entries_allowed is False


def test_cancel_uses_original_pending_id_instead_of_hashing_the_current_cycle():
    pending = prepare_execution_decision(cycle(), context(), decision()).intents[0]
    current = cycle(cycle_id="cancel-1", source_digest="b" * 64)
    result = prepare_execution_decision(
        current,
        context(has_pending_order=True, entries_allowed=False),
        TradeDecision(reason="cancel_original", cancel_pending=True),
        pending_intent=pending,
    )
    assert result.intents == () and result.cancel_intent is pending
    assert result.cancel_client_order_id == pending.request.client_order_id
    assert (
        result.cancel_client_order_id
        != ExecutionIntent(current, BTC, 0, pending.proposal).request.client_order_id
    )


@pytest.mark.parametrize("change", ["missing", "account", "strategy", "instrument", "not_pending"])
def test_cancel_requires_original_owner_instrument_and_pending_state(change):
    pending = ExecutionIntent(cycle(), BTC, 0, decision().orders[0])
    sample = context(has_pending_order=True)
    if change == "missing":
        pending = None
    elif change == "account":
        pending = replace(pending, cycle=cycle(account_id="other"))
    elif change == "strategy":
        pending = replace(pending, cycle=cycle(strategy_id="other"))
    elif change == "instrument":
        pending = replace(pending, instrument=ETH)
    else:
        sample = context()
    with pytest.raises((ValidationError, TradingBlocked)):
        prepare_execution_decision(
            cycle(cycle_id="cancel"),
            sample,
            TradeDecision(cancel_pending=True),
            pending_intent=pending,
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"entries_allowed": False},
        {"state_certain": False},
        {"control_owned": False},
        {"has_pending_order": True},
    ],
)
def test_existing_risk_controls_block_order_preparation(changes):
    with pytest.raises(TradingBlocked):
        prepare_execution_decision(cycle(), context(**changes), decision())


def test_pause_keeps_valid_reduction_but_a_reversal_still_requires_split_planning():
    sample = context(position=PositionView(D(1), D(100)), entries_allowed=False)
    close = TradeDecision((OrderProposal(Side.SELL, D(".5"), reducing=True),))
    result = prepare_execution_decision(cycle(), sample, close)
    assert result.intents[0].reducing and result.intents[0].request.reduce_only is True
    with pytest.raises(TradingBlocked, match="close the current direction"):
        prepare_execution_decision(
            cycle(),
            replace(sample, entries_allowed=True),
            TradeDecision((OrderProposal(Side.SELL, D(2)),)),
        )
    with pytest.raises(TradingBlocked, match="exceed or reverse"):
        prepare_execution_decision(
            cycle(),
            sample,
            TradeDecision((OrderProposal(Side.SELL, D(2), reducing=True),)),
        )


def test_spot_inventory_reduction_is_preserved_without_a_venue_reduce_only_flag():
    sample = context(
        instrument=SPOT,
        position=PositionView(D(1), D(100)),
        constraints=ExecutionConstraints(),
        entries_allowed=False,
    )
    result = prepare_execution_decision(
        cycle(),
        sample,
        TradeDecision((OrderProposal(Side.SELL, D(".25"), reducing=True),)),
    )
    assert result.intents[0].reducing is True
    assert result.intents[0].request.reduce_only is False
    with pytest.raises(TradingBlocked):
        prepare_execution_decision(
            cycle(),
            replace(sample, position=PositionView(D(-1), D(100))),
            TradeDecision((OrderProposal(Side.BUY, D(".25"), reducing=True),)),
        )


def test_optional_normalized_rules_and_actual_supplied_clock_keep_their_strict_bounds():
    proposal = decision()
    assert prepare_execution_decision(cycle(), context(), proposal, rules=rules()).intents
    with pytest.raises(TradingBlocked, match="fresh trading-status"):
        prepare_execution_decision(cycle(), context(observed_at_ms=120001), proposal, rules=rules())
    with pytest.raises(ValidationError, match="range"):
        prepare_execution_decision(
            cycle(),
            context(),
            proposal,
            rules=rules(quantity=DecimalRange(D(0), D(".1"))),
        )
    with pytest.raises(TradingBlocked, match="supported"):
        prepare_execution_decision(
            cycle(),
            context(),
            proposal,
            rules=rules(unsupported_filters=("PERCENT_PRICE",)),
        )
    with pytest.raises(ValidationError, match="instrument"):
        prepare_execution_decision(cycle(), context(), proposal, rules=rules(instrument=ETH))


def test_market_notional_needs_a_matching_provider_reference_not_context_price_guessing():
    snapshot = rules(
        notionals=(
            NotionalRule(
                minimum=D(10),
                apply_min_to_market=True,
                reference_kind="mark_price",
            ),
        )
    )
    with pytest.raises(TradingBlocked, match="provider reference"):
        prepare_execution_decision(cycle(), context(), decision(), rules=snapshot)
    reference = NotionalReference(BTC, D(100), "mark_price", 0, 60000, "synthetic_mark")
    compiled = prepare_execution_decision(
        cycle(),
        context(),
        decision(),
        rules=snapshot,
        rule_references=(reference,),
    )
    assert compiled.intents[0].request.price is None
    with pytest.raises(ValidationError, match="explicit execution rules"):
        prepare_execution_decision(cycle(), context(), decision(), rule_references=(reference,))


@pytest.mark.parametrize(
    "changes",
    [
        {"account_id": ""},
        {"strategy_id": "x" * 129},
        {"cycle_id": 1},
        {"source_digest": "A" * 64},
        {"source_digest": "missing"},
    ],
)
def test_cycle_identity_and_source_are_strictly_bounded(changes):
    with pytest.raises(ValidationError):
        cycle(**changes)


@pytest.mark.parametrize("ordinal", [True, -1, 100])
def test_intent_ordinal_does_not_accept_boolean_or_out_of_bounds_values(ordinal):
    with pytest.raises(ValidationError):
        ExecutionIntent(cycle(), BTC, ordinal, decision().orders[0])


def test_typed_revalidation_and_closed_candle_contract_are_preserved():
    sample = context()
    object.__setattr__(sample, "entries_allowed", 1)
    with pytest.raises(ValidationError):
        prepare_execution_decision(cycle(), sample, decision())
    with pytest.raises(ValidationError):
        prepare_execution_decision(cycle(), context(observed_at_ms=MAX_TIME + 1), decision())
    with pytest.raises(ValidationError):
        prepare_execution_decision(
            cycle(),
            context(constraints=ExecutionConstraints(can_short="false")),
            decision(),
        )
    candle = Candle(BTC, 0, 60000, *(D(value) for value in (100, 100, 100, 100, 1)))
    assert prepare_execution_decision(cycle(), context(closed_candle=candle), decision()).intents
    forged = context()
    object.__setattr__(forged, "closed_candle", replace(candle, instrument=ETH))
    with pytest.raises(ValidationError, match="mismatched"):
        prepare_execution_decision(cycle(), forged, decision())


def test_serial_guard_and_exact_decimal_capacity_do_not_create_partial_intents():
    with pytest.raises(TradingBlocked, match="serial"):
        prepare_execution_decision(cycle(), context(), TradeDecision(decision().orders * 2))
    with pytest.raises(ValidationError, match="calculation bound"):
        prepare_execution_decision(
            cycle(),
            context(),
            TradeDecision((OrderProposal(Side.BUY, D("1e61")),)),
        )
