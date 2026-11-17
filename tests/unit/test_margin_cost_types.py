"""Exact virtual cost types/math only; no journal, network, model or real orders."""

from dataclasses import fields, replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.application import margin_risk
from omega.application.margin_cost import (
    COST_MODEL,
    QUOTE_MODEL,
    CostPendingFillAdmission,
    FrozenCostPendingOrder,
    MarginCostPolicy,
    PreparedCostMarginBatch,
    approve_cost_order,
    cost_quote,
    cost_reservation,
    freeze_cost_pending,
    make_cost_envelope,
    reservation_fee,
    reservation_valuation,
    validate_entry_envelope,
)
from omega.application.margin_execution import FrozenPendingOrder
from omega.application.paper_fill import BarFillQuote
from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.margin import (
    MAX_TIME,
    MarginAccount,
    MarginBracket,
    MarginBracketSnapshot,
    MarginMode,
    MarginPosition,
    MarkSnapshot,
)
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side
from omega.domain.trading import OrderProposal

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def policy(**changes):
    return MarginCostPolicy(
        **(
            {
                "slippage_rate": D(".01"),
                "execution_price_bound_rate": D(".1"),
                "mark_drift_bound_rate": D(".1"),
                "entry_delay_ms": 0,
                "reduction_delay_ms": 0,
            }
            | changes
        )
    )


def account(mode=MarginMode.CROSS, *, quantity="0"):
    quantity = D(quantity)
    return MarginAccount(
        "account",
        "USDT",
        D(10000),
        (
            MarginPosition(
                BTC,
                "strategy",
                mode,
                10,
                D(5000),
                quantity,
                D(100) if quantity else D(0),
                D(10000) if mode == MarginMode.ISOLATED else D(0),
            ),
        ),
    )


def references():
    return (
        (MarkSnapshot(BTC, D(100), 60000, 60000, "actual_fixture_proxy"),),
        (
            MarginBracketSnapshot(
                BTC,
                (MarginBracket(D(0), D(10000000), D(".01"), D(0), 20),),
                60000,
                60000,
                "fixture",
                "a" * 64,
            ),
        ),
    )


def prepared(
    *,
    side=Side.BUY,
    mode=MarginMode.CROSS,
    proposal=None,
    config=None,
    issued=60000,
    observed=True,
    fee=".001",
    current=None,
):
    proposal = proposal or OrderProposal(side, D(1))
    current = current or account(
        mode, quantity=("1" if side == Side.SELL else "-1") if proposal.reducing else "0"
    )
    marks, brackets = references()
    marks = marks if observed else ()
    envelope = make_cost_envelope(
        current,
        "order",
        proposal,
        policy=config or policy(),
        instrument=BTC,
        owner_id="strategy",
        as_of_ms=issued,
        marks=marks,
        worst_execution_price=D(100),
    )
    worst = (
        D(100)
        if proposal.reducing
        else envelope.execution_upper
        if proposal.side == Side.BUY
        else envelope.execution_lower
    )
    base = margin_risk.approve_order(
        current,
        "order",
        proposal,
        instrument=BTC,
        owner_id="strategy",
        as_of_ms=issued,
        marks=marks,
        brackets=brackets,
        worst_execution_price=worst,
        fee_rate=D(fee),
    )
    return current, envelope, base, cost_reservation(base, envelope)


def test_policy_exact_wire_canonical_identity_and_explicit_zero_mode():
    original = policy()
    document = original.document()
    document["slippage_rate"] = "0.0100"
    parsed = MarginCostPolicy.parse(document)
    assert parsed == original and parsed.document()["slippage_rate"] == "0.01"
    assert parsed.sha256 == original.sha256
    assert parsed.model == COST_MODEL
    zero = policy(slippage_rate=D(0), execution_price_bound_rate=D(0), mark_drift_bound_rate=D(0))
    assert zero.model == COST_MODEL and zero.sha256 != original.sha256


@pytest.mark.parametrize(
    "key,value",
    [
        ("schema_version", True),
        ("model", "legacy"),
        ("limit_price_policy", "unknown"),
        ("slippage_rate", "0.250000000000000001"),
        ("slippage_rate", "-0.1"),
        ("execution_price_bound_rate", "0.51"),
        ("mark_drift_bound_rate", "0.51"),
        ("slippage_rate", 0.01),
        ("slippage_rate", "NaN"),
        ("slippage_rate", "0.0000000000000000001"),
        ("slippage_rate", ".01"),
        ("entry_delay_ms", True),
        ("entry_delay_ms", 86400001),
        ("reduction_delay_ms", -1),
        ("reduction_delay_ms", 1.0),
    ],
)
def test_policy_parser_rejects_unbounded_non_native_and_unsupported_values(key, value):
    document = policy().document()
    document[key] = value
    with pytest.raises(ValidationError):
        MarginCostPolicy.parse(document)
    with pytest.raises(ValidationError):
        MarginCostPolicy.parse(
            {key: member for key, member in policy().document().items() if key != "model"}
        )
    with pytest.raises(ValidationError):
        MarginCostPolicy.parse(policy().document() | {"actual": True})


@pytest.mark.parametrize(
    "changes",
    [
        {"slippage_rate": 0.01},
        {"entry_delay_ms": False},
        {"schema_version": True},
        {"model": "unknown"},
        {"slippage_rate": D("NaN")},
    ],
)
def test_direct_policy_constructor_is_also_strict(changes):
    with pytest.raises(ValidationError):
        policy(**changes)


@pytest.mark.parametrize("mode", [MarginMode.CROSS, MarginMode.ISOLATED])
@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_exact_bands_effective_reserves_and_original_mark_are_independent(mode, side):
    current, envelope, base, result = prepared(side=side, mode=mode)
    assert (envelope.execution_lower, envelope.execution_upper) == (D("89.1"), D("111.1"))
    assert (envelope.mark_lower, envelope.mark_upper) == (D(90), D(110))
    assert envelope.anchor.price == D(100) and base.mark_price == D(100)
    assert envelope.reference_price == D(100)
    assert result.extra_entry_loss == D(10)
    assert reservation_valuation(result) == D("111.1")
    assert reservation_fee(result) == D(".11110000")
    if side == Side.BUY:
        assert result.extra_initial_margin == 0 and result.extra_fee == 0
        assert result.total_reserved == D("32.3211")
    else:
        assert result.extra_initial_margin == D("1.11")
        assert result.extra_fee == D(".0111")
        assert result.total_reserved == D("32.1211")
    assert base.request_fingerprint == result.base_request_fingerprint
    assert result.request_fingerprint != base.request_fingerprint
    assert freeze_cost_pending(result).eligible_at_ms == 60000
    assert current.ledger == ()


def test_context_precision_does_not_change_policy_bands_or_cost_math():
    expected = prepared()[-1]
    with localcontext() as context:
        context.prec = 2
        actual = prepared()[-1]
        assert actual == expected
        assert actual.total_reserved == expected.total_reserved
        assert actual.envelope.document() == expected.envelope.document()


def test_fee_ceiling_extra_is_reserved_once_at_effective_valuation():
    _, _, base, result = prepared(fee=".0000000001")
    assert base.fee_reserve == D(".00000001111")
    assert reservation_fee(result) == D(".00000002")
    assert result.extra_fee == D(".00000000889")


def test_exact_derived_normalization_and_external_capacity_rejection():
    original = policy(
        slippage_rate=D(".123456789012345678"), execution_price_bound_rate=D(".123456789012345678")
    )
    marks, _ = references()
    with pytest.raises(ValidationError, match="bound|capacity"):
        make_cost_envelope(
            account(),
            "o",
            OrderProposal(Side.BUY, D(1)),
            policy=original,
            instrument=BTC,
            owner_id="strategy",
            as_of_ms=60000,
            marks=marks,
            worst_execution_price=D(100),
        )
    assert prepared()[-1].envelope.execution_upper.as_tuple().digits == (1, 1, 1, 1)


@pytest.mark.parametrize(
    "mark,quote,reason",
    [
        (None, None, "cost_mark_unavailable"),
        (D(89), None, "cost_mark_bound_exceeded"),
        (D(111), None, "cost_mark_bound_exceeded"),
        (D(100), D(89), "cost_execution_bound_exceeded"),
        (D(100), D(112), "cost_execution_bound_exceeded"),
    ],
)
def test_pre_fill_validates_actual_mark_and_quote_against_original_bounds(mark, quote, reason):
    result = prepared()[-1]
    with pytest.raises(TradingBlocked, match=reason):
        validate_entry_envelope(result, mark_price=mark, quote_price=quote)
    validate_entry_envelope(result, mark_price=D(90), quote_price=D("89.1"))


def test_prefill_new_prefix_reservation_is_valid_but_not_an_original_pending():
    initial, envelope, base, _ = prepared()
    later = replace(initial, cross_wallet=D(9999), opening_balance=D(9999))
    marks, brackets = references()
    new_base = margin_risk.approve_order(
        later,
        "order",
        base.proposal,
        instrument=BTC,
        owner_id="strategy",
        as_of_ms=120000,
        marks=marks,
        brackets=brackets,
        worst_execution_price=envelope.execution_upper,
        fee_rate=base.fee_rate,
    )
    prefilling = cost_reservation(new_base, envelope)
    assert prefilling.account_fingerprint != envelope.anchor_account_fingerprint
    with pytest.raises(ValidationError):
        freeze_cost_pending(prefilling)


@pytest.mark.parametrize(
    "delay,issued,expected",
    [
        (0, 60000, 60000),
        (1, 60000, 120000),
        (59999, 60000, 120000),
        (60000, 60000, 120000),
        (60001, 60000, 180000),
        (0, 60001, 120000),
        (60000, 60001, 180000),
    ],
)
def test_original_delay_eligibility_is_frozen_once(delay, issued, expected):
    reservation = prepared(config=policy(entry_delay_ms=delay), issued=issued)[-1]
    pending = freeze_cost_pending(reservation)
    assert pending.eligible_at_ms == expected
    with pytest.raises(ValidationError):
        replace(pending, eligible_at_ms=expected + 60000)
    assert pending.original.as_of_ms == issued


def test_max_time_overflow_never_wraps_original_delay():
    _, _, base, result = prepared()
    envelope = replace(result.envelope, issued_at_ms=MAX_TIME)
    data = {
        field.name: getattr(base, field.name) for field in fields(margin_risk.MarginReservation)
    }
    data["as_of_ms"] = MAX_TIME
    original = cost_reservation(margin_risk.MarginReservation(**data), envelope)
    with pytest.raises(ValidationError, match="timestamp"):
        freeze_cost_pending(original)


def test_direct_quote_cannot_precede_original_delay_and_noteligible_source_stays_none():
    from omega.application.margin_execution import _quote

    original = prepared(config=policy(entry_delay_ms=1))[-1]
    pending = freeze_cost_pending(original)
    assert pending.eligible_at_ms == 120000
    with pytest.raises(ValidationError, match="delayed eligibility"):
        cost_quote(pending, BarFillQuote(D(100), 60000, "open"))
    candle = Candle(BTC, 60000, 120000, D(100), D(101), D(99), D(100), D(1))
    assert _quote(pending, candle, "open") is None
    assert cost_quote(pending, BarFillQuote(D(100), 120000, "open")).price == D(101)


@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_reduction_without_anchor_retains_cost_fee_and_delay_without_entry_bands(side):
    proposal = OrderProposal(side, D(1), reducing=True)
    config = policy(entry_delay_ms=86400000, reduction_delay_ms=1)
    _, envelope, _, result = prepared(side=side, proposal=proposal, config=config, observed=False)
    assert envelope.anchor is None
    assert (
        envelope.execution_lower,
        envelope.execution_upper,
        envelope.mark_lower,
        envelope.mark_upper,
    ) == (None, None, None, None)
    assert result.extra_initial_margin == result.extra_entry_loss == 0
    pending = freeze_cost_pending(result)
    assert pending.eligible_at_ms == 120000
    quote = cost_quote(pending, BarFillQuote(D(100), 120000, "open"))
    assert quote.price == (D(101) if side == Side.BUY else D(99))
    validate_entry_envelope(result, mark_price=None, quote_price=quote.price)


@pytest.mark.parametrize(
    "side,limit,raw,expected,capped",
    [
        (Side.BUY, "100", "99", "99.99", False),
        (Side.BUY, "100", "100", "100", True),
        (Side.SELL, "100", "102", "100.98", False),
        (Side.SELL, "100", "100", "100", True),
    ],
)
def test_limit_quote_uses_original_touch_and_side_cap(side, limit, raw, expected, capped):
    proposal = OrderProposal(side, D(1), kind=OrderKind.LIMIT, price=D(limit))
    result = prepared(side=side, proposal=proposal)[-1]
    pending = freeze_cost_pending(result)
    original = BarFillQuote(D(raw), 120000, "close")
    quote = cost_quote(pending, original)
    assert quote.price == D(expected) and quote.limit_capped is capped
    assert quote.raw_price == original.price and quote.event_time_ms == original.event_time_ms
    assert quote.phase == original.phase and quote.model == QUOTE_MODEL
    assert quote.policy_sha256 == pending.original.envelope.policy.sha256


def test_arbitrary_reservation_subclass_properties_and_legacy_wrappers_are_not_cost_authority():
    _, _, base, result = prepared()

    class Unregistered(margin_risk.MarginReservation):
        @property
        def total_reserved(self):
            raise AssertionError("untrusted property invoked")

    forged = Unregistered(**{field.name: getattr(base, field.name) for field in fields(base)})
    for helper in (reservation_fee, reservation_valuation):
        with pytest.raises(ValidationError, match="unregistered"):
            helper(forged)
    with pytest.raises(ValidationError):
        FrozenCostPendingOrder(base, 60000)
    with pytest.raises(ValidationError):
        cost_quote(FrozenPendingOrder(result, 60000), BarFillQuote(D(100), 60000, "open"))
    with pytest.raises(ValidationError):
        replace(result, extra_fee=result.extra_fee + D(1))
    with pytest.raises(ValidationError):
        replace(result, base_request_fingerprint="f" * 64)


def test_approval_helper_delegates_actual_references_and_side_bound_without_fake_mark(monkeypatch):
    observed = {}
    sentinel = object()

    def approve(*args, **kwargs):
        observed.update(kwargs)
        assert args[0] is current
        return sentinel

    monkeypatch.setattr(margin_risk, "approve_order", approve)
    current = account()
    marks, brackets = references()
    assert (
        approve_cost_order(
            current,
            "o",
            OrderProposal(Side.BUY, D(1)),
            policy=policy(),
            instrument=BTC,
            owner_id="strategy",
            as_of_ms=60000,
            marks=marks,
            brackets=brackets,
            worst_execution_price=D(100),
            fee_rate=D(".001"),
        )
        is sentinel
    )
    assert observed["marks"] is marks
    assert observed["worst_execution_price"] == D("111.1")
    assert observed["cost_envelope"].anchor is marks[0]


def test_exact_admission_and_mixed_batch_type_mapping():
    current, _, base, reservation = prepared()
    pending = freeze_cost_pending(reservation)
    quote = cost_quote(pending, BarFillQuote(D(100), 60000, "open"))
    admission = CostPendingFillAdmission(
        "order", reservation.request_fingerprint, "fill", None, quote, reservation
    )
    bar = Candle(BTC, 60000, 120000, D(100), D(101), D(99), D(100), D(1))
    batch = PreparedCostMarginBatch(
        "a" * 64,
        "b" * 64,
        "open",
        120000,
        (bar,),
        (),
        (pending,),
        (admission,),
        (),
        (current,),
        ("c" * 64,),
        (pending,),
        (),
    )
    assert batch.model == COST_MODEL and batch.candidate_account == current
    with pytest.raises(ValidationError):
        replace(batch, original_pending=(FrozenPendingOrder(base, 60000),))
    with pytest.raises(ValidationError):
        replace(admission, quote=BarFillQuote(D(100), 60000, "open"))
    with pytest.raises(ValidationError):
        replace(batch, admissions=(replace(admission, order_id="different"),))
