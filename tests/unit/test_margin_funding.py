"""Exact scenario cashflows, frozen source recovery and independent legacy goldens."""

import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from decimal import ROUND_UP, localcontext
from decimal import Decimal as D

import pytest

from omega.application.margin_funding import (
    FundingPoint,
    FundingSchedule,
    frozen_funding_document,
    prepare_funding_batch,
    restore_funding_batch,
)
from omega.application.virtual_margin import AccountAnchor, EntryControl, VirtualMarginRecord
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.margin import (
    CollateralTransfer,
    FillEvent,
    MarginAccount,
    MarginMode,
    MarginPosition,
    apply_fill,
    apply_funding,
    apply_transfer,
    funding_amount,
)
from omega.domain.models import Instrument, Market
from omega.runtime.margin_store import account_document

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def account(quantity="2", *, mode=MarginMode.CROSS, cash="1000"):
    return MarginAccount(
        "a",
        "USDT",
        D(cash),
        (MarginPosition(BTC, "s", mode, 10, D(100), D(quantity), D(100) if D(quantity) else D(0)),),
    )


def point(rate=".001", *, when=60000, available=60001):
    return FundingPoint(BTC, when, available, when, D(rate), D(100))


@pytest.mark.parametrize("quantity", ["-2", "0", "2"])
@pytest.mark.parametrize("rate", ["-.001", ".001"])
@pytest.mark.parametrize("mode", [MarginMode.CROSS, MarginMode.ISOLATED])
def test_signed_settlement_preserves_position_trade_pnl_and_wallet_boundary(quantity, rate, mode):
    initial = account(quantity, mode=mode)
    plan = prepare_funding_batch(initial, (point(rate),), as_of_ms=120000)
    expected = D(
        {
            ("-2", "-.001"): "-.2",
            ("-2", ".001"): ".2",
            ("0", "-.001"): "0",
            ("0", ".001"): "0",
            ("2", "-.001"): ".2",
            ("2", ".001"): "-.2",
        }[(quantity, rate)]
    )
    final = plan.candidate_account
    assert len(final.ledger) == 1 and final.ledger[0].fact.amount == expected
    assert final.positions[0].quantity == initial.positions[0].quantity
    assert final.positions[0].entry_price == initial.positions[0].entry_price
    assert final.positions[0].realized_pnl == 0 and final.ledger[0].fee == 0
    assert final.cross_wallet == D(1000) + (expected if mode == MarginMode.CROSS else 0)
    assert final.positions[0].isolated_wallet == (expected if mode == MarginMode.ISOLATED else 0)
    assert final.cross_wallet + final.positions[0].isolated_wallet == D(1000) + expected


def test_decimal_policy_is_fixed_and_does_not_divide_by_leverage():
    with localcontext() as context:
        context.prec, context.rounding = 2, ROUND_UP
        assert funding_amount(D(3), D("99.99"), D(".0001")) == D("-.02999700")
        assert funding_amount(D(3), D(".000000005"), D(1)) == D("-.00000002")
        with pytest.raises(ValidationError):
            funding_amount(D(1), D(100), D("1.00000000000000001"))


def test_debt_is_recorded_and_identical_old_fact_remains_noop_after_position_changes():
    initial = account("2", cash=".01")
    plan = prepare_funding_batch(initial, (point(),), as_of_ms=120000)
    final = plan.candidate_account
    assert final.cross_wallet == D("-.19")
    closed = apply_fill(
        final, FillEvent("close", "order", BTC, "s", D(-2), D(100), D(0), "USDT", 180000, 180001)
    ).account
    repeated = apply_funding(closed, plan.facts[0])
    assert repeated.account is closed and repeated.entry is None
    with pytest.raises(ProtocolError, match="conflicts"):
        apply_funding(final, replace(plan.facts[0], rate=D(".002"), amount=D("-.4")))


def test_source_timestamp_is_not_rewritten_to_fit_previous_close_availability():
    initial = account("0")
    baseline = apply_fill(
        initial, FillEvent("entry", "order", BTC, "s", D(1), D(100), D(0), "USDT", 60000, 60010)
    ).account
    plan = prepare_funding_batch(baseline, (point(),), as_of_ms=120000)
    assert plan.facts[0].source_available_at_ms == 60001
    assert plan.facts[0].available_at_ms == 120000
    assert restore_funding_batch(frozen_funding_document(plan), baseline) == plan
    with pytest.raises(ProtocolError, match="late"):
        apply_funding(baseline, replace(plan.facts[0], available_at_ms=60001))


def test_shared_wallet_cutoff_rejects_future_facts_on_another_instrument():
    eth = Instrument("binance", Market.USDM, "ETHUSDT")
    initial = account()
    baseline = replace(
        initial,
        positions=initial.positions + (MarginPosition(eth, "other", MarginMode.CROSS, 5, D(50)),),
    )
    future = apply_fill(
        baseline,
        FillEvent("future", "order", eth, "other", D(1), D(100), D(0), "USDT", 180000, 180001),
    ).account
    with pytest.raises(ProtocolError, match="future account"):
        prepare_funding_batch(future, (point(),), as_of_ms=120000)


@pytest.mark.parametrize("change", ["amount", "source", "baseline", "cutoff", "settlement_bool"])
def test_frozen_recovery_rejects_changed_evidence_instead_of_recalculating(change):
    initial = account()
    plan = prepare_funding_batch(initial, (point(),), as_of_ms=120000)
    frozen = deepcopy(frozen_funding_document(plan))
    if change == "amount":
        frozen["facts"][0]["amount"] = "0"
    elif change == "source":
        frozen["points"][0]["rate"] = ".002"
    elif change == "baseline":
        frozen["baseline_sha256"] = "0" * 64
    elif change == "cutoff":
        frozen["as_of_ms"] += 1
    else:
        frozen["settlement_time_ms"] = False
    with pytest.raises((ProtocolError, ValidationError)):
        restore_funding_batch(frozen, initial)


@pytest.mark.parametrize("available", [59999, 120001, True])
def test_required_source_outside_first_supported_minute_is_rejected(available):
    with pytest.raises(ValidationError):
        point(available=available)


def test_schedule_lookup_distinguishes_no_event_from_zero_cashflow_and_rejects_duplicate():
    schedule = FundingSchedule((point(),))
    assert FundingSchedule.parse(schedule.document()) == schedule
    assert schedule.at(0) == () and schedule.at(60000) == (point(),)
    plan = prepare_funding_batch(account("0"), schedule.at(60000), as_of_ms=120000)
    assert len(plan.facts) == 1 and plan.facts[0].amount == 0
    with pytest.raises(ValidationError):
        FundingSchedule((point(), point()))
    with pytest.raises(ValidationError):
        schedule.validate_for((BTC,), 120000)


@pytest.mark.parametrize("mode", [MarginMode.CROSS, MarginMode.ISOLATED])
def test_no_funding_bytes_match_independent_prefunding_source_golden(mode):
    # Captured from immutable typed-ts-context-2987... before funding changes.
    golden = {
        MarginMode.CROSS: (
            "7be79ad49fc97e335aa38a92511413c24bc37d9ba3aa1c502b9c360647c14df0",
            "0fedb14e6bf5b3a2095603e22cdf2df7874e22e5093d566ad67fa0dfb71175df",
            "1ed82008bf1d3150a6b6c4604ed806fa7ed374b02df81c6757dbee26f2643cad",
        ),
        MarginMode.ISOLATED: (
            "c83568d42580ab0ee7e6101b89bb7e73dc24139b683a2e99568fdc29982d6c8c",
            "fa9e9a7c88263efe3a1d1461c87e67607c5ebbde9550c6f416a0a5aad4d1b122",
            "c9382c7258deb5513f171f1f73b42d4920e699dd7fdf7e60c59ad5860349022f",
        ),
    }[mode]
    initial = MarginAccount(
        "legacy", "USDT", D(1000), (MarginPosition(BTC, "s", mode, 10, D(100)),)
    )
    state = initial
    if mode == MarginMode.ISOLATED:
        state = apply_transfer(
            state, CollateralTransfer("transfer", BTC, "s", D(100), 0, 0)
        ).account
    for identity, quantity, price, fee, when in (
        ("entry", "1", "100", ".1", 60000),
        ("exit", "-1", "90", ".09", 120000),
    ):
        state = apply_fill(
            state,
            FillEvent(
                identity, identity, BTC, "s", D(quantity), D(price), D(fee), "USDT", when, when + 1
            ),
        ).account
    raw = json.dumps(account_document(state), sort_keys=True, separators=(",", ":")).encode()
    anchor = VirtualMarginRecord("legacy", 1, None, "a" * 64, AccountAnchor(initial))
    pause = VirtualMarginRecord("legacy", 2, anchor.checksum, "b" * 64, EntryControl(True, "pause"))
    assert (hashlib.sha256(raw).hexdigest(), anchor.checksum, pause.checksum) == golden
