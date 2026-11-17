"""Native partial evidence is protection-only, including general TS caching."""

import json
from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.application.trading import TradingEngine
from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market
from omega.domain.protection import ProtectionObservation
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext
from omega.strategies.tef.module import TefModule

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def observation(**changes):
    values = dict(
        instrument=BTC,
        generation="a" * 32,
        connection=1,
        sequence=1,
        event_time_ms=10000,
        received_at_ms=10001,
        open_time_ms=0,
        close_time_ms=60000,
        source="binance_ws_kline_1m_partial",
        last=D(100),
        high=D(100),
        low=D(94),
    )
    values.update(changes)
    return ProtectionObservation(**values)


def context(obs, *, changed=0, pending=False):
    return TradeContext(
        BTC,
        obs.last,
        10002,
        PositionView(D(1), D(100), 1, changed),
        BudgetView(D(100), D(100)),
        ExecutionConstraints(leverage=D(10), can_short=True),
        entries_allowed=False,
        has_pending_order=pending,
        protection_observation=obs,
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"connection": True},
        {"sequence": 0},
        {"event_time_ms": 60000},
        {"received_at_ms": 9999},
        {"generation": "../foreign"},
        {"high": D(99)},
        {"low": D("NaN")},
        {"close_time_ms": 60001},
        {"source": "invalid source label"},
    ],
)
def test_native_observation_rejects_wrong_identity_clock_and_prices(changes):
    with pytest.raises(ValidationError):
        observation(**changes)


def test_strict_roundtrip_and_original_receipt_expiry():
    original = observation()
    assert ProtectionObservation.from_document(original.document()) == original
    original.current(15000)
    with pytest.raises(ValidationError):
        original.current(15002)
    late = observation(event_time_ms=59999, received_at_ms=60000)
    with pytest.raises(ValidationError):
        late.current(60000)  # A late partial packet is not the next minute's wick.
    malformed = original.document() | {"last": "100.0"}
    # Decimal scale is original payload, not silently normalized away.
    assert ProtectionObservation.from_document(malformed).last == original.last
    with pytest.raises(ValidationError):
        ProtectionObservation.from_document(original.document() | {"closed": False})


def test_partial_wick_only_for_position_existing_before_whole_window(monkeypatch):
    for changed, expected in ((0, True), (5000, False)):
        module = TefModule(BTC, {"fullStopLossImmediate": ".05", "fullStopLossClose": ".01"})
        monkeypatch.setattr(module.model, "_calculate", lambda *_: pytest.fail("no signal"))
        before = module.last_open_time_ms
        engine = TradingEngine(module)
        decision = engine.evaluate(context(observation(), changed=changed), protection_only=True)
        assert bool(decision.orders) is expected
        if expected:
            assert decision.orders[0].reducing and decision.orders[0].price is None
        assert engine.module.last_open_time_ms == before is None
    # A genuinely post-entry last price can trigger, unlike the earlier cumulative wick.
    result = TradingEngine(TefModule(BTC, {"fullStopLossImmediate": ".05"})).evaluate(
        context(observation(last=D(94)), changed=5000),
        protection_only=True,
    )
    assert result.orders[0].reducing


def test_short_touch_uses_post_entry_high_and_partial_never_triggers_close_only_stop():
    sample = observation(high=D(106), low=D(100))
    short = replace(context(sample), position=PositionView(D(-1), D(100), 1))
    decision = TradingEngine(TefModule(BTC, {"fullStopLossImmediate": ".05"})).evaluate(
        short,
        protection_only=True,
    )
    assert decision.orders[0].reducing and decision.orders[0].side.value == "BUY"
    close_only = TradingEngine(TefModule(BTC, {"fullStopLossClose": ".01"})).evaluate(
        context(observation(last=D(94))),
        protection_only=True,
    )
    assert not close_only.orders and not close_only.cancel_pending


def test_general_engine_key_includes_partial_evidence_and_cannot_admit_entries():
    engine = TradingEngine(TefModule(BTC, {"fullStopLossImmediate": ".05"}))
    safe = observation(low=D(100))
    first = engine.evaluate(context(safe), protection_only=True)
    assert not first.orders
    changed = replace(safe, sequence=2, low=D(94))
    second = engine.evaluate(context(changed), protection_only=True)
    assert second.orders and second != first
    with pytest.raises(ValidationError, match="only permits"):
        TradingEngine(TefModule(BTC)).evaluate(context(safe))
    with pytest.raises(ValidationError):
        replace(context(safe), entries_allowed=True)


@pytest.mark.parametrize("market", [Market.USDM, Market.CASH_EQUITY])
def test_common_observation_has_no_binance_provider_or_market_activation_assumption(market):
    sample = observation(
        instrument=Instrument("external_venue", market, "ABCUSD"),
        source="external_venue.ws.1m",
        price_kind="kline_last",
    )
    assert ProtectionObservation.from_document(sample.document()) == sample
    assert sample.price_kind == "kline_last"
    with pytest.raises(ValidationError):
        replace(sample, price_kind="mark_price")


@pytest.mark.parametrize("rollover", [False, True])
def test_partial_cancel_trigger_survives_recovery_restart_and_pre_entry_wick_exclusion(
    monkeypatch,
    rollover,
):
    config = {"fullStopLossImmediate": ".05"}
    module = TefModule(BTC, config)
    monkeypatch.setattr(module.model, "_calculate", lambda *_: pytest.fail("no signal calculation"))
    original = observation(last=D(94))
    pending = context(original, changed=5000, pending=True)
    cancellation = module.evaluate_protection(pending)
    assert cancellation.cancel_pending and not cancellation.orders
    trigger = module.snapshot()["pending_stop"]
    assert trigger == ("LONG", 1, pending.observed_at_ms, "FSLIMMED")
    before = json.loads(json.dumps(module.checkpoint()))
    duplicate = module.evaluate_protection(pending)
    assert not duplicate.orders and not duplicate.cancel_pending
    assert duplicate.reason == "await_pending_protection_order"
    for flag in ("state_certain", "control_owned"):
        uncertain = module.evaluate_protection(replace(pending, **{flag: False}))
        assert not uncertain.orders and not uncertain.cancel_pending
    assert json.loads(json.dumps(module.checkpoint())) == before
    restored = TefModule(BTC, config)
    restored.restore(before)
    monkeypatch.setattr(
        restored.model, "_calculate", lambda *_: pytest.fail("no signal calculation")
    )
    assert json.loads(json.dumps(restored.checkpoint())) == before
    start = 60000 if rollover else 0
    later = observation(
        sequence=2,
        open_time_ms=start,
        close_time_ms=start + 60000,
        event_time_ms=start + 10003,
        received_at_ms=start + 10004,
        last=D(100),
        high=D(100),
        low=D(100) if rollover else D(94),
    )
    recovered = replace(
        pending,
        price=later.last,
        observed_at_ms=later.received_at_ms + 1,
        protection_observation=later,
        has_pending_order=False,
    )
    # Without the stored trigger, neither recovered last nor eligible current wick touches.
    fresh = TefModule(BTC, config).evaluate_protection(recovered)
    assert not fresh.orders and not fresh.cancel_pending
    changed_side = replace(recovered, position=PositionView(D(-1), D(100), 2, 5001))
    with pytest.raises(ValidationError, match="direction changed"):
        restored.evaluate_protection(changed_side)
    assert json.loads(json.dumps(restored.checkpoint())) == before
    reduction = restored.evaluate_protection(recovered)
    assert len(reduction.orders) == 1 and reduction.orders[0].reducing
    assert reduction.orders[0].side.value == "SELL" and reduction.orders[0].quantity == 1
    assert reduction.orders[0].price is None and reduction.reason == "FSLIMMED"
    assert restored.snapshot()["pending_stop"] == trigger
    assert restored.last_open_time_ms is None
