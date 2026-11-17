"""Real paper owner/TS transactions with synthetic rules and explicit feed provenance."""

from copy import deepcopy
from dataclasses import replace
from decimal import ROUND_FLOOR, localcontext
from decimal import Decimal as D

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.application.paper_state import validate_state
from omega.application.trading import TradingEngine
from omega.domain.errors import ValidationError
from omega.domain.execution_rules import DecimalRange, ExecutionRulesSnapshot, NotionalRule
from omega.domain.models import Candle, Instrument, Market
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    PositionView,
    TradeContext,
    floor_to_step,
)
from omega.strategies.registry import StrategyRegistry

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
MINUTE = 60000


def rules(now=180000, **changes):
    return replace(
        ExecutionRulesSnapshot(
            INSTRUMENT,
            now,
            "binance:usdm:exchangeInfo",
            "TRADING",
            DecimalRange(D(".005"), D(100), D(".005")),
            None,
            DecimalRange(step=D(".05")),
            reductions_allowed=True,
        ),
        **changes,
    )


class Provider:
    def __init__(self, snapshot):
        self.snapshot, self.calls = snapshot, []

    def __call__(self, key, *, max_age_ms):
        self.calls.append((key, max_age_ms))
        if isinstance(self.snapshot, Exception):
            raise self.snapshot
        return self.snapshot


def candle(index, price="100"):
    amount = D(price)
    return Candle(
        INSTRUMENT, index * MINUTE, (index + 1) * MINUTE, amount, amount, amount, amount, D(10)
    )


def create(
    *,
    provider=None,
    policy=None,
    now=None,
    module="target_quantity",
    config=None,
    state=None,
    features=create_features,
    persist=lambda state: None,
):
    now = now or [180100]
    policy = (
        policy
        if policy is not None
        else {"mode": "static", "max_age_ms": 60000, "apply_to_manual": False}
    )
    service = PaperTradingService(
        StrategyRegistry().create,
        features,
        state=state,
        persist=persist,
        rules_provider=provider,
        execution_policy_provider=lambda: dict(policy),
        clock_ms=lambda: now[0],
    )
    if state is None:
        service.command(
            "paper.account.create", {"account_id": "a", "balance": "10000", "fee_rate": "0.001"}
        )
        service.command(
            "paper.strategy.add",
            {
                "instance": "s",
                "account_id": "a",
                "instrument": INSTRUMENT.key,
                "module_id": module,
                "configuration": config or {"quantity": "1"},
                "budget": "1000",
            },
        )
        service.command("resume", {})
    return service, now


def row(service):
    return service.data["strategies"]["s"]


def manual(service, index, price="100"):
    bar = candle(index, price)
    return service.command(
        "paper.step",
        {
            "instrument": INSTRUMENT.key,
            "candle": {
                "open_time_ms": bar.open_time_ms,
                **{
                    key: str(getattr(bar, key))
                    for key in ("open", "high", "low", "close", "volume")
                },
            },
        },
    )


def prepared(service):
    service.feed_configure(True, start_ms=120000, warmup_minutes=1)
    service.feed_warmup({"s": ()}, expected=service.feed_snapshot(), now_ms=180100)


def database(service, index, now):
    return service.feed_apply(
        (candle(index),),
        expected=service.feed_snapshot(),
        entries_allowed=True,
        reason="ready",
        now_ms=now,
    )


def test_database_feed_injects_real_snapshot_sizing_and_pins_full_pending_provenance():
    provider = Provider(rules(quantity=DecimalRange(D(".003"), D(100), D(".006"), D(".003"))))
    service, now = create(provider=provider, config={"quantity": ".020"})
    prepared(service)
    database(service, 2, now[0])
    pending = row(service)["pending"]
    assert D(pending["quantity"]) == D(".015")
    pin = pending["execution"]
    assert pin["snapshot"] == provider.snapshot.document() and pin["decision_at_ms"] == 180100
    assert pin["input_source"] == "database_committed" and not pin["retrospective"]
    assert service.detail("s")["execution_health"]["mode"] == "static"
    assert service.execution_health()["incidents"] == []
    validate_state(service.data)


def test_manual_remains_explicitly_synthetic_unless_enabled_and_provider_none_is_legacy():
    provider = Provider(None)
    service, _ = create(provider=provider)
    manual(service, 0)
    assert row(service)["pending"] and "execution" not in row(service)["pending"]
    assert row(service)["execution_health"]["mode"] == "synthetic" and provider.calls == []
    legacy, _ = create()
    prepared(legacy)
    database(legacy, 2, 180100)
    assert legacy.execution_health()["configured"] is False
    assert row(legacy)["execution_health"]["input_source"] == "database_committed"
    assert row(legacy)["execution_health"]["state"] == "synthetic"


@pytest.mark.parametrize(
    "value", [None, ValueError("private secret"), "wrong type", rules(now=180101), rules(now=1)]
)
def test_missing_bad_future_or_stale_current_rules_block_entry_without_quarantine(value):
    service, now = create(provider=Provider(value))
    prepared(service)
    database(service, 2, now[0])
    assert row(service)["pending"] is None and row(service)["status"] == "ready"
    assert row(service)["last_candle"] == 120000
    assert row(service)["execution_health"]["state"] == "blocked"
    assert "private secret" not in str(service.execution_health())
    validate_state(service.data)


def test_pending_and_fill_keep_original_rules_policy_after_gui_refresh_and_mode_change():
    provider, policy = (
        Provider(rules()),
        {"mode": "static", "max_age_ms": 60000, "apply_to_manual": False},
    )
    service, now = create(provider=provider, policy=policy)
    prepared(service)
    database(service, 2, now[0])
    original = deepcopy(row(service)["pending"])
    provider.snapshot = rules(240000, quantity=DecimalRange(D(".5"), D(100), D(".5")))
    policy["mode"] = "strict"
    now[0] = 240100
    database(service, 3, now[0])
    assert row(service)["quantity"] == "0" and row(service)["pending"] == original
    now[0] = 300100
    database(service, 4, now[0])
    assert row(service)["quantity"] == original["quantity"]
    fill = row(service)["fills"][0]["execution"]
    assert fill["snapshot_hash"] == original["execution"]["snapshot"]["content_hash"]
    assert fill["policy"]["mode"] == "static" and "snapshot" not in fill
    assert D(row(service)["fills"][0]["fee"]) == D("0.1")
    validate_state(service.data)


def test_static_records_unchecked_reference_and_strict_blocks_it_without_fake_average():
    venue = rules(
        notionals=(NotionalRule(D(10), apply_min_to_market=True, reference_kind="mark_price"),)
    )
    static, _ = create(provider=Provider(venue))
    prepared(static)
    database(static, 2, 180100)
    assert row(static)["pending"]["execution"]["unchecked"] == [
        "missing_market_reference:mark_price:0"
    ]
    assert row(static)["execution_health"]["state"] == "unchecked"
    strict, _ = create(
        provider=Provider(venue),
        policy={"mode": "strict", "max_age_ms": 60000, "apply_to_manual": False},
    )
    prepared(strict)
    database(strict, 2, 180100)
    assert row(strict)["pending"] is None and row(strict)["status"] == "ready"
    assert strict.engines["s"]._last_decision.orders == ()
    validate_state(static.data)
    validate_state(strict.data)


def test_known_rules_allow_stale_reduction_but_none_never_invents_protection():
    provider = Provider(rules())
    service, now = create(provider=provider)
    prepared(service)
    database(service, 2, now[0])
    now[0] = 240100
    provider.snapshot = None
    database(service, 3, now[0])
    # The original submitted entry is preserved and settles; the target can then be reduced.
    service.engines["s"].module.target = D(0)
    now[0] = 300100
    database(service, 4, now[0])
    pending = row(service)["pending"]
    assert pending["reducing"] and "stale_rules_reduction" in pending["execution"]["unchecked"]
    assert "current_execution_rules_unavailable" in pending["execution"]["unchecked"]
    unknown, _ = create(
        provider=Provider(None),
        policy={"mode": "static", "max_age_ms": 60000, "apply_to_manual": True},
        config={"quantity": "0"},
    )
    row(unknown).update(quantity="1", entry_price="100")
    manual(unknown, 0)
    assert row(unknown)["pending"] is None and row(unknown)["status"] == "ready"
    incident = unknown.execution_health()["incidents"][0]
    assert set(incident) == {"account_id", "instance", "instrument", "code", "severity"}
    assert (
        incident["severity"] == "critical"
        and row(unknown)["execution_health"]["protection_blocked"]
    )
    validate_state(service.data)


class FixedFeatures:
    def update(self, candle):
        return {"0_MMACD_MSDELTAABSMAREL": 1.0, "CLOSED_0": True}


def test_rejected_tef_stop_rolls_model_forward_clears_unsent_latch_and_can_retry():
    provider = Provider(rules(600000, quantity=DecimalRange(D(".005"), D(".5"), D(".005"))))
    policy = {"mode": "static", "max_age_ms": 60000, "apply_to_manual": True}
    service, _ = create(
        provider=provider,
        policy=policy,
        now=[600100],
        module="tef",
        config={"fullStopLossClose": ".05"},
        features=lambda *_: FixedFeatures(),
    )
    row(service).update(quantity="1", entry_price="100")
    manual(service, 0, "90")
    assert row(service)["status"] == "ready" and row(service)["pending"] is None
    assert service.engines["s"].module._pending_stop is None
    assert service.engines["s"].module.last_open_time_ms == 0
    assert service.engines["s"]._last_decision.orders == ()
    provider.snapshot = rules(600000)
    manual(service, 1, "85")
    assert (
        row(service)["pending"]["reducing"]
        and service.engines["s"].module.last_open_time_ms == 60000
    )
    assert row(service)["pending"]["execution"]["retrospective"]
    validate_state(service.data)


def test_pending_snapshot_roundtrip_restores_old_and_new_schema_and_bad_pin_fails_closed():
    provider = Provider(rules())
    service, _ = create(provider=provider)
    prepared(service)
    database(service, 2, 180100)
    state = deepcopy(service.data)
    recovered, _ = create(provider=provider, state=state)
    assert (
        recovered.detail("s")["pending"]["execution"]
        == state["strategies"]["s"]["pending"]["execution"]
    )
    assert recovered.snapshot()["paused"] and recovered.detail("s")["status"] == "recovery_required"
    bad = deepcopy(state)
    bad["strategies"]["s"]["pending"]["execution"]["snapshot"]["content_hash"] = "0" * 64
    with pytest.raises(ValidationError):
        create(provider=provider, state=bad)
    legacy = deepcopy(state)
    del legacy["strategies"]["s"]["pending"]["execution"]
    del legacy["strategies"]["s"]["execution_health"]
    del legacy["strategies"]["s"]["execution_rules_snapshot"]
    validate_state(legacy)


@pytest.mark.parametrize("damage", ["source", "bool_clock", "off_grid", "unknown_field"])
def test_new_pin_metadata_is_strict_and_revalidates_owned_pending(damage):
    service, _ = create(provider=Provider(rules()))
    prepared(service)
    database(service, 2, 180100)
    state = deepcopy(service.data)
    pending = state["strategies"]["s"]["pending"]
    if damage == "source":
        pending["execution"]["input_source"] = "guess_from_history"
    elif damage == "bool_clock":
        pending["execution"]["decision_at_ms"] = True
    elif damage == "off_grid":
        pending["quantity"] = "1.0001"
    else:
        pending["execution"]["secret"] = "not permitted"
    with pytest.raises(ValidationError):
        validate_state(state)


def test_execution_rejection_updates_engine_cache_and_rejects_stale_decision():
    from omega.strategies.target_quantity.module import TargetQuantityModule

    engine = TradingEngine(TargetQuantityModule(INSTRUMENT, {"quantity": "1"}))
    ctx = TradeContext(
        INSTRUMENT,
        D(100),
        60000,
        PositionView(),
        BudgetView(D(1000), D(1000)),
        ExecutionConstraints(can_short=True),
    )
    proposed = engine.evaluate(ctx)
    blocked = engine.reject_execution(proposed, "synthetic_maximum")
    assert engine.evaluate(ctx) is blocked and not blocked.orders
    with pytest.raises(ValidationError):
        engine.reject_execution(proposed, "stale proposal")


def test_invalid_policy_and_reversed_decision_clock_cannot_use_freshness_from_bar_time():
    provider = Provider(rules())
    service, now = create(
        provider=provider, policy={"mode": "invalid", "max_age_ms": True, "apply_to_manual": False}
    )
    prepared(service)
    database(service, 2, 180100)
    assert row(service)["pending"] is None and row(service)["status"] == "ready"
    assert row(service)["execution_health"]["reason"] == "invalid_execution_policy"
    reversed_clock, now = create(provider=Provider(rules(90000)), now=[100000])
    prepared(reversed_clock)
    database(reversed_clock, 2, 180100)
    assert row(reversed_clock)["pending"] is None
    assert row(reversed_clock)["execution_health"]["reason"] == "execution_clock_unavailable"


def test_full_snapshot_source_mismatch_and_compact_fill_corruption_fail_closed():
    bad_source = Provider(replace(rules(), source="manual:synthetic"))
    service, _ = create(provider=bad_source)
    prepared(service)
    database(service, 2, 180100)
    assert (
        row(service)["pending"] is None
        and row(service)["execution_health"]["reason"] == "rules_provider_unavailable"
    )
    good, now = create(provider=Provider(rules()))
    prepared(good)
    database(good, 2, 180100)
    now[0] = 240100
    database(good, 3, now[0])
    now[0] = 300100
    database(good, 4, now[0])
    state = deepcopy(good.data)
    state["strategies"]["s"]["fills"][0]["execution"]["snapshot_source"] = "manual:synthetic"
    with pytest.raises(ValidationError):
        validate_state(state)


@pytest.mark.parametrize(
    ("raw", "step", "expected"),
    [
        ("1", ".001", "1"),
        ("1.0", ".001", "1.0"),
        ("1.000", ".001", "1.000"),
        ("1", ".002", "1.0"),
        ("1.23456", ".001", "1.234"),
        ("0", ".001", "0"),
        ("0.000", ".001", "0.000"),
        ("-1", ".001", "-1"),
        (".0005", ".001", "0.000"),
    ],
)
def test_zero_origin_helpers_keep_old_decimal_divide_scale_without_low_context_rounding(
    raw, step, expected
):
    value, unit = D(raw), D(step)
    with localcontext() as original_context:
        original_context.prec = 28
        # Completed Omega helper before the integer-grid bridge.
        original = (value / unit).to_integral_value(rounding=ROUND_FLOOR) * unit
    assert str(original) == expected
    with localcontext() as actual_context:
        actual_context.prec = 2
        assert str(floor_to_step(value, unit)) == expected
