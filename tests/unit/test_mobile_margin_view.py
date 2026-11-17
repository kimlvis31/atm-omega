"""Pure public projection and actual cached service reads, no finance/IO calls."""

import json
import threading
from copy import deepcopy
from decimal import Decimal as D

import pytest

from omega.application.margin_funding import FundingPoint, prepare_funding_batch
from omega.domain.margin import MarginAccount, MarginMode, MarginPosition
from omega.domain.models import Instrument, Market
from omega.runtime import mobile_margin_view as module
from omega.runtime.margin_store import account_document
from omega.runtime.margin_trading import MarginTradingService

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def view(identity="a", *, quantity="-2.00", funding=False):
    quantity = D(quantity)
    account = MarginAccount(
        identity,
        "USDT",
        D("1000.00"),
        (
            MarginPosition(
                BTC,
                "strategy",
                MarginMode.CROSS,
                10,
                D(100),
                quantity,
                D(100) if quantity else D(0),
            ),
        ),
    )
    if funding:
        account = prepare_funding_batch(
            account, (FundingPoint(BTC, 60000, 60010, 60000, D(".01"), D(100)),), as_of_ms=120000
        ).candidate_account
    return {
        "account_id": identity,
        "state": "ready",
        "paused": False,
        "new_risk_ready": True,
        "cursor_ms": 60000,
        "pending_orders": [],
        "pending_frame": False,
        "account": account_document(account, ledger_limit=128),
    }


def project(views, declared=None, *, ready=True, blocked=()):
    selected = module.select_cached_margin_view(
        {identity: {} for identity in views} if declared is None else declared,
        views,
        blocked_accounts=blocked,
    )
    return module.mobile_margin_portfolio(selected, service_ready=ready)


@pytest.mark.parametrize("quantity", ["-2.00", "1.000", "0"])
@pytest.mark.parametrize("funding", [False, True])
def test_actual_schema_one_two_packets_signed_open_positions_and_confirmed_flat(quantity, funding):
    cached = view(quantity=quantity, funding=funding)
    assert cached["account"]["schema_version"] == (2 if funding else 1)
    assert type(cached["account"]["account"]["positions"][0]["margin_mode"]) is MarginMode
    result = project({"a": cached})
    assert result["state"] == "ready" and result["summary"]["complete"] is True
    account = result["accounts"][0]
    assert account["position_status"] == "confirmed"
    assert account["pending_order_count"] == 0
    if D(quantity):
        assert account["positions"] == [
            {
                "instrument": BTC.key,
                "quantity": quantity,
                "entry_price": "100",
                "margin_mode": "cross",
                "leverage": 10,
            }
        ]
    else:
        assert account["positions"] == []
    assert module.sanitize_margin_portfolio(result) == result


@pytest.mark.parametrize(
    "state,reason",
    [
        ("unknown", "account_unknown"),
        ("conflict", "account_conflict"),
        ("financial_only", "account_recovery_required"),
        ("shutdown_recovery_required", "account_recovery_required"),
        ("unavailable", "account_invalid"),
    ],
)
def test_uncertain_cache_never_becomes_known_empty_or_reuses_old_positions(state, reason):
    cached = view()
    cached["state"] = state
    result = project({"a": cached})
    account = result["accounts"][0]
    assert account["positions"] is None and account["position_status"] == "unknown"
    assert account["pending_order_count"] is None and account["entry_paused"] is None
    assert account["new_risk_ready"] is False and account["reason_code"] == reason
    assert result["summary"] == {
        "known_open_positions": 0,
        "known_pending_orders": 0,
        "unknown_accounts": 1,
        "complete": False,
    }
    assert module.sanitize_margin_portfolio(result) == result


def test_missing_partial_blocked_and_service_unavailable_remain_explicit_unknown():
    result = project({"a": view(), "undeclared": view("undeclared")}, {"a": {}, "b": {}})
    assert result["state"] == "partial"
    assert [account["account_id"] for account in result["accounts"]] == ["a", "b"]
    assert result["accounts"][1]["reason_code"] == "cache_missing"
    assert result["summary"]["unknown_accounts"] == 1
    assert not result["summary"]["complete"]
    assert project({"a": view()}, blocked={"a"})["accounts"][0]["positions"] is None
    unavailable = project({"a": view()}, ready=False)
    assert unavailable["state"] == "unavailable" and not unavailable["summary"]["complete"]
    assert unavailable["accounts"][0]["reason_code"] == "service_unavailable"


@pytest.mark.parametrize(
    "fault",
    [
        "schema_bool",
        "schema_unknown",
        "account_mismatch",
        "cached_id",
        "cursor_bool",
        "cursor_misaligned",
        "missing_cursor",
        "paused_int",
        "ready_int",
        "positions_missing",
        "duplicate",
        "oversized",
        "float_quantity",
        "nonfinite",
        "negative_price",
        "zero_entry",
        "flat_price",
        "leverage_bool",
        "leverage_large",
        "wrong_instrument",
        "mode_int",
    ],
)
def test_malformed_native_cache_is_unknown_without_numeric_defaults(fault):
    cached = view()
    packet = cached["account"]
    positions = packet["account"]["positions"]
    if fault == "schema_bool":
        packet["schema_version"] = True
    elif fault == "schema_unknown":
        packet["schema_version"] = 99
    elif fault == "account_mismatch":
        packet["account"]["account_id"] = "other"
    elif fault == "cached_id":
        cached["account_id"] = "other"
    elif fault == "cursor_bool":
        cached["cursor_ms"] = False
    elif fault == "cursor_misaligned":
        cached["cursor_ms"] = 60001
    elif fault == "missing_cursor":
        del cached["cursor_ms"]
    elif fault == "paused_int":
        cached["paused"] = 0
    elif fault == "ready_int":
        cached["new_risk_ready"] = 1
    elif fault == "positions_missing":
        del packet["account"]["positions"]
    elif fault == "duplicate":
        positions.append(dict(positions[0]))
    elif fault == "oversized":
        packet["account"]["positions"] = positions * 101
    else:
        field, value = {
            "float_quantity": ("quantity", -2.0),
            "nonfinite": ("quantity", "NaN"),
            "negative_price": ("entry_price", "-1"),
            "zero_entry": ("entry_price", "0"),
            "flat_price": ("quantity", "0"),
            "leverage_bool": ("leverage", True),
            "leverage_large": ("leverage", 1001),
            "wrong_instrument": ("instrument", "binance:spot:BTCUSDT"),
            "mode_int": ("margin_mode", 1),
        }[fault]
        positions[0][field] = value
    result = project({"a": cached})
    assert result["accounts"][0]["reason_code"] == "account_invalid"
    assert result["accounts"][0]["positions"] is None
    assert not result["summary"]["complete"]


@pytest.mark.parametrize(
    "frame,pending,reason",
    [
        (True, [], "pending_frame"),
        (1, [], "pending_frame_unknown"),
        (False, None, "pending_count_unknown"),
        (False, ["duplicate", "duplicate"], "pending_count_unknown"),
        (False, ["x"] * 101, "pending_count_unknown"),
    ],
)
def test_pending_or_unknown_order_count_prevents_flat_complete_claim(frame, pending, reason):
    cached = view(quantity="0")
    cached.update(pending_frame=frame, pending_orders=pending)
    result = project({"a": cached})
    assert result["state"] == "partial" and not result["summary"]["complete"]
    assert result["accounts"][0]["positions"] == []
    assert result["accounts"][0]["state"] == "pending"
    assert result["accounts"][0]["reason_code"] == reason
    assert result["accounts"][0]["new_risk_ready"] is False
    assert module.sanitize_margin_portfolio(result) == result


def test_confirmed_pending_orders_are_counted_and_retained_when_entry_paused():
    cached = view(quantity="0")
    cached.update(paused=True, new_risk_ready=False, pending_orders=["accepted_entry"])
    result = project({"a": cached})
    assert result["summary"] == {
        "known_open_positions": 0,
        "known_pending_orders": 1,
        "unknown_accounts": 0,
        "complete": True,
    }
    assert result["accounts"][0]["entry_paused"] is True
    assert result["accounts"][0]["pending_order_count"] == 1


class PrivateBomb:
    def __deepcopy__(self, memo):
        raise AssertionError("private data must never be copied")

    def __str__(self):
        raise AssertionError("private data must never be serialized")


def test_actual_runtime_method_selects_only_cached_scalars_without_io_clock_or_owner(tmp_path):
    def forbidden(*args, **kwargs):
        pytest.fail("readonly portfolio must not call a source/clock/entry/owner")

    runtime = MarginTradingService(
        tmp_path, None, forbidden, core_epoch="core", clock_ms=forbidden, entry_gate=forbidden
    )
    runtime.ready = True
    runtime.data = {"accounts": {"a": PrivateBomb()}, "operations": PrivateBomb()}
    cached = view(funding=True)
    cached.update(
        configuration=PrivateBomb(), strategies=PrivateBomb(), external_features=PrivateBomb()
    )
    cached["account"]["account"]["ledger"] = PrivateBomb()
    runtime.views = {"a": cached, "undeclared": PrivateBomb()}
    runtime.operations = PrivateBomb()
    runtime.sessions = PrivateBomb()
    runtime.snapshot = forbidden
    result = runtime.readonly_portfolio()
    assert result == project({"a": view(funding=True)})
    text = json.dumps(result)
    assert '"ledger"' not in text and '"configuration"' not in text and '"strategy"' not in text
    result["accounts"][0]["positions"][0]["quantity"] = "9"
    assert cached["account"]["account"]["positions"][0]["quantity"] == "-2.00"
    assert runtime.readonly_portfolio()["accounts"][0]["positions"][0]["quantity"] == "-2.00"


def test_projection_validation_runs_outside_short_service_cache_lock(tmp_path, monkeypatch):
    runtime = MarginTradingService(tmp_path, None, lambda: None, core_epoch="core")
    runtime.ready, runtime.data, runtime.views = True, {"accounts": {"a": {}}}, {"a": view()}
    original = module.mobile_margin_portfolio
    acquired = []

    def outside(*args, **kwargs):
        def inspect():
            allowed = runtime.condition.acquire(blocking=False)
            acquired.append(allowed)
            if allowed:
                runtime.condition.release()

        thread = threading.Thread(target=inspect)
        thread.start()
        thread.join(2)
        assert not thread.is_alive() and acquired == [True]
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "mobile_margin_portfolio", outside)
    assert runtime.readonly_portfolio()["state"] == "ready"


@pytest.mark.parametrize(
    "registry", [None, [], {"bad/id": {}}, {str(index): {} for index in range(11)}]
)
def test_invalid_or_missing_registry_never_establishes_flatness(registry):
    result = (
        project({}, declared=registry)
        if registry is not None
        else module.mobile_margin_portfolio(
            module.select_cached_margin_view(None, {}), service_ready=True
        )
    )
    assert result == module.unavailable_margin_portfolio()
    assert module.sanitize_margin_portfolio(result) == result
    empty = project({})
    assert empty["state"] == "ready" and empty["summary"]["complete"] is True


def test_wire_sanitizer_copies_allowlisted_native_fields_without_private_history():
    original = project({"a": view()})
    dirty = deepcopy(original)
    dirty["operations"] = PrivateBomb()
    dirty["accounts"][0]["configuration"] = PrivateBomb()
    dirty["accounts"][0]["positions"][0]["model"] = PrivateBomb()
    dirty["summary"]["secret"] = PrivateBomb()
    assert module.sanitize_margin_portfolio(dirty) == original


@pytest.mark.parametrize(
    "fault",
    [
        "summary",
        "summary_bool",
        "scope",
        "schema_bool",
        "state",
        "accounts",
        "duplicate",
        "overcount",
        "status",
        "paused",
        "quantity",
        "pending_bool",
        "cursor",
        "reason",
        "flat_open",
    ],
)
def test_wire_sanitizer_rejects_inconsistent_or_malformed_provider_entirely(fault):
    result = project({"a": view()})
    row = result["accounts"][0]
    if fault == "summary":
        result["summary"]["complete"] = False
    elif fault == "summary_bool":
        result["summary"]["known_open_positions"] = True
    elif fault == "scope":
        result["scope"] = "owner_private_state"
    elif fault == "schema_bool":
        result["schema_version"] = True
    elif fault == "state":
        result["state"] = "partial"
    elif fault == "accounts":
        result["accounts"] = [None]
    elif fault == "duplicate":
        result["accounts"].append(deepcopy(row))
    elif fault == "overcount":
        result["summary"]["known_open_positions"] = 100
    elif fault == "status":
        row["position_status"] = "unknown"
    elif fault == "paused":
        row["entry_paused"] = 0
    elif fault == "quantity":
        row["positions"][0]["quantity"] = -2
    elif fault == "pending_bool":
        row["pending_order_count"] = False
    elif fault == "cursor":
        row["cursor_ms"] = True
    elif fault == "reason":
        row["reason_code"] = "private/path/traceback"
    else:
        row["positions"][0]["quantity"] = "0"
        row["positions"][0]["entry_price"] = "0"
    assert module.sanitize_margin_portfolio(result) == module.unavailable_margin_portfolio()
