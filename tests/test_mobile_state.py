"""Paired state reads cannot gain owner authority or execute telemetry/plugins."""

from copy import deepcopy
from threading import Event, Thread
from types import SimpleNamespace

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.runtime.mobile_margin_view import sanitize_margin_portfolio
from omega.runtime.mobile_state import paper_device_snapshot
from omega.runtime.service import CoreService
from omega.strategies.registry import StrategyRegistry


def paired(service):
    code = service.control.create_pairing("phone", ["read", "pause", "resume"])["code"]
    return service.request("POST", "/api/pair", "", {"code": code})


def portfolio():
    return {
        "schema_version": 1,
        "scope": "virtual_margin_v2",
        "state": "ready",
        "accounts": [
            {
                "account_id": "a",
                "state": "ready",
                "entry_paused": True,
                "new_risk_ready": False,
                "cursor_ms": 120000,
                "pending_order_count": 1,
                "positions": [
                    {
                        "instrument": "binance:usdm:BTCUSDT",
                        "quantity": "-1.25",
                        "entry_price": "100.00",
                        "margin_mode": "cross",
                        "leverage": 3,
                    }
                ],
                "position_status": "confirmed",
                "reason_code": "confirmed_cached_ledger",
            }
        ],
        "summary": {
            "known_open_positions": 1,
            "known_pending_orders": 1,
            "unknown_accounts": 0,
            "complete": True,
        },
    }


@pytest.fixture
def service(tmp_path):
    instance = CoreService(tmp_path)
    yield instance
    instance.close()


def test_device_margin_view_is_allowlisted_and_retains_signed_position_and_pending(service):
    private = portfolio()
    private["config"] = {"file": "PRIVATE_PATH"}
    private["accounts"][0]["ledger"] = ["PRIVATE_LEDGER"]
    private["accounts"][0]["positions"][0]["token"] = "PRIVATE_TOKEN"
    service.margin_trading = SimpleNamespace(readonly_portfolio=lambda: private, close=lambda: None)
    device = paired(service)
    state = service.request("GET", "/api/state", device["token"])
    assert state["margin_portfolio"] == portfolio()
    assert "PRIVATE" not in str(state)
    state["margin_portfolio"]["accounts"][0]["positions"].clear()
    assert len(private["accounts"][0]["positions"]) == 1
    for path in (
        "/api/margin-trading",
        "/api/margin-trading/accounts/a",
        "/api/external-feature-bindings",
        "/api/causal-inference",
        "/api/shutdown",
    ):
        with pytest.raises(PermissionError):
            service.request("GET", path, device["token"])


@pytest.mark.parametrize("permissions", [[], ["pause"], None, "read"])
def test_state_read_permission_is_checked_on_server_before_any_provider(service, permissions):
    device = paired(service)
    service.control.data["devices"][device["device_id"]]["permissions"] = permissions
    service.margin_trading = SimpleNamespace(
        readonly_portfolio=lambda: pytest.fail("denied read reached provider"), close=lambda: None
    )
    with pytest.raises(PermissionError, match="read permission"):
        service.request("GET", "/api/state", device["token"])


@pytest.mark.parametrize("value", [None, {}, {"accounts": []}, True])
def test_invalid_or_failed_margin_cache_is_unavailable_not_flat(service, value):
    service.margin_trading = SimpleNamespace(readonly_portfolio=lambda: value, close=lambda: None)
    device = paired(service)
    state = service.request("GET", "/api/state", device["token"])
    assert state["margin_portfolio"]["state"] == "unavailable"
    assert state["margin_portfolio"]["summary"]["complete"] is False
    assert state["core"]["paused"] is True


def test_provider_exception_is_not_disclosed_and_emergency_pause_remains_available(service):
    def broken():
        raise OSError("PRIVATE_PATH_AND_TOKEN")

    service.margin_trading = SimpleNamespace(readonly_portfolio=broken, close=lambda: None)
    device = paired(service)
    state = service.request("GET", "/api/state", device["token"])
    assert state["margin_portfolio"]["summary"]["complete"] is False
    assert "PRIVATE" not in str(state)
    result = service.request(
        "POST", "/api/commands", device["token"], {"id": "stop", "kind": "pause", "payload": {}}
    )
    assert result["status"] == "completed"


def test_device_state_does_not_call_owner_telemetry_or_snapshot_or_touch_files(
    service, monkeypatch
):
    def forbidden(*args, **kwargs):
        pytest.fail("device read executed owner telemetry or filesystem IO")

    paper = SimpleNamespace(
        snapshot=forbidden,
        readonly_snapshot=lambda: {"accounts": [], "PRIVATE": "SECRET"},
    )
    service.paper = paper
    service.research = SimpleNamespace(
        health=forbidden,
        list_jobs=forbidden,
        handler_scope=forbidden,
        supports=forbidden,
        close=lambda: None,
    )
    service.candles_provider = SimpleNamespace(health=forbidden)
    service.paper_feed = SimpleNamespace(status=forbidden, close=lambda: None)
    device = paired(service)
    with monkeypatch.context() as patch:
        patch.setattr("builtins.open", forbidden)
        patch.setattr("pathlib.Path.open", forbidden)
        state = service.request("GET", "/api/state", device["token"])
    assert state["accounts"] == []
    assert state["settings"] == state["research"] == state["execution"] == {}
    assert state["jobs"] == state["strategies"] == []
    assert "PRIVATE" not in str(state)
    service.paper = None


def test_slow_readonly_provider_does_not_hold_emergency_control_lock_or_mutation_fence(service):
    entered, release, completed = Event(), Event(), Event()

    def read():
        entered.set()
        assert release.wait(5)
        return portfolio()

    service.margin_trading = SimpleNamespace(readonly_portfolio=read, close=lambda: None)
    device = paired(service)
    reader = Thread(target=lambda: service.request("GET", "/api/state", device["token"]))
    reader.start()
    assert entered.wait(5)
    writer = Thread(
        target=lambda: (
            service.request(
                "POST",
                "/api/commands",
                device["token"],
                {"id": "stop", "kind": "pause", "payload": {}},
            ),
            completed.set(),
        )
    )
    writer.start()
    try:
        assert completed.wait(2), "a device read blocked emergency pause"
    finally:
        release.set()
        reader.join(5)
        writer.join(5)
    assert not reader.is_alive() and not writer.is_alive()


def test_paper_cached_read_never_consults_plugin_or_persistence():
    paper = PaperTradingService(StrategyRegistry().create, create_features)
    paper.command("paper.account.create", {"account_id": "a", "balance": "1000", "fee_rate": "0"})
    paper.command(
        "paper.strategy.add",
        {
            "instance": "s",
            "account_id": "a",
            "instrument": "binance:usdm:BTCUSDT",
            "module_id": "target_quantity",
            "budget": "100",
            "configuration": {"quantity": "-1"},
        },
    )
    paper.data["strategies"]["s"].update(quantity="-1", entry_price="100")

    class UnreadablePlugin:
        @property
        def history_priming_ready(self):
            pytest.fail("device read invoked plugin")

    paper.engines["s"].module = UnreadablePlugin()
    paper.persist = lambda *args: pytest.fail("device read persisted financial state")
    before = deepcopy(paper.data)
    view = paper_device_snapshot(paper.readonly_snapshot())
    assert view["accounts"] == [
        {
            "account_id": "a",
            "positions": [
                {
                    "instrument": "binance:usdm:BTCUSDT",
                    "quantity": "-1",
                    "entry_price": "100",
                }
            ],
        }
    ]
    assert paper.data == before


@pytest.mark.parametrize("quantity", ["NaN", "Infinity", "1e999999", True, {}, "private-text"])
def test_invalid_paper_position_is_unknown_and_private_fields_are_not_copied(quantity):
    value = {
        "accounts": [
            {
                "account_id": "a",
                "secret": "PRIVATE",
                "positions": [
                    {
                        "instrument": "binance:usdm:BTCUSDT",
                        "quantity": quantity,
                        "entry_price": "100",
                        "secret": "PRIVATE",
                    }
                ],
            }
        ]
    }
    result = paper_device_snapshot(value)
    assert result["accounts"] == [{"account_id": "a", "positions": None}]
    assert "PRIVATE" not in str(result)


def test_forged_margin_complete_summary_cannot_hide_an_unknown_account():
    value = portfolio()
    value["accounts"][0].update(positions=None, position_status="unknown")
    assert sanitize_margin_portfolio(value)["summary"]["complete"] is False


@pytest.mark.parametrize(
    "flag,value",
    [
        ("_recording_available", False),
        ("_recording_manual_resume_required", True),
        ("_shutdown_uncertain", True),
        ("_owner_retired", True),
    ],
)
def test_uncertain_paper_owner_cannot_report_confirmed_flatness(flag, value):
    paper = PaperTradingService(StrategyRegistry().create, create_features)
    paper.command("paper.account.create", {"account_id": "a", "balance": "1000", "fee_rate": "0"})
    setattr(paper, flag, value)
    assert paper.readonly_snapshot()["accounts"] == [{"account_id": "a", "positions": None}]
