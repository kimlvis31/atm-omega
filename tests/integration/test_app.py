"""Composition checks through authenticated dispatch, with no server or real DB."""

import json
from copy import deepcopy
from decimal import Decimal

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime import app

INSTRUMENT = "binance:spot:BTCUSDT"


def test_bootstrap_evidence_budget_reaches_actual_service_without_loading_model(tmp_path):
    (tmp_path / "master.config.toml").write_text(
        "[neural_evidence]\nmax_bytes=8388608\nmax_records=100\nmax_pins=10\nwarning_percent=70\n"
    )
    service = app.build_core(tmp_path)
    try:
        snapshot = service.causal_inference.snapshot()["evidence_storage"]
        assert snapshot["limits"] == {
            "max_bytes": 8388608,
            "max_records": 100,
            "max_pins": 10,
            "warning_percent": 70,
        }
        assert snapshot["state"] == "unavailable" and snapshot["charged_bytes"] is None
        assert service.causal_inference.thread is None
    finally:
        service.close()


def command(core, commandID, kind, payload=None):
    return core.request(
        "POST",
        "/api/commands",
        core.token,
        {"id": commandID, "kind": kind, "payload": payload or {}},
    )


def resume_payload(core):
    state = core.request("GET", "/api/state", core.token)["core"]
    return {
        "expected_epoch": state["epoch"],
        "expected_revision": state["control_revision"],
        "expires_at_ms": state["server_time_ms"] + 20_000,
    }


def step(core, minute):
    return command(
        core,
        f"step-{minute}",
        "paper.step",
        {
            "instrument": INSTRUMENT,
            "candle": {
                "open_time_ms": minute * 60_000,
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100",
                "volume": "10",
            },
        },
    )


def add_strategy(core):
    assert (
        command(
            core,
            "account",
            "paper.account.create",
            {
                "account_id": "a",
                "balance": "1000",
                "fee_rate": "0.001",
            },
        )["status"]
        == "completed"
    )
    assert (
        command(
            core,
            "strategy",
            "paper.strategy.add",
            {
                "instance": "s",
                "account_id": "a",
                "instrument": INSTRUMENT,
                "module_id": "target_quantity",
                "budget": "500",
                "configuration": {"quantity": "1"},
            },
        )["status"]
        == "completed"
    )


@pytest.fixture
def core(tmp_path):
    service = app.build_core(tmp_path)
    try:
        yield service
    finally:
        service.close()


def test_composed_dispatch_pause_preserves_pending_and_duplicate_is_read_only(core):
    with pytest.raises(PermissionError):
        core.request("GET", "/api/state", "invalid")
    state = core.request("GET", "/api/state", core.token)
    assert state["core"]["paused"]
    assert state["core"]["capabilities"] == {
        "shutdown_management": True,
        "paper": True,
        "research": True,
        "margin_simulation": True,
        "margin_funding": True,
        "margin_funding_scope": "minute_boundary_scenario_rate_v1",
        "margin_cost_policy": True,
        "margin_cost_policy_scope": "fixed_adverse_full_fill_delay_v1",
        "margin_native_observation_replay": True,
        "margin_native_observation_replay_scope": "native_trade_flow_available_cutoff_v1",
        "native_observation_bundle_v2": True,
        "native_observation_bundle_v2_scope": "recorded_native_inputs_v2",
        "margin_native_observation_replay_v2": True,
        "margin_native_observation_replay_v2_scope": "native_auxiliary_available_cutoff_v2",
        "margin_analysis_execution": True,
        "margin_analysis_execution_scope": "required_fields_v1",
        "auxiliary_export": True,
        "market": True,
        "database": True,
        "analysis_library": True,
        "analysis_execution": True,
        "auxiliary_jobs": False,
        "auxiliary_schedule": False,
        "analysis_auxiliary_inputs": True,
        "paper_auxiliary_inputs": True,
        "venue_rules": True,
        "paper_feed": True,
        "calendar_bars": True,
        "chart_series": True,
        "chart_analysis_kinds": ["WOI", "MST", "STS"],
        "chart_observations": True,
        "shadow_inference": True,
        "causal_inference": True,
        "margin_trading": True,
        "depth_collection": False,
    }
    assert not state["core"]["live_orders_enabled"]
    modules = core.request("GET", "/api/modules", core.token)
    assert {item["id"] for item in modules["ts"]} >= {"tef", "target_quantity"}
    add_strategy(core)
    assert command(core, "resume", "resume", resume_payload(core))["status"] == "completed"
    assert step(core, 0)["status"] == "completed"
    pending = deepcopy(core.paper.data["strategies"]["s"]["pending"])
    assert pending["eligible_at_ms"] == 60_000
    assert core.paper.data["strategies"]["s"]["quantity"] == "0"
    assert command(core, "pause", "pause")["status"] == "completed"
    assert core.paper.data["strategies"]["s"]["pending"] == pending

    result = step(core, 1)
    assert result["status"] == "completed"
    row = core.paper.data["strategies"]["s"]
    assert Decimal(row["quantity"]) == 1
    assert row["pending"] is None
    assert len(row["fills"]) == 1
    assert Decimal(core.paper.data["accounts"]["a"]["balance"]) == Decimal("899.900")
    before = deepcopy(core.paper.data)
    sequence = core.control.snapshot()["sequence"]
    assert step(core, 1) == result
    assert core.request("GET", "/api/operations/step-1", core.token) == result
    assert core.paper.data == before
    assert core.control.snapshot()["sequence"] == sequence
    assert core.paper.data["paused"]
    market = core.request("GET", f"/api/candles?instrument={INSTRUMENT}&limit=1", core.token)
    assert market["source"] == "manual_paper"
    assert len(market["candles"]) == 1
    assert market["candles"][0]["open_time_ms"] == 60_000


def test_restart_changes_epoch_and_requires_manual_strategy_recovery(tmp_path):
    first = app.build_core(tmp_path)
    try:
        add_strategy(first)
        stale_resume = resume_payload(first)
        command(first, "resume", "resume", stale_resume)
        step(first, 0)
        step(first, 1)
        previous = deepcopy(first.paper.data)
        epoch = first.control.epoch
    finally:
        first.close()
    restarted = app.build_core(tmp_path)
    try:
        assert restarted.control.epoch != epoch
        assert restarted.state()["core"]["paused"]
        assert restarted.paper.data["paused"]
        row = restarted.paper.data["strategies"]["s"]
        assert row["status"] == "recovery_required"
        assert row["fills"] == previous["strategies"]["s"]["fills"]
        assert restarted.paper.data["accounts"] == previous["accounts"]
        with pytest.raises(ValidationError, match="epoch"):
            command(restarted, "stale-resume", "resume", stale_resume)
        assert step(restarted, 2)["status"] == "completed"
        assert restarted.paper.data["strategies"]["s"]["status"] == "recovery_required"
        assert (
            command(restarted, "recover", "paper.strategy.recover", {"instance": "s"})["status"]
            == "completed"
        )
        assert restarted.paper.data["paused"]
        assert (
            command(restarted, "fresh-resume", "resume", resume_payload(restarted))["status"]
            == "completed"
        )
        assert not restarted.paper.data["paused"]
        assert restarted.paper.data["strategies"]["s"]["status"] == "ready"
    finally:
        restarted.close()


def test_second_build_cannot_read_or_mutate_provider_state(core, tmp_path, monkeypatch):
    add_strategy(core)
    command(core, "resume", "resume", resume_payload(core))
    step(core, 0)
    command(
        core,
        "settings",
        "settings.patch",
        {
            "revision": 0,
            "changes": {"notifications": {"sound": False}},
        },
    )
    # A queued record would be rewritten as interrupted if ResearchService were opened again.
    job = tmp_path / "jobs" / ("a" * 32 + ".json")
    job.write_text(
        json.dumps({"id": "a" * 32, "kind": "export", "state": "queued", "created_at_ms": 0}),
        encoding="utf-8",
    )
    before = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file() and path.name != "core.lock"
    }
    paper_before = deepcopy(core.paper.data)

    def forbidden(*args, **kwargs):
        pytest.fail("second core reached a provider before acquiring its owner lock")

    monkeypatch.setattr(app, "PaperTradingService", forbidden)
    monkeypatch.setattr(app, "ResearchService", forbidden)
    with pytest.raises(RuntimeError, match="already owns"):
        app.build_core(tmp_path)
    after = {
        path.relative_to(tmp_path): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file() and path.name != "core.lock"
    }
    assert after == before
    assert core.paper.data == paper_before
    assert not core.paper.data["paused"]


def test_failed_build_releases_owner_lock(tmp_path, monkeypatch):
    original = app.ResearchService

    def fail(_root, **_kwargs):
        raise OSError("research initialization failed")

    monkeypatch.setattr(app, "ResearchService", fail)
    with pytest.raises(OSError, match="research initialization"):
        app.build_core(tmp_path)
    monkeypatch.setattr(app, "ResearchService", original)
    service = app.build_core(tmp_path)
    service.close()


def test_demo_is_explicit_and_does_not_open_database(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("demo attempted to read database configuration")

    monkeypatch.setattr(app, "load_database_settings", forbidden)
    service = app.build_core(tmp_path, demo=True)
    try:
        data = service.request(
            "GET", f"/api/candles?instrument={INSTRUMENT}&limit=3", service.token
        )
        assert data["source"] == "demo"
        assert data["quality"]["status"] == "synthetic_demo"
        assert len(data["candles"]) == 3
        assert all(
            b["open_time_ms"] - a["open_time_ms"] == 60_000
            for a, b in zip(data["candles"], data["candles"][1:], strict=False)
        )
        assert service.state()["market"]["demo"] is True
        assert service.paper.data["candles"] == {}
    finally:
        service.close()


def test_unavailable_database_is_not_replaced_with_demo(core, monkeypatch):
    calls = []

    def fail(path):
        calls.append(path)
        raise StorageError("database unavailable")

    monkeypatch.setattr(app, "load_database_settings", fail)
    for _ in range(2):
        result = core.request("GET", f"/api/candles?instrument={INSTRUMENT}", core.token)
        assert result["candles"] == []
        assert result["source"] == "database"
        assert result["quality"]["status"] == "unavailable"
    assert len(calls) == 1
    assert core.state()["market"]["demo"] is False


def test_market_database_snapshot_reports_gaps_caches_and_closes_store(core, monkeypatch):
    instrument = Instrument("binance", Market.SPOT, "BTCUSDT")
    candles = tuple(
        Candle(
            instrument,
            minute * 60_000,
            (minute + 1) * 60_000,
            Decimal(100),
            Decimal(101),
            Decimal(99),
            Decimal(100),
            Decimal(10),
        )
        for minute in (0, 2)
    )
    calls = []

    class Store:
        def __init__(self, settings):
            assert settings == "isolated mock settings"

        def latest(self, instrument, limit):
            calls.append((instrument, limit))
            return candles

        def close(self):
            calls.append("closed")

    monkeypatch.setattr(app, "load_database_settings", lambda _path: "isolated mock settings")
    monkeypatch.setattr(app, "PostgresCandleStore", Store)
    first = core.request("GET", f"/api/candles?instrument={INSTRUMENT}&limit=2", core.token)
    assert first["source"] == "database"
    assert first["quality"] == {"status": "gapped", "missing_minutes": 1, "interpolation_rate": 0}
    assert [r["open_time_ms"] for r in first["candles"]] == [0, 120_000]
    assert core.request("GET", f"/api/candles?instrument={INSTRUMENT}&limit=2", core.token) == first
    assert calls == [(instrument, 2), "closed"]
    assert core.state()["market"]["status"] == "available"
