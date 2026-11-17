"""Actual Core routes, financial owner, native files and archive reader together."""

import hashlib
import time

import pytest
from test_margin_session import configuration

from omega.domain.errors import StorageError
from omega.runtime.control import InstanceLock
from omega.runtime.margin_retirement import MarginRetirement
from omega.runtime.margin_retirement_client import (
    history_job_url,
    history_load_body,
    history_load_url,
    retirement_body,
    retirement_url,
)
from omega.runtime.margin_session import MarginTradingSession
from omega.runtime.margin_trading import MarginTradingService
from omega.runtime.service import CoreService


def wait(read, check):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        value = read()
        if check(value):
            return value
        time.sleep(0.01)
    raise AssertionError(value)


def financial_files(root):
    ignored = {"control.json", "margin-runtime.json", "settings.json", "core.json"}
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (root / "state").rglob("*.json")
        if "margin-retired" not in path.parts and path.name not in ignored
    }


def start(root, calls):
    core = CoreService(root)

    def forbidden_source():
        calls.append("source")
        raise AssertionError("a paused future account must not fetch market data")

    core.margin_trading = MarginTradingService(
        root,
        None,
        forbidden_source,
        core_epoch=core.control.snapshot()["epoch"],
        clock_ms=lambda: 0,
        mutation_lock=core.owner_mutation_lock,
    )
    core.margin_trading.start()
    wait(core.margin_trading.snapshot, lambda row: row["state"] == "ready")
    return core


def operation(core, identity, kind, parameters):
    epoch = core.control.snapshot()["epoch"]
    core.request(
        "POST",
        "/api/margin-trading",
        core.token,
        {"id": identity, "kind": kind, "parameters": parameters, "core_epoch": epoch},
    )
    return wait(
        lambda: core.request("GET", "/api/margin-trading/" + identity, core.token),
        lambda row: row["state"] in {"completed", "failed", "unknown"},
    )


def configure(core):
    account, rows = configuration()
    parameters = {
        "account": account,
        "strategies": rows,
        "start_ms": 60000,
        "fee_rate": "0.001",
        "brackets": [],
    }
    assert operation(core, "configure", "configure", parameters)["state"] == "completed"
    return parameters


def page(core, snapshot=None):
    path = "/api/margin-trading/accounts/session-test/ledger"
    if snapshot is not None:
        path += "?snapshot=" + snapshot
    return core.request("GET", path, core.token)


def test_real_owner_retirement_archive_load_and_restart_preserve_old_ledger(tmp_path, monkeypatch):
    calls, core = [], None
    try:
        core = start(tmp_path, calls)
        parameters = configure(core)
        # Explicit account pause retains the running feed for local protection.
        # Retirement itself establishes the host barrier only after flat proof.
        assert (
            operation(core, "resume", "resume", {"account_id": "session-test"})["state"]
            == "completed"
        )
        assert (
            operation(core, "pause", "pause", {"account_id": "session-test"})["state"]
            == "completed"
        )
        assert core.margin_trading.data["accounts"]["session-test"]["feed_enabled"] is True
        original_page, original_files = page(core), financial_files(tmp_path)
        epoch = core.control.snapshot()["epoch"]
        observed = core.request("GET", retirement_url("session-test"), core.token)
        assert observed["state"] == "ready" and observed["source_record_count"] >= 3
        body = retirement_body("retire", "session-test", observed["observation_sha256"], epoch)
        core.request("POST", "/api/margin-trading", core.token, body)
        done = wait(
            lambda: core.request("GET", "/api/margin-trading/retire", core.token),
            lambda row: row["state"] in {"completed", "failed", "unknown"},
        )
        assert done["state"] == "completed", done
        assert financial_files(tmp_path) == original_files and calls == []
        runtime = core.margin_trading
        assert runtime.sessions == runtime.views == {}
        assert runtime.data["schema_version"] == 2 and runtime.data["accounts"] == {}
        tombstone = runtime.data["retired"]["session-test"]
        assert tombstone["feed_enabled_before"] is True
        assert core.request("POST", "/api/margin-trading", core.token, body)["state"] == "completed"
        assert page(core)["reason_code"] == "archive_not_loaded"
        assert (
            core.request(
                "GET", "/api/margin-trading/accounts/session-test/configuration", core.token
            )["state"]
            == "unavailable"
        )
        lease = InstanceLock(
            tmp_path
            / "state/margin-sessions"
            / (hashlib.sha256(b"session-test").hexdigest() + ".lock")
        )
        lease.close()
        core.request(
            "POST",
            history_load_url(),
            core.token,
            history_load_body("read", "session-test", tombstone["receipt_sha256"], epoch),
        )
        loaded = wait(
            lambda: core.request("GET", history_job_url("read"), core.token),
            lambda row: row["state"] in {"completed", "failed"},
        )
        assert loaded["state"] == "completed" and loaded["cached"]
        archived_page = page(core, original_page["snapshot"])
        assert archived_page["state"] == "ready"
        assert archived_page["snapshot"] == original_page["snapshot"]
        copied = core.request(
            "GET", "/api/margin-trading/accounts/session-test/configuration", core.token
        )
        assert copied["parameters"] == parameters
        assert financial_files(tmp_path) == original_files and calls == []
        core.close()
        assert core.instance.released
        core = None

        def no_session(*_args, **_kwargs):
            raise AssertionError("a retired lineage must not open a financial Session")

        monkeypatch.setattr(MarginTradingService, "_open", no_session)
        core = start(tmp_path, calls)
        assert core.control.snapshot()["epoch"] != epoch
        assert core.margin_trading.sessions == {}
        historical = core.request("GET", "/api/margin-trading/retire", core.token)
        assert historical["state"] == "completed" and historical["operation_core_epoch"] == epoch
        assert historical["core_epoch"] == core.control.snapshot()["epoch"]
        assert page(core)["reason_code"] == "archive_not_loaded"
        rejected = operation(core, "reuse", "configure", parameters)
        assert rejected["state"] == "failed"
        assert financial_files(tmp_path) == original_files and calls == []
    finally:
        if core is not None:
            core.close()


def test_native_close_unknown_is_gated_until_explicit_same_operation_retry(tmp_path, monkeypatch):
    calls, core = [], start(tmp_path, [])
    original_close = MarginTradingSession.close

    def after_release(session):
        original_close(session)
        calls.append(session)
        if len(calls) == 1:
            raise StorageError("native release acknowledgement lost")

    try:
        configure(core)
        observed = core.request("GET", retirement_url("session-test"), core.token)
        source = financial_files(tmp_path)
        monkeypatch.setattr(MarginTradingSession, "close", after_release)
        parameters = {
            "account_id": "session-test",
            "expected_observation_sha256": observed["observation_sha256"],
        }
        unknown = operation(core, "retire", "retire", parameters)
        assert unknown["state"] == "unknown" and len(calls) == 1
        original_session = calls[0]
        assert core.margin_trading.sessions["session-test"] is original_session
        assert core.margin_trading.data.get("retired", {}) == {}
        assert financial_files(tmp_path) == source
        view = core.request("GET", retirement_url("session-test"), core.token)
        assert view["state"] == "retiring" and view["operation_id"] == "retire"
        assert view["observation_sha256"] == parameters["expected_observation_sha256"]
        assert (
            operation(core, "blocked", "resume", {"account_id": "session-test"})["state"]
            == "failed"
        )
        assert len(calls) == 1 and financial_files(tmp_path) == source
        done = operation(core, "retire", "retire", parameters)
        assert done["state"] == "completed", done
        assert calls == [original_session, original_session]
        assert core.margin_trading.sessions == {}
        assert core.margin_trading.data["operations"]["blocked"]["state"] == "failed"
        assert financial_files(tmp_path) == source
    finally:
        core.close()


@pytest.mark.parametrize("phase", ["fenced", "prepared", "closed"])
def test_actual_core_restart_finishes_fenced_retirement_before_opening_any_session(
    tmp_path, monkeypatch, phase
):
    calls, core = [], None
    try:
        core = start(tmp_path, calls)
        configure(core)
        observed = core.request("GET", retirement_url("session-test"), core.token)
        source = financial_files(tmp_path)
        epoch = core.control.snapshot()["epoch"]
        method = {"fenced": "prepare", "prepared": "close", "closed": "finish"}[phase]

        def stop_phase(*_args, **_kwargs):
            raise StorageError("controlled retirement interruption")

        with monkeypatch.context() as patch:
            patch.setattr(MarginRetirement, method, stop_phase)
            unknown = operation(
                core,
                "retire",
                "retire",
                {
                    "account_id": "session-test",
                    "expected_observation_sha256": observed["observation_sha256"],
                },
            )
            assert unknown["state"] == "unknown"
            assert "retirement_control" in core.margin_trading.data["accounts"]["session-test"]
            assert financial_files(tmp_path) == source
            authority = MarginRetirement(tmp_path, "session-test").read()
            assert authority is None if phase == "fenced" else authority["state"] == phase
            core.close()
            core = None

        def no_session(*_args, **_kwargs):
            raise AssertionError("retirement recovery must not reopen a financial Session")

        monkeypatch.setattr(MarginTradingService, "_open", no_session)
        core = start(tmp_path, calls)
        result = core.request("GET", "/api/margin-trading/retire", core.token)
        assert result["state"] == "completed", result
        assert result["operation_core_epoch"] == epoch
        assert result["core_epoch"] != epoch
        assert core.margin_trading.data["accounts"] == {} and core.margin_trading.sessions == {}
        receipt = MarginRetirement(tmp_path, "session-test").read()
        assert receipt["state"] == "retired" and receipt["request"]["core_epoch"] == epoch
        assert financial_files(tmp_path) == source and calls == []
    finally:
        if core is not None:
            core.close()
