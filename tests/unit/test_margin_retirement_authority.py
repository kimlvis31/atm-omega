"""Actual file/lease retirement transactions; no financial replay or plugin IO."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from functools import partial

import pytest
from test_margin_resource_cleanup import fault_once, financial_bytes, probe
from test_margin_session import BTC, ETH, bars, configuration, run, tables
from test_margin_strategy_costs import policy

from omega.application.margin_funding import FundingPoint, FundingSchedule
from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.ports.journal import Durability
from omega.runtime import margin_retirement
from omega.runtime.margin_retirement import MarginRetirement, RetirementPlan, retirement_sha256
from omega.runtime.margin_session import MarginTradingSession
from omega.runtime.margin_store import account_from_document


def create(root, *, mode="cross"):
    account, strategies = configuration()
    for position in account["positions"]:
        position["margin_mode"] = mode
        if mode == "isolated":
            position["isolated_collateral"] = "100"
    session = MarginTradingSession(root, account, strategies, 0, create=True)
    parameters = {"account": account, "strategies": strategies, "start_ms": 0, "brackets": []}
    helper = MarginRetirement(root, account["account_id"])
    return session, parameters, helper


def planned(session, parameters, helper):
    observation = session.retirement_snapshot(parameters)
    request = {
        "operation_id": "retire-original",
        "core_epoch": "original-core",
        "issued_at_ms": 300000,
    }
    return helper.make_plan(request, parameters, observation)


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_preflight_plan_is_readonly_and_native_retirement_preserves_original_bytes(
    tmp_path, monkeypatch, mode
):
    session, parameters, helper = create(tmp_path, mode=mode)
    original = financial_bytes(tmp_path)
    try:
        for module in session._modules.values():
            monkeypatch.setattr(module, "snapshot", lambda: pytest.fail("module callback"))
            monkeypatch.setattr(module, "checkpoint", lambda: pytest.fail("module callback"))
        for graph in session._graphs.values():
            monkeypatch.setattr(graph, "checkpoint", lambda: pytest.fail("graph callback"))
        plan = planned(session, parameters, helper)
        assert financial_bytes(tmp_path) == original and not helper.path.exists()
        assert "account" not in plan.document["observation"] and "account" in plan.archive
        assert helper.prepare(plan)["state"] == "prepared"
        assert probe(session.lock_path) == "held"
        assert helper.close(session)["state"] == "closed"
        assert probe(session.lock_path) == "released"
        receipt = helper.finish()
        assert receipt["state"] == "retired" and receipt["request"] == plan.document["request"]
        assert helper.finish() == receipt
        assert helper.archive()["account"] == plan.archive["account"]
        for relative, raw in original.items():
            assert (tmp_path / relative).read_bytes() == raw
    finally:
        session.close()


@pytest.mark.parametrize("when", ["before", "after"])
def test_first_native_close_error_stays_prepared_until_same_owner_explicit_retry(
    tmp_path, monkeypatch, when
):
    session, parameters, helper = create(tmp_path)
    plan = planned(session, parameters, helper)
    helper.prepare(plan)
    lease = session._resources[-1].owner
    calls = fault_once(lease, when, monkeypatch)
    try:
        with pytest.raises(StorageError):
            helper.close(session)
        assert helper.read()["state"] == "prepared" and len(calls) == 1
        with pytest.raises(TradingBlocked):
            session.resume("must-stay-closed")
        assert helper.close(session)["state"] == "closed"
        assert len(calls) == (2 if when == "before" else 1)
        assert helper.finish()["state"] == "retired"
    finally:
        session.close()


@pytest.mark.parametrize("phase", ["archive", "prepared", "closed", "retired"])
@pytest.mark.parametrize("when", ["before", "after"])
def test_each_write_ack_loss_retries_exact_original_without_financial_callbacks(
    tmp_path, monkeypatch, phase, when
):
    session, parameters, helper = create(tmp_path)
    plan = planned(session, parameters, helper)
    original, failures = margin_retirement.atomic_json, []

    def write(path, value):
        selected = "archive" if path == helper.archive_path else value["state"]
        if selected == phase and not failures:
            failures.append(deepcopy(value))
            if when == "after":
                original(path, value)
            raise OSError("injected original write acknowledgement loss")
        return original(path, value)

    monkeypatch.setattr(margin_retirement, "atomic_json", write)
    try:
        action = partial(helper.prepare, plan)
        if phase in {"closed", "retired"}:
            helper.prepare(plan)
            action = partial(helper.close, session)
        if phase == "retired":
            helper.close(session)
            action = helper.finish
        with pytest.raises(OSError):
            action()
        assert failures
        result = action()
        assert result["request"] == plan.document["request"]
        if phase in {"archive", "prepared"}:
            helper.close(session)
        if phase != "retired":
            helper.finish()
        assert helper.read()["state"] == "retired"
    finally:
        session.close()


@pytest.mark.parametrize("phase", ["fence_only", "prepared", "closed", "retired"])
def test_restart_recovery_uses_exact_fence_and_leases_without_session_construction(
    tmp_path, monkeypatch, phase
):
    session, parameters, helper = create(tmp_path)
    plan = planned(session, parameters, helper)
    if phase != "fence_only":
        helper.prepare(plan)
    if phase in {"closed", "retired"}:
        helper.close(session)
    if phase == "retired":
        helper.finish()
    session.close()  # models process-exit release, not a durable retired claim
    original = financial_bytes(tmp_path)
    monkeypatch.setattr(
        MarginTradingSession, "__init__", lambda *args, **kwargs: pytest.fail("reopened Session")
    )
    recovered = MarginRetirement(tmp_path, parameters["account"]["account_id"])
    result = recovered.recover(json.loads(json.dumps(plan.document)), parameters)
    assert result["state"] == "retired" and result["request"] == plan.document["request"]
    assert result["release_evidence"]["method"] == (
        "recovery_exclusive_reacquire"
        if phase in {"fence_only", "prepared"}
        else "owned_native_cleanup"
    )
    for relative, raw in original.items():
        if "margin-retired" not in relative:
            assert (tmp_path / relative).read_bytes() == raw


@pytest.mark.parametrize("fault", ["unpaused", "pending_frame", "source_unknown", "staged", "open"])
def test_uncertain_or_nonflat_preflight_is_rejected_without_retirement_files(tmp_path, fault):
    session, parameters, helper = create(tmp_path)
    try:
        if fault == "unpaused":
            session.resume("start")
        elif fault == "pending_frame":
            session._meta["pending"] = {"unconfirmed": True}
        elif fault == "source_unknown":
            session.execution._state = "unavailable"
        elif fault == "staged":
            next(iter(session._adapters.values()))._state = "unavailable"
        else:
            session.resume("start")
            run(session, 0)
            session.pause("paused")
            run(session, 1)
        with pytest.raises(TradingBlocked):
            planned(session, parameters, helper)
        assert not helper.path.exists() and not helper.archive_path.exists()
    finally:
        session.close()


def test_source_control_change_without_new_ledger_fact_invalidates_frozen_plan(tmp_path):
    session, parameters, helper = create(tmp_path)
    try:
        first = session.execution.confirmed_source_observation()
        plan = planned(session, parameters, helper)
        session.pause("another-explicit-control")
        second = session.execution.confirmed_source_observation()
        assert first[0] == second[0] and first[-1] == second[-1]
        assert first[1:4] != second[1:4]
        with pytest.raises(ProtocolError, match="source evidence changed"):
            helper.prepare(plan)
        assert not session._closed and not helper.path.exists()
    finally:
        session.close()


def test_conflicting_plan_and_same_account_changed_source_are_never_overwritten(tmp_path):
    session, parameters, helper = create(tmp_path)
    try:
        plan = planned(session, parameters, helper)
        helper.prepare(plan)
        original = helper.path.read_bytes()
        conflicting = deepcopy(plan.document)
        conflicting["request"]["operation_id"] = "another"
        conflicting["sha256"] = retirement_sha256(
            {key: value for key, value in conflicting.items() if key != "sha256"}
        )
        with pytest.raises(ProtocolError):
            helper.prepare(RetirementPlan(conflicting, plan.archive))
        assert helper.path.read_bytes() == original
        session.path.write_bytes(session.path.read_bytes() + b" ")
        with pytest.raises(ProtocolError):
            helper.close(session)
        assert helper.path.read_bytes() == original and not session._closed
    finally:
        session.close()


@pytest.mark.parametrize("fault", ["account", "metadata", "checkpoint"])
def test_plan_cannot_bind_changed_files_to_an_older_confirmed_cache(tmp_path, fault):
    session, parameters, helper = create(tmp_path)
    observation = session.retirement_snapshot(parameters)
    try:
        if fault == "account":
            path = session._resources[2].path
        elif fault == "metadata":
            path = session.path
        else:
            path = session._resources[-1].path
        document = json.loads(path.read_bytes())
        if fault == "account":
            document["data"]["account"]["cross_wallet"] = "123"
        elif fault == "metadata":
            document["data"]["cursor_ms"] = 60000
        else:
            document["data"]["schema_version"] = 99
        document["sha256"] = retirement_sha256(document["data"])
        path.write_text(json.dumps(document))
        with pytest.raises((ProtocolError, StorageError, ValueError, ValidationError)):
            helper.make_plan(
                {"operation_id": "retire", "core_epoch": "original", "issued_at_ms": 0},
                parameters,
                observation,
            )
        assert not session._closed and not helper.path.exists()
    finally:
        session.close()


def test_fake_closed_authority_cannot_upgrade_empty_native_resource_proof(tmp_path):
    session, parameters, helper = create(tmp_path)
    try:
        plan = planned(session, parameters, helper)
        authority = helper.prepare(plan)
        authority.update(
            state="closed",
            release_evidence={
                "method": "owned_native_cleanup",
                "generation": authority["cleanup_generation"],
                "resources": [],
            },
        )
        authority["sha256"] = retirement_sha256(
            {key: value for key, value in authority.items() if key != "sha256"}
        )
        helper.path.write_text(json.dumps(authority))
        with pytest.raises(ProtocolError):
            helper.finish()
        assert probe(session.lock_path) == "held"
    finally:
        session.close()


def test_incomplete_recovery_acquisition_is_not_retried_as_complete_and_cleanup_is_explicit(
    tmp_path, monkeypatch
):
    session, parameters, helper = create(tmp_path)
    plan = planned(session, parameters, helper)
    session.close()
    recovered = MarginRetirement(tmp_path, parameters["account"]["account_id"])
    original, calls = margin_retirement.InstanceLock, []

    def acquire(path):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("injected journal lease acquisition failure")
        return original(path)

    monkeypatch.setattr(margin_retirement, "InstanceLock", acquire)
    try:
        with pytest.raises(OSError):
            recovered.recover(plan.document, parameters)
        assert probe(session.lock_path) == "held"
        with pytest.raises(StorageError, match="incomplete"):
            recovered.recover(plan.document, parameters)
        assert len(calls) == 2 and not helper.path.exists()
        recovered.close_resources()
        assert probe(session.lock_path) == "released"
        assert recovered.cleanup_status()["state"] == "complete"
        assert not helper.path.exists()  # resource cleanup does not retire
    finally:
        recovered.close_resources()


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_actual_flat_financial_shutdown_funding_cost_history_retires_without_model_callbacks(
    tmp_path, monkeypatch, mode
):
    account, strategies = configuration()
    for position in account["positions"]:
        position["margin_mode"] = mode
        if mode == "isolated":
            position["isolated_collateral"] = "100"
    funding = FundingSchedule(
        tuple(
            FundingPoint(instrument, 120000, 120000, 120000, Decimal(".001"), Decimal(100))
            for instrument in (BTC, ETH)
        )
    ).document()
    selected = policy(entry_delay_ms=0).document()
    parameters = {
        "account": account,
        "strategies": strategies,
        "start_ms": 0,
        "brackets": [],
        "funding": funding,
        "cost_policy": selected,
    }
    session = MarginTradingSession(
        tmp_path, account, strategies, 0, create=True, funding=funding, cost_policy=selected
    )
    helper = MarginRetirement(tmp_path, account["account_id"])
    try:
        session.resume("start")
        run(session, 0)
        session.pause("pause")
        run(session, 1)
        session.shutdown_drain("exit", 120100, brackets=tables())
        session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
        assert (
            session.shutdown_run_once(bars(3), as_of_ms=240000, brackets=tables())["state"]
            == "complete"
        )
        original_account = session.execution.snapshot().account
        assert len(original_account.ledger) == 6
        for module in session._modules.values():
            monkeypatch.setattr(module, "snapshot", lambda: pytest.fail("module snapshot"))
            monkeypatch.setattr(module, "checkpoint", lambda: pytest.fail("module checkpoint"))
        for graph in session._graphs.values():
            monkeypatch.setattr(graph, "checkpoint", lambda: pytest.fail("graph checkpoint"))
        plan = planned(session, parameters, helper)
        helper.prepare(plan)
        helper.close(session)
        helper.finish()
        assert account_from_document(helper.archive()["account"]) == original_account
        assert session._shutdown.path.relative_to(tmp_path).as_posix() in {
            row["path"] for row in plan.document["source_files"]
        }
    finally:
        session.close()


def test_retirement_snapshot_is_confirmed_memory_only_without_file_or_journal_reads(
    tmp_path, monkeypatch
):
    session, parameters, _helper = create(tmp_path)
    try:
        with monkeypatch.context() as guarded:
            guarded.setattr(
                margin_retirement.os, "open", lambda *args, **kwargs: pytest.fail("file read")
            )
            guarded.setattr(
                session._resources[1], "read", lambda *args: pytest.fail("journal read")
            )
            guarded.setattr(session, "_persist", lambda *args: pytest.fail("session write"))
            guarded.setattr(session.execution, "reconcile", lambda: pytest.fail("reconcile"))
            result = session.retirement_snapshot(parameters)
            assert result["paused"] is True and result["orders"] == result["staged"] == 0
            assert result["source"]["count"] >= 1 and result["ledger_total"] == 0
    finally:
        session.close()


@pytest.mark.parametrize("field", ["balance", "strategy", "analysis", "start", "fee"])
def test_same_ids_cannot_bind_altered_registered_settings_to_original_session(tmp_path, field):
    session, parameters, _helper = create(tmp_path)
    changed = deepcopy(parameters)
    if field == "balance":
        changed["account"]["balance"] = "1234"
    elif field == "strategy":
        changed["strategies"][0]["configuration"]["quantity"] = "2"
    elif field == "analysis":
        changed["strategies"][0]["analysis"] = {"ema_period": 3}
    elif field == "start":
        changed["start_ms"] = 60000
    else:
        changed["fee_rate"] = ".002"
    try:
        with pytest.raises(TradingBlocked, match="parameters differ"):
            session.retirement_snapshot(changed)
        assert session.retirement_snapshot(parameters)["paused"] is True
    finally:
        session.close()


@pytest.mark.parametrize("when", ["before", "after"])
def test_successful_native_close_is_never_recalled_for_closed_authority_ack_retry(
    tmp_path, monkeypatch, when
):
    session, parameters, helper = create(tmp_path)
    helper.prepare(planned(session, parameters, helper))
    original_close, original_write = session.close, margin_retirement.atomic_json
    close_calls, writes = [], []

    def close():
        close_calls.append(True)
        if len(close_calls) > 1:
            pytest.fail("confirmed native close callback repeated")
        return original_close()

    def write(path, value):
        if path == helper.path and value["state"] == "closed" and not writes:
            writes.append(deepcopy(value))
            if when == "after":
                original_write(path, value)
            raise OSError("lost closed-authority acknowledgement")
        if path == helper.path and value["state"] == "closed":
            assert value == writes[0]
        return original_write(path, value)

    monkeypatch.setattr(session, "close", close)
    monkeypatch.setattr(margin_retirement, "atomic_json", write)
    try:
        with pytest.raises(OSError):
            helper.close(session)
        assert helper.close(session) == writes[0]
        assert len(close_calls) == 1 and helper.finish()["state"] == "retired"
    finally:
        original_close()


def test_effective_fallback_pause_cannot_replace_explicit_financial_pause(tmp_path):
    session, parameters, _helper = create(tmp_path)
    try:
        original_history = session.execution._history
        session.execution._paused = False
        session.execution._history = [
            replace(receipt, durability=Durability.FALLBACK)
            for receipt in session.execution._history
        ]
        assert session.execution.snapshot().paused is True
        before = financial_bytes(tmp_path)
        assert len(session.execution.confirmed_source_observation()) == 5
        with pytest.raises(TradingBlocked, match="explicit financial pause"):
            session.retirement_snapshot(parameters)
        with pytest.raises(ValidationError, match="native boolean"):
            session.execution.confirmed_source_observation(require_paused=1)
        assert financial_bytes(tmp_path) == before
        # The observation-only fake classification is not a real journal ACK;
        # restore actual receipts before exercising the existing write path.
        session.execution._history = original_history
        session.pause("explicit-user-pause")
        observation = session.retirement_snapshot(parameters)
        assert observation["paused"] is True
    finally:
        session.close()
