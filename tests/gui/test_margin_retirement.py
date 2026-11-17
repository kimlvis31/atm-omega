"""Lifecycle GUI fences with strict production DTOs and actual account codec."""

import json
import os
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

from omega.domain.margin import MarginAccount
from omega.gui.margin_page import BASE, MarginPage
from omega.gui.margin_retirement import retired_accounts
from omega.runtime.margin_retirement_client import retirement_response
from omega.runtime.margin_store import account_document

EPOCH = "a" * 32


class Client(QObject):
    resultReceived = Signal(str, dict)
    failed = Signal(str, str)
    connectionChanged = Signal(bool)
    attached = True

    def __init__(self):
        super().__init__()
        self.calls = []

    def request(self, method, path, payload=None, *, tag):
        self.calls.append((method, path, deepcopy(payload), tag))


def active(identity="account"):
    return {
        "account_id": identity,
        "state": "ready",
        "paused": True,
        "pending_orders": [],
        "account": account_document(MarginAccount(identity, "USDT", Decimal("42.5"))),
        "strategies": [],
    }


def metadata(identity="archived", state="retired"):
    return {
        "account_id": identity,
        "state": state,
        "operation_id": "old-operation",
        "receipt_sha256": "f" * 64 if state == "retired" else None,
        "registered_parameters_sha256": "b" * 64,
        "session_config_hash": "c" * 64,
        "ledger_namespace": "d" * 64,
        "archive_sha256": "e" * 64,
        "ledger_total": 0,
        "reason_code": None,
    }


def registry(rows=None, retired=None, supported=True, epoch=EPOCH):
    return {
        "schema_version": 1,
        "core_epoch": epoch,
        "state": "ready",
        "accounts": [active()] if rows is None else rows,
        "retired_accounts": [] if retired is None else retired,
        "capabilities": {"retirement": supported},
        "operations": [],
    }


def observation(*, checksum="1" * 64, state="ready", operation=None, account_id="account"):
    return retirement_response(
        account_id,
        EPOCH,
        state=state,
        phase=None if state == "ready" else "retired" if state == "retired" else "fenced",
        observation_sha256=checksum,
        registered_parameters_sha256="b" * 64,
        session_config_hash="c" * 64,
        ledger_namespace="d" * 64,
        paused=True,
        open_positions=0,
        pending_orders=0,
        staged=0,
        source_pending=False,
        strategy_count=1,
        source_record_count=1,
        ledger_total=0,
        operation_id=operation,
        receipt_sha256="f" * 64 if state == "retired" else None,
    )


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    return QApplication.instance() or QApplication(["retirement-form"])


@pytest.fixture
def page(app):
    widget = MarginPage(Client())
    widget.update_state({"core": {"epoch": EPOCH, "capabilities": {"margin_trading": True}}})
    widget.apply_snapshot(registry())
    widget.account_table.selectRow(0)
    widget.set_presentation("light", "en")
    widget.resize(800, 1000)
    widget.show()
    assert not widget.retirement.isChecked()
    widget.retirement.setChecked(True)
    app.processEvents()
    widget.client.calls.clear()
    yield widget
    widget.close()
    widget.deleteLater()
    app.processEvents()


def reply(page, value, call=None):
    call = call or page.client.calls[-1]
    page.client.resultReceived.emit(call[3], value)


def begin(page):
    page.retirement.inspect()
    reply(page, observation())
    page.retirement.consent.setChecked(True)
    page.retirement.retire()
    assert page.client.calls[-1][0] == "GET"
    reply(page, observation())
    return page.client.calls[-1]


def operation(body, state):
    digest = sha256(
        json.dumps(
            (body["kind"], body["parameters"]),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()
    return {
        "id": body["id"],
        "core_epoch": EPOCH,
        "kind": "retire",
        "state": state,
        "request_digest": digest,
    }


def history(body, state="completed", cached=True):
    return {
        "schema_version": 1,
        "scope": "virtual_margin_retired_history",
        "core_epoch": EPOCH,
        "job_id": body["id"],
        "account_id": body["account_id"],
        "receipt_sha256": body["expected_receipt_sha256"],
        "state": state,
        "cached": cached,
        "ledger_total": 0,
        "reason_code": None,
    }


def test_initial_old_core_keeps_active_contract_and_starts_no_lifecycle_io(page):
    packet = registry(supported=False)
    packet.pop("retired_accounts")
    page.apply_snapshot(packet)
    assert page.lifecycle.currentData() == "active"
    assert len(page.accounts) == 1 and page.retired_accounts == {}
    assert not page.retirement.inspect_button.isEnabled()
    assert page.client.calls == []


def test_retire_requires_two_reads_explicit_consent_and_exact_owner_body(page):
    post = begin(page)
    assert [call[0] for call in page.client.calls] == ["GET", "GET", "POST"]
    assert post[:2] == ("POST", BASE)
    assert post[2] == {
        "id": page.retirement.pending["body"]["id"],
        "kind": "retire",
        "core_epoch": EPOCH,
        "parameters": {"account_id": "account", "expected_observation_sha256": "1" * 64},
    }
    assert not page.resume_button.isEnabled() and not page.brackets_button.isEnabled()
    assert not page.resume_strategy_button.isEnabled() and not page.configure_button.isEnabled()
    assert not page.retirement.consent.isChecked()
    assert all(call[2] is None or call[2].get("kind") == "retire" for call in page.client.calls)


def test_changed_fresh_observation_revokes_confirmation_without_post(page):
    page.retirement.inspect()
    reply(page, observation())
    page.retirement.consent.setChecked(True)
    page.retirement.retire()
    reply(page, observation(checksum="2" * 64))
    assert not any(call[0] == "POST" for call in page.client.calls)
    assert page.retirement.pending is None and not page.retirement.consent.isChecked()


def test_unknown_retire_never_autoposts_and_explicit_retry_reuses_original_fence(page):
    post = begin(page)
    original = deepcopy(post[2])
    page.client.failed.emit(post[3], "lost acceptance acknowledgement")
    before = len(page.client.calls)
    page.retirement.tick()
    assert len(page.client.calls) == before and page.retirement.pending["state"] == "unknown"
    page.retirement.retry()
    assert page.client.calls[-1][0] == "GET"
    reply(page, observation(state="retiring", operation=original["id"]))
    assert page.client.calls[-1][2] == original
    reply(page, operation(original, "queued"))
    page.retirement.tick()
    assert page.client.calls[-1][:2] == ("GET", BASE + "/" + original["id"])
    assert [call[2] for call in page.client.calls if call[0] == "POST"] == [original, original]


def test_completed_operation_requires_retired_receipt_before_finishing(page):
    post = begin(page)
    reply(page, operation(post[2], "completed"))
    assert page.retirement.busy and page.retirement.pending["state"] == "running"
    reply(page, observation(state="retired", operation=post[2]["id"]))
    assert page.retirement.pending["state"] == "completed"
    assert page.retirement.blocks("account") and not page.resume_button.isEnabled()


@pytest.mark.parametrize("change", ["selection", "epoch", "disconnect", "close"])
def test_late_inspection_cannot_authorize_a_new_context(page, change):
    page.retirement.inspect()
    call = page.client.calls[-1]
    if change == "selection":
        page.apply_snapshot(registry(rows=[active(), active("second")]))
        page.account_table.selectRow(1)
    elif change == "epoch":
        page.update_state({"core": {"epoch": "b" * 32, "capabilities": {"margin_trading": True}}})
    elif change == "disconnect":
        page.set_attached(False)
    else:
        page.close()
    reply(page, observation(), call)
    assert page.retirement.observation is None and page.retirement.pending is None
    assert not any(call[0] == "POST" for call in page.client.calls)


def test_retiring_reservation_disables_every_trading_action_while_still_active(page):
    page.apply_snapshot(registry(retired=[metadata("account", "retiring")]))
    page.account_table.selectRow(0)
    assert len(page.accounts) == 1 and len(page.retired_accounts) == 1
    for button in (
        page.pause_button,
        page.resume_button,
        page.reconcile_button,
        page.brackets_button,
        page.resume_strategy_button,
    ):
        assert not button.isEnabled()
    assert "1/10" in page.lifecycle.itemText(0)


def test_explicit_archive_load_gates_existing_readonly_ledger_and_configuration_paths(page):
    page.apply_snapshot(registry(retired=[metadata()]))
    page.lifecycle.setCurrentIndex(1)
    page.account_table.selectRow(0)
    assert page.selected_account()["account_id"] == "archived"
    assert not page.copy_configuration_button.isEnabled() and not page.ledger_button.isEnabled()
    assert "current amounts unconfirmed" in page.account_summary.text()
    assert "42.5" not in page.account_summary.text()
    before = len(page.client.calls)
    page.retirement.tick()
    assert len(page.client.calls) == before
    page.retirement.load()
    call = page.client.calls[-1]
    assert call[:2] == ("POST", BASE + "/history")
    reply(page, history(call[2], "queued", False))
    page.retirement.tick()
    assert page.client.calls[-1][:2] == ("GET", BASE + "/history/" + call[2]["id"])
    reply(page, history(call[2]))
    assert page.ledger_button.isEnabled() and page.copy_configuration_button.isEnabled()
    assert not page.resume_button.isEnabled() and not page.brackets_button.isEnabled()
    page.copy_configuration()
    assert page.client.calls[-1][:2] == ("GET", BASE + "/accounts/archived/configuration")


def test_history_unknown_is_not_loaded_or_reposted(page):
    page.apply_snapshot(registry(retired=[metadata()]))
    page.lifecycle.setCurrentIndex(1)
    page.account_table.selectRow(0)
    page.retirement.load()
    call = page.client.calls[-1]
    page.client.failed.emit(call[3], "unknown job outcome")
    before = len(page.client.calls)
    page.retirement.tick()
    assert len(page.client.calls) == before
    assert not page.ledger_button.isEnabled() and not page.copy_configuration_button.isEnabled()


@pytest.mark.parametrize("bad", [{"state": []}, {"ledger_total": True}, {"receipt_sha256": None}])
def test_malformed_retired_metadata_preserves_previous_registry(page, bad):
    before = deepcopy(page.accounts)
    with pytest.raises(ValueError):
        page.apply_snapshot(registry(retired=[metadata() | bad]))
    assert page.accounts == before
    with pytest.raises(ValueError):
        retired_accounts([metadata() | bad])


def test_reconnect_or_receipt_change_revokes_loaded_archive_authority(page):
    page.apply_snapshot(registry(retired=[metadata()]))
    page.lifecycle.setCurrentIndex(1)
    page.account_table.selectRow(0)
    page.retirement.load()
    reply(page, history(page.client.calls[-1][2]))
    assert page.copy_configuration_button.isEnabled()
    page.set_attached(False)
    page.set_attached(True)
    page.apply_snapshot(registry(retired=[metadata()]))
    page.account_table.selectRow(0)
    assert not page.copy_configuration_button.isEnabled()


@pytest.mark.parametrize("change", ["selection", "epoch", "disconnect", "receipt"])
def test_late_history_response_cannot_unlock_another_archive_context(page, change):
    page.apply_snapshot(registry(retired=[metadata(), metadata("second")]))
    page.lifecycle.setCurrentIndex(1)
    page.account_table.selectRow(0)
    page.retirement.load()
    call = page.client.calls[-1]
    if change == "selection":
        page.account_table.selectRow(1)
    elif change == "epoch":
        page.update_state({"core": {"epoch": "b" * 32, "capabilities": {"margin_trading": True}}})
    elif change == "disconnect":
        page.set_attached(False)
    else:
        page.apply_snapshot(registry(retired=[metadata() | {"receipt_sha256": "9" * 64}]))
    reply(page, history(call[2]), call)
    assert not page.ledger_button.isEnabled() and not page.copy_configuration_button.isEnabled()
    assert not page.retirement.loaded


@pytest.mark.parametrize("bad", [{"paused": 1}, {"open_positions": False}, {"scope": "wrong"}])
def test_malformed_inspection_never_authorizes_retirement(page, bad):
    page.retirement.inspect()
    reply(page, observation() | bad)
    assert page.retirement.observation is None and not page.retirement.retire_button.isEnabled()
    assert not any(call[0] == "POST" for call in page.client.calls)


def test_unknown_epoch_change_keeps_original_body_and_blocks_replay_and_new_risk(page):
    post = begin(page)
    original = deepcopy(post[2])
    page.client.failed.emit(post[3], "unknown acceptance")
    page.update_state({"core": {"epoch": "b" * 32, "capabilities": {"margin_trading": True}}})
    page.apply_snapshot(registry(epoch="b" * 32))
    page.account_table.selectRow(0)
    before = len(page.client.calls)
    page.retirement.retry()
    page.retirement.tick()
    assert len(page.client.calls) == before and page.retirement.pending["body"] == original
    assert not page.configure_button.isEnabled() and not page.resume_button.isEnabled()


def test_archive_capability_loss_keeps_metadata_but_blocks_archive_read_actions(page):
    page.apply_snapshot(registry(retired=[metadata()]))
    page.lifecycle.setCurrentIndex(1)
    page.account_table.selectRow(0)
    page.retirement.load()
    reply(page, history(page.client.calls[-1][2]))
    assert page.ledger_button.isEnabled()
    page.apply_snapshot(registry(retired=[metadata()], supported=1))
    assert "archived" in page.retired_accounts
    assert not page.ledger_button.isEnabled() and not page.retirement.load_button.isEnabled()


@pytest.mark.parametrize("bad", ["old_epoch", "other_operation", "other_hash"])
def test_cross_epoch_readonly_reconciliation_requires_fresh_matching_retired_receipt(page, bad):
    post = begin(page)
    page.client.failed.emit(post[3], "unknown acceptance")
    page.update_state({"core": {"epoch": "b" * 32, "capabilities": {"margin_trading": True}}})
    page.apply_snapshot(registry(epoch="b" * 32))
    page.account_table.selectRow(0)
    page.retirement.query()
    call = page.client.calls[-1]
    assert call[:2] == ("GET", BASE + "/accounts/account/retirement")
    current = observation(state="retired", operation=post[2]["id"]) | {"core_epoch": "b" * 32}
    if bad == "old_epoch":
        current["core_epoch"] = EPOCH
    elif bad == "other_operation":
        current["operation_id"] = "other"
    else:
        current["session_config_hash"] = "9" * 64
    reply(page, current, call)
    assert page.retirement.busy and not page.resume_button.isEnabled()
    assert len([call for call in page.client.calls if call[0] == "POST"]) == 1


def test_fresh_cross_epoch_retired_receipt_clears_busy_readonly_without_replaying_post(page):
    post = begin(page)
    original = deepcopy(post[2])
    page.client.failed.emit(post[3], "unknown acceptance")
    page.update_state({"core": {"epoch": "b" * 32, "capabilities": {"margin_trading": True}}})
    page.apply_snapshot(registry(epoch="b" * 32))
    page.account_table.selectRow(0)
    page.retirement.query()
    reply(page, observation(state="retired", operation=post[2]["id"]) | {"core_epoch": "b" * 32})
    assert not page.retirement.busy and page.retirement.pending["state"] == "completed"
    assert page.retirement.pending["body"] == original
    assert page.retirement.blocks("account") and not page.resume_button.isEnabled()
    assert not page.retirement.loaded
    assert len([call for call in page.client.calls if call[0] == "POST"]) == 1
