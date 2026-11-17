"""Local module stop clearing is an explicit operation, separate from account resume."""

import json
import os
from copy import deepcopy
from hashlib import sha256

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

from omega.gui.margin_page import BASE, MarginPage


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


def account(identity="account", **changes):
    return {
        "account_id": identity,
        "state": "ready",
        "paused": False,
        "pending_frame": False,
        "strategies": [{"id": "s1", "state": "ready"}, {"id": "s2", "state": "ready"}],
        "pending_orders": ["accepted_original"],
        "shutdown": {"active": False, "recovery_required": False, "needs_settlement": False},
        **changes,
    }


def registry(*rows, epoch="core"):
    return {
        "core_epoch": epoch,
        "state": "ready",
        "accounts": list(rows),
        "operations": [],
        "capabilities": {"module_local_resume": True},
    }


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    return QApplication.instance() or QApplication(["local-stop-controls"])


@pytest.fixture
def page(app):
    value = MarginPage(Client())
    value.timer.stop()
    value.update_state({"core": {"epoch": "core", "capabilities": {"margin_trading": True}}})
    value.apply_snapshot(registry(account()))
    value.account_table.selectRow(0)
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def posts(page):
    return [call for call in page.client.calls if call[0] == "POST"]


def choose(page, identity="s1"):
    page.local_strategy.setCurrentIndex(page.local_strategy.findData(identity))


def test_default_requires_explicit_strategy_and_dispatch_is_exact_owner_command(page):
    assert page.local_strategy.count() == 3
    assert page.local_strategy.currentData() is None
    assert not page.resume_strategy_button.isEnabled()
    page.resume_strategy()
    assert not posts(page)
    choose(page, "s2")
    assert page.resume_strategy_button.isEnabled()
    page.resume_strategy_button.click()
    assert len(posts(page)) == 1
    method, path, body, _ = posts(page)[0]
    assert (method, path, body["kind"], body["core_epoch"]) == (
        "POST",
        BASE,
        "resume_strategy",
        "core",
    )
    assert body["parameters"] == {"account_id": "account", "strategy_id": "s2"}
    assert not page.resume_strategy_button.isEnabled()
    assert not page.resume_button.isEnabled()
    page.resume_strategy()
    page.control("resume")
    assert len(posts(page)) == 1


def test_account_switch_removal_epoch_and_disconnect_clear_strategy_choice(page):
    choose(page)
    page.apply_snapshot(registry(account(), account("second")))
    assert page.local_strategy.currentData() == "s1"
    page.account_table.selectRow(1)
    assert page.local_strategy.currentData() is None
    choose(page)
    page.apply_snapshot(registry(account("second", strategies=[{"id": "s2", "state": "ready"}])))
    assert page.local_strategy.currentData() is None
    choose(page, "s2")
    page.set_attached(False)
    assert page.local_strategy.currentData() is None
    page.set_attached(True)
    page.apply_snapshot(registry(account("second")))
    choose(page)
    page.update_state({"core": {"epoch": "new", "capabilities": {"margin_trading": True}}})
    assert page.local_strategy.currentData() is None
    assert not page.resume_strategy_button.isEnabled()
    assert not posts(page)


@pytest.mark.parametrize(
    "change",
    [
        {"pending_frame": True},
        {"state": "recovery_required"},
        {"manual_recovery_required": True},
        {"shutdown": {"active": True}},
        {"shutdown": {"needs_settlement": True}},
        {"shutdown": {"recovery_required": True}},
        {"strategies": [{"id": "s1", "state": "recovery_required"}]},
    ],
)
def test_local_resume_blocks_during_unconfirmed_or_shutdown_financial_work(page, change):
    page.apply_snapshot(registry(account(**change)))
    choose(page)
    assert not page.resume_strategy_button.isEnabled()
    page.resume_strategy()
    assert not posts(page)


@pytest.mark.parametrize("strategies", [None, [{"id": "../escape"}], [{"id": "s1"}] * 2])
def test_invalid_strategy_snapshot_never_becomes_a_control_target(page, strategies):
    page.apply_snapshot(registry(account(strategies=strategies)))
    assert page.local_strategy.count() == 1
    assert not page.resume_strategy_button.isEnabled()


def test_unknown_local_clear_preserves_original_id_and_only_polls_never_reposts(page):
    choose(page)
    page.resume_strategy()
    original = deepcopy(page._pending)
    page.client.failed.emit(posts(page)[0][3], "timeout")
    assert page._pending["id"] == original["id"] and page._pending["unknown"]
    page._tick()
    page.resume_strategy()
    page.control("resume")
    page.query_operation()
    assert page.client.calls[-1][1] == BASE + "/" + original["id"]
    assert len(posts(page)) == 1
    assert page._pending["parameters"] == {"account_id": "account", "strategy_id": "s1"}


def test_confirmed_local_clear_does_not_send_account_or_global_resume_or_cancel_original(page):
    choose(page)
    page.resume_strategy()
    call, pending = posts(page)[0], deepcopy(page._pending)
    digest = sha256(
        json.dumps(
            (pending["kind"], pending["parameters"]), sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    page.client.resultReceived.emit(
        call[3],
        {
            "id": pending["id"],
            "kind": "resume_strategy",
            "core_epoch": "core",
            "operation_core_epoch": "core",
            "state": "completed",
            "request_digest": digest,
            "result": {"account_id": "account", "strategy_id": "s1"},
            "error": None,
        },
    )
    assert page._pending is None
    page.apply_snapshot(registry(account(paused=True)))
    assert page.selected_account()["paused"] is True
    assert page.selected_account()["pending_orders"] == ["accepted_original"]
    assert len(posts(page)) == 1 and posts(page)[0][2]["kind"] == "resume_strategy"
    page.set_presentation("light", "en")
    assert "local stop" in page.resume_strategy_button.text()
    assert page.local_strategy.currentData() == "s1"


@pytest.mark.parametrize(
    "capabilities", [{}, {"module_local_resume": False}, {"module_local_resume": 1}, None]
)
def test_missing_or_non_native_capability_disables_and_explains_local_stop_control(
    page, capabilities
):
    value = registry(account())
    value["capabilities"] = capabilities
    page.apply_snapshot(value)
    choose(page)
    assert not page.resume_strategy_button.isEnabled()
    page.set_presentation("light", "en")
    assert "does not support" in page.local_resume_note.text()
    page.resume_strategy()
    assert not posts(page)


def test_closed_page_never_dispatches_delayed_local_resume_click(page):
    choose(page)
    assert page.resume_strategy_button.isEnabled()
    page.close()
    page.resume_strategy()
    assert not posts(page)
