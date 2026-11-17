"""Three explicit exit choices, receipt/epoch fences and GUI-only close behavior."""

import importlib.util
import os
from copy import deepcopy
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtGui import QFont, QFontDatabase
    from PySide6.QtWidgets import QApplication

    from omega.gui.presentation import configure_fonts

    value = QApplication.instance() or QApplication(["omega-shutdown-tests"])
    configure_fonts(value)
    font = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts/malgun.ttf"
    if font.is_file():
        identity = QFontDatabase.addApplicationFont(str(font))
        if identity >= 0:
            value.setFont(QFont(QFontDatabase.applicationFontFamilies(identity)[0], 10))
    return value


def core_state(epoch="core-a", enabled=True):
    return {"core": {"epoch": epoch, "capabilities": {"shutdown_management": enabled}}}


def summary(positions=0, pending=0, unknown=0, *, durable=True, flatten=True):
    return {
        "positions_count": positions,
        "pending_count": pending,
        "unknown_count": unknown,
        "durable": durable,
        "supports_flatten": flatten,
        "notes": [],
    }


def snapshot(revision=1, *, state="idle", exposure=None, operation_id=None, **changes):
    value = {
        "schema_version": 1,
        "core_epoch": "core-a",
        "revision": revision,
        "state": state,
        "operation_id": operation_id,
        "summary": summary() if exposure is None else exposure,
        "participants": [],
        "operations": [],
        "ready": True,
        "closed": False,
    }
    value.update(changes)
    return value


@pytest.fixture
def client(app):
    from PySide6.QtCore import QObject, Signal

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        connectionChanged = Signal(bool)
        stateReceived = Signal(dict)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls = []

        def request(self, method, path, payload=None, *, tag):
            self.calls.append((method, path, deepcopy(payload), tag))

    return Client()


@pytest.fixture
def dialog(client):
    from omega.gui.shutdown_dialog import ShutdownDialog

    view = ShutdownDialog(client, core_state())
    view.timer.stop()
    yield view
    view.close()
    view.deleteLater()


def receipt(dialog, state="completed", **changes):
    request = dialog._pending["payload"]
    value = {
        **deepcopy(request),
        "operation_core_epoch": request["core_epoch"],
        "state": state,
        "summary": None,
        "result": {},
        "error": None,
    }
    value.update(changes)
    return value


def emit(dialog, value):
    dialog.client.resultReceived.emit(dialog.client.calls[-1][3], value)


def reviewed(dialog, *, exposure=None):
    emit(dialog, snapshot(0, exposure=exposure))
    assert dialog._pending["payload"]["kind"] == "review"
    emit(dialog, receipt(dialog))
    emit(dialog, snapshot(1, exposure=exposure))


def posts(dialog):
    return [row for row in dialog.client.calls if row[0] == "POST"]


def test_open_reads_cached_state_then_explicit_review_never_saves_or_flattens(dialog):
    assert dialog.client.calls[0][:2] == ("GET", "/api/shutdown")
    assert not posts(dialog) and not dialog.save_button.isEnabled()
    reviewed(dialog)
    assert len(posts(dialog)) == 1
    body = posts(dialog)[0][2]
    assert set(body) == {"id", "core_epoch", "expected_revision", "kind", "parameters"}
    assert body["expected_revision"] == 0 and body["parameters"] == {}
    assert dialog.save_button.isEnabled() and dialog.cancel_button.isEnabled()


def test_starting_owner_waits_for_ready_snapshot_before_initial_review(dialog):
    emit(dialog, snapshot(0, state="starting", ready=False))
    assert not posts(dialog) and not dialog.save_button.isEnabled()
    dialog.refresh()
    emit(dialog, snapshot(1))
    assert len(posts(dialog)) == 1 and posts(dialog)[0][2]["kind"] == "review"


def test_explicit_position_review_uses_fresh_revision_instead_of_trusting_cached_facts(dialog):
    reviewed(dialog)
    dialog.refresh_button.click()
    emit(dialog, snapshot(4))
    assert posts(dialog)[-1][2]["kind"] == "review"
    assert posts(dialog)[-1][2]["expected_revision"] == 4
    assert len(posts(dialog)) == 2


def test_exposures_need_default_off_ack_for_save_and_exact_revision(dialog):
    reviewed(dialog, exposure=summary(1, 2))
    assert not dialog.acknowledge.isChecked() and not dialog.save_button.isEnabled()
    dialog.save_button.click()
    assert len(posts(dialog)) == 1
    dialog.acknowledge.setChecked(True)
    dialog.save_button.click()
    body = posts(dialog)[-1][2]
    assert body["kind"] == "save_exit" and body["expected_revision"] == 1
    assert body["parameters"] == {"acknowledge_unmanaged_positions": True}


def test_v2_active_exposure_blocks_flatten_but_known_save_and_cancel_remain(dialog):
    reviewed(dialog, exposure=summary(1, 1, flatten=False))
    assert not dialog.flatten_button.isEnabled()
    assert "전체 정리" in dialog.notes.text()
    dialog.acknowledge.setChecked(True)
    assert dialog.save_button.isEnabled() and dialog.cancel_button.isEnabled()
    dialog.cancel_button.click()
    assert posts(dialog)[-1][2]["kind"] == "cancel" and posts(dialog)[-1][2]["parameters"] == {}


@pytest.mark.parametrize("exposure", [summary(unknown=1), summary(durable=False)])
def test_unknown_or_unrecorded_state_never_looks_like_flat_safe_exit(dialog, exposure):
    reviewed(dialog, exposure=exposure)
    assert not dialog.save_button.isEnabled() and not dialog.flatten_button.isEnabled()
    assert dialog.cancel_button.isEnabled()


def test_poll_preserves_ack_and_lookup_draft_but_changed_financial_rows_reset_ack(dialog):
    reviewed(dialog, exposure=summary(1))
    dialog.acknowledge.setChecked(True)
    dialog.operation_id.setText("previous-request-id")
    dialog.refresh()
    emit(dialog, snapshot(2, exposure=summary(1)))
    assert dialog.acknowledge.isChecked()
    assert dialog.operation_id.text() == "previous-request-id"
    dialog.refresh()
    emit(
        dialog,
        snapshot(
            3,
            exposure=summary(1),
            participants=[{"owner": "paper_v1", "positions": [{"instance": "a", "quantity": "2"}]}],
        ),
    )
    assert not dialog.acknowledge.isChecked()


def test_lost_post_only_queries_frozen_id_and_does_not_enable_second_write(dialog):
    reviewed(dialog)
    dialog.flatten_button.click()
    request = deepcopy(dialog._pending["payload"])
    dialog.client.failed.emit(dialog.client.calls[-1][3], "lost ACK")
    dialog.show()
    dialog.poll()
    lookups = [row for row in dialog.client.calls if row[1].endswith("/" + request["id"])]
    assert lookups and all(row[0] == "GET" for row in lookups)
    assert len(posts(dialog)) == 2
    assert dialog._pending["payload"] == request
    assert not dialog.cancel_button.isEnabled() and not dialog.save_button.isEnabled()


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", "wrong"),
        ("expected_revision", 999),
        ("operation_core_epoch", "other"),
        ("parameters", {"extra": True}),
    ],
)
def test_receipt_must_match_id_epoch_revision_and_parameters(dialog, field, value):
    reviewed(dialog)
    dialog.save_button.click()
    emit(dialog, receipt(dialog, **{field: value}))
    assert dialog._pending is not None and dialog._pending["unknown"]
    assert not dialog.flatten_button.isEnabled()


def test_known_drain_allows_cancel_but_never_posts_order_cancel_or_resume(dialog):
    reviewed(dialog)
    dialog.flatten_button.click()
    identity = dialog._pending["payload"]["id"]
    emit(dialog, receipt(dialog, state="waiting_fill"))
    emit(dialog, snapshot(2, state="draining", operation_id=identity, exposure=summary(1, 1)))
    assert not dialog.save_button.isEnabled() and dialog.cancel_button.isEnabled()
    dialog.cancel_button.click()
    assert posts(dialog)[-1][2]["kind"] == "cancel"
    emit(dialog, receipt(dialog, result={"canceled": True, "pending_preserved": True}))
    assert all(row[1] == "/api/shutdown" for row in posts(dialog))
    assert all(row[2]["kind"] != "resume" for row in posts(dialog))


def test_exit_requested_and_disconnect_never_claim_completed_exit(dialog):
    reviewed(dialog)
    dialog.save_button.click()
    emit(dialog, receipt(dialog, state="exit_requested"))
    emit(dialog, snapshot(2, state="exit_requested"))
    assert "아직" in dialog.status.text() and "미확인" in dialog.outcome.text()
    dialog.client.connectionChanged.emit(False)
    assert "확인되지" in dialog.status.text()
    assert not dialog.save_button.isEnabled()


def test_old_epoch_reply_rejected_then_saved_old_receipt_can_confirm_previous_core_only(dialog):
    reviewed(dialog)
    dialog.save_button.click()
    old_tag = dialog.client.calls[-1][3]
    result = receipt(dialog, result={"core_exit_confirmed": True})
    dialog.client.stateReceived.emit(core_state("core-b"))
    dialog.client.resultReceived.emit(old_tag, result)
    assert dialog._pending is not None and not dialog.save_button.isEnabled()
    identity = dialog._pending["payload"]["id"]
    dialog.operation_id.setText(identity)
    dialog.query()
    emit(dialog, result | {"core_epoch": "core-b"})
    assert dialog._pending is None
    assert "이전 코어" in dialog.outcome.text()
    assert len(posts(dialog)) == 2


@pytest.mark.parametrize("mutation", ["bool_count", "old_epoch", "unsupported_state"])
def test_invalid_snapshot_preserves_last_valid_state_and_blocks_actions(dialog, mutation):
    reviewed(dialog)
    valid = deepcopy(dialog.snapshot)
    changed = snapshot(2)
    if mutation == "bool_count":
        changed["summary"]["positions_count"] = False
    elif mutation == "old_epoch":
        changed["core_epoch"] = "old"
    else:
        changed["state"] = "pretend_success"
    dialog.refresh()
    emit(dialog, changed)
    assert dialog.snapshot == valid and not dialog.save_button.isEnabled()


def test_dialog_x_and_close_button_do_not_cancel_or_stop_core(dialog):
    reviewed(dialog)
    before = len(posts(dialog))
    dialog.show()
    dialog.close()
    assert len(posts(dialog)) == before and not dialog.timer.isActive()


@pytest.mark.parametrize("width", [800, 1360])
def test_three_choices_and_ack_are_accessible_in_resizable_dialog(dialog, app, tmp_path, width):
    reviewed(dialog, exposure=summary(2, 1))
    dialog.refresh()
    emit(
        dialog,
        snapshot(
            2,
            exposure=summary(2, 1),
            participants=[
                {"owner": "paper_v1", "ready": True, "durable": True, "supports_flatten": True},
                {"owner": "margin_v2", "ready": True, "durable": True, "supports_flatten": True},
            ],
        ),
    )
    dialog.resize(width, 760)
    dialog.show()
    app.processEvents()
    assert dialog.width() == width
    assert all(
        button.isVisible()
        for button in (dialog.save_button, dialog.cancel_button, dialog.flatten_button)
    )
    assert dialog.acknowledge.isVisible()
    assert dialog.grab().save(str(tmp_path / f"shutdown-{width}.png"))


def test_power_menu_capability_and_gui_close_are_independent_of_core(tmp_path, app, monkeypatch):
    from PySide6.QtWidgets import QSystemTrayIcon

    from omega.gui.window import OmegaWindow

    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    window = OmegaWindow(tmp_path)
    calls = []
    monkeypatch.setattr(
        window.client, "request", lambda *args, **kwargs: calls.append((args, kwargs))
    )
    try:
        assert not window.shutdown_action.isEnabled()
        window.snapshot = core_state()
        window.client._set_attached(True)
        window._connection_changed(True)
        assert window.shutdown_action.isEnabled()
        window.shutdown_action.trigger()
        assert window._shutdown_dialog is not None
        assert calls[-1][0][:2] == ("GET", "/api/shutdown")
        window.close_gui_action.trigger()
        assert window.client._closed
        assert not any(args[0] == "POST" for args, _kwargs in calls)
    finally:
        window.close()
        window.deleteLater()
        app.processEvents()
