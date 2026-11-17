"""Safe selected-incident guidance and revision-specific local ACK confirmation."""

import importlib.util
import os
from copy import deepcopy
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


def record(identity="one", scope="previous_core", **changes):
    return {
        "id": identity,
        "revision": 1,
        "severity": "critical",
        "acknowledged": False,
        "code": "CONTROL_TRANSPORT_OR_STORAGE_FAILURE",
        "timestamp": 100,
        "last_seen_at": 200,
        "occurrences": 3,
        "observation_scope": scope,
        "context": {
            "stage": "operation",
            "operation": "POST",
            "error_kind": "timeout",
            "errno": 13,
        },
    } | changes


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["notification-guidance-tests"])


@pytest.fixture
def window(app, tmp_path, monkeypatch):
    from PySide6.QtCore import QObject, Signal
    from PySide6.QtWidgets import QApplication, QSystemTrayIcon

    from omega.gui.window import OmegaWindow

    class Client(QObject):
        stateReceived = Signal(dict)
        candlesReceived = Signal(str, dict)
        modulesReceived = Signal(dict)
        resultReceived = Signal(str, object)
        failed = Signal(str, str)
        connectionChanged = Signal(bool)
        attached = True

        def __init__(self):
            super().__init__()
            self.commands, self.requests = [], []

        def command(self, kind, payload):
            identity = "operation-" + str(len(self.commands))
            self.commands.append((identity, kind, deepcopy(payload)))
            return identity

        def request(self, *args, **kwargs):
            self.requests.append((args, kwargs))

        def poll(self):
            pass

        def stop(self):
            pass

    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    monkeypatch.setattr(QApplication, "beep", lambda: pytest.fail("GUI test must not emit audio"))
    view = OmegaWindow(tmp_path, client=Client())
    view.notifications.configure(sound=False, voice=False, language="en")
    view.language = "en"
    view._last_state_at = datetime(2026, 11, 4, tzinfo=UTC)
    view.snapshot = {
        "mode": "paper",
        "core": {"epoch": "a", "storage_available": True, "paused": True},
        "incidents": [record()],
    }
    view._update_incidents()
    view._connection_changed(True)
    view.incident_table.selectRow(0)
    view.notifications.tracker.update(view.snapshot["incidents"])
    yield view
    view.close()
    view.deleteLater()
    app.processEvents()


@pytest.mark.parametrize("language", ["ko", "en"])
@pytest.mark.parametrize("scope", ["current_core", "previous_core", "unknown"])
def test_guidance_is_actionable_without_inventing_a_cause_or_recovery(language, scope):
    from omega.gui.incident_presentation import incident_guidance

    incident = record(scope=scope)
    original = deepcopy(incident)
    incident["context"].update(
        raw="SECRET_RAW", path="SECRET_PATH", token="SECRET_TOKEN", balance="SECRET_BALANCE"
    )
    incident["message"] = "SECRET_MESSAGE"
    text = incident_guidance(
        incident, {"storage_available": True}, attached=True, last_received=None, language=language
    )
    assert "SECRET" not in text and "CONTROL_TRANSPORT_OR_STORAGE_FAILURE" not in text
    assert ("다음 조치" if language == "ko" else "Next step") in text
    assert ("정확한 실패 대상" if language == "ko" else "exact failed target") in text
    assert ("시장 DB" if language == "ko" else "market DB") in text
    assert ("해결" if language == "ko" else "recovery") in text
    expected = {
        "current_core": ("현재 코어", "current core"),
        "previous_core": ("이전 코어", "previous core"),
        "unknown": ("발생 코어가 기록되지", "recording core is unknown"),
    }[scope]
    assert expected[0 if language == "ko" else 1] in text
    assert incident["timestamp"] == original["timestamp"] and incident["acknowledged"] is False


def test_critical_count_retains_record_identity_and_filters_ack_resolved_and_closed():
    from omega.gui.incident_presentation import incident_critical_summary

    rows = [
        record(),
        record("current", "current_core"),
        record("unknown", "unrecognized"),
        record("ack", acknowledged=True),
        record("resolved", resolved=True),
        record("closed", status="closed"),
        record("warning", severity="warning"),
        record("malformed", severity=[]),
    ]
    original = deepcopy(rows)
    text = incident_critical_summary(rows, "en")
    assert "critical: 3" in text and "current observations 1" in text
    assert "previous records 1" in text and "provenance unknown 1" in text
    assert rows == original


def test_plain_guidance_keeps_selection_by_id_through_reorder_language_and_removal(window):
    from PySide6.QtCore import Qt

    first, second = record(), record("second", "current_core")
    window.snapshot["incidents"] = [first, second]
    window._update_incidents()
    window.incident_table.selectRow(1)
    assert "current core" in window.incident_guidance.text()
    window.snapshot["incidents"] = [second, first]
    window.language = "ko"
    window._update_incidents()
    assert window.incident_table.item(window.incident_table.currentRow(), 0).text() == "second"
    assert "현재 코어" in window.incident_guidance.text()
    assert window.incident_guidance.textFormat() == Qt.TextFormat.PlainText
    window._acknowledge()
    assert window.client.commands[-1][1:] == ("incident.acknowledge", {"incident_id": "second"})
    window.snapshot["incidents"] = [first]
    window._update_incidents()
    assert window.incident_table.currentRow() == -1 and not window.ack_button.isEnabled()
    assert window.incident_guidance_scroll.isHidden()


def test_disconnected_detail_uses_last_snapshot_without_claiming_storage_available(window):
    original = deepcopy(window.snapshot)
    window._connection_changed(False)
    text = window.incident_guidance.text()
    assert "current status unavailable" in text and "2026-11-04" in text
    assert "Current control recording available" not in text
    assert window.snapshot == original and not window.ack_button.isEnabled()


@pytest.mark.parametrize(
    "status,durable,confirmed",
    [
        ("completed", True, True),
        ("completed", False, False),
        ("completed", None, False),
        ("volatile", False, False),
        ("rejected", False, False),
        ("unknown", False, False),
    ],
)
def test_ack_feedback_requires_completed_durable_result_without_mutating_facts(
    window, status, durable, confirmed
):
    original = deepcopy(window.snapshot)
    window._acknowledge()
    identity = window.client.commands[-1][0]
    assert window.client.commands[-1][1:] == ("incident.acknowledge", {"incident_id": "one"})
    assert (
        window.notifications.tracker.critical()
        and "Waiting for sound" in window.incident_guidance.text()
    )
    data = {"acknowledged": True}
    if durable is not None:
        data["durable"] = durable
    window._result("command:" + identity, {"id": identity, "status": status, "result": data})
    assert bool(window.notifications.tracker.critical()) is not confirmed
    assert window.snapshot == original and not any(
        call[1] == "resume" for call in window.client.commands
    )
    text = window.incident_guidance.text()
    if confirmed:
        assert "acknowledgement was saved" in text
    elif status == "volatile":
        assert "temporary" in text and "not saved" in text
    elif status == "rejected":
        assert "was rejected" in text
    else:
        assert "unconfirmed" in text and "acknowledgement was saved" not in text


@pytest.mark.parametrize(
    "status,durable", [("completed", True), ("volatile", False), ("unknown", None)]
)
def test_revision_arriving_while_ack_pending_is_not_locally_silenced_or_claimed_saved(
    window, status, durable
):
    window._acknowledge()
    identity = window.client.commands[-1][0]
    revised = record(revision=2, scope="current_core")
    window.snapshot["incidents"] = [revised]
    window._update_incidents()
    window.notifications.tracker.update([revised])
    result = {"acknowledged": True, "durable": durable}
    window._result("command:" + identity, {"id": identity, "status": status, "result": result})
    assert window.notifications.tracker.critical() == [revised]
    text = window.incident_guidance.text()
    assert "earlier revision" in text and "acknowledgement was saved" not in text
    assert revised["acknowledged"] is False


def test_older_result_does_not_overwrite_new_revision_pending_feedback(window):
    window._acknowledge()
    old_id = window.client.commands[-1][0]
    revised = record(revision=2)
    window.snapshot["incidents"] = [revised]
    window._update_incidents()
    window.notifications.tracker.update([revised])
    window._acknowledge()
    new_id = window.client.commands[-1][0]
    window._result(
        "command:" + old_id,
        {"id": old_id, "status": "completed", "result": {"acknowledged": True, "durable": True}},
    )
    assert "Waiting for sound" in window.incident_guidance.text()
    assert not window.ack_button.isEnabled() and window.notifications.tracker.critical() == [
        revised
    ]
    window._result(
        "command:" + new_id,
        {"id": new_id, "status": "completed", "result": {"acknowledged": True, "durable": True}},
    )
    assert not window.notifications.tracker.critical()


def test_uncertain_ack_keeps_existing_operation_lookup_and_never_leaks_error_detail(window):
    window._acknowledge()
    identity = window.client.commands[-1][0]
    window._failed("command:" + identity, "SECRET_TRANSPORT_MESSAGE")
    assert window.notifications.tracker.critical() and not window.ack_button.isEnabled()
    assert (
        "SECRET" not in window.incident_guidance.text()
        and "unconfirmed" in window.incident_guidance.text()
    )
    window._verify_operation()
    assert window.client.requests[-1] == (
        ("GET", "/api/operations/" + identity),
        {"tag": "operation:" + identity},
    )
    assert len(window.client.commands) == 1


def test_old_core_ack_cannot_confirm_same_signature_in_new_core(window):
    window._acknowledge()
    identity = window.client.commands[-1][0]
    window.snapshot["core"]["epoch"] = "b"
    window._result(
        "command:" + identity,
        {"id": identity, "status": "completed", "result": {"acknowledged": True, "durable": True}},
    )
    assert (
        window.notifications.tracker.critical()
        and "earlier revision" in window.incident_guidance.text()
    )


@pytest.mark.parametrize("prefix", ["command:", "operation:"])
@pytest.mark.parametrize("receipt_id", [None, "different-operation"])
def test_missing_or_mismatched_ack_receipt_id_cannot_confirm_tagged_operation(
    window, prefix, receipt_id
):
    original = deepcopy(window.snapshot)
    window._acknowledge()
    identity = window.client.commands[-1][0]
    result = {"status": "completed", "result": {"acknowledged": True, "durable": True}}
    if receipt_id is not None:
        result["id"] = receipt_id
    window._result(prefix + identity, result)
    assert window.notifications.tracker.critical() and not window.notifications.tracker.acknowledged
    assert "unconfirmed" in window.incident_guidance.text()
    assert "acknowledgement was saved" not in window.incident_guidance.text()
    assert (
        "command:" + identity in window._commands
        and "command:" + identity in window._incident_ack_targets
    )
    assert window._uncertain_operation == identity and not window.verify_button.isHidden()
    assert window.snapshot == original and len(window.client.commands) == 1
    window._verify_operation()
    assert window.client.requests[-1] == (
        ("GET", "/api/operations/" + identity),
        {"tag": "operation:" + identity},
    )
    window._result(
        "operation:" + identity,
        {"id": identity, "status": "completed", "result": {"acknowledged": True, "durable": True}},
    )
    assert not window.notifications.tracker.critical()
    assert window._uncertain_operation is None and window.verify_button.isHidden()


def test_guidance_uses_existing_narrow_dock_scroll_and_keeps_font_speech_independent(window, app):
    from PySide6.QtCore import Qt

    original_font = deepcopy(window.font_preferences)
    notification_draft = deepcopy(window.settings.notification_settings.current)
    window.resize(800, 600)
    window.show()
    window.bell.click()
    for _ in range(5):
        app.processEvents()
    assert window.incident_dock.isVisible() and window.incident_guidance_scroll.isVisible()
    assert (
        window.incident_guidance_scroll.horizontalScrollBarPolicy()
        == Qt.ScrollBarPolicy.ScrollBarAlwaysOff
    )
    assert window.incident_guidance_scroll.verticalScrollBar().maximum() > 0
    assert window.incident_guidance_scroll.height() <= 128
    window._acknowledge()
    assert window.font_preferences == original_font
    assert window.settings.notification_settings.current == notification_draft
    assert all(call[1] == "incident.acknowledge" for call in window.client.commands)
