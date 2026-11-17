"""Real Qt notification controller, with OS/audio/speech outputs intercepted."""

import importlib.util
from copy import deepcopy
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture(scope="module")
def app():
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["incident-history-delivery"])


@pytest.fixture
def controller(app, monkeypatch):
    from PySide6.QtWidgets import QApplication, QSystemTrayIcon

    from omega.gui.notifications import NotificationController

    delivered = {"beeps": [], "messages": [], "speech": [], "icons": []}
    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    monkeypatch.setattr(QApplication, "beep", lambda: delivered["beeps"].append(True))
    result = NotificationController()
    result.tray = SimpleNamespace(
        showMessage=lambda *args: delivered["messages"].append(args),
        setIcon=lambda value: delivered["icons"].append(value),
        hide=lambda: None,
    )
    result.speech = SimpleNamespace(
        state=lambda: SimpleNamespace(name="Ready"),
        say=lambda message: delivered["speech"].append(message),
        stop=lambda: None,
    )
    result.sound, result.voice = True, True
    try:
        yield result, delivered
    finally:
        result.close()
        result.deleteLater()
        app.processEvents()


def incident(*, identity="saved-critical", acknowledged=False, revision=1, severity="critical"):
    return dict(
        id=identity,
        revision=revision,
        severity=severity,
        acknowledged=acknowledged,
        resolved=False,
        code="CONTROL_TRANSPORT_OR_STORAGE_FAILURE",
        message="private payload must not be spoken",
        first_seen_at="2026-10-24T19:54:22Z",
    )


def test_new_gui_keeps_acknowledged_history_without_new_tray_sound_or_voice(controller):
    notifications, delivered = controller
    rows = [
        incident(acknowledged=True),
        incident(identity="saved-warning", acknowledged=True, severity="warning"),
    ]
    original = deepcopy(rows)
    notifications.update(rows)
    notifications.repeat_critical()
    notifications.update(rows)
    assert rows == original  # Delivery does not acknowledge/resolve/edit persisted history.
    assert len(notifications.tracker.active) == 2
    assert not notifications.tracker.critical()
    assert delivered == {"beeps": [], "messages": [], "speech": [], "icons": []}


def test_unacknowledged_history_repeats_until_ack_and_new_revision_still_alerts(controller):
    notifications, delivered = controller
    old = incident()
    notifications.update([old])
    assert len(delivered["messages"]) == len(delivered["beeps"]) == len(delivered["speech"]) == 1
    assert old["message"] not in delivered["speech"][0]
    notifications.update([old])
    assert len(delivered["messages"]) == len(delivered["beeps"]) == 1
    notifications.repeat_critical()
    assert len(delivered["beeps"]) == len(delivered["speech"]) == 2
    notifications.tracker.acknowledge(old)
    notifications.repeat_critical()
    notifications.update([old | {"acknowledged": True}])
    assert len(delivered["beeps"]) == 2
    assert len(notifications.tracker.active) == 1 and not old["acknowledged"]
    next_occurrence = old | {"revision": 2, "acknowledged": False}
    notifications.update([next_occurrence])
    assert len(delivered["messages"]) == 2
    assert len(delivered["beeps"]) == len(delivered["speech"]) == 3
    assert notifications.tracker.critical() == [next_occurrence]
