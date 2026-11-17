"""First delivery must not describe saved/unknown incidents as newly detected."""

import importlib.util
from copy import deepcopy
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture(scope="module")
def app():
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["startup-notification-wording"])


@pytest.fixture
def controller(app, monkeypatch):
    from PySide6.QtWidgets import QApplication, QSystemTrayIcon

    from omega.gui.notifications import NotificationController

    delivered = {"beeps": [], "messages": [], "speech": []}
    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    monkeypatch.setattr(QApplication, "beep", lambda: delivered["beeps"].append(True))
    result = NotificationController()
    result.tray = SimpleNamespace(
        showMessage=lambda *args: delivered["messages"].append(args),
        setIcon=lambda _icon: None,
        hide=lambda: None,
    )
    result.speech = SimpleNamespace(
        state=lambda: SimpleNamespace(name="Ready"),
        say=lambda message: delivered["speech"].append(message),
        stop=lambda: None,
    )
    result.sound = result.voice = True
    try:
        yield result, delivered
    finally:
        result.close()
        result.deleteLater()
        app.processEvents()


def incident(identity, scope=None, **changes):
    row = {
        "id": identity,
        "revision": 1,
        "severity": "critical",
        "acknowledged": False,
        "resolved": False,
        "timestamp": 1_729_000_000,
        "message": "PRIVATE_MESSAGE_42",
        "code": "PRIVATE_CODE_42",
        "context": {"path": "PRIVATE_PATH_42", "balance": "PRIVATE_BALANCE_42"},
    }
    if scope is not None:
        row["observation_scope"] = scope
    return row | changes


@pytest.mark.parametrize("language", ["ko", "en"])
@pytest.mark.parametrize(
    "scopes",
    [("previous_core",), (None,), ("current_core",), ("previous_core", "current_core", None)],
    ids=["history", "legacy-unknown", "current", "mixed"],
)
def test_first_delivery_is_neutral_private_and_critical_for_each_provenance(
    controller, language, scopes
):
    from PySide6.QtWidgets import QSystemTrayIcon

    notifications, delivered = controller
    notifications.language = language
    rows = [incident(str(index), scope) for index, scope in enumerate(scopes)]
    original = deepcopy(rows)
    notifications.update(rows)

    assert rows == original
    assert len(delivered["messages"]) == len(delivered["beeps"]) == 1
    title, text, icon, duration = delivered["messages"][0]
    assert title == "Omega · CRITICAL" and icon == QSystemTrayIcon.MessageIcon.Critical
    assert duration == 10_000
    expected = (
        "확인하지 않은 중요 알림이 있습니다. Omega 알림을 확인하세요."
        if language == "ko"
        else "An unacknowledged incident needs attention. Check Omega notifications."
    )
    assert text == expected and delivered["speech"] == [expected]
    assert "감지" not in text and "detected" not in text.lower()
    assert "PRIVATE_" not in title + text
    assert notifications.tracker.critical() == rows


@pytest.mark.parametrize("language", ["ko", "en"])
def test_neutral_history_delivery_retains_repeat_ack_revision_and_mute_contract(
    controller, language
):
    notifications, delivered = controller
    notifications.language = language
    acknowledged = incident("acknowledged", "previous_core", acknowledged=True)
    pending = incident("pending", "previous_core")
    original = deepcopy([acknowledged, pending])
    notifications.update([acknowledged])
    notifications.repeat_critical()
    assert delivered == {"beeps": [], "messages": [], "speech": []}

    notifications.update([acknowledged, pending])
    notifications.update([acknowledged, pending])
    notifications.repeat_critical()
    assert len(delivered["messages"]) == 1
    assert len(delivered["beeps"]) == len(delivered["speech"]) == 2
    assert delivered["speech"][-1] == (
        "Omega 중요 알림이 아직 확인되지 않았습니다."
        if language == "ko"
        else "Omega has an unacknowledged critical incident."
    )
    notifications.tracker.acknowledge(pending)
    notifications.repeat_critical()
    notifications.update([acknowledged, pending])
    assert len(delivered["beeps"]) == len(delivered["speech"]) == 2
    assert not notifications.tracker.critical()
    assert [acknowledged, pending] == original

    revised = pending | {"revision": 2, "observation_scope": "current_core"}
    notifications.sound = notifications.voice = False
    notifications.update([acknowledged, revised])
    notifications.repeat_critical()
    assert len(delivered["messages"]) == 2
    assert len(delivered["beeps"]) == len(delivered["speech"]) == 2
    assert notifications.tracker.critical() == [revised]
