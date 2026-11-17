"""A settings reply must confirm its own submitted draft without erasing newer edits."""

import importlib.util
from copy import deepcopy

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


def settings(revision=0, *, volume=0.8, level="INFO"):
    return {
        "schema_version": 1,
        "revision": revision,
        "logging": {"level": level, "retention_days": 30, "max_file_bytes": 5000000},
        "notifications": {
            "sound": False,
            "speech": False,
            "speech_language": "en_US",
            "speech_voice": "",
            "speech_rate": 0,
            "speech_volume": volume,
            "repeat_critical_s": 30,
        },
        "training": {"recording": "standard", "optional_recording_failure": "continue"},
        "simulation": {"backend": "auto", "intrabar": "interpolation", "seed": 0},
    }


@pytest.fixture(scope="module")
def app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["notification-draft-tests"])


@pytest.fixture
def page(app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.settings_notifications import NotificationSettingsPage

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)

        def __init__(self):
            super().__init__()
            self.commands = []

        def command(self, kind, payload):
            identity = f"command-{len(self.commands)}"
            self.commands.append((identity, kind, deepcopy(payload)))
            return identity

    value = NotificationSettingsPage(Client())
    value.update_settings(settings())
    value.set_attached(True)
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def test_own_completion_preserves_post_submit_edits_and_advances_known_base_revision(page):
    page.volume.setValue(0.4)
    page.apply()
    identity = page._pending
    assert not page.apply_button.isEnabled()
    page.volume.setValue(0.2)
    page.client.resultReceived.emit(
        "command:" + identity, {"status": "completed", "result": settings(1, volume=0.4)}
    )
    assert page.volume.value() == 0.2 and page.dirty
    assert page.revision == 1 and page._pending is None
    page.apply()
    assert page.client.commands[-1][2]["revision"] == 1
    assert page.client.commands[-1][2]["changes"]["notifications"]["speech_volume"] == 0.2


def test_unchanged_submitted_draft_becomes_saved_and_unrelated_reply_does_not_clear_new_edits(page):
    page.volume.setValue(0.4)
    page.apply()
    identity = page._pending
    page.client.resultReceived.emit(
        "command:" + identity, {"status": "completed", "result": settings(1, volume=0.4)}
    )
    assert not page.dirty and page.preview_button.isEnabled()
    page.volume.setValue(0.2)
    page.client.resultReceived.emit(
        "command:another-form", {"status": "completed", "result": settings(2, volume=0.4)}
    )
    assert page.dirty and page.volume.value() == 0.2


def test_failure_is_not_reposted_and_global_operation_query_can_confirm_it(page):
    page.volume.setValue(0.4)
    page.apply()
    identity = page._pending
    page.client.failed.emit("command:" + identity, "transport unavailable")
    page.apply()
    assert len(page.client.commands) == 1 and page._pending == identity
    page.client.resultReceived.emit(
        "operation:" + identity, {"status": "completed", "result": settings(1, volume=0.4)}
    )
    assert page._pending is None and not page.dirty


def test_delayed_receipt_preserves_newest_snapshot_and_its_own_draft_base(page):
    page.volume.setValue(0.4)
    page.apply()
    identity = page._pending
    page.volume.setValue(0.2)
    page.update_settings(settings(2, volume=0.6))
    page.client.resultReceived.emit(
        "command:" + identity, {"status": "completed", "result": settings(1, volume=0.4)}
    )
    assert page.current["revision"] == 2
    # The newer external revision must not silently rebase an older edited form.
    assert page.revision == 1 and page.volume.value() == 0.2 and page.dirty


@pytest.fixture
def window(app, tmp_path, monkeypatch):
    from PySide6.QtWidgets import QSystemTrayIcon

    from omega.gui.window import OmegaWindow

    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    value = OmegaWindow(tmp_path)
    monkeypatch.setattr(value.client, "request", lambda *_a, **_k: None)
    monkeypatch.setattr(value.client, "poll", lambda: None)
    value.settings.update_settings(settings())
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def test_window_completion_does_not_erase_another_form_or_unfinished_json(window):
    page = window.settings
    draft = '{"logging":'
    page.settings_json.setPlainText(draft)
    page.notification_settings.volume.setValue(0.2)
    window._result("command:external-settings", {"status": "completed", "result": settings(1)})
    assert page.settings_json.toPlainText() == draft and page.dirty
    assert page.notification_settings.volume.value() == 0.2 and page.notification_settings.dirty


def test_window_reloads_only_reflected_runtime_json_and_preserves_post_submit_changes(window):
    page = window.settings
    page.settings_json.setPlainText('{"logging":{"level":"DEBUG"}}')
    window._result(
        "command:reflected", {"status": "completed", "result": settings(1, level="DEBUG")}
    )
    assert not page.dirty and page.revision == 1
    page.settings_json.setPlainText('{"logging":{"level":"ERROR"}}')
    window._result(
        "command:older-draft", {"status": "completed", "result": settings(2, level="DEBUG")}
    )
    assert page.dirty and '"ERROR"' in page.settings_json.toPlainText()


@pytest.mark.parametrize(
    "gate,detail",
    [
        ({"data_gate": False, "data_gate_reason": "missing_minute"}, "missing_minute"),
        ({"paper_recording": False, "paper_manual_resume_required": True}, "recording unavailable"),
        ({"paper_manual_resume_required": True}, "manual resume"),
        ({"paper_paused": True}, "engine paused"),
    ],
)
def test_header_never_treats_unpaused_control_as_effective_entry_permission(window, gate, detail):
    window.language = "en"
    window.snapshot = {
        "mode": "paper",
        "core": {
            "paused": False,
            "control_paused": False,
            "effective_entries_allowed": False,
            **gate,
        },
    }
    window._connection_changed(True)
    assert detail in window.connection_status.text()
    assert "ENTRIES ENABLED" not in window.connection_status.text()


def test_header_distinguishes_data_recovery_user_pause_and_per_strategy_readiness(window):
    window.language = "en"
    core = {
        "paused": False,
        "control_paused": False,
        "paper_paused": False,
        "data_gate": True,
        "paper_recording": True,
        "control_recording": True,
        "paper_manual_resume_required": False,
        "effective_entries_allowed": True,
    }
    window.snapshot = {"mode": "paper", "core": core}
    window._connection_changed(True)
    assert "Global entry gates passed" in window.connection_status.text()
    assert "strategy readiness/orders/budgets are separate" in window.connection_status.text()
    window.snapshot["core"] = core | {
        "paused": True,
        "control_paused": True,
        "effective_entries_allowed": False,
    }
    window._connection_changed(True)
    assert "NEW ENTRIES PAUSED" in window.connection_status.text()
    assert "gates passed" not in window.connection_status.text()
