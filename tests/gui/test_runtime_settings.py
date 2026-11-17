"""Typed settings revision/draft/unknown handling without a core or database."""

import importlib.util
import os
from copy import deepcopy
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


def snapshot(revision=2):
    return {
        "schema_version": 1,
        "revision": revision,
        "logging": {"level": "INFO", "retention_days": 30, "max_file_bytes": 5000000},
        "training": {"recording": "standard", "optional_recording_failure": "continue"},
        "simulation": {"backend": "auto", "intrabar": "interpolation", "seed": 0},
        "execution": {"mode": "static", "max_age_ms": 60000, "apply_to_manual": False},
        "analysis": {"input_storage_mb": 1024},
        "notifications": {"sound": True},
        "future_category": {"never_drop": "preserve"},
    }


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtGui import QFont, QFontDatabase
    from PySide6.QtWidgets import QApplication

    value = QApplication.instance() or QApplication(["runtime-settings-tests"])
    font = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts/segoeui.ttf"
    if not QFontDatabase.families() and font.is_file():
        registered = QFontDatabase.addApplicationFont(str(font))
        value.setFont(QFont(QFontDatabase.applicationFontFamilies(registered)[0], 10))
    return value


@pytest.fixture
def page(app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.runtime_settings import RuntimeSettingsPage

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        stateReceived = Signal(dict)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls, self.commands = [], []

        def command(self, kind, payload):
            operation = f"{len(self.commands) + 1:032x}"
            self.commands.append((kind, deepcopy(payload), operation))
            return operation

        def request(self, method, path, *, tag):
            self.calls.append((method, path, tag))

    view = RuntimeSettingsPage(Client())
    view.set_presentation("light", "en")
    view.update_state({"core": {"epoch": "a"}, "settings": snapshot()})
    view.set_attached(True)
    yield view
    view.close()
    view.deleteLater()
    app.processEvents()


def complete(page, changes=None, revision=3):
    result = snapshot(revision)
    for category, fields in (changes or page._submitted_changes).items():
        result[category].update(fields)
    page.client.resultReceived.emit(
        "command:" + page._pending, {"status": "completed", "result": result}
    )
    return result


def test_changed_fields_only_preserve_unknown_categories(page):
    page.editors["logging", "retention_days"].setValue(42)
    page.apply()
    kind, payload, _ = page.client.commands[-1]
    assert kind == "settings.patch"
    assert payload == {"revision": 2, "changes": {"logging": {"retention_days": 42}}}
    complete(page)
    assert not page.dirty and page.revision == 3
    assert page.current["future_category"] == {"never_drop": "preserve"}


def test_exact_64bit_seed_and_quota_mode_patch(page):
    page.editors["simulation", "seed"].setText("9223372036854775807")
    page.editors["execution", "mode"].setCurrentText("strict")
    page.editors["execution", "apply_to_manual"].setChecked(True)
    page.editors["analysis", "input_storage_mb"].setValue(16)
    page.apply()
    changes = page.client.commands[-1][1]["changes"]
    assert changes["simulation"]["seed"] == 2**63 - 1
    assert type(changes["simulation"]["seed"]) is int
    assert changes["execution"] == {"mode": "strict", "apply_to_manual": True}
    assert changes["analysis"] == {"input_storage_mb": 16}


@pytest.mark.parametrize("seed", ["-1", "9223372036854775808", "1.0", "1e3", "bad", ""])
def test_invalid_seed_blocks_post_without_erasing_draft(page, seed):
    page.editors["simulation", "seed"].setText(seed)
    page.apply()
    assert not page.client.commands
    assert page.editors["simulation", "seed"].text() == seed
    assert "Seed must" in page.status.text()


def test_old_server_missing_groups_disabled_never_injected(page):
    old = snapshot(3)
    del old["execution"], old["analysis"]
    page.update_settings(old)
    assert not page.editors["execution", "mode"].isEnabled()
    assert not page.editors["analysis", "input_storage_mb"].isEnabled()
    page.editors["logging", "level"].setCurrentText("DEBUG")
    assert page.changes() == {"logging": {"level": "DEBUG"}}


def test_dirty_poll_retains_revision_and_draft_then_rejection(page):
    page.editors["logging", "retention_days"].setValue(99)
    newest = snapshot(3)
    newest["logging"]["retention_days"] = 60
    page.update_settings(newest)
    assert page.revision == 2 and page.current["revision"] == 3
    assert page.editors["logging", "retention_days"].value() == 99
    page.apply()
    assert page.client.commands[-1][1]["revision"] == 2
    page.client.resultReceived.emit(
        "command:" + page._pending, {"status": "rejected", "code": "revision_conflict"}
    )
    assert page._pending is None and page.dirty
    assert page.editors["logging", "retention_days"].value() == 99
    page.reload()
    assert page.revision == 3 and page.editors["logging", "retention_days"].value() == 60


def test_later_edit_survives_save_completion_and_uses_new_base(page):
    page.editors["logging", "retention_days"].setValue(42)
    page.apply()
    page.editors["logging", "retention_days"].setValue(43)
    complete(page)
    assert page.revision == 3 and page.dirty
    assert page.editors["logging", "retention_days"].value() == 43
    assert page.changes() == {"logging": {"retention_days": 43}}


def test_unknown_only_checks_original_operation_and_disconnect_preserves(page):
    page.editors["training", "recording"].setCurrentText("light")
    page.apply()
    operation = page._pending
    page.client.failed.emit("command:" + operation, "timeout")
    page.apply()
    assert len(page.client.commands) == 1
    page.check_operation()
    assert page.client.calls[-1] == (
        "GET",
        "/api/operations/" + operation,
        "operation:" + operation,
    )
    page.set_attached(False)
    assert not page.check_button.isEnabled()
    page.set_attached(True)
    result = snapshot(3)
    result["training"]["recording"] = "light"
    page.client.resultReceived.emit(
        "operation:" + operation, {"status": "completed", "result": result}
    )
    assert page._pending is None and not page.dirty


def test_core_restart_requires_explicit_reload_and_ignores_old_completion(page):
    page.editors["logging", "retention_days"].setValue(42)
    page.apply()
    operation = page._pending
    page.update_state({"core": {"epoch": "b"}, "settings": snapshot(0)})
    assert not page.apply_button.isEnabled() and not page.check_button.isEnabled()
    assert page.revision is None and page.editors["logging", "retention_days"].value() == 42
    page.client.resultReceived.emit(
        "command:" + operation, {"status": "completed", "result": snapshot(3)}
    )
    assert page._pending == operation
    page.reload()
    assert page.revision == 0 and page._pending is None
    assert "remains unconfirmed" in page.status.text()


@pytest.mark.parametrize(
    "data",
    [
        {"schema_version": 1, "revision": 3},
        snapshot(2),
        {**snapshot(3), "logging": {"retention_days": 1}},
    ],
)
def test_malformed_or_unreflected_completed_stays_uncertain(page, data):
    page.editors["logging", "retention_days"].setValue(42)
    page.apply()
    operation = page._pending
    page.client.resultReceived.emit("command:" + operation, {"status": "completed", "result": data})
    assert page._pending == operation and page._uncertain
    assert page.editors["logging", "retention_days"].value() == 42


def test_late_completion_does_not_regress_newer_snapshot(page):
    page.editors["logging", "retention_days"].setValue(42)
    page.apply()
    page.update_settings(snapshot(4))
    complete(page, revision=3)
    assert page.current["revision"] == 4
    assert page.dirty and page.revision == 2
    assert "newer settings" in page.status.text()


@pytest.mark.parametrize("width", [800, 1360])
def test_typed_settings_scroll_and_settings_page_bridge(page, app, tmp_path, width):
    from PySide6.QtWidgets import QTabWidget

    from omega.gui.pages import SettingsPage

    owner = SettingsPage(page.client, {"language": "en"})
    owner.set_attached(True)
    page.client.stateReceived.emit({"core": {"epoch": "fresh"}, "settings": snapshot()})
    owner.update_settings(snapshot())
    tabs = owner.findChild(QTabWidget)
    tabs.setCurrentWidget(owner.runtime_settings)
    owner.resize(width, 900)
    owner.show()
    app.processEvents()
    assert owner.runtime_settings.epoch == "fresh"
    assert owner.runtime_settings.editors["simulation", "seed"].isEnabled()
    assert owner.grab().save(str(tmp_path / f"runtime-settings-{width}.png"))
    owner.runtime_settings.scroll.verticalScrollBar().setValue(
        owner.runtime_settings.scroll.verticalScrollBar().maximum()
    )
    app.processEvents()
    assert owner.grab().save(str(tmp_path / f"runtime-settings-bottom-{width}.png"))
    assert "static" in owner.runtime_settings.hints["execution"].text().lower()
    assert "Referenced inputs" in owner.runtime_settings.hints["analysis"].text()
    owner.close()
    owner.deleteLater()
