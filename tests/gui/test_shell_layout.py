"""The global shell remains compact and its controls preserve explicit user drafts."""

import importlib.util
import os

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture
def window(tmp_path, monkeypatch):
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication, QSystemTrayIcon

    from omega.gui.window import OmegaWindow

    app = QApplication.instance() or QApplication(["shell-layout"])
    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    result = OmegaWindow(tmp_path)
    monkeypatch.setattr(result.client, "request", lambda *_a, **_k: None)
    result.show()
    app.processEvents()
    yield result, app
    result.close()
    result.deleteLater()
    app.processEvents()


@pytest.mark.parametrize("width,height", [(800, 560), (1360, 860), (1920, 1080)])
def test_top_actions_boxed_clocks_and_collapsed_dashboard_fit(window, width, height):
    from PySide6.QtCore import QPoint

    view, app = window
    view.resize(width, height)
    app.processEvents()
    assert view.width() == width and view.height() == height
    top = view.home_button.mapTo(view, QPoint()).y()
    assert abs(view.pause_button.mapTo(view, QPoint()).y() - top) <= 8
    assert all(field.isReadOnly() and field.text() for field in view.clock_fields.values())
    assert not view.dashboard.activity_panel.isVisible()
    assert not view.dashboard.health.isVisible()
    view.dashboard.activity_toggle.click()
    assert view.dashboard.activity_panel.isVisible()
    view.dashboard.details.click()
    assert view.dashboard.health.isVisible()
    view.dashboard.activity_toggle.click()
    assert not view.dashboard.health.isVisible()


def test_saved_ca_draft_becomes_explicit_stale_instead_of_silently_manual(window):
    view, _ = window
    page = view.trading
    instrument = page.instrument.text()

    def library(revision):
        return {
            "instances": [
                {"id": "ca1", "name": "A", "revision": revision, "instrument": instrument}
            ]
        }

    page.set_analysis_library(library(1))
    page.analysis_choice.setCurrentIndex(1)
    page.set_analysis_library(library(2))
    assert page.analysis_choice.currentData()["revision"] == 1
    assert "Stale" in page.analysis_choice.currentText()
    page.instrument.setText("binance:usdm:ETHUSDT")
    assert page.analysis_choice.currentData() is None


def test_tts_controls_save_revision_and_preserve_unsaved_draft(window, monkeypatch):
    view, _ = window
    from omega.runtime.settings import DEFAULT_SETTINGS

    page = view.settings.notification_settings
    calls = []
    monkeypatch.setattr(view.client, "command", lambda *args: calls.append(args))
    page.update_settings(DEFAULT_SETTINGS)
    page.set_attached(True)
    page.speech.setChecked(True)
    page.volume.setValue(0.3)
    page.update_settings(DEFAULT_SETTINGS | {"revision": 3})
    assert page.volume.value() == 0.3 and page.revision == 0
    assert not page.preview_button.isEnabled()
    page.apply()
    assert calls[-1][1]["revision"] == 0
    assert calls[-1][1]["changes"]["notifications"]["speech_volume"] == 0.3
    page.update_speech({"state": "unavailable", "reason": "no_speech_engine", "voices": []})
    assert "no_speech_engine" in page.status.text()
