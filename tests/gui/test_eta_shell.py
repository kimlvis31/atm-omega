"""Eta navigation must lead to working Omega workspaces without changing the core."""

import importlib.util
import json
import os

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    yield QApplication.instance() or QApplication(["eta-shell-tests"])


@pytest.fixture
def view(app, tmp_path, monkeypatch):
    from PySide6.QtWidgets import QSystemTrayIcon

    from omega.gui.window import OmegaWindow

    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    window = OmegaWindow(tmp_path)
    calls = []
    monkeypatch.setattr(window.client, "request", lambda *a, **k: calls.append((a, k)))
    window._calls = calls
    window.show()
    app.processEvents()
    yield window
    window.close()
    window.deleteLater()
    app.processEvents()


def test_eta_dashboard_navigates_to_database_and_home_without_core_start(view, app):
    from omega.gui.window import PAGE_IDS

    assert view.theme == "light"
    assert len(view.dashboard.tiles) == 9
    assert not view.navigation.isVisible()
    view.dashboard.tiles[7][1].click()
    app.processEvents()
    assert view.stack.currentWidget() is view.database
    assert view.navigation.currentRow() == PAGE_IDS.index("database")
    view.home_button.click()
    assert view.stack.currentWidget() is view.dashboard
    assert not view._calls


def test_trade_history_tile_opens_observed_fill_view_instead_of_position_table(view):
    view.dashboard.tiles[3][1].click()
    assert view.stack.currentWidget() is view.history
    assert view.stack.currentWidget() is not view.trading


def test_optional_page_menu_hides_after_navigation(view):
    from omega.gui.window import PAGE_IDS

    view.menu_button.click()
    assert view.navigation.isVisible()
    view.navigation.setCurrentRow(PAGE_IDS.index("data"))
    assert view.stack.currentWidget() is view.data
    assert not view.navigation.isVisible()


def test_existing_explicit_dark_preference_is_preserved(app, tmp_path, monkeypatch):
    from PySide6.QtWidgets import QSystemTrayIcon

    from omega.gui.window import OmegaWindow

    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    (tmp_path / "state").mkdir()
    (tmp_path / "state/gui.preferences.json").write_text(
        json.dumps({"theme": "dark", "language": "en"}), encoding="utf-8"
    )
    view = OmegaWindow(tmp_path)
    try:
        assert view.theme == "dark" and view.language == "en"
        assert view.settings.theme.currentText() == "dark"
    finally:
        view.close()
        view.deleteLater()


def test_module_form_uses_manifest_schema_and_preserves_per_module_drafts(view, app):
    from omega.strategies.registry import StrategyRegistry

    page = view.trading
    page._set_modules({"ts": StrategyRegistry().list_modules()})
    page.configuration.setPlainText('{"model":"BSCALPHA","parameters":{"HTF_Bias":0.3}}')
    app.processEvents()
    assert ("parameters", "HTF_Bias") in page.configuration_editor.controls
    page.module.setCurrentIndex(page.module.findData("target_quantity"))
    assert ("quantity",) in page.configuration_editor.controls
    page.configuration.setPlainText('{"quantity":"0.75","custom":"preserve"}')
    page.module.setCurrentIndex(page.module.findData("tef"))
    app.processEvents()
    assert json.loads(page.configuration.toPlainText())["parameters"]["HTF_Bias"] == 0.3
    page.module.setCurrentIndex(page.module.findData("target_quantity"))
    assert json.loads(page.configuration.toPlainText())["custom"] == "preserve"


def test_gui_close_button_only_detaches_this_client(view):
    view.close_gui_action.trigger()
    assert view.client._closed
    assert not view._calls
