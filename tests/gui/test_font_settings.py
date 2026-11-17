"""Fonts are GUI-local, explicit, durable and failure-preserving; speech is independent."""

import importlib.util
import json
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt is not installed"
)


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["gui-font-tests"])


@pytest.fixture
def window(app, tmp_path, monkeypatch):
    from PySide6.QtWidgets import QSystemTrayIcon

    from omega.gui.window import OmegaWindow

    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    view = OmegaWindow(tmp_path)
    calls = []
    monkeypatch.setattr(view.client, "command", lambda *a, **kw: calls.append((a, kw)))
    view._font_test_commands = calls
    view.show()
    app.processEvents()
    yield view
    view.close()
    view.deleteLater()
    app.processEvents()


@pytest.fixture
def font_file():
    paths = (
        Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts" / "malgun.ttf",
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf"),
    )
    result = next((path for path in paths if path.is_file()), None)
    if result is None:
        pytest.skip("no system TTF file is available for actual app-local load verification")
    return result


def installed_settings(panel, family, size=14):
    from PySide6.QtGui import QFont

    panel.mode.setCurrentIndex(panel.mode.findData("installed"))
    panel.family.setCurrentFont(QFont(family))
    panel.font_size.setValue(size)


def test_installed_font_apply_is_local_only_saves_and_reopens_without_changing_core(window, app):
    from PySide6.QtGui import QFontDatabase

    from omega.gui.window import OmegaWindow

    panel = window.settings.font_settings
    family = QFontDatabase.families()[0]
    installed_settings(panel, family)
    assert not (window.root / "state/gui.preferences.json").exists()
    panel.apply_button.click()
    assert window.font_preferences == {
        "mode": "installed",
        "family": family,
        "path": "",
        "size_px": 14,
    }
    assert app.font().family() == family and app.font().pixelSize() == 14
    saved = json.loads((window.root / "state/gui.preferences.json").read_text(encoding="utf-8"))
    assert saved["font"] == window.font_preferences
    assert not window._font_test_commands
    root = window.root
    window.close()
    reopened = OmegaWindow(root)
    try:
        assert reopened.font_preferences == saved["font"]
        assert reopened.settings.font_settings.font_size.value() == 14
    finally:
        reopened.close()
        reopened.deleteLater()


def test_trusted_ttf_is_registered_only_in_qt_and_referenced_without_copying(
    window, app, font_file
):
    panel = window.settings.font_settings
    panel.mode.setCurrentIndex(panel.mode.findData("file"))
    panel.file.setText(str(font_file.resolve()))
    panel.apply_button.click()
    assert window._font_registration is not None
    assert window.font_preferences["mode"] == "file"
    assert window.font_preferences["path"] == str(font_file.resolve())
    assert window.font_preferences["family"] == app.font().family()
    assert not list(window.root.rglob("*.ttf")) and not list(window.root.rglob("*.otf"))
    assert not window._font_test_commands


def test_app_only_family_cannot_be_saved_as_installed_and_lose_its_registration(
    window, app, font_file
):
    """Ownership guard does not depend on which families an OS bundles."""
    from PySide6.QtGui import QFont, QFontDatabase

    panel = window.settings.font_settings
    panel.mode.setCurrentIndex(panel.mode.findData("file"))
    panel.file.setText(str(font_file.resolve()))
    panel.apply_button.click()
    original, registration = dict(window.font_preferences), window._font_registration
    current = QFont(app.font())
    family = original["family"]
    window._installed_font_families = window._installed_font_families - {family}
    panel.mode.setCurrentIndex(panel.mode.findData("installed"))
    panel.family.setCurrentFont(QFont(family))
    panel.apply_button.click()
    assert window.font_preferences == original and app.font() == current
    assert window._font_registration == registration
    assert family in QFontDatabase.applicationFontFamilies(registration)
    assert "not installed when the GUI started" in panel.status.text()
    saved = json.loads((window.root / "state/gui.preferences.json").read_text(encoding="utf-8"))
    assert saved["font"] == original and saved["font"]["path"]


def test_actual_app_only_cascadia_ttf_stays_file_backed_on_rejected_source_switch(window, app):
    """Optional local evidence uses a personal reference; no font is shipped."""
    from PySide6.QtGui import QFontDatabase

    from omega.gui.window import OmegaWindow

    explicit = os.environ.get("OMEGA_TEST_APP_ONLY_TTF")
    path = (
        Path(explicit)
        if explicit
        else Path(__file__).resolve().parents[3]
        / ("AutoTradeMachine_Eta/AutoTradeMachine_Eta/GUI/fonts/CascadiaCode.ttf")
    )
    if not path.is_file():
        pytest.skip("optional local app-only TTF reference is not available")
    panel = window.settings.font_settings
    panel.mode.setCurrentIndex(panel.mode.findData("file"))
    panel.file.setText(str(path.resolve()))
    panel.apply_button.click()
    assert window._font_registration is not None
    family = window.font_preferences["family"]
    if family in window._installed_font_families:
        pytest.skip("the reference TTF family is already installed on this OS")
    previous = dict(window.font_preferences)
    installed_settings(panel, family, 14)
    panel.apply_button.click()
    assert window.font_preferences == previous
    assert app.font().family() == family
    assert family in QFontDatabase.applicationFontFamilies(window._font_registration)
    assert "not installed when the GUI started" in panel.status.text()
    root = window.root
    window.close()
    reopened = OmegaWindow(root)
    try:
        assert reopened.font_preferences == previous and app.font().family() == family
        assert reopened._font_registration is not None
    finally:
        reopened.close()
        reopened.deleteLater()


def test_bad_font_file_and_missing_file_leave_active_font_preferences_and_draft_intact(
    window, app, tmp_path
):
    from PySide6.QtGui import QFont

    panel = window.settings.font_settings
    panel.reset_button.click()
    original_font, original_settings = QFont(app.font()), dict(window.font_preferences)
    original_bytes = (window.root / "state/gui.preferences.json").read_bytes()
    invalid = tmp_path / "invalid.ttf"
    invalid.write_bytes(b"not a font")
    for path in (invalid, tmp_path / "missing.ttf"):
        panel.mode.setCurrentIndex(panel.mode.findData("file"))
        panel.file.setText(str(path))
        panel.font_size.setValue(17)
        panel.apply_button.click()
        assert app.font() == original_font and window.font_preferences == original_settings
        assert (window.root / "state/gui.preferences.json").read_bytes() == original_bytes
        assert panel.file.text() == str(path) and panel.status.text()


def test_failed_local_preference_save_keeps_previous_font_and_unsaved_draft(
    window, app, monkeypatch
):
    from PySide6.QtGui import QFont, QFontDatabase

    import omega.gui.window as module

    panel = window.settings.font_settings
    panel.reset_button.click()
    original_font, original_settings = QFont(app.font()), dict(window.font_preferences)
    original_bytes = (window.root / "state/gui.preferences.json").read_bytes()
    installed_settings(panel, QFontDatabase.families()[0], 16)
    monkeypatch.setattr(
        module, "save_gui_preferences", lambda *_args: (_ for _ in ()).throw(OSError("read-only"))
    )
    panel.apply_button.click()
    assert app.font() == original_font and window.font_preferences == original_settings
    assert panel.font_size.value() == 16 and "read-only" in panel.status.text()
    assert (window.root / "state/gui.preferences.json").read_bytes() == original_bytes


def test_runtime_poll_and_language_change_do_not_overwrite_font_draft(window):
    panel = window.settings.font_settings
    panel.mode.setCurrentIndex(panel.mode.findData("file"))
    panel.file.setText("C:/user-draft/custom.ttf")
    panel.font_size.setValue(15)
    before = panel.draft()
    window.settings.update_settings({"revision": 2, "notifications": {"speech": True}})
    window.set_presentation("dark", "en")
    assert panel.draft() == before
    assert window.settings.current["notifications"]["speech"] is True
    assert not window._font_test_commands


def test_restore_defaults_is_explicit_and_saved(window, app):
    from PySide6.QtGui import QFontDatabase

    panel = window.settings.font_settings
    installed_settings(panel, QFontDatabase.families()[0], 18)
    panel.apply_button.click()
    panel.reset_button.click()
    assert window.font_preferences == {"mode": "default", "family": "", "path": "", "size_px": 12}
    assert app.font().family() == window._default_font.family()
    assert window._font_registration is None


@pytest.mark.parametrize(
    "value",
    [
        {"mode": []},
        {"size_px": True},
        {"size_px": 25},
        {"family": "x\x00y"},
        {"mode": "installed", "family": ""},
        {"mode": "file", "path": ""},
    ],
)
def test_malformed_font_preferences_are_rejected(value):
    from omega.gui.font_settings import validate_font_preferences

    with pytest.raises(ValueError):
        validate_font_preferences(value)


def test_missing_startup_font_preserves_reference_and_reports_fallback(app, tmp_path, monkeypatch):
    from PySide6.QtWidgets import QSystemTrayIcon

    from omega.gui.window import OmegaWindow

    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    reference = {
        "mode": "file",
        "family": "missing font",
        "path": str(tmp_path / "gone.ttf"),
        "size_px": 18,
    }
    (tmp_path / "state").mkdir()
    (tmp_path / "state/gui.preferences.json").write_text(
        json.dumps({"font": reference}), encoding="utf-8"
    )
    view = OmegaWindow(tmp_path)
    try:
        assert view.font_preferences == reference and view.settings.font_settings.status.text()
        assert view._active_font_size == 12 and view._font_registration is None
    finally:
        view.close()
        view.deleteLater()
    saved = json.loads((tmp_path / "state/gui.preferences.json").read_text(encoding="utf-8"))
    assert saved["font"] == reference


def test_font_settings_and_header_fit_real_800_and_1360_windows(window, app):
    from PySide6.QtGui import QFontDatabase
    from PySide6.QtWidgets import QScrollArea

    from omega.gui.window import PAGE_IDS

    panel = window.settings.font_settings
    installed_settings(panel, QFontDatabase.families()[0], 18)
    panel.apply_button.click()
    window.navigation.setCurrentRow(PAGE_IDS.index("settings"))
    for width, height in ((1360, 860), (800, 560)):
        window.resize(width, height)
        app.processEvents()
        assert window.width() == width and window.height() == height
        area = next(
            area
            for area in window.settings.findChildren(QScrollArea)
            if area.widget() is not None and area.widget().isAncestorOf(panel)
        )
        area.ensureWidgetVisible(panel.apply_button)
        app.processEvents()
        assert panel.preview.geometry().bottom() < panel.notice.geometry().top()
        assert panel.apply_button.geometry().right() < panel.reset_button.geometry().left()
        assert not window.grab().isNull()
