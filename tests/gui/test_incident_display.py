"""The notification table keeps stable ACK targets and stale-state wording."""

import importlib.util
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture(scope="module")
def app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["incident-display-tests"])


@pytest.fixture
def window(app, tmp_path, monkeypatch):
    from PySide6.QtWidgets import QSystemTrayIcon

    from omega.gui.window import OmegaWindow

    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    value = OmegaWindow(tmp_path)
    monkeypatch.setattr(value.client, "request", lambda *_a, **_k: None)
    monkeypatch.setattr(value.client, "poll", lambda: None)
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def test_history_details_language_and_selected_ack_survive_refresh(window, monkeypatch):
    rows = [
        {
            "id": "old",
            "code": "CONTROL_TRANSPORT_OR_STORAGE_FAILURE",
            "severity": "critical",
            "timestamp": 100,
            "observation_scope": "previous_core",
            "acknowledged": False,
        },
        {
            "id": "new",
            "code": "PAPER_RECORDING_UNAVAILABLE",
            "severity": "critical",
            "timestamp": 200,
            "last_seen_at": 210,
            "observation_scope": "current_core",
            "acknowledged": False,
        },
    ]
    window.snapshot = {"incidents": rows, "core": {"storage_available": True}}
    window.language = "en"
    window._update_incidents()
    table = window.incident_table
    assert table.isColumnHidden(0) and "Previous core" in table.item(0, 3).text()
    assert "CONTROL_TRANSPORT_OR_STORAGE_FAILURE" in table.item(0, 5).toolTip()
    table.selectRow(1)
    window.language = "ko"
    window._update_incidents()
    assert table.item(table.currentRow(), 0).text() == "new"
    assert "현재 코어" in table.item(1, 3).text()
    calls = []
    monkeypatch.setattr(window, "_command", lambda *args: calls.append(args))
    window._acknowledge()
    assert calls == [("incident.acknowledge", {"incident_id": "new"})]
    window.language = "en"
    window._last_state_at = datetime(2026, 11, 2, tzinfo=UTC)
    window._connection_changed(False)
    assert "current status unavailable" in window.incident_connection.text()
    assert table.rowCount() == 2  # A disconnect does not erase the stored record.


def test_compact_first_open_shows_three_rows_then_preserves_user_resize(window, app):
    from PySide6.QtCore import Qt

    window.snapshot = {
        "incidents": [
            {
                "id": str(index),
                "code": "CONTROL_TRANSPORT_OR_STORAGE_FAILURE",
                "severity": "critical",
                "timestamp": 100,
                "observation_scope": scope,
                "acknowledged": False,
            }
            for index, scope in enumerate(("previous_core", "current_core", "unknown"))
        ]
    }
    window.notifications.configure(sound=False, voice=False, language="ko")
    window.resize(800, 600)
    window._update_incidents()
    window.show()
    app.processEvents()
    assert window.incident_dock.isHidden()
    window.bell.click()
    for _ in range(5):
        app.processEvents()
    table = window.incident_table
    assert window.incident_dock.isVisible() and table.rowCount() == 3
    assert table.viewport().height() >= sum(table.rowHeight(row) for row in range(3))
    assert table.isColumnHidden(0)
    if window.incident_dock.isFloating():
        assert window.incident_dock.windowModality() == Qt.WindowModality.NonModal
        window.incident_dock.resize(window.incident_dock.width(), 220)
    else:
        window.resizeDocks([window.incident_dock], [220], Qt.Orientation.Vertical)
    app.processEvents()
    user_height = window.incident_dock.height()
    window.bell.click()
    assert window.incident_dock.isHidden()
    window.bell.click()
    app.processEvents()
    assert abs(window.incident_dock.height() - user_height) <= 2


def test_incident_columns_follow_actual_width_without_losing_ack_identity(window, app, monkeypatch):
    window.snapshot = {
        "incidents": [
            {
                "id": "selected",
                "code": "CONTROL_TRANSPORT_OR_STORAGE_FAILURE",
                "severity": "critical",
                "timestamp": 100,
                "last_seen_at": 200,
                "observation_scope": "previous_core",
                "acknowledged": False,
            }
        ]
    }
    window._update_incidents()
    window.show()
    window.bell.click()
    for _ in range(5):
        app.processEvents()
    dock, table = window.incident_dock, window.incident_table
    dock.setFloating(True)  # Users may float or redock the same notification panel.
    dock.resize(760, 320)
    for _ in range(3):
        app.processEvents()
    assert table.width() < 950 and table.isColumnHidden(1)
    assert table.isColumnHidden(0) and not table.isColumnHidden(2)
    assert "00:01:40 UTC" in table.item(0, 5).toolTip()
    assert "00:03:20 UTC" in table.item(0, 5).toolTip()
    table.selectRow(0)
    dock.resize(1100, 320)
    for _ in range(3):
        app.processEvents()
    assert table.width() >= 950 and not table.isColumnHidden(1)
    assert table.isColumnHidden(0) and table.item(table.currentRow(), 0).text() == "selected"
    calls = []
    monkeypatch.setattr(window, "_command", lambda *args: calls.append(args))
    window._acknowledge()
    assert calls == [("incident.acknowledge", {"incident_id": "selected"})]
