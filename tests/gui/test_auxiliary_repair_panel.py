"""Explicit bounded one-minute repair UI; no collection/network/database execution."""

import importlib.util
import os
from copy import deepcopy
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")
OPEN = 1700000040000
INSTRUMENT = "binance:usdm:币安人生USDT"


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtGui import QFont, QFontDatabase
    from PySide6.QtWidgets import QApplication

    current = QApplication.instance() or QApplication(["auxiliary-repair-tests"])
    font = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts/segoeui.ttf"
    if not QFontDatabase.families() and font.is_file():
        registered = QFontDatabase.addApplicationFont(str(font))
        current.setFont(QFont(QFontDatabase.applicationFontFamilies(registered)[0], 10))
    return current


@pytest.fixture
def panel(app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.auxiliary_panel import AuxiliaryPanel

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls = []

        def request(self, method, path, payload=None, *, tag):
            self.calls.append((method, path, deepcopy(payload), tag))

    value = AuxiliaryPanel(Client())
    value.set_presentation("light", "en")
    value.update_state({"core": {"epoch": "a", "capabilities": {"auxiliary_jobs": True}}})
    value.set_instrument(INSTRUMENT)
    value.setChecked(True)
    listing(value, [parent()])
    value.table.selectRow(0)
    yield value
    value.timer.stop()
    value.close()
    value.deleteLater()
    app.processEvents()


def parent(**changes):
    result = {
        "repairable_minutes": [
            {"open_time_ms": OPEN, "quality": "partial", "revision": 1, "checksum": "c" * 64},
            {
                "open_time_ms": OPEN + 60000,
                "quality": "missing",
                "revision": 2,
                "checksum": "d" * 64,
            },
        ]
    }
    row = {
        "id": "a" * 32,
        "execution_id": "b" * 32,
        "generation": 0,
        "kind": "trade_flow",
        "parameters": {
            "instrument": INSTRUMENT,
            "start_ms": OPEN,
            "end_ms": OPEN + 120000,
            "limit": 500,
            "max_pages": 20,
        },
        "destination_id": "f" * 64,
        "state": "partial",
        "worker_started": True,
        "cancel_requested": False,
        "archive_id": None,
        "result": result,
        "error": None,
    }
    row.update(changes)
    return row


def listing(panel, rows):
    if not panel._requests:
        panel.refresh()
    tag = panel.client.calls[-1][3]
    panel.client.resultReceived.emit(
        tag,
        {
            "schema_version": 1,
            "core_epoch": panel.epoch,
            "jobs": rows,
            "closed": False,
            "storage_failed": False,
        },
    )


def repair_row(post, **changes):
    value = parent(
        id=post[2]["id"],
        kind="trade_flow_repair",
        parameters=post[2]["parameters"],
        execution_id="e" * 32,
        state="queued",
        result=None,
    )
    value.update(changes)
    return value


def test_repair_is_explicit_new_id_one_minute_parent_attempt_fenced(panel):
    assert panel.kind.count() == 4
    assert panel.kind.findData("trade_flow_repair") == -1
    assert panel.repair_minute.count() == 2 and panel.repair_button.isEnabled()
    panel.repair_minute.setCurrentIndex(1)
    panel.repair_limit.setValue(750)
    panel.repair_pages.setValue(6)
    panel.repair_button.click()
    post = panel.client.calls[-1]
    assert post[:2] == ("POST", "/api/auxiliary-jobs")
    assert post[2]["id"] != "a" * 32 and len(post[2]["id"]) == 32
    assert post[2]["kind"] == "trade_flow_repair"
    assert post[2]["parameters"] == {
        "instrument": INSTRUMENT,
        "parent_job_id": "a" * 32,
        "parent_execution_id": "b" * 32,
        "open_time_ms": OPEN + 60000,
        "limit": 750,
        "max_pages": 6,
    }
    panel.client.resultReceived.emit(post[3], repair_row(post))
    assert panel._pending is None and panel.table.rowCount() == 2


def test_poll_preserves_selected_repair_minute_and_limits(panel):
    panel.repair_minute.setCurrentIndex(1)
    panel.repair_limit.setValue(321)
    panel.repair_pages.setValue(4)
    listing(panel, [parent()])
    assert panel.repair_minute.currentData() == OPEN + 60000
    assert panel.repair_limit.value() == 321 and panel.repair_pages.value() == 4


@pytest.mark.parametrize("state", ["running", "completed", "cancelled", "unknown", "error"])
def test_only_partial_trade_parent_can_offer_repair(panel, state):
    listing(panel, [parent(state=state)])
    panel.table.selectRow(0)
    assert not panel.repair_button.isEnabled()
    before = len(panel.client.calls)
    panel.repair()
    assert len(panel.client.calls) == before


@pytest.mark.parametrize("result", [None, {}, {"repairable_minutes": []}])
def test_legacy_no_repair_metadata_is_explicitly_unavailable(panel, result):
    listing(panel, [parent(result=result)])
    panel.table.selectRow(0)
    assert not panel.repair_button.isEnabled()
    assert "missing or invalid" in panel.repair_hint.text()


@pytest.mark.parametrize(
    "change",
    [
        {"open_time_ms": OPEN - 60000},
        {"open_time_ms": OPEN + 1},
        {"open_time_ms": True},
        {"revision": True},
        {"revision": 0},
        {"checksum": "bad"},
        {"quality": "observed"},
    ],
)
def test_malformed_repair_minute_never_becomes_actionable(panel, change):
    changed = parent()
    changed["result"]["repairable_minutes"][0].update(change)
    listing(panel, [changed])
    panel.table.selectRow(0)
    assert not panel.repair_button.isEnabled() and panel.repair_minute.count() == 0


def test_unknown_repair_posts_once_then_gets_unchanged_job_id(panel):
    panel.repair()
    post = panel.client.calls[-1]
    panel.client.failed.emit(post[3], "timeout")
    panel.repair()
    panel.refresh()
    assert panel.client.calls[-1][:2] == ("GET", "/api/auxiliary-jobs/" + post[2]["id"])
    assert sum(call[0] == "POST" for call in panel.client.calls) == 1
    panel.client.resultReceived.emit(panel.client.calls[-1][3], repair_row(post, state="unknown"))
    assert panel._pending is None


def test_parent_selection_change_does_not_move_repair_to_another_instrument(panel):
    panel.repair()
    post = panel.client.calls[-1]
    panel.set_instrument("binance:spot:BTCUSDT")
    panel.client.resultReceived.emit(post[3], repair_row(post))
    assert panel.table.rowCount() == 0
    assert panel.jobs[post[2]["id"]]["parameters"]["instrument"] == INSTRUMENT
    assert not panel.repair_button.isEnabled()


def test_new_parent_attempt_refreshes_fence_before_next_repair(panel):
    listing(panel, [parent(execution_id="d" * 32, generation=1)])
    panel.table.selectRow(0)
    panel.repair()
    assert panel.client.calls[-1][2]["parameters"]["parent_execution_id"] == "d" * 32


def test_core_epoch_change_drops_old_repair_response(panel):
    panel.repair()
    post = panel.client.calls[-1]
    panel.update_state({"core": {"epoch": "b", "capabilities": {"auxiliary_jobs": True}}})
    panel.client.resultReceived.emit(post[3], repair_row(post))
    assert not panel.jobs and panel._pending is None


def test_repair_job_resume_is_exact_frozen_attempt_not_new_fetch(panel):
    panel.repair()
    post = panel.client.calls[-1]
    panel.client.resultReceived.emit(post[3], repair_row(post, state="partial"))
    panel.table.selectRow(1)
    assert panel.resume_button.isEnabled()
    assert "replays frozen pending" in panel.summary.text()
    assert not panel.repair_button.isEnabled()
    panel.resume_button.click()
    method, path, payload, _ = panel.client.calls[-1]
    assert method == "POST" and path.endswith("/resume")
    assert payload == {"execution_id": "e" * 32}


def test_disconnect_blocks_new_repair_and_retains_parent(panel):
    panel.set_attached(False)
    assert not panel.repair_button.isEnabled()
    assert panel._selected()["id"] == "a" * 32


def test_out_of_calendar_range_metadata_is_not_rendered_as_utc_date(panel):
    changed = parent()
    changed["parameters"].update(start_ms=0, end_ms=10**11 * 60000)
    changed["result"]["repairable_minutes"][0]["open_time_ms"] = 10**10 * 60000
    # A different ID avoids conflating metadata validation with immutable-plan checks.
    changed["id"] = "c" * 32
    listing(panel, [changed])
    panel.table.selectRow(1)
    assert not panel.repair_button.isEnabled() and panel.repair_minute.count() == 0


@pytest.mark.parametrize("width", [800, 1360])
def test_repair_layout_is_scrollable_and_keeps_parent_scope(panel, app, tmp_path, width):
    from PySide6.QtWidgets import QScrollArea

    scroll = QScrollArea()
    scroll.setWidgetResizable(True)
    scroll.setWidget(panel)
    scroll.resize(width, 900)
    scroll.show()
    app.processEvents()
    scroll.ensureWidgetVisible(panel.repair_box)
    app.processEvents()
    assert panel.repair_button.width() > 100
    assert scroll.grab().save(str(tmp_path / f"aux-repair-{width}.png"))
    scroll.takeWidget()
    scroll.close()
