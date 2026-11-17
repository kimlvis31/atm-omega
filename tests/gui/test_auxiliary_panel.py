"""Qt-only public auxiliary job controls; real collection is a separate integration scope."""

import importlib.util
import os
from copy import deepcopy
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt is not installed"
)

ETH = "binance:usdm:ETHUSDT"
BTC = "binance:usdm:BTCUSDT"


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtGui import QFont, QFontDatabase
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication(["auxiliary-panel-tests"])
    # Windows offscreen may expose no OS families; register a local read-only test font.
    font_path = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts/segoeui.ttf"
    if not QFontDatabase.families() and font_path.is_file():
        registered = QFontDatabase.addApplicationFont(str(font_path))
        if registered >= 0:
            app.setFont(QFont(QFontDatabase.applicationFontFamilies(registered)[0], 10))
    return app


@pytest.fixture
def panel(qt_app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.auxiliary_panel import AuxiliaryPanel

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)

        def __init__(self):
            super().__init__()
            self.attached, self.calls = True, []

        def request(self, method, path, payload=None, *, tag):
            self.calls.append((method, path, payload, tag))

    view = AuxiliaryPanel(Client())
    view.update_state({"core": {"epoch": "epoch-a", "capabilities": {"auxiliary_jobs": True}}})
    view.set_instrument(ETH)
    view.setChecked(True)
    view.client.calls.clear()
    view._requests.clear()
    yield view
    view.timer.stop()
    view.close()
    view.deleteLater()
    qt_app.processEvents()


def job(instrument=ETH, **changes):
    row = {
        "id": "a" * 32,
        "kind": "trade_flow",
        "parameters": {
            "instrument": instrument,
            "start_ms": 1700000040000,
            "end_ms": 1700000100000,
            "limit": 500,
            "max_pages": 20,
        },
        "destination_id": "c" * 64,
        "state": "running",
        "execution_id": "b" * 32,
        "generation": 0,
        "cancel_requested": False,
        "worker_started": True,
        "archive_id": None,
        "result": None,
        "error": None,
    }
    row.update(changes)
    return row


def reply_list(panel, rows, **extra):
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
            "worker_alive": True,
            **extra,
        },
    )


def test_submit_uses_selected_instrument_and_frozen_closed_range(panel):
    panel.submit()
    method, path, payload, tag = panel.client.calls[-1]
    assert method == "POST" and path == "/api/auxiliary-jobs"
    assert len(payload["id"]) == 32
    assert payload["parameters"]["instrument"] == ETH
    assert payload["parameters"]["end_ms"] - payload["parameters"]["start_ms"] == 300000
    assert payload["parameters"]["max_pages"] == 20
    panel.client.resultReceived.emit(tag, job(id=payload["id"], parameters=payload["parameters"]))
    assert panel._pending is None
    assert payload["id"] in panel.jobs


def test_unknown_submit_only_gets_original_id_without_repost(panel):
    panel.submit()
    posted = panel.client.calls[-1]
    panel.client.failed.emit(posted[3], "timeout")
    assert not panel.submit_button.isEnabled()
    panel.refresh()
    assert panel.client.calls[-1][0:2] == ("GET", "/api/auxiliary-jobs/" + posted[2]["id"])
    panel.client.resultReceived.emit(
        panel.client.calls[-1][3],
        job(id=posted[2]["id"], parameters=posted[2]["parameters"], state="unknown"),
    )
    assert sum(call[0] == "POST" for call in panel.client.calls) == 1
    assert panel._pending is None


def test_selection_change_retains_unknown_identity_and_filters_result(panel):
    panel.submit()
    posted = panel.client.calls[-1]
    panel.set_instrument(BTC)
    panel.client.failed.emit(posted[3], "timeout")
    panel.refresh()
    panel.client.resultReceived.emit(
        panel.client.calls[-1][3], job(id=posted[2]["id"], parameters=posted[2]["parameters"])
    )
    assert panel.table.rowCount() == 0
    assert panel.instrument == BTC


@pytest.mark.parametrize("kind", ["current_oi", "history_oi", "long_short_ratio"])
def test_spot_disables_usdm_only_jobs(panel, kind):
    panel.set_instrument("binance:spot:ETHUSDT")
    panel.kind.setCurrentIndex(panel.kind.findData(kind))
    assert not panel.submit_button.isEnabled()
    panel.submit()
    assert not any(call[0] == "POST" for call in panel.client.calls)


def test_current_oi_has_only_instrument_no_fake_minute(panel):
    panel.kind.setCurrentIndex(panel.kind.findData("current_oi"))
    assert panel.parameters() == {"instrument": ETH}
    assert not panel.start.isEnabled()
    assert not panel.pages.isEnabled()


def test_closed_trade_range_limit_and_poll_draft_preservation(panel):
    from PySide6.QtCore import QDateTime, QTimeZone

    panel.start.setDateTime(QDateTime.fromMSecsSinceEpoch(1700000040000, QTimeZone(b"UTC")))
    panel.end.setDateTime(QDateTime.fromMSecsSinceEpoch(1700000700000, QTimeZone(b"UTC")))
    panel.submit()
    assert not panel.client.calls
    assert "10" in panel.status.text()
    panel.limit.setValue(123)
    panel.pages.setValue(4)
    reply_list(panel, [])
    assert panel.limit.value() == 123 and panel.pages.value() == 4
    assert panel.start.dateTime().toMSecsSinceEpoch() == 1700000040000


def test_cancel_uses_exact_attempt_and_reports_completed_after_cancel(panel):
    reply_list(panel, [job()])
    panel.table.selectRow(0)
    panel.action("cancel")
    method, path, payload, tag = panel.client.calls[-1]
    assert method == "POST" and path.endswith("/cancel")
    assert payload == {"execution_id": "b" * 32}
    panel.client.resultReceived.emit(
        tag, job(state="completed", cancel_requested=True, result={"complete": True, "missing": 0})
    )
    assert "completed" in panel.summary.text()
    assert "True" in panel.summary.text()
    assert not panel.resume_button.isEnabled()
    assert panel.results.rowCount() == 2


def test_resume_keeps_original_plan_and_new_attempt_fence(panel):
    reply_list(panel, [job(state="partial")])
    panel.table.selectRow(0)
    panel.action("resume")
    _, path, payload, tag = panel.client.calls[-1]
    assert path.endswith("/resume") and payload == {"execution_id": "b" * 32}
    panel.client.resultReceived.emit(tag, job(state="queued", execution_id="d" * 32, generation=1))
    assert panel.jobs["a" * 32]["generation"] == 1
    assert panel._pending is None


def test_statistics_without_archive_unknown_is_not_resumable(panel):
    reply_list(panel, [job(kind="current_oi", parameters={"instrument": ETH}, state="unknown")])
    panel.table.selectRow(0)
    assert not panel.resume_button.isEnabled()


def test_late_list_does_not_replace_command_result(panel):
    panel.refresh()
    old_tag = panel.client.calls[-1][3]
    panel.submit()
    posted = panel.client.calls[-1]
    saved = job(id=posted[2]["id"], parameters=posted[2]["parameters"], state="completed")
    panel.client.resultReceived.emit(posted[3], saved)
    panel.client.resultReceived.emit(
        old_tag, {"schema_version": 1, "core_epoch": "epoch-a", "jobs": []}
    )
    assert panel.jobs[saved["id"]]["state"] == "completed"


def test_epoch_change_ignores_old_command_and_releases_old_pending(panel):
    panel.submit()
    posted = panel.client.calls[-1]
    panel.update_state({"core": {"epoch": "epoch-b", "capabilities": {"auxiliary_jobs": True}}})
    panel.client.resultReceived.emit(
        posted[3], job(id=posted[2]["id"], parameters=posted[2]["parameters"])
    )
    assert not panel.jobs and panel._pending is None


def test_stale_cancel_response_keeps_unknown_and_exact_target(panel):
    reply_list(panel, [job()])
    panel.table.selectRow(0)
    panel.action("cancel")
    panel.client.resultReceived.emit(
        panel.client.calls[-1][3], job(execution_id="d" * 32, generation=1)
    )
    assert panel._pending["unknown"] is True
    assert panel.jobs["a" * 32]["execution_id"] == "b" * 32


@pytest.mark.parametrize(
    "changes",
    [
        {"generation": True},
        {"state": "success"},
        {"execution_id": "x"},
        {"result": {"value": float("nan")}},
    ],
)
def test_malformed_snapshot_does_not_enable_actions(panel, changes):
    reply_list(panel, [job(**changes)])
    assert not panel.jobs
    assert not panel.cancel_button.isEnabled()


def test_disconnect_and_storage_failure_disable_mutations(panel):
    reply_list(panel, [job()], storage_failed=True)
    assert not panel.submit_button.isEnabled()
    panel.set_attached(False)
    assert not panel.refresh_button.isEnabled()
    panel.set_attached(True)
    panel.update_state({"core": {"epoch": "epoch-b", "capabilities": {"auxiliary_jobs": False}}})
    assert not panel.submit_button.isEnabled()


@pytest.mark.parametrize("width", [800, 1360])
def test_database_selection_bridge_and_compact_layout(panel, qt_app, tmp_path, width):
    from omega.gui.database_page import DatabasePage

    view = DatabasePage(panel.client)
    view.update_state({"core": {"epoch": "epoch-a", "capabilities": {"auxiliary_jobs": True}}})
    view.accept_snapshot(
        {
            "instruments": [{"instrument": ETH, "symbol": "ETHUSDT", "market": "usdm"}],
            "capabilities": {},
        }
    )
    view.table.selectRow(0)
    assert view.auxiliary.instrument == ETH
    assert not view.auxiliary.isChecked()
    view.auxiliary.setChecked(True)
    view.set_presentation("light", "en")
    view.resize(width, 900)
    view.show()
    qt_app.processEvents()
    view.compact_tabs.setCurrentWidget(view.detail_panel)
    qt_app.processEvents()
    view.detail_panel.verticalScrollBar().setValue(view.auxiliary.y())
    qt_app.processEvents()
    assert view.detail_panel.widget().layout().indexOf(view.auxiliary) >= 0
    assert view.auxiliary.submit_button.width() > 70
    assert view.grab().save(str(tmp_path / f"auxiliary-database-{width}.png"))
    view.table.clearSelection()
    assert view.auxiliary.instrument is None
    view.timer.stop()
    view.auxiliary.timer.stop()
    view.close()
    view.deleteLater()


def test_lower_attempt_does_not_replace_selected_result(panel):
    reply_list(panel, [job(generation=2)])
    panel.table.selectRow(0)
    reply_list(panel, [job(generation=1)])
    assert panel.jobs["a" * 32]["generation"] == 2
    assert "2" in panel.summary.text()


def test_snapshot_results_are_not_synthetic_prices(panel):
    original = job(
        state="partial", result={"complete": False, "source_minutes": 1, "missing_minutes": 1}
    )
    reply_list(panel, [original])
    panel.table.selectRow(0)
    assert panel.jobs[original["id"]] == deepcopy(original)
    assert "실시간 자동 구독이 아닙니다" in panel.hint.text()
    assert panel.results.rowCount() == 3


def test_invalid_later_row_does_not_partially_apply_snapshot(panel):
    reply_list(panel, [job()])
    valid = job(id="c" * 32)
    invalid = job(id="d" * 32, generation=True)
    reply_list(panel, [valid, invalid])
    assert set(panel.jobs) == {"a" * 32}


def test_existing_job_plan_cannot_change_under_same_identity(panel):
    reply_list(panel, [job()])
    reply_list(panel, [job(instrument=BTC)])
    assert panel.jobs["a" * 32]["parameters"]["instrument"] == ETH


@pytest.mark.parametrize("kind", ["history_oi", "long_short_ratio"])
@pytest.mark.parametrize("new_metadata", [True, False])
def test_completed_native_statistics_never_claim_full_requested_coverage(panel, kind, new_metadata):
    result = {"status": "committed", "rows": [{"revision": 1, "checksum": "f" * 64}]}
    if new_metadata:
        result.update(
            received_rows=1,
            coverage_scope="received_native_batch",
            full_requested_range_verified=False,
        )
    reply_list(panel, [job(kind=kind, state="completed", result=result)])
    panel.table.selectRow(0)
    panel.kind.setCurrentIndex(panel.kind.findData(kind))
    panel.set_presentation("light", "en")
    assert "completed" in panel.summary.text()
    assert "full requested range unverified" in panel.summary.text()
    assert "1 rows" in panel.summary.text() if new_metadata else "? rows" in panel.summary.text()
    assert "up to 500" in panel.kind_hint.text()
    assert "coverage is unverified" in panel.kind_hint.text()


def test_coverage_draft_changes_preserve_unknown_submission_plan(panel):
    panel.submit()
    posted = panel.client.calls[-1]
    panel.client.failed.emit(posted[3], "timeout")
    frozen = deepcopy(panel._pending)
    panel.kind.setCurrentIndex(panel.kind.findData("history_oi"))
    panel.limit.setValue(25)
    panel.set_instrument(BTC)
    assert panel._pending == frozen
    panel.refresh()
    assert panel.client.calls[-1][0:2] == ("GET", "/api/auxiliary-jobs/" + frozen["id"])
    assert len([call for call in panel.client.calls if call[0] == "POST"]) == 1


def test_point_observation_does_not_gain_historical_coverage_from_draft_kind(panel):
    reply_list(
        panel,
        [
            job(
                kind="current_oi",
                state="completed",
                parameters={"instrument": ETH},
                result={
                    "received_rows": 1,
                    "coverage_scope": "point_observation",
                    "full_requested_range_verified": False,
                },
            )
        ],
    )
    panel.table.selectRow(0)
    panel.kind.setCurrentIndex(panel.kind.findData("history_oi"))
    panel.set_presentation("light", "en")
    assert "single point observation" in panel.summary.text()
    assert "full requested range unverified" not in panel.summary.text()
