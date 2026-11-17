"""Simulation results remain read-only, bounded, currency-aware and responsive."""

import importlib.util
import json
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional PySide6 is not installed"
)


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["omega-simulation-results-tests"])


@pytest.fixture
def page(qt_app, tmp_path):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.simulation_results import SimulationResultsPage

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True
        root = tmp_path

        def __init__(self):
            super().__init__()
            self.calls = []

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    view = SimulationResultsPage(Client())
    yield view
    view.pool.waitForDone(5000)
    qt_app.processEvents()
    view.close()
    view.deleteLater()
    qt_app.processEvents()


def result_record():
    return {
        "schema_version": 1,
        "state": "complete",
        "quote_asset": "USDT",
        "quote_assets": {"A": "USDT"},
        "spec": {"initial_wallet": "100", "latency_ms": 50},
        "wallet": "99",
        "positions": {"A": "1"},
        "pending_orders": 2,
        "equity": [
            {"timestamp_ms": 60000, "wallet": "99", "equity": "95"},
            {"timestamp_ms": 300000, "wallet": "99", "equity": "104"},
        ],
        "fills": [
            {
                "timestamp_ms": "60001",
                "instrument": "A",
                "quantity": "1",
                "price": "10",
                "fee": "1",
                "realized_pnl": "0",
                "tag": "ENTRY",
            }
        ],
        "events": [],
        "assumptions": ["synthetic extrema path"],
        "backend": {"selected": "cpu"},
    }


def load_direct(page, record=None):
    from omega.research.result_summary import summarize_simulation

    page.accept_result(
        {
            "path": "fixture/simulation.json",
            "sha256": "a" * 64,
            "summary": summarize_simulation(record or result_record()),
        }
    )


def jobs():
    return [
        {"id": "a" * 32, "kind": "simulate", "state": "complete", "created_at_ms": 1000},
        {"id": "b" * 32, "kind": "train", "state": "complete"},
        {"id": "c" * 32, "kind": "simulate", "state": "failed"},
        {"id": "d" * 32, "kind": "simulate", "state": "complete", "created_at_ms": 2000},
    ]


def test_only_completed_simulations_are_listed_and_selection_fetches_details(page):
    page.update_jobs(jobs())
    assert page.runs.rowCount() == 2
    page.runs.selectRow(0)
    args, kwargs = page.client.calls[-1]
    assert args == ("GET", "/api/jobs/" + "d" * 32)
    assert kwargs["tag"] == "simulation-results:job:" + "d" * 32
    page.refresh()
    assert any(args == ("GET", "/api/jobs") for args, _kwargs in page.client.calls)
    page.search_runs.setText("a")
    assert page.runs.rowCount() == 1
    assert page.runs.item(0, 0).text() == "a" * 32


def test_result_summary_positions_pending_costs_and_curve_are_real_records(page):
    load_direct(page)
    assert page.fills.rowCount() == 1
    assert page.fills.item(0, 4).text() == "1 USDT"
    assert page.positions.item(0, 1).text() == "1"
    assert page.costs.item(0, 1).text() == "1"
    assert page.summary["pending_orders"] == 2
    assert page.plot.times == [60000, 300000]  # Irregular UTC positions, not sample indices.
    page.curve_field.setCurrentIndex(2)
    assert page.plot.points[0] == (60000, 5.0)
    assert page.events.rowCount() == 0 and page.event_count.text() == "0 / 0"
    assert page.settings.topLevelItemCount() >= 5


def test_missing_records_are_unavailable_not_zero_and_no_old_result_retained(page):
    load_direct(page)
    load_direct(page, {"schema_version": 1, "state": "complete"})
    assert page.fill_count.text() == "기록 없음"
    assert page.event_count.text() == "기록 없음"
    assert not page.plot.points
    assert page.positions.item(0, 1).text() == "기록 없음"
    assert page.summary["metrics"]["final_wallet"] is None


def test_stale_api_reply_cannot_open_result_for_previous_selection(page, monkeypatch):
    page.update_jobs(jobs())
    page.runs.selectRow(0)
    old_tag = page._expected_job
    page.runs.selectRow(1)
    opened = []
    monkeypatch.setattr(page, "open_path", lambda *args, **kwargs: opened.append(args))
    page.client.resultReceived.emit(
        old_tag, {"id": "d" * 32, "kind": "simulate", "state": "complete", "result_file": "bad"}
    )
    assert not opened


def test_auto_open_rejects_paths_outside_the_selected_job(page, monkeypatch):
    page.update_jobs(jobs())
    page.runs.selectRow(0)
    opened = []
    monkeypatch.setattr(page, "open_path", lambda *args, **kwargs: opened.append(args))
    page.client.resultReceived.emit(
        page._expected_job,
        {
            "id": "d" * 32,
            "kind": "simulate",
            "state": "complete",
            "result_file": str(Path(page.client.root) / "secrets/simulation.json"),
        },
    )
    assert not opened
    assert "own simulation.json" in page.status.text()


def test_actual_background_reader_loads_selected_job_without_mutating_source(page, qt_app):
    from PySide6.QtTest import QTest

    identity = "d" * 32
    path = page.client.root / "artifacts" / identity / "simulation.json"
    path.parent.mkdir(parents=True)
    raw = json.dumps(result_record()).encode("utf-8")
    path.write_bytes(raw)
    page.update_jobs(jobs())
    page.runs.selectRow(0)
    page.client.resultReceived.emit(
        page._expected_job,
        {"id": identity, "kind": "simulate", "state": "complete", "result_file": str(path)},
    )
    for _ in range(100):
        qt_app.processEvents()
        if page.summary is not None:
            break
        QTest.qWait(5)
    assert page.summary is not None
    assert path.read_bytes() == raw
    assert "SHA256" in page.status.text()


def test_selecting_another_job_invalidates_prior_worker_result(page):
    page.update_jobs(jobs())
    page.runs.selectRow(0)
    old_generation = page._generation
    page.runs.selectRow(1)
    page._loaded(old_generation, {"summary": {}, "path": "bad", "sha256": "bad"}, "")
    assert page.summary is None


def test_record_filter_scans_before_display_limit_and_keeps_currency(page):
    record = result_record()
    sample = record["fills"][0]
    record["fills"] = [sample | {"tag": f"fill-{i}"} for i in range(2001)]
    record["fills"][-1].update(tag="LAST-ONLY", fee_asset="BNB", fee="0.01")
    load_direct(page, record)
    assert page.fills.rowCount() == 2000
    page.search_records.setText("LAST-ONLY")
    assert page.fills.rowCount() == 1
    assert page.fills.item(0, 4).text() == "0.01 BNB"
    assert page.fill_count.text().startswith("1 / 1")


def test_curve_reduction_keeps_full_time_range_and_extrema(page):
    rows = [
        {"timestamp_ms": i * 60000, "equity": str(1 if i != 2500 else 1000)} for i in range(5000)
    ]
    page.plot.set_points("fixture", rows, "equity")
    assert len(page.plot.geometry_points) <= 2000
    assert page.plot.geometry_points[0][0] == 0
    assert page.plot.geometry_points[-1][0] == 4999 * 60000
    assert max(point[1] for point in page.plot.geometry_points) == 1000


@pytest.mark.parametrize("theme", ["light", "dark"])
def test_resize_theme_and_shell_title_preserve_access_to_results(page, qt_app, theme):
    from omega.gui.presentation import configure_fonts, stylesheet

    configure_fonts(qt_app)
    page.setStyleSheet(stylesheet(theme))
    page.set_presentation(theme, "ko")
    page.set_shell_embedded(True)
    page.update_jobs(jobs())
    load_direct(page)
    page.show()
    for width, height in ((1360, 740), (800, 440), (1360, 740)):
        page.resize(width, height)
        qt_app.processEvents()
        assert page.width() == width and page.height() == height
        assert page.run_combo.isVisible() is (width < 1000)
        assert not page.title.isVisible()
        for index in (0, 1, 2, 3, 4):
            page.tabs.setCurrentIndex(index)
            assert not page.grab().isNull()
    page.set_presentation(theme, "en")
    assert page.tabs.tabText(1) == "Equity / drawdown"


def test_compact_curve_keeps_plot_and_long_explanation_accessible_by_scroll(page, qt_app):
    from PySide6.QtWidgets import QScrollArea

    page.set_shell_embedded(True)
    page.tabs.setCurrentIndex(1)
    page.curve_note.setText("\n".join(["Recorded data; gaps are retained."] * 30))
    page.show()
    page.resize(800, 440)
    qt_app.processEvents()
    scroll = page.tabs.widget(1)
    assert isinstance(scroll, QScrollArea)
    assert page.size().width() == 800 and page.size().height() == 440
    assert page.plot.height() >= 180
    assert scroll.verticalScrollBar().maximum() > 0
    scroll.ensureWidgetVisible(page.curve_note)
    qt_app.processEvents()
    assert scroll.verticalScrollBar().value() > 0


def test_disconnection_blocks_api_but_retains_loaded_inert_results(page):
    load_direct(page)
    page.client.attached = False
    page.set_attached(False)
    page.client.calls.clear()
    page.refresh()
    assert not page.client.calls
    assert not page.refresh_button.isEnabled()
    assert page.open_button.isEnabled()
    assert page.summary is not None


def test_copy_setup_requires_actual_job_request_and_emits_independent_draft(page):
    load_direct(page)
    assert not page.copy_button.isEnabled()
    parameters = {"dataset": "recorded-dataset.json", "ts_module_id": "tef", "spec": {"seed": 7}}
    # The same successful load callback used by the background reader enables copying.
    page._copy_parameters = parameters
    page._update_copy_button()
    copied = []
    page.configurationDraft.connect(copied.append)
    page.copy_button.click()
    assert copied == [parameters]
    copied[0]["spec"]["seed"] = 9
    assert parameters["spec"]["seed"] == 7
    assert not any(args[0] == "POST" for args, _kwargs in page.client.calls)
    assert "현재 설치된 TS" in page.copy_button.toolTip()
    page._clear()
    assert not page.copy_button.isEnabled()


def test_job_parameters_are_passed_only_with_validated_selected_result_path(page, monkeypatch):
    identity = "d" * 32
    page.select_run(identity)
    captures = []
    monkeypatch.setattr(page, "open_path", lambda *args, **kwargs: captures.append((args, kwargs)))
    parameters = {"dataset": "fixture.json", "spec": {"seed": 1}}
    page.client.resultReceived.emit(
        page._expected_job,
        {
            "id": identity,
            "kind": "simulate",
            "state": "complete",
            "result_file": str(page.client.root / "artifacts" / identity / "simulation.json"),
            "parameters": parameters,
        },
    )
    assert captures[0][1]["parameters"] == parameters
    assert captures[0][1]["allowed_root"] == page.client.root / "artifacts"
