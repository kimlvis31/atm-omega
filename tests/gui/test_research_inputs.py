"""Actual Qt input controls submit jobs; they never open datasets or market storage."""

import importlib.util
import json
import os

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)

BTC, ETH = "binance:usdm:BTCUSDT", "binance:usdm:ETHUSDT"


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["omega-research-input-tests"])


@pytest.fixture
def pages(qt_app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.pages import ResearchPage

    class Client(QObject):
        modulesReceived = Signal(dict)
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True

        def __init__(self):
            super().__init__()
            self.state = {"core": {"epoch": "epoch-1", "capabilities": {"auxiliary_export": True}}}
            self.calls, self.requests = [], []

        def submit_job(self, kind, parameters):
            self.calls.append((kind, parameters))
            return f"submit:{len(self.calls)}"

        def request(self, method, path, *, tag):
            self.requests.append((method, path, tag))
            return tag

    client = Client()
    widgets = [ResearchPage(client, kinds) for kinds in (["export", "monte_carlo"], ["train"])]
    for widget in widgets:
        widget.dataset.setText("selected/dataset.json")
        widget.set_attached(True)
    yield widgets
    for widget in widgets:
        widget.close()
        widget.deleteLater()
    qt_app.processEvents()


def _summary(auxiliary=False):
    return {
        "schema_id": "omega.market-observations.1m.v1" if auxiliary else "omega.ohlcv.1m.v1",
        "rows": 120,
        "instruments": [BTC, ETH],
        "time_range": [0, 3_600_000],
        "instrument_ranges": {BTC: [0, 3_600_000], ETH: [0, 3_600_000]},
        "auxiliary": (
            {
                "schema_id": "omega.market-observations.1m.v1",
                "channels": {"open_interest": {"count": 24}},
                "scope": "retrospective_training_only",
            }
            if auxiliary
            else None
        ),
    }


def _inspect(page, summary=None, identity="a" * 32):
    controls, client = page.dataset_controls, page.client
    controls.inspect_button.click()
    tag = f"submit:{len(client.calls)}"
    accepted = {"id": identity, "kind": "inspect_dataset", "state": "queued"}
    client.resultReceived.emit(tag, accepted)
    page.update_jobs([accepted | {"state": "complete"}])
    query = client.requests[-1][2]
    client.resultReceived.emit(
        query, accepted | {"state": "complete", "result": summary or _summary()}
    )
    return controls


def _selection(controls, source="binance:usdm:BTCUSDT:open_interest_history", instrument=BTC):
    controls.instrument.setCurrentText(instrument)
    controls.channel.setCurrentText("open_interest")
    controls.observation_source.setText(source)
    controls.add_button.click()


def test_inspection_runs_in_worker_and_populates_verified_instruments(pages):
    page = pages[0]
    controls = _inspect(page)
    assert page.client.calls == [("inspect_dataset", {"dataset": "selected/dataset.json"})]
    assert page.client.requests[-1][0:2] == ("GET", "/api/jobs/" + "a" * 32)
    assert controls.metadata == _summary()
    assert [controls.instrument.itemText(i) for i in range(controls.instrument.count())] == [
        BTC,
        ETH,
    ]
    assert "OHLCV 검증 완료" in controls.notice.text()
    assert controls.inspect_button.isEnabled()


def test_structured_auxiliary_export_has_explicit_source_and_cutoff(pages):
    page = pages[0]
    page.kind.setCurrentText("export_auxiliary")
    controls = _inspect(page)
    _selection(controls)
    controls.as_of.setText("9000000")
    page.submit()
    assert page.client.calls[-1] == (
        "export_auxiliary",
        {
            "candle_dataset": "selected/dataset.json",
            "selections": [
                {
                    "instrument": BTC,
                    "channel": "open_interest",
                    "source": "binance:usdm:BTCUSDT:open_interest_history",
                }
            ],
            "as_of_ms": 9_000_000,
        },
    )
    assert "실제 availability" in controls.export_notice.text()
    assert all(path.startswith("/api/jobs/") for _, path, _ in page.client.requests)


def test_auxiliary_inputs_remain_available_for_mc_and_nn_training(pages):
    data, neural = pages
    _inspect(data, _summary(True))
    data.kind.setCurrentText("monte_carlo")
    data.submit()
    assert data.client.calls[-1][0] == "monte_carlo"
    assert "회고 학습/MC 전용" in data.dataset_controls.notice.text()
    _inspect(neural, _summary(True), "b" * 32)
    neural.demo_instance.setChecked(True)
    neural.mc_training.setChecked(True)
    neural.submit()
    kind, parameters = neural.client.calls[-1]
    assert kind == "train" and parameters["demo"] is True
    assert parameters["dataset"] == "selected/dataset.json"
    assert parameters["monte_carlo"]["enabled"] is True
    assert not any(kind in {"nn_prepare", "nn_predict"} for kind, _ in neural.client.calls)
    assert "인과 추론 준비/배포 불가" in neural.dataset_controls.notice.text()


def test_path_change_and_core_restart_discard_late_inspection(pages):
    page = pages[0]
    controls, client = page.dataset_controls, page.client
    controls.inspect()
    client.resultReceived.emit(
        "submit:1", {"id": "a" * 32, "kind": "inspect_dataset", "state": "queued"}
    )
    page.update_jobs([{"id": "a" * 32, "state": "complete"}])
    query = client.requests[-1][2]
    page.dataset.setText("different/dataset.json")
    client.resultReceived.emit(
        query,
        {"id": "a" * 32, "kind": "inspect_dataset", "state": "complete", "result": _summary()},
    )
    assert controls.metadata is None and controls.instrument.count() == 0
    _inspect(page, identity="b" * 32)
    _selection(controls)
    client.state["core"]["epoch"] = "epoch-2"
    page.set_attached(True)
    assert controls.metadata is None and controls.selections.rowCount() == 0
    with pytest.raises(ValueError, match="먼저 검증"):
        controls.export_parameters()


def test_export_limits_use_exact_instrument_ranges_and_explicit_selection_count(pages):
    page = pages[0]
    summary = _summary()
    summary["time_range"] = [0, 1_200_000_000]
    summary["instrument_ranges"] = {BTC: [0, 1_200_000_000], ETH: [0, 600_000]}
    controls = _inspect(page, summary)
    _selection(controls, "small:ETH", ETH)
    assert controls.export_parameters()["selections"][0]["instrument"] == ETH
    _selection(controls, "large:BTC", BTC)
    with pytest.raises(ValueError, match="10,000분"):
        controls.export_parameters()
    controls.selections.selectRow(1)
    controls.remove_button.click()
    for index in range(7):
        _selection(controls, f"explicit-source:{index}", ETH)
    assert controls.selections.rowCount() == 8
    _selection(controls, "ninth-source", ETH)
    assert controls.selections.rowCount() == 8
    assert "최대 8개" in controls.notice.text()


def test_duplicate_missing_source_and_invalid_asof_are_rejected(pages):
    controls = _inspect(pages[0])
    controls.add_button.click()
    assert controls.selections.rowCount() == 0 and "source" in controls.notice.text()
    _selection(controls)
    _selection(controls)
    assert controls.selections.rowCount() == 1 and "이미 있습니다" in controls.notice.text()
    controls.as_of.setText("1.5")
    with pytest.raises(ValueError, match="정수"):
        controls.export_parameters()


def test_failed_or_pending_inspection_cannot_be_used_as_validated_input(pages):
    page = pages[0]
    controls = page.dataset_controls
    controls.inspect()
    page.kind.setCurrentText("monte_carlo")
    count = len(page.client.calls)
    page.submit()
    assert len(page.client.calls) == count
    assert "검증 완료" in page.result.toPlainText()
    page.client.failed.emit("submit:1", "synthetic input hash mismatch")
    page.submit()
    assert len(page.client.calls) == count and controls.metadata is None
    assert "검증에 실패" in page.result.toPlainText()


def test_completed_dataset_selection_creates_validated_mc_draft_without_running_mc(pages):
    page = pages[0]
    identity = "c" * 32
    page.update_jobs(
        [
            {
                "id": identity,
                "kind": "export_auxiliary",
                "state": "complete",
                "result_file": "saved/aux/dataset.json",
            }
        ]
    )
    page.jobs.selectRow(0)
    assert page.use_dataset_button.isEnabled()
    page.use_dataset_button.click()
    assert page.dataset.text() == "saved/aux/dataset.json"
    assert page.kind.currentText() == "monte_carlo"
    assert page.client.calls == [("inspect_dataset", {"dataset": "saved/aux/dataset.json"})]


def test_export_capability_and_plain_csv_export_are_preserved(pages):
    page = pages[0]
    page.client.state["core"]["capabilities"]["auxiliary_export"] = False
    page.kind.setCurrentText("export_auxiliary")
    assert not page.run_button.isEnabled()
    page.submit()
    assert not page.client.calls and "지원하지 않습니다" in page.result.toPlainText()
    page.kind.setCurrentText("export")
    page.dataset.setText("original.csv")
    page.submit()
    kind, parameters = page.client.calls[-1]
    assert kind == "export" and parameters["source_file"] == "original.csv"
    assert "split_ratios" in parameters and "candle_dataset" not in parameters


def test_malformed_inspection_does_not_make_an_export_ready(pages):
    summary = _summary()
    summary["instrument_ranges"][BTC] = [True, 60000]
    controls = _inspect(pages[0], summary)
    assert controls.metadata is None and "Invalid" in controls.notice.text()
    with pytest.raises(ValueError, match="검증에 실패"):
        controls.export_parameters()


def test_dataset_form_resizes_and_keeps_user_advanced_draft(pages, qt_app):
    page = pages[0]
    page.kind.setCurrentText("export_auxiliary")
    controls = _inspect(page)
    _selection(controls)
    draft = '{"unfinished": '
    page.parameters.setPlainText(draft)
    page.show()
    for width, height in ((1360, 860), (800, 560)):
        page.resize(width, height)
        qt_app.processEvents()
        assert not page.grab().isNull()
        assert page.parameters.toPlainText() == draft
    assert json.loads(json.dumps(controls.export_parameters()))["selections"]
