"""Actual Qt configuration controls; no NN plugin is imported/executed by the GUI."""

import importlib.util
import json
import os

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["omega-neural-controls-tests"])


@pytest.fixture
def page(qt_app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.pages import ResearchPage

    class Client(QObject):
        modulesReceived = Signal(dict)
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls = []

        def submit_job(self, kind, parameters):
            self.calls.append((kind, parameters))
            return f"submit:{len(self.calls)}"

        def request(self, *_args, **_kwargs):
            return "request"

    client = Client()
    widget = ResearchPage(client, ["train"])
    widget.dataset.setText("datasets/test/dataset.json")
    widget.set_attached(True)
    yield widget
    widget.close()
    widget.deleteLater()
    qt_app.processEvents()


def select_source(page, name):
    combo = page.neural_controls.source
    combo.setCurrentIndex(combo.findData(name))


def test_builtin_pytorch_has_structured_mlp_gru_configuration_and_no_external_path(page):
    select_source(page, "pytorch_return")
    controls = page.neural_controls.editor.controls
    architecture = controls[("config", "architecture")][0]
    architecture.setCurrentIndex(architecture.findData("gru"))
    lookback = controls[("config", "lookback")][0]
    lookback.setText("6")
    lookback.editingFinished.emit()
    page.submit()
    kind, parameters = page.client.calls[-1]
    assert kind == "train"
    assert parameters["builtin"] == "pytorch_return"
    assert parameters["config"]["architecture"] == "gru"
    assert parameters["config"]["lookback"] == 6
    assert "module" not in parameters and "demo" not in parameters
    assert "PyTorch" in page.neural_controls.notice.text()


def test_demo_and_builtin_source_selection_are_exclusive(page):
    page.demo_instance.setChecked(True)
    assert page.neural_controls.source.currentData() == "demo"
    select_source(page, "pytorch_return")
    assert not page.demo_instance.isChecked()
    page.inspect_module()
    assert page.client.calls[-1] == ("nn_inspect", {"builtin": "pytorch_return"})


def test_external_config_is_owned_by_module_and_invalid_drafts_survive_polling(page):
    page.module.setText("trusted/neuralnetwork.py")
    data = {"config": {"custom_layers": [4, 7, 1]}, "metadata": "keep"}
    page.parameters.setPlainText(json.dumps(data))
    assert not page.neural_controls.editor.controls
    page.submit()
    assert page.client.calls[-1][1]["config"] == data["config"]
    invalid = '{"config": {'
    page.parameters.setPlainText(invalid)
    page.neural_controls.resume.setChecked(True)
    assert page.parameters.toPlainText() == invalid


def test_checkpoint_resume_requires_relative_path_and_preserves_draft(page):
    select_source(page, "pytorch_return")
    controls = page.neural_controls
    controls.run_directory.setText("research/artifacts/old/training")
    controls.checkpoint.setText("checkpoints/epoch_00002.pt")
    controls.resume.setChecked(True)
    page.submit()
    assert page.client.calls[-1][1]["resume"] == {
        "run": "research/artifacts/old/training",
        "checkpoint": "checkpoints/epoch_00002.pt",
    }
    count = len(page.client.calls)
    controls.checkpoint.setText("../escape.pt")
    page.submit()
    assert len(page.client.calls) == count
    assert "상대 경로" in page.result.toPlainText()


def test_resume_blocks_new_monte_carlo_generation(page):
    select_source(page, "demo")
    controls = page.neural_controls
    controls.run_directory.setText("old/training")
    controls.checkpoint.setText("model.json")
    controls.resume.setChecked(True)
    page.mc_training.setChecked(True)
    page.submit()
    assert not page.client.calls
    assert "Monte Carlo" in page.result.toPlainText()


def test_selected_completed_training_creates_editable_resume_without_submitting(page):
    job_id = "a" * 32
    detail = {
        "id": job_id,
        "kind": "train",
        "state": "complete",
        "result_file": "research/artifacts/old/training/run.json",
        "parameters": {
            "builtin": "pytorch_return",
            "dataset": "original/dataset.json",
            "seed": 123,
            "device": "cpu",
            "config": {"epochs": 3, "architecture": "gru"},
            "monte_carlo": {"enabled": True, "seed": 123},
        },
        "result": {
            "checkpoint": "checkpoints/epoch_00003.pt",
            "completed_epochs": 3,
            "training_dataset_file": "research/artifacts/old/generated_dataset",
        },
    }
    page.update_jobs([detail])
    page.jobs.selectRow(0)
    page.client.resultReceived.emit("job:" + job_id, detail)
    assert page.resume_selected_button.isEnabled()
    page.resume_selected_button.click()
    assert not page.client.calls
    assert page.dataset.text() == "research/artifacts/old/generated_dataset"
    assert page.neural_controls.source.currentData() == "pytorch_return"
    assert page.seed.value() == 123
    assert not page.mc_training.isChecked()
    parameters = json.loads(page.parameters.toPlainText())
    assert parameters["config"]["epochs"] == 4
    assert parameters["resume"]["checkpoint"] == "checkpoints/epoch_00003.pt"
    assert "monte_carlo" not in parameters


def test_nn_configuration_resizes_with_actual_scrollable_page(page, qt_app):
    select_source(page, "pytorch_return")
    page.show()
    for width, height in ((1360, 860), (800, 560)):
        page.resize(width, height)
        qt_app.processEvents()
        assert page.width() == width and page.height() == height
        assert not page.grab().isNull()


def test_external_schema_is_data_only_and_stale_inspection_cannot_replace_current_form(page):
    page.module.setText("trusted/first.py")
    page.inspect_module()
    identity = "b" * 32
    page.client.resultReceived.emit(
        "submit:1", {"id": identity, "kind": "nn_inspect", "state": "queued"}
    )
    descriptor = {
        "id": identity,
        "kind": "nn_inspect",
        "state": "complete",
        "result": {
            "configuration_schema": {
                "type": "object",
                "properties": {
                    "layers": {"type": "array", "default": [4, 8, 1]},
                },
            },
        },
    }
    wrong_job = "c" * 32
    page.update_jobs([{"id": wrong_job, "kind": "nn_inspect", "state": "complete"}])
    page.jobs.selectRow(0)
    page.client.resultReceived.emit("job:" + wrong_job, descriptor | {"id": wrong_job})
    assert not page.neural_controls.editor.controls
    page.client.resultReceived.emit("nn_descriptor:" + identity, descriptor)
    assert ("config", "layers") in page.neural_controls.editor.controls
    page.module.setText("trusted/second.py")
    page.module.textEdited.emit("trusted/second.py")
    assert not page.neural_controls.editor.controls
    page.client.resultReceived.emit("nn_descriptor:" + identity, descriptor)
    assert not page.neural_controls.editor.controls


def test_completed_detail_enables_artifact_before_next_snapshot_and_stale_snapshot_cannot_regress(
    page,
):
    identity = "e" * 32
    running = {"id": identity, "kind": "train", "state": "running"}
    complete = running | {"state": "complete", "result_file": "old/training/run.json"}
    captured = []
    page.openArtifact.connect(captured.append)
    page.update_jobs([running])
    page.jobs.selectRow(0)
    assert not page.open_button.isEnabled()
    page.client.resultReceived.emit("job:" + identity, complete)
    assert page.open_button.isEnabled()
    page.open_button.click()
    assert captured == ["old/training/run.json"]
    page.update_jobs([running])
    assert page._jobs[identity]["state"] == "complete"
    assert page.open_button.isEnabled()
    page.update_jobs([running | {"state": "unknown"}])
    assert not page.open_button.isEnabled()
    page.client.resultReceived.emit("job:" + identity, complete)
    assert page._jobs[identity]["state"] == "unknown"
    assert not page.open_button.isEnabled()
    page.update_jobs([running])
    assert page._jobs[identity]["state"] == "unknown"
    assert not page.open_button.isEnabled()


def test_old_epoch_detail_is_not_retagged_by_reselecting_same_pending_request(page):
    identity = "f" * 32
    tag = "job:" + identity
    page.client.state = {"core": {"epoch": "old"}}
    running = {"id": identity, "kind": "train", "state": "running"}
    complete = running | {"state": "complete", "result_file": "old/training/run.json"}
    page.update_jobs([running])
    page.jobs.selectRow(0)
    assert page._job_query_epochs[tag] == "old"
    page.client._pending = {tag: object()}
    page.client.state = {"core": {"epoch": "new"}}
    page.update_jobs([running])
    page.inspect_job()
    assert page._job_query_epochs[tag] == "old"
    page.client.resultReceived.emit(tag, complete)
    assert not page.open_button.isEnabled()
    assert not page._selected_job_detail
    page.client._pending.clear()
    page.inspect_job()
    page.client.resultReceived.emit(tag, complete)
    assert page.open_button.isEnabled()


def test_narrow_page_preserves_accessible_action_geometry_in_scroll_area(page, qt_app):
    from PySide6.QtCore import QPoint

    page.show()
    page.resize(800, 560)
    qt_app.processEvents()
    for button in (page.run_button, page.open_button, page.resume_selected_button):
        page.configuration_scroll.ensureWidgetVisible(button)
        qt_app.processEvents()
        origin = button.mapTo(page.configuration_scroll.viewport(), QPoint(0, 0))
        assert 0 <= origin.y()
        assert origin.y() + button.height() <= page.configuration_scroll.viewport().height()
        assert button.height() >= button.fontMetrics().height()
