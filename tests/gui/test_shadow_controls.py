"""Structured shadow GUI API contracts; no model import, real HTTP or account IO."""

import importlib.util
import os
from copy import deepcopy
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt unavailable"
)
TRAIN, OTHER, CANDIDATE = "a" * 32, "b" * 32, "c" * 64
INSTRUMENT = "binance:usdm:BTCUSDT"


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtGui import QFont, QFontDatabase
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication(["shadow-gui-tests"])
    font = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts/malgun.ttf"
    if not QFontDatabase.families() and font.is_file():
        identity = QFontDatabase.addApplicationFont(str(font))
        if identity >= 0:
            app.setFont(QFont(QFontDatabase.applicationFontFamilies(identity)[0], 10))
    return app


@pytest.fixture
def client(qt_app):
    from PySide6.QtCore import QObject, Signal

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        stateReceived = Signal(dict)
        modulesReceived = Signal(dict)

        def __init__(self):
            super().__init__()
            self.attached, self.calls = True, []
            self.state = state()

        def request(self, method, path, payload=None, *, tag):
            self.calls.append((method, path, deepcopy(payload), tag))

        def submit_job(self, kind, parameters):
            return "research-job-not-used"

    return Client()


def state(epoch="epoch-a", capable=True):
    return {"core": {"epoch": epoch, "capabilities": {"shadow_inference": capable}}}


def snapshot(*, active=False, paused=False, revision=2, operations=None, **changes):
    result = {
        "schema_version": 1,
        "scope": "paper_shadow_only",
        "core_epoch": "epoch-a",
        "state": "ready",
        "closed": False,
        "error": None,
        "deployment": {
            "document": {
                "schema_version": 1,
                "scope": "paper_shadow_only",
                "core_epoch": "epoch-a",
                "revision": revision,
                "paused": paused,
                "candidate": {"candidate_id": CANDIDATE, "path": "owned/candidate"}
                if active
                else None,
                "streams": {
                    "gui-preview|" + INSTRUMENT: {
                        "epoch": "d" * 32,
                        "sequence": 0,
                        "open_ms": None,
                        "health": "ready",
                        "reason": "explicit_start",
                    }
                }
                if active
                else {},
                "requests": {},
            },
            "checksum": "e" * 64,
        },
        "candidates": [
            {
                "candidate_id": CANDIDATE,
                "path": "owned/candidate",
                "training_job_id": TRAIN,
                "fixture_verified": True,
            }
        ]
        if active
        else [],
        "operations": operations or [],
    }
    result.update(changes)
    return result


def metadata(*, defaults=True, **changes):
    contract = {
        "schema_id": "omega.ohlcv.1m.v1",
        "features": ["close/open-1", "high/open-1", "low/open-1", "log_volume"],
        "lookback": 2,
        "input_shape": [None, 2, 4],
        "output_shape": [None],
        "state_shape": None,
        "state_version": None,
        "allow_no_signal": False,
    }
    result = {
        "schema_version": 1,
        "training_job_id": TRAIN,
        "module": {
            "module_id": "omega.example.pytorch_return",
            "input": {"schema_id": contract["schema_id"], "features": contract["features"]},
            "capabilities": {"stateful_inference": True},
        },
        "config": {"lookback": 2, "architecture": "mlp", "hidden_size": 4},
        "device": {"used": "cpu"},
        "checkpoint": {"file": "checkpoints/epoch_00001.pt", "sha256": "f" * 64},
        "contract_defaults": contract if defaults else None,
        "shape_confirmation_required": not defaults,
    }
    result.update(changes)
    return result


@pytest.fixture
def panel(qt_app, client):
    from omega.gui.shadow_controls import ShadowControls

    view = ShadowControls(client)
    view.update_state(state())
    view.set_training_selection(TRAIN, core_epoch="epoch-a")
    view.setChecked(True)
    client.resultReceived.emit(client.calls[-1][3], snapshot())
    yield view
    view.timer.stop()
    view.close()
    view.deleteLater()
    qt_app.processEvents()


def operation(panel, *, state_value="completed", result=None, **changes):
    posted = panel._pending
    row = {
        "id": posted["id"],
        "kind": posted["kind"],
        "parameters": deepcopy(posted["parameters"]),
        "execution_id": "1" * 32,
        "core_epoch": panel.epoch,
        "operation_core_epoch": posted["epoch"],
        "state": state_value,
        "job_id": None,
        "result": result,
        "error": None,
        "scope": "paper_shadow_only",
    }
    row.update(changes)
    return row


def finish(panel, *, result=None, snapshot_value=None, state_value="completed", **changes):
    row = operation(panel, result=result, state_value=state_value, **changes)
    tag = panel.client.calls[-1][3]
    panel.client.resultReceived.emit(tag, row)
    if panel._pending is None and panel.client.calls[-1][1] == "/api/shadow-models":
        value = deepcopy(snapshot_value or snapshot())
        value["operations"] = [row]
        panel.client.resultReceived.emit(panel.client.calls[-1][3], value)
    return row


def inspect(panel, value=None, *, snapshot_value=None):
    panel.inspect_training()
    finish(panel, result=metadata() if value is None else value, snapshot_value=snapshot_value)


def ready_input(panel, *, active=False):
    inspect(panel, snapshot_value=snapshot(active=active))
    panel.apply_metadata()
    panel.build_input()
    panel.fill_example()
    panel.shape_confirm.setChecked(True)
    panel.trusted.setChecked(True)


def test_defaults_collapsed_and_no_automatic_model_activation(client, qt_app):
    from omega.gui.shadow_controls import ShadowControls

    view = ShadowControls(client)
    try:
        view.update_state(state())
        view.set_training_selection(TRAIN, core_epoch="epoch-a")
        assert not view.isChecked() and client.calls == []
        assert not view.activate_button.isEnabled()
    finally:
        view.timer.stop()
        view.deleteLater()


def test_inspect_uses_real_training_id_and_never_invents_a_contract(panel):
    panel.inspect_training()
    method, path, payload, _ = panel.client.calls[-1]
    assert (method, path) == ("POST", "/api/shadow-models")
    assert payload["kind"] == "inspect_training"
    assert payload["parameters"] == {"training_job_id": TRAIN}
    finish(panel, result=metadata())
    assert panel.schema.text() == ""  # Explicit form application is separate.
    panel.apply_metadata()
    assert panel.schema.text() == "omega.ohlcv.1m.v1"
    assert panel.features.text().split(", ") == metadata()["module"]["input"]["features"]
    assert panel.contract()["input_shape"] == [None, 2, 4]
    assert not panel.shape_confirm.isChecked() and not panel.trusted.isChecked()
    assert not any(call[2] and call[2].get("kind") == "activate" for call in panel.client.calls)


def test_fixture_requires_explicit_trust_shape_and_finite_prepared_grid(panel):
    inspect(panel)
    panel.apply_metadata()
    panel.build_input()
    assert not panel.fixture_button.isEnabled()
    panel.shape_confirm.setChecked(True)
    panel.trusted.setChecked(True)
    count = len(panel.client.calls)
    panel.prepare_fixture()
    assert len(panel.client.calls) == count and "explicit finite" in panel.status.text()
    panel.fill_example()
    panel.prepare_fixture()
    posted = panel.client.calls[-1][2]
    assert posted["kind"] == "prepare_fixture"
    assert posted["parameters"]["training_job_id"] == TRAIN
    assert posted["parameters"]["trusted_module"] is True
    assert len(posted["parameters"]["fixture_batch"][0]) == 2
    assert posted["parameters"]["fixture_batch"][0][0] == [0.01, 0.02, 0.03, 0.04]
    assert "합성" in panel.input_hint.text()


def test_verified_candidate_activation_is_explicit_and_revision_bound(panel):
    ready_input(panel)
    panel.prepare_fixture()
    finish(
        panel, result={"candidate_id": CANDIDATE}, snapshot_value=snapshot(active=True, revision=4)
    )
    assert not any(call[2] and call[2].get("kind") == "activate" for call in panel.client.calls)
    panel.candidates.setCurrentIndex(panel.candidates.findData(CANDIDATE))
    panel.activate()
    assert panel.client.calls[-1][2]["parameters"] == {
        "candidate_id": CANDIDATE,
        "expected_revision": 4,
    }


def test_predict_uses_active_training_contract_and_explicit_shadow_input(panel):
    ready_input(panel, active=True)
    panel.predict()
    parameters = panel.client.calls[-1][2]["parameters"]
    assert parameters["stream"] == "gui-preview" and parameters["instrument"] == INSTRUMENT
    assert parameters["sequence"] == 1 and len(parameters["input_digest"]) == 64
    assert parameters["schema_id"] == metadata()["contract_defaults"]["schema_id"]
    assert parameters["features"] == metadata()["contract_defaults"]["features"]
    assert parameters["as_of_ms"] < parameters["deadline_ms"] <= parameters["as_of_ms"] + 60000
    result = {
        "scope": "paper_shadow_only",
        "values": [0.125],
        "no_signal": False,
        "open_time_ms": parameters["open_time_ms"],
        "sequence": 1,
    }
    finish(panel, result=result, snapshot_value=snapshot(active=True, revision=5))
    assert panel.output.item(0, 1).text() == "0.125"
    assert "TS orders: 0" in panel.output_label.text()


def test_stream_reset_pause_resume_are_explicit_revision_commands(panel):
    ready_input(panel, active=True)
    panel.reset_stream()
    assert panel.client.calls[-1][2]["parameters"] == {
        "stream": "gui-preview",
        "instrument": INSTRUMENT,
        "expected_revision": 2,
        "reason": "gui_explicit_reset",
    }
    finish(panel, snapshot_value=snapshot(active=True, revision=3))
    panel.pause()
    assert panel.client.calls[-1][2]["kind"] == "pause"
    finish(panel, snapshot_value=snapshot(active=True, paused=True, revision=4))
    panel.resume()
    assert panel.client.calls[-1][2]["parameters"] == {"expected_revision": 4}


def test_unknown_post_checks_same_id_and_never_reposts(panel):
    panel.inspect_training()
    original = panel.client.calls[-1]
    panel.client.failed.emit(original[3], "timeout")
    panel.inspect_training()
    panel.refresh()
    assert panel.client.calls[-1][:2] == ("GET", "/api/shadow-models/" + original[2]["id"])
    assert len([c for c in panel.client.calls if c[0] == "POST"]) == 1
    finish(panel, result=metadata())
    assert panel.metadata["training_job_id"] == TRAIN


def test_acknowledge_does_not_unlock_a_known_still_running_operation(panel):
    panel.inspect_training()
    panel.client.failed.emit(panel.client.calls[-1][3], "timeout")
    panel.reconcile()
    row = operation(panel, state_value="running")
    panel.client.resultReceived.emit(panel.client.calls[-1][3], snapshot(operations=[row]))
    assert panel._pending is not None and panel._pending["unknown"] is False
    assert not panel.inspect_button.isEnabled()


def test_inspection_selection_change_and_dirty_input_do_not_apply_late_forms(panel):
    panel.inspect_training()
    panel.schema.setText("preserved")
    panel.features.setText("edited")
    panel.set_training_selection(OTHER, core_epoch="epoch-a")
    finish(panel, result=metadata())
    assert panel.metadata is None and panel.schema.text() == "preserved"
    assert panel.features.text() == "edited"


def test_form_edit_while_inference_runs_prevents_late_output_replacement(panel):
    ready_input(panel, active=True)
    panel.predict()
    panel.input_table.item(0, 0).setText("0.9")
    result = {"scope": "paper_shadow_only", "values": [9.0], "no_signal": False}
    finish(panel, result=result, snapshot_value=snapshot(active=True, revision=4))
    assert panel.output.rowCount() == 0
    assert panel.input_table.item(0, 0).text() == "0.9"


def test_new_core_rejects_old_reply_keeps_draft_and_requires_new_metadata(panel):
    ready_input(panel)
    panel.prepare_fixture()
    old_tag = panel.client.calls[-1][3]
    old_row = operation(panel, result={"candidate_id": CANDIDATE})
    before = panel.input_table.item(0, 0).text()
    panel.update_state(state("epoch-b"))
    panel.client.resultReceived.emit(old_tag, old_row)
    assert panel._pending["unknown"] and panel._metadata_epoch is None
    assert panel.input_table.item(0, 0).text() == before
    assert not panel.fixture_button.isEnabled() and panel.metadata is not None


@pytest.mark.parametrize(
    "mutation", ["bool_revision", "duplicate_candidate", "unsafe_operation", "changed_scope"]
)
def test_malformed_snapshot_preserves_previous_authoritative_state(panel, mutation):
    original = deepcopy(panel.snapshot)
    value = snapshot(active=True)
    if mutation == "bool_revision":
        value["deployment"]["document"]["revision"] = True
    elif mutation == "duplicate_candidate":
        value["candidates"].append(deepcopy(value["candidates"][0]))
    elif mutation == "unsafe_operation":
        value["operations"] = [{"id": "bad"}]
    else:
        value["scope"] = "live"
    panel.refresh()
    panel.client.resultReceived.emit(panel.client.calls[-1][3], value)
    assert panel.snapshot == original


def test_unavailable_and_disconnect_never_allow_new_operations(panel):
    panel.set_attached(False)
    count = len(panel.client.calls)
    panel.inspect_training()
    panel.refresh()
    panel.activate()
    assert len(panel.client.calls) == count
    panel.set_attached(True)
    panel.update_state(state(capable=False))
    assert not panel.inspect_button.isEnabled() and not panel.fixture_button.isEnabled()


def test_external_shape_requires_explicit_structured_confirmation(panel):
    value = metadata(defaults=False)
    inspect(panel, value)
    panel.apply_metadata()
    assert panel.input_mode.currentData() == "sequence"
    assert not panel.shape_confirm.isChecked() and not panel.fixture_button.isEnabled()
    panel.recurrent.setChecked(True)
    panel.state_shape.setText("1, 1, 4")
    panel.state_version.setText("hidden.v1")
    assert panel.contract()["state_shape"] == [1, 1, 4]


def test_training_selection_change_preserves_draft_but_blocks_old_fixture(panel):
    ready_input(panel)
    before = panel.input_table.item(0, 0).text()
    panel.set_training_selection(OTHER, core_epoch="epoch-a")
    assert panel.input_table.item(0, 0).text() == before
    assert not panel.fixture_button.isEnabled()
    assert panel.metadata["training_job_id"] == TRAIN


def test_gpu_metadata_does_not_enable_cpu_fixture(panel):
    inspect(panel, metadata(device={"used": "cuda"}))
    panel.apply_metadata()
    panel.build_input()
    panel.fill_example()
    panel.shape_confirm.setChecked(True)
    panel.trusted.setChecked(True)
    assert not panel.fixture_button.isEnabled()


@pytest.mark.parametrize("mutation", ["output_shape", "feature_order", "state_shape"])
def test_invalid_metadata_form_application_is_atomic(panel, mutation):
    ready_input(panel)
    before = panel.contract()
    broken = metadata()
    if mutation == "output_shape":
        broken["contract_defaults"]["output_shape"] = []
    elif mutation == "feature_order":
        broken["contract_defaults"]["features"] = list(
            reversed(broken["module"]["input"]["features"])
        )
    else:
        broken["contract_defaults"]["state_shape"] = [1, 0, 4]
    panel.metadata = broken
    panel.apply_metadata()
    assert panel.contract() == before


def test_stored_prediction_requires_explicit_selection_and_survives_poll(panel):
    ready_input(panel, active=True)
    panel.predict()
    result = {"scope": "paper_shadow_only", "values": [None], "no_signal": True}
    row = finish(panel, result=result, snapshot_value=snapshot(active=True, revision=5))
    panel.output.setRowCount(0)
    index = list(panel._operations).index(row["id"])
    panel.operations.selectRow(index)
    panel._render()
    assert panel.show_result_button.isEnabled()
    panel.show_stored_prediction()
    assert panel.output.item(0, 1).text() == "— / no signal"
    assert "저장 결과" in panel.output_label.text()


def test_failed_operation_is_visible_without_opening_history(panel):
    panel.inspect_training()
    finish(panel, state_value="failed", error="training_metadata_invalid")
    assert "failed" in panel.status.text() and "training_metadata_invalid" in panel.status.text()


def test_research_quarantine_blocks_child_jobs_but_allows_metadata_reads(panel):
    ready_input(panel, active=True)
    panel.set_research_available(False)
    assert not panel.fixture_button.isEnabled() and not panel.predict_button.isEnabled()
    assert panel.inspect_button.isEnabled()
    assert panel.pause_button.isEnabled()


@pytest.mark.parametrize("text", ["NaN", "Infinity", "", "nonnumeric"])
def test_prepared_grid_rejects_nonfinite_or_absent_values(panel, text):
    ready_input(panel)
    panel.input_table.item(0, 0).setText(text)
    count = len(panel.client.calls)
    panel.prepare_fixture()
    assert len(panel.client.calls) == count


def test_gui_pages_nn_only_integration_preserves_existing_training(client, qt_app):
    from omega.gui.pages import ResearchPage

    page = ResearchPage(client, ["train"])
    try:
        assert hasattr(page, "neural_controls") and hasattr(page, "shadow_controls")
        assert not page.shadow_controls.isChecked()
        page.update_jobs(
            [
                {
                    "id": TRAIN,
                    "kind": "train",
                    "state": "complete",
                    "result_file": "owner/training/run.json",
                }
            ]
        )
        page.jobs.selectRow(0)
        assert page.shadow_controls.training_id == TRAIN
        page.set_attached(False)
        assert not page.shadow_controls.attached
        page.set_presentation("dark", "en")
        assert page.shadow_controls.language == "en"
    finally:
        page.shadow_controls.timer.stop()
        page.close()
        page.deleteLater()
        qt_app.processEvents()


@pytest.mark.parametrize("width", [800, 1360])
def test_actual_nn_page_keeps_training_and_shadow_controls_accessible(
    client, qt_app, tmp_path, width
):
    from omega.gui.pages import ResearchPage

    page = ResearchPage(client, ["train"])
    try:
        page.resize(width, 900)
        page.show()
        page.update_jobs(
            [
                {
                    "id": TRAIN,
                    "kind": "train",
                    "state": "complete",
                    "result_file": "owner/training/run.json",
                }
            ]
        )
        page.jobs.selectRow(0)
        shadow = page.shadow_controls
        shadow.setChecked(True)
        client.resultReceived.emit(client.calls[-1][3], snapshot(active=True))
        ready_input(shadow, active=True)
        page.configuration_scroll.ensureWidgetVisible(shadow)
        qt_app.processEvents()
        assert page.width() == width
        assert page.configuration_scroll.horizontalScrollBar().maximum() == 0
        assert page.grab().save(str(tmp_path / f"neural-page-shadow-{width}.png"))
        assert (
            page.neural_controls.isVisible()
            or page.configuration_scroll.verticalScrollBar().maximum() > 0
        )
    finally:
        page.shadow_controls.timer.stop()
        page.close()
        page.deleteLater()
        qt_app.processEvents()


@pytest.mark.parametrize("width", [800, 1360])
def test_qt_all_shadow_tabs_are_accessible_at_two_widths(panel, qt_app, tmp_path, width):
    from PySide6.QtWidgets import QScrollArea

    ready_input(panel, active=True)
    host = QScrollArea()
    host.setWidgetResizable(True)
    host.setWidget(panel)
    host.resize(width, 900)
    host.show()
    qt_app.processEvents()
    try:
        for tab in range(panel.tabs.count()):
            panel.tabs.setCurrentIndex(tab)
            qt_app.processEvents()
            assert panel.width() <= host.viewport().width()
            assert host.grab().save(str(tmp_path / f"shadow-{width}-tab{tab}.png"))
        assert panel.contract()["features"] == metadata()["module"]["input"]["features"]
    finally:
        host.takeWidget()
        host.close()
        host.deleteLater()


def raw_ready(panel, monkeypatch, *, supported=True):
    from PySide6.QtCore import QDateTime, QTimeZone

    value = metadata()
    value["module"]["capabilities"]["prepare_inference"] = supported
    inspect(panel, value, snapshot_value=snapshot(active=True))
    panel.apply_metadata()
    panel.raw_dataset.setText("owner/source/dataset.json")
    panel.raw_confirm.setChecked(True)
    panel.open_time.setDateTime(QDateTime.fromMSecsSinceEpoch(600000, QTimeZone(b"UTC")))
    monkeypatch.setattr("omega.gui.shadow_controls.time.time_ns", lambda: 1_000_000_000_000)


def raw_prediction():
    value = {
        "scope": "offline_retrospective_shadow_only",
        "candidate_id": CANDIDATE,
        "deployment_epoch": "d" * 32,
        "stream_epoch": "e" * 32,
        "open_time_ms": 600000,
        "sequence": 1,
        "input_digest": "f" * 64,
        "values": [0.25],
        "no_signal": False,
    }
    value["preparation"] = {
        "scope": value["scope"],
        "reference_time_ms": 660000,
        "snapshot_available_at_ms": 1_000_000,
        "as_of_ms": 1_000_000,
        "dataset_sha256": "1" * 64,
        "rows_sha256": "2" * 64,
        "window_sha256": "3" * 64,
        "prepared_sha256": "f" * 64,
        "batch_sha256": "4" * 64,
        "request": {
            key: value[key]
            for key in (
                "candidate_id",
                "sequence",
                "open_time_ms",
                "deployment_epoch",
                "stream_epoch",
            )
        },
        "lineage": {
            "dataset_kind": "normalized_source",
            "source": {"file": "source.csv"},
            "quality": {"interpolated_rows": 0},
            "synthetic": True,
            "availability": "actual_snapshot_only; historical received times unavailable",
        },
    }
    return value


def test_raw_submission_uses_exact_owner_fields_and_no_gui_batch_hash(panel, monkeypatch):
    raw_ready(panel, monkeypatch)
    assert panel.input_table.rowCount() == 0  # No prepared tensor or contract edit is required.

    def forbidden_hash(*_args, **_kwargs):
        raise AssertionError("The GUI must not calculate a raw source hash")

    monkeypatch.setattr("omega.gui.shadow_controls.sha256", forbidden_hash)
    panel.predict_raw()
    body = panel.client.calls[-1][2]
    assert body["kind"] == "predict_raw"
    assert body["parameters"] == {
        "expected_revision": 2,
        "stream": "gui-preview",
        "instrument": INSTRUMENT,
        "sequence": 1,
        "open_time_ms": 600000,
        "deadline_ms": 1_015_000,
        "dataset": "owner/source/dataset.json",
        "retrospective_confirmed": True,
    }


def test_raw_consent_and_capability_are_required_but_prepared_path_remains(panel, monkeypatch):
    raw_ready(panel, monkeypatch, supported=False)
    assert not panel.raw_predict_button.isEnabled()
    assert panel.predict_button.isEnabled()
    assert "재학습" in panel.raw_support.text()
    count = len(panel.client.calls)
    panel.predict_raw()
    assert len(panel.client.calls) == count
    raw_ready(panel, monkeypatch)
    panel.raw_confirm.setChecked(False)
    panel.predict_raw()
    assert panel._pending is None
    panel.raw_confirm.setChecked(True)
    panel.raw_dataset.setText("different/source")
    assert not panel.raw_confirm.isChecked() and not panel.raw_predict_button.isEnabled()


def test_raw_dataset_handoff_is_explicit_and_poll_never_replaces_dirty_path(panel, monkeypatch):
    raw_ready(panel, monkeypatch)
    panel.set_dataset_source("NN/new/dataset.json")
    assert panel.raw_dataset.text() == "owner/source/dataset.json"
    panel.refresh()
    panel.client.resultReceived.emit(panel.client.calls[-1][3], snapshot(active=True))
    assert panel.raw_dataset.text() == "owner/source/dataset.json"
    assert panel.raw_confirm.isChecked()
    panel.raw_source_button.click()
    assert panel.raw_dataset.text() == "NN/new/dataset.json"
    assert not panel.raw_confirm.isChecked()


def test_raw_unknown_submission_queries_original_id_without_resending(panel, monkeypatch):
    raw_ready(panel, monkeypatch)
    panel.predict_raw()
    posted = deepcopy(panel._pending)
    panel.client.failed.emit(panel.client.calls[-1][3], "lost acceptance")
    panel.refresh()
    assert panel.client.calls[-1][0:2] == ("GET", "/api/shadow-models/" + posted["id"])
    assert (
        sum(call[0] == "POST" and call[2]["kind"] == "predict_raw" for call in panel.client.calls)
        == 1
    )
    assert panel._pending["parameters"] == posted["parameters"]


def test_raw_result_distinguishes_reference_and_actual_snapshot_lineage(panel, monkeypatch):
    raw_ready(panel, monkeypatch)
    panel.predict_raw()
    finish(panel, result=raw_prediction(), snapshot_value=snapshot(active=True, revision=3))
    assert panel.output.item(0, 1).text() == "0.25"
    assert "Offline retrospective shadow" in panel.output_label.text()
    assert "Historical reference close=660000" in panel.raw_lineage.text()
    assert "Actual snapshot availability=1000000" in panel.raw_lineage.text()
    assert '"synthetic": true' in panel.raw_lineage.text()
    assert "historical received times unavailable" in panel.raw_lineage.text()


def test_raw_late_completion_preserves_new_draft_and_old_output(panel, monkeypatch):
    raw_ready(panel, monkeypatch)
    panel.predict_raw()
    identity = panel._pending["id"]
    panel.raw_dataset.setText("edited/while/running")
    finish(panel, result=raw_prediction(), snapshot_value=snapshot(active=True, revision=3))
    assert panel.output.rowCount() == 0
    assert panel.raw_dataset.text() == "edited/while/running"
    assert not panel.raw_confirm.isChecked()
    panel.operations.selectRow(
        next(
            index
            for index in range(panel.operations.rowCount())
            if panel.operations.item(index, 0).text() == identity
        )
    )
    panel.show_stored_prediction()
    assert panel.output.item(0, 1).text() == "0.25"  # Explicit historical lookup is separate.
    assert "Stored result" in panel.output_label.text() or "저장 결과" in panel.output_label.text()


@pytest.mark.parametrize(
    "fault", ["reference", "availability", "MC", "interpolation", "candidate", "hash", "scope"]
)
def test_invalid_raw_output_never_replaces_valid_display(panel, fault):
    value = raw_prediction()
    if fault == "reference":
        value["preparation"]["reference_time_ms"] += 60000
    elif fault == "availability":
        value["preparation"]["as_of_ms"] = 600000
    elif fault == "MC":
        value["preparation"]["lineage"]["dataset_kind"] = "monte_carlo"
    elif fault == "interpolation":
        value["preparation"]["lineage"]["quality"]["interpolated_rows"] = 1
    elif fault == "candidate":
        value["preparation"]["request"]["candidate_id"] = "0" * 64
    elif fault == "hash":
        value["input_digest"] = "0" * 64
    else:
        value["scope"] = "paper_shadow_only"
    with pytest.raises(ValueError):
        panel._show_prediction(value, retrospective=True)
    assert panel.output.rowCount() == 0


def test_new_epoch_blocks_raw_reply_and_preserves_source_path(panel, monkeypatch):
    raw_ready(panel, monkeypatch)
    panel.predict_raw()
    tag, result = panel.client.calls[-1][3], operation(panel, result=raw_prediction())
    panel.update_state(state("epoch-b"))
    panel.client.resultReceived.emit(tag, result)
    assert panel.raw_dataset.text() == "owner/source/dataset.json"
    assert panel.output.rowCount() == 0 and not panel.raw_predict_button.isEnabled()


def test_research_page_suggests_dataset_without_overwriting_raw_draft(client, qt_app):
    from omega.gui.pages import ResearchPage

    page = ResearchPage(client, ["train"])
    try:
        page.shadow_controls.raw_dataset.setText("raw/draft")
        page.dataset.setText("training/dataset.json")
        assert page.shadow_controls._dataset_source == "training/dataset.json"
        assert page.shadow_controls.raw_dataset.text() == "raw/draft"
    finally:
        page.shadow_controls.timer.stop()
        page.close()
        page.deleteLater()
        qt_app.processEvents()


def test_raw_manifest_and_folder_selection_only_edit_path(panel, monkeypatch):
    monkeypatch.setattr(
        "omega.gui.shadow_controls.QFileDialog.getOpenFileName",
        lambda *_args: ("chosen/dataset.json", ""),
    )
    monkeypatch.setattr(
        "omega.gui.shadow_controls.QFileDialog.getExistingDirectory", lambda *_args: "chosen/folder"
    )
    before = len(panel.client.calls)
    panel.raw_file_button.click()
    assert panel.raw_dataset.text() == "chosen/dataset.json"
    panel.raw_confirm.setChecked(True)
    panel.raw_folder_button.click()
    assert panel.raw_dataset.text() == "chosen/folder" and not panel.raw_confirm.isChecked()
    assert len(panel.client.calls) == before


def test_raw_uses_next_confirmed_stream_sequence_and_rejects_a_gap(panel, monkeypatch):
    from PySide6.QtCore import QDateTime, QTimeZone

    raw_ready(panel, monkeypatch)
    stream = panel._document()["streams"]["gui-preview|" + INSTRUMENT]
    stream.update(open_ms=600000, sequence=3)
    panel.predict_raw()  # Same old input minute cannot advance this stream.
    assert panel._pending is None
    panel.open_time.setDateTime(QDateTime.fromMSecsSinceEpoch(660000, QTimeZone(b"UTC")))
    panel.predict_raw()
    assert panel._pending["parameters"]["sequence"] == 4
    assert panel._pending["parameters"]["open_time_ms"] == 660000


@pytest.mark.parametrize("width", [800, 1360])
def test_raw_controls_and_result_lineage_remain_accessible(
    panel, qt_app, monkeypatch, tmp_path, width
):
    from PySide6.QtWidgets import QScrollArea

    raw_ready(panel, monkeypatch)
    host = QScrollArea()
    host.setWidgetResizable(True)
    host.setWidget(panel)
    host.resize(width, 900)
    host.show()
    try:
        panel.tabs.setCurrentIndex(4)
        qt_app.processEvents()
        assert panel.raw_dataset.isVisible() and panel.raw_confirm.isVisible()
        assert host.horizontalScrollBar().maximum() == 0
        assert host.grab().save(str(tmp_path / f"raw-shadow-controls-{width}.png"))
        panel._show_prediction(raw_prediction(), retrospective=True)
        panel.tabs.setCurrentIndex(3)
        qt_app.processEvents()
        assert host.horizontalScrollBar().maximum() == 0
        assert host.grab().save(str(tmp_path / f"raw-shadow-result-{width}.png"))
    finally:
        host.takeWidget()
        host.close()
        host.deleteLater()
