"""Actual Qt committed-inference controls; backend responses are explicit fixtures."""

import importlib.util
import os
from copy import deepcopy

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)
JOB, OP, NOW = "a" * 32, "b" * 32, 1_700_000_040_000


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["omega-causal-controls-tests"])


@pytest.fixture
def client(qt_app):
    from PySide6.QtCore import QObject, Signal

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        stateReceived = Signal(dict)
        modulesReceived = Signal(dict)
        attached = True

        def __init__(self):
            super().__init__()
            self.state = {"core": {"epoch": "core-1", "capabilities": {"causal_inference": True}}}
            self.calls = []

        def request(self, method, path, payload=None, *, tag):
            self.calls.append((method, path, deepcopy(payload), tag))
            return tag

        def submit_job(self, kind, parameters):
            return self.request(
                "POST", "/api/jobs", {"kind": kind, "parameters": parameters}, tag="job"
            )

    return Client()


@pytest.fixture
def panel(client):
    from omega.gui.causal_controls import CausalControls

    widget = CausalControls(client)
    yield widget
    widget.timer.stop()
    widget.close()
    widget.deleteLater()


def snapshot(*, revision=0, state="disabled", deployed=False, stream=False, feature=False):
    value = {
        "schema_version": 1,
        "scope": "committed_inference_service",
        "core_epoch": "core-1",
        "live_orders_enabled": False,
        "strategy_delivery_enabled": False,
        "revision": revision,
        "state": state,
        "ready": True,
        "storage_available": True,
        "actor_alive": True,
        "server_time_ms": NOW,
        "active_deployment_id": "deployment-1" if deployed else None,
        "deployments": [],
        "streams": [],
        "operations": [],
        "error": None,
        "limits": {"active_deployments": 1, "streams": 128, "worker_queue": 8, "control_queue": 32},
    }
    if deployed:
        value["deployments"].append(
            {
                "id": "deployment-1",
                "state": "ready",
                "training_job_id": JOB,
                "module_id": "demo",
                "model_revision": "r1",
                "module_sha256": "1" * 64,
                "checkpoint_sha256": "2" * 64,
                "available_at_ms": NOW - 60000,
                "training_cutoff_ms": NOW - 120000,
                "manual_required": False,
                "contract": {},
                "bindings": [{"name": "NN.score", "path": ["values", 0]}],
                "error": None,
            }
        )
    if stream:
        row = {
            "deployment_id": "deployment-1",
            "stream": "primary",
            "stream_epoch": "stream-1",
            "instrument": "binance:usdm:BTCUSDT",
            "state": "ready",
            "reason": None,
            "last_open_ms": NOW - 60000,
            "sequence": 1,
            "pending_operation_id": None,
            "feature": None,
            "manual_required": False,
        }
        if feature:
            row["feature"] = {
                "request_id": OP,
                "input_sha256": "3" * 64,
                "frame_sha256": "4" * 64,
                "open_time_ms": NOW - 60000,
                "decision_at_ms": NOW,
                "accepted_at_ms": NOW,
                "valid_until_ms": NOW + 1000,
                "values": {"NN.score": "0.1234567890123456789"},
            }
        value["streams"].append(row)
    return value


def accept_snapshot(panel, value=None):
    panel.setChecked(True)
    panel.refresh()
    tags = [tag for tag, row in panel._requests.items() if row["context"] == "snapshot"]
    assert len(tags) == 1
    panel.client.resultReceived.emit(tags[0], value or snapshot())


def contract(panel):
    panel.schema.setText("omega.ohlcv.1m.v1")
    panel.features.setText("normalized_volume")
    panel.add_binding_button.click()
    panel.shape_confirm.setChecked(True)
    panel.trusted.setChecked(True)


def ready_stream(panel):
    accept_snapshot(panel, snapshot(revision=2, state="ready", deployed=True, stream=True))
    panel.deployments.setCurrentIndex(panel.deployments.findData("deployment-1"))
    panel.stream_name.setText("primary")
    panel.symbol.setText("BTCUSDT")


def receipt(panel, state="complete", *, revision=1, kind=None, durable=True):
    return {
        "id": panel._pending["id"],
        "kind": kind or panel._pending["kind"],
        "core_epoch": panel._pending["epoch"],
        "request_digest": "5" * 64,
        "submitted_revision": 0,
        "revision": revision,
        "state": state,
        "durable": durable,
        "created_at_ms": NOW,
        "started_at_ms": NOW,
        "finished_at_ms": NOW,
        "result": {"deployment_id": "deployment-1"},
        "error": None,
        "scope": "committed_inference_service",
        "live_orders_enabled": False,
        "strategy_delivery_enabled": False,
    }


def test_initial_panel_is_collapsed_inert_and_has_no_mutation_or_path_inputs(panel):
    assert not panel.isChecked() and not panel.client.calls
    assert not panel.activate_button.isEnabled() and not panel.predict_button.isEnabled()
    assert "CPU" in panel.title() and "PostgreSQL" in panel.hint.text()
    assert not hasattr(panel, "batch") and not hasattr(panel, "checkpoint")
    panel.setChecked(True)
    assert [(method, path) for method, path, _, _ in panel.client.calls] == [
        ("GET", "/api/causal-inference")
    ]


def test_typed_virtual_margin_scope_accepts_snapshot_and_durable_operation_receipt(panel):
    value = snapshot()
    value.update(strategy_delivery_enabled=True, strategy_delivery_scope="virtual_margin_v2_opt_in")
    accept_snapshot(panel, value)
    assert panel._fresh() and panel.snapshot["ready"]
    panel.set_training_selection(JOB, core_epoch="core-1")
    contract(panel)
    panel.activate()
    result = receipt(panel)
    result.update(
        strategy_delivery_enabled=True, strategy_delivery_scope="virtual_margin_v2_opt_in"
    )
    panel.client.resultReceived.emit(panel.client.calls[-1][3], result)
    assert panel._pending is None
    assert len([call for call in panel.client.calls if call[0] == "POST"]) == 1


@pytest.mark.parametrize("response_kind", ["snapshot", "operation"])
@pytest.mark.parametrize(
    "change", ["missing_scope", "wrong_scope", "integer", "string", "live", "wrong_service"]
)
def test_delivery_authority_rejects_non_native_or_out_of_scope_responses(
    panel, response_kind, change
):
    if response_kind == "operation":
        accept_snapshot(panel)
        panel.set_training_selection(JOB, core_epoch="core-1")
        contract(panel)
        panel.activate()
        value = receipt(panel)
    else:
        value = snapshot()
    value.update(strategy_delivery_enabled=True, strategy_delivery_scope="virtual_margin_v2_opt_in")
    if change == "missing_scope":
        del value["strategy_delivery_scope"]
    elif change == "wrong_scope":
        value["strategy_delivery_scope"] = "live_orders"
    elif change == "integer":
        value["strategy_delivery_enabled"] = 1
    elif change == "string":
        value["strategy_delivery_enabled"] = "true"
    elif change == "live":
        value["live_orders_enabled"] = True
    else:
        value["scope"] = "retrospective_shadow"
    if response_kind == "snapshot":
        accept_snapshot(panel, value)
        assert panel.snapshot is None
    else:
        panel.client.resultReceived.emit(panel.client.calls[-1][3], value)
        assert panel._pending is not None and panel._pending["unknown"]
    assert not panel._fresh() and not panel.predict_button.isEnabled()


def test_activation_sends_only_selected_training_explicit_contract_trust_and_mapping(panel):
    accept_snapshot(panel)
    panel.set_training_selection(JOB, core_epoch="core-1")
    contract(panel)
    assert panel.activate_button.isEnabled()
    panel.activate_button.click()
    method, path, payload, _ = panel.client.calls[-1]
    assert (method, path) == ("POST", "/api/causal-inference")
    assert set(payload) == {"id", "kind", "parameters"}
    assert payload["kind"] == "activate"
    parameters = payload["parameters"]
    assert set(parameters) == {
        "core_epoch",
        "revision",
        "training_job_id",
        "deployment_id",
        "trusted_module",
        "contract",
        "bindings",
    }
    assert parameters["core_epoch"] == "core-1" and parameters["revision"] == 0
    assert parameters["training_job_id"] == JOB and parameters["deployment_id"] is None
    assert parameters["trusted_module"] is True
    assert parameters["contract"]["features"] == ["normalized_volume"]
    assert parameters["bindings"] == [{"name": "NN.score", "path": ["values", 0]}]


def test_contract_and_binding_changes_require_new_shape_and_trust_confirmation(panel):
    accept_snapshot(panel)
    panel.set_training_selection(JOB, core_epoch="core-1")
    contract(panel)
    panel.output_width.setValue(2)
    assert not panel.shape_confirm.isChecked() and not panel.trusted.isChecked()
    panel.binding_name.setText("NN.second")
    panel.binding_index.setValue(1)
    panel.add_binding_button.click()
    assert panel._binding_documents()[-1] == {"name": "NN.second", "path": ["values", 0, 1]}
    panel.binding_name.setText("../bad")
    panel.add_binding_button.click()
    assert len(panel._bindings) == 2
    panel.recurrent.setChecked(True)
    panel.state_shape.setText("1, 1, 8")
    panel.state_version.setText("rnn-v1")
    assert panel.contract()["state_shape"] == [1, 1, 8]
    panel.features.setText("a, b")
    with pytest.raises(ValueError, match="shape mismatch"):
        panel.contract()


def test_activation_has_no_automatic_predict_reset_or_second_post(panel):
    accept_snapshot(panel)
    panel.set_training_selection(JOB, core_epoch="core-1")
    contract(panel)
    panel.activate()
    response = receipt(panel)
    tag = panel.client.calls[-1][3]
    panel.client.resultReceived.emit(tag, response)
    accept_snapshot(panel, snapshot(revision=1, state="ready", deployed=True))
    assert panel._pending is None
    assert panel.deployments.currentData() == "deployment-1"
    assert not panel.predict_button.isEnabled()
    assert [
        payload["kind"] for method, _, payload, _ in panel.client.calls if method == "POST"
    ] == ["activate"]


def test_pending_ack_loss_blocks_duplicate_post_and_queries_original_operation(panel):
    accept_snapshot(panel)
    panel.set_training_selection(JOB, core_epoch="core-1")
    contract(panel)
    panel.activate()
    identity, tag = panel._pending["id"], panel.client.calls[-1][3]
    panel.client.failed.emit(tag, "synthetic ACK lost")
    assert panel._pending["unknown"]
    panel.activate()
    assert len([row for row in panel.client.calls if row[0] == "POST"]) == 1
    panel.reconcile_button.click()
    assert panel.client.calls[-1][0:2] == ("GET", "/api/causal-inference/" + identity)
    reply = receipt(panel)
    panel.client.resultReceived.emit(panel.client.calls[-1][3], reply)
    assert panel._pending is None
    assert not panel.activate_button.isEnabled()


def test_snapshot_ttl_blocks_mutations_and_expired_feature_is_masked_without_server_get(
    panel, monkeypatch
):
    from omega.gui import causal_controls

    accept_snapshot(
        panel, snapshot(revision=2, state="ready", deployed=True, stream=True, feature=True)
    )
    panel.deployments.setCurrentIndex(panel.deployments.findData("deployment-1"))
    assert "0.1234567890123456789" in panel.streams.item(0, 2).text()
    assert "0.1234567890123456789" in panel.feature_summary.text()
    current = panel._received_at
    monkeypatch.setattr(causal_controls.time, "monotonic", lambda: current + 2)
    calls = len(panel.client.calls)
    panel._tick()
    assert len(panel.client.calls) == calls
    assert "0.1234567890123456789" not in panel.streams.item(0, 2).text()
    assert "0.1234567890123456789" not in panel.feature_summary.text()
    monkeypatch.setattr(causal_controls.time, "monotonic", lambda: current + 11)
    panel._controls()
    assert not panel.reset_button.isEnabled() and not panel.predict_button.isEnabled()
    assert "재조회 필요" in panel.status.text()


@pytest.mark.parametrize("policy", ["reset", "preserve"])
def test_reset_requires_explicit_policy_and_keeps_actual_state_out_of_payload(panel, policy):
    ready_stream(panel)
    assert not panel.reset_button.isEnabled()
    panel.reset_stream()
    assert not any(row[0] == "POST" for row in panel.client.calls)
    panel.state_policy.setCurrentIndex(panel.state_policy.findData(policy))
    panel.reset_button.click()
    parameters = panel.client.calls[-1][2]["parameters"]
    assert parameters == {
        "core_epoch": "core-1",
        "revision": 2,
        "deployment_id": "deployment-1",
        "stream": "primary",
        "instrument": "binance:usdm:BTCUSDT",
        "state_policy": policy,
    }


def test_predict_uses_only_committed_minute_identity_and_bounded_deadline(panel):
    from PySide6.QtCore import QDateTime, QTimeZone

    ready_stream(panel)
    panel.open_time.setDateTime(QDateTime.fromMSecsSinceEpoch(NOW, QTimeZone(b"UTC")))
    panel.timeout.setValue(30000)
    assert panel.predict_button.isEnabled()
    panel.predict_button.click()
    payload = panel.client.calls[-1][2]
    assert payload["kind"] == "predict_committed"
    assert payload["parameters"] == {
        "core_epoch": "core-1",
        "revision": 2,
        "deployment_id": "deployment-1",
        "stream": "primary",
        "instrument": "binance:usdm:BTCUSDT",
        "open_time_ms": NOW,
        "timeout_ms": 30000,
    }
    assert not panel.predict_button.isEnabled()


def test_epoch_change_clears_selection_trust_snapshot_and_discards_old_response(panel):
    panel.refresh()
    tag = panel.client.calls[-1][3]
    panel.set_training_selection(JOB, core_epoch="core-1")
    panel.trusted.setChecked(True)
    panel.client.state["core"]["epoch"] = "core-2"
    panel.update_state(panel.client.state)
    panel.client.resultReceived.emit(tag, snapshot())
    assert panel.snapshot is None and panel.training_id is None
    assert not panel.trusted.isChecked() and not panel.activate_button.isEnabled()
    panel.set_training_selection(JOB, core_epoch="core-1")
    assert panel.training_id is None


def test_terminal_receipt_does_not_accept_pre_mutation_snapshot_revision(panel):
    accept_snapshot(panel)
    panel.set_training_selection(JOB, core_epoch="core-1")
    contract(panel)
    panel.activate()
    panel.client.resultReceived.emit(panel.client.calls[-1][3], receipt(panel, revision=3))
    accept_snapshot(panel, snapshot(revision=0))
    assert not panel._fresh()
    accept_snapshot(panel, snapshot(revision=3, state="ready", deployed=True))
    assert panel._fresh() and panel.snapshot["revision"] == 3


def test_manual_required_or_paused_streams_never_expose_or_predict_feature(panel):
    value = snapshot(revision=2, state="paused", deployed=True, stream=True, feature=True)
    value["deployments"][0]["state"] = "paused"
    value["streams"][0]["state"], value["streams"][0]["manual_required"] = "manual_required", True
    accept_snapshot(panel, value)
    panel.deployments.setCurrentIndex(panel.deployments.findData("deployment-1"))
    panel.streams.selectRow(0)
    panel.load_stream_button.click()
    assert panel.state_policy.currentData() is None
    assert not panel.predict_button.isEnabled()
    assert "NN.score=" not in panel.streams.item(0, 2).text()


def test_invalid_live_enabled_snapshot_is_rejected_and_disconnect_hides_features(panel):
    value = snapshot()
    value["live_orders_enabled"] = True
    accept_snapshot(panel, value)
    assert panel.snapshot is None and not panel._fresh()
    accept_snapshot(
        panel, snapshot(revision=2, state="ready", deployed=True, stream=True, feature=True)
    )
    panel.deployments.setCurrentIndex(panel.deployments.findData("deployment-1"))
    panel.set_attached(False)
    assert "NN.score=" not in panel.streams.item(0, 2).text()
    assert not panel.predict_button.isEnabled()


def test_unknown_predict_allows_separate_protective_pause_without_replaying_original(panel):
    ready_stream(panel)
    panel.predict()
    original = receipt(panel, state="unknown", revision=3)
    original_id = original["id"]
    panel.client.failed.emit(panel.client.calls[-1][3], "synthetic prediction ACK lost")
    current = snapshot(revision=3, state="ready", deployed=True, stream=True)
    current["operations"] = [original]
    current["streams"][0].update(state="manual_required", manual_required=True)
    accept_snapshot(panel, current)
    assert not panel.predict_button.isEnabled() and panel.pause_button.isEnabled()
    panel.pause_button.click()
    assert original_id in panel._unresolved
    assert panel._pending["kind"] == "pause"
    paused = receipt(panel, revision=4)
    panel.client.resultReceived.emit(panel.client.calls[-1][3], paused)
    current.update(revision=4, state="paused", operations=[original, paused])
    current["deployments"][0]["state"] = "paused"
    accept_snapshot(panel, current)
    assert panel._pending is None and original_id in panel._unresolved
    assert not panel.reset_button.isEnabled() and panel.retire_button.isEnabled()
    panel.reconcile_button.click()
    assert panel.client.calls[-1][0:2] == ("GET", "/api/causal-inference/" + original_id)
    panel.client.resultReceived.emit(panel.client.calls[-1][3], original)
    accept_snapshot(panel, current)
    assert not panel._unresolved and panel.state_policy.currentData() is None
    panel.state_policy.setCurrentIndex(panel.state_policy.findData("preserve"))
    assert not panel.reset_button.isEnabled()
    panel.state_policy.setCurrentIndex(panel.state_policy.findData("reset"))
    assert panel.reset_button.isEnabled()
    posts = [row[2] for row in panel.client.calls if row[0] == "POST"]
    assert [row["kind"] for row in posts] == ["predict_committed", "pause"]
    assert posts[0]["id"] != posts[1]["id"]


def test_only_registered_completed_train_selection_is_handed_from_research_page(client, qt_app):
    from omega.gui.pages import ResearchPage

    page = ResearchPage(client, ["train"])
    try:
        page.update_jobs([{"id": JOB, "kind": "train", "state": "running"}])
        page.jobs.selectRow(0)
        assert page.causal_controls.training_id is None
        page.update_jobs([{"id": JOB, "kind": "train", "state": "complete"}])
        page.jobs.selectRow(0)
        assert page.causal_controls.training_id == JOB
        assert not page.causal_controls.isChecked()
        page.update_jobs([{"id": JOB, "kind": "train", "state": "unknown"}])
        assert page.causal_controls.training_id is None
        assert not any(path == "/api/causal-inference" for _, path, _, _ in client.calls)
    finally:
        page.causal_controls.timer.stop()
        page.close()
        page.deleteLater()
        qt_app.processEvents()


def test_storage_unconfirmed_blocks_protective_posts_and_foreign_scope_receipt_is_not_promoted(
    panel,
):
    ready_stream(panel)
    panel.predict()
    foreign = receipt(panel)
    foreign["scope"] = "paper_shadow_only"
    panel.client.resultReceived.emit(panel.client.calls[-1][3], foreign)
    assert panel._pending is not None and panel._pending["unknown"]
    value = snapshot(revision=3, state="recording_unconfirmed", deployed=True, stream=True)
    value["storage_available"] = False
    accept_snapshot(panel, value)
    posts = len([row for row in panel.client.calls if row[0] == "POST"])
    assert not panel.pause_button.isEnabled() and not panel.retire_button.isEnabled()
    panel._deployment_operation("retire")
    assert len([row for row in panel.client.calls if row[0] == "POST"]) == posts


def test_old_operation_origin_is_reconciled_through_current_core_envelope(panel):
    accept_snapshot(panel)
    panel.set_training_selection(JOB, core_epoch="core-1")
    contract(panel)
    panel.activate()
    old_receipt = receipt(panel, revision=200)
    panel.client.state["core"]["epoch"] = "core-2"
    panel.update_state(panel.client.state)
    value = snapshot()
    value["core_epoch"] = "core-2"
    accept_snapshot(panel, value)
    panel.reconcile()
    envelope = old_receipt | {"core_epoch": "core-2", "operation_core_epoch": "core-1"}
    panel.client.resultReceived.emit(panel.client.calls[-1][3], envelope)
    accept_snapshot(panel, value)
    assert panel._pending is None and panel._minimum_revision == 0 and panel._fresh()


def test_snapshot_accepts_other_clients_bounded_operation_names(panel):
    value = snapshot()
    value["operations"] = [
        {
            "id": "owner_console-activate-1",
            "kind": "activate",
            "core_epoch": "core-1",
            "state": "complete",
            "submitted_revision": 0,
            "revision": 0,
            "durable": True,
            "request_digest": "8" * 64,
        }
    ]
    accept_snapshot(panel, value)
    assert panel._fresh()


def test_normal_no_signal_is_complete_ready_and_does_not_display_a_numeric_feature(panel):
    ready_stream(panel)
    panel.predict()
    result = receipt(panel, revision=3)
    result["result"] = {
        "deployment_id": "deployment-1",
        "stream": "primary",
        "request_id": OP,
        "no_signal": True,
        "feature": None,
    }
    panel.client.resultReceived.emit(panel.client.calls[-1][3], result)
    value = snapshot(revision=3, state="ready", deployed=True, stream=True)
    value["streams"][0].update(reason="no_signal", feature=None, sequence=2)
    accept_snapshot(panel, value)
    assert panel._pending is None and panel.predict_button.isEnabled()
    assert "정상 보류" in panel.status.text()
    assert "정상 보류(no_signal)" in panel.streams.item(0, 2).text()


def test_delayed_http_reply_consumes_feature_and_snapshot_lifetimes(panel, monkeypatch):
    from omega.gui import causal_controls

    current = [1000.0]
    monkeypatch.setattr(causal_controls.time, "monotonic", lambda: current[0])
    panel.setChecked(True)
    tag = panel.client.calls[-1][3]
    current[0] += 2
    panel.client.resultReceived.emit(
        tag, snapshot(revision=2, state="ready", deployed=True, stream=True, feature=True)
    )
    panel.deployments.setCurrentIndex(panel.deployments.findData("deployment-1"))
    assert panel._fresh()
    assert "NN.score=" not in panel.streams.item(0, 2).text()
    panel.refresh()
    tag = panel.client.calls[-1][3]
    current[0] += 11
    panel.client.resultReceived.emit(tag, snapshot(revision=3))
    assert not panel._fresh() and not panel.activate_button.isEnabled()


def test_pending_poll_is_one_second_cached_get_and_never_replays_prediction(panel, monkeypatch):
    from omega.gui import causal_controls

    ready_stream(panel)
    panel.show()
    panel.predict()
    queued = receipt(panel, state="running", revision=3, durable=True)
    panel.client.resultReceived.emit(panel.client.calls[-1][3], queued)
    value = snapshot(revision=3, state="ready", deployed=True, stream=True)
    value["operations"] = [queued]
    accept_snapshot(panel, value)
    current = panel._received_at
    monkeypatch.setattr(causal_controls.time, "monotonic", lambda: current + 1.1)
    calls = len(panel.client.calls)
    panel._tick()
    assert len(panel.client.calls) == calls + 1
    assert panel.client.calls[-1][0:2] == ("GET", "/api/causal-inference")
    assert [row[2]["kind"] for row in panel.client.calls if row[0] == "POST"] == [
        "predict_committed"
    ]


def test_feature_accepted_after_snapshot_clock_is_not_displayed_as_current(panel):
    value = snapshot(revision=2, state="ready", deployed=True, stream=True, feature=True)
    value["streams"][0]["feature"]["accepted_at_ms"] = NOW + 100
    accept_snapshot(panel, value)
    panel.deployments.setCurrentIndex(panel.deployments.findData("deployment-1"))
    assert "NN.score=" not in panel.feature_summary.text()
