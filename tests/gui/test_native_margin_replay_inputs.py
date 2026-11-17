"""Structured native replay drafts use inspected data without GUI file/DB access."""

import importlib.util
import os
from copy import deepcopy

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)

BTC = "binance:usdm:BTCUSDT"


@pytest.fixture(scope="module")
def native_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["native-margin-input-tests"])


@pytest.fixture
def native_widgets(native_app):
    from PySide6.QtCore import QObject, Signal
    from PySide6.QtWidgets import QLineEdit

    from omega.gui.margin_replay_controls import MarginReplayControls
    from omega.gui.research_inputs import ResearchInputs

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True

        def __init__(self):
            super().__init__()
            self.state = {"core": {"epoch": "epoch-a"}}
            self.calls = []

        def submit_job(self, kind, parameters):
            self.calls.append((kind, parameters))
            return f"inspect:{len(self.calls)}"

        def request(self, *args, **kwargs):
            pytest.fail("complete inspection response does not need another request")

    client, source = Client(), QLineEdit("selected/original/dataset.json")
    inputs, replay = ResearchInputs(client, source), MarginReplayControls()
    replay.add_strategy()
    replay.manual.setChecked(True)
    yield inputs, replay, client, source
    for widget in (inputs, replay, source):
        widget.close()
        widget.deleteLater()
    native_app.processEvents()


def summary(*, auxiliary=True, transform=None):
    return {
        "schema_id": "omega.market-observations.1m.v1" if auxiliary else "omega.ohlcv.1m.v1",
        "rows": 3,
        "instruments": [BTC],
        "time_range": [0, 180000],
        "instrument_ranges": {BTC: [0, 180000]},
        "auxiliary": {
            "schema_id": "omega.market-observations.1m.v1",
            "channels": {"trade_flow": {"count": 3}},
            "scope": "retrospective_training_only",
            "manifest_sha256": "a" * 64,
            "observations_sha256": "b" * 64,
            "transform": transform,
        }
        if auxiliary
        else None,
    }


def inspected(widgets, data=None):
    inputs, _, client, _ = widgets
    inputs.inspect()
    client.resultReceived.emit(
        f"inspect:{len(client.calls)}",
        {
            "id": "c" * 32,
            "kind": "inspect_dataset",
            "state": "complete",
            "result": data or summary(),
        },
    )
    assert inputs.metadata is not None
    return inputs


def test_plain_draft_preserves_original_parameters_and_does_not_request_auxiliary(native_widgets):
    inputs, replay, client, _ = native_widgets
    result = replay.parameters()
    assert "auxiliary_replay" not in result
    assert result["spec"] == {"fee_rate": "0.001", "backend": "cpu"}
    assert replay.auxiliary_can_submit()
    assert not replay.decision_latency_ms.isEnabled()
    inspected(native_widgets, summary(auxiliary=False))
    inputs.validate_usage("simulate_margin")
    assert client.calls == [("inspect_dataset", {"dataset": "selected/original/dataset.json"})]


@pytest.mark.parametrize("latency", [0, 60000])
def test_native_control_and_current_inspection_emit_exact_optional_contract(
    native_widgets, latency
):
    inputs, replay, client, _ = native_widgets
    inputs = inspected(native_widgets)
    replay.set_auxiliary_supported(True)
    replay.auxiliary_mode.setCurrentIndex(1)
    replay.decision_latency_ms.setValue(latency)
    assert replay.decision_latency_ms.minimum() == 0
    assert replay.decision_latency_ms.maximum() == 60000
    assert replay.decision_latency_ms.isEnabled()
    expected = {"mode": "native", "decision_latency_ms": latency}
    assert replay.parameters()["auxiliary_replay"] == expected
    assert inputs.native_replay_parameters(latency) == expected
    assert replay.auxiliary_can_submit()
    assert len(client.calls) == 1 and client.calls[0][0] == "inspect_dataset"
    assert "generated/MC/proxy" in inputs.notice.text()
    assert "원본" in replay.auxiliary_notice.text()


def test_native_selection_needs_capability_and_never_silently_drops_loaded_mode(native_widgets):
    _, replay, _, _ = native_widgets
    replay.auxiliary_mode.setCurrentIndex(1)
    assert not replay.auxiliary_can_submit()
    with pytest.raises(ValueError, match="Native replay unavailable"):
        replay.parameters()
    replay.set_auxiliary_supported(True)
    replay.decision_latency_ms.setValue(321)
    saved = replay.parameters()
    original = deepcopy(saved)
    replay.load_parameters(saved)
    assert not replay.manual.isChecked() and not replay.noncausal.isChecked()
    assert replay.auxiliary_parameters() == {"mode": "native", "decision_latency_ms": 321}
    replay.set_auxiliary_supported(False)
    assert replay.auxiliary_mode.currentData() == "native"
    assert not replay.auxiliary_can_submit()
    with pytest.raises(ValueError, match="Native replay unavailable"):
        replay.auxiliary_parameters()
    assert saved == original


@pytest.mark.parametrize("latency", [True, 0.0, -1, 60001, "0"])
def test_recorded_latency_and_public_input_validation_reject_without_clamping(
    native_widgets, latency
):
    inputs, replay, _, _ = native_widgets
    inputs = inspected(native_widgets)
    original = replay.parameters()
    invalid = original | {"auxiliary_replay": {"mode": "native", "decision_latency_ms": latency}}
    with pytest.raises(ValueError):
        replay.load_parameters(invalid)
    assert replay.parameters() == original
    with pytest.raises(ValueError, match="Integer decision latency"):
        inputs.native_replay_parameters(latency)


@pytest.mark.parametrize(
    "document",
    [
        {"mode": "proxy", "decision_latency_ms": 0},
        {"mode": "native"},
        {"mode": "native", "decision_latency_ms": 0, "assume_available": True},
    ],
)
def test_invalid_native_draft_cannot_replace_current_form(native_widgets, document):
    _, replay, _, _ = native_widgets
    original = replay.parameters()
    with pytest.raises(ValueError):
        replay.load_parameters(original | {"auxiliary_replay": document})
    assert replay.parameters() == original


@pytest.mark.parametrize("damage", ["generated", "missing_transform", "hash", "plain"])
def test_generated_or_unproven_bundle_cannot_be_used_as_native(native_widgets, damage):
    data = summary()
    if damage == "generated":
        data["auxiliary"]["transform"] = {"kind": "monte_carlo"}
    elif damage == "missing_transform":
        data["auxiliary"].pop("transform")
    elif damage == "hash":
        data["auxiliary"]["observations_sha256"] = "not-a-sha"
    else:
        data = summary(auxiliary=False)
    inputs = inspected(native_widgets, data)
    with pytest.raises(ValueError, match="Validate original observations"):
        inputs.native_replay_parameters()
    inputs.validate_usage("train")  # Existing retrospective training remains available.


@pytest.mark.parametrize("invalidate", ["path", "epoch", "pending", "failed", "disconnected"])
def test_stale_or_unfinished_inspection_cannot_authorize_native_input(native_widgets, invalidate):
    inputs, _, client, source = native_widgets
    inputs = inspected(native_widgets)
    if invalidate == "path":
        source.setText("different/dataset.json")
    elif invalidate == "epoch":
        client.state["core"]["epoch"] = "epoch-b"
        inputs.set_context(True)
    elif invalidate == "disconnected":
        inputs.set_context(False)
    else:
        inputs.inspect()
        if invalidate == "failed":
            client.failed.emit(f"inspect:{len(client.calls)}", "synthetic hash mismatch")
    with pytest.raises(ValueError):
        inputs.native_replay_parameters()


def test_native_rejects_noncausal_consent_and_plain_mode_rejects_auxiliary_bundle(native_widgets):
    inputs, replay, _, _ = native_widgets
    inputs = inspected(native_widgets)
    with pytest.raises(ValueError, match="회고 학습"):
        inputs.validate_usage("simulate_margin")
    replay.set_auxiliary_supported(True)
    replay.auxiliary_mode.setCurrentIndex(1)
    replay.noncausal.setChecked(True)
    with pytest.raises(ValueError, match="비인과/생성"):
        replay.parameters()
