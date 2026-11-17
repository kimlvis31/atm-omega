"""Structured V2 drafts require current inspection and exact owner capabilities."""

import importlib.util
import os
from copy import deepcopy

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)
KEY = "binance:usdm:BTCUSDT"
V2 = "omega.market-observations.1m.v2"
CAPS = {
    "auxiliary_export": True,
    "margin_simulation": True,
    "margin_native_observation_replay": True,
    "margin_native_observation_replay_scope": "native_trade_flow_available_cutoff_v1",
    "native_observation_bundle_v2": True,
    "native_observation_bundle_v2_scope": "recorded_native_inputs_v2",
    "margin_native_observation_replay_v2": True,
    "margin_native_observation_replay_v2_scope": "native_auxiliary_available_cutoff_v2",
}


@pytest.fixture(scope="module")
def v2_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["native-bundle-v2-controls"])


@pytest.fixture
def v2_page(v2_app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.pages import ResearchPage

    class Client(QObject):
        modulesReceived = Signal(dict)
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True

        def __init__(self):
            super().__init__()
            self.state = {"core": {"epoch": "epoch-a", "capabilities": deepcopy(CAPS)}}
            self.calls = []

        def submit_job(self, kind, parameters):
            self.calls.append((kind, deepcopy(parameters)))
            return f"submitted:{len(self.calls)}"

        def request(self, *args, **kwargs):
            pytest.fail("complete inspection needs no additional request")

    page = ResearchPage(Client(), ["export", "monte_carlo", "simulate", "train"])
    page.dataset.setText("selected/dataset.json")
    page.margin_replay.add_strategy()
    page.margin_replay.manual.setChecked(True)
    page.set_attached(True)
    yield page
    page.close()
    page.deleteLater()
    v2_app.processEvents()


def summary(schema=V2):
    return {
        "schema_id": schema,
        "rows": 3,
        "instruments": [KEY],
        "time_range": [600000, 780000],
        "instrument_ranges": {KEY: [600000, 780000]},
        "auxiliary": None
        if schema == "omega.ohlcv.1m.v1"
        else {
            "schema_id": schema,
            "scope": "recorded_native_inputs_v2" if schema == V2 else "retrospective_training_only",
            "manifest_sha256": "a" * 64,
            "observations_sha256": "b" * 64,
            "observations_count": 4,
            "transform": None,
            "channels": {"trade_flow": {"count": 3}, "open_interest": {"count": 1}},
            "selection": {"version": 1, "metric_lookback_ms": 330000, "ranges": []},
        },
    }


def inspect(page, document):
    controls = page.dataset_controls
    controls.inspect()
    tag = f"submitted:{len(page.client.calls)}"
    page.client.resultReceived.emit(
        tag,
        {
            "id": "c" * 32,
            "kind": "inspect_dataset",
            "state": "complete",
            "result": document,
        },
    )
    return controls


def select(controls, channel="open_interest"):
    controls.channel.setCurrentText(channel)
    suffix = "binance_depth_ws_v1" if channel == "depth" else "open_interest_history"
    controls.observation_source.setText(KEY + ":" + suffix)
    controls.add_button.click()


@pytest.mark.parametrize("lookback", [0, 330000, 86400000])
def test_explicit_v2_format_and_visible_metric_seed_preserve_legacy_payload(v2_page, lookback):
    controls = inspect(v2_page, summary("omega.ohlcv.1m.v1"))
    select(controls)
    legacy = {
        "candle_dataset": "selected/dataset.json",
        "selections": [
            {
                "instrument": KEY,
                "channel": "open_interest",
                "source": KEY + ":open_interest_history",
            }
        ],
    }
    assert controls.export_parameters() == legacy
    controls.export_format.setCurrentIndex(1)
    controls.metric_lookback_ms.setValue(lookback)
    assert not controls.metric_lookback_ms.isHidden()
    assert controls.export_parameters() == legacy | {
        "format_version": 2,
        "metric_lookback_ms": lookback,
    }
    assert len(v2_page.client.calls) == 1  # no automatic export/submission


def test_depth_requires_explicit_v2_and_capability_loss_never_falls_back(v2_page):
    controls = inspect(v2_page, summary("omega.ohlcv.1m.v1"))
    controls.export_format.setCurrentIndex(1)
    select(controls, "depth")
    assert controls.export_parameters()["format_version"] == 2
    assert "metric_lookback_ms" not in controls.export_parameters()
    controls.export_format.setCurrentIndex(0)
    with pytest.raises(ValueError, match="Depth"):
        controls.export_parameters()
    controls.export_format.setCurrentIndex(1)
    v2_page.client.state["core"]["capabilities"]["native_observation_bundle_v2_scope"] = "wrong"
    controls.set_context(True)
    assert controls.export_format.currentData() == 2
    with pytest.raises(ValueError, match="V2 export capability"):
        controls.export_parameters()


def test_v2_inspection_native_policy_and_capability_are_forwarded_without_auto_submit(v2_page):
    controls = inspect(v2_page, summary())
    assert controls.metadata == summary() and len(v2_page.client.calls) == 1
    assert "희소" in controls.notice.text()
    replay = v2_page.margin_replay
    v2_page.simulation_mode.setCurrentIndex(1)
    replay.auxiliary_mode.setCurrentIndex(1)
    replay.decision_latency_ms.setValue(234)
    assert "V2" in replay.auxiliary_notice.text() and replay.auxiliary_can_submit()
    assert controls.native_replay_parameters(234) == {"mode": "native", "decision_latency_ms": 234}
    v2_page.submit()
    assert v2_page.client.calls[-1][0] == "simulate_margin"
    assert v2_page.client.calls[-1][1]["auxiliary_replay"] == {
        "mode": "native",
        "decision_latency_ms": 234,
    }
    caps = v2_page.client.state["core"]["capabilities"]
    caps["margin_native_observation_replay_v2"] = False
    v2_page._simulation_mode_changed()
    assert replay.auxiliary_mode.currentData() == "native" and not replay.auxiliary_can_submit()
    assert not v2_page.run_button.isEnabled()
    with pytest.raises(ValueError, match="V2 native replay capability"):
        controls.native_replay_parameters()
    assert caps["margin_native_observation_replay"] is True  # V1 cannot authorize V2


@pytest.mark.parametrize("kind", ["train", "monte_carlo", "simulate", "simulate_margin"])
def test_v2_never_becomes_v1_training_mc_or_plain_replay_input(v2_page, kind):
    controls = inspect(v2_page, summary())
    with pytest.raises(ValueError, match="NN training and MC are unsupported"):
        controls.validate_usage(kind)
    assert len(v2_page.client.calls) == 1


def test_v2_inspection_is_bound_to_path_epoch_and_original_identity(v2_page):
    controls = inspect(v2_page, summary())
    v2_page.dataset.setText("different/dataset.json")
    with pytest.raises(ValueError, match="Validate original"):
        controls.native_replay_parameters()
    controls = inspect(v2_page, summary())
    v2_page.client.state["core"]["epoch"] = "epoch-b"
    with pytest.raises(ValueError, match="Validate original"):
        controls.native_replay_parameters()
    controls.set_context(True)
    assert controls.metadata is None
    forged = summary()
    forged["auxiliary"]["scope"] = "retrospective_training_only"
    controls = inspect(v2_page, forged)
    assert controls.metadata is None and controls._failed
    assert len(v2_page.client.calls) == 3
