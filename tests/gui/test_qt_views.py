"""Optional offscreen smoke tests: no core service, network, broker or DB is started."""

import importlib.util
import json
import os

import pytest

from omega.gui.models import parse_candles

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None,
    reason="optional PySide6 GUI dependency is not installed",
)


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication(["omega-gui-tests"])
    yield app


@pytest.fixture
def window(qt_app, tmp_path, monkeypatch):
    from PySide6.QtWidgets import QSystemTrayIcon

    from omega.gui.window import OmegaWindow

    monkeypatch.setattr(QSystemTrayIcon, "isSystemTrayAvailable", lambda: False)
    view = OmegaWindow(tmp_path)
    calls = []
    monkeypatch.setattr(
        view.client, "request", lambda *args, **kwargs: calls.append((args, kwargs))
    )
    view._request_calls = calls
    yield view
    view.close()
    view.deleteLater()
    qt_app.processEvents()


def state():
    return {
        "schema_version": 1,
        "mode": "paper",
        "sequence": 1,
        "core": {
            "status": "running",
            "paused": True,
            "epoch": "epoch-test",
            "control_revision": 3,
            "server_time_ms": 1_700_000_000_000,
        },
        "market": {"source": "demo"},
        "accounts": [],
        "strategies": [],
        "jobs": [],
        "incidents": [],
        "settings": {"revision": 0, "notifications": {"sound": False}},
    }


def candles(count=5000):
    return parse_candles(
        [
            {
                "open_time_ms": 1_700_000_000_000 + index * 60_000,
                "open": "100",
                "high": "105",
                "low": "95",
                "close": "101",
                "volume": "10",
            }
            for index in range(count)
        ]
    )


def test_resize_all_pages_and_change_theme_without_core(window, qt_app):
    window.show()
    window.accept_state(state())
    for index in range(window.stack.count()):
        window.navigation.setCurrentRow(index)
        for width, height in ((1400, 900), (800, 560)):
            window.resize(width, height)
            qt_app.processEvents()
            image = window.grab().toImage()
            assert image.width() == width
            assert image.height() == height
    window.set_presentation("light", "en")
    assert window.navigation.item(0).text() == "Dashboard"
    window.set_presentation("dark", "ko")
    assert window.navigation.item(0).text() == "대시보드"


def test_display_lod_and_geometry_cache(window, qt_app):
    chart = window.charts.rows[0][3]
    chart.resize(500, 250)
    source = candles()
    chart.nSamples = len(source)
    chart.set_candles(source)
    chart._geometry()
    cached = chart._paths
    assert len(chart._display) <= int(chart._plot().width() / 3)
    chart._geometry()
    assert chart._paths is cached
    assert chart.candles is source
    assert not chart.grab().isNull()


def test_utc_time_range_signal_does_not_truncate_64_bit_timestamp(window):
    chart = window.charts.rows[0][3]
    chart.set_candles(candles(50))
    captured = []
    chart.rangeChanged.connect(lambda start, stop: captured.append((start, stop)))
    chart._emit_range()
    assert captured[0][0] == 1_700_000_000_000
    assert captured[0][1] > 2**31


def test_commands_include_fresh_core_epoch_revision_and_deadline(window):
    client = window.client
    client.state = state()
    client._attached = True
    client.command("resume")
    args, kwargs = window._request_calls[-1]
    envelope = args[2]
    assert envelope["kind"] == "resume"
    assert envelope["payload"] == {
        "expected_epoch": "epoch-test",
        "expected_revision": 3,
        "expires_at_ms": 1_700_000_030_000,
    }
    assert kwargs["tag"] == "command:" + envelope["id"]


def test_disconnected_resume_does_not_create_a_request(window):
    window.client.command("resume")
    assert not window._request_calls
    assert not window.resume_button.isEnabled()


def test_gui_close_detaches_without_shutdown_command(window):
    window.close()
    assert not window._request_calls
    assert window.client._closed
    preferences = json.loads((window.root / "state" / "gui.preferences.json").read_text("utf-8"))
    assert preferences["theme"] == "light"


def test_nn_mc_training_is_one_core_owned_job(window):
    page = window.neural_job
    page.dataset.setText("datasets/source/dataset.json")
    page.module.setText("plugins/neuralnetwork.py")
    page.mc_training.setChecked(True)
    page.submit()
    args, _ = window._request_calls[-1]
    assert args[1] == "/api/jobs"
    spec = args[2]
    assert spec["kind"] == "train"
    assert spec["parameters"]["monte_carlo"] == {
        "enabled": True,
        "seed": 0,
        "block_bars": 60,
        "splits": ["train"],
    }


def test_export_uses_source_file_service_contract(window):
    page = window.data
    page.dataset.setText("source.csv")
    page.submit()
    args, _ = window._request_calls[-1]
    assert args[2]["parameters"]["source_file"] == "source.csv"


def test_offline_run_viewer_reads_metrics_and_structure_without_plugin(window, tmp_path):
    run = tmp_path / "run.json"
    run.write_text('{"schema_version":1,"module":{"module_id":"demo"}}', encoding="utf-8")
    (tmp_path / "metrics.jsonl").write_text(
        '{"metric":"mse","value":0.1,"step":1,"split":"train"}\n', encoding="utf-8"
    )
    (tmp_path / "structures.jsonl").write_text(
        '{"step":0,"structure":{"nodes":[{"id":"x","type":"input"}]}}\n', encoding="utf-8"
    )
    (tmp_path / "instance_module.py").write_text(
        "raise RuntimeError('never import')", encoding="utf-8"
    )
    assert window.open_artifact(run)
    assert window.artifacts.metric_plot.values == [0.1]
    assert len(window.artifacts.graph.layers) == 1


def test_unknown_command_is_queried_without_reposting(window):
    window.client._attached = True
    window.snapshot = state()
    window._failed("command:unknown-operation", "timeout")
    assert not window.resume_button.isEnabled()
    window._verify_operation()
    args, _ = window._request_calls[-1]
    assert args == ("GET", "/api/operations/unknown-operation")


def test_paper_strategy_and_position_display_contract(window):
    window.trading.update_state(
        state()
        | {
            "strategies": [
                {
                    "instance": "tef-01",
                    "module_id": "tef",
                    "instrument": "binance:usdm:BTCUSDT",
                    "status": "ready",
                }
            ]
        }
    )
    assert window.trading.strategies.item(0, 0).text() == "tef-01"
    window.trading.configuration.setPlainText('{"model":"MMACDDEFAULT"}')
    window.trading.analysis.setPlainText('{"moving_averages":[]}')
    window.client._attached = True
    window.client.state = state()
    window.trading._add_strategy()
    args, _ = window._request_calls[-1]
    assert args[2]["kind"] == "paper.strategy.add"
    assert args[2]["payload"]["analysis"] == {"moving_averages": []}


def test_nn_builtin_demo_does_not_require_external_module(window):
    page = window.neural_job
    page.dataset.setText("datasets/source/dataset.json")
    page.demo_instance.setChecked(True)
    page.submit()
    args, _ = window._request_calls[-1]
    assert args[2]["parameters"]["demo"] is True
    assert "module" not in args[2]["parameters"]


def test_localized_header_labels_round_trip(window):
    window.set_presentation("dark", "ko")
    assert window.trading.accounts.horizontalHeaderItem(0).text() == "계정"
    window.set_presentation("dark", "en")
    assert window.trading.accounts.horizontalHeaderItem(0).text() == "Account"


def test_simulation_equity_artifact_can_be_plotted_offline(window, tmp_path):
    path = tmp_path / "simulation.json"
    path.write_text(
        json.dumps(
            {
                "equity": [
                    {"wallet": "10000", "equity": "10010"},
                    {"wallet": "10005", "equity": "10020"},
                ]
            }
        ),
        encoding="utf-8",
    )
    assert window.artifacts.open_path(path)
    window.artifacts.metric_select.setCurrentText("equity / simulation")
    assert window.artifacts.metric_plot.values == [10010, 10020]


def settings(revision=1, *, profile="light", policy="stop", backend="gpu", seed=42):
    return {
        "revision": revision,
        "notifications": {"sound": False},
        "training": {"recording": profile, "optional_recording_failure": policy},
        "simulation": {"backend": backend, "seed": seed, "intrabar": "interpolation"},
    }


def test_core_settings_initialize_research_forms_and_recording_policy(window):
    window.accept_state(state() | {"settings": settings(seed=2**62 + 7)})
    assert window.simulation.device.currentText() == "gpu"
    assert window.simulation.seed.value() == 2**62 + 7
    spec = json.loads(window.simulation.parameters.toPlainText())["spec"]
    assert spec["backend"] == "gpu" and spec["seed"] == 2**62 + 7
    assert spec["path"] == "O-L-H-C"
    assert window.neural_job.profile.currentText() == "lightweight"
    recording = json.loads(window.neural_job.parameters.toPlainText())["recording"]
    assert recording == {"profile": "lightweight", "optional_detail_failure": "stop"}


def test_repeated_state_poll_preserves_job_specific_research_edits(window):
    snapshot = state() | {"settings": settings()}
    window.accept_state(snapshot)
    window.simulation.seed.setValue(77)
    window.simulation.device.setCurrentText("cpu")
    window.neural_job.profile.setCurrentText("detailed")
    draft = '{"config":{"epochs":77},"recording":{"optional_detail_failure":"warn_continue"}}'
    window.neural_job.parameters.setPlainText(draft)
    window.accept_state(snapshot)
    assert window.simulation.seed.value() == 77
    assert window.simulation.device.currentText() == "cpu"
    assert window.neural_job.profile.currentText() == "detailed"
    assert window.neural_job.parameters.toPlainText() == draft


def test_new_settings_revision_updates_defaults_without_erasing_unrelated_parameters(window):
    window.accept_state(state() | {"settings": settings()})
    window.neural_job.parameters.setPlainText('{"config":{"epochs":77},"recording":{}}')
    window.accept_state(
        state()
        | {"settings": settings(2, profile="standard", policy="continue", backend="cpu", seed=99)}
    )
    parameters = json.loads(window.neural_job.parameters.toPlainText())
    assert parameters["config"]["epochs"] == 77
    assert parameters["recording"] == {
        "profile": "standard",
        "optional_detail_failure": "warn_continue",
    }
    assert window.simulation.device.currentText() == "cpu"
    assert window.simulation.seed.value() == 99


def test_settings_preserve_unfinished_json_and_independent_mc_nn_seeds(window):
    window.neural_job.seed.setValue(123)
    window.data.seed.setValue(456)
    window.neural_job.parameters.setPlainText('{"config":')
    window.accept_state(state() | {"settings": settings(seed=888)})
    assert window.neural_job.parameters.toPlainText() == '{"config":'
    assert "draft was preserved" in window.neural_job.result.toPlainText()
    assert window.neural_job.seed.value() == 123
    assert window.data.seed.value() == 456


def test_seed_input_does_not_silently_truncate_or_round_64_bit_values(window):
    page = window.simulation
    page.dataset.setText("datasets/source/dataset.json")
    page.seed.setValue(2**63 - 1)
    page.submit()
    args, _ = window._request_calls[-1]
    assert args[2]["parameters"]["spec"]["seed"] == 2**63 - 1
    page.seed.setText(str(2**63))
    page.submit()
    assert "below 2^63" in page.result.toPlainText()


def test_quarantined_research_disables_new_jobs_without_hiding_trading(window):
    window.client._attached = True
    snapshot = state() | {"research": {"state": "degraded", "quarantined": True}}
    window.accept_state(snapshot)
    before = len(window._request_calls)
    for page in (window.data, window.simulation, window.neural_job):
        assert not page.run_button.isEnabled()
        assert not page.health_notice.isHidden()
        page.submit()
    assert len(window._request_calls) == before
    assert window.pause_button.isEnabled()
    window.accept_state(state() | {"research": {"state": "ready", "quarantined": False}})
    for page in (window.data, window.simulation, window.neural_job):
        assert page.run_button.isEnabled()
        assert page.health_notice.isHidden()
