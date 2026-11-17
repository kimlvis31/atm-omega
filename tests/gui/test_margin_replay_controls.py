"""Structured shared-margin drafts stay separate from the legacy replay policy."""

import importlib.util
import json

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional PySide6 is not installed"
)


@pytest.fixture(scope="module")
def app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["margin-replay-gui-tests"])


@pytest.fixture
def page(app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.pages import ResearchPage

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        connectionChanged = Signal(bool)
        modulesReceived = Signal(dict)

        def __init__(self):
            super().__init__()
            self.attached, self.calls = True, []
            self.state = {"core": {"epoch": "first", "capabilities": {"margin_simulation": True}}}

        def submit_job(self, kind, parameters):
            self.calls.append((kind, parameters))
            return "job-one"

        def request(self, *_args, **_kwargs):
            pass

    widget = ResearchPage(Client(), ["simulate"])
    yield widget
    widget.close()
    widget.deleteLater()
    app.processEvents()


def draft(page):
    page.simulation_mode.setCurrentIndex(1)
    controls = page.margin_replay
    controls.add_strategy()
    controls.manual.setChecked(True)
    page.dataset.setText("/data/dataset.json")
    return controls


def test_shared_structured_submit_uses_separate_contract_without_legacy_cost_keys(page):
    controls = draft(page)
    page.parameters.setPlainText('{"spec":{"slippage_rate":".2","path":"random"}}')
    page.device.setCurrentText("gpu")
    page.submit()
    kind, parameters = page.client.calls[0]
    assert kind == "simulate_margin"
    assert parameters["account"]["positions"][0]["margin_mode"] == "cross"
    assert parameters["strategies"][0]["module"] == "target_quantity"
    assert parameters["spec"] == {"fee_rate": "0.001", "backend": "cpu"}
    assert "seed" not in parameters and "slippage_rate" not in parameters["spec"]
    assert not controls.noncausal.isChecked()


def test_explicit_assumption_confirmation_required_and_no_submit_on_error(page):
    controls = draft(page)
    controls.manual.setChecked(False)
    page.submit()
    assert page.client.calls == []
    assert "consent" in page.result.toPlainText()


def test_old_core_cannot_submit_shared_mode_and_legacy_mode_still_works(page):
    draft(page)
    page.client.state["core"]["capabilities"] = {}
    page.set_attached(True)
    page.submit()
    assert not page.client.calls and not page.run_button.isEnabled()
    page.simulation_mode.setCurrentIndex(0)
    assert page.run_button.isEnabled()
    page.parameters.setPlainText('{"quantity":"1","spec":{}}')
    page.submit()
    assert page.client.calls[0][0] == "simulate"
    assert "account" not in page.client.calls[0][1]


def test_multiple_strategies_keep_cross_isolated_budgets_and_collateral(page):
    controls = draft(page)
    controls.symbol.setText("ETHUSDT")
    controls.margin_mode.setCurrentText("isolated")
    controls.configuration.setPlainText('{"quantity":"-1"}')
    controls.add_strategy()
    parameters = controls.parameters()
    assert len(parameters["strategies"]) == 2
    assert parameters["account"]["positions"][1]["isolated_collateral"] == "100"
    assert parameters["strategies"][1]["configuration"] == {"quantity": "-1"}
    controls.symbol.setText("ETHUSDT")
    controls.add_strategy()
    assert len(controls.rows) == 2 and "unique" in controls.status.text()


def test_copy_shared_draft_never_submits_or_automatically_accepts_consent(page):
    controls = draft(page)
    parameters = controls.parameters() | {"dataset": "/data/source.json"}
    page.simulation_mode.setCurrentIndex(0)
    page.apply_simulation_draft(parameters)
    assert page.simulation_mode.currentData() == "margin"
    assert page.dataset.text() == "/data/source.json"
    assert not controls.manual.isChecked() and not controls.noncausal.isChecked()
    assert not page.client.calls


def test_copy_legacy_draft_preserves_legacy_policy_and_seed(page):
    page.apply_simulation_draft(
        {"dataset": "/data/linear.json", "spec": {"seed": 13, "backend": "cpu", "path": "random"}}
    )
    assert page.simulation_mode.currentData() == "linear"
    assert page.seed.value() == 13 and page.device.currentText() == "cpu"
    assert json.loads(page.parameters.toPlainText())["spec"]["path"] == "random"


def test_tef_uses_manifest_fields_and_normal_feature_configuration(page):
    controls = page.margin_replay
    controls.module.setCurrentIndex(controls.module.findData("tef"))
    controls.set_modules(
        {
            "ts": [
                {
                    "module_id": "tef",
                    "configuration_schema": {
                        "properties": {"fsl": {"type": "string", "default": "0.05"}}
                    },
                }
            ]
        }
    )
    controls.configuration.setPlainText('{"model":"MMACDDEFAULT","fsl":"0.05"}')
    controls.add_strategy()
    controls.manual.setChecked(True)
    p = controls.parameters()
    assert p["strategies"][0]["analysis"] == {
        "ema_period": 20,
        "mmacd_periods": [12, 26],
        "signal_period": 9,
    }


def test_unsafe_budget_and_heterogeneous_recorded_references_are_not_silently_changed(page):
    controls = draft(page)
    controls.balance.setText("1")
    with pytest.raises(ValueError, match="budgets"):
        controls.parameters()
    controls.balance.setText("10000")
    parameters = controls.parameters()
    before = controls.parameters()
    parameters["account"]["balance"] = "12000"
    parameters["brackets"][0]["available_at_ms"] = 1
    with pytest.raises(ValueError, match="heterogeneous"):
        controls.load_parameters(parameters)
    assert controls.parameters() == before


def test_shared_results_are_listed_and_open_their_own_recorded_artifact(page, tmp_path):
    from omega.gui.simulation_results import SimulationResultsPage

    view = SimulationResultsPage(page.client)
    identity = "a" * 32
    page.client.root = tmp_path
    try:
        view.update_jobs(
            [
                {
                    "id": identity,
                    "kind": "simulate_margin",
                    "state": "complete",
                    "created_at_ms": 0,
                },
                {"id": "b" * 32, "kind": "simulate_margin", "state": "unknown"},
                {"id": "c" * 32, "kind": "train", "state": "complete"},
            ]
        )
        assert list(view.jobs) == [identity]
        calls = []
        view.open_path = lambda *args, **kwargs: calls.append((args, kwargs))
        view._selected_id, view._expected_job = identity, "selected-shared"
        result = {
            "id": identity,
            "kind": "simulate_margin",
            "state": "complete",
            "result_file": str(tmp_path / "artifacts" / identity / "simulation.json"),
            "parameters": {"dataset": "/data/dataset.json"},
        }
        view._result("selected-shared", result)
        assert len(calls) == 1 and calls[0][1]["parameters"] == result["parameters"]
        result["result_file"] = str(tmp_path / "other.json")
        view._result("selected-shared", result)
        assert len(calls) == 1
    finally:
        view.close()
        view.deleteLater()
