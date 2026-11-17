"""Explicit per-strategy execution policy; real Qt controls, synthetic API only."""

import importlib.util
import os
from copy import deepcopy

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)
KEY, EPOCH = "binance:usdm:BTCUSDT", "analysis-core-a"
POLICY = {"version": 1, "policy": "required_fields"}


def profile():
    return {
        "moving_averages": [
            {
                "interval_id": 0,
                "ema_period": 2000,
                "mmacd_periods": [2, 3],
                "signal_period": 2,
                "price_precision": 8,
            }
        ],
        "momentum": [],
        "market_sentiment": [
            {"interval_id": 5, "nSamples": 14, "line_index": 0, "maximum_metric_age_ms": 330000}
        ],
        "order_flow": [{"interval_id": 0, "analysis_code": "WOI", "nSamples": 14}],
    }


def library(document):
    return {
        "schema_version": 1,
        "revision": 1,
        "persistence": "ready",
        "templates": [],
        "instances": [
            {
                "id": "projection-ca",
                "name": "Full saved analysis",
                "revision": 1,
                "instrument": KEY,
                "template_id": "template",
                "template_revision": 1,
                "configuration": deepcopy(document),
            }
        ],
    }


@pytest.fixture(scope="module")
def projection_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["margin-analysis-projection-controls"])


@pytest.fixture
def forms(projection_app):
    from PySide6.QtCore import QDateTime, QObject, QTimeZone, Signal

    from omega.gui.margin_page import MarginPage
    from omega.gui.pages import ResearchPage

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        connectionChanged = Signal(bool)
        modulesReceived = Signal(dict)
        attached = True

        def __init__(self):
            super().__init__()
            self.state = {
                "core": {
                    "epoch": EPOCH,
                    "capabilities": {
                        "margin_trading": True,
                        "margin_simulation": True,
                        "margin_analysis_execution": True,
                        "margin_analysis_execution_scope": "required_fields_v1",
                    },
                }
            }
            self.requests, self.jobs = [], []

        def request(self, method, path, payload=None, *, tag):
            assert method == "GET", "Local checkbox changes must not submit financial commands"
            self.requests.append((method, path, deepcopy(payload), tag))

        def submit_job(self, kind, parameters):
            self.jobs.append((kind, deepcopy(parameters)))
            return "job:projection"

    client = Client()
    margin, research = MarginPage(client), ResearchPage(client, ["simulate"])
    margin.timer.stop()
    margin.update_state(client.state)
    margin.set_attached(True)
    margin.account_id.setText("projection-account")
    margin.balance.setText("1000")
    for edit in (margin.start, margin.bracket_time):
        edit.setDateTime(QDateTime.fromMSecsSinceEpoch(0, QTimeZone(b"UTC")))
    research.dataset.setText("selected/candles/dataset.json")
    research.set_attached(True)
    yield margin, research, client
    for widget in (margin, research):
        widget.close()
        widget.deleteLater()
    projection_app.processEvents()


def apply(selector, document=None):
    selected = profile() if document is None else document
    selector.set_context(KEY, core_epoch=EPOCH, attached=True)
    snapshot = library(selected)
    selector.set_analysis_library(snapshot, core_epoch=EPOCH)
    selector.choice.setCurrentIndex(selector.choice.findData("projection-ca"))
    selector.apply_button.click()
    return snapshot


def test_default_off_preserves_legacy_strategy_and_no_requests(forms):
    margin, research, client = forms
    assert not margin.required_analysis.isChecked() and not margin.required_analysis.isEnabled()
    legacy = margin.strategy_document()
    assert "analysis_execution" not in legacy["strategy"]
    assert legacy["strategy"]["analysis"] == {
        "ema_period": 20,
        "mmacd_periods": [12, 26],
        "signal_period": 9,
    }
    replay = research.margin_replay
    assert not replay.required_analysis.isChecked() and not replay.required_analysis.isEnabled()
    replay.add_strategy()
    assert "analysis_execution" not in replay.rows[0]["strategy"]
    assert not client.requests and not client.jobs


def test_typed_ca_opt_in_is_exact_and_preserves_full_unused_analysis(forms):
    margin, _research, client = forms
    snapshot = apply(margin.analysis_selector)
    original = deepcopy(snapshot)
    assert margin.required_analysis.isEnabled() and not margin.required_analysis.isChecked()
    assert "analysis_execution" not in margin.strategy_document()["strategy"]
    margin.required_analysis.click()
    row = margin.strategy_document()
    assert row["strategy"]["analysis_execution"] == POLICY
    assert row["strategy"]["analysis"] == profile()
    row["strategy"]["analysis"]["moving_averages"][0]["ema_period"] = 999
    assert margin.strategy_document()["strategy"]["analysis"] == profile()
    margin.save_strategy()
    assert margin.rows[0]["strategy"]["analysis_execution"] == POLICY
    assert margin.required_analysis.isChecked()
    assert margin.rows[0]["strategy"]["analysis"] == profile() and snapshot == original
    previous = deepcopy(margin.rows)
    margin.new_strategy()
    assert not margin.required_analysis.isChecked() and margin.rows == previous
    assert not client.requests and not client.jobs


@pytest.mark.parametrize("scope", [None, "other", True])
def test_capability_loss_holds_checked_choice_and_never_silently_downgrades(forms, scope):
    margin, research, client = forms
    apply(margin.analysis_selector)
    margin.required_analysis.click()
    margin.save_strategy()
    saved = deepcopy(margin.rows)
    caps = client.state["core"]["capabilities"]
    caps["margin_analysis_execution_scope"] = scope
    margin.update_state(client.state)
    research._simulation_mode_changed()
    assert margin.required_analysis.isChecked() and not margin.required_analysis.isEnabled()
    with pytest.raises(ValueError, match="required_fields_v1"):
        margin.strategy_document()
    assert margin.rows == saved and not client.requests and not client.jobs


def test_research_checkbox_carries_same_explicit_policy_and_imported_rows(forms, projection_app):
    from omega.gui.margin_replay_controls import MarginReplayControls

    _margin, research, client = forms
    replay = research.margin_replay
    replay.module.setCurrentIndex(replay.module.findData("tef"))
    apply(replay.analysis_selector)
    assert replay.required_analysis.isEnabled() and not replay.required_analysis.isChecked()
    replay.required_analysis.click()
    replay.add_strategy()
    replay.manual.setChecked(True)
    parameters = replay.parameters()
    assert parameters["strategies"][0]["analysis_execution"] == POLICY
    assert parameters["strategies"][0]["analysis"] == profile()
    other = MarginReplayControls()
    try:
        other.set_analysis_context(core_epoch=EPOCH, attached=True)
        other.set_analysis_execution_supported(True)
        other.load_parameters(parameters)
        assert other.parameters(_preview=True)["strategies"] == parameters["strategies"]
        assert "required indicators" in other.table.item(0, 2).toolTip()
        assert other.analysis_execution_can_submit()
        before = deepcopy(other.rows)
        other.set_analysis_execution_supported(False)
        assert not other.analysis_execution_can_submit() and other.rows == before
        with pytest.raises(ValueError, match="required_fields_v1"):
            other.parameters(_preview=True)
        with pytest.raises(ValueError, match="required_fields_v1"):
            other.load_parameters(parameters)
        assert other.rows == before
    finally:
        other.close()
        other.deleteLater()
        projection_app.processEvents()
    assert not client.jobs
    research.simulation_mode.setCurrentIndex(1)
    research.submit()
    assert client.jobs[-1][0] == "simulate_margin"
    assert client.jobs[-1][1]["strategies"][0]["analysis_execution"] == POLICY
    caps = client.state["core"]["capabilities"]
    caps["margin_analysis_execution"] = False
    research._simulation_mode_changed()
    assert not replay.required_analysis.isEnabled() and replay.required_analysis.isChecked()
    assert not replay.analysis_execution_can_submit()
    assert not research.run_button.isEnabled()


@pytest.mark.parametrize(
    "document", [{"moving_averages": [], "momentum": []}, {"market_sentiment": []}]
)
def test_explicit_empty_typed_profiles_are_not_replaced_with_legacy_defaults(forms, document):
    margin, _research, _client = forms
    margin.module.setCurrentIndex(margin.module.findData("target_quantity"))
    apply(margin.analysis_selector, document)
    assert margin.required_analysis.isEnabled()
    margin.required_analysis.click()
    row = margin.strategy_document()
    assert row["strategy"]["analysis"] == document
    assert row["strategy"]["analysis_execution"] == POLICY


@pytest.mark.parametrize(
    "policy",
    [
        None,
        {},
        {"version": True, "policy": "required_fields"},
        {"version": 1, "policy": "other"},
        POLICY | {"extra": True},
    ],
)
def test_imported_invalid_policy_rejects_before_changing_research_draft(forms, policy):
    _margin, research, _client = forms
    replay = research.margin_replay
    replay.add_strategy()
    replay.manual.setChecked(True)
    original = replay.parameters()
    changed = deepcopy(original)
    changed["strategies"][0]["analysis"] = profile()
    changed["strategies"][0]["analysis_execution"] = policy
    before = deepcopy(replay.rows)
    with pytest.raises(ValueError, match="Invalid analysis_execution"):
        replay.load_parameters(changed)
    assert replay.rows == before


def test_registered_configuration_copy_preserves_explicit_policy_or_rejects_unsupported(forms):
    from omega.gui.margin_config_copy import new_parameters, prepare_source
    from omega.gui.margin_page import _identity, _money

    margin, _research, _client = forms
    apply(margin.analysis_selector)
    margin.required_analysis.click()
    margin.save_strategy()
    document = margin.parameters(_preview=True)
    kwargs = {
        "modules": {"tef", "target_quantity"},
        "schema_for": margin._schema,
        "number": _money,
        "identity": _identity,
    }
    with pytest.raises(ValueError, match="required_fields_v1"):
        prepare_source(document, **kwargs)
    source = prepare_source(document, analysis_execution_supported=True, **kwargs)
    copied = new_parameters(
        source,
        new_id="new-copy",
        balance="1000",
        start_ms=0,
        known_ids=set(),
        number=_money,
        identity=_identity,
    )
    assert copied["strategies"][0]["analysis_execution"] == POLICY
    assert copied["strategies"][0]["analysis"] == profile()
    assert document["strategies"][0]["analysis"] == profile()


@pytest.mark.parametrize(
    "document",
    [{}, {"ema_period": 20, "mmacd_periods": [12, 26], "signal_period": 9}, {"unknown": []}],
)
def test_legacy_or_unknown_profiles_cannot_opt_in(forms, document):
    from omega.gui.margin_config_copy import analysis_execution_policy

    with pytest.raises(ValueError):
        analysis_execution_policy(POLICY, document, supported=True)


def test_ca_revision_and_epoch_changes_disable_only_future_draft_choice(forms):
    margin, _research, client = forms
    apply(margin.analysis_selector)
    margin.required_analysis.click()
    margin.save_strategy()
    before = deepcopy(margin.rows)
    changed = library(profile())
    changed["revision"] = changed["instances"][0]["revision"] = 2
    margin.analysis_selector.set_analysis_library(changed, core_epoch=EPOCH)
    assert not margin.required_analysis.isEnabled() and margin.required_analysis.isChecked()
    assert margin.rows == before
    client.state["core"]["epoch"] = "analysis-core-b"
    margin.update_state(client.state)
    assert not margin.required_analysis.isEnabled() and margin.rows == before
    assert not client.requests and not client.jobs
