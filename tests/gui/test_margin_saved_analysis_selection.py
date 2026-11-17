"""Real Qt saved-CA copies preserve full CGALPHA analysis; all API IO is synthetic."""

import importlib.util
import json
import os
from copy import deepcopy
from importlib.resources import files

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)
BTC = "binance:usdm:BTCUSDT"
EPOCH = "core-a"


def profile():
    return {
        "moving_averages": [
            {
                "interval_id": 7,
                "ema_period": None,
                "mmacd_periods": [12, 26],
                "signal_period": 9,
                "price_precision": 8,
            }
        ],
        "momentum": [
            {"interval_id": 5, "analysis_code": "MFI", "nSamples": 14, "line_index": 0},
            {"interval_id": 7, "analysis_code": "DMIxADX", "nSamples": 14, "line_index": 0},
        ],
        "parabolic": [
            {
                "interval_id": 5,
                "line_index": 0,
                "start": 0.02,
                "acceleration": 0.005,
                "maximum": 0.2,
                "price_precision": 8,
            }
        ],
        "order_flow": [
            {
                "interval_id": 0,
                "analysis_code": code,
                "nSamples": 14,
                "line_index": 0,
                "quote_precision": 8,
            }
            for code in ("NES", "WOI")
        ],
    }


def library(revision=1, *, instrument=BTC):
    return {
        "schema_version": 1,
        "revision": revision,
        "persistence": "ready",
        "templates": [],
        "instances": [
            {
                "id": "ca-cgalpha",
                "name": "CGALPHA full",
                "revision": revision,
                "instrument": instrument,
                "template_id": "cac",
                "template_revision": 1,
                "configuration": profile(),
            }
        ],
    }


@pytest.fixture(scope="module")
def saved_analysis_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["saved-analysis-tests"])


@pytest.fixture
def widgets(saved_analysis_app):
    from PySide6.QtCore import QObject, Signal

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
                    "capabilities": {"margin_trading": True, "margin_simulation": True},
                }
            }
            self.calls, self.jobs = [], []

        def request(self, method, path, payload=None, *, tag):
            assert method == "GET", "analysis selection must not submit financial commands"
            self.calls.append((method, path, payload, tag))

        def submit_job(self, kind, parameters):
            self.jobs.append((kind, deepcopy(parameters)))
            return "job:synthetic"

    client = Client()
    margin, research = MarginPage(client), ResearchPage(client, ["simulate"])
    margin.timer.stop()
    margin.update_state(client.state)
    margin.set_attached(True)
    research.set_attached(True)
    manifest = json.loads(files("omega.strategies.tef").joinpath("manifest.json").read_text())
    margin.set_modules({"ts": [manifest]})
    research.set_modules({"ts": [manifest]})
    yield margin, research, client
    for widget in (margin, research):
        widget.close()
        widget.deleteLater()
    saved_analysis_app.processEvents()


def apply(selector, *, instrument=BTC, epoch=EPOCH):
    selector.set_context(instrument, core_epoch=epoch, attached=True)
    selector.set_analysis_library(library(instrument=instrument), core_epoch=epoch)
    selector.choice.setCurrentIndex(selector.choice.findData("ca-cgalpha"))
    selector.apply_button.click()


def choose_cgalpha(form, app):
    form.module.setCurrentIndex(form.module.findData("tef"))
    editor = getattr(form, "configuration_editor", None) or form.editor
    choice = editor.controls[("model",)][0]
    choice.setCurrentIndex(choice.findData("CGALPHA"))
    app.processEvents()


def test_full_profile_is_explicit_copied_and_accepted_by_actual_cgalpha_graph(
    widgets, saved_analysis_app
):
    from omega.analysis.catalog import configured_output_fields, create_features
    from omega.domain.models import Instrument, Market
    from omega.strategies.tef.module import TefModule

    _, research, client = widgets
    replay = research.margin_replay
    choose_cgalpha(replay, saved_analysis_app)
    selector = replay.analysis_selector
    selector.set_analysis_library(library(), core_epoch=EPOCH)
    selector.choice.setCurrentIndex(selector.choice.findData("ca-cgalpha"))
    replay.add_strategy()
    assert replay.rows == [] and "Apply" in replay.status.text()
    selector.apply_button.click()
    replay.add_strategy()
    row = replay.rows[0]["strategy"]
    assert row["configuration"]["model"] == "CGALPHA"
    assert row["analysis"] == profile()
    instrument = Instrument("binance", Market.USDM, "BTCUSDT")
    module = TefModule(instrument, row["configuration"])
    assert len(module.descriptor.required_fields) == 8
    assert module.descriptor.required_fields <= configured_output_fields(row["analysis"])
    create_features(instrument, module.descriptor.required_fields, row["analysis"])
    assert "ca-cgalpha r1" in selector.status.text()
    assert not replay.fast.isEnabled()
    assert "WOI/depth" in replay.auxiliary_notice.text()  # Research bundle support stays separate.
    original = deepcopy(row)
    selector.set_analysis_library(library(2), core_epoch=EPOCH)
    assert replay.rows[0]["strategy"] == original
    with pytest.raises(ValueError, match="Apply"):
        selector.analysis_document(BTC, core_epoch=EPOCH)
    assert client.jobs == []


def test_selector_deep_copy_instrument_epoch_and_unknown_fields(widgets):
    margin, _, _ = widgets
    selector = margin.analysis_selector
    apply(selector)
    copied = selector.analysis_document(BTC, core_epoch=EPOCH)
    copied["order_flow"][0]["nSamples"] = 99
    assert selector.analysis_document(BTC, core_epoch=EPOCH) == profile()
    with pytest.raises(ValueError):
        selector.load_document(profile() | {"unknown_group": []}, BTC)
    assert selector.analysis_document(BTC, core_epoch=EPOCH) == profile()
    with pytest.raises(ValueError, match="context changed"):
        selector.analysis_document("binance:usdm:ETHUSDT", core_epoch=EPOCH)
    selector.set_context(BTC, core_epoch="core-b", attached=True)
    with pytest.raises(ValueError):
        selector.analysis_document(BTC, core_epoch="core-b")
    with pytest.raises(ValueError, match="Stale"):
        selector.set_analysis_library(library(), core_epoch=EPOCH)


def test_v2_row_edit_and_registered_copy_retain_full_typed_profile(widgets, saved_analysis_app):
    from omega.gui.margin_config_copy import prepare_source
    from omega.gui.margin_page import _identity, _money

    margin, _, client = widgets
    choose_cgalpha(margin, saved_analysis_app)
    apply(margin.analysis_selector)
    margin.save_strategy()
    assert len(margin.rows) == 1
    margin.strategy_table.selectRow(0)
    margin._strategy_selected()
    margin.budget.setText("200")
    margin.save_strategy()
    assert margin.rows[0]["strategy"]["analysis"] == profile()
    assert "no inferred CA ID" in margin.analysis_selector.status.text()
    # Build an isolated copy input without account/source API calls or execution.
    row = deepcopy(margin.rows[0])
    request = {
        "account": {"account_id": "source", "balance": "1000", "positions": [row["position"]]},
        "strategies": [row["strategy"]],
        "start_ms": 0,
        "brackets": [
            {
                "instrument": BTC,
                "observed_at_ms": 0,
                "available_at_ms": 0,
                "rows": [
                    {
                        "notional_floor": "0",
                        "notional_cap": "1000000",
                        "maintenance_rate": "0.01",
                        "maintenance_amount": "0",
                        "max_leverage": 20,
                    }
                ],
            }
        ],
    }
    original = deepcopy(request)
    prepared = prepare_source(
        request, modules={"tef"}, schema_for=margin._schema, number=_money, identity=_identity
    )
    assert prepared["rows"][0]["strategy"]["analysis"] == profile() and request == original
    request["strategies"][0]["analysis"]["unknown"] = []
    with pytest.raises(ValueError):
        prepare_source(
            request, modules={"tef"}, schema_for=margin._schema, number=_money, identity=_identity
        )
    assert client.jobs == []


@pytest.mark.parametrize("owner", ["margin", "research"])
def test_pages_own_get_only_epoch_tags_and_ignore_late_responses(widgets, owner):
    margin, research, client = widgets
    page = margin if owner == "margin" else research
    selector = (
        margin.analysis_selector if owner == "margin" else research.margin_replay.analysis_selector
    )
    assert client.calls == [] and client.jobs == []
    selector.refresh_button.click()
    method, path, payload, tag = client.calls[-1]
    assert (method, path, payload) == ("GET", "/api/analysis-library", None)
    # Existing endpoint has no JSON core_epoch; the originating request is fenced.
    client.resultReceived.emit(tag, library())
    assert selector.choice.findData("ca-cgalpha") > 0 and client.jobs == []
    selector.refresh_button.click()
    old_tag = client.calls[-1][3]
    client.state["core"]["epoch"] = "core-b"
    if owner == "margin":
        page.update_state(client.state)
    else:
        page.set_attached(True)
    client.resultReceived.emit(old_tag, library(9))
    assert selector.choice.findData("ca-cgalpha") == -1
    selector.refresh_button.click()
    client.failed.emit(client.calls[-1][3], "synthetic unsupported API")
    assert selector.analysis_document(BTC, core_epoch="core-b") is None
    assert client.jobs == []


def test_legacy_defaults_still_work_without_analysis_api(widgets):
    margin, research, client = widgets
    replay = research.margin_replay
    replay.module.setCurrentIndex(replay.module.findData("tef"))
    replay.add_strategy()
    assert replay.rows[0]["strategy"]["analysis"] == {
        "ema_period": 20,
        "mmacd_periods": [12, 26],
        "signal_period": 9,
    }
    margin.module.setCurrentIndex(margin.module.findData("tef"))
    assert margin.strategy_document()["strategy"]["analysis"] == {
        "ema_period": 20,
        "mmacd_periods": [12, 26],
        "signal_period": 9,
    }
    assert client.calls == [] and client.jobs == []


def test_foreign_ca_needs_matching_instrument_and_selector_fits_small_form(saved_analysis_app):
    from omega.gui.analysis_template_selector import AnalysisTemplateSelector

    selector = AnalysisTemplateSelector()
    selector.set_context(BTC, core_epoch=EPOCH, attached=True)
    selector.set_analysis_library(library(instrument="binance:usdm:ETHUSDT"), core_epoch=EPOCH)
    selector.choice.setCurrentIndex(selector.choice.findData("ca-cgalpha"))
    selector.apply_button.click()
    assert "must match" in selector.status.text()
    with pytest.raises(ValueError, match="Apply"):
        selector.analysis_document(BTC, core_epoch=EPOCH)
    selector.resize(420, 240)
    selector.show()
    saved_analysis_app.processEvents()
    assert selector.refresh_button.isVisible() and selector.apply_button.isVisible()
    assert selector.refresh_button.width() > 40 and selector.apply_button.width() > 40
    selector.close()
    selector.deleteLater()
    saved_analysis_app.processEvents()
