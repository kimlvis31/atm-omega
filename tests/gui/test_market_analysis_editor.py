"""Structured market indicators round-trip installed contracts without GUI analysis imports."""

import json
import os
from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication, QLineEdit, QSpinBox

from omega.analysis.catalog import validate_configuration
from omega.analysis.market_sentiment import SentimentConfiguration
from omega.analysis.order_flow import OrderFlowConfiguration
from omega.analysis.short_term_scalar import ScalarConfiguration
from omega.analysis.volume_profile import ProfileConfiguration
from omega.gui.analysis_page import AnalysisPage, configuration_rows, form_configuration
from omega.gui.presentation import configure_fonts, stylesheet
from omega.runtime.analysis_library import AnalysisLibrary


@pytest.mark.parametrize("capability", [True, False, None])
def test_auxiliary_hint_follows_core_capability_and_keeps_depth_limit(page, capability):
    page.update_state(
        {"core": {"epoch": "aux-hint", "capabilities": {"analysis_auxiliary_inputs": capability}}}
    )
    page.set_presentation("light", "en")
    hint = page.support_hint.text()
    if capability is True:
        assert "committed DB auxiliary observations automatically" in hint
        assert "Depth collection for WOI/STS" in hint
        assert "live-trading strategy subscriptions remain unsupported" in hint
        assert "current CAs do not subscribe automatically" not in hint
    else:
        assert "current CAs do not subscribe automatically" in hint


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    current = QApplication.instance() or QApplication(["market-analysis-editor"])
    configure_fonts(current)
    return current


@pytest.fixture
def page(app, tmp_path):
    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        connectionChanged = Signal(bool)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls, self.commands = [], []
            self.library = AnalysisLibrary(
                tmp_path / "analysis.json", references_provider=lambda: []
            )

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))

        def command(self, kind, payload):
            identity = f"{len(self.commands) + 1:032x}"
            self.commands.append((identity, kind, deepcopy(payload)))
            return identity

        def complete(self):
            identity, kind, payload = self.commands[-1]
            outcome = self.library.command(kind, payload, operation_id=identity)
            self.resultReceived.emit(
                "command:" + identity, {"status": "completed", "result": outcome}
            )
            self.resultReceived.emit("analysis-library", self.library.snapshot())

    current = AnalysisPage(Client())
    current.set_presentation("light", "en")
    current.apply_snapshot(current.client.library.snapshot())
    current.resize(1360, 900)
    current.show()
    yield current
    current.close()
    current.deleteLater()
    app.processEvents()


@pytest.mark.parametrize(
    "kind,group,expected",
    [
        ("NES", "order_flow", asdict(OrderFlowConfiguration(0, "NES"))),
        ("WOI", "order_flow", asdict(OrderFlowConfiguration(0, "WOI"))),
        ("MST", "market_sentiment", asdict(SentimentConfiguration(0))),
        ("STS", "short_term_scalar", asdict(ScalarConfiguration(0))),
        ("IVP", "volume_profile", asdict(ProfileConfiguration(0))),
    ],
)
def test_new_kind_widgets_produce_exact_installed_defaults_and_roundtrip(
    page, kind, group, expected
):
    page.kind.setCurrentText(kind)
    page.add_definition()
    payload = form_configuration(page._definitions)
    assert payload[group] == [expected]
    canonical = validate_configuration(payload)
    assert canonical[group] == [expected]
    assert validate_configuration(form_configuration(configuration_rows(canonical))) == canonical
    assert page.market_details.isVisible()
    assert not page.json_input.isVisible()  # Its supplementary ancestor is collapsed.


def test_old_cac_has_no_new_empty_groups_or_hash_changes():
    original = validate_configuration(
        {
            "moving_averages": [{"interval_id": 0, "ema_period": 2, "price_precision": 8}],
            "momentum": [],
        }
    )
    reconstructed = validate_configuration(form_configuration(configuration_rows(original)))

    def digest(value):
        return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    assert reconstructed == original and digest(reconstructed) == digest(original)
    assert (
        not {"order_flow", "market_sentiment", "short_term_scalar", "volume_profile"}
        & reconstructed.keys()
    )


def test_line_keys_for_flow_and_sentiment_but_single_sts_profile_per_interval(page):
    for kind in ("NES", "WOI", "MST"):
        page.kind.setCurrentText(kind)
        for line in (0, 1):
            page.line.setValue(line)
            page.add_definition()
    count = len(page._definitions)
    page.add_definition()
    assert len(page._definitions) == count and "Duplicate" in page.status.text()
    for kind in ("STS", "IVP"):
        page.kind.setCurrentText(kind)
        assert not page.line.isEnabled()
        page.add_definition()
        count = len(page._definitions)
        page.add_definition()
        assert len(page._definitions) == count and "Duplicate" in page.status.text()
        assert "line_index" not in page._definitions[-1]


@pytest.mark.parametrize(
    "kind,fields",
    [
        ("NES", {"nSamples": 100001}),
        ("WOI", {"line_index": True}),
        ("MST", {"nSamples": 20001}),
        ("MST", {"maximum_metric_age_ms": -1}),
        ("MST", {"maximum_metric_age_ms": 86400001}),
        ("STS", {"MMACDFast": 20, "MMACDSlow": 20}),
        ("STS", {"OSCScale": 0}),
        ("STS", {"WMMACD": 0, "WMFI": 0, "WDMIxADX": 0, "WWOI": 0}),
        ("STS", {"WWOI": -1}),
        ("STS", {"WMMACD": "NaN"}),
        ("STS", {"WMFI": float("inf")}),
        ("STS", {"MFINSamples": 20001}),
        ("STS", {"line_index": 1}),
        ("IVP", {"line_index": 1}),
        ("IVP", {"gammaFactor": 0.001}),
        ("IVP", {"deltaFactor": 0.01}),
        ("IVP", {"prominence": 0}),
        ("IVP", {"distance": 0}),
        ("IVP", {"height": 1.001}),
        ("IVP", {"max_divisions": 32769}),
    ],
)
def test_invalid_market_parameters_are_rejected_before_sending(kind, fields):
    with pytest.raises(ValueError):
        form_configuration([{"kind": kind, "interval_id": 0, **fields}])


def test_supported_parameter_edges_match_producer_and_no_synthetic_ttl_adjustment():
    rows = [
        {
            "kind": "NES",
            "interval_id": 11,
            "nSamples": 100000,
            "line_index": 9,
            "quote_precision": 16,
        },
        {"kind": "MST", "interval_id": 0, "nSamples": 20000, "maximum_metric_age_ms": 0},
        {
            "kind": "IVP",
            "interval_id": 0,
            "gammaFactor": 0.005,
            "deltaFactor": 10,
            "prominence": 1,
            "height": 0,
            "distance": 100,
            "max_divisions": 32768,
        },
    ]
    canonical = validate_configuration(form_configuration(rows))
    assert canonical["market_sentiment"][0]["maximum_metric_age_ms"] == 0
    assert canonical["volume_profile"][0]["max_divisions"] == 32768
    with pytest.raises(ValueError, match="interval 0..11"):
        form_configuration([{"kind": "STS", "interval_id": 12}])


def test_sts_and_ivp_fields_load_and_update_without_json(page):
    page.kind.setCurrentText("STS")
    page.market_inputs["STS"]["WOIBaseNSamples"].setValue(333)
    page.market_inputs["STS"]["WWOI"].setText("0.125")
    page.market_inputs["STS"]["quote_precision"].setValue(12)
    page.add_definition()
    index = len(page._definitions) - 1
    page.definition_table.setCurrentCell(index, 0)
    assert page.market_inputs["STS"]["WOIBaseNSamples"].value() == 333
    assert page.market_inputs["STS"]["WWOI"].text() == "0.125"
    page.market_inputs["STS"]["OSCScale"].setText("2.5")
    page.update_definition()
    assert page._definitions[index]["OSCScale"] == 2.5
    page.kind.setCurrentText("IVP")
    page.market_inputs["IVP"]["gammaFactor"].setText("0.025")
    page.market_inputs["IVP"]["max_divisions"].setValue(4096)
    page.add_definition()
    page.definition_table.setCurrentCell(len(page._definitions) - 1, 0)
    assert page.market_inputs["IVP"]["max_divisions"].value() == 4096
    assert page.market_inputs["IVP"]["gammaFactor"].text() == "0.025"
    assert not page.client.commands


def test_new_template_save_and_ca_copy_remain_explicit_with_market_groups(page):
    page.kind.setCurrentText("MST")
    page.add_definition()
    page.template_name.setText("Market template")
    page.save_template()
    page.client.complete()
    template = page.client.library.snapshot()["templates"][0]
    page.instance_name.setText("ETH analysis")
    page.symbol.setText("ETHUSDT")
    page.create_instance()
    page.client.complete()
    instance = page.client.library.snapshot()["instances"][0]
    assert (
        instance["configuration"]["market_sentiment"]
        == template["configuration"]["market_sentiment"]
    )
    page.template_list.setCurrentRow(0)
    selected = next(
        index for index, value in enumerate(page._definitions) if value["kind"] == "MST"
    )
    page.definition_table.setCurrentCell(selected, 0)
    page.samples.setValue(30)
    page.update_definition()
    page.save_template()
    page.client.complete()
    assert (
        page.client.library.snapshot()["instances"][0]["configuration"] == instance["configuration"]
    )


@pytest.mark.parametrize(
    "bad",
    [
        {"order_flow": [{"interval_id": 0, "analysis_code": "BAD"}]},
        {"market_sentiment": [{"interval_id": 0, "unknown": 1}]},
        {"short_term_scalar": "not a list"},
        {"volume_profile": [None]},
    ],
)
def test_bad_market_json_preserves_existing_form_and_original_input(page, bad):
    before = deepcopy(page._definitions)
    entered = json.dumps(bad)
    page.json_input.setPlainText(entered)
    page.apply_json()
    assert page._definitions == before and page.json_input.toPlainText() == entered
    assert not page.client.commands


def test_library_success_stops_loading_without_hiding_rejection_or_user_drafts(page):
    page.refresh()
    page.client.resultReceived.emit("analysis-library", page.client.library.snapshot())
    assert page.status.text() == "Core library refreshed"
    page.status.setText("REVISION_CONFLICT")
    page.client.resultReceived.emit("analysis-library", page.client.library.snapshot())
    assert page.status.text() == "REVISION_CONFLICT"
    page.template_name.setText("Draft")
    page.template_name.textEdited.emit("Draft")
    page.refresh()
    page.client.resultReceived.emit("analysis-library", page.client.library.snapshot())
    assert page.template_name.text() == "Draft" and page.status.text() == "Editing · not saved"


@pytest.mark.parametrize("width", [800, 1360])
def test_market_form_is_scrollable_and_collapsible_with_honest_data_and_scipy_notes(
    page, app, tmp_path, width
):
    page.resize(width, 900)
    page.setStyleSheet(stylesheet("light"))
    page.kind.setCurrentText("STS")
    page.tabs.setCurrentIndex(0)
    app.processEvents()
    scroll = page.tabs.widget(0).widget(1)
    scroll.verticalScrollBar().setValue(scroll.verticalScrollBar().maximum())
    app.processEvents()
    widgets = list(page.market_inputs["STS"].values())
    assert all(isinstance(widget, (QSpinBox, QLineEdit)) for widget in widgets)
    assert scroll.verticalScrollBar().maximum() > 0
    note = page.tr_text("unsupported")
    assert "do not subscribe automatically" in note and "NumPy/SciPy" in note
    page.grab().save(str(tmp_path / f"market-editor-{width}.png"))
    page.market_details.setChecked(False)
    assert page.market_content.isHidden()
    assert page._read_definition()["kind"] == "STS"
