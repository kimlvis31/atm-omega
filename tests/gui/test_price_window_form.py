"""Saved SMA/WMA/BOL definitions through the structured owner-only analysis form."""

import json
import math
import os
from copy import deepcopy

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication, QComboBox, QLineEdit

from omega.analysis.catalog import validate_configuration
from omega.gui.analysis_page import AnalysisPage, configuration_rows, form_configuration
from omega.runtime.analysis_library import AnalysisLibrary


def definition(kind, **values):
    row = {"kind": kind, "interval_id": 0, "nSamples": 10, "line_index": 0, "price_precision": 8}
    if kind == "BOL":
        row.update(MAType="SMA", bandWidth=2.0)
    return row | values


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    return QApplication.instance() or QApplication(["price-window-form"])


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

    widget = AnalysisPage(Client())
    widget.set_presentation("light", "en")
    widget.apply_snapshot(widget.client.library.snapshot())
    widget.resize(800, 900)
    widget.show()
    app.processEvents()
    yield widget
    widget.close()
    widget.deleteLater()
    app.processEvents()


@pytest.mark.parametrize("kind", ["SMA", "WMA", "BOL"])
def test_structured_defaults_match_actual_catalog_without_new_overlay_claim(page, kind):
    page.kind.setCurrentText(kind)
    assert page.samples.value() == 10 and page.samples.maximum() == 20000
    assert page.samples.isEnabled() and page.precision.isEnabled() and page.line.isEnabled()
    page.add_definition()
    row = page._definitions[-1]
    assert row == definition(kind)
    encoded = form_configuration(page._definitions)
    assert validate_configuration(encoded) == encoded
    assert definition(kind) in configuration_rows(encoded)
    assert not page.json_input.isVisible()
    assert "MAIN chart rendering is separate" in page.support_hint.text()
    if kind == "BOL":
        combo = page.market_inputs["BOL"]["MAType"]
        assert isinstance(combo, QComboBox) and not combo.isEditable()
        assert [combo.itemText(i) for i in range(combo.count())] == ["SMA", "WMA", "EMA"]
        assert isinstance(page.market_inputs["BOL"]["bandWidth"], QLineEdit)


@pytest.mark.parametrize("ma_type", ["SMA", "WMA", "EMA"])
@pytest.mark.parametrize("width", [-3.25, -0.0, 0.0, 2.0, 0.123456789012345])
def test_bollinger_enum_and_signed_finite_width_roundtrip_without_clamping(ma_type, width):
    row = definition("BOL", MAType=ma_type, bandWidth=width)
    encoded = form_configuration([row])
    assert encoded["bollinger"][0]["bandWidth"] == width
    assert math.copysign(1, encoded["bollinger"][0]["bandWidth"]) == math.copysign(1, width)
    assert configuration_rows(validate_configuration(encoded)) == [row]


@pytest.mark.parametrize(
    "change",
    [
        {"MAType": "VWMA"},
        {"MAType": "sma"},
        {"MAType": True},
        {"MAType": ["SMA"]},
        {"MAType": {}},
        {"bandWidth": True},
        {"bandWidth": float("nan")},
        {"bandWidth": float("inf")},
        {"bandWidth": "-Infinity"},
        {"bandWidth": "1e999"},
        {"nSamples": True},
        {"nSamples": 2.0},
        {"nSamples": 1},
        {"nSamples": 20001},
        {"interval_id": True},
        {"interval_id": 12},
        {"line_index": -1},
        {"line_index": 10},
        {"price_precision": True},
        {"price_precision": 17},
    ],
)
def test_new_enum_and_scalar_rejections_are_explicit(change):
    with pytest.raises(ValueError):
        form_configuration([definition("BOL", **change)])


def test_average_group_carries_each_explicit_code_and_boundaries():
    rows = [definition("SMA", interval_id=11, nSamples=20000, line_index=9, price_precision=16)]
    rows.append(definition("WMA", nSamples=2, price_precision=0))
    encoded = form_configuration(rows)
    assert [row["analysis_code"] for row in encoded["window_averages"]] == ["SMA", "WMA"]
    assert configuration_rows(validate_configuration(encoded)) == sorted(
        rows, key=lambda row: (row["interval_id"], row["kind"], row["line_index"])
    )
    for invalid in ("EMA", True, [], None):
        changed = deepcopy(encoded)
        changed["window_averages"][0]["analysis_code"] = invalid
        with pytest.raises(ValueError):
            configuration_rows(changed)


def test_combined_price_window_capacity_is_preflighted():
    maximum = [definition("SMA", interval_id=i, nSamples=19997) for i in range(5)]
    assert validate_configuration(form_configuration(maximum))
    with pytest.raises(ValueError, match="100000"):
        form_configuration([definition("SMA", interval_id=i, nSamples=20000) for i in range(5)])
    with pytest.raises(ValueError, match="100000"):
        form_configuration([*maximum, definition("BOL")])
    rows = [
        definition("WMA", interval_id=i // 10, line_index=i % 10, nSamples=2) for i in range(64)
    ]
    form_configuration(rows)
    with pytest.raises(ValueError, match="64"):
        form_configuration([*rows, definition("BOL")])


def test_add_edit_save_and_load_use_actual_library_and_keep_selected_enum_values(page):
    for kind in ("SMA", "WMA", "BOL"):
        page.kind.setCurrentText(kind)
        page.add_definition()
    index = next(i for i, row in enumerate(page._definitions) if row["kind"] == "BOL")
    page.definition_table.selectRow(index)
    page.market_inputs["BOL"]["MAType"].setCurrentText("WMA")
    page.market_inputs["BOL"]["bandWidth"].setText("-2.75")
    page.samples.setValue(17)
    page.line.setValue(9)
    page.precision.setValue(4)
    page.interval.setCurrentIndex(page.interval.findData(11))
    page.update_definition()
    expected = definition(
        "BOL",
        interval_id=11,
        nSamples=17,
        line_index=9,
        price_precision=4,
        MAType="WMA",
        bandWidth=-2.75,
    )
    assert page._definitions[index] == expected
    page.template_name.setText("Price windows")
    page.save_template()
    page.client.complete()
    saved = page.client.library.snapshot()["templates"][0]
    saved_id = saved["id"]
    before = deepcopy(saved["configuration"])
    page.new_template()
    page.template_list.setCurrentRow(0)
    assert page._selected_template == saved_id
    assert form_configuration(page._definitions) == before
    index = next(i for i, row in enumerate(page._definitions) if row["kind"] == "BOL")
    page.definition_table.selectRow(index)
    assert page.market_inputs["BOL"]["MAType"].currentText() == "WMA"
    assert page.market_inputs["BOL"]["bandWidth"].text() == "-2.75"
    assert page.samples.value() == 17 and page.line.value() == 9 and page.precision.value() == 4
    page.market_inputs["BOL"]["MAType"].setCurrentText("EMA")
    page.market_inputs["BOL"]["bandWidth"].setText("0")
    page.update_definition()
    page.save_template()
    page.client.complete()
    updated = page.client.library.snapshot()["templates"][0]
    assert updated["id"] == saved_id
    assert updated["configuration"]["bollinger"][0]["MAType"] == "EMA"
    assert updated["configuration"]["bollinger"][0]["bandWidth"] == 0.0
    assert updated["configuration"]["window_averages"] == before["window_averages"]


@pytest.mark.parametrize("bad", [{"MAType": "other"}, {"bandWidth": "NaN"}, {"nSamples": True}])
def test_invalid_json_or_library_leaves_existing_draft_and_selection_intact_without_post(page, bad):
    page.kind.setCurrentText("SMA")
    page.add_definition()
    before, library = deepcopy(page._definitions), deepcopy(page.library)
    ids, commands = deepcopy(page._draft_ids), len(page.client.commands)
    invalid = {
        "bollinger": [
            {key: value for key, value in definition("BOL", **bad).items() if key != "kind"}
        ]
    }
    page.json_input.setPlainText(json.dumps(invalid))
    page.apply_json()
    assert page._definitions == before and page._draft_ids == ids
    packet = {
        "schema_version": 1,
        "revision": 2,
        "templates": [
            {"id": "invalid", "name": "invalid", "revision": 1, "configuration": invalid}
        ],
        "instances": [],
    }
    page.apply_snapshot(packet)
    assert page.library == library and page._definitions == before
    assert len(page.client.commands) == commands


def test_duplicate_or_invalid_band_edit_is_atomic(page):
    page.kind.setCurrentText("BOL")
    page.add_definition()
    before = deepcopy(page._definitions)
    page.add_definition()
    assert page._definitions == before and "Duplicate" in page.status.text()
    page.definition_table.selectRow(len(before) - 1)
    page.market_inputs["BOL"]["bandWidth"].setText("Infinity")
    page.update_definition()
    assert page._definitions == before
    assert page.client.commands == []


def test_legacy_ema_mmacd_wire_has_no_new_fields_or_empty_price_groups():
    old = {
        "moving_averages": [
            {
                "interval_id": 0,
                "ema_period": 20,
                "mmacd_periods": [12, 26],
                "signal_period": 9,
                "price_precision": 8,
            }
        ],
        "momentum": [],
    }

    def wire(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

    assert wire(form_configuration(configuration_rows(old))) == wire(old)
    assert not {"window_averages", "bollinger"} & form_configuration(configuration_rows(old)).keys()
