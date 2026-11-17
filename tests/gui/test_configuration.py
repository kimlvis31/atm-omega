"""Declarative configuration form contracts; advanced JSON remains authoritative."""

import importlib.util
import json
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None,
    reason="optional PySide6 GUI dependency is not installed",
)


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["omega-configuration-tests"])


@pytest.fixture
def editor(qt_app):
    from PySide6.QtWidgets import QPlainTextEdit

    from omega.gui.configuration import ConfigurationEditor

    document = QPlainTextEdit()
    view = ConfigurationEditor(document)
    document.setPlainText("{}")
    yield view
    view.close()
    document.close()
    view.deleteLater()
    document.deleteLater()
    qt_app.processEvents()


def tef_schema():
    # Read declarative package data without importing strategy/plugin implementations.
    manifest = Path(__file__).parents[2] / "src/omega/strategies/tef/manifest.json"
    return json.loads(manifest.read_text(encoding="utf-8"))["configuration_schema"]


def load(editor, schema, data):
    editor.document.setPlainText(json.dumps(data))
    editor.set_schema(schema)


def parsed(editor):
    return json.loads(editor.document.toPlainText())


def edit(editor, path, value):
    control, _ = editor.controls[path]
    control.setText(value)
    control.editingFinished.emit()


def test_tef_existing_values_and_single_field_merge_preserve_unknown_keys(editor):
    data = {
        "model": "BSCALPHA",
        "direction": "SHORT",
        "fullStopLossImmediate": "0.025",
        "stopTradeOnFSL": False,
        "parameters": {"HTF_Bias": 0.3, "unknown_parameter": {"source": "keep"}},
        "external_metadata": ["keep", 7],
    }
    load(editor, tef_schema(), data)
    assert editor.controls[("model",)][0].currentData() == "BSCALPHA"
    assert editor.controls[("direction",)][0].currentData() == "SHORT"
    assert editor.controls[("fullStopLossImmediate",)][0].text() == "2.5"
    assert editor.controls[("stopTradeOnFSL",)][0].isChecked() is False
    assert editor.controls[("parameters", "HTF_Bias")][0].text() == "0.3"
    assert parsed(editor) == data  # Building a form never materializes defaults.
    edit(editor, ("parameters", "HTF_Bias"), "-0.5")
    data["parameters"]["HTF_Bias"] = -0.5
    assert parsed(editor) == data


def test_tef_keyboard_model_change_discards_previous_parameters_only(editor, qt_app):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    load(
        editor,
        tef_schema(),
        {"model": "MMACDDEFAULT", "parameters": {"old": 0.8}, "metadata": "keep"},
    )
    model = editor.controls[("model",)][0]
    QTest.keyClick(model, Qt.Key.Key_Home)
    qt_app.processEvents()  # Rebuild after the combo's native key event has returned.
    assert parsed(editor) == {"model": "BSCALPHA", "metadata": "keep"}
    assert ("parameters", "HTF_Bias") in editor.controls
    assert ("parameters", "old") not in editor.controls


def test_repeated_keyboard_model_changes_rebuild_after_events_return(editor, qt_app):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    load(editor, tef_schema(), {"model": "MMACDDEFAULT", "metadata": "keep"})
    for key in (Qt.Key.Key_Home, Qt.Key.Key_End, Qt.Key.Key_Home, Qt.Key.Key_End):
        combo = editor.controls[("model",)][0]
        QTest.keyClick(combo, key)
        qt_app.processEvents()
        assert parsed(editor)["metadata"] == "keep"
        assert parsed(editor)["model"] in {"BSCALPHA", "SPDDEFAULT"}
        assert editor.controls[("model",)][0].currentData() == parsed(editor)["model"]


def test_nonmodel_enum_change_preserves_parameters(editor):
    load(editor, tef_schema(), {"model": "BSCALPHA", "parameters": {"HTF_Bias": 0.3}})
    combo = editor.controls[("direction",)][0]
    combo.setCurrentIndex(combo.findData("LONG"))
    assert parsed(editor)["parameters"] == {"HTF_Bias": 0.3}
    assert parsed(editor)["direction"] == "LONG"


def test_tef_nullable_fsl_and_boolean_user_events(editor):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    load(editor, tef_schema(), {"model": "BSCALPHA"})
    edit(editor, ("fullStopLossImmediate",), "1")
    assert parsed(editor)["fullStopLossImmediate"] == "0.01"
    edit(editor, ("fullStopLossImmediate",), "")
    assert parsed(editor)["fullStopLossImmediate"] is None
    checkbox = editor.controls[("postStopLossReentry",)][0]
    QTest.keyClick(checkbox, Qt.Key.Key_Space)
    assert parsed(editor)["postStopLossReentry"] is True


@pytest.mark.parametrize("value", ["-1.01", "1.01", "nan", "inf", "not-a-number"])
def test_invalid_numeric_draft_keeps_json_unchanged_and_explains_error(editor, value):
    data = {"model": "BSCALPHA", "parameters": {"HTF_Bias": 0.3}}
    load(editor, tef_schema(), data)
    edit(editor, ("parameters", "HTF_Bias"), value)
    assert parsed(editor) == data
    assert editor.controls[("parameters", "HTF_Bias")][0].text() == value
    assert editor.error.text()


def test_integer_range_boundaries_and_no_float_coercion(editor):
    schema = {"properties": {"bars": {"type": "integer", "minimum": 1, "maximum": 5}}}
    load(editor, schema, {"bars": 2, "unknown": True})
    edit(editor, ("bars",), "5")
    assert parsed(editor) == {"bars": 5, "unknown": True}
    for invalid in ("5.1", "0", "6"):
        edit(editor, ("bars",), invalid)
        assert parsed(editor)["bars"] == 5
        assert editor.error.text()


@pytest.mark.parametrize("draft", ['{"parameters":', "[]", "null"])
def test_invalid_or_nonobject_json_is_preserved_on_form_edit(editor, draft):
    load(editor, {"properties": {"count": {"type": "integer"}}}, {"count": 2})
    editor.document.setPlainText(draft)
    edit(editor, ("count",), "3")
    assert editor.document.toPlainText() == draft
    assert editor.error.text()


def test_external_json_updates_fields_without_rewriting_unknown_keys(editor):
    schema = {"properties": {"count": {"type": "integer"}, "on": {"type": "boolean"}}}
    load(editor, schema, {"count": 2, "on": False})
    editor.document.setPlainText('{"count": 8, "on": true, "extra": [1]}')
    assert editor.controls[("count",)][0].text() == "8"
    assert editor.controls[("on",)][0].isChecked()
    assert editor.document.toPlainText() == '{"count": 8, "on": true, "extra": [1]}'


def test_form_has_global_64_field_budget(editor):
    schema = {
        "properties": {
            group: {
                "type": "object",
                "properties": {f"p{i}": {"type": "number"} for i in range(50)},
            }
            for group in ("first", "second", "third")
        }
    }
    load(editor, schema, {"unknown": "keep"})
    assert len(editor.controls) == 64
    assert parsed(editor) == {"unknown": "keep"}


@pytest.mark.parametrize("nested", ["wrong", 5, [1], True])
def test_wrong_nested_document_types_do_not_crash_or_overwrite_json(editor, nested):
    data = {"parameters": nested}
    load(
        editor,
        {
            "properties": {
                "parameters": {"type": "object", "properties": {"threshold": {"type": "number"}}}
            }
        },
        data,
    )
    assert parsed(editor) == data
    if ("parameters", "threshold") in editor.controls:
        edit(editor, ("parameters", "threshold"), "0.5")
        assert parsed(editor) == data
        assert editor.error.text()


@pytest.mark.parametrize(
    "schema",
    [
        None,
        [],
        {"properties": None},
        {"properties": []},
        {"allOf": None},
        {"allOf": [None, {"if": None}, {"then": None}]},
        {"properties": {"wrong": None, "next": {"type": "string"}}},
        {"properties": {"enum": {"enum": None}}},
        {"properties": {"enum": {"enum": 42}}},
        {"properties": {"kind": {"type": {"wrong": "type"}}}},
    ],
)
def test_malformed_or_unsupported_schema_preserves_document(editor, schema):
    load(editor, schema, {"unknown": ["keep"]})
    assert parsed(editor) == {"unknown": ["keep"]}
    assert len(editor.controls) <= 64


def test_unknown_enum_value_is_not_misrepresented_as_first_option(editor):
    load(editor, {"properties": {"mode": {"enum": ["A", "B"]}}}, {"mode": "future"})
    control = editor.controls[("mode",)][0]
    assert control.currentIndex() == -1 or control.currentData() == "future"
    assert parsed(editor) == {"mode": "future"}


def test_explicit_null_is_distinct_from_missing_key_on_external_update(editor):
    schema = {"properties": {"stop": {"type": ["string", "null"], "default": "0.1"}}}
    load(editor, schema, {})
    assert editor.controls[("stop",)][0].text() == "0.1"
    editor.document.setPlainText('{"stop": null}')
    assert editor.controls[("stop",)][0].text() == ""
    assert parsed(editor) == {"stop": None}


def test_nullable_number_retains_numeric_json_type(editor):
    load(editor, {"properties": {"stop": {"type": ["number", "null"]}}}, {"stop": None})
    edit(editor, ("stop",), "0.25")
    assert parsed(editor) == {"stop": 0.25}
    edit(editor, ("stop",), "")
    assert parsed(editor) == {"stop": None}


def test_array_values_display_valid_json_and_preserve_types(editor):
    values = ["ema", True, None, 3]
    load(editor, {"properties": {"features": {"type": "array"}}}, {"features": values})
    control = editor.controls[("features",)][0]
    assert json.loads(control.text()) == values
    edit(editor, ("features",), '["price", false, 4]')
    assert parsed(editor) == {"features": ["price", False, 4]}
    editor.document.setPlainText('{"features": ["volume", true, null]}')
    assert json.loads(control.text()) == ["volume", True, None]


@pytest.mark.parametrize("draft", ['{"not": "array"}', "[", json.dumps(list(range(65)))])
def test_invalid_or_oversized_array_edits_preserve_document(editor, draft):
    load(editor, {"properties": {"features": {"type": "array"}}}, {"features": [1, 2]})
    edit(editor, ("features",), draft)
    assert parsed(editor) == {"features": [1, 2]}
    assert editor.error.text()


def test_schema_size_guard_preserves_advanced_document(editor):
    load(editor, {"description": "x" * 65537}, {"unknown": "keep"})
    assert not editor.controls
    assert parsed(editor) == {"unknown": "keep"}
