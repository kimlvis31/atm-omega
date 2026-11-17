"""Bounded form renderer for a declarative subset of module JSON Schema.

The core still validates configuration. No plugin code is imported by the GUI.
Unsupported schema features remain editable in the advanced JSON document.
"""

import json
import math
from copy import deepcopy
from decimal import Decimal, InvalidOperation

from PySide6.QtCore import QSignalBlocker, QTimer
from PySide6.QtWidgets import QCheckBox, QComboBox, QFormLayout, QLabel, QLineEdit, QWidget

_MISSING = object()


class ConfigurationEditor(QWidget):
    def __init__(self, document, parent=None):
        super().__init__(parent)
        self.document = document
        self.schema = {}
        self.controls = {}
        self._busy = False
        self._selector_values = {}
        self._rebuild_timer = QTimer(self)
        self._rebuild_timer.setSingleShot(True)
        self._rebuild_timer.timeout.connect(self._build)
        self.form = QFormLayout(self)
        self.form.setContentsMargins(0, 0, 0, 0)
        self.form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.note = QLabel()
        self.note.setWordWrap(True)
        self.note.setProperty("role", "muted")
        self.error = QLabel()
        self.error.setWordWrap(True)
        self.document.textChanged.connect(self._document_changed)

    def set_schema(self, schema):
        try:
            candidate = deepcopy(schema) if isinstance(schema, dict) else {}
            if len(json.dumps(candidate, allow_nan=False).encode("utf-8")) > 65_536:
                candidate = {}
        except (ValueError, TypeError, RecursionError):
            candidate = {}
        self.schema = candidate
        self._build()

    def _read(self):
        try:
            data = json.loads(self.document.toPlainText())
            return data if isinstance(data, dict) else None
        except (ValueError, TypeError):
            return None

    def _properties(self, data):
        raw = self.schema.get("properties", {})
        properties = dict(raw) if isinstance(raw, dict) else {}
        rules = self.schema.get("allOf", [])
        for rule in (rules if isinstance(rules, list) else [])[:32]:
            if not isinstance(rule, dict):
                continue
            condition_schema = rule.get("if", {})
            consequent = rule.get("then", {})
            if not isinstance(condition_schema, dict) or not isinstance(consequent, dict):
                continue
            conditions = condition_schema.get("properties", {})
            if not isinstance(conditions, dict):
                continue
            if conditions and all(
                isinstance(condition, dict)
                and data.get(key, self._default(properties.get(key))) == condition.get("const")
                for key, condition in conditions.items()
            ):
                additions = consequent.get("properties", {})
                if isinstance(additions, dict):
                    properties.update(additions)
        return properties

    @staticmethod
    def _default(definition):
        return definition.get("default") if isinstance(definition, dict) else None

    def _build(self):
        self._rebuild_timer.stop()
        self._busy = True
        while self.form.rowCount():
            self.form.removeRow(0)
        self.controls = {}
        data = self._read() or {}
        properties = self._properties(data)
        self._selector_values = {
            key: data.get(key, definition.get("default"))
            for key, definition in properties.items()
            if isinstance(definition, dict) and "enum" in definition
        }
        self.note = QLabel(str(self.schema.get("description", ""))[:1500])
        self.note.setWordWrap(True)
        self.note.setProperty("role", "muted")
        if self.note.text():
            self.form.addRow(self.note)
        self._add_fields(properties, data)
        self.error = QLabel()
        self.error.setWordWrap(True)
        self.form.addRow(self.error)
        self._busy = False

    def _add_fields(self, properties, data, prefix=()):
        if not isinstance(properties, dict):
            return
        if not isinstance(data, dict):
            data = {}
        for key, definition in list(properties.items())[:64]:
            if len(self.controls) >= 64:
                break
            if not isinstance(definition, dict):
                continue
            path = (*prefix, key)
            value = data.get(key, definition.get("default"))
            kind = definition.get("type", "string")
            if kind == "object" and not prefix:
                self._add_fields(definition.get("properties", {}), value or {}, path)
                continue
            if isinstance(definition.get("enum"), list):
                control = QComboBox()
                for option in definition["enum"][:64]:
                    control.addItem(str(option)[:100], option)
                self._select_enum(control, value)
                control.currentIndexChanged.connect(lambda _index, p=path: self._changed(p))
            elif kind == "boolean":
                control = QCheckBox()
                control.setChecked(value is True)
                control.toggled.connect(lambda _checked, p=path: self._changed(p))
            elif kind in ("string", "number", "integer", "array") or isinstance(kind, list):
                control = QLineEdit(self._display_value(value, definition))
                control.setMaxLength(1000)
                if isinstance(kind, list) and "null" in kind:
                    control.setPlaceholderText("—")
                control.editingFinished.connect(lambda p=path: self._changed(p))
            else:
                continue
            control.setToolTip(str(definition.get("description", key))[:1000])
            self.form.addRow(str(definition.get("title", key))[:100], control)
            self.controls[path] = control, definition

    def _value(self, control, definition):
        if isinstance(control, QComboBox):
            return control.currentData()
        if isinstance(control, QCheckBox):
            return control.isChecked()
        text = control.text().strip()
        kind = definition.get("type", "string")
        if not text and isinstance(kind, list) and "null" in kind:
            return None
        if isinstance(kind, list):
            kind = next((item for item in kind if item != "null"), "string")
        if definition.get("x-display-percent") is True:
            try:
                ratio = Decimal(text) / 100
                if not ratio.is_finite():
                    raise ValueError("Finite percentage required")
                text = format(ratio, "f")
            except InvalidOperation:
                raise ValueError("A numeric percentage is required") from None
        if kind == "array":
            value = json.loads(text)
            if not isinstance(value, list) or len(value) > 64:
                raise ValueError("A JSON array with at most 64 items is required")
            json.dumps(value, allow_nan=False)
            return value
        if kind in ("number", "integer"):
            value = int(text) if kind == "integer" else float(text)
            if not math.isfinite(value):
                raise ValueError("Finite number required")
            if value < definition.get("minimum", -math.inf) or value > definition.get(
                "maximum", math.inf
            ):
                raise ValueError("Value is outside the module's allowed range")
            return value
        return text

    @staticmethod
    def _display_value(value, definition=None):
        if value is None:
            return ""
        if definition and definition.get("x-display-percent") is True:
            try:
                scaled = Decimal(str(value)) * 100
                if scaled.is_finite():
                    return format(scaled.normalize(), "f")
            except InvalidOperation:
                pass
        return (
            json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else str(value)
        )

    @staticmethod
    def _select_enum(control, value):
        index = control.findData(value)
        if index < 0:
            control.addItem(f"? {str(value)[:80]}", value)
            index = control.count() - 1
        control.setCurrentIndex(index)

    def _changed(self, path):
        if self._busy:
            return
        data = self._read()
        if data is None:
            self.error.setText("Fix the advanced JSON document before editing this form.")
            return
        control, definition = self.controls[path]
        try:
            value = self._value(control, definition)
            target = data
            for key in path[:-1]:
                target = target.setdefault(key, {})
                if not isinstance(target, dict):
                    raise ValueError("Nested configuration must be an object")
            target[path[-1]] = value
            # A model change must not carry incompatible parameters from the previous model.
            if len(path) == 1 and "enum" in definition:
                rules = self.schema.get("allOf", [])
                for rule in (rules if isinstance(rules, list) else [])[:32]:
                    if not isinstance(rule, dict):
                        continue
                    condition = rule.get("if", {})
                    consequent = rule.get("then", {})
                    if not isinstance(condition, dict) or not isinstance(consequent, dict):
                        continue
                    conditions = condition.get("properties", {})
                    additions = consequent.get("properties", {})
                    if (
                        isinstance(conditions, dict)
                        and path[0] in conditions
                        and isinstance(additions, dict)
                    ):
                        for nested in additions:
                            data.pop(nested, None)
            self.document.setPlainText(json.dumps(data, ensure_ascii=False, indent=2))
            self.error.setText("")
        except (ValueError, TypeError, OverflowError) as error:
            self.error.setText(str(error))

    def _document_changed(self):
        if self._busy:
            return
        data = self._read()
        if data is None:
            return  # Preserve invalid drafts for manual repair.
        properties = self._properties(data)
        selectors = {
            key: data.get(key, definition.get("default"))
            for key, definition in properties.items()
            if isinstance(definition, dict) and "enum" in definition
        }
        if selectors != self._selector_values:
            # Never delete an emitting combo while Qt is still handling its key/mouse event.
            self._rebuild_timer.start(0)
            return
        for path, (control, definition) in self.controls.items():
            value = data
            for key in path:
                value = value.get(key, _MISSING) if isinstance(value, dict) else _MISSING
            if value is _MISSING:
                value = definition.get("default")
            with QSignalBlocker(control):
                if isinstance(control, QComboBox):
                    self._select_enum(control, value)
                elif isinstance(control, QCheckBox):
                    control.setChecked(value is True)
                elif not control.hasFocus():
                    control.setText(self._display_value(value, definition))
