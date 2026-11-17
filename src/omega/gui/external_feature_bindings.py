"""Data-only selection of core-registered external inputs; no model or file IO."""

import re
import time
from copy import deepcopy

from PySide6.QtCore import QSignalBlocker, Qt, Signal
from PySide6.QtWidgets import QComboBox, QHBoxLayout, QLabel, QPushButton, QVBoxLayout, QWidget

from omega.domain.errors import ValidationError
from omega.domain.external_features import ExternalFeatureBinding

PATH = "/api/external-feature-bindings"


def validate_wait_ms(value):
    if type(value) is not int or not 0 <= value <= 30000:
        raise ValueError(
            "NN 결과 대기는 0~30000ms 정수입니다 / NN wait must be an integer in 0..30000ms"
        )
    return value


def required_fields(schema, configuration):
    """Resolve declarative inputs without importing a TS instance."""
    properties = schema.get("properties", {}) if type(schema) is dict else {}
    if type(properties) is not dict or type(configuration) is not dict:
        raise ValueError("External input metadata is unavailable")
    result = []
    for name, definition in properties.items():
        if type(definition) is not dict or "x-omega-external-feature" not in definition:
            continue
        annotation = definition["x-omega-external-feature"]
        if (
            type(annotation) is not dict
            or set(annotation) != {"namespace", "required"}
            or type(annotation["namespace"]) is not str
            or re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,31}", annotation["namespace"]) is None
            or type(annotation["required"]) is not bool
        ):
            raise ValueError("External input metadata is unsupported")
        field = configuration.get(name, definition.get("default"))
        if field is None and not annotation["required"]:
            continue
        if (
            type(field) is not str
            or len(field) > 128
            or re.fullmatch(
                re.escape(annotation["namespace"]) + r"\.[A-Za-z][A-Za-z0-9_.]{0,123}", field
            )
            is None
        ):
            raise ValueError("External input field differs from its declared namespace")
        result.append(field)
    if len(result) > 64 or len(set(result)) != len(result):
        raise ValueError("Duplicate or excessive required external fields")
    return tuple(result)


class ExternalFeatureBindings(QWidget):
    refreshRequested = Signal()

    def __init__(self, parent=None, *, clock=time.monotonic):
        super().__init__(parent)
        self.clock, self.epoch, self.received_at = clock, None, None
        self.bindings, self.instrument, self.required = {}, None, ()
        self.language = "ko"
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.note, self.selection = QLabel(), QComboBox()
        self.note.setWordWrap(True)
        self.note.setTextFormat(Qt.TextFormat.PlainText)
        self.selection.setEditable(False)
        self.selection.setMinimumContentsLength(18)
        self.selection.setSizeAdjustPolicy(
            QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        self.refresh_button = QPushButton()
        self.refresh_button.clicked.connect(self.refreshRequested)
        layout.addWidget(self.note)
        controls = QHBoxLayout()
        controls.addWidget(self.selection, 1)
        controls.addWidget(self.refresh_button)
        layout.addLayout(controls)
        self.set_presentation("ko")

    def invalidate(self):
        self.epoch, self.received_at, self.bindings = None, None, {}
        self._render()

    def apply_snapshot(self, document, epoch, *, requested_at=None):
        try:
            if (
                type(document) is not dict
                or document.get("core_epoch") != epoch
                or document.get("strategy_delivery_enabled") is not True
                or document.get("strategy_delivery_scope") != "virtual_margin_v2_opt_in"
                or document.get("live_orders_enabled") is not False
                or type(document.get("bindings")) is not list
                or len(document["bindings"]) > 128
            ):
                raise ValueError("Invalid registered input response")
            parsed = [ExternalFeatureBinding.parse(row) for row in document["bindings"]]
            bindings = {row.binding_sha256: row for row in parsed}
            if len(bindings) != len(parsed):
                raise ValueError("Duplicate registered input")
        except (ValueError, TypeError, KeyError, ValidationError):
            self.invalidate()
            raise ValueError("Cannot validate registered NN inputs") from None
        if self.epoch != epoch:
            self.invalidate()
        now = self.clock()
        self.epoch, self.received_at, self.bindings = (
            epoch,
            now if requested_at is None else min(now, requested_at),
            bindings,
        )
        self._render()

    def set_context(self, instrument, required):
        self.instrument, self.required = instrument, tuple(required)
        self.setVisible(bool(self.required))
        self._render()

    def _matches(self, binding):
        return binding.instrument.key == self.instrument and set(self.required) == set(
            binding.fields
        )

    def _render(self):
        previous = self.selection.currentData()
        with QSignalBlocker(self.selection):
            self.selection.clear()
            self.selection.addItem(
                (
                    "입력 연결 메타데이터 미지원 / Unsupported input metadata"
                    if self.required == ("<invalid external input metadata>",)
                    else "등록된 NN 입력을 선택하세요"
                    if self.language == "ko"
                    else "Select a registered NN input"
                ),
                None,
            )
            for identity, binding in self.bindings.items():
                if self._matches(binding):
                    self.selection.addItem(
                        f"{binding.instance_id} / {binding.stream} · {', '.join(binding.fields)}",
                        identity,
                    )
            index = self.selection.findData(previous) if previous else -1
            if index > 0:
                self.selection.setCurrentIndex(index)

    def set_presentation(self, language):
        self.language = language
        self.refresh_button.setText("입력 새로 고침" if language == "ko" else "Refresh inputs")
        self.note.setText(
            "NN 페이지 → 확정봉 추론에서 모델 활성화·스트림 초기화 후 입력을 새로 조회하세요. "
            "현재 종목·필수 필드가 맞는 등록 연결만 선택합니다. "
            "신호 미제공 시 신규 진입은 차단되고 "
            "기존 주문·로컬 보호는 유지됩니다."
            if language == "ko"
            else "NN page → Committed inference: activate the model and reset its stream, "
            "then refresh inputs here. Select a registered connection matching this instrument "
            "and required fields. "
            "Unavailable signals block new entries; accepted orders and local protection continue."
        )
        self._render()

    def documents(self, epoch, *, saved=None):
        if not self.required:
            return []
        if (
            self.epoch != epoch
            or self.received_at is None
            or not 0 <= self.clock() - self.received_at <= 10
        ):
            raise ValueError("NN 입력 목록을 새로 조회하세요 / Refresh registered NN inputs")
        if saved is None:
            identities = [self.selection.currentData()]
        else:
            if type(saved) is not list or len(saved) != 1:
                raise ValueError("Select one registered NN input")
            try:
                identities = [ExternalFeatureBinding.parse(saved[0]).binding_sha256]
            except ValidationError:
                raise ValueError("Invalid saved NN input") from None
        binding = self.bindings.get(identities[0])
        if binding is None or not self._matches(binding):
            raise ValueError(
                "종목·필수 필드가 맞는 NN 입력을 선택하세요 / Select a matching registered NN input"
            )
        return [deepcopy(binding.document())]

    def select_documents(self, documents):
        identity = (
            documents[0].get("binding_sha256") if type(documents) is list and documents else None
        )
        index = self.selection.findData(identity) if identity else -1
        self.selection.setCurrentIndex(index if index > 0 else 0)
