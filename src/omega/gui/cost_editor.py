"""Structured opt-in scenario policy; no pricing, reservations or financial I/O."""

import json

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QFormLayout, QGroupBox, QLabel, QLineEdit, QSpinBox, QVBoxLayout

from omega.application.margin_cost import MarginCostPolicy
from omega.domain.errors import ValidationError

COST_SCOPE = "fixed_adverse_full_fill_delay_v1"


class CostEditor(QGroupBox):
    changed = Signal()

    def __init__(self, parent=None, *, validate_candidate=None, max_request_bytes=131072):
        super().__init__(parent)
        self.validate_candidate, self.max_request_bytes = validate_candidate, max_request_bytes
        self.supported, self.language, self._loading = False, "ko", False
        self.setCheckable(True)
        self.setChecked(False)
        layout = QVBoxLayout(self)
        self.body = QGroupBox()
        content = QVBoxLayout(self.body)
        self.note, self.identity, self.status = QLabel(), QLabel(), QLabel()
        for label in (self.note, self.identity, self.status):
            label.setWordWrap(True)
            label.setTextFormat(Qt.TextFormat.PlainText)
        content.addWidget(self.note)
        form = QFormLayout()
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.slippage, self.execution_bound, self.mark_drift = (QLineEdit("0") for _ in range(3))
        self.entry_delay, self.reduction_delay = QSpinBox(), QSpinBox()
        self._fields = []
        for ko, en, control in (
            ("불리한 가격 비율 (0..0.25)", "Adverse slippage ratio (0..0.25)", self.slippage),
            (
                "진입 체결가격 범위 비율 (0..0.5)",
                "Entry execution bound ratio (0..0.5)",
                self.execution_bound,
            ),
            (
                "관측 mark 변동 범위 비율 (0..0.5)",
                "Observed mark drift bound ratio (0..0.5)",
                self.mark_drift,
            ),
            ("신규 진입 지연 (ms)", "Entry delay (ms)", self.entry_delay),
            (
                "감소·보호 체결 지연 (ms)",
                "Reduction / protective fill delay (ms)",
                self.reduction_delay,
            ),
        ):
            label = QLabel()
            self._fields.append((label, ko, en))
            form.addRow(label, control)
            if isinstance(control, QLineEdit):
                control.setMaxLength(128)
                control.textChanged.connect(self._edited)
            else:
                control.setRange(0, 86400000)
                control.valueChanged.connect(self._edited)
        content.addLayout(form)
        content.addWidget(self.identity)
        content.addWidget(self.status)
        layout.addWidget(self.body)
        self.body.hide()
        self.toggled.connect(self._toggled)
        self.set_language("ko")

    def text(self, ko, en):
        return ko if self.language == "ko" else en

    def set_language(self, language):
        self.language = language
        self.setTitle(
            self.text("고급 비용 · 체결 시나리오", "Advanced costs · Execution scenario")
            + ("" if self.supported else self.text(" (코어 미지원)", " (unsupported core)"))
        )
        for label, ko, en in self._fields:
            label.setText(self.text(ko, en))
        self.note.setText(
            self.text(
                "기본 미적용. 모든 값 0도 별도 시나리오 모델입니다. 비율 0.01=1%. "
                "BUY raw×(1+s), SELL raw×(1−s) 후 LIMIT 가격 한도로 제한하며 "
                "미접촉 체결은 만들지 않습니다. "
                "원래 issue+지연을 다음 분 경계로 올리므로 1ms도 한 봉을 더 기다릴 수 있습니다. "
                "감소 지연>0은 손절·금융 정리 체결도 늦춥니다(보호 판단은 계속). "
                "EOD 미체결·노출은 유지합니다. 실제 venue 지연/가격충격·부분체결·"
                "tick 반올림·청산은 "
                "미지원입니다. NN 입력 대기와 별개이며 최종 price/fee 외 현금 차감은 없습니다.",
                "Off by default; all-zero values still select a separate scenario model. "
                "Ratio 0.01=1%. "
                "BUY raw×(1+s), SELL raw×(1−s), then cap at the LIMIT price; no invented touches. "
                "Original issue plus delay rounds up to a minute, so 1ms can add a bar. "
                "Reduction delay>0 also delays stop/financial-flatten fills, "
                "not protection decisions. "
                "EOD orders/exposure remain open. No measured venue latency/impact, partial fills, "
                "tick rounding or liquidation. NN input wait is separate; no extra cash debit.",
            )
        )
        self._render()

    def set_supported(self, supported):
        supported = supported is True
        if self.supported != supported:
            self.supported = supported
            self.set_language(self.language)

    def _raw(self):
        return {
            "schema_version": 1,
            "model": "linear_usdt_ohlcv_cost_v1",
            "slippage_rate": self.slippage.text(),
            "execution_price_bound_rate": self.execution_bound.text(),
            "mark_drift_bound_rate": self.mark_drift.text(),
            "entry_delay_ms": self.entry_delay.value(),
            "reduction_delay_ms": self.reduction_delay.value(),
            "limit_price_policy": "adverse_then_cap_v1",
        }

    @staticmethod
    def _parse(value):
        try:
            return MarginCostPolicy.parse(value)
        except ValidationError as error:
            raise ValueError(str(error)) from None

    def _render(self):
        try:
            policy = self._parse(self._raw())
        except ValueError as error:
            self.identity.clear()
            self.status.setText(str(error))
            return
        self.identity.setText(
            f"{policy.document()['model']} · adverse_then_cap_v1 · SHA256 {policy.sha256}"
        )
        self.status.setText(
            self.text(
                "연결 코어가 미지원입니다. 입력과 선택은 유지하며 제출은 차단됩니다.",
                "Unsupported core: inputs and selection are retained; submission is blocked.",
            )
            if self.isChecked() and not self.supported
            else ""
        )

    def _edited(self, *_args):
        if not self._loading:
            self._render()
            self.changed.emit()

    def _toggled(self, checked):
        self.body.setVisible(checked)
        self._edited()

    def document(self):
        if not self.isChecked():
            return None
        if not self.supported:
            raise ValueError("Connected core does not support this cost policy; selection retained")
        raw = self._raw()
        self._parse(raw)
        return raw

    def validate_document(self, value, *, request=None):
        if not self.supported:
            raise ValueError("Connected core does not support this cost policy; selection retained")
        raw = json.loads(json.dumps(value, allow_nan=False))
        self._parse(raw)
        candidate = (
            request
            if request is not None
            else (
                self.validate_candidate(raw)
                if self.validate_candidate is not None
                else {"cost_policy": raw}
            )
        )
        if type(candidate) is not dict or candidate.get("cost_policy") != raw:
            raise ValueError("Candidate request differs from the selected cost policy")
        if (
            len(
                json.dumps(
                    candidate, allow_nan=False, sort_keys=True, separators=(",", ":")
                ).encode()
            )
            > self.max_request_bytes
        ):
            raise ValueError("Full cost request exceeds capacity; previous draft preserved")
        return raw

    def load_document(self, value, *, request=None):
        raw = self.validate_document(value, request=request)
        self._loading = True
        try:
            for control, key in (
                (self.slippage, "slippage_rate"),
                (self.execution_bound, "execution_price_bound_rate"),
                (self.mark_drift, "mark_drift_bound_rate"),
            ):
                control.setText(raw[key])
            self.entry_delay.setValue(raw["entry_delay_ms"])
            self.reduction_delay.setValue(raw["reduction_delay_ms"])
            self.setChecked(True)
        finally:
            self._loading = False
        self._edited()

    def reset(self):
        self._loading = True
        try:
            self.setChecked(False)
            for control in (self.slippage, self.execution_bound, self.mark_drift):
                control.setText("0")
            self.entry_delay.setValue(0)
            self.reduction_delay.setValue(0)
        finally:
            self._loading = False
        self._edited()

    def can_submit(self):
        if not self.isChecked():
            return True
        if not self.supported:
            return False
        try:
            self._parse(self._raw())
        except ValueError:
            return False
        return True
