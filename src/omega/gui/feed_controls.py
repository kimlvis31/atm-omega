"""Explicit opt-in DB-to-paper feed controls; pausing entries remains independent."""

from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QFormLayout,
    QGroupBox,
    QLabel,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)


class FeedControls(QGroupBox):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client = client
        self.server_time = None
        self.dirty = False
        self.pending = None
        self.attached = False
        self._edit_serial = 0
        self._submitted_serial = None
        self._unconfirmed = False
        self._operation_state = None
        self._operation_detail = ""
        self._feed_revision = -1
        self._epoch = None
        self._state = {}
        self._plans = {}
        self._pending_kind = None
        self.setCheckable(True)
        self.setChecked(False)
        layout = QVBoxLayout(self)
        panel = QWidget()
        form = QFormLayout(panel)
        self.enabled = QCheckBox()
        self.warmup = QSpinBox()
        self.warmup.setRange(1, 525600)
        self.warmup.setValue(240)
        self.hint = QLabel()
        self.hint.setWordWrap(True)
        self.warmup_label = QLabel()
        form.addRow(self.enabled)
        form.addRow(self.warmup_label, self.warmup)
        self.apply_button = QPushButton()
        self.apply_button.setEnabled(False)
        form.addRow(self.apply_button)
        self.continue_button = QPushButton()
        self.continue_button.setEnabled(False)
        self.continue_button.clicked.connect(self.continue_warmup)
        form.addRow(self.continue_button)
        form.addRow(self.hint)
        self.safety_hint = QLabel()
        self.safety_hint.setWordWrap(True)
        self.safety_hint.setTextFormat(Qt.TextFormat.PlainText)
        form.addRow(self.safety_hint)
        layout.addWidget(panel)
        panel.hide()
        self.toggled.connect(panel.setVisible)
        self.status = QLabel()
        self.status.setWordWrap(True)
        self.status.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self.status)
        self.plan_summary = QLabel()
        self.plan_summary.setWordWrap(True)
        self.plan_summary.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self.plan_summary)
        self.plan_details = QPlainTextEdit()
        self.plan_details.setReadOnly(True)
        self.plan_details.setMinimumHeight(80)
        self.plan_details.setMaximumHeight(165)
        self.plan_details.hide()
        layout.addWidget(self.plan_details)
        self.operation_status = QLabel()
        self.operation_status.setWordWrap(True)
        self.operation_status.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self.operation_status)
        self.resolve_button = QPushButton()
        self.resolve_button.setEnabled(False)
        self.resolve_button.clicked.connect(self.resolve_operation)
        layout.addWidget(self.resolve_button)
        self.apply_button.clicked.connect(self.apply)
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        self.enabled.toggled.connect(self._dirty)
        self.warmup.valueChanged.connect(self._dirty)
        self.set_presentation("light", "ko")

    def _dirty(self, *_):
        self._edit_serial += 1
        self.dirty = True
        self._update_controls()

    def update_state(self, state):
        self.server_time = state.get("core", {}).get("server_time_ms")
        epoch = state.get("core", {}).get("epoch")
        if epoch != self._epoch:
            self._feed_revision = -1
            self._epoch = epoch
        feed = state.get("feed", {})
        revision = feed.get("revision", -1)
        if type(revision) is int and revision < self._feed_revision:
            return
        if type(revision) is int:
            self._feed_revision = revision
        self._state = state
        plans = feed.get("warmup_plans", {})
        self._plans = plans if isinstance(plans, dict) else {}
        if not self.dirty and self.pending is None:
            self.enabled.blockSignals(True)
            self.warmup.blockSignals(True)
            self.enabled.setChecked(bool(feed.get("enabled", False)))
            self.warmup.setValue(feed.get("warmup_minutes", 240))
            self.enabled.blockSignals(False)
            self.warmup.blockSignals(False)
        self.status.setText(f"{feed.get('state', 'unknown')} · {feed.get('reason', '—')}")
        self._render_plans()
        self._update_controls()

    def apply(self):
        if not self.attached or type(self.server_time) is not int or self.pending is not None:
            return
        if self.enabled.isChecked() and self.warmup.value() > 2000 and self._safety_reason():
            self.safety_hint.setText(self._safety_reason())
            return
        self._submitted_serial = self._edit_serial
        self._pending_kind = "configure"
        self.pending = self.client.command(
            "paper.feed.configure",
            {
                "enabled": self.enabled.isChecked(),
                "start_ms": self.server_time // 60_000 * 60_000 - 60_000,
                "warmup_minutes": self.warmup.value(),
            },
        )
        self.apply_button.setEnabled(False)
        self._set_operation_status("waiting")

    def continue_warmup(self):
        if (
            not self.attached
            or type(self.server_time) is not int
            or self.pending is not None
            or not self._continuable_plans()
            or self._safety_reason()
        ):
            return
        self._submitted_serial = self._edit_serial
        self._pending_kind = "continue"
        # Do not send a new start/target or the current editable warmup length.
        self.pending = self.client.command("paper.feed.continue", {})
        self._set_operation_status("continuing")
        self._update_controls()

    def _unfinished_plans(self):
        return any(
            isinstance(plan, dict) and plan.get("state") in {"warming", "not_ready"}
            for plan in self._plans.values()
        )

    def _continuable_plans(self):
        return any(
            isinstance(plan, dict) and plan.get("state") == "warming"
            for plan in self._plans.values()
        )

    def _safety_reason(self):
        ko = self.language == "ko"
        core = self._state.get("core", {})
        if core.get("control_paused") is not True or core.get("paper_paused") is not True:
            return (
                "장기 분석 준비/이어하기 전에 전체 신규 진입을 정지하세요."
                if ko
                else "Pause global new entries before long warmup or continuation."
            )
        strategies = self._state.get("strategies")
        if not isinstance(strategies, list):
            return "전략 상태 미수신" if ko else "Strategy safety state has not been received."
        for row in strategies:
            try:
                quantity = Decimal(str(row["quantity"]))
                pending = row["has_pending_order"]
                if not quantity.is_finite() or quantity != 0 or pending is not False:
                    raise ValueError
            except (InvalidOperation, ValueError, TypeError, KeyError):
                return (
                    "열린 포지션·미체결이 있거나 상태가 미확인입니다. "
                    "장기 준비는 모든 전략이 flat일 때만 가능합니다."
                    if ko
                    else "An open position, pending order or unknown state prevents long warmup. "
                    "Every strategy must be flat."
                )
        return ""

    @staticmethod
    def _utc(value):
        if type(value) is not int:
            return "?"
        try:
            return datetime.fromtimestamp(value / 1000, UTC).strftime("%Y/%m/%d %H:%M UTC")
        except (ValueError, OverflowError, OSError):
            return "?"

    def _render_plans(self):
        records = [
            (str(identity)[:160], plan)
            for identity, plan in list(self._plans.items())[:100]
            if isinstance(plan, dict)
        ]
        self.plan_summary.setVisible(bool(records))
        self.plan_details.setVisible(bool(records))
        complete = sum(plan.get("state") == "complete" for _, plan in records)
        ko = self.language == "ko"
        self.plan_summary.setText(
            f"고정 범위 준비 {complete}/{len(records)} 완료 · 진입 재개는 별도"
            if ko
            else f"Fixed-range warmup: {complete}/{len(records)} complete · resume is separate"
        )
        if any(plan.get("state") == "not_ready" for _, plan in records):
            self.plan_summary.setText(
                self.plan_summary.text()
                + "\n"
                + (
                    "고정 범위 끝에서도 필수 지표가 미준비입니다. "
                    "더 길거나 새로운 준비 계획을 적용하세요. "
                    "이어하기는 범위를 늘리지 않습니다."
                    if ko
                    else "Required features remain unready at the fixed target. "
                    "Apply a longer/new plan. "
                    "Continue does not extend the range."
                )
            )
        lines = []
        for identity, plan in records:
            processed, total = plan.get("processed_minutes"), plan.get("total_minutes")
            progress = (
                f"{processed:,}/{total:,} min"
                if type(processed) is int and type(total) is int and 0 <= processed <= total
                else "?"
            )
            lines.append(
                f"{identity} · {str(plan.get('state', '?'))[:50]} · {progress}\n"
                f"  plan {str(plan.get('plan_id', '?'))[:128]}\n"
                f"  UTC [{self._utc(plan.get('start_ms'))}, "
                f"{self._utc(plan.get('target_end_ms'))})\n"
                f"  next {self._utc(plan.get('next_open_ms'))} · "
                f"alignment {str(plan.get('alignment_minutes', '?'))[:20]} min · "
                f"chunk {str(plan.get('chunk_minutes', '?'))[:20]} min"
            )
        text = "\n\n".join(lines)
        if text != self.plan_details.toPlainText():
            scroll = self.plan_details.verticalScrollBar().value()
            self.plan_details.setPlainText(text)
            self.plan_details.verticalScrollBar().setValue(scroll)

    def _result(self, tag, result):
        if self.pending and tag in {"command:" + self.pending, "operation:" + self.pending}:
            status = result.get("status")
            data = result.get("result")
            if (
                status == "completed"
                and isinstance(data, dict)
                and type(data.get("revision")) is int
            ):
                continued = self._pending_kind == "continue"
                if not continued:
                    self.dirty = self._edit_serial != self._submitted_serial
                if type(data.get("revision")) is int:
                    self._feed_revision = max(self._feed_revision, data["revision"])
                self.pending = None
                self._pending_kind = None
                self._unconfirmed = False
                self._set_operation_status(
                    "continued" if continued else "newer_draft" if self.dirty else "confirmed"
                )
            elif status == "rejected":
                self.pending = None
                self._pending_kind = None
                self._unconfirmed = False
                self._set_operation_status("rejected", result.get("code", ""))
            else:
                self._unconfirmed = True
                self._set_operation_status("unconfirmed")
            self._update_controls()

    def _failed(self, tag, _message):
        if self.pending and tag in {"command:" + self.pending, "operation:" + self.pending}:
            self._unconfirmed = True
            self._set_operation_status("unconfirmed")
            self._update_controls()

    def resolve_operation(self):
        if self.pending and self.attached and self._unconfirmed:
            self.client.request(
                "GET", "/api/operations/" + self.pending, tag="operation:" + self.pending
            )

    def _set_operation_status(self, state, detail=""):
        self._operation_state, self._operation_detail = state, str(detail)[:200]
        messages = {
            "waiting": ("공급 설정 적용 확인 중", "Waiting for feed configuration confirmation"),
            "continuing": (
                "기존 고정 준비 이어하기 확인 중",
                "Waiting for existing-plan continuation",
            ),
            "continued": (
                "기존 준비 이어하기 확인 · 편집 초안과 준비 완료는 별도",
                "Existing-plan continuation confirmed; draft and readiness are separate",
            ),
            "confirmed": (
                "공급 설정 저장 확인 · 데이터 준비와 별도",
                "Feed settings saved; data readiness is separate",
            ),
            "newer_draft": (
                "저장 확인 · 이후 수정한 초안 유지",
                "Saved; edits made after submission were preserved",
            ),
            "rejected": (
                "설정 적용 거절 · 초안 유지",
                "Feed configuration rejected; draft preserved",
            ),
            "unconfirmed": (
                "결과 미확인 · 재전송하지 않고 요청 상태를 확인하세요.",
                "Outcome unconfirmed; check the operation without reposting.",
            ),
        }
        text = messages[state][0 if self.language == "ko" else 1]
        self.operation_status.setText(text + (" · " + self._operation_detail if detail else ""))

    def set_attached(self, attached):
        if not attached:
            self.server_time = None
            if self.pending:
                self._unconfirmed = True
                self._set_operation_status("unconfirmed")
        self.attached = bool(attached)
        self._update_controls()

    def _update_controls(self):
        reason = self._safety_reason() if hasattr(self, "language") else ""
        long_start = self.enabled.isChecked() and self.warmup.value() > 2000
        self.apply_button.setEnabled(
            self.attached
            and type(self.server_time) is int
            and not self.pending
            and not (long_start and reason)
        )
        self.continue_button.setEnabled(
            self.attached
            and type(self.server_time) is int
            and not self.pending
            and self._continuable_plans()
            and not reason
        )
        self.continue_button.setToolTip(
            reason
            or (
                "고정 목표 범위 끝 · 새로운 준비 계획을 적용하세요."
                if self.language == "ko"
                else "Fixed target exhausted; apply a new preparation plan."
            )
            if self._unfinished_plans() and not self._continuable_plans()
            else reason
        )
        self.safety_hint.setText(reason if long_start or self._unfinished_plans() else "")
        self.resolve_button.setEnabled(self.attached and bool(self.pending) and self._unconfirmed)

    def set_presentation(self, _theme, language):
        self.language = language
        ko = language == "ko"
        self.setTitle("DB → 가상 거래 자동 공급" if ko else "DB → paper automatic feed")
        self.enabled.setText("확정 1분봉 자동 공급 사용" if ko else "Enable committed 1m delivery")
        self.warmup_label.setText("비거래 분석 준비 (분)" if ko else "Non-trading warmup (minutes)")
        self.apply_button.setText(
            "새 기준 시각으로 공급 설정 적용" if ko else "Apply using a new target time"
        )
        self.continue_button.setText(
            "기존 고정 준비 이어하기" if ko else "Continue the existing fixed-range warmup"
        )
        self.resolve_button.setText("요청 상태 확인" if ko else "Check operation status")
        if self._operation_state:
            self._set_operation_status(self._operation_state, self._operation_detail)
        self.hint.setText(
            "저장된 실제 관측을 사용합니다. 비거래 준비·신규 진입 정지는 별도입니다. "
            "필요 feature가 부족하면 진입을 차단합니다. 수동 봉과 동시에 사용할 수 없습니다."
            " 2,000분 초과는 전체 신규 진입 정지와 모든 전략의 포지션·미체결 없음이 필요합니다. "
            "적용은 서버 시각 기준 새 범위를 만들고, 이어하기는 기존 시작·목표 범위를 유지합니다. "
            "서버가 UTC 상위 봉 경계로 정렬하므로 실제 준비 길이가 입력보다 길 수 있습니다."
            if ko
            else "Committed observations; warmup never trades and entry pause is independent. "
            "Insufficient features block entries. Manual candle input is disabled."
            " Above 2,000 minutes: pause globally; "
            "all strategies must be flat without pending orders. "
            "Apply creates a new range; Continue preserves the original start and target. "
            "Server alignment to higher-bar UTC boundaries may extend the requested length."
        )
        if self._plans:
            self._render_plans()
        self._update_controls()
