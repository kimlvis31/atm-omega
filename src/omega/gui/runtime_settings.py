"""Typed revisioned runtime settings using only the owner command API."""

# Imports
import re
from copy import deepcopy

from PySide6.QtCore import QSignalBlocker, Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from omega.domain.errors import ValidationError
from omega.runtime.mobile_origin import canonical_mobile_origin

# Fields are UI metadata. The core remains the authoritative validator.
FIELDS = {
    "logging": {
        "level": ("로그 수준", "Log level", ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")),
        "retention_days": ("보관 일수", "Retention days", (1, 3650)),
        "max_file_bytes": ("파일당 최대 bytes", "Maximum bytes per file", (4096, 100_000_000)),
    },
    "training": {
        "recording": ("학습 기록 수준", "Training recording", ("light", "standard", "detailed")),
        "optional_recording_failure": (
            "선택 상세 기록 실패",
            "Optional recording failure",
            ("continue", "stop"),
        ),
    },
    "simulation": {
        "backend": ("연산 장치", "Compute backend", ("auto", "cpu", "gpu")),
        "intrabar": ("봉 내부 경로", "Intrabar path", ("conservative", "interpolation", "random")),
        "seed": ("난수 seed (정확한 정수)", "Random seed (exact integer)", "seed"),
    },
    "execution": {
        "mode": ("실행 검증", "Execution validation", ("static", "strict")),
        "max_age_ms": ("자료 최대 나이 (ms)", "Maximum data age (ms)", (1, 86400000)),
        "apply_to_manual": ("수동 주문에도 적용", "Apply to manual orders", "bool"),
    },
    "analysis": {
        "input_storage_mb": ("입력 기록 용량 (MiB)", "Input recording quota (MiB)", (16, 16384)),
    },
    "mobile": {
        "https_origin": ("신뢰할 HTTPS 프록시 주소", "Trusted HTTPS proxy origin", "origin"),
    },
}
TITLES = {
    "logging": ("로깅", "Logging"),
    "training": ("학습 기록", "Training records"),
    "simulation": ("시뮬레이션", "Simulation"),
    "execution": ("실행 검증 정책", "Execution validation policy"),
    "analysis": ("분석 입력 기록", "Analysis input records"),
    "mobile": ("모바일 연결", "Mobile connection"),
}


def checked(value, spec):
    if spec == "origin":
        try:
            canonical_mobile_origin(value)
        except ValidationError:
            return False
        return True
    if spec == "seed":
        return type(value) is int and 0 <= value < 2**63
    if spec == "bool":
        return type(value) is bool
    if type(spec[0]) is int:
        return type(value) is int and spec[0] <= value <= spec[1]
    return type(value) is str and value in spec


class RuntimeSettingsPage(QWidget):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language = client, "ko"
        self.current, self.baseline = {}, {}
        self.revision, self.epoch = None, None
        self.attached, self.dirty = False, False
        self._serial, self._pending, self._submitted_serial = 0, None, None
        self._pending_epoch, self._uncertain = None, False
        self._submitted_revision, self._submitted_changes = None, {}
        self.editors, self.groups, self.labels, self.hints = {}, {}, [], {}
        root = QVBoxLayout(self)
        self.revision_label, self.status = QLabel(), QLabel()
        for label in (self.revision_label, self.status):
            label.setWordWrap(True)
            label.setTextFormat(Qt.TextFormat.PlainText)
        root.addWidget(self.revision_label)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        contents = QWidget()
        layout = QVBoxLayout(contents)
        for category, definitions in FIELDS.items():
            group = QGroupBox()
            self.groups[category] = group
            form = QFormLayout(group)
            form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
            for name, (ko, en, spec) in definitions.items():
                if isinstance(spec, str) and spec in {"seed", "origin"}:
                    widget = QLineEdit()
                    widget.setMaxLength(512 if spec == "origin" else 32)
                    signal = widget.textChanged
                elif spec == "bool":
                    widget = QCheckBox()
                    signal = widget.toggled
                elif type(spec[0]) is int:
                    widget = QSpinBox()
                    widget.setRange(*spec)
                    signal = widget.valueChanged
                else:
                    widget = QComboBox()
                    for option in spec:
                        widget.addItem(option, option)
                    signal = widget.currentIndexChanged
                label = QLabel()
                self.labels.append((label, ko, en))
                form.addRow(label, widget)
                self.editors[category, name] = widget
                widget.setEnabled(False)
                signal.connect(self._edited)
            hint = QLabel()
            hint.setWordWrap(True)
            hint.setTextFormat(Qt.TextFormat.PlainText)
            self.hints[category] = hint
            form.addRow(hint)
            layout.addWidget(group)
        layout.addStretch()
        scroll.setWidget(contents)
        root.addWidget(scroll, 1)
        self.scroll = scroll
        buttons = QGridLayout()
        self.apply_button, self.reload_button, self.check_button = (QPushButton() for _ in range(3))
        buttons.addWidget(self.apply_button, 0, 0)
        buttons.addWidget(self.reload_button, 0, 1)
        buttons.addWidget(self.check_button, 1, 0, 1, 2)
        root.addLayout(buttons)
        root.addWidget(self.status)
        self.apply_button.clicked.connect(self.apply)
        self.reload_button.clicked.connect(self.reload)
        self.check_button.clicked.connect(self.check_operation)
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        self.set_presentation("light", "ko")
        self._controls()

    def text(self, ko, en):
        return ko if self.language == "ko" else en

    def _supported(self, category, name, data):
        values = data.get(category)
        return (
            isinstance(values, dict)
            and name in values
            and checked(values[name], FIELDS[category][name][2])
        )

    def _edited(self, *_args):
        self._serial += 1
        self.dirty = True
        self._controls()

    def update_state(self, state):
        epoch = (state.get("core") or {}).get("epoch")
        if isinstance(epoch, str) and epoch and epoch != self.epoch:
            changed = self.epoch is not None
            self.epoch = epoch
            if changed:
                if self._pending:
                    self._uncertain = True
                    self.status.setText(
                        self.text(
                            "코어 변경 · 이전 저장 결과 미확인. "
                            "새 코어 값을 명시적으로 다시 읽으세요.",
                            "Core changed · previous save unconfirmed. "
                            "Explicitly reload the new core settings.",
                        )
                    )
                # Preserve the draft, but its revision belongs to the former owner.
                self.revision = None
                self.current = {}
        if isinstance(state.get("settings"), dict):
            self.update_settings(state["settings"])
        self._controls()

    def update_settings(self, settings):
        if (
            not isinstance(settings, dict)
            or type(settings.get("schema_version")) is not int
            or settings.get("schema_version") != 1
            or type(settings.get("revision")) is not int
            or settings["revision"] < 0
        ):
            return
        if self.current and settings["revision"] < self.current["revision"]:
            return
        self.current = deepcopy(settings)
        if not self.dirty and self._pending is None and self.revision is not None:
            self.reload()
        elif not self.baseline and self._pending is None:
            self.reload()
        self._controls()

    def _write(self, widget, value):
        with QSignalBlocker(widget):
            if isinstance(widget, QLineEdit):
                widget.setText(str(value))
            elif isinstance(widget, QCheckBox):
                widget.setChecked(value)
            elif isinstance(widget, QComboBox):
                widget.setCurrentIndex(widget.findData(value))
            else:
                widget.setValue(value)

    def reload(self):
        if not self.current or self._pending and self._pending_epoch == self.epoch:
            return
        old_unknown = self._pending is not None
        self._pending, self._pending_epoch, self._uncertain = None, None, False
        self.baseline = deepcopy(self.current)
        self.revision = self.current["revision"]
        for (category, name), widget in self.editors.items():
            if self._supported(category, name, self.current):
                self._write(widget, self.current[category][name])
        self.dirty = False
        if old_unknown:
            self.status.setText(
                self.text(
                    "새 코어 설정을 읽었습니다. 이전 저장 결과는 미확인입니다.",
                    "New core settings loaded; previous save outcome remains unconfirmed.",
                )
            )
        self._controls()

    def changes(self):
        result = {}
        for (category, name), widget in self.editors.items():
            if not self._supported(category, name, self.baseline) or not self._supported(
                category, name, self.current
            ):
                continue
            if isinstance(widget, QLineEdit):
                raw = widget.text().strip()
                if FIELDS[category][name][2] == "origin":
                    try:
                        value = canonical_mobile_origin(widget.text())
                    except ValidationError as error:
                        raise ValueError(str(error)) from None
                    if value != self.baseline[category][name]:
                        result.setdefault(category, {})[name] = value
                    continue
                if not re.fullmatch(r"[0-9]{1,19}", raw) or int(raw) >= 2**63:
                    raise ValueError(
                        self.text(
                            "seed는 0~9223372036854775807 정수여야 합니다.",
                            "Seed must be an integer in 0..9223372036854775807.",
                        )
                    )
                value = int(raw)
            elif isinstance(widget, QCheckBox):
                value = widget.isChecked()
            elif isinstance(widget, QComboBox):
                value = widget.currentData()
            else:
                value = widget.value()
            if value != self.baseline[category][name]:
                result.setdefault(category, {})[name] = value
        return result

    def apply(self):
        self._controls()
        if not self.apply_button.isEnabled():
            return
        try:
            changes = self.changes()
        except ValueError as error:
            self.status.setText(str(error))
            return
        if not changes:
            self.status.setText(self.text("변경된 값이 없습니다.", "No changed values."))
            return
        self._pending_epoch, self._submitted_serial = self.epoch, self._serial
        self._submitted_revision, self._submitted_changes = self.revision, deepcopy(changes)
        self._pending = self.client.command(
            "settings.patch", {"revision": self.revision, "changes": changes}
        )
        self._uncertain = False
        self.status.setText(self.text("설정 저장 확인 중", "Confirming settings save"))
        self._controls()

    def check_operation(self):
        if (
            self.attached
            and self._pending
            and self._uncertain
            and self._pending_epoch == self.epoch
        ):
            self.client.request(
                "GET", "/api/operations/" + self._pending, tag="operation:" + self._pending
            )

    def _result(self, tag, result):
        if (
            not self._pending
            or self._pending_epoch != self.epoch
            or tag not in {"command:" + self._pending, "operation:" + self._pending}
        ):
            return
        status, data = result.get("status"), result.get("result")
        confirmed = (
            status == "completed"
            and isinstance(data, dict)
            and type(data.get("schema_version")) is int
            and data.get("schema_version") == 1
            and type(data.get("revision")) is int
            and self._submitted_revision is not None
            and data["revision"] > self._submitted_revision
            and all(
                self._supported(category, name, data) and data[category][name] == value
                for category, fields in self._submitted_changes.items()
                for name, value in fields.items()
            )
        )
        if confirmed:
            edited_after = self._serial != self._submitted_serial
            self._pending, self._pending_epoch, self._uncertain = None, None, False
            if self.current and self.current["revision"] > data["revision"]:
                self.dirty = True
                self.status.setText(
                    self.text(
                        "저장됐지만 더 최신 설정이 있습니다. "
                        "초안을 유지했습니다. 다시 읽고 비교하세요.",
                        "Saved, but newer settings exist. Draft preserved; reload and compare.",
                    )
                )
            else:
                self.current = deepcopy(data)
                if edited_after:
                    self.baseline, self.revision = deepcopy(data), data["revision"]
                    self.dirty = True
                else:
                    self.reload()
                self.status.setText(
                    self.text(
                        "저장 확인 · 이후 편집한 초안 유지" if edited_after else "설정 저장 확인",
                        "Saved; later edits preserved" if edited_after else "Settings saved",
                    )
                )
        elif status == "rejected":
            self._pending, self._pending_epoch, self._uncertain = None, None, False
            self.status.setText(
                self.text(
                    "설정 저장 거절 · 초안 유지. 최신 설정을 다시 읽어 비교하세요.",
                    "Settings rejected; draft preserved. Reload current settings to compare.",
                )
            )
        else:
            self._uncertain = True
            self.status.setText(
                self.text(
                    "결과 미확인 · 같은 요청 상태만 조회합니다.",
                    "Outcome unconfirmed; check the same operation without reposting.",
                )
            )
        self._controls()

    def _failed(self, tag, _message):
        if self._pending and tag in {"command:" + self._pending, "operation:" + self._pending}:
            self._uncertain = True
            self.status.setText(
                self.text(
                    "저장 결과 미확인 · 새 저장 요청을 자동 전송하지 않습니다.",
                    "Save unconfirmed; no automatic new save request.",
                )
            )
            self._controls()

    def set_attached(self, attached):
        self.attached = bool(attached)
        if not attached and self._pending:
            self._uncertain = True
        self._controls()

    def _controls(self):
        for (category, name), widget in self.editors.items():
            supported = self._supported(category, name, self.current)
            widget.setEnabled(supported)
            widget.setToolTip(
                ""
                if supported
                else self.text(
                    "코어 값 미제공 또는 지원 범위 밖 · 전송하지 않음",
                    "Core value unavailable or unsupported; never submitted",
                )
            )
        self.apply_button.setEnabled(
            self.attached
            and bool(self.epoch)
            and self.revision is not None
            and self.dirty
            and self._pending is None
        )
        self.reload_button.setEnabled(
            bool(self.current) and (not self._pending or self._pending_epoch != self.epoch)
        )
        self.reload_button.setText(
            self.text("새 코어 값 읽기 (이전 결과 미확인)", "Reload new core (old outcome unknown)")
            if self._pending and self._pending_epoch != self.epoch
            else self.text("최신 값 다시 읽기 · 초안 버림", "Reload latest values · discard draft")
        )
        self.check_button.setEnabled(
            self.attached
            and bool(self._pending)
            and self._uncertain
            and self._pending_epoch == self.epoch
        )
        current = self.current.get("revision", "?")
        self.revision_label.setText(
            self.text("편집 기준", "Editing revision")
            + f" {self.revision if self.revision is not None else '?'} / "
            + self.text("코어 최신", "Latest core")
            + f" {current}"
        )

    def set_presentation(self, _theme, language):
        self.language = language if language in {"ko", "en"} else "ko"
        for label, ko, en in self.labels:
            label.setText(self.text(ko, en))
        for category, group in self.groups.items():
            group.setTitle(self.text(*TITLES[category]))
        self.hints["logging"].setText(
            self.text(
                "분류별/날짜별 로그 보관. 기간·파일 상한을 코어에서 적용합니다.",
                "Category/date logs use core-owned retention and file limits.",
            )
        )
        self.hints["mobile"].setText(
            self.text(
                "빈 값은 외부 주소를 비활성화합니다. 신뢰할 사설 HTTPS 프록시를 별도로 "
                "설정해야 합니다. 코어는 계속 127.0.0.1에서만 연결을 받습니다.",
                "Empty disables proxy access. Configure a trusted private HTTPS proxy separately; "
                "the core still listens on 127.0.0.1 only.",
            )
        )
        self.hints["training"].setText(
            self.text(
                "경량/표준/상세 기록과 선택 상세 기록 실패 정책입니다. "
                "핵심 기록의 안정성은 별개입니다.",
                "Light/standard/detailed recording and optional detail failure policy; "
                "essential recording safety is separate.",
            )
        )
        self.hints["simulation"].setText(
            self.text(
                "GPU 및 conservative 모드는 기능 지원 여부에 따라 실행이 거절될 수 있습니다. "
                "선택만으로 가속/체결 검증이 보장되지는 않습니다.",
                "GPU and conservative mode may be rejected when unsupported. "
                "Selection does not guarantee acceleration or validated fills.",
            )
        )
        self.hints["execution"].setText(
            self.text(
                "static은 가상 정적 제약이며 미검증 조건이 남을 수 있습니다. "
                "strict는 미지원 조건 시 신규 진입을 막습니다. "
                "이미 생성된 pending 주문의 정책은 고정돼 "
                "설정 변경으로 취소/수량 변경되지 않습니다.",
                "Static uses paper static constraints with unverified conditions. "
                "Strict blocks new entries for unsupported conditions. "
                "Existing pending policies are frozen; changing settings "
                "does not cancel or resize them.",
            )
        )
        self.hints["analysis"].setText(
            self.text(
                "참조 중인 입력은 자동 삭제하지 않습니다. "
                "용량을 낮춰 상한을 넘으면 신규 기록을 막을 수 있습니다.",
                "Referenced inputs are not automatically deleted. "
                "Lowering the quota may block new recordings while usage exceeds it.",
            )
        )
        self.apply_button.setText(self.text("변경된 설정 저장", "Save changed settings"))
        self.check_button.setText(
            self.text("미확인 저장 요청 상태 확인", "Check unconfirmed save operation")
        )
        self._controls()
