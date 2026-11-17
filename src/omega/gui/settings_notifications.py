"""Structured notification settings; speech runs in a core-owned optional worker."""

from PySide6.QtCore import QSignalBlocker, Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

TEXT = {
    "sound": ("알림 소리", "Notification sound"),
    "speech": ("음성 알림 (TTS)", "Spoken notifications (TTS)"),
    "language": ("음성 언어", "Speech language"),
    "voice": ("설치된 음성", "Installed voice"),
    "rate": ("읽기 속도 (-1~1)", "Speech rate (-1 to 1)"),
    "volume": ("음성 볼륨", "Speech volume"),
    "repeat": ("미확인 critical 반복 간격 (초)", "Unacknowledged critical repeat (seconds)"),
    "apply": ("알림 설정 저장", "Save notification settings"),
    "reload": ("현재 설정 다시 읽기", "Reload current settings"),
    "probe": ("음성 엔진·목록 확인", "Check speech engine / voices"),
    "preview": ("저장된 설정으로 미리듣기", "Preview saved speech settings"),
    "hint": (
        "음성은 코어가 소유하므로 GUI 종료 후에도 유지됩니다. 설치된 언어·음성이 필요합니다."
        " 미리듣기는 소리 발생을 요청하며 거래 동작은 실행하지 않습니다.",
        "Speech belongs to the core and survives closing the GUI. An installed voice is required."
        " Preview requests sound only; it performs no trading action.",
    ),
    "default": ("언어에 맞는 기본 음성", "Default voice for selected language"),
    "unknown": ("엔진 상태 미확인", "Speech engine not checked"),
    "ready": ("음성 준비됨", "Speech ready"),
    "disabled": ("음성 알림 꺼짐", "Speech disabled"),
    "starting": ("음성 프로세스 준비 중", "Starting speech worker"),
    "speaking": ("발화 중", "Speaking"),
    "unavailable": ("음성 사용 불가", "Speech unavailable"),
    "busy": ("음성 처리 중", "Speech busy"),
    "saving": ("알림 설정 저장 확인 중", "Waiting for notification settings confirmation"),
    "saved": ("알림 설정 저장 확인", "Notification settings saved"),
    "newer_draft": (
        "저장 확인 · 이후 수정한 초안은 유지했습니다.",
        "Saved; edits made after submission were preserved.",
    ),
    "rejected": ("설정 저장 거절 · 초안 유지", "Settings rejected; draft preserved"),
    "uncertain": (
        "저장 결과 미확인 · 상단에서 요청 상태를 확인하세요.",
        "Save unconfirmed; check the operation in the header.",
    ),
}


class NotificationSettingsPage(QWidget):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client = client
        self.language = "ko"
        self.current = {}
        self.revision = 0
        self.dirty = False
        self.attached = False
        self.status_data = {}
        self._edit_serial = 0
        self._pending = None
        self._submitted_serial = None
        self._save_state = None
        self._save_detail = ""
        self.labels = []
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.sound, self.speech = QCheckBox(), QCheckBox()
        self.speech_language = QComboBox()
        for text, key in (("System", "auto"), ("한국어", "ko_KR"), ("English", "en_US")):
            self.speech_language.addItem(text, key)
        self.voice = QComboBox()
        self.rate, self.volume = QDoubleSpinBox(), QDoubleSpinBox()
        self.rate.setRange(-1, 1)
        self.rate.setSingleStep(0.1)
        self.volume.setRange(0, 1)
        self.volume.setSingleStep(0.1)
        self.repeat = QSpinBox()
        self.repeat.setRange(5, 3600)
        for key, widget in (
            ("sound", self.sound),
            ("speech", self.speech),
            ("language", self.speech_language),
            ("voice", self.voice),
            ("rate", self.rate),
            ("volume", self.volume),
            ("repeat", self.repeat),
        ):
            label = QLabel()
            self.labels.append((label, key))
            form.addRow(label, widget)
            signal = (
                widget.toggled
                if isinstance(widget, QCheckBox)
                else widget.currentIndexChanged
                if isinstance(widget, QComboBox)
                else widget.valueChanged
            )
            signal.connect(self._mark_dirty)
        layout.addLayout(form)
        controls = QHBoxLayout()
        self.apply_button, self.reload_button = QPushButton(), QPushButton()
        self.probe_button, self.preview_button = QPushButton(), QPushButton()
        for key, button in (
            ("apply", self.apply_button),
            ("reload", self.reload_button),
            ("probe", self.probe_button),
            ("preview", self.preview_button),
        ):
            self.labels.append((button, key))
            controls.addWidget(button)
        layout.addLayout(controls)
        self.save_status = QLabel()
        self.save_status.setWordWrap(True)
        self.save_status.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self.save_status)
        self.status = QLabel()
        self.status.setWordWrap(True)
        layout.addWidget(self.status)
        hint = QLabel()
        hint.setWordWrap(True)
        self.labels.append((hint, "hint"))
        layout.addWidget(hint)
        layout.addStretch()
        self.apply_button.clicked.connect(self.apply)
        self.reload_button.clicked.connect(self.reload)
        self.probe_button.clicked.connect(lambda: client.command("notifications.speech.probe", {}))
        self.preview_button.clicked.connect(
            lambda: client.command("notifications.speech.preview", {})
        )
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        self.set_presentation("light", self.language)
        self.set_attached(False)

    def tr(self, key):
        return TEXT[key][0 if self.language == "ko" else 1]

    def _mark_dirty(self, *_args):
        self._edit_serial += 1
        self.dirty = True
        self.set_attached(self.attached)

    def update_settings(self, settings):
        if settings.get("revision", 0) < self.current.get("revision", 0):
            return
        self.current = settings
        if not self.dirty and self._pending is None:
            self.reload()

    def reload(self):
        self.revision = self.current.get("revision", 0)
        values = self.current.get("notifications", {})
        widgets = (
            self.sound,
            self.speech,
            self.speech_language,
            self.voice,
            self.rate,
            self.volume,
            self.repeat,
        )
        blockers = [QSignalBlocker(widget) for widget in widgets]
        self.sound.setChecked(values.get("sound", True))
        self.speech.setChecked(values.get("speech", False))
        self.speech_language.setCurrentIndex(
            max(0, self.speech_language.findData(values.get("speech_language", "auto")))
        )
        self.rate.setValue(values.get("speech_rate", 0))
        self.volume.setValue(values.get("speech_volume", 0.8))
        self.repeat.setValue(values.get("repeat_critical_s", 30))
        self._voices(values.get("speech_voice", ""))
        del blockers
        self.dirty = False
        self.set_attached(self.attached)

    def _voices(self, selected):
        with QSignalBlocker(self.voice):
            self.voice.clear()
            self.voice.addItem(self.tr("default"), "")
            for item in self.status_data.get("voices", []):
                if isinstance(item, dict) and isinstance(item.get("id"), str):
                    self.voice.addItem(
                        f"{item.get('locale', '')} · {item.get('name', '')}", item["id"]
                    )
            index = self.voice.findData(selected)
            if index < 0:
                self.voice.addItem("Unavailable · " + str(selected), selected)
                index = self.voice.count() - 1
            self.voice.setCurrentIndex(index)

    def apply(self):
        if self._pending is not None or not self.attached or not self.current:
            return
        self._submitted_serial = self._edit_serial
        self._pending = self.client.command(
            "settings.patch",
            {
                "revision": self.revision,
                "changes": {
                    "notifications": {
                        "sound": self.sound.isChecked(),
                        "speech": self.speech.isChecked(),
                        "speech_language": self.speech_language.currentData(),
                        "speech_voice": self.voice.currentData(),
                        "speech_rate": self.rate.value(),
                        "speech_volume": self.volume.value(),
                        "repeat_critical_s": self.repeat.value(),
                    }
                },
            },
        )
        self._set_save_status("saving")
        self.set_attached(self.attached)

    def _set_save_status(self, key, detail=""):
        self._save_state, self._save_detail = key, str(detail)[:200]
        self.save_status.setText(self.tr(key) + (" · " + self._save_detail if detail else ""))

    def _result(self, tag, result):
        if not self._pending or tag not in {
            "command:" + self._pending,
            "operation:" + self._pending,
        }:
            return
        status, data = result.get("status"), result.get("result")
        if (
            status == "completed"
            and isinstance(data, dict)
            and isinstance(data.get("notifications"), dict)
        ):
            edited_after_submit = self._edit_serial != self._submitted_serial
            self._pending = None
            self.update_settings(data)
            self.revision = max(self.revision, data.get("revision", self.revision))
            self.dirty = edited_after_submit
            if not edited_after_submit:
                self.reload()
            self._set_save_status("newer_draft" if edited_after_submit else "saved")
        elif status == "rejected":
            self._pending = None
            self._set_save_status("rejected", result.get("code", ""))
        else:
            self._set_save_status("uncertain")
        self.set_attached(self.attached)

    def _failed(self, tag, _message):
        if self._pending and tag == "command:" + self._pending:
            self._set_save_status("uncertain")
            self.set_attached(self.attached)

    def update_speech(self, status):
        selected = self.voice.currentData() or ""
        if status.get("voices") != self.status_data.get("voices"):
            self.status_data = status
            self._voices(selected)
        else:
            self.status_data = status
        self.status.setText(
            self.tr(status.get("state") if status.get("state") in TEXT else "unknown")
            + " · "
            + str(status.get("engine") or "—")
            + (" · " + str(status["reason"]) if status.get("reason") else "")
        )

    def set_attached(self, attached):
        self.attached = bool(attached)
        self.apply_button.setEnabled(self.attached and bool(self.current) and self._pending is None)
        self.reload_button.setEnabled(bool(self.current) and self._pending is None)
        self.probe_button.setEnabled(self.attached)
        self.preview_button.setEnabled(self.attached and not self.dirty and self._pending is None)

    def set_presentation(self, _theme, language):
        self.language = language
        for widget, key in self.labels:
            widget.setText(self.tr(key))
        self._voices(self.voice.currentData() or "")
        self.update_speech(self.status_data)
        if self._save_state:
            self._set_save_status(self._save_state, self._save_detail)
