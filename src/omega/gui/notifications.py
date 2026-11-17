"""Desktop sound, optional TTS and tray notifications outside trading execution."""

from PySide6.QtCore import QObject, QTimer
from PySide6.QtWidgets import QApplication, QMenu, QStyle, QSystemTrayIcon

from omega.gui.models import IncidentTracker


class NotificationController(QObject):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.tracker = IncidentTracker()
        self.sound = True
        self.voice = False
        self.language = "ko"
        self.speech = None
        self.tray = None
        self.last_voice = ""
        self.timer = QTimer(self)
        self.timer.setInterval(15_000)
        self.timer.timeout.connect(self.repeat_critical)
        self.timer.start()
        if QSystemTrayIcon.isSystemTrayAvailable():
            app = QApplication.instance()
            self.tray = QSystemTrayIcon(
                app.style().standardIcon(QStyle.StandardPixmap.SP_ComputerIcon), self
            )
            self.tray.setToolTip("ATM Omega · paper")
            self.menu = QMenu()
            show = self.menu.addAction("Omega")
            show.triggered.connect(parent.showNormal)
            self.tray.setContextMenu(self.menu)
            self.tray.activated.connect(lambda _reason: parent.showNormal())
            self.tray.show()

    def configure(self, *, sound: bool, voice: bool, language: str, repeat_s: int = 15) -> None:
        if not voice and self.speech is not None:
            self.speech.stop()
        self.sound, self.voice, self.language = sound, voice, language
        self.timer.setInterval(max(5, min(3600, repeat_s)) * 1000)
        if voice and self.speech is None:
            try:
                from PySide6.QtTextToSpeech import QTextToSpeech

                if QTextToSpeech.availableEngines():
                    self.speech = QTextToSpeech(self)
            except ImportError:
                self.speech = None

    def update(self, incidents: list[dict]) -> None:
        fresh = self.tracker.update(incidents)
        if not fresh:
            return
        critical = any(
            str(item.get("severity", "")).lower() in {"critical", "fatal"} for item in fresh
        )
        if self.sound:
            QApplication.beep()
        title = "Omega · CRITICAL" if critical else "Omega"
        # Avoid reading arbitrary payloads/account balances aloud or into OS notification previews.
        message = (
            "확인하지 않은 중요 알림이 있습니다. Omega 알림을 확인하세요."
            if self.language == "ko"
            else "An unacknowledged incident needs attention. Check Omega notifications."
        )
        if self.tray:
            icon = (
                QSystemTrayIcon.MessageIcon.Critical
                if critical
                else QSystemTrayIcon.MessageIcon.Information
            )
            self.tray.showMessage(title, message, icon, 10_000)
            self.tray.setIcon(
                QApplication.style().standardIcon(
                    QStyle.StandardPixmap.SP_MessageBoxCritical
                    if critical
                    else QStyle.StandardPixmap.SP_MessageBoxInformation
                )
            )
        self._speak(message)

    def repeat_critical(self) -> None:
        if not self.tracker.critical():
            return
        if self.sound:
            QApplication.beep()
        message = (
            "Omega 중요 알림이 아직 확인되지 않았습니다."
            if self.language == "ko"
            else "Omega has an unacknowledged critical incident."
        )
        self._speak(message)

    def _speak(self, message: str) -> None:
        if not self.voice or self.speech is None:
            return
        # Never accumulate an unbounded voice backlog behind repeated incidents.
        if self.speech.state().name == "Ready":
            self.speech.say(message)

    def close(self) -> None:
        self.timer.stop()
        if self.speech is not None:
            self.speech.stop()
        if self.tray is not None:
            self.tray.hide()
