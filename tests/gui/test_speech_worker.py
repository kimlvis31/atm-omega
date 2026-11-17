"""Real Qt event delivery with an inert fake voice engine: no audible playback."""

import importlib.util
import json
from enum import Enum

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None,
    reason="optional Qt speech dependency is not installed",
)


@pytest.fixture(scope="module")
def qt_app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["inert-speech-tests"])


@pytest.fixture
def worker(qt_app):
    from PySide6.QtCore import QLocale, QObject, Signal

    from omega.gui.speech_worker import create_worker

    class State(Enum):
        Ready = 0
        Speaking = 1
        Error = 2

    class Voice:
        def __init__(self, name):
            self.value = name

        def name(self):
            return self.value

    class Engine(QObject):
        stateChanged = Signal(object)
        errorOccurred = Signal(object, str)
        engines = ["fake"]
        locales_by_engine = None
        broken_engines = set()

        def __init__(self, _name, parent):
            super().__init__(parent)
            self.engine_name = _name
            self.current_state = State.Error if _name in self.broken_engines else State.Ready
            self.locale = QLocale("en_US")
            self.texts, self.stops = [], 0
            self.rate = self.volume = None

        @classmethod
        def availableEngines(cls):
            return cls.engines

        def state(self):
            return self.current_state

        def availableLocales(self):
            if self.locales_by_engine is not None:
                return [QLocale(name) for name in self.locales_by_engine[self.engine_name]]
            return [QLocale("en_US"), QLocale("ko_KR")]

        def setLocale(self, locale):
            self.locale = locale

        def availableVoices(self):
            return [Voice("Korean" if self.locale.name() == "ko_KR" else "English")]

        def setVoice(self, voice):
            self.voice = voice

        def setRate(self, rate):
            self.rate = rate

        def setVolume(self, volume):
            self.volume = volume

        def stop(self):
            self.stops += 1
            self.current_state = State.Ready
            self.stateChanged.emit(self.current_state)

        def say(self, text):
            self.texts.append(text)
            self.current_state = State.Speaking
            self.stateChanged.emit(self.current_state)

    reports = []
    value = create_worker(qt_app, reports.append, engine_class=Engine)
    value.reports, value.engine_class, value.states = reports, Engine, State
    yield value
    value.deleteLater()
    qt_app.processEvents()


def preferences(enabled=True, **kwargs):
    from omega.runtime.speech import speech_preferences

    return speech_preferences({"speech": enabled, "speech_language": "en_US", **kwargs})


def test_injected_engine_needs_no_native_speech_library(worker, qt_app, monkeypatch):
    import builtins

    from omega.gui.speech_worker import create_worker

    original_import = builtins.__import__

    def without_speech(name, *args, **kwargs):
        if name == "PySide6.QtTextToSpeech":
            raise ImportError("OS speech dependency unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_speech)
    reports = []
    other = create_worker(qt_app, reports.append, engine_class=worker.engine_class)
    try:
        configure(other)
        assert reports[-1]["state"] == "ready"
    finally:
        other.deleteLater()
        qt_app.processEvents()


def test_native_worker_missing_os_library_reports_unavailable(monkeypatch, capsys):
    import builtins

    from omega.gui.speech_worker import main

    original_import = builtins.__import__

    def without_speech(name, *args, **kwargs):
        if name == "PySide6.QtTextToSpeech":
            raise ImportError("OS speech dependency unavailable")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_speech)
    assert main() == 2
    status = json.loads(capsys.readouterr().out)
    assert status == {"state": "unavailable", "reason": "qt_speech_not_installed", "voices": []}


def configure(worker, generation=1, **kwargs):
    worker.command(
        {"kind": "configure", "generation": generation, "preferences": preferences(**kwargs)}
    )


def test_bounded_mailbox_emits_one_wakeup_and_latest_disable_revokes_all_queued_speech(
    worker, qt_app
):
    worker.submit({"kind": "preview", "generation": 1, "preferences": preferences()})
    for _ in range(1000):
        worker.submit({"kind": "say", "generation": 1, "critical": True, "repeat": True})
    assert len(worker.mailbox.queue) == 8
    assert worker.mailbox.scheduled
    worker.submit({"kind": "configure", "generation": 2, "preferences": preferences(False)})
    assert len(worker.mailbox.queue) == 1
    qt_app.processEvents()
    assert worker.engine is None
    assert any(report["state"] == "disabled" for report in worker.reports)


def test_fixed_announcement_uses_selected_locale_and_rejects_old_generation(worker):
    configure(worker, speech_language="ko_KR", speech_rate=0.4, speech_volume=0)
    worker.command({"kind": "say", "generation": 0, "critical": True, "repeat": False})
    assert worker.engine is None or not worker.engine.texts
    worker.command({"kind": "say", "generation": 1, "critical": True, "repeat": False})
    assert worker.engine.texts == ["확인이 필요한 중요한 알림이 있습니다. Omega 알림을 확인하세요."]
    assert worker.engine.rate == 0.4 and worker.engine.volume == 0
    assert worker.reports[-1]["state"] == "speaking"


def test_explicit_muted_preview_can_run_while_automatic_speech_is_off(worker):
    worker.command(
        {"kind": "preview", "generation": 0, "preferences": preferences(False, speech_volume=0)}
    )
    assert len(worker.engine.texts) == 1
    assert worker.engine.volume == 0
    assert worker.reports[-1]["enabled"] is False


def test_no_installed_engine_reports_unavailable_without_ready_or_say(worker):
    worker.engine_class.engines = []
    configure(worker)
    assert worker.engine is None
    assert worker.reports[-1]["state"] == "unavailable"
    assert worker.reports[-1]["reason"] == "no_speech_engine"


def test_missing_language_or_voice_conflict_does_not_speak(worker):
    configure(worker, speech_voice="fake|ko_KR|Korean")
    assert worker.reports[-1]["reason"] == "voice_language_mismatch"
    worker.command({"kind": "say", "generation": 1, "critical": True, "repeat": False})
    assert worker.engine is None or not worker.engine.texts
    configure(worker, generation=2, speech_voice="fake|en_US|Absent")
    assert worker.reports[-1]["reason"] == "selected_voice_or_language_unavailable"


def test_old_engine_callback_does_not_corrupt_new_engine_state(worker):
    configure(worker)
    old = worker.engine
    old.current_state = worker.states.Error
    configure(worker, generation=2)
    assert worker.engine is not old
    worker._error(old, None, "private engine failure")
    assert worker.reason is None
    assert worker.reports[-1]["state"] == "ready"


def test_auto_selection_checks_second_engine_and_catalog_covers_both_engines(worker):
    worker.engine_class.engines = ["korean-only", "english"]
    worker.engine_class.locales_by_engine = {"korean-only": ["ko_KR"], "english": ["en_US"]}
    configure(worker)
    assert worker.engine_name == "english"
    assert worker.reports[-1]["state"] == "ready"
    assert {voice["id"].split("|", 1)[0] for voice in worker.voices} == {"korean-only", "english"}
    assert worker.voices[0]["id"] == "english|en_US|English"


def test_explicit_voice_never_falls_back_to_another_installed_engine(worker):
    worker.engine_class.engines = ["korean-only", "english"]
    worker.engine_class.locales_by_engine = {"korean-only": ["ko_KR"], "english": ["en_US"]}
    configure(worker, speech_voice="korean-only|en_US|Absent")
    assert worker.reason == "selected_voice_or_language_unavailable"
    assert worker.engine is None
    configure(worker, generation=2, speech_voice="missing|en_US|English")
    assert worker.reason == "selected_engine_unavailable"
    assert worker.engine is None


def test_erroring_first_engine_does_not_hide_working_second_engine(worker):
    worker.engine_class.engines = ["broken", "english"]
    worker.engine_class.broken_engines = {"broken"}
    worker.engine_class.locales_by_engine = {"broken": ["en_US"], "english": ["en_US"]}
    configure(worker)
    assert worker.engine_name == "english" and worker.reason is None


def test_invalid_command_and_oversized_catalog_are_bounded():
    from omega.gui.speech_worker import bounded_report, command_message

    with pytest.raises(ValueError):
        command_message(
            {"kind": "say", "generation": 1, "critical": True, "repeat": False, "text": "private"}
        )
    with pytest.raises(ValueError):
        command_message({"kind": "say", "generation": True, "critical": True, "repeat": False})
    voices = [{"id": "한" * 256, "name": "한" * 256, "locale": "ko_KR"} for _ in range(128)]
    report = bounded_report({"state": "ready", "engine": "fake", "voices": voices})
    assert len(json.dumps(report, ensure_ascii=True).encode()) <= 60000
    assert len(report["voices"]) < 128


@pytest.mark.parametrize("locale_name,expected", [("en_US", "ready"), ("fr_FR", "unavailable")])
def test_auto_uses_system_language_or_reports_unsupported_instead_of_korean_fallback(
    worker, qt_app, locale_name, expected
):
    from PySide6.QtCore import QLocale

    from omega.gui.speech_worker import create_worker

    class Locale:
        def __new__(cls, value):
            return QLocale(value)

        @staticmethod
        def system():
            return QLocale(locale_name)

    reports = []
    other = create_worker(
        qt_app, reports.append, engine_class=worker.engine_class, locale_class=Locale
    )
    try:
        configure(other, speech_language="auto")
        assert reports[-1]["state"] == expected
        other.command({"kind": "say", "generation": 1, "critical": False, "repeat": False})
        if expected == "ready":
            assert other.engine.texts == ["Omega has a new notification."]
        else:
            assert reports[-1]["reason"] == "unsupported_system_language"
            assert other.engine is None or not other.engine.texts
    finally:
        other.deleteLater()
        qt_app.processEvents()
