"""Optional Qt speech worker: one bounded mailbox, one Qt wake event, fixed phrases."""

import json
import sys
import threading
from collections import deque

from omega.runtime.speech import announcement, speech_preferences, speech_status


def command_message(value):
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("invalid command")
    kind = value.get("kind")
    fields = {
        "configure": {"preferences"},
        "preview": {"preferences"},
        "probe": set(),
        "say": {"critical", "repeat"},
    }
    if kind not in fields or set(value) != fields[kind] | {"kind", "generation"}:
        raise ValueError("invalid command")
    if type(value["generation"]) is not int or not 0 <= value["generation"] < 2**63:
        raise ValueError("invalid generation")
    result = dict(value)
    if kind in {"configure", "preview"}:
        result["preferences"] = speech_preferences(value["preferences"])
    if kind == "say" and any(type(value[key]) is not bool for key in ("critical", "repeat")):
        raise ValueError("invalid speech flags")
    return result


def bounded_report(status):
    status = speech_status(status)
    while len(json.dumps(status, ensure_ascii=True, allow_nan=False).encode("utf-8")) > 60000:
        status["voices"].pop()
    return status


class Mailbox:
    """The reader emits at most one pending wake, never an unbounded Qt event per line."""

    def __init__(self):
        self.lock = threading.Lock()
        self.queue = deque()
        self.scheduled = self.overflow = self.closed = False
        self.generation = -1

    def put(self, message):
        with self.lock:
            if self.closed:
                return False, False
            if message is None:
                self.queue.clear()
                self.queue.append(None)
                self.closed = True
            elif message["generation"] < self.generation:
                return False, False
            elif message["kind"] in {"configure", "preview"}:
                self.generation = message["generation"]
                self.queue.clear()
                self.queue.append(message)
            elif message["kind"] == "probe" and any(
                item and item["kind"] == "probe" for item in self.queue
            ):
                return True, False
            elif len(self.queue) >= 8:
                self.overflow = True
                return False, False
            else:
                self.queue.append(message)
            wake = not self.scheduled
            self.scheduled = True
            return True, wake

    def take(self):
        with self.lock:
            if self.queue:
                return True, self.queue.popleft()
            return False, None

    def finish(self):
        with self.lock:
            again = bool(self.queue)
            self.scheduled = again
            overflow, self.overflow = self.overflow, False
            return again, overflow


def create_worker(app, emit, *, engine_class=None, locale_class=None):
    """Dependency injection permits Qt-event-loop tests without invoking an OS voice."""
    from PySide6.QtCore import QLocale, QObject, Qt, Signal

    if engine_class is None:
        from PySide6.QtTextToSpeech import QTextToSpeech

        engine_class = QTextToSpeech
    locale_class = locale_class or QLocale

    class Worker(QObject):
        received = Signal()

        def __init__(self):
            super().__init__()
            self.mailbox = Mailbox()
            self.engine = self.engine_name = None
            self.voices = []
            self.preferences = speech_preferences({})
            self.generation = -1
            self.reason = None
            self.setting_up = False
            self.spoken_language = "en_US"
            self.received.connect(self.drain, Qt.ConnectionType.QueuedConnection)

        def submit(self, message):
            try:
                message = command_message(message)
            except (ValueError, TypeError, KeyError):
                # Invalid IPC terminates the worker; no engine details or input are echoed.
                message = None
            accepted, wake = self.mailbox.put(message)
            if wake:
                self.received.emit()
            return accepted

        def drain(self):
            for _ in range(8):
                present, message = self.mailbox.take()
                if not present:
                    break
                self.command(message)
                if message is None:
                    break
            again, overflow = self.mailbox.finish()
            if overflow:
                self.status(override="busy", reason="speech_queue_full")
            if again:
                self.received.emit()

        def status(self, *, override=None, reason=None):
            if self.setting_up:
                return
            state = self.engine.state().name if self.engine else None
            if state == "Error" and self.reason is None:
                self.reason = "speech_engine_error"
            actual = (
                "unavailable"
                if self.reason
                else "speaking"
                if state == "Speaking"
                else "busy"
                if state not in {None, "Ready"}
                else "disabled"
                if not self.preferences["speech"]
                else "ready"
                if self.engine
                else "unavailable"
            )
            emit(
                bounded_report(
                    {
                        "state": override or actual,
                        "reason": reason or self.reason,
                        "engine": self.engine_name,
                        "voices": self.voices,
                        "enabled": self.preferences["speech"],
                        "generation": max(0, self.generation),
                    }
                )
            )

        def _state_changed(self, engine, *_):
            if self.engine is engine:
                self.status()

        def _error(self, engine, *_):
            if self.engine is engine:
                self.reason = "speech_engine_error"
                self.status()

        def setup(self, preferences, *, force=False):
            self.setting_up = True
            old, old_name = self.engine, self.engine_name
            had_error = old is not None and old.state().name == "Error"
            candidates, choices = [], []
            self.preferences, self.reason = preferences, None
            try:
                if old:
                    old.stop()
                if not preferences["speech"] and not force:
                    return
                self.engine = self.engine_name = None
                available = list(dict.fromkeys(engine_class.availableEngines()))
                available = [
                    name
                    for name in available
                    if isinstance(name, str)
                    and 0 < len(name) <= 128
                    and all(c.isprintable() for c in name)
                ]
                self.voices = []
                if not available:
                    self.reason = "no_speech_engine"
                    return
                voice_id = preferences["speech_voice"]
                requested = voice_id.split("|", 1)[0] if voice_id else None
                engines = available[:16]
                if requested in available and requested not in engines:
                    engines = [requested, *engines[:15]]
                # Enumerate every bounded installed backend. The first backend is
                # not evidence that the requested language is absent elsewhere.
                for name in engines:
                    candidate = None
                    try:
                        candidate = (
                            old if name == old_name and not had_error else engine_class(name, self)
                        )
                        candidates.append(candidate)
                        if candidate.state().name == "Error":
                            continue
                        count = 0
                        for locale in candidate.availableLocales()[:64]:
                            candidate.setLocale(locale)
                            for voice in candidate.availableVoices()[:128]:
                                key = f"{name}|{locale.name()}|{voice.name()}"
                                if (
                                    len(key) > 256
                                    or len(voice.name()) > 256
                                    or not all(c.isprintable() for c in key)
                                ):
                                    continue
                                record = {"id": key, "name": voice.name(), "locale": locale.name()}
                                choices.append((record, name, candidate, locale, voice))
                                count += 1
                                if count >= 128:
                                    break
                            if count >= 128:
                                break
                    except (RuntimeError, ValueError, TypeError, AttributeError):
                        continue
                # Preserve a chosen voice at the head if the catalog is truncated.
                self.voices = [choice[0] for choice in choices[:128]]
                if requested is not None and requested not in available:
                    self.reason = "selected_engine_unavailable"
                    return
                language = preferences["speech_language"]
                target = locale_class.system() if language == "auto" else locale_class(language)
                if target.name().split("_", 1)[0] not in {"ko", "en"}:
                    self.reason = "unsupported_system_language"
                    return
                matches = [
                    choice
                    for choice in choices
                    if (
                        choice[0]["id"] == voice_id
                        if voice_id
                        else choice[3].language() == target.language()
                    )
                ]
                if not voice_id:
                    matches.sort(key=lambda choice: choice[3].name() != target.name())
                if not matches:
                    self.reason = "selected_voice_or_language_unavailable"
                    return
                if voice_id and matches[0][3].language() != target.language():
                    self.reason = "voice_language_mismatch"
                    return
                self.reason = "speech_engine_error"
                for record, name, candidate, locale, voice in matches:
                    self.engine, self.engine_name = candidate, name
                    if not getattr(candidate, "_omega_handlers_connected", False):
                        candidate.stateChanged.connect(
                            lambda *_, engine=candidate: self._state_changed(engine)
                        )
                        candidate.errorOccurred.connect(
                            lambda *_, engine=candidate: self._error(engine)
                        )
                        candidate._omega_handlers_connected = True
                    self.reason = None
                    candidate.setLocale(locale)
                    candidate.setVoice(voice)
                    candidate.setRate(preferences["speech_rate"])
                    candidate.setVolume(preferences["speech_volume"])
                    if candidate.state().name == "Error":
                        self.reason = "speech_engine_error"
                        if voice_id:
                            break
                        continue
                    self.spoken_language = locale.name()
                    self.voices = [
                        record,
                        *[item for item in self.voices if item["id"] != record["id"]],
                    ][:128]
                    break
            except (RuntimeError, ValueError, TypeError, AttributeError):
                self.reason = "speech_engine_error"
            finally:
                # Retain only the selected backend; stale callbacks are identity-guarded.
                for candidate in [old, *candidates]:
                    if candidate is not None and candidate is not self.engine:
                        try:
                            candidate.stop()
                            candidate.deleteLater()
                        except RuntimeError:
                            pass
                self.setting_up = False

        def command(self, message):
            if message is None:
                if self.engine:
                    self.engine.stop()
                app.quit()
                return
            kind, generation = message["kind"], message["generation"]
            if generation < self.generation:
                return
            if kind in {"configure", "preview"}:
                self.generation = generation
                self.setup(message["preferences"], force=kind == "preview")
            elif kind == "probe":
                self.generation = generation
                self.setup(self.preferences, force=True)
            elif generation != self.generation:
                return
            if kind in {"say", "preview"} and self.engine and not self.reason:
                with self.mailbox.lock:
                    if generation < self.mailbox.generation:
                        return
                if (
                    self.preferences["speech"] or kind == "preview"
                ) and self.engine.state().name == "Ready":
                    self.engine.say(
                        announcement(
                            self.spoken_language,
                            critical=message.get("critical", False),
                            repeat=message.get("repeat", False),
                        )
                    )
            self.status()

    return Worker()


def main():
    def emit(status):
        print(json.dumps(status, ensure_ascii=True, allow_nan=False), flush=True)

    try:
        from PySide6.QtCore import QCoreApplication
        from PySide6.QtTextToSpeech import QTextToSpeech  # noqa: F401
    except ImportError:
        emit({"state": "unavailable", "reason": "qt_speech_not_installed", "voices": []})
        return 2
    app = QCoreApplication(["omega-speech"])
    worker = create_worker(app, emit)

    def read():
        try:
            while True:
                line = sys.stdin.buffer.readline(8193)
                if not line or len(line) > 8192:
                    break
                if not worker.submit(json.loads(line)):
                    # A full mailbox reports busy; it does not shut down the worker.
                    continue
        except (ValueError, OSError, UnicodeError):
            pass
        finally:
            worker.submit(None)

    threading.Thread(target=read, daemon=True, name="omega-speech-input").start()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
