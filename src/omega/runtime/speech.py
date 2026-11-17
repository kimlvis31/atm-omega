"""Bounded optional core-owned speech subprocess; Qt remains in its worker."""

import json
import math
import os
import queue
import subprocess
import sys
import threading
import time
from copy import deepcopy
from dataclasses import dataclass, field

from omega.domain.errors import StorageError

_STATES = {"starting", "ready", "disabled", "speaking", "unavailable", "busy"}
_REASONS = {
    None,
    "qt_speech_not_installed",
    "no_speech_engine",
    "selected_engine_unavailable",
    "selected_voice_or_language_unavailable",
    "unsupported_system_language",
    "voice_language_mismatch",
    "speech_engine_error",
    "speech_queue_full",
    "invalid_command",
    "worker_exited",
    "worker_transport_failed",
    "worker_start_failed",
}


def announcement(language, *, critical=False, repeat=False):
    korean = language.startswith("ko") or language == "auto"
    if repeat:
        return (
            "Omega 중요 알림이 아직 확인되지 않았습니다."
            if korean
            else "Omega has an unacknowledged critical incident."
        )
    if critical:
        return (
            "확인이 필요한 중요한 알림이 있습니다. Omega 알림을 확인하세요."
            if korean
            else "A critical incident needs attention. Check Omega notifications."
        )
    return "Omega 새 알림이 있습니다." if korean else "Omega has a new notification."


def speech_preferences(preferences):
    defaults = {
        "speech": False,
        "speech_language": "auto",
        "speech_voice": "",
        "speech_rate": 0.0,
        "speech_volume": 0.8,
    }
    if not isinstance(preferences, dict):
        raise ValueError("invalid speech preferences")
    selected = {key: preferences.get(key, value) for key, value in defaults.items()}
    if (
        type(selected["speech"]) is not bool
        or selected["speech_language"] not in {"auto", "ko_KR", "en_US"}
        or not isinstance(selected["speech_voice"], str)
        or len(selected["speech_voice"]) > 256
        or any(not c.isprintable() for c in selected["speech_voice"])
    ):
        raise ValueError("invalid speech preferences")
    for key, minimum, maximum in (("speech_rate", -1, 1), ("speech_volume", 0, 1)):
        value = selected[key]
        if (
            type(value) not in {int, float}
            or not math.isfinite(value)
            or not minimum <= value <= maximum
        ):
            raise ValueError("invalid speech preference range")
    return selected


def speech_status(status):
    if not isinstance(status, dict) or status.get("state") not in _STATES:
        raise ValueError("invalid speech status")
    engine, reason = status.get("engine"), status.get("reason")
    if (
        engine is not None
        and (not isinstance(engine, str) or len(engine) > 128)
        or reason not in _REASONS
    ):
        raise ValueError("invalid speech status metadata")
    voices = status.get("voices", [])
    if not isinstance(voices, list) or len(voices) > 128:
        raise ValueError("invalid voice catalog")
    for voice in voices:
        if not isinstance(voice, dict) or set(voice) != {"id", "name", "locale"}:
            raise ValueError("invalid voice entry")
        if any(
            not isinstance(voice[key], str)
            or len(voice[key]) > limit
            or any(not c.isprintable() for c in voice[key])
            for key, limit in (("id", 256), ("name", 256), ("locale", 40))
        ):
            raise ValueError("invalid voice entry")
    result = {
        "state": status["state"],
        "engine": engine,
        "voices": deepcopy(voices),
        "reason": reason,
    }
    if "generation" in status:
        if type(status["generation"]) is not int or not 0 <= status["generation"] < 2**63:
            raise ValueError("invalid speech status generation")
        result["generation"] = status["generation"]
    if "enabled" in status:
        if type(status["enabled"]) is not bool:
            raise ValueError("invalid speech enabled state")
        result["enabled"] = status["enabled"]
    return result


@dataclass(eq=False)
class _Session:
    process: object
    generation: int
    outbound: queue.Queue = field(default_factory=lambda: queue.Queue(maxsize=8))
    stopped: threading.Event = field(default_factory=threading.Event)
    reader: threading.Thread | None = None
    writer: threading.Thread | None = None


class SpeechProcess:
    def __init__(self, *, popen=subprocess.Popen, clock=time.monotonic):
        self.popen, self.clock = popen, clock
        self.lock = threading.RLock()
        self.lifecycle = threading.RLock()
        self.diagnostic_lock = threading.Lock()
        self.process = self.session = None
        self.pending_session = None
        self.cleanup_error = None
        self.preferences = None
        self.generation = self.preference_generation = 0
        self.retry_after = 0
        self.closed = False
        self.report = {"state": "disabled", "engine": None, "voices": [], "reason": None}

    def snapshot(self):
        with self.lock:
            return deepcopy(self.report)

    def _start(self, *, force=False):
        if self.closed:
            return None
        with self.lifecycle:
            return self._start_owned(force=force)

    def _start_owned(self, *, force=False):
        with self.lock:
            if self.closed:
                return None
            old = self.session
            if old is not None and not old.stopped.is_set() and old.process.poll() is None:
                return old
            if old is not None:
                self._retain(old)
                self.retry_after = max(self.retry_after, self.clock() + 60)
                self.report.update(state="unavailable", reason="worker_exited")
            pending = self.pending_session
        if pending is not None:
            try:
                self._cleanup_session(pending, time.monotonic() + 1.5)
            except StorageError:
                return None
        with self.lock:
            if self.closed:
                return None
            if not force and self.clock() < self.retry_after:
                return None
            self.report.update(state="starting", reason=None)
            try:
                process = self.popen(
                    [sys.executable, "-I", "-m", "omega.gui.speech_worker"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
            except OSError:
                self.report.update(state="unavailable", reason="worker_start_failed")
                self.retry_after = self.clock() + 60
                return None
            self.generation += 1
            session = _Session(process, self.generation)
            self.session, self.process = session, process
            if self.closed:
                self._retain(session)
                return None
            if self.preferences is not None:
                session.outbound.put_nowait(
                    {
                        "kind": "configure",
                        "preferences": deepcopy(self.preferences),
                        "generation": self.preference_generation,
                    }
                )
            session.reader = threading.Thread(
                target=self._read,
                args=(session,),
                daemon=True,
                name=f"omega-tts-read-{session.generation}",
            )
            session.writer = threading.Thread(
                target=self._write,
                args=(session,),
                daemon=True,
                name=f"omega-tts-write-{session.generation}",
            )
            session.reader.start()
            if self.closed:
                self._retain(session)
                return None
            session.writer.start()
            if self.closed:
                self._retain(session)
                return None
            return session

    def _note_cleanup(self, code):
        # Never wait for the cache lock held by an in-flight OS spawn here.
        with self.diagnostic_lock:
            if self.cleanup_error is None:
                self.cleanup_error = code

    def _retain(self, session):
        """Called under lock; active aliases may clear, owned resources may not."""
        if self.pending_session not in (None, session):
            raise StorageError("speech cleanup already owns another session")
        self.pending_session = session
        session.stopped.set()
        if self.session is session:
            self.session = self.process = None
        while True:
            try:
                session.outbound.get_nowait()
            except queue.Empty:
                break
        try:
            session.outbound.put_nowait(None)  # Wake an idle writer after revocation.
        except queue.Full:
            pass

    def _terminate(self, process, *, deadline=None):
        # Never acquire the stdin buffer lock before terminating a blocked writer.
        deadline = time.monotonic() + 1 if deadline is None else deadline
        for action in (process.terminate, process.kill):
            try:
                if type(process.poll()) is int:
                    return
                if time.monotonic() >= deadline:
                    break
                action()
                process.wait(timeout=min(0.5, max(0, deadline - time.monotonic())))
            except Exception:
                self._note_cleanup("speech_process_release_unconfirmed")
        try:
            if type(process.poll()) is int:
                return
        except Exception:
            self._note_cleanup("speech_process_release_unconfirmed")
        raise StorageError("speech process termination is unconfirmed")

    def _cleanup_session(self, session, deadline):
        session.stopped.set()
        self._terminate(session.process, deadline=deadline)
        for thread in (session.reader, session.writer):
            if thread is threading.current_thread():
                self._note_cleanup("speech_thread_cleanup_unconfirmed")
                raise StorageError("speech cannot confirm its own thread termination")
            if thread is not None:
                try:
                    if thread.is_alive() is not False:
                        try:
                            thread.join(timeout=max(0, deadline - time.monotonic()))
                        except Exception:
                            self._note_cleanup("speech_thread_cleanup_unconfirmed")
                    if thread.is_alive() is not False:
                        raise StorageError("speech reader/writer cleanup is unconfirmed")
                except Exception as error:
                    self._note_cleanup("speech_thread_cleanup_unconfirmed")
                    raise StorageError("speech reader/writer cleanup is unconfirmed") from error
        # Only stopped readers/writers can relinquish their buffered pipe locks.
        for stream in (session.process.stdout, session.process.stdin):
            try:
                if not stream.closed:
                    stream.close()
            except Exception:
                self._note_cleanup("speech_pipe_cleanup_unconfirmed")
            try:
                confirmed = stream.closed is True
            except Exception:
                confirmed = False
            if not confirmed:
                self._note_cleanup("speech_pipe_cleanup_unconfirmed")
                raise StorageError("speech pipe cleanup is unconfirmed")
        with self.lock:
            if self.pending_session is session:
                self.pending_session = None

    def _cleanup_pending(self, deadline):
        if not self.lifecycle.acquire(timeout=max(0, deadline - time.monotonic())):
            self._note_cleanup("speech_cleanup_busy")
            raise StorageError("speech cleanup deadline exhausted")
        try:
            with self.lock:
                pending = self.pending_session
            if pending is not None:
                self._cleanup_session(pending, deadline)
        finally:
            self.lifecycle.release()

    def _retire(self, session, reason):
        with self.lock:
            if self.session is session:
                self._retain(session)
                if not (
                    reason == "worker_exited"
                    and self.report.get("state") == "unavailable"
                    and self.report.get("reason") is not None
                ):
                    self.report.update(state="unavailable", reason=reason)
                self.retry_after = self.clock() + 60
            else:
                session.stopped.set()
        try:
            self._terminate(session.process)
        except StorageError:
            self._note_cleanup("speech_process_release_unconfirmed")

    def _send(self, message, *, force=False, expected_generation=None):
        if self.closed:
            return False
        with self.lock:
            if (
                expected_generation is not None
                and expected_generation != self.preference_generation
            ):
                return False
        session = self._start(force=force)
        if session is None:
            return False
        with self.lock:
            if (
                expected_generation is not None
                and expected_generation != self.preference_generation
            ):
                return False
            if self.session is not session or session.stopped.is_set() or self.closed:
                return False
            message = deepcopy(message) | {"generation": self.preference_generation}
            try:
                session.outbound.put_nowait(message)
                return True
            except queue.Full:
                self.report.update(state="busy", reason="speech_queue_full")
                return False

    def configure(self, preferences, *, stop_if_disabled=False):
        if self.closed:
            return
        selected, retiring = speech_preferences(preferences), None
        with self.lock:
            if self.closed or (
                selected == self.preferences and not (stop_if_disabled and not selected["speech"])
            ):
                return
            self.preferences = selected
            self.preference_generation += 1
            generation = self.preference_generation
            if not selected["speech"]:
                # Revokes even previews already delivered to the pipe/Qt mailbox.
                retiring = self.session
                if retiring is not None:
                    self._retain(retiring)
                self.report.update(state="disabled", reason=None, engine=None, enabled=False)
                self.retry_after = 0
        if retiring is not None or self.pending_session is not None:
            self._cleanup_pending(time.monotonic() + 1.5)
        if selected["speech"]:
            self._send(
                {"kind": "configure", "preferences": selected}, expected_generation=generation
            )

    def cancel(self):
        """ACK revokes already queued speech without changing the saved preferences."""
        with self.lock:
            session = self.session
            self.preference_generation += 1
            if session is not None:
                self._retain(session)
            self.report.update(state="disabled", engine=None, reason=None)
        self._cleanup_pending(time.monotonic() + 1.5)

    def probe(self):
        self._send({"kind": "probe"}, force=True)
        return self.snapshot()

    def preview(self, preferences):
        self.configure(preferences)
        with self.lock:
            selected, generation = deepcopy(self.preferences), self.preference_generation
        self._send({"kind": "preview", "preferences": selected}, expected_generation=generation)
        return self.snapshot()

    def say(self, *, critical=False, repeat=False):
        if self.closed:
            return False
        with self.lock:
            if not self.preferences or not self.preferences["speech"]:
                return False
            generation = self.preference_generation
        return self._send(
            {"kind": "say", "critical": bool(critical), "repeat": bool(repeat)},
            expected_generation=generation,
        )

    def _write(self, session):
        try:
            while not session.stopped.is_set():
                try:
                    message = session.outbound.get(timeout=0.25)
                except queue.Empty:
                    continue
                with self.lock:
                    if (
                        self.session is not session
                        or self.closed
                        or message["generation"] != self.preference_generation
                    ):
                        continue
                    if message["kind"] == "say" and not self.preferences["speech"]:
                        continue
                payload = json.dumps(message, allow_nan=False).encode("utf-8") + b"\n"
                if len(payload) > 8192:
                    raise ValueError("speech command exceeds limit")
                session.process.stdin.write(payload)
                session.process.stdin.flush()
        except (OSError, ValueError, AttributeError, TypeError):
            self._retire(session, "worker_transport_failed")

    def _read(self, session):
        try:
            while not session.stopped.is_set():
                line = session.process.stdout.readline(65537)
                if not line:
                    break
                if len(line) > 65536:
                    raise ValueError("speech status exceeds bound")
                status = speech_status(json.loads(line))
                with self.lock:
                    if (
                        self.session is session
                        and not session.stopped.is_set()
                        and status.get("generation", self.preference_generation)
                        == self.preference_generation
                    ):
                        self.report = status
        except (OSError, ValueError, UnicodeError, TypeError, AttributeError):
            pass
        finally:
            if not session.stopped.is_set():
                self._retire(session, "worker_exited")

    def close(self, *, timeout_s=5):
        if (
            type(timeout_s) not in {int, float}
            or not math.isfinite(timeout_s)
            or not 0 <= timeout_s <= 5
        ):
            raise ValueError("speech close timeout must be between zero and five seconds")
        deadline = time.monotonic() + timeout_s
        # Revocation cannot depend on acquiring a lock held by Popen/start.
        self.closed = True
        if not self.lock.acquire(timeout=max(0, deadline - time.monotonic())):
            self._note_cleanup("speech_startup_cleanup_unconfirmed")
            raise StorageError("speech startup cache ownership is unconfirmed")
        try:
            self.report.update(state="disabled", engine=None, reason=None, enabled=False)
            session = self.session
            if session is not None:
                self._retain(session)
        finally:
            self.lock.release()
        self._cleanup_pending(deadline)
