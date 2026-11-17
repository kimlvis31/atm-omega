"""Core-owned audio survives GUI restarts; idle state waits for a change event."""

import inspect
import os
import sys
import threading
import time

from omega.domain.errors import StorageError
from omega.runtime.speech import SpeechProcess


def desktop_beep(critical: bool):
    if os.name == "nt":
        import winsound

        winsound.Beep(1100 if critical else 800, 200)
    else:
        sys.stderr.write("\a")
        sys.stderr.flush()


class NotificationLoop:
    def __init__(self, control, settings, *, emit=desktop_beep, clock=time.monotonic, speech=None):
        self.control, self.settings, self.emit, self.clock = control, settings, emit, clock
        self.stopped = threading.Event()
        self.last_emitted = {}
        self.last_revision = {}
        self.delivery_failed = False
        self.thread = None
        self.cleanup_error = None
        self.speech = speech or SpeechProcess()

    def tick(self):
        if self.stopped.is_set():
            return None
        now = self.clock()
        preferences = self.settings()["notifications"]
        try:
            self.speech.configure(preferences)
        except Exception:
            self.delivery_failed = True
        incidents = self.control.snapshot()["incidents"]
        active = {item["id"] for item in incidents if not item["acknowledged"]}
        if set(self.last_emitted) - active:
            cancel = getattr(self.speech, "cancel", None)
            if cancel is not None:
                try:
                    cancel()
                except Exception:
                    self.delivery_failed = True
        self.last_emitted = {
            key: value for key, value in self.last_emitted.items() if key in active
        }
        self.last_revision = {
            key: value for key, value in self.last_revision.items() if key in active
        }
        delay, pending, due_critical = None, [], []
        for incident in incidents:
            if self.stopped.is_set():
                break
            if incident["acknowledged"]:
                continue
            identity = incident["id"]
            critical = incident["severity"] == "critical"
            previous = self.last_emitted.get(identity)
            revision = incident.get("revision", 1)
            due = (
                previous is None
                or self.last_revision.get(identity) != revision
                or (critical and now - previous >= preferences["repeat_critical_s"])
            )
            repeat = previous is not None and self.last_revision.get(identity) == revision
            pending.append((identity, revision, critical, previous, repeat, due))
            if critical and due:
                due_critical.append((identity, revision, repeat))
        critical_delivered = False
        for identity, revision, critical, previous, repeat, due in pending:
            if self.stopped.is_set():
                break
            if due:
                if critical:
                    if not critical_delivered:
                        # Emit at the first due critical's position, not behind later warnings.
                        self._deliver(
                            preferences,
                            critical=True,
                            repeat=all(row[2] for row in due_critical),
                        )
                        for member, member_revision, _repeat in due_critical:
                            self.last_emitted[member] = now
                            self.last_revision[member] = member_revision
                        critical_delivered = True
                else:
                    self._deliver(preferences, critical=False, repeat=repeat)
                    self.last_emitted[identity] = now
                    self.last_revision[identity] = revision
            if critical:
                remaining = max(
                    0.01, preferences["repeat_critical_s"] - (now - (now if due else previous))
                )
                delay = remaining if delay is None else min(delay, remaining)
        return delay

    def _deliver(self, preferences, *, critical, repeat):
        if preferences["sound"]:
            try:
                self.emit(critical)
            except Exception:
                self.delivery_failed = True
        if preferences["speech"]:
            try:
                if not self.speech.say(critical=critical, repeat=repeat):
                    self.delivery_failed = True
            except Exception:
                self.delivery_failed = True

    def start(self):
        if self.stopped.is_set() or self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run, name="omega-notifications", daemon=True)
        self.thread.start()

    def _run(self):
        while not self.stopped.is_set():
            self.control.changed.clear()
            try:
                delay = self.tick()
            except Exception:
                self.delivery_failed = True
                delay = 5
            self.control.changed.wait(delay)

    def close(self):
        deadline = time.monotonic() + 5
        self.stopped.set()
        self.control.changed.set()
        failure = None
        try:
            if self.thread is threading.current_thread():
                raise StorageError("notification thread cannot confirm its own termination")
            if self.thread is not None:
                self.thread.join(timeout=max(0, deadline - time.monotonic()))
                if self.thread.is_alive() is not False:
                    raise StorageError("notification thread cleanup is unconfirmed")
        except Exception as error:
            failure = error
        try:
            close = self.speech.close
            parameters = inspect.signature(close).parameters
            if "timeout_s" in parameters or any(
                item.kind is inspect.Parameter.VAR_KEYWORD for item in parameters.values()
            ):
                close(timeout_s=max(0, deadline - time.monotonic()))
            else:
                close()  # Existing injected owners retain their no-argument contract.
            if time.monotonic() > deadline:
                raise StorageError("notification cleanup deadline exhausted")
        except Exception as error:
            failure = failure or error
        if failure is not None:
            if self.cleanup_error is None:
                self.cleanup_error = type(failure).__name__
            raise StorageError("notification/speech cleanup is unconfirmed") from failure
