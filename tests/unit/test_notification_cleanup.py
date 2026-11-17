"""Retryable owned speech/notification cleanup, including a silent text child."""

import os
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError
from omega.runtime import control_notifications
from omega.runtime import speech as speech_module
from omega.runtime.control_notifications import NotificationLoop
from omega.runtime.speech import SpeechProcess, _Session


class Pipe:
    def __init__(self):
        self.closed = False
        self.fail = self.after_close_error = False

    def close(self):
        if self.fail:
            raise OSError("pipe close unconfirmed")
        self.closed = True
        if self.after_close_error:
            raise OSError("pipe close acknowledgement lost")


class Thread:
    def __init__(self, alive=False):
        self.alive, self.joins = alive, []

    def join(self, timeout):
        self.joins.append(timeout)

    def is_alive(self):
        return self.alive


class Process:
    def __init__(self):
        self.stdin, self.stdout = Pipe(), Pipe()
        self.code = None
        self.releasable = False
        self.after_exit_error = False
        self.terminations, self.kills = 0, 0

    def poll(self):
        return self.code

    def terminate(self):
        self.terminations += 1
        if self.releasable:
            self.code = -15
        if self.after_exit_error:
            raise OSError("termination acknowledgement lost")

    def kill(self):
        self.kills += 1
        if self.releasable:
            self.code = -9

    def wait(self, timeout):
        if self.code is None:
            raise subprocess.TimeoutExpired("owned fake", timeout)
        return self.code


def owner(process=None, *, reader=False, writer=False):
    child, starts = process or Process(), []

    def forbidden(*args, **kwargs):
        starts.append(1)
        raise AssertionError("pending cleanup must prevent another child")

    speech = SpeechProcess(popen=forbidden)
    session = _Session(child, 1, reader=Thread(reader), writer=Thread(writer))
    speech.session, speech.process = session, child
    return speech, session, starts


def test_live_child_close_retains_owned_session_for_same_object_retry_and_blocks_new_work():
    speech, session, starts = owner()
    with pytest.raises(StorageError, match="termination is unconfirmed"):
        speech.close(timeout_s=0.02)
    assert speech.closed and session.stopped.is_set()
    assert speech.pending_session is session and session.process.code is None
    assert not session.process.stdin.closed and not session.process.stdout.closed
    assert speech.cleanup_error == "speech_process_release_unconfirmed"
    speech.probe()
    assert not speech.say() and starts == []
    session.process.releasable = True
    speech.close(timeout_s=0.02)
    assert speech.pending_session is None and type(session.process.poll()) is int
    assert session.process.stdin.closed and session.process.stdout.closed
    assert speech.cleanup_error == "speech_process_release_unconfirmed"
    speech.close()


@pytest.mark.parametrize("blocked", ["reader", "writer", "unknown"])
def test_native_thread_false_is_required_before_any_pipe_close(blocked):
    speech, session, _ = owner(reader=blocked == "reader", writer=blocked == "writer")
    session.process.code = 0
    if blocked == "unknown":
        session.reader.alive = None
    with pytest.raises(StorageError, match="reader/writer"):
        speech.close(timeout_s=0.01)
    assert speech.pending_session is session
    assert not session.process.stdin.closed and not session.process.stdout.closed
    session.reader.alive = session.writer.alive = False
    speech.close(timeout_s=0.01)
    assert speech.pending_session is None


def test_pipe_partial_failure_keeps_original_handles_until_observed_closed():
    speech, session, _ = owner()
    session.process.code = 0
    session.process.stdin.fail = True
    with pytest.raises(StorageError, match="pipe cleanup"):
        speech.close(timeout_s=0.02)
    assert session.process.stdout.closed and not session.process.stdin.closed
    assert speech.pending_session is session
    session.process.stdin.fail = False
    speech.close(timeout_s=0.02)
    assert speech.pending_session is None
    assert speech.cleanup_error == "speech_pipe_cleanup_unconfirmed"


def test_after_release_errors_reconcile_only_from_actual_exit_and_closed_handle_observations():
    speech, session, _ = owner()
    session.process.releasable = session.process.after_exit_error = True
    session.process.stdout.after_close_error = True
    speech.close(timeout_s=0.02)
    assert speech.pending_session is None
    assert session.process.code == -15 and session.process.stdout.closed
    assert speech.cleanup_error == "speech_process_release_unconfirmed"


def test_join_acknowledgement_error_reconciles_only_after_observed_thread_death():
    speech, session, _ = owner()
    session.process.code = 0

    class AfterJoin(Thread):
        def join(self, timeout):
            self.alive = False
            raise RuntimeError("thread join acknowledgement lost")

    session.reader = AfterJoin(True)
    speech.close(timeout_s=0.02)
    assert speech.pending_session is None
    assert speech.cleanup_error == "speech_thread_cleanup_unconfirmed"


def test_poll_observation_error_preserves_process_until_same_object_retry(monkeypatch):
    speech, session, _ = owner()
    poll = session.process.poll

    def unknown():
        raise OSError("native child status unconfirmed")

    monkeypatch.setattr(session.process, "poll", unknown)
    with pytest.raises(StorageError):
        speech.close(timeout_s=0.02)
    assert speech.pending_session is session
    monkeypatch.setattr(session.process, "poll", poll)
    session.process.code = 0
    speech.close(timeout_s=0.02)
    assert speech.pending_session is None


def test_exited_child_without_remaining_join_budget_keeps_live_threads_and_handles():
    speech, session, _ = owner(reader=True)
    session.process.code = 0
    with pytest.raises(StorageError, match="reader/writer"):
        speech.close(timeout_s=0)
    assert session.reader.joins == [0]
    assert speech.pending_session is session and not session.process.stdout.closed
    session.reader.alive = False
    speech.close(timeout_s=0.02)


def test_cancel_retains_failed_cleanup_and_even_forced_probe_cannot_start_another_child():
    speech, session, starts = owner()
    with pytest.raises(StorageError):
        speech.cancel()
    assert not speech.closed and speech.pending_session is session
    speech.probe()
    assert starts == [] and speech.pending_session is session
    session.process.releasable = True
    speech.cancel()
    assert speech.pending_session is None
    speech.close()


def test_cleanup_lifecycle_contention_obeys_deadline_and_retains_session():
    speech, session, _ = owner()
    acquired, release = threading.Event(), threading.Event()

    def hold():
        with speech.lifecycle:
            acquired.set()
            release.wait(2)

    thread = threading.Thread(target=hold)
    thread.start()
    assert acquired.wait(1)
    try:
        started = time.monotonic()
        with pytest.raises(StorageError, match="deadline"):
            speech.close(timeout_s=0.02)
        assert time.monotonic() - started < 0.5
        assert speech.pending_session is session
    finally:
        release.set()
        thread.join(2)
        session.process.code = 0
        speech.close()


def test_speech_cannot_claim_its_own_reader_has_stopped():
    speech, session, _ = owner()
    session.process.code = 0
    session.reader = threading.current_thread()
    with pytest.raises(StorageError, match="own thread"):
        speech.close(timeout_s=0.02)
    assert speech.pending_session is session
    session.reader = Thread(False)
    speech.close()


@pytest.mark.parametrize("blocked", ["popen", "reader_start"])
def test_inflight_start_cache_lock_wait_is_bounded_and_original_session_survives_retry(
    monkeypatch, blocked
):
    entered, release = threading.Event(), threading.Event()
    process, starts, results = Process(), [], []

    def start(*args, **kwargs):
        starts.append(1)
        if blocked == "popen":
            entered.set()
            assert release.wait(3)
        return process

    original_start = threading.Thread.start

    def thread_start(thread):
        if blocked == "reader_start" and thread.name.startswith("omega-tts-read-"):
            entered.set()
            assert release.wait(3)
        return original_start(thread)

    monkeypatch.setattr(speech_module.threading.Thread, "start", thread_start)
    speech = SpeechProcess(popen=start)
    probing = threading.Thread(
        target=lambda: results.append(speech.probe()), name="controlled-probe"
    )
    probing.start()
    assert entered.wait(1)
    try:
        started = time.monotonic()
        with pytest.raises(StorageError, match="startup cache"):
            speech.close(timeout_s=0.02)
        assert time.monotonic() - started < 0.5
        assert speech.closed
        # These dispatch gates do not wait for the cache lock held by startup.
        assert not speech.say() and not speech._send({"kind": "probe"}, force=True)
        speech.configure({"speech": False})
        assert starts == [1]
    finally:
        release.set()
        probing.join(2)
        assert not probing.is_alive()
        assert speech.pending_session is not None
        assert speech.pending_session.process is process
        process.releasable = True
        speech.close()
    assert len(results) == 1 and starts == [1]
    assert process.poll() is not None and process.stdin.closed and process.stdout.closed
    assert speech.pending_session is None
    assert speech.cleanup_error == "speech_startup_cleanup_unconfirmed"


class Voice:
    def __init__(self):
        self.calls = 0
        self.fail = False

    def close(self):
        self.calls += 1
        if self.fail:
            raise OSError("voice cleanup unconfirmed")


def loop(voice):
    return NotificationLoop(
        SimpleNamespace(changed=threading.Event()),
        lambda: pytest.fail("logically closed loop must reject tick"),
        speech=voice,
    )


def test_notification_loop_requires_own_thread_and_speech_cleanup_then_retries_same_objects():
    voice = Voice()
    notifications = loop(voice)
    thread = Thread(True)
    notifications.thread = thread
    with pytest.raises(StorageError):
        notifications.close()
    assert notifications.thread is thread and notifications.stopped.is_set()
    assert voice.calls == 1 and notifications.tick() is None
    thread.alive = False
    voice.fail = True
    with pytest.raises(StorageError):
        notifications.close()
    voice.fail = False
    notifications.close()
    assert voice.calls == 3 and notifications.cleanup_error == "StorageError"
    notifications.start()
    assert notifications.thread is thread


def test_notification_close_passes_only_remaining_shared_deadline(monkeypatch):
    now, timeouts = [100.0], []

    class TimedThread(Thread):
        def join(self, timeout):
            timeouts.append(timeout)
            now[0] += 2

    class TimedVoice:
        def close(self, *, timeout_s):
            timeouts.append(timeout_s)
            now[0] += 0.25

    monkeypatch.setattr(control_notifications, "time", SimpleNamespace(monotonic=lambda: now[0]))
    notifications = loop(TimedVoice())
    notifications.thread = TimedThread(False)
    notifications.close()
    assert timeouts == [5, 3]


def test_notification_speech_typeerror_does_not_retry_call_with_another_signature():
    class BrokenVoice:
        calls = 0

        def close(self, *, timeout_s):
            self.calls += 1
            raise TypeError("failure inside close")

    voice = BrokenVoice()
    with pytest.raises(StorageError):
        loop(voice).close()
    assert voice.calls == 1


def test_notification_self_close_remains_unconfirmed_and_still_revokes_speech():
    voice, notifications = Voice(), None
    notifications = loop(voice)
    notifications.thread = threading.current_thread()
    with pytest.raises(StorageError):
        notifications.close()
    assert voice.calls == 1 and notifications.stopped.is_set()


def test_real_owned_silent_text_child_exits_and_all_pipe_threads_and_handles_are_closed():
    children = []
    script = (
        "import sys,json\n"
        "print(json.dumps({'state':'ready','engine':'owned-text','voices':[]}),flush=True)\n"
        "for line in sys.stdin:\n"
        " json.loads(line)\n"
    )

    def start(*args, **kwargs):
        process = subprocess.Popen(
            [sys.executable, "-I", "-u", "-c", script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        children.append(process)
        return process

    speech = SpeechProcess(popen=start)
    session = None
    try:
        speech.probe()
        session = speech.session
        deadline = time.monotonic() + 3
        while speech.snapshot()["engine"] != "owned-text" and time.monotonic() < deadline:
            time.sleep(0.005)
        assert speech.snapshot()["engine"] == "owned-text"
        speech.close()
        assert speech.pending_session is None and speech.session is None
        assert type(session.process.poll()) is int
        assert session.reader.is_alive() is session.writer.is_alive() is False
        assert session.process.stdin.closed and session.process.stdout.closed
        assert len(children) == 1
    finally:
        for process in children:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)
        speech.close()
