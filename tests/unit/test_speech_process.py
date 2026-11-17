"""Speech supervision with fake local pipes; no OS voice or external connection."""

import json
import queue
import subprocess
import threading
import time

import pytest

from omega.runtime.speech import SpeechProcess, _Session, speech_preferences, speech_status


def eventually(predicate):
    deadline = time.monotonic() + 2
    while not predicate():
        assert time.monotonic() < deadline, "worker did not reach expected state"
        threading.Event().wait(0.005)


class Output:
    def __init__(self):
        self.lines = queue.Queue()
        self.closed = False

    def readline(self, _limit):
        return self.lines.get()

    def feed(self, value):
        self.lines.put(json.dumps(value).encode() + b"\n")

    def close(self):
        self.closed = True


class Input:
    def __init__(self, owner, block=False):
        self.owner = owner
        self.block = block
        self.started, self.release = threading.Event(), threading.Event()
        self.messages = []
        self.closed = False

    def write(self, value):
        self.started.set()
        if self.block:
            assert self.release.wait(2)
        if self.owner.code is not None:
            raise BrokenPipeError
        self.messages.append(json.loads(value))

    def flush(self):
        pass

    def close(self):
        assert not self.block or self.release.is_set(), "pipe was closed before unblocking writer"
        self.closed = True


class Process:
    def __init__(self, block=False, resist_terminate=False):
        self.stdout = Output()
        self.stdin = Input(self, block)
        self.code = None
        self.terminated = self.killed = False
        self.waits = 0
        self.resist_terminate = resist_terminate

    def poll(self):
        return self.code

    def terminate(self):
        self.terminated = True
        if not self.resist_terminate:
            self.exit(-15)

    def kill(self):
        self.killed = True
        self.exit(-9)

    def wait(self, timeout):
        self.waits += 1
        if self.code is None:
            raise subprocess.TimeoutExpired("fake", timeout)
        return self.code

    def exit(self, code):
        self.code = code
        self.stdout.lines.put(b"")
        self.stdin.release.set()


@pytest.fixture
def speech():
    now, processes = [0], []

    def popen(*_args, **_kwargs):
        process = Process()
        processes.append(process)
        return process

    value = SpeechProcess(popen=popen, clock=lambda: now[0])
    yield value, processes, now
    value.close()
    for process in processes:
        process.exit(-9)


def preferences(enabled=True, **values):
    return speech_preferences({"speech": enabled, "speech_language": "en_US", **values})


def test_off_is_lazy_and_enabled_dispatch_contains_only_fixed_flags(speech):
    owner, processes, _ = speech
    owner.configure(preferences(False))
    assert not processes and not owner.say(critical=True)
    owner.configure(preferences())
    eventually(lambda: bool(processes[0].stdin.messages))
    assert owner.say(critical=True)
    eventually(lambda: any(item["kind"] == "say" for item in processes[0].stdin.messages))
    message = next(item for item in processes[0].stdin.messages if item["kind"] == "say")
    assert set(message) == {"kind", "critical", "repeat", "generation"}
    processes[0].stdout.feed({"state": "ready", "engine": "fake", "voices": [], "enabled": True})
    eventually(lambda: owner.snapshot()["state"] == "ready")


def test_dead_worker_recovers_after_backoff_and_probe_can_override(speech):
    owner, processes, now = speech
    owner.configure(preferences())
    processes[0].exit(2)
    eventually(lambda: owner.session is None)
    assert not owner.say() and len(processes) == 1
    now[0] = 61
    assert owner.say() and len(processes) == 2
    processes[1].exit(2)
    eventually(lambda: owner.session is None)
    owner.probe()
    assert len(processes) == 3


def test_disable_terminates_active_and_queued_preview_without_resurrection(speech):
    owner, processes, _ = speech
    owner.preview(preferences())
    old_generation = owner.preference_generation
    owner.configure(preferences(False))
    assert processes[0].terminated and owner.session is None
    assert owner.snapshot()["state"] == "disabled"
    assert not owner._send(
        {"kind": "preview", "preferences": preferences()}, expected_generation=old_generation
    )
    assert len(processes) == 1 and not owner.say()


def test_explicit_saved_off_can_stop_preview_but_periodic_configuration_does_not(speech):
    owner, processes, _ = speech
    owner.preview(preferences(False))
    process = processes[0]
    owner.configure(preferences(False))
    assert not process.terminated
    owner.configure(preferences(False), stop_if_disabled=True)
    assert process.terminated and owner.session is None


def test_old_generation_reader_cannot_overwrite_new_worker_report(speech):
    owner, processes, _ = speech
    owner.configure(preferences())
    old = owner.session
    owner.configure(preferences(False))
    owner.configure(preferences())
    processes[1].stdout.feed({"state": "ready", "engine": "new", "voices": []})
    eventually(lambda: owner.snapshot()["engine"] == "new")
    old.stopped.clear()
    processes[0].stdout = Output()
    processes[0].stdout.feed(
        {"state": "unavailable", "reason": "speech_engine_error", "voices": []}
    )
    processes[0].stdout.lines.put(b"")
    owner._read(old)
    assert owner.snapshot()["engine"] == "new"


def test_start_failure_backoff_is_overridden_only_by_explicit_probe():
    attempts, now = [], [0]

    def unavailable(*_args, **_kwargs):
        attempts.append(1)
        raise OSError("not installed")

    owner = SpeechProcess(popen=unavailable, clock=lambda: now[0])
    owner.configure(preferences())
    assert owner.snapshot()["reason"] == "worker_start_failed"
    assert not owner.say() and len(attempts) == 1
    owner.probe()
    assert len(attempts) == 2
    now[0] = 61
    owner.say()
    assert len(attempts) == 3
    owner.close()


def test_blocked_writer_shutdown_kills_then_waits_without_buffer_deadlock():
    process = Process(block=True, resist_terminate=True)
    owner = SpeechProcess(popen=lambda *_a, **_k: process)
    owner.configure(preferences())
    assert process.stdin.started.wait(1)
    started = time.monotonic()
    owner.close()
    assert time.monotonic() - started < 2
    assert process.killed and process.waits >= 2
    assert process.stdin.closed and process.stdout.closed


def test_parent_queue_is_bounded_and_cancel_preserves_saved_preference(speech):
    owner, processes, _ = speech
    process = Process()
    session = _Session(process, 99)
    owner.preferences = preferences()
    owner.session, owner.process = session, process
    for _ in range(8):
        assert owner.say()
    assert not owner.say() and session.outbound.qsize() == 8
    assert owner.snapshot()["reason"] == "speech_queue_full"
    owner.cancel()
    assert session.stopped.is_set() and process.terminated
    assert owner.preferences["speech"]


@pytest.mark.parametrize(
    "status",
    [
        {"state": "fake"},
        {"state": "ready", "voices": [{}]},
        {"state": "ready", "voices": [{}] * 129},
        {"state": "ready", "reason": "private error message"},
    ],
)
def test_invalid_reports_are_rejected_without_echoing_them(status):
    with pytest.raises(ValueError):
        speech_status(status)


def test_status_overflow_retires_worker(speech):
    owner, processes, _ = speech
    owner.configure(preferences())
    processes[0].stdout.lines.put(b"x" * 65537)
    eventually(lambda: owner.session is None)
    assert owner.snapshot()["state"] == "unavailable"


def test_core_dispatch_repeat_ack_and_gui_independence(tmp_path):
    from omega.runtime.control import ControlState
    from omega.runtime.control_notifications import NotificationLoop
    from omega.runtime.settings import SettingsStore

    class StubSpeech:
        def __init__(self):
            self.calls, self.cancelled, self.closed = [], 0, False

        def configure(self, _):
            pass

        def say(self, **flags):
            self.calls.append(flags)
            return True

        def cancel(self):
            self.cancelled += 1

        def close(self):
            self.closed = True

    settings = SettingsStore(tmp_path / "settings.json")
    settings.update({"notifications": {"speech": True, "sound": False}}, revision=0)
    control, voice, now = ControlState(tmp_path / "control.json"), StubSpeech(), [0]
    loop = NotificationLoop(
        control,
        settings.snapshot,
        emit=lambda _: pytest.fail("sound disabled"),
        speech=voice,
        clock=lambda: now[0],
    )
    control.incident("GUI_OFF_TEST", "critical")
    loop.tick()
    loop.tick()
    assert voice.calls == [{"critical": True, "repeat": False}]
    now[0] = 30
    loop.tick()
    assert voice.calls[-1] == {"critical": True, "repeat": True}
    control.acknowledge(control.snapshot()["incidents"][0]["id"])
    assert loop.tick() is None and voice.cancelled == 1
    loop.close()
    assert voice.closed


def test_authenticated_core_notification_path_dispatches_once_without_a_gui(tmp_path):
    from omega.runtime.service import CoreService

    class StubSpeech:
        def __init__(self):
            self.calls = []
            self.cancelled = 0

        def configure(self, _preferences, **_kwargs):
            pass

        def say(self, **flags):
            self.calls.append(flags)
            return True

        def snapshot(self):
            return {"state": "ready", "engine": "inert-test", "voices": []}

        def cancel(self):
            self.cancelled += 1

        def close(self):
            pass

    service = CoreService(tmp_path)
    voice = StubSpeech()
    service.notifications.speech.close()
    service.notifications.speech = voice
    try:
        service.request(
            "POST",
            "/api/commands",
            service.token,
            {
                "id": "speech-config",
                "kind": "settings.patch",
                "payload": {
                    "revision": 0,
                    "changes": {"notifications": {"speech": True, "sound": False}},
                },
            },
        )
        service.notifications.start()
        service.control.incident("CORE_WITHOUT_GUI", "warning")
        eventually(lambda: len(voice.calls) == 1)
        owner = service.request("GET", "/api/state", service.token)
        assert owner["core"]["notification_speech_owner"] == "core"
        assert owner["core"]["speech"]["engine"] == "inert-test"
        assert service.state(owner=False)["core"]["speech"] == {}
        service.request("GET", "/api/state", service.token)
        assert len(voice.calls) == 1
        incident = service.control.snapshot()["incidents"][0]
        service.request(
            "POST",
            "/api/commands",
            service.token,
            {
                "id": "speech-ack",
                "kind": "incident.acknowledge",
                "payload": {"incident_id": incident["id"]},
            },
        )
        eventually(lambda: voice.cancelled == 1)
    finally:
        service.close()


def test_optional_speech_failure_does_not_block_beep_or_critical_repeat_schedule(tmp_path):
    from omega.runtime.control import ControlState
    from omega.runtime.control_notifications import NotificationLoop
    from omega.runtime.settings import SettingsStore

    class BrokenSpeech:
        def configure(self, _):
            raise OSError("inert speech unavailable")

        def say(self, **_):
            raise OSError("inert speech unavailable")

    control, settings = (
        ControlState(tmp_path / "control.json"),
        SettingsStore(tmp_path / "settings.json"),
    )
    settings.update({"notifications": {"speech": True}}, revision=0)
    delivered = []
    loop = NotificationLoop(
        control, settings.snapshot, emit=delivered.append, clock=lambda: 0, speech=BrokenSpeech()
    )
    control.incident("INERT_FAILURE")
    assert loop.tick() == 30
    assert delivered == [True] and loop.delivery_failed
    loop.tick()
    assert delivered == [True]


@pytest.mark.parametrize(
    "changes",
    [{"speech_volume": float("nan")}, {"speech_rate": True}, {"speech_language": "unsupported"}],
)
def test_worker_port_revalidates_nonfinite_or_unsupported_settings(changes):
    with pytest.raises(ValueError):
        speech_preferences(changes)
