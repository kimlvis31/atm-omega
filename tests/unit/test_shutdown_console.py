"""Real console/authenticated Core dispatch; shutdown manager is data-only fake.

No real stdin subprocess is spawned. Blocking input callbacks are explicitly
released after verifying return, not presented as native-stdin cleanup proof.
"""

import json
import threading
from copy import deepcopy
from io import BytesIO

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.console import ControlConsole
from omega.runtime.service import CoreService


class Manager:
    core_epoch = "shutdown-console-core"
    blocks_new_risk = False

    def __init__(self):
        self.revision, self.requests, self.closed = 7, [], False

    def snapshot(self):
        return {"core_epoch": self.core_epoch, "revision": self.revision, "operations": []}

    def submit(self, identity, epoch, revision, kind, parameters):
        if epoch != self.core_epoch or revision != self.revision:
            raise ValidationError("stale shutdown context")
        row = {
            "id": identity,
            "core_epoch": epoch,
            "expected_revision": revision,
            "kind": kind,
            "parameters": deepcopy(parameters),
            "state": "queued",
        }
        self.requests.append(row)
        self.revision += 1
        return deepcopy(row)

    def status(self, identity):
        return deepcopy(next(row for row in self.requests if row["id"] == identity))

    def close(self):
        self.closed = True

    def finish_exit(self, **kwargs):
        pass


@pytest.fixture
def boundary(tmp_path, monkeypatch):
    core, manager, calls = CoreService(tmp_path), Manager(), []
    core.shutdown_management = manager

    class LocalHTTP:
        def __init__(self, host, port, **kwargs):
            assert host == "127.0.0.1" and port == 8765

        def request(self, method, path, body, headers):
            payload = json.loads(body) if body is not None else None
            calls.append((method, path, payload))
            try:
                self.value = core.request(
                    method, path, headers["Authorization"].removeprefix("Bearer "), payload
                )
                self.code = 200
            except ValidationError:
                self.value, self.code = {"error": "ValidationError"}, 400

        def getresponse(self):
            response = BytesIO(json.dumps(self.value).encode())
            response.status = self.code
            return response

        def close(self):
            pass

    monkeypatch.setattr(
        "omega.runtime.console.probe_core", lambda *a, **k: {"url": "http://127.0.0.1:8765"}
    )
    monkeypatch.setattr("omega.runtime.console.http.client.HTTPConnection", LocalHTTP)
    output = []
    console = ControlConsole(tmp_path, output=output.append)
    yield console, core, manager, calls, output
    core.close()


def test_shutdown_status_is_read_only_and_available_through_owner_auth(boundary):
    console, _, manager, calls, _ = boundary
    assert console.execute("shutdown")["revision"] == 7
    assert console.execute("shutdown status")["core_epoch"] == manager.core_epoch
    assert [(method, path) for method, path, _ in calls] == [("GET", "/api/shutdown")] * 2
    assert manager.requests == []


@pytest.mark.parametrize(
    "verb,kind",
    [
        ("review", "review"),
        ("save", "save_exit"),
        ("flatten", "flatten_exit"),
        ("cancel", "cancel"),
    ],
)
def test_shutdown_actions_bind_fresh_epoch_revision_and_native_ack(boundary, verb, kind):
    console, _, manager, calls, _ = boundary
    row = console.execute("shutdown " + verb)
    assert row["kind"] == kind and row["expected_revision"] == 7
    assert row["core_epoch"] == manager.core_epoch and len(row["id"]) == 32
    assert row["parameters"] == (
        {"acknowledge_unmanaged_positions": True} if verb == "save" else {}
    )
    assert [(method, path) for method, path, _ in calls] == [
        ("GET", "/api/shutdown"),
        ("POST", "/api/shutdown"),
    ]
    assert len(manager.requests) == 1


def test_shutdown_transport_ack_loss_prints_id_and_never_reposts(boundary, monkeypatch):
    console, _, manager, calls, output = boundary
    original = console.request

    def lose(method, path, body=None):
        result = original(method, path, body)
        if method == "POST":
            raise OSError("synthetic response lost after submit")
        return result

    monkeypatch.setattr(console, "request", lose)
    with pytest.raises(OSError):
        console.execute("shutdown flatten")
    assert len(manager.requests) == 1
    identity = manager.requests[0]["id"]
    assert "request GET /api/shutdown/" + identity in output[-1]
    assert console.execute("request GET /api/shutdown/" + identity)["id"] == identity
    assert len([value for value in calls if value[0] == "POST"]) == 1


def test_remote_stopped_event_returns_without_another_input_line(boundary):
    console, _, manager, calls, _ = boundary
    entered, release, stopped = threading.Event(), threading.Event(), threading.Event()
    result = []
    prior = set(threading.enumerate())

    def blocked(prompt):
        entered.set()
        assert release.wait(3)
        raise EOFError

    worker = threading.Thread(
        target=lambda: result.append(
            console.loop(input_fn=blocked, stopped=stopped, shutdown_enabled=True)
        )
    )
    worker.start()
    try:
        assert entered.wait(1)
        stopped.set()
        worker.join(1)
        assert not worker.is_alive() and result == [0]
        assert not calls and not manager.closed
    finally:
        # Arbitrary input callbacks cannot be interrupted by a queue sentinel.
        # Release our fake explicitly, without claiming this resolves real stdin.
        release.set()
        worker.join(3)
        for thread in set(threading.enumerate()) - prior:
            if thread.name == "Omega-ConsoleInput":
                thread.join(1)


def test_embedded_exit_then_cancel_sends_no_stop_choice_and_keeps_core(boundary):
    console, _, manager, calls, _ = boundary
    lines = iter(("exit", "2"))

    def input_line(prompt):
        try:
            return next(lines)
        except StopIteration:
            raise EOFError from None

    assert console.loop(input_fn=input_line, stopped=threading.Event(), shutdown_enabled=True) == 0
    assert [row["kind"] for row in manager.requests] == ["review", "cancel"]
    assert not any(row["kind"] in {"save_exit", "flatten_exit"} for row in manager.requests)
    assert not any(path == "/api/commands" for _, path, _ in calls)
    assert not manager.closed and "core" in console.execute("status")


def test_standalone_exit_is_disconnect_without_shutdown_requests(boundary):
    console, _, manager, calls, _ = boundary
    assert console.loop(input_fn=lambda prompt: "exit") == 0
    assert calls == [] and manager.requests == [] and not manager.closed


def test_unknown_shutdown_command_is_rejected_before_any_http(boundary):
    console, _, manager, calls, _ = boundary
    with pytest.raises(ValueError, match="shutdown status"):
        console.execute("shutdown unexpected")
    assert calls == [] and manager.requests == []
