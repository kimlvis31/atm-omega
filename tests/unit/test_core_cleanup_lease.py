"""An unconfirmed child shutdown must not free the namespace for another core."""

from hashlib import sha256
from threading import BoundedSemaphore, Lock
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError
from omega.runtime.service import BoundedHTTPServer, CoreService


class PendingCleanup:
    def __init__(self):
        self.attempts = 0

    def close(self):
        self.attempts += 1
        if self.attempts == 1:
            raise StorageError("owned child cleanup is not confirmed")


def test_core_retains_real_root_lease_until_explicit_cleanup_retry(tmp_path):
    resource = PendingCleanup()
    core = CoreService(tmp_path, margin_trading=resource)
    try:
        with pytest.raises(StorageError, match="cleanup"):
            core.close()
        assert core._closing and resource.attempts == 1
        # This performs another actual native lock acquisition, not a mock flag.
        with pytest.raises(RuntimeError, match="already owns"):
            CoreService(tmp_path)
        core.close()
        assert resource.attempts == 2
        replacement = CoreService(tmp_path)
        replacement.close()
    finally:
        core.close()


@pytest.mark.parametrize("released_before_error", [False, True])
def test_root_native_close_fault_preserves_receipt_and_old_core_cannot_write_new_owner(
    tmp_path, released_before_error
):
    from test_shutdown_service import owner, submit, wait

    core = CoreService(tmp_path)
    context = owner(tmp_path, epoch=core.control.epoch)
    core.shutdown_management = context.service
    submit(context, "exit", "save_exit", {"acknowledge_unmanaged_positions": True})
    wait(lambda: context.service.status("exit"), lambda row: row["state"] == "exit_requested")
    native_close = core.instance.close
    attempts = []

    def close_once():
        attempts.append(True)
        if len(attempts) == 1:
            if released_before_error:
                native_close()
            raise OSError("injected root native close boundary")
        native_close()

    core.instance.close = close_once
    replacement = None
    try:
        with pytest.raises(OSError, match="native close"):
            core.close()
        receipt = context.service.status("exit")
        if not released_before_error:
            assert receipt["state"] == "failed"
            assert receipt["result"]["core_exit_confirmed"] is False
            with pytest.raises(RuntimeError, match="already owns"):
                CoreService(tmp_path)
            core.close()
        else:
            assert receipt["state"] == "completed"
            assert receipt["result"]["core_exit_confirmed"] is True
        replacement = CoreService(tmp_path)
        replacement.control.incident("NEW_OWNER", "info")
        files = {
            path: sha256(path.read_bytes()).hexdigest()
            for path in (tmp_path / "state").rglob("*.json")
        }
        core.close()
        assert {path: sha256(path.read_bytes()).hexdigest() for path in files} == files
        assert len(attempts) == (1 if released_before_error else 2)
    finally:
        core.instance.close = native_close
        core.close()
        if replacement is not None:
            replacement.close()


@pytest.mark.parametrize("phase", ["shutdown", "server_close", "join"])
def test_http_failure_retains_same_server_until_every_phase_is_confirmed(tmp_path, phase):
    calls = {key: 0 for key in ("shutdown", "server_close", "join")}

    def action(name):
        calls[name] += 1
        if phase == name and calls[name] == 1 and name != "join":
            raise OSError("injected HTTP cleanup failure")

    def join(**kwargs):
        assert kwargs == {"timeout": 5}
        action("join")

    thread = SimpleNamespace(
        ident=1,
        join=join,
        is_alive=lambda: calls["join"] == 0 or phase == "join" and calls["join"] == 1,
    )
    server = SimpleNamespace(
        shutdown=lambda: action("shutdown"), server_close=lambda: action("server_close")
    )
    core = CoreService(tmp_path)
    core.server, core.thread = server, thread
    try:
        with pytest.raises((OSError, StorageError), match="HTTP"):
            core.close()
        assert core.server is server
        with pytest.raises(RuntimeError, match="already owns"):
            CoreService(tmp_path)
        core.close()
        assert core.server is None and core._cleanup_complete
        assert calls == {name: 2 if name == phase else 1 for name in calls}
    finally:
        core.close()


def test_http_worker_join_timeout_does_not_confirm_server_cleanup():
    alive, joins = [True], []

    class Worker:
        def join(self, *, timeout):
            joins.append(timeout)

        def is_alive(self):
            return alive[0]

    server = object.__new__(BoundedHTTPServer)
    server.socket = SimpleNamespace(close=lambda: None)
    server.workers_lock, server.workers = Lock(), {Worker()}
    server.slots = BoundedSemaphore(1)
    assert server.slots.acquire(blocking=False)
    with pytest.raises(StorageError, match="request worker"):
        server.server_close()
    assert not server.slots.acquire(blocking=False)
    alive[0] = False
    server.server_close()
    assert len(joins) == 2 and all(0 <= value <= 6 for value in joins)
    assert not server.workers and server.slots.acquire(blocking=False)
    server.slots.release()


def test_http_worker_start_failure_returns_admission_slot(monkeypatch):
    from omega.runtime import service as module

    server = object.__new__(BoundedHTTPServer)
    server.slots, server.workers_lock, server.workers = BoundedSemaphore(1), Lock(), set()
    closed = []
    server.shutdown_request = lambda request: closed.append(request)
    request = SimpleNamespace(settimeout=lambda value: None)

    class Unstarted:
        ident = None

        def __init__(self, **kwargs):
            pass

        def start(self):
            raise RuntimeError("cannot start request worker")

    monkeypatch.setattr(module.threading, "Thread", Unstarted)
    with pytest.raises(RuntimeError, match="cannot start"):
        server.process_request(request, ("127.0.0.1", 1))
    assert closed == [request] and not server.workers
    assert server.slots.acquire(blocking=False)
    server.slots.release()


def test_failed_listener_start_does_not_wait_for_serve_forever(tmp_path, monkeypatch):
    from omega.runtime import service as module

    def forbidden_shutdown():
        pytest.fail("shutdown would wait forever for a listener that never started")

    server = SimpleNamespace(
        server_port=8765,
        serve_forever=lambda: None,
        shutdown=forbidden_shutdown,
        server_close=lambda: None,
    )

    class Unstarted:
        ident = None

        def __init__(self, **kwargs):
            pass

        def start(self):
            raise RuntimeError("cannot start listener")

        def join(self, **kwargs):
            pytest.fail("cannot join a listener that never started")

        def is_alive(self):
            return False

    core = CoreService(tmp_path)
    monkeypatch.setattr(module, "BoundedHTTPServer", lambda *args: server)
    monkeypatch.setattr(module.threading, "Thread", Unstarted)
    with pytest.raises(RuntimeError, match="cannot start listener"):
        core.start()
    core.close()
    assert core._cleanup_complete and core.server is None


@pytest.mark.parametrize("document", ["record", "index"])
@pytest.mark.parametrize("committed_before_error", [False, True])
def test_exit_completion_ack_loss_retries_guard_before_native_release_failure(
    tmp_path, document, committed_before_error
):
    from test_shutdown_service import owner, submit, wait

    core = CoreService(tmp_path)
    context = owner(tmp_path, epoch=core.control.epoch)
    core.shutdown_management = context.service
    submit(context, "exit", "save_exit", {"acknowledge_unmanaged_positions": True})
    wait(lambda: context.service.status("exit"), lambda row: row["state"] == "exit_requested")
    write, native_close = context.service._write, core.instance.close
    write_failed, native_failed = [], []

    def uncertain_write(path, value):
        kind = "index" if path.name == "index.json" else "record"
        if kind == document and not write_failed:
            write_failed.append(True)
            if committed_before_error:
                write(path, value)
            raise StorageError("completion ACK lost")
        write(path, value)

    def uncertain_release():
        if not native_failed:
            native_failed.append(True)
            raise OSError("native release failed before closing")
        native_close()

    context.service._write = uncertain_write
    core.instance.close = uncertain_release
    try:
        with pytest.raises(StorageError, match="ACK lost"):
            core.close()
        assert not core.instance.released and not native_failed
        with pytest.raises(OSError, match="native release"):
            core.close()
        receipt = context.service.status("exit")
        assert receipt["state"] == "failed"
        assert receipt["result"]["core_exit_confirmed"] is False
        assert receipt["error"] == "root_lease_release_unconfirmed"
        assert not core.instance.released
        core.close()
        assert core.instance.released
        assert context.service.status("exit") == receipt
    finally:
        context.service._write, core.instance.close = write, native_close
        core.close()
