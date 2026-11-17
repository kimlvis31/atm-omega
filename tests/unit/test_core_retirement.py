"""A stopped owner cannot persist a late request after another core acquires ownership."""

import hashlib
import json
import threading
import time
from copy import deepcopy
from email.message import Message
from io import BytesIO

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.runtime.service import CoreService


def test_close_waits_for_an_accepted_writer_then_blocks_late_command_receipts(tmp_path):
    core = CoreService(tmp_path)
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    responses = []

    def slow(kind, payload, **_kwargs):
        entered.set()
        assert release.wait(5)
        return {"saved": True}

    core._execute = slow
    writer = threading.Thread(
        target=lambda: responses.append(
            core.request(
                "POST", "/api/commands", core.token, {"id": "old", "kind": "fixture", "payload": {}}
            )
        )
    )
    writer.start()
    assert entered.wait(2)
    closer = threading.Thread(target=lambda: (core.close(), closed.set()))
    closer.start()
    try:
        deadline = time.monotonic() + 2
        while not core._closing and time.monotonic() < deadline:
            time.sleep(0.01)
        assert not closed.is_set()
        with pytest.raises(RuntimeError):
            CoreService(tmp_path)
    finally:
        release.set()
        writer.join(3)
        closer.join(3)
    assert closed.is_set() and responses[0]["status"] == "completed"
    new = CoreService(tmp_path)
    try:
        before = hashlib.sha256((tmp_path / "state/control.json").read_bytes()).hexdigest()
        with pytest.raises(ValidationError, match="stopping"):
            core.request(
                "POST", "/api/commands", core.token, {"id": "late", "kind": "pause", "payload": {}}
            )
        assert hashlib.sha256((tmp_path / "state/control.json").read_bytes()).hexdigest() == before
    finally:
        new.close()


def test_notification_close_failure_does_not_skip_other_resource_cleanup(tmp_path):
    core = CoreService(tmp_path)
    closed = []

    class Resource:
        def __init__(self, name):
            self.name = name

        def close(self):
            closed.append(self.name)

    core.market_data = Resource("market")
    core.research = Resource("research")

    def fail():
        raise StorageError("speech cleanup failure")

    close_notifications = core.notifications.close
    core.notifications.close = fail
    with pytest.raises(StorageError):
        core.close()
    assert closed == ["market", "research"]
    with pytest.raises(RuntimeError, match="already owns"):
        CoreService(tmp_path)
    assert core.instance.released is False
    core.notifications.close = close_notifications
    core.close()
    assert core.instance.released is True
    new = CoreService(tmp_path)
    new.close()


@pytest.mark.parametrize("failure", [OSError, RuntimeError])
def test_admitted_get_error_after_owner_release_cannot_overwrite_new_core_state(
    tmp_path, monkeypatch, failure
):
    from omega.runtime import service as module

    class InertServer:
        """Models a handler outliving server_close's bounded join; no socket exists."""

        def __init__(self, address, handler):
            self.server_port = address[1]
            self.stopped = threading.Event()

        def serve_forever(self):
            self.stopped.wait()

        def shutdown(self):
            self.stopped.set()

        def server_close(self):
            pass

    monkeypatch.setattr(module, "BoundedHTTPServer", InertServer)
    entered, release = threading.Event(), threading.Event()

    def late_provider(*_args):
        entered.set()
        assert release.wait(5)
        raise failure("injected readonly provider failure")

    old = CoreService(tmp_path, candles_provider=late_provider)
    old.start(0)
    handler = object.__new__(old.handler_class)
    handler.server, handler.headers = old.server, Message()
    handler.headers["Host"] = "127.0.0.1:0"
    handler.headers["Authorization"] = "Bearer " + old.token
    handler.path = "/api/candles?instrument=binance:spot:BTCUSDT"
    handler.rfile, handler.wfile = BytesIO(), BytesIO()
    statuses = []
    handler.send_response = lambda code: statuses.append(code)
    handler.send_header = lambda *_args: None
    handler.end_headers = lambda: None
    worker = threading.Thread(target=lambda: handler._handle("GET"))
    worker.start()
    current = None
    try:
        assert entered.wait(2)
        old.close()
        # This must acquire the real OS instance lock before the stale GET returns.
        current = CoreService(tmp_path)
        current.record_incident("NEW_OWNER_MARKER", "warning")
        path = tmp_path / "state/control.json"
        expected_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        old_state = deepcopy(old.control.data)
        late_logs = []
        old.logger.write = lambda *_args, **_kwargs: late_logs.append("unexpected late log")
        release.set()
        worker.join(2)
        assert not worker.is_alive()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected_hash
        assert old.control.data == old_state
        assert not late_logs
        assert statuses == [503 if failure is OSError else 500]
        assert any(
            row["code"] == "NEW_OWNER_MARKER" for row in json.loads(path.read_text())["incidents"]
        )

        # The secondary logging-failure path and direct delayed incident are fenced too.
        def broken_log(*_args, **_kwargs):
            raise OSError("late logger unavailable")

        old.logger.write = broken_log
        old.log("system", "ERROR", "LATE_ERROR")
        old.record_incident("LATE_TRANSPORT_ERROR")
        old.control.incident("LATE_DIRECT_INCIDENT")
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected_hash
    finally:
        release.set()
        worker.join(2)
        if current:
            current.close()
        else:
            old.close()


def test_retired_control_rejects_all_mutations_without_writes_or_callback(tmp_path):
    core = CoreService(tmp_path)
    pairing = core.control.create_pairing("phone", ["read", "pause"])
    device = core.control.pair(pairing["code"])
    core.control.incident("BEFORE_RETIRE")
    incident = core.control.snapshot()["incidents"][0]["id"]
    core.close()
    before = (tmp_path / "state/control.json").read_bytes()
    old_state, calls = deepcopy(core.control.data), []
    for action in (
        lambda: core.control.command(
            "admin",
            {"id": "late", "kind": "pause", "payload": {}},
            lambda *_: calls.append("provider"),
        ),
        lambda: core.control.create_pairing("late"),
        lambda: core.control.pair("expired"),
        lambda: core.control.revoke(device["device_id"]),
        lambda: core.control.acknowledge(incident),
        core.control._save,
        lambda: core.start(0),
    ):
        with pytest.raises(ValidationError, match="retired|stopping"):
            action()
    core.control.incident("IGNORED_RETIRED_DIAGNOSTIC")
    assert core.control.data == old_state and not calls
    assert (tmp_path / "state/control.json").read_bytes() == before


def test_close_waits_for_a_direct_incident_commit_before_releasing_owner(tmp_path, monkeypatch):
    from omega.runtime import control as module

    core = CoreService(tmp_path)
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    original = module.atomic_json

    def blocking_save(path, data):
        entered.set()
        assert release.wait(3)
        original(path, data)

    with monkeypatch.context() as patch:
        patch.setattr(module, "atomic_json", blocking_save)
        diagnostic = threading.Thread(target=lambda: core.control.incident("ACCEPTED_DIAGNOSTIC"))
        diagnostic.start()
        assert entered.wait(1)
        closer = threading.Thread(target=lambda: (core.close(), closed.set()))
        closer.start()
        try:
            assert not closed.wait(0.02)
            with pytest.raises(RuntimeError, match="already owns"):
                CoreService(tmp_path)
        finally:
            release.set()
            diagnostic.join(2)
            closer.join(2)
    assert closed.is_set() and not diagnostic.is_alive()
    current = CoreService(tmp_path)
    try:
        assert any(
            row["code"] == "ACCEPTED_DIAGNOSTIC" for row in current.control.snapshot()["incidents"]
        )
        before = (tmp_path / "state/control.json").read_bytes()
        core.control.incident("RETIRED_DIAGNOSTIC")
        assert (tmp_path / "state/control.json").read_bytes() == before
    finally:
        current.close()
