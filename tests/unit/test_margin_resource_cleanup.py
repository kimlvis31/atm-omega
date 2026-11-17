"""Real owned OS leases, faulted cleanup/retry and unchanged financial records."""

import json
import subprocess
import sys
import threading

import pytest
from test_margin_session import configuration
from test_margin_strategy_costs import policy
from test_margin_trading import Source, await_value

from omega.domain.errors import StorageError, TradingBlocked
from omega.runtime.control import InstanceLock
from omega.runtime.margin_cleanup import MarginCleanup, confirmed_cleanup
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration
from omega.runtime.margin_store import FileMarginStateStore
from omega.runtime.margin_strategy_store import FileMarginStrategyStore
from omega.runtime.margin_trading import MarginTradingService
from omega.runtime.virtual_margin_journal import FileVirtualMarginJournal


def probe(path):
    script = """
import sys
from pathlib import Path
from omega.runtime.control import InstanceLock
try:
    lease = InstanceLock(Path(sys.argv[1]))
except RuntimeError:
    print('held')
else:
    lease.close()
    print('released')
"""
    observed = subprocess.run(
        [sys.executable, "-I", "-c", script, str(path)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert observed.returncode == 0, observed.stderr
    return observed.stdout.strip()


def fault_once(lease, when, monkeypatch):
    original, calls = lease.close, []

    def close():
        calls.append(threading.get_ident())
        if len(calls) == 1:
            if when == "after":
                original()
            raise OSError("one injected lease close failure")
        return original()

    monkeypatch.setattr(lease, "close", close)
    return calls


def financial_bytes(root):
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*.json")
        if path.name != "margin-runtime.json"
    }


@pytest.mark.parametrize("when", ["before", "after"])
def test_instance_lock_observes_its_native_stream_and_real_child_acquisition(
    tmp_path, monkeypatch, when
):
    lease = InstanceLock(tmp_path / "native.lock")
    assert lease.released is False and probe(tmp_path / "native.lock") == "held"
    original = lease.stream

    class FaultingStream:
        @property
        def closed(self):
            return original.closed

        def close(self):
            if when == "after":
                original.close()
            raise OSError("injected stream close")

    monkeypatch.setattr(lease, "stream", FaultingStream())
    try:
        with pytest.raises(OSError):
            lease.close()
        assert lease.released is (when == "after")
        assert probe(tmp_path / "native.lock") == ("released" if when == "after" else "held")
    finally:
        monkeypatch.setattr(lease, "stream", original)
        lease.close()
    assert lease.released and probe(tmp_path / "native.lock") == "released"


@pytest.mark.parametrize("kind", ["account", "strategy", "journal"])
@pytest.mark.parametrize("when", ["before", "after"])
def test_store_closed_gate_does_not_skip_held_native_lease_retry(tmp_path, monkeypatch, kind, when):
    constructor = {
        "account": FileMarginStateStore,
        "strategy": FileMarginStrategyStore,
        "journal": FileVirtualMarginJournal,
    }[kind]
    store = constructor(tmp_path, "cleanup")
    before = financial_bytes(tmp_path)
    calls = fault_once(store.owner, when, monkeypatch)
    try:
        assert probe(store.lock_path) == "held"
        with pytest.raises(OSError):
            store.close()
        assert store.closed is True and store.released is (when == "after")
        assert probe(store.lock_path) == ("released" if when == "after" else "held")
        store.close()
        assert len(calls) == (2 if when == "before" else 1)
        assert store.released and probe(store.lock_path) == "released"
        assert financial_bytes(tmp_path) == before
    finally:
        store.close()


@pytest.mark.parametrize("when", ["before", "after"])
def test_session_holds_outer_lease_until_children_confirm_and_preserves_first_error(
    tmp_path, monkeypatch, when
):
    account, strategies = configuration()
    session = MarginTradingSession(tmp_path, account, strategies, 0, create=True)
    before = financial_bytes(tmp_path)
    child = session._resources[-1]
    calls = fault_once(child.owner, when, monkeypatch)
    fresh = session.cleanup_status()
    try:
        assert fresh["logical_closed"] is False and fresh["state"] == "not_started"
        with pytest.raises(StorageError):
            session.close()
        status = session.cleanup_status()
        assert status["generation"] == fresh["generation"] and status["logical_closed"] is True
        assert status["state"] == ("pending" if when == "before" else "complete")
        assert probe(session.lock_path) == ("held" if when == "before" else "released")
        errors = [row for row in status["resources"] if row["error_type"]]
        assert len(errors) == 1 and errors[0]["error_type"] == "OSError"
        completed = {
            row["name"]: row["attempts"]
            for row in status["resources"]
            if row["state"] == "complete"
        }
        with pytest.raises(TradingBlocked):
            session.resume("cannot-reopen")
        wrong_owner = []

        def foreign():
            try:
                session.close()
            except Exception as error:
                wrong_owner.append(type(error))

        worker = threading.Thread(target=foreign)
        worker.start()
        worker.join(1)
        assert wrong_owner == [TradingBlocked] and not worker.is_alive()
        assert len(calls) == 1
        session.close()
        final = session.cleanup_status()
        assert confirmed_cleanup(final, generation=fresh["generation"])
        assert final["generation"] == fresh["generation"]
        assert [row["error_type"] for row in final["resources"] if row["error_type"]] == ["OSError"]
        assert all(
            row["attempts"] == completed[row["name"]]
            for row in final["resources"]
            if row["name"] in completed
        )
        assert probe(session.lock_path) == probe(child.lock_path) == "released"
        assert financial_bytes(tmp_path) == before
        with pytest.raises(TradingBlocked):
            session.resume("still-cannot-reopen")
    finally:
        session.close()


def test_returning_none_while_lease_held_never_completes_session_cleanup(tmp_path, monkeypatch):
    account, strategies = configuration()
    session = MarginTradingSession(tmp_path, account, strategies, 0, create=True)
    child = session._resources[-1]
    original = child.owner.close
    monkeypatch.setattr(child.owner, "close", lambda: None)
    try:
        for _ in range(2):
            with pytest.raises(StorageError):
                session.close()
            assert session.cleanup_status()["state"] == "pending"
            assert probe(session.lock_path) == probe(child.lock_path) == "held"
    finally:
        monkeypatch.setattr(child.owner, "close", original)
        session.close()


def test_legacy_failure_cannot_be_confirmed_by_later_none_or_closed_boolean():
    cleanup = MarginCleanup()

    class Legacy:
        closed = False
        calls = 0

        def close(self):
            self.calls += 1
            self.closed = True
            if self.calls == 1:
                raise OSError("unknown")

    resource = Legacy()
    cleanup.add("legacy", resource, initial_ack=True)
    for _ in range(2):
        with pytest.raises(StorageError):
            cleanup.close()
    status = cleanup.status()
    assert resource.closed and resource.calls == 2
    assert status["state"] == "pending" and status["resources"][0]["error_type"] == "OSError"
    assert not confirmed_cleanup(status)


@pytest.mark.parametrize("fault", ["generation", "scope", "resource", "logical_gate"])
def test_typed_cleanup_confirmation_rejects_changed_instance_or_incomplete_lease(fault):
    cleanup = MarginCleanup()
    cleanup.close()
    status = cleanup.status()
    assert confirmed_cleanup(status, generation=cleanup.generation)
    if fault == "generation":
        status["generation"] = "0" * 32
    elif fault == "scope":
        status["scope"] = "financial_authority"
    elif fault == "resource":
        status["resources"] = [
            {"name": "lease", "state": "pending", "attempts": 1, "error_type": None}
        ]
    else:
        status["logical_closed"] = False
    assert not confirmed_cleanup(status, generation=cleanup.generation)


def test_cleanup_status_does_not_wait_for_a_blocked_native_close_callback():
    started, release = threading.Event(), threading.Event()
    cleanup = MarginCleanup()

    class Blocking:
        def close(self):
            started.set()
            assert release.wait(5)

    cleanup.add("lease", Blocking(), released=release.is_set)
    errors = []

    def close():
        try:
            cleanup.close()
        except Exception as error:
            errors.append(type(error))

    worker = threading.Thread(target=close)
    worker.start()
    try:
        assert started.wait(1)
        status = cleanup.status()
        assert status["state"] == "pending" and status["resources"][0]["attempts"] == 1
        status["resources"][0]["name"] = "changed"
        assert cleanup.status()["resources"][0]["name"] == "lease"
    finally:
        release.set()
        worker.join(2)
    assert not worker.is_alive() and not errors
    assert confirmed_cleanup(cleanup.status())


@pytest.mark.parametrize("when", ["before", "after"])
def test_runtime_retries_same_native_session_on_its_original_owner_thread(
    tmp_path, monkeypatch, when
):
    sessions, calls = [], []

    class ActualSession(MarginTradingSession):
        prepare_configuration = staticmethod(prepare_margin_configuration)

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            calls.append(fault_once(self._resources[-1].owner, when, monkeypatch))
            sessions.append(self)

    account, strategies = configuration()
    runtime = MarginTradingService(
        tmp_path, None, lambda: Source(), core_epoch="cleanup", session_factory=ActualSession
    )
    runtime.start()
    await_value(runtime.snapshot, lambda value: value["state"] == "ready")
    runtime.submit(
        "create",
        "configure",
        {"account": account, "strategies": strategies, "start_ms": 0, "brackets": []},
    )
    await_value(lambda: runtime.status("create"), lambda value: value["state"] == "completed")
    before = financial_bytes(tmp_path)
    identity = runtime.thread.ident
    try:
        with pytest.raises(StorageError, match="resource cleanup"):
            runtime.close()
        if when == "before":
            assert (
                runtime.thread.is_alive() and runtime.snapshot()["error"] == "cleanup_unconfirmed"
            )
            assert probe(sessions[0].lock_path) == "held"
        runtime.close()
        assert not runtime.thread.is_alive()
        assert runtime.snapshot()["error"] is None
        assert confirmed_cleanup(runtime.cleanup_status())
        assert all(thread == identity for thread in calls[0])
        assert len(calls[0]) == (2 if when == "before" else 1)
        assert financial_bytes(tmp_path) == before
        assert runtime.data["accounts"]["session-test"]["feed_enabled"] is False
    finally:
        runtime.close()


@pytest.mark.parametrize("when", ["before", "after"])
def test_rejected_cost_session_final_retry_actually_releases_original_native_leases(
    tmp_path, monkeypatch, when
):
    sessions, calls = [], []

    class RejectedSession(MarginTradingSession):
        prepare_configuration = staticmethod(prepare_margin_configuration)

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            calls.append(fault_once(self._resources[-1].owner, when, monkeypatch))
            sessions.append(self)

        def snapshot(self):
            result = super().snapshot()
            result["cost_policy"]["scope"] = "unsupported"
            return result

    account, strategies = configuration()
    runtime = MarginTradingService(
        tmp_path, None, lambda: Source(), core_epoch="rejected", session_factory=RejectedSession
    )
    runtime.start()
    await_value(runtime.snapshot, lambda value: value["state"] == "ready")
    runtime.submit(
        "create",
        "configure",
        {
            "account": account,
            "strategies": strategies,
            "start_ms": 0,
            "brackets": [],
            "cost_policy": policy().document(),
        },
    )
    await_value(lambda: runtime._cleanup_complete, lambda value: value is True)
    try:
        assert runtime._rejected_sessions == sessions
        assert confirmed_cleanup(sessions[0].cleanup_status())
        assert len(calls[0]) == (2 if when == "before" else 1)
        assert probe(sessions[0].lock_path) == "released"
        assert not runtime.sessions
        assert runtime.status("create")["state"] == "unknown"
        assert runtime.data["accounts"]["session-test"]["creation_state"] == "creating"
        assert json.dumps(runtime.cleanup_status()).find(str(tmp_path)) == -1
    finally:
        runtime.close()


def test_late_success_after_reported_timeout_remains_visible_until_explicit_close(
    tmp_path, monkeypatch
):
    started, release = threading.Event(), threading.Event()

    class SlowSession(MarginTradingSession):
        prepare_configuration = staticmethod(prepare_margin_configuration)

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            lease = self._resources[-1].owner
            original = lease.close

            def slow_close():
                started.set()
                assert release.wait(5)
                original()

            monkeypatch.setattr(lease, "close", slow_close)

    account, strategies = configuration()
    runtime = MarginTradingService(
        tmp_path, None, lambda: Source(), core_epoch="late", session_factory=SlowSession
    )
    runtime.start()
    await_value(runtime.snapshot, lambda value: value["state"] == "ready")
    runtime.submit(
        "create",
        "configure",
        {"account": account, "strategies": strategies, "start_ms": 0, "brackets": []},
    )
    await_value(lambda: runtime.status("create"), lambda value: value["state"] == "completed")
    original_wait = runtime.condition.wait_for

    def timeout_once(predicate, timeout=None):
        # Trigger exactly the caller timeout branch; owner waits and the actual
        # blocked native-lease close still execute on the original worker.
        if threading.get_ident() != runtime.thread.ident and timeout == 12:
            return False
        return original_wait(predicate, timeout)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(runtime.condition, "wait_for", timeout_once)
            with pytest.raises(StorageError, match="resource cleanup"):
                runtime.close()
        assert started.wait(1)
        assert runtime.snapshot()["error"] == "cleanup_unconfirmed"
        release.set()
        await_value(lambda: runtime._cleanup_complete, lambda value: value is True)
        runtime.thread.join(1)
        assert not runtime.thread.is_alive()
        assert confirmed_cleanup(runtime.cleanup_status())
        assert runtime.snapshot()["error"] == "cleanup_unconfirmed"
        assert runtime.inspect_shutdown()["unknown"] == ["owner_cleanup_unconfirmed"]
        attempts = runtime.cleanup_status()
        runtime.close()
        assert runtime.snapshot()["error"] is None
        assert runtime.cleanup_status() == attempts
    finally:
        release.set()
        runtime.close()
