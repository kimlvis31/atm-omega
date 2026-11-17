"""Owner API shutdown connects real paper files, actor observation and core teardown."""

import threading
import time

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.app import build_core


def wait_for(read, predicate):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        result = read()
        if predicate(result):
            return result
        time.sleep(0.01)
    raise AssertionError("shutdown owner did not reach its expected state")


@pytest.mark.parametrize("kind", ["save_exit", "flatten_exit"])
def test_api_exit_ack_requires_core_cleanup_and_survives_restart(tmp_path, kind):
    core = build_core(tmp_path)
    stopped = threading.Event()
    manager = core.shutdown_management
    manager.on_exit = stopped.set
    core.margin_trading.start()
    manager.start()
    try:
        snapshot = wait_for(manager.snapshot, lambda row: row["ready"])
        body = {
            "id": "exit_test",
            "core_epoch": snapshot["core_epoch"],
            "expected_revision": snapshot["revision"],
            "kind": kind,
            "parameters": {"acknowledge_unmanaged_positions": True} if kind == "save_exit" else {},
        }
        assert core.state()["core"]["capabilities"]["shutdown_management"]
        core.request("POST", "/api/shutdown", core.token, body)
        assert stopped.wait(5)
        receipt = core.request("GET", "/api/shutdown/exit_test", core.token)
        assert receipt["state"] == "exit_requested"
        assert receipt["result"]["core_exit_confirmed"] is False
        assert core.control.snapshot()["paused"]
        with pytest.raises(ValidationError):
            core.request(
                "POST",
                "/api/commands",
                core.token,
                {"id": "bad_resume", "kind": "resume", "payload": {}},
            )
    finally:
        core.close()
    assert manager.status("exit_test")["result"]["core_exit_confirmed"] is True

    restored = build_core(tmp_path)
    try:
        restored.shutdown_management.start()
        wait_for(restored.shutdown_management.snapshot, lambda row: row["ready"])
        receipt = restored.request("GET", "/api/shutdown/exit_test", restored.token)
        assert receipt["state"] == "completed"
        assert receipt["result"]["core_exit_confirmed"] is True
        assert receipt["operation_core_epoch"] != receipt["core_epoch"]
        assert restored.shutdown_management.snapshot()["operation_id"] is None
        assert restored.control.snapshot()["paused"]
    finally:
        restored.close()


def test_exit_drain_keeps_core_alive_until_a_new_eligible_minute(tmp_path):
    core = build_core(tmp_path, demo=True)
    stopped = threading.Event()
    paper, manager = core.paper, core.shutdown_management
    # Explicit synthetic execution has no exchange rules or market data connection.
    paper.rules_provider = None
    paper.execution_clock_ms = manager.clock_ms = lambda: 120100
    manager.on_exit = stopped.set
    core.margin_trading.start()
    manager.start()

    def command(kind, payload):
        return core.request(
            "POST",
            "/api/commands",
            core.token,
            {"id": kind.replace(".", "_") + str(time.time_ns()), "kind": kind, "payload": payload},
        )

    def minute(index):
        return {
            "instrument": "binance:usdm:ETHUSDT",
            "candle": {
                "open_time_ms": index * 60000,
                "open": "100",
                "high": "100",
                "low": "100",
                "close": "100",
                "volume": "10",
            },
        }

    try:
        command("paper.account.create", {"account_id": "a", "balance": "10000"})
        command(
            "paper.strategy.add",
            {
                "instance": "s",
                "account_id": "a",
                "instrument": "binance:usdm:ETHUSDT",
                "budget": "1000",
                "module_id": "target_quantity",
                "configuration": {"quantity": "1"},
            },
        )
        control = core.control.snapshot()
        response = command(
            "resume",
            {
                "expected_epoch": control["epoch"],
                "expected_revision": control["control_revision"],
                "expires_at_ms": control["server_time_ms"] + 5000,
            },
        )
        assert response["status"] == "completed"
        for index in range(2):
            command("paper.step", minute(index))
        assert paper.data["strategies"]["s"]["quantity"] == "1"
        snapshot = wait_for(manager.snapshot, lambda row: row["ready"])
        core.request(
            "POST",
            "/api/shutdown",
            core.token,
            {
                "id": "open_exit",
                "core_epoch": snapshot["core_epoch"],
                "expected_revision": snapshot["revision"],
                "kind": "flatten_exit",
                "parameters": {},
            },
        )
        wait_for(lambda: manager.status("open_exit"), lambda row: row["state"] == "waiting_fill")
        assert not stopped.is_set()
        command("paper.step", minute(2))
        assert not stopped.is_set() and paper.data["strategies"]["s"]["pending"]
        command("paper.step", minute(3))
        assert stopped.wait(5)
        assert paper.data["strategies"]["s"]["quantity"] == "0"
        assert paper.data["strategies"]["s"]["pending"] is None
    finally:
        core.close()
    assert manager.status("open_exit")["result"]["core_exit_confirmed"] is True
