"""Only Popen handles created by this owner may be stopped or replaced."""

import json
import subprocess
import threading

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.runtime import market_data_worker as module


class Input:
    def __init__(self):
        self.messages = []
        self.closed = False

    def write(self, value):
        self.messages.append(value)

    def flush(self):
        pass

    def close(self):
        self.closed = True


class Process:
    def __init__(self):
        self.stdin = Input()
        self.code = None
        self.terminated = False
        self.timeout = False

    def poll(self):
        return self.code

    def wait(self, timeout):
        if self.timeout and not self.terminated:
            raise subprocess.TimeoutExpired("owned collector", timeout)
        self.code = 0
        return 0

    def terminate(self):
        self.terminated = True


def test_two_market_processes_are_hidden_and_reconfiguration_stops_only_owned_handles(
    tmp_path, monkeypatch
):
    processes, calls = [], []

    def spawn(command, **options):
        process = Process()
        processes.append(process)
        calls.append((command, options))
        return process

    monkeypatch.setattr(module.subprocess, "Popen", spawn)
    owner = module.OwnedCollectors(tmp_path)
    owner.configure("usdm", ["binance:usdm:BTCUSDT"], 500, True)
    owner.configure("spot", ["binance:spot:ETHUSDT"], 500, False)
    assert len(owner.children) == 2
    assert owner.running("usdm")
    for command, options in calls:
        assert command[command.index("--root") + 1] == str(tmp_path.resolve())
        assert command[command.index("--db-config") + 1] == str(tmp_path / "database.config.toml")
        assert options["stdin"] == subprocess.PIPE
        if module.os.name == "nt":
            assert options["creationflags"] == subprocess.CREATE_NO_WINDOW
        else:
            assert options["start_new_session"] is True
    owner.configure("usdm", ["binance:usdm:SOLUSDT"], 200, True)
    assert processes[0].stdin.messages == ['{"command":"stop"}\n']
    assert processes[0].stdin.closed and not processes[0].terminated
    assert processes[1].stdin.messages == []
    owner.close()
    assert not owner.children
    with pytest.raises(StorageError):
        owner.configure("spot", ["binance:spot:ETHUSDT"], 500, True)


def test_timeout_termination_cannot_target_any_process_not_created_here(tmp_path, monkeypatch):
    process = Process()
    process.timeout = True
    monkeypatch.setattr(module.subprocess, "Popen", lambda *_args, **_kwargs: process)
    owner = module.OwnedCollectors(tmp_path)
    owner.stop("spot")
    assert not process.terminated
    owner.configure("spot", ["binance:spot:ETHUSDT"], 500, True)
    owner.stop("spot")
    assert process.terminated
    owner.close()


def test_symbol_history_plan_reaches_each_actual_collector_start(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        module.subprocess, "Popen", lambda command, **_kwargs: calls.append(command) or Process()
    )
    owner = module.OwnedCollectors(tmp_path)
    keys = ["binance:usdm:BTCUSDT", "binance:usdm:ETHUSDT"]
    owner.configure("usdm", keys, 20000, True, starts={keys[0]: 0, keys[1]: 20000})
    command = calls[0]
    plan = json.loads(command[command.index("--start-plan") + 1])
    instruments = tuple(Instrument("binance", Market.USDM, key.split(":")[2]) for key in keys)
    starts = module.collection_starts(instruments, 20000, plan, 2_000_000_012)
    assert starts[instruments[0]] == 1_999_980_000
    assert starts[instruments[1]] == 799_980_000
    owner.close()


@pytest.mark.parametrize("plan", [{}, {"BTCUSDT": True}, {"BTCUSDT": -1}, {"BTCUSDT": 20001}, []])
def test_invalid_or_incomplete_start_plan_is_rejected(plan):
    instruments = (Instrument("binance", Market.USDM, "BTCUSDT"),)
    with pytest.raises(ValidationError):
        module.collection_starts(instruments, 500, plan, 2_000_000_000)


def test_closing_during_reconfiguration_never_spawns_a_new_orphan(tmp_path, monkeypatch):
    processes = []

    def spawn(*_args, **_kwargs):
        process = Process()
        processes.append(process)
        return process

    monkeypatch.setattr(module.subprocess, "Popen", spawn)
    owner = module.OwnedCollectors(tmp_path)
    owner.configure("spot", ["binance:spot:ETHUSDT"], 500, True)
    entered, release = threading.Event(), threading.Event()

    def wait(timeout):
        entered.set()
        assert release.wait(2)
        processes[0].code = 0
        return 0

    processes[0].wait = wait
    configure = threading.Thread(
        target=lambda: owner.configure("spot", ["binance:spot:BTCUSDT"], 500, True)
    )
    configure.start()
    assert entered.wait(1)
    shutdown = threading.Thread(target=owner.close)
    shutdown.start()
    assert owner.closed.wait(1)
    release.set()
    configure.join(2)
    shutdown.join(2)
    assert not configure.is_alive() and not shutdown.is_alive()
    assert len(processes) == 1 and not owner.children


def test_process_alive_is_not_reported_as_data_ready_and_stale_health_is_rejected(
    tmp_path, monkeypatch
):
    process, key = Process(), "binance:spot:BTCUSDT"
    monkeypatch.setattr(module.subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(module.time, "time_ns", lambda: 180_001 * 1_000_000)
    owner = module.OwnedCollectors(tmp_path)
    owner.configure("spot", [key], 1, True)
    assert owner.status("spot")["state"] == "starting"
    path = owner.children["spot"][2].with_suffix(".health.json")
    record = {
        "schema_version": 1,
        "market": "spot",
        "instruments": [key],
        "updated_at_ms": 180_000,
        "ready": True,
        "reason": "ready",
        "instrument_ready": {key: True},
        "last_close_ms": {key: 180_000},
    }
    path.write_text(json.dumps(record))
    assert owner.status("spot")["ready"] is True
    monkeypatch.setattr(module.time, "time_ns", lambda: 240_001 * 1_000_000)
    record["updated_at_ms"] = 239_999
    path.write_text(json.dumps(record))
    assert owner.status("spot")["state"] == "stale"
    assert not owner.status("spot")["ready"]
    monkeypatch.setattr(module.time, "time_ns", lambda: 180_001 * 1_000_000)
    record["updated_at_ms"] = 120_000
    path.write_text(json.dumps(record))
    assert owner.status("spot")["state"] == "health_unavailable"
    assert not owner.status("spot")["ready"]
    record["updated_at_ms"] = 180_000
    path.write_text(json.dumps(record))
    process.code = 1
    assert owner.status("spot")["state"] == "failed"
    assert not owner.status("spot")["ready"]
    owner.close()


@pytest.mark.parametrize("invalid", [[], None, "{", {"ready": True}])
def test_malformed_health_is_not_a_readiness_confirmation(tmp_path, monkeypatch, invalid):
    monkeypatch.setattr(module.subprocess, "Popen", lambda *_args, **_kwargs: Process())
    owner = module.OwnedCollectors(tmp_path)
    owner.configure("usdm", ["binance:usdm:BTCUSDT"], 1, True)
    path = owner.children["usdm"][2].with_suffix(".health.json")
    path.write_text(invalid if isinstance(invalid, str) else json.dumps(invalid))
    assert owner.status("usdm")["state"] == "health_unavailable"
    assert not owner.status("usdm")["ready"]
    owner.close()


def test_health_snapshot_does_not_wait_for_a_collector_reconfiguration(tmp_path):
    owner = module.OwnedCollectors(tmp_path)
    entered, release = threading.Event(), threading.Event()

    def hold():
        with owner.locks["spot"]:
            entered.set()
            assert release.wait(2)

    thread = threading.Thread(target=hold)
    thread.start()
    assert entered.wait(1)
    try:
        assert owner.status("spot")["state"] == "configuring"
    finally:
        release.set()
        thread.join(2)
        owner.close()
