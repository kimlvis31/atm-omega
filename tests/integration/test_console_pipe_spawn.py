"""Native Windows Core PIPE input and spawned research coexist; no DB/exchange."""

import _socket
import ctypes
import http.client
import json
import os
import socket
import subprocess
import sys
import time
import traceback
from pathlib import Path
from uuid import uuid4

import pytest

import omega
from omega.research.datasets import MarketRow, export_dataset, write_rows
from omega.runtime.control import InstanceLock

pytestmark = pytest.mark.skipif(os.name != "nt", reason="native Windows PIPE startup regression")


def test_native_core_console_pipe_keeps_commands_and_research_and_remote_exit(tmp_path):
    """Keep the stdin pipe open throughout the original25s worker boundary and exit."""
    from decimal import Decimal

    root = tmp_path / ("core-" + uuid4().hex)
    root.mkdir()
    (root / "master.config.toml").write_text(
        "schema_version=1\n[gui]\nautostart=false\n"
        "[console]\nlog_autostart=false\n"
        "[references]\nrefresh_leverage_on_start=false\n",
        encoding="utf-8",
    )
    csv = tmp_path / "eight-synthetic-bars.csv"
    write_rows(
        csv,
        [
            MarketRow(
                "binance:usdm:TEST_PIPEUSDT",
                index * 60000,
                *(Decimal(100) for _ in range(4)),
                Decimal(10),
            )
            for index in range(8)
        ],
    )
    dataset = tmp_path / "eight-bars"
    export_dataset(csv, dataset, {})
    output, errors = root / "core.stdout.log", root / "core.stderr.log"
    record = {
        "scope": "native_WINDOWS_console_PIPE_synthetic_research_only",
        "real_accounts": 0,
        "real_orders": 0,
        "database_configured": False,
        "passed": False,
        "root": str(root),
        "settings_observations": [],
    }
    exit_id = "pipe-exit-" + uuid4().hex
    child = None
    port = token = None
    exit_request_started = False
    out = output.open("wb")
    err = errors.open("wb")

    def save():
        (root / "pipe-spawn-result.json").write_text(
            json.dumps(record, indent=2, allow_nan=False) + "\n", encoding="utf-8"
        )

    def wait(read, accept, label, seconds):
        until = time.monotonic() + seconds
        last = None
        while time.monotonic() < until:
            last = read()
            if accept(last):
                return last
            if child.poll() is not None:
                raise AssertionError(f"Core exited before {label}; native={child.returncode}")
            time.sleep(0.02)
        raise AssertionError(f"{label}: original bounded wait expired; last={last}")

    def request(method, path, body=None):
        assert method in {"GET", "POST"} and path.startswith("/api/")
        assert port is not None and token is not None
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)

        def owned_loopback(address, timeout, source_address=None, **_options):
            # Keep the suite's global DNS/network guards intact. Only this exact
            # newly owned Core endpoint may use the native connect descriptor.
            assert address == ("127.0.0.1", port) and source_address is None
            value = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            value.settimeout(timeout)
            try:
                _socket.socket.connect(value, address)
            except BaseException:
                value.close()
                raise
            return value

        connection._create_connection = owned_loopback
        try:
            connection.request(
                method,
                path,
                None if body is None else json.dumps(body).encode(),
                {"Authorization": "Bearer " + token, "Content-Type": "application/json"},
            )
            response = connection.getresponse()
            value = json.loads(response.read())
            assert response.status in {200, 201, 202}, (path, response.status, value)
            return value
        finally:
            connection.close()

    def state():
        return request("GET", "/api/state")

    def settings_command(value):
        before = state()["settings"]["revision"]
        child.stdin.write(f"set notifications.speech_volume {value}\r\n".encode("ascii"))
        child.stdin.flush()
        current = wait(
            state,
            lambda data: (
                data["settings"]["notifications"]["speech_volume"] == value
                and data["settings"]["revision"] == before + 1
            ),
            "actual PIPE console setting mutation",
            5,
        )
        record["settings_observations"].append(
            {"value": value, "revision": current["settings"]["revision"]}
        )
        save()

    def shutdown():
        nonlocal exit_request_started
        assert not exit_request_started, "an uncertain shutdown must not be reposted"
        snapshot = wait(
            lambda: request("GET", "/api/shutdown"),
            lambda value: value["ready"],
            "Core shutdown observation",
            5,
        )
        exit_request_started = True
        request(
            "POST",
            "/api/shutdown",
            {
                "id": exit_id,
                "core_epoch": snapshot["core_epoch"],
                "expected_revision": snapshot["revision"],
                "kind": "save_exit",
                "parameters": {"acknowledge_unmanaged_positions": True},
            },
        )

    try:
        # CPython's own Windows spawn backend uses this base-executable/venv
        # launcher pair. The Popen handle then owns the actual Core interpreter,
        # not an intervening venv redirector. The bootstrap selects this test's
        # current staged src; -I must not silently import an installed old tree.
        environment = os.environ.copy()
        environment["__PYVENV_LAUNCHER__"] = sys.executable
        project_src = Path(omega.__file__).resolve().parents[1]
        bootstrap = (
            "import runpy,sys;sys.path.insert(0,sys.argv.pop(1));"
            "runpy.run_module('omega.runtime.app',run_name='__main__',alter_sys=True)"
        )
        child = subprocess.Popen(
            [
                sys._base_executable,
                "-I",
                "-c",
                bootstrap,
                str(project_src),
                "--root",
                str(root),
                "--port",
                "0",
                "--console",
                "--no-gui",
            ],
            stdin=subprocess.PIPE,
            stdout=out,
            stderr=err,
            env=environment,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
        times = [ctypes.c_ulonglong() for _ in range(4)]
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.GetProcessTimes.argtypes = [ctypes.c_void_p] * 5
        kernel.GetProcessTimes.restype = ctypes.c_int
        record["core_pid"] = child.pid
        save()
        assert kernel.GetProcessTimes(int(child._handle), *(ctypes.byref(item) for item in times))
        record["core_creation_filetime"] = times[0].value
        record["module_source"] = str(project_src)
        save()
        descriptor_path = root / "state/core.json"
        wait(lambda: descriptor_path.is_file(), bool, "native Core descriptor", 15)
        descriptor = json.loads(descriptor_path.read_bytes())
        assert descriptor["url"].startswith("http://127.0.0.1:")
        port = int(descriptor["url"].rsplit(":", 1)[1])
        assert 1 <= port <= 65535
        token = (root / "secrets/core_token").read_text(encoding="utf-8").strip()
        wait(lambda: output.read_bytes(), lambda raw: b"omega> " in raw, "actual input prompt", 5)
        settings_command(0.37)
        # Original read must have returned and the next console prompt must be
        # active before spawn: this is not bypassed by postponing the reader.
        wait(
            lambda: output.read_bytes(),
            lambda raw: raw.count(b"omega> ") >= 2,
            "next console prompt before spawn",
            5,
        )
        job = request(
            "POST",
            "/api/jobs",
            {"kind": "inspect_dataset", "parameters": {"dataset": str(dataset / "dataset.json")}},
        )
        job_id = job["id"]
        record["job_id"] = job_id
        save()
        completed = wait(
            lambda: request("GET", "/api/jobs/" + job_id),
            lambda value: value["state"] in {"complete", "failed", "cancelled", "interrupted"},
            "real spawned8bar dataset inspection",
            25,  # Original reproducer boundary retained.
        )
        record["job_final"] = completed
        assert completed["state"] == "complete" and completed["result"]["rows"] == 8
        assert (root / "artifacts" / job_id).is_dir()
        assert not (root / "artifacts" / (".partial-" + job_id)).exists()
        settings_command(0.41)
        assert not child.stdin.closed
        shutdown()
        record["core_native_exit"] = child.wait(timeout=15)
        assert record["core_native_exit"] == 0 and not child.stdin.closed
        receipt = json.loads(
            (root / "state/shutdown/operations" / (exit_id + ".json")).read_bytes()
        )["data"]
        assert receipt["state"] == "completed" and receipt["result"]["core_exit_confirmed"] is True
        record["durable_exit_ack"] = True
        lease = InstanceLock(root / "state/core.lock")
        lease.close()
        assert lease.released
        record["root_lease_reacquired_released"] = True
        assert b"Traceback" not in errors.read_bytes()
        record["stdin_open_at_native_exit"] = True
        record["passed"] = True
    finally:
        original = sys.exc_info()
        if original[0] is not None:
            record["first_failure"] = "".join(traceback.format_exception(*original))
        if child is not None and child.poll() is None:
            try:
                if port is not None and token is not None:
                    if not exit_request_started:
                        shutdown()
                    child.wait(timeout=15)
            except BaseException:
                record["cleanup_failure"] = traceback.format_exc()
        if child is not None:
            record["core_pending"] = child.poll() is None
            record["core_native_final"] = child.returncode
            # Keep a pipe open if cleanup is unconfirmed; the outer main-owned
            # supervisor has this exact original PID/creation/root for retirement.
            if child.poll() is not None:
                child.stdin.close()
        out.close()
        err.close()
        save()
        assert not record.get("core_pending"), (
            "Original owned Core cleanup unconfirmed; retain PID/creation/root",
            record,
        )
