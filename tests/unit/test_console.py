"""The terminal uses the same owner API and never retries an uncertain mutation."""

import json
import threading
from io import BytesIO, TextIOWrapper
from pathlib import Path

import pytest

from omega.runtime.console import ControlConsole, follow_logs, main
from omega.runtime.service import CoreService


def test_console_help_works_with_windows_korean_legacy_output(monkeypatch):
    encoded = BytesIO()
    output = TextIOWrapper(encoded, encoding="cp949")
    with monkeypatch.context() as local:
        local.setattr("sys.stdout", output)
        with pytest.raises(SystemExit) as exit_info:
            main(["--help"])
        output.flush()
    assert exit_info.value.code == 0
    assert "paper only" in encoded.getvalue().decode("cp949")


@pytest.fixture
def running(tmp_path, monkeypatch):
    core = CoreService(tmp_path)

    # Exercise the complete console JSON boundary and authenticated dispatch while
    # keeping the suite's blanket DNS/socket prohibition intact.
    class LocalHTTP:
        def __init__(self, host, port, **_kwargs):
            assert host == "127.0.0.1" and port == 8765

        def request(self, method, path, body, headers):
            self.value = core.request(
                method,
                path,
                headers["Authorization"].removeprefix("Bearer "),
                json.loads(body) if body is not None else None,
            )

        def getresponse(self):
            response = BytesIO(json.dumps(self.value).encode())
            response.status = 200
            return response

        def close(self):
            pass

    monkeypatch.setattr(
        "omega.runtime.console.probe_core", lambda *_a, **_k: {"url": "http://127.0.0.1:8765"}
    )
    monkeypatch.setattr("omega.runtime.console.http.client.HTTPConnection", LocalHTTP)
    yield core
    core.close()


def test_console_settings_revision_pause_and_owner_reads(running):
    console = ControlConsole(running.root, output=lambda *_: None)
    assert console.execute("settings")["notifications"]["speech"] is False
    result = console.execute("set notifications.speech_volume 0.4")
    assert result["status"] == "completed"
    assert console.execute("settings")["notifications"]["speech_volume"] == 0.4
    assert console.execute("pause")["status"] == "completed"
    assert "core" in console.execute("status")
    assert "settings" in console.execute("help")


def test_invalid_setting_is_known_rejection_without_new_incident(running):
    console = ControlConsole(running.root, output=lambda *_: None)
    assert console.execute("set notifications.speech_volume 2")["status"] == "rejected"
    assert not running.control.snapshot()["incidents"]
    with pytest.raises(ValueError):
        console.execute("set private.key 2")
    with pytest.raises(ValueError):
        console.execute("request GET https://outside.example/api/state")


def test_mutation_transport_failure_prints_original_id_and_does_not_retry(tmp_path, monkeypatch):
    output, calls = [], []
    console = ControlConsole(tmp_path, output=output.append)

    def failed(*args):
        calls.append(args)
        raise OSError("failed")

    monkeypatch.setattr(console, "request", failed)
    with pytest.raises(OSError):
        console.command("pause", {})
    assert len(calls) == 1
    operation = calls[0][2]["id"]
    assert operation in output[0] and operation in output[1]


def test_first_log_tail_does_not_consume_partial_record(tmp_path, monkeypatch):
    path = tmp_path / "logs/system/day.jsonl"
    path.parent.mkdir(parents=True)
    path.write_bytes(b'{"n":1}\n{"n":')
    output, stopped = [], threading.Event()

    def later(_seconds):
        with path.open("ab") as stream:
            stream.write(b"2}\n")
        stopped.set()

    monkeypatch.setattr("omega.runtime.console.time.sleep", later)
    follow_logs(tmp_path, follow=True, stopped=stopped, output=output.append)
    assert [json.loads(row) for row in output] == [{"n": 1}, {"n": 2}]


def test_retention_removing_file_during_enumeration_does_not_stop_logs(tmp_path, monkeypatch):
    path = tmp_path / "logs/system/removed.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("{}\n", encoding="utf-8")
    original = Path.stat

    def vanished(self, *args, **kwargs):
        if self == path:
            raise FileNotFoundError("retention")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", vanished)
    follow_logs(tmp_path, output=lambda _: pytest.fail("must not print removed file"))


def test_console_help_and_invalid_command_are_available_without_core(tmp_path, capsys):
    assert main(["--root", str(tmp_path), "exec", "--command", "help"]) == 0
    assert "notifications.speech" in capsys.readouterr().out
    assert main(["--root", str(tmp_path), "exec", "--command", "unknown"]) == 2
