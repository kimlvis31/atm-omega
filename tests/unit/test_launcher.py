"""Launcher menus preserve owners, protect credentials and never kill services."""

import importlib.util
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from omega.runtime import launcher


@pytest.fixture
def runner():
    path = Path(__file__).resolve().parents[2] / "scripts/run_omega.py"
    spec = importlib.util.spec_from_file_location("omega_run_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("arguments,tty", [([], False), (["--help"], True), (["-h"], True)])
def test_help_and_redirected_stdin_do_not_open_a_menu(runner, monkeypatch, capsys, arguments, tty):
    monkeypatch.setattr(runner.sys, "argv", ["run", *arguments])
    monkeypatch.setattr(runner.sys, "stdin", SimpleNamespace(isatty=lambda: tty))
    monkeypatch.setattr(launcher, "DesktopLauncher", lambda *_: pytest.fail("unexpected menu"))
    assert runner.main() == 0
    assert "Usage:" in capsys.readouterr().out


def test_no_arguments_in_a_terminal_open_the_persistent_menu(runner, monkeypatch):
    calls = []
    monkeypatch.setattr(runner.sys, "argv", ["run"])
    monkeypatch.setattr(runner.sys, "stdin", SimpleNamespace(isatty=lambda: True))
    monkeypatch.setattr(
        launcher,
        "DesktopLauncher",
        lambda root: SimpleNamespace(menu=lambda: calls.append(root) or 0),
    )
    assert runner.main() == 0
    assert calls == [runner.path_PROJECT]


def test_existing_command_arguments_and_exit_status_are_preserved(runner, monkeypatch):
    from omega.runtime import ts_cli

    calls = []
    monkeypatch.setattr(runner.sys, "argv", ["run", "check-ts", "tef", "--json"])
    monkeypatch.setattr(ts_cli, "main", lambda: calls.append(list(runner.sys.argv)) or 3)
    assert runner.main() == 3
    assert calls == [["omega-check-ts", "tef", "--json"]]
    assert Path.cwd() == runner.path_PROJECT


@pytest.mark.parametrize("status", [0, 1, 3, 130])
def test_backfill_dispatch_preserves_arguments_and_gap_or_interrupt_status(
    runner, monkeypatch, status
):
    from omega.runtime import backfill_cli

    calls = []
    arguments = ["backfill", "--root", "a root", "--db-config", "db.toml", "--resume", "a" * 64]
    monkeypatch.setattr(runner.sys, "argv", ["run", *arguments])
    monkeypatch.setattr(backfill_cli, "main", lambda: calls.append(list(runner.sys.argv)) or status)
    assert runner.main() == status
    assert calls == [["omega-backfill", *arguments[1:]]]


def test_menu_survives_bad_selection_and_failed_action_then_exits(tmp_path):
    output = []
    desktop = launcher.DesktopLauncher(tmp_path, output=output.append)
    desktop.gui = lambda: (_ for _ in ()).throw(launcher.LauncherError("GUI unavailable"))
    choices = iter(["bad", "1", "0"])
    assert desktop.menu(input_fn=lambda _prompt: next(choices)) == 0
    assert output.count("\nATM Omega — 실행 메뉴") == 3
    assert "GUI unavailable" in output


def test_no_gui_dependency_does_not_start_a_preview_core(tmp_path, monkeypatch):
    desktop = launcher.DesktopLauncher(tmp_path)
    monkeypatch.setattr(launcher.importlib.util, "find_spec", lambda _name: None)
    monkeypatch.setattr(
        launcher.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("spawned")
    )
    with pytest.raises(launcher.LauncherError, match="setup.bat --gui"):
        desktop.preview()


@pytest.mark.parametrize("action", ["headless", "preview"])
def test_existing_core_is_reused_without_starting_or_changing_it(tmp_path, monkeypatch, action):
    desktop = launcher.DesktopLauncher(tmp_path)
    monkeypatch.setattr(desktop, "_gui_ready", lambda: None)
    monkeypatch.setattr(desktop, "gui", lambda: 0)
    monkeypatch.setattr(launcher, "probe_core", lambda *_args, **_kwargs: {"mode": "paper"})
    monkeypatch.setattr(
        launcher.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("duplicate core")
    )
    assert getattr(desktop, action)() == 0


def test_owned_but_unresponsive_core_is_never_replaced(tmp_path, monkeypatch):
    desktop = launcher.DesktopLauncher(tmp_path)
    monkeypatch.setattr(desktop, "_gui_ready", lambda: None)
    monkeypatch.setattr(launcher, "probe_core", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(launcher, "core_owned", lambda _root: True)
    monkeypatch.setattr(
        launcher.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("replaced owner")
    )
    with pytest.raises(launcher.LauncherError):
        desktop.preview()


def test_preview_starts_hidden_with_root_logs_and_waits_for_authenticated_readiness(
    tmp_path, monkeypatch
):
    root = tmp_path / "project with spaces"
    desktop = launcher.DesktopLauncher(root)
    monkeypatch.setattr(desktop, "_gui_ready", lambda: None)
    events = []
    monkeypatch.setattr(desktop, "gui", lambda: events.append("gui") or 0)
    probes = iter([None, {"mode": "paper"}])
    monkeypatch.setattr(launcher, "probe_core", lambda *_args, **_kwargs: next(probes))
    monkeypatch.setattr(launcher, "core_owned", lambda _root: False)

    def spawn(command, **options):
        events.append("core")
        assert "--demo" in command
        assert "--no-gui" in command and command[-2:] == ["--port", "0"]
        assert command[command.index("--root") + 1] == str(root.resolve())
        assert options["cwd"] == root.resolve()
        assert options["stdin"] == subprocess.DEVNULL
        assert options["stderr"] == subprocess.STDOUT
        assert options["stdout"].name.startswith(str(root / "logs" / "launcher"))
        if launcher.os.name == "nt":
            assert options["creationflags"] == subprocess.CREATE_NO_WINDOW
        else:
            assert options["start_new_session"] is True
        return SimpleNamespace(pid=123, poll=lambda: None)

    monkeypatch.setattr(launcher.subprocess, "Popen", spawn)
    assert desktop.preview() == 0
    assert events == ["core", "gui"]


def test_failed_or_timed_out_preview_reports_log_and_never_kills(tmp_path, monkeypatch):
    desktop = launcher.DesktopLauncher(tmp_path, ready_timeout=0.1)
    monkeypatch.setattr(desktop, "_gui_ready", lambda: None)
    monkeypatch.setattr(launcher, "probe_core", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(launcher, "core_owned", lambda _root: False)
    process = SimpleNamespace(pid=123, poll=lambda: None)
    monkeypatch.setattr(launcher.subprocess, "Popen", lambda *_args, **_kwargs: process)
    times = iter([0, 1])
    monkeypatch.setattr(launcher.time, "monotonic", lambda: next(times))
    with pytest.raises(launcher.LauncherError, match="강제 종료하지 않았습니다"):
        desktop.preview()
    assert desktop.children == [process]


def test_foreground_ctrl_c_does_not_force_kill_a_child(tmp_path, monkeypatch):
    count = [0]

    def wait():
        count[0] += 1
        if count[0] == 1:
            raise KeyboardInterrupt
        return 0

    monkeypatch.setattr(
        launcher.subprocess, "Popen", lambda *_args, **_kwargs: SimpleNamespace(wait=wait)
    )
    assert launcher.DesktopLauncher(tmp_path)._foreground(["python", "--help"]) == 0
    assert count[0] == 2


def test_crashed_preview_has_actionable_log_and_does_not_open_gui(tmp_path, monkeypatch):
    desktop = launcher.DesktopLauncher(tmp_path)
    monkeypatch.setattr(desktop, "_gui_ready", lambda: None)
    monkeypatch.setattr(desktop, "gui", lambda: pytest.fail("GUI opened before core readiness"))
    monkeypatch.setattr(launcher, "probe_core", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(launcher, "core_owned", lambda _root: False)
    monkeypatch.setattr(
        launcher.subprocess,
        "Popen",
        lambda *_args, **_kwargs: SimpleNamespace(pid=123, poll=lambda: 1),
    )
    with pytest.raises(launcher.LauncherError, match="core-preview-.*.log"):
        desktop.preview()


@pytest.mark.skipif(launcher.os.name != "nt", reason="Windows batch behavior")
def test_missing_venv_with_redirected_input_never_waits_for_a_key(tmp_path):
    source = Path(__file__).resolve().parents[2] / "run.bat"
    target = tmp_path / "run.bat"
    target.write_bytes(source.read_bytes())
    result = subprocess.run(
        ["cmd", "/d", "/c", str(target)],
        cwd=tmp_path,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=15,
    )
    assert result.returncode == 1
    assert b"setup.bat" in result.stderr


def write_descriptor(root, url="http://127.0.0.1:8765"):
    (root / "state").mkdir()
    (root / "secrets").mkdir()
    (root / "state/core.json").write_text(json.dumps({"url": url}))
    (root / "secrets/core_token").write_text("private-token-never-print")


def test_probe_authenticates_only_to_literal_loopback_and_does_not_print_secret(
    tmp_path, monkeypatch, capsys
):
    write_descriptor(tmp_path, "http://localhost:8765")
    requests = []

    class Connection:
        def __init__(self, host, port, timeout):
            assert host == "127.0.0.1" and port == 8765

        def request(self, method, path, headers):
            requests.append((method, path, headers))

        def getresponse(self):
            payload = json.dumps(
                {
                    "schema_version": 1,
                    "mode": "paper",
                    "core": {"status": "running", "live_orders_enabled": False},
                }
            ).encode()
            return SimpleNamespace(status=200, read=lambda _limit: payload)

        def close(self):
            pass

    monkeypatch.setattr(launcher.http.client, "HTTPConnection", Connection)
    assert launcher.probe_core(tmp_path) == {"url": "http://127.0.0.1:8765", "mode": "paper"}
    assert requests[0][2]["Authorization"] == "Bearer private-token-never-print"
    assert "private-token" not in str(capsys.readouterr())


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1:8765",
        "http://example.com:8765",
        "http://127.0.0.1:8765/path",
        "http://owner:secret@127.0.0.1:8765",
    ],
)
def test_descriptor_cannot_send_credentials_to_remote_or_redirected_origins(
    tmp_path, monkeypatch, url
):
    write_descriptor(tmp_path, url)
    monkeypatch.setattr(
        launcher.http.client,
        "HTTPConnection",
        lambda *_args, **_kwargs: pytest.fail("network attempted"),
    )
    with pytest.raises(launcher.LauncherError):
        launcher.probe_core(tmp_path)
