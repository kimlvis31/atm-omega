"""Installation failures must not mutate a global or unrelated Python environment."""

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest


@pytest.fixture
def installer():
    path = Path(__file__).resolve().parents[2] / "scripts/setup_environment.py"
    spec = importlib.util.spec_from_file_location("omega_setup_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def environment(tmp_path, installer):
    root = tmp_path / "project with spaces"
    relative = "Scripts/python.exe" if installer.sys.platform == "win32" else "bin/python"
    python = root / ".venv" / relative
    python.parent.mkdir(parents=True)
    python.touch()
    return root, python


def probe_result(root, version=None, prefix=None):
    return subprocess.CompletedProcess(
        [],
        0,
        json.dumps(
            [
                version or [3, 13],
                "cpython",
                str(prefix or root / ".venv"),
                str(root / "base-python"),
            ]
        ),
    )


def test_wrong_python_cannot_create_environment(tmp_path, installer, monkeypatch):
    monkeypatch.setattr(installer.sys, "version_info", (3, 12))
    with pytest.raises(RuntimeError, match="CPython 3.13"):
        installer.install(tmp_path, development=False, wheelhouse=None)
    assert list(tmp_path.iterdir()) == []


def test_missing_offline_wheels_cannot_create_environment(tmp_path, installer):
    with pytest.raises(RuntimeError, match="Wheel directory does not exist"):
        installer.install(tmp_path, development=False, wheelhouse=tmp_path / "missing")
    assert not (tmp_path / ".venv").exists()


def test_incomplete_environment_is_preserved(tmp_path, installer):
    environment = tmp_path / ".venv"
    environment.mkdir()
    marker = environment / "keep.txt"
    marker.write_text("keep")
    with pytest.raises(RuntimeError, match="incomplete"):
        installer.install(tmp_path, development=False, wheelhouse=None)
    assert marker.read_text() == "keep"


@pytest.mark.parametrize("mismatch", ["version", "prefix"])
def test_unrelated_or_wrong_version_environment_cannot_install(
    environment, installer, monkeypatch, mismatch
):
    root, _ = environment
    commands = []
    result = probe_result(
        root,
        version=[3, 12] if mismatch == "version" else None,
        prefix=root / "other" if mismatch == "prefix" else None,
    )
    monkeypatch.setattr(installer.subprocess, "run", lambda *args, **kwargs: result)
    monkeypatch.setattr(installer, "run", lambda command, root: commands.append(command))
    with pytest.raises(RuntimeError, match="incompatible"):
        installer.install(root, development=False, wheelhouse=None)
    assert commands == []


def test_failed_dependency_install_stops_before_installing_project(
    environment, installer, monkeypatch
):
    root, python = environment
    commands = []
    monkeypatch.setattr(installer.subprocess, "run", lambda *a, **k: probe_result(root))

    def fail(command, cwd):
        commands.append(command)
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(installer, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        installer.install(root, development=True, wheelhouse=None)
    assert len(commands) == 1
    assert "--require-hashes" in commands[0]
    assert "--editable" not in commands[0]
    assert python.exists()


def test_offline_install_keeps_all_commands_in_project_environment(
    environment, installer, monkeypatch, tmp_path
):
    root, python = environment
    wheels = tmp_path / "wheels with spaces"
    wheels.mkdir()
    commands = []
    monkeypatch.setattr(installer.subprocess, "run", lambda *a, **k: probe_result(root))
    monkeypatch.setattr(installer, "run", lambda command, cwd: commands.append((command, cwd)))
    monkeypatch.chdir(tmp_path)
    assert installer.install(root, development=True, wheelhouse=wheels) == python
    for command, cwd in commands:
        assert command[0] == str(python)
        assert cwd == root
        if "install" in command:
            assert "--no-index" in command
            assert command[command.index("--find-links") + 1] == str(wheels)
    dependency_installs = [cmd for cmd, _ in commands if "--require-hashes" in cmd]
    assert len(dependency_installs) == 2
    project_install = [cmd for cmd, _ in commands if "--editable" in cmd][0]
    assert "--no-deps" in project_install
    assert "--no-build-isolation" in project_install
    assert project_install[-1] == str(root)


def test_setup_failure_returns_failure_exit_code(installer, monkeypatch, capsys):
    def fail(*args, **kwargs):
        raise RuntimeError("disk is full")

    monkeypatch.setattr(installer, "install", fail)
    assert installer.main([]) == 1
    assert "disk is full" in capsys.readouterr().err


@pytest.mark.parametrize("offline", [False, True])
def test_optional_analysis_is_hash_locked_and_does_not_select_neural_or_gui(
    environment, installer, monkeypatch, tmp_path, offline
):
    root, python = environment
    wheels = tmp_path / "analysis wheels"
    wheels.mkdir()
    commands = []
    monkeypatch.setattr(installer.platform, "machine", lambda: "AMD64")
    monkeypatch.setattr(installer.subprocess, "run", lambda *a, **k: probe_result(root))
    monkeypatch.setattr(installer, "run", lambda command, cwd: commands.append(command))
    assert (
        installer.install(
            root, development=False, wheelhouse=wheels if offline else None, analysis=True
        )
        == python
    )
    locked = [command for command in commands if "--require-hashes" in command]
    assert len(locked) == 2
    os_name = "win" if installer.sys.platform == "win32" else "linux"
    assert locked[-1][-1] == str(root / f"requirements/analysis-{os_name}-cp313.lock")
    assert all("--only-binary=:all:" in command for command in locked)
    assert not any("neural" in str(command) or "gui-cp313" in str(command) for command in commands)
    if offline:
        assert all("--no-index" in command for command in locked)


@pytest.mark.parametrize("offline", [False, True])
def test_optional_neural_cpu_uses_its_hash_lock_and_never_mixes_indexes(
    environment, installer, monkeypatch, tmp_path, offline
):
    root, python = environment
    commands = []
    wheels = tmp_path / "cpu wheels"
    wheels.mkdir()
    monkeypatch.setattr(installer.platform, "machine", lambda: "AMD64")
    monkeypatch.setattr(installer.subprocess, "run", lambda *a, **k: probe_result(root))
    monkeypatch.setattr(installer, "run", lambda command, cwd: commands.append(command))
    installer.install(
        root, development=False, gui=False, wheelhouse=wheels if offline else None, neural_cpu=True
    )
    locked = [cmd for cmd in commands if "--require-hashes" in cmd]
    assert len(locked) == 2
    neural = locked[-1]
    assert neural[0] == str(python)
    assert "--only-binary=:all:" in neural and "--extra-index-url" not in neural
    os_name = "win" if installer.sys.platform == "win32" else "linux"
    assert neural[-1] == str(root / f"requirements/neural-cpu-{os_name}-cp313.lock")
    if offline:
        assert "--no-index" in neural and neural[neural.index("--find-links") + 1] == str(wheels)
    else:
        assert neural[neural.index("--index-url") + 1] == "https://download.pytorch.org/whl/cpu"
    assert not any("gui-cp313.lock" in str(arg) for cmd in commands for arg in cmd)
