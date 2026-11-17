"""CUDA setup stays explicit, platform bounded and offline-testable without Torch."""

import importlib.util
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def cuda_installer():
    path = Path(__file__).resolve().parents[2] / "scripts/setup_environment.py"
    spec = importlib.util.spec_from_file_location("omega_cuda_setup_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("platform_name", "machine", "version", "implementation"),
    [
        ("linux", "x86_64", (3, 13), "cpython"),
        ("darwin", "x86_64", (3, 13), "cpython"),
        ("win32", "ARM64", (3, 13), "cpython"),
        ("win32", "AMD64", (3, 12), "cpython"),
        ("win32", "AMD64", (3, 13), "pypy"),
    ],
)
def test_unsupported_cuda_rejects_before_any_environment_or_process(
    tmp_path, cuda_installer, monkeypatch, platform_name, machine, version, implementation
):
    installer = cuda_installer
    root = tmp_path / "not created"
    monkeypatch.setattr(installer.sys, "platform", platform_name)
    monkeypatch.setattr(installer.sys, "version_info", version)
    monkeypatch.setattr(installer.sys, "implementation", SimpleNamespace(name=implementation))
    monkeypatch.setattr(installer.platform, "machine", lambda: machine)

    def forbidden(*args, **kwargs):
        pytest.fail("unsupported CUDA setup reached a subprocess")

    monkeypatch.setattr(installer, "run", forbidden)
    monkeypatch.setattr(installer.subprocess, "run", forbidden)
    with pytest.raises(RuntimeError):
        installer.install(root, development=False, wheelhouse=None, neural_cuda=True)
    assert not root.exists()


def test_both_neural_variants_reject_before_any_side_effect(tmp_path, cuda_installer, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("conflicting neural variants reached a subprocess")

    monkeypatch.setattr(cuda_installer, "run", forbidden)
    monkeypatch.setattr(cuda_installer.subprocess, "run", forbidden)
    with pytest.raises(RuntimeError, match="Choose only one"):
        cuda_installer.install(
            tmp_path, development=False, wheelhouse=None, neural_cpu=True, neural_cuda=True
        )
    assert list(tmp_path.iterdir()) == []
    monkeypatch.setattr(cuda_installer, "install", forbidden)
    with pytest.raises(SystemExit) as failure:
        cuda_installer.main(["--neural-cpu", "--neural-cuda"])
    assert failure.value.code == 2


@pytest.mark.parametrize(("maxsize", "abi"), [(2**31 - 1, ""), (2**63 - 1, "t")])
def test_incompatible_wheel_abi_cannot_mutate_environment(
    tmp_path, cuda_installer, monkeypatch, maxsize, abi
):
    monkeypatch.setattr(cuda_installer.sys, "platform", "win32")
    monkeypatch.setattr(cuda_installer.sys, "version_info", (3, 13))
    monkeypatch.setattr(cuda_installer.sys, "maxsize", maxsize)
    monkeypatch.setattr(cuda_installer.sys, "abiflags", abi, raising=False)
    monkeypatch.setattr(cuda_installer.platform, "machine", lambda: "AMD64")

    def forbidden(*args, **kwargs):
        pytest.fail("unsupported CUDA wheel ABI reached a subprocess")

    monkeypatch.setattr(cuda_installer, "run", forbidden)
    monkeypatch.setattr(cuda_installer.subprocess, "run", forbidden)
    with pytest.raises(RuntimeError, match="standard CPython 3.13 ABI"):
        cuda_installer.install(tmp_path, development=False, wheelhouse=None, neural_cuda=True)
    assert list(tmp_path.iterdir()) == []


@pytest.fixture
def cuda_environment(tmp_path, cuda_installer, monkeypatch):
    installer = cuda_installer
    monkeypatch.setattr(installer.sys, "platform", "win32")
    monkeypatch.setattr(installer.sys, "version_info", (3, 13))
    monkeypatch.setattr(installer.sys, "maxsize", 2**63 - 1)
    monkeypatch.setattr(installer.sys, "abiflags", "", raising=False)
    monkeypatch.setattr(installer.platform, "machine", lambda: "AMD64")
    root = tmp_path / "project with spaces"
    python = root / ".venv/Scripts/python.exe"
    python.parent.mkdir(parents=True)
    python.touch()
    commands = []
    probe = subprocess.CompletedProcess(
        [],
        0,
        json.dumps([[3, 13], "cpython", str(root / ".venv"), str(root / "base")]),
    )
    monkeypatch.setattr(installer.subprocess, "run", lambda *a, **k: probe)
    monkeypatch.setattr(installer, "run", lambda command, cwd: commands.append((command, cwd)))
    return root, python, commands


def test_cuda_online_acquires_only_official_hashed_torch_then_locked_dependencies(
    cuda_installer, cuda_environment
):
    root, python, commands = cuda_environment
    assert (
        cuda_installer.install(root, development=False, wheelhouse=None, neural_cuda=True) == python
    )
    downloads = [command for command, _ in commands if "download" in command]
    assert len(downloads) == 1
    download = downloads[0]
    assert download[-1] == (
        "https://download-r2.pytorch.org/whl/cu126/"
        "torch-2.14.1%2Bcu126-cp313-cp313-win_amd64.whl"
        "#sha256=92d0ae66f10ae9dfaaf85d684d6794a3d6ca4ed938dcfaa25e94c54cff3764eb"
    )
    assert {"--no-deps", "--no-index", "--require-hashes", "--only-binary=:all:"} <= set(download)
    cache = root / ".venv/neural-cuda-wheels"
    assert download[download.index("--dest") + 1] == str(cache)
    neural = next(command for command, _ in commands if command[-1].endswith("cuda-win-cp313.lock"))
    assert neural[neural.index("--index-url") + 1] == "https://pypi.org/simple"
    assert neural[neural.index("--find-links") + 1] == str(cache)
    assert "--require-hashes" in neural and "--only-binary=:all:" in neural
    assert commands.index((download, root)) < commands.index((neural, root))
    for command, cwd in commands:
        assert command[0] == str(python) and cwd == root
        assert "--extra-index-url" not in command
        assert "neural-cpu" not in " ".join(command)


def test_cuda_offline_never_downloads_or_selects_an_index(
    tmp_path, cuda_installer, cuda_environment
):
    root, _, commands = cuda_environment
    wheels = tmp_path / "local CUDA wheels"
    wheels.mkdir()
    cuda_installer.install(root, development=False, wheelhouse=wheels, neural_cuda=True)
    assert not any("download" in command for command, _ in commands)
    neural = next(command for command, _ in commands if command[-1].endswith("cuda-win-cp313.lock"))
    assert "--require-hashes" in neural and "--only-binary=:all:" in neural
    for command, _ in commands:
        if "install" in command:
            assert "--no-index" in command
            assert command[command.index("--find-links") + 1] == str(wheels)
        assert "--index-url" not in command
        assert not any("https://" in argument for argument in command)
    assert not (root / ".venv/neural-cuda-wheels").exists()


def test_default_setup_cannot_acquire_or_select_cuda(cuda_installer, cuda_environment):
    root, _, commands = cuda_environment
    cuda_installer.install(root, development=False, wheelhouse=None)
    assert not any("download" in command for command, _ in commands)
    assert not any("neural-cuda" in argument for command, _ in commands for argument in command)


def test_linked_cuda_cache_is_rejected_without_download(
    cuda_installer, cuda_environment, monkeypatch
):
    root, _, commands = cuda_environment
    original = Path.is_symlink
    cache = root / ".venv/neural-cuda-wheels"
    monkeypatch.setattr(Path, "is_symlink", lambda path: path == cache or original(path))
    with pytest.raises(RuntimeError, match="linked CUDA wheel directory"):
        cuda_installer.install(root, development=False, wheelhouse=None, neural_cuda=True)
    assert not any("download" in command or "--editable" in command for command, _ in commands)


def test_cuda_cli_is_explicitly_forwarded_without_cpu_selection(
    tmp_path, cuda_installer, monkeypatch
):
    calls = []

    def capture(root, **options):
        calls.append(options)
        return tmp_path / ".venv/Scripts/python.exe"

    monkeypatch.setattr(cuda_installer, "install", capture)
    assert cuda_installer.main(["--neural-cuda"]) == 0
    assert len(calls) == 1
    assert calls[0]["neural_cuda"] is True and calls[0]["neural_cpu"] is False


def test_failed_torch_download_preserves_environment_and_prevents_project_install(
    cuda_installer, cuda_environment, monkeypatch
):
    root, python, commands = cuda_environment
    marker = root / ".venv/keep.txt"
    marker.write_text("existing CPU environment", encoding="utf-8")

    def fail_download(command, cwd):
        commands.append((command, cwd))
        if "download" in command:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(cuda_installer, "run", fail_download)
    with pytest.raises(subprocess.CalledProcessError):
        cuda_installer.install(root, development=False, wheelhouse=None, neural_cuda=True)
    assert marker.read_text(encoding="utf-8") == "existing CPU environment"
    assert python.is_file()
    assert not any("--editable" in command or "configure" in command for command, _ in commands)
    assert not any(command[-1].endswith("cuda-win-cp313.lock") for command, _ in commands)


def test_cuda_lock_keeps_cpu_dependencies_and_exact_variant_hash():
    requirements = Path(__file__).resolve().parents[2] / "requirements"
    cpu = (requirements / "neural-cpu-win-cp313.lock").read_text(encoding="utf-8")
    cuda = (requirements / "neural-cuda-win-cp313.lock").read_text(encoding="utf-8")
    cuda = cuda.replace("torch==2.14.1+cu126", "torch==2.14.1+cpu").replace(
        "92d0ae66f10ae9dfaaf85d684d6794a3d6ca4ed938dcfaa25e94c54cff3764eb",
        "4eec5a4fadd7a18f9c75b97fdb374fbb579ce4b1604bf66eeb14dc2201d54def",
    )
    assert cpu.splitlines()[2:] == cuda.splitlines()[2:]
