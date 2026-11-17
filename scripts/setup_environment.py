"""Install the checked-out project into its own virtual environment."""

from __future__ import annotations

# Imports
import argparse
import json
import platform
import subprocess
import sys
from pathlib import Path

# Paths
path_PROJECT = Path(__file__).resolve().parents[1]
CUDA_TORCH_URL = (
    "https://download-r2.pytorch.org/whl/cu126/"
    "torch-2.14.1%2Bcu126-cp313-cp313-win_amd64.whl"
    "#sha256=92d0ae66f10ae9dfaaf85d684d6794a3d6ca4ed938dcfaa25e94c54cff3764eb"
)


# Setup Functions
def run(command: list[str], root: Path) -> None:
    subprocess.run(command, cwd=root, check=True)


def install(
    root: Path,
    *,
    development: bool,
    wheelhouse: Path | None,
    gui: bool = False,
    neural_cpu: bool = False,
    neural_cuda: bool = False,
    analysis: bool = False,
) -> Path:
    # [1]: Runtime & Input Validation
    if neural_cpu and neural_cuda:
        raise RuntimeError("Choose only one of --neural-cpu and --neural-cuda.")
    if sys.implementation.name != "cpython" or sys.version_info[:2] != (3, 13):
        raise RuntimeError("CPython 3.13 is required; no environment was changed.")
    if sys.platform not in {"win32", "linux"}:
        raise RuntimeError("This installer supports Windows and Linux.")
    if neural_cuda and (
        sys.platform != "win32"
        or platform.machine().lower() not in {"amd64", "x86_64"}
        or sys.maxsize <= 2**32
        or getattr(sys, "abiflags", "")
    ):
        raise RuntimeError("--neural-cuda requires Windows x86-64 standard CPython 3.13 ABI.")
    if (development or neural_cpu or analysis) and platform.machine().lower() not in {
        "amd64",
        "x86_64",
    }:
        raise RuntimeError("Development locks currently support x86-64 only.")
    if wheelhouse is not None:
        wheelhouse = wheelhouse.resolve()
        if not wheelhouse.is_dir():
            raise RuntimeError(f"Wheel directory does not exist: {wheelhouse}")

    # [2]: Project Environment Creation & Verification
    environment = root / ".venv"
    python = environment / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    if environment.is_symlink() or environment.is_junction():
        raise RuntimeError("Refusing a linked .venv directory; use a project-local environment.")
    if not environment.exists():
        run([sys.executable, "-I", "-m", "venv", str(environment)], root)
    if not python.is_file():
        raise RuntimeError("Existing .venv is incomplete. Move it aside and retry setup.")

    probe = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            "import json,sys; print(json.dumps([list(sys.version_info[:2]),"
            "sys.implementation.name,sys.prefix,sys.base_prefix]))",
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    version, implementation, prefix, base_prefix = json.loads(probe.stdout)
    if (
        version != [3, 13]
        or implementation != "cpython"
        or Path(prefix).resolve() != environment.resolve()
        or Path(prefix).resolve() == Path(base_prefix).resolve()
    ):
        raise RuntimeError("Existing .venv is incompatible. Move it aside and retry setup.")

    # [3]: Hash-Checked Dependencies & Local Package Installation
    pip = [str(python), "-I", "-m", "pip", "--isolated", "--disable-pip-version-check"]
    source = (
        ["--no-index", "--find-links", str(wheelhouse)]
        if wheelhouse is not None
        else ["--index-url", "https://pypi.org/simple"]
    )
    locks = [root / "requirements/install-cp313.lock"]
    if gui:
        locks.append(root / "requirements/gui-cp313.lock")
    if development:
        os_name = "win" if sys.platform == "win32" else "linux"
        locks.append(root / f"requirements/dev-{os_name}-cp313.lock")
    if analysis:
        os_name = "win" if sys.platform == "win32" else "linux"
        locks.append(root / f"requirements/analysis-{os_name}-cp313.lock")
    for lock in locks:
        run(
            pip
            + [
                "install",
                "--no-input",
                *source,
                "--require-hashes",
                "--only-binary=:all:",
                "-r",
                str(lock),
            ],
            root,
        )
    if neural_cpu:
        os_name = "win" if sys.platform == "win32" else "linux"
        neural_source = (
            source
            if wheelhouse is not None
            else ["--index-url", "https://download.pytorch.org/whl/cpu"]
        )
        run(
            pip
            + [
                "install",
                "--no-input",
                *neural_source,
                "--require-hashes",
                "--only-binary=:all:",
                "-r",
                str(root / f"requirements/neural-cpu-{os_name}-cp313.lock"),
            ],
            root,
        )
    if neural_cuda:
        cuda_source = source
        if wheelhouse is None:
            cuda_wheels = environment / "neural-cuda-wheels"
            if cuda_wheels.is_symlink() or cuda_wheels.is_junction():
                raise RuntimeError("Refusing a linked CUDA wheel directory.")
            # Acquire only the official pinned Torch wheel; dependencies use PyPI.
            run(
                pip
                + [
                    "download",
                    "--no-input",
                    "--no-deps",
                    "--no-index",
                    "--require-hashes",
                    "--only-binary=:all:",
                    "--dest",
                    str(cuda_wheels),
                    CUDA_TORCH_URL,
                ],
                root,
            )
            cuda_source = [*source, "--find-links", str(cuda_wheels)]
        run(
            pip
            + [
                "install",
                "--no-input",
                *cuda_source,
                "--require-hashes",
                "--only-binary=:all:",
                "-r",
                str(root / "requirements/neural-cuda-win-cp313.lock"),
            ],
            root,
        )
    run(
        pip
        + [
            "install",
            "--no-input",
            "--no-deps",
            "--no-build-isolation",
            *source,
            "--editable",
            str(root),
        ],
        root,
    )
    run(pip + ["check"], root)
    run(
        [str(python), "-I", "-m", "omega.runtime.database_cli", "--root", str(root), "configure"],
        root,
    )
    return python


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create/reuse this project's .venv and install Omega. Never starts trading."
    )
    parser.add_argument("--dev", action="store_true", help="also install locked development tools")
    parser.add_argument(
        "--analysis",
        action="store_true",
        help="install optional hash-locked NumPy/SciPy for volume-profile analysis",
    )
    neural = parser.add_mutually_exclusive_group()
    neural.add_argument(
        "--neural-cpu",
        action="store_true",
        help="install optional hash-locked PyTorch CPU examples (large download)",
    )
    neural.add_argument(
        "--neural-cuda",
        action="store_true",
        help="install optional locked CUDA 12.6 PyTorch research (Windows x86-64 only)",
    )
    parser.add_argument(
        "--gui",
        "--speech",
        dest="gui",
        action="store_true",
        help="install optional Qt components for desktop or headless speech",
    )
    parser.add_argument(
        "--wheelhouse",
        type=Path,
        metavar="DIRECTORY",
        help="install offline using only wheels in this directory",
    )
    args = parser.parse_args(argv)
    try:
        python = install(
            path_PROJECT,
            development=args.dev,
            wheelhouse=args.wheelhouse,
            gui=args.gui,
            neural_cpu=args.neural_cpu,
            neural_cuda=args.neural_cuda,
            analysis=args.analysis,
        )
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as exc:
        print(f"Setup failed: {exc}", file=sys.stderr)
        print(
            "Fix the reported issue and rerun setup. Existing environments are not deleted.",
            file=sys.stderr,
        )
        return 1
    print(f"Setup complete. Environment: {python.parent.parent}")
    collector = python.parent / (
        "omega-collect.exe" if sys.platform == "win32" else "omega-collect"
    )
    print(f'Collector options: "{collector}" --help')
    database = python.parent / ("omega-db.exe" if sys.platform == "win32" else "omega-db")
    print(f'Database startup and migration (Docker required): "{database}" init')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
