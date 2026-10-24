# Python Dependencies

Use CPython 3.13. The development environment and CI use the version in [`.python-version`](../.python-version).

- `runtime.in`: the optional streaming dependency. The core and REST adapters use the standard library.
- `dev.in`: development, testing, build tools, and the streaming dependency.
- `requirements.in`: the development dependency entry point.
- `dev-win-cp313.lock`: Windows x64 / CPython 3.13 pinned versions and release hashes.
- `dev-linux-cp313.lock`: Linux x64 / CPython 3.13 pinned versions and release hashes.

Create a virtual environment before installing dependencies. From the repository root:

```powershell
# Windows
py -3.13 -m venv .venv
.venv/Scripts/python.exe -I -m pip --isolated install --require-hashes --only-binary=:all: -r requirements/dev-win-cp313.lock
```

```sh
# Linux
python3.13 -m venv .venv
.venv/bin/python -I -m pip --isolated install --require-hashes --only-binary=:all: -r requirements/dev-linux-cp313.lock
```

The locks select compatible binary distributions. Re-resolve and verify them before changing Python versions or target platforms. See the root [README](../README.md) for package installation, public data collection, and test commands.
