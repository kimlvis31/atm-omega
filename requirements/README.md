# Python Dependencies

Use **CPython 3.13**. The exact development/CI version is in [`.python-version`](../.python-version). Start with the repository-root setup script rather than installing the broad PySide6 metapackage or an NN framework by default.

| File | Purpose |
| :--- | :--- |
| `runtime.in` | Streaming and PostgreSQL dependencies; core/REST use the standard library |
| `install-cp313.lock` | Hash-checked runtime and local build dependencies for setup; shared Windows/Linux wheel hashes |
| `gui-cp313.lock` | Optional Qt GUI: PySide6 Essentials/Addons and shiboken6 6.12.0, Windows/Linux x86-64 wheels |
| `dev.in` / `requirements.in` | Development dependency inputs/entry point |
| `dev-win-cp313.lock` | Windows x86-64 / CPython 3.13 development, testing and build dependencies |
| `analysis-win-cp313.lock` / `analysis-linux-cp313.lock` | Optional NumPy/SciPy arithmetic for Eta volume profiles; CPython 3.13 x86-64 |
| `dev-linux-cp313.lock` | Linux x86-64 / CPython 3.13 development, testing and build dependencies |

## Recommended setup

```powershell
# Windows: choose the options you need
.\setup.bat
.\setup.bat --gui
.\setup.bat --dev --gui
```

```sh
# Linux: choose the options you need
bash ./setup.sh
bash ./setup.sh --gui
bash ./setup.sh --dev --gui
```

Setup creates/reuses `.venv`, installs the local package and selected hash-checked locks, then checks dependency consistency. Base setup omits Qt and test/lint tools; `--gui` and `--dev` are independent options. Existing configuration/secrets are preserved. Setup does not start Docker or the application.

The GUI uses QtWidgets/QtNetwork and optional QtTextToSpeech; it does not require QtWebEngine. The NN host and standard-library linear-neuron training demo require no PyTorch/Triton/GPU package. Optional PyTorch MLP/GRU examples use `setup --neural-cpu` and the platform-specific `neural-cpu-*-cp313.lock` files. Their CPU wheels come from the official PyTorch CPU index, separately from the base PyPI install. The examples support recorded training, compatible epoch resume and offline prediction; live inference and arbitrary-module compatibility are not claimed. Selecting an external `.py` executes trusted user code; it does not automatically install its requirements or create a security sandbox. Triton/GPU acceleration is not implemented; GPU simulation requests report CPU fallback.

Qt wheels still need distribution graphics/font/desktop libraries on Linux. The tested Debian offscreen dependencies are listed in the root README; consult [Qt Linux requirements](https://doc.qt.io/qt-6/linux-requirements.html) for other distributions. Setup does not install OS packages.

Set `OMEGA_PYTHON` to choose an installed Python executable. For offline setup, add `--wheelhouse /path/to/wheels` and provide every required runtime/build/GUI/development wheel for the selected options and platform. Linux may require a distribution Python 3.13 venv package. An incompatible or incomplete existing environment must be moved aside explicitly before recreating it.

## Manual dependency installation

These commands install dependencies into a manually created environment; they do not install the local project or generate database configuration. Use setup for the complete installation flow.

```powershell
# Windows
py -3.13 -m venv .venv
.venv/Scripts/python.exe -I -m pip --isolated install --require-hashes --only-binary=:all: -r requirements/dev-win-cp313.lock
# Optional desktop dependencies:
.venv/Scripts/python.exe -I -m pip --isolated install --require-hashes --only-binary=:all: -r requirements/gui-cp313.lock
.venv/Scripts/python.exe -I -m pip --isolated check
```

```sh
# Linux
python3.13 -m venv .venv
.venv/bin/python -I -m pip --isolated install --require-hashes --only-binary=:all: -r requirements/dev-linux-cp313.lock
# Optional desktop dependencies:
.venv/bin/python -I -m pip --isolated install --require-hashes --only-binary=:all: -r requirements/gui-cp313.lock
.venv/bin/python -I -m pip --isolated check
```

Locks select compatible binary distributions. Re-resolve and verify versions, platforms and hashes before changing Python or dependencies. Keep runtime/build pins and shared hashes in `install-cp313.lock` synchronized with the development locks. Keep optional GUI pins synchronized with `pyproject.toml`.

The root [English README](../README.md) / [한국어 README](../README.ko.md) covers core/GUI launchers, public collection, database export, TS validation and tests. Optional Qt/Node cases may skip when dependencies are absent; default database-test skips do not establish real PostgreSQL coverage.
