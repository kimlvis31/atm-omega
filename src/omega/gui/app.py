"""Run with `python -m omega.gui.app --root <project>`; the core is never auto-started."""

import argparse
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ATM Omega independent desktop / artifact viewer")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="Omega project/runtime root")
    parser.add_argument(
        "--artifact", type=Path, help="open inert JSON/JSONL/CSV without loading a model"
    )
    parser.add_argument(
        "--offline", action="store_true", help="viewer only; do not attach to a core"
    )
    args = parser.parse_args(argv)
    try:
        from PySide6.QtCore import QLockFile
        from PySide6.QtWidgets import QApplication

        from omega.gui.window import OmegaWindow
    except ImportError as error:
        print(
            f"GUI dependency is unavailable ({error}). Run setup.bat --gui / setup.sh --gui.",
            file=sys.stderr,
        )
        return 2
    root = args.root.resolve()
    try:
        (root / "state").mkdir(parents=True, exist_ok=True)
        lock = QLockFile(str(root / "state" / "gui.lock"))
        lock.setStaleLockTime(0)
        if not lock.tryLock(0):
            print("An Omega GUI is already running for this project, or its lock is inaccessible.")
            return 1
        app = QApplication.instance() or QApplication(sys.argv[:1])
        app.setApplicationName("ATM Omega")
        app.setOrganizationName("ATM")
        window = OmegaWindow(root)
        window.show()
        if args.artifact:
            window.open_artifact(args.artifact.resolve())
        if not args.offline:
            window.start()
        result = app.exec()
        lock.unlock()
        return result
    except (OSError, ValueError) as error:
        print(f"Unable to start Omega GUI: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
