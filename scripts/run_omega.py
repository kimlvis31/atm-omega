"""Launch the available Omega commands relative to this checkout."""

# Imports
import os
import sys
from pathlib import Path

# Paths
path_PROJECT = Path(__file__).resolve().parents[1]


def main() -> int:
    args = sys.argv[1:]
    os.chdir(path_PROJECT)
    if not args and sys.stdin is not None and sys.stdin.isatty():
        from omega.runtime.launcher import DesktopLauncher

        return DesktopLauncher(path_PROJECT).menu()
    if not args or args[0] in {"--help", "-h"}:
        print(
            "Usage: run.bat {core,gui,console,logs,collect,collect-aux,"
            "backfill,db,check-ts,export,margin} [options]"
        )
        print("Linux: bash run.sh with the same arguments. Use collect --help for options.")
        print("Start core first; GUI reconnects independently. Core --demo uses synthetic data.")
        print("Open run.bat / bash run.sh without arguments in a terminal for the persistent menu.")
        return 0
    if args[0] == "db":
        from omega.runtime.database_cli import main as entrypoint
    elif args[0] == "collect":
        from omega.runtime.collect_cli import main as entrypoint
    elif args[0] == "collect-aux":
        from omega.runtime.auxiliary_collect_cli import main as entrypoint
    elif args[0] == "backfill":
        from omega.runtime.backfill_cli import main as entrypoint
    elif args[0] == "core":
        from omega.runtime.app import main as entrypoint
    elif args[0] == "gui":
        from omega.gui.app import main as entrypoint
    elif args[0] == "check-ts":
        from omega.runtime.ts_cli import main as entrypoint
    elif args[0] == "export":
        from omega.runtime.export_cli import main as entrypoint
    elif args[0] == "margin":
        from omega.runtime.margin_console import main as entrypoint
    elif args[0] in {"console", "logs"}:
        from omega.runtime.console import main as entrypoint
    else:
        print("Unknown command. Run with --help for usage.", file=sys.stderr)
        return 2
    sys.argv = [f"omega-{args[0]}", *args[1:]]
    result = entrypoint(["logs", *args[1:]]) if args[0] == "logs" else entrypoint()
    return result if isinstance(result, int) else 0


if __name__ == "__main__":
    raise SystemExit(main())
