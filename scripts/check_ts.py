"""Project-root entry for interactive, offline Trade Strategy validation."""

from pathlib import Path

from omega.runtime.ts_cli import main

if __name__ == "__main__":
    import sys

    raise SystemExit(main(["--root", str(Path(__file__).resolve().parents[1]), *sys.argv[1:]]))
