#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ -n "${OMEGA_PYTHON:-}" ]]; then
    interpreter="$OMEGA_PYTHON"
elif [[ -x "$project_root/.venv/bin/python" ]]; then
    interpreter="$project_root/.venv/bin/python"
elif command -v python3.13 >/dev/null 2>&1; then
    interpreter=python3.13
elif command -v python3 >/dev/null 2>&1; then
    interpreter=python3
else
    echo 'CPython 3.13 is required. Install it or set OMEGA_PYTHON to its executable path.' >&2
    exit 1
fi
exec "$interpreter" -I "$project_root/scripts/setup_environment.py" "$@"
