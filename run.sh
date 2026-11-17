#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ ! -x "$project_root/.venv/bin/python" ]]; then
    echo 'Project environment is missing. Run bash setup.sh first.' >&2
    if [[ $# -eq 0 && -t 0 ]]; then
        read -r -p 'Press Enter to close this launcher.' || true
    fi
    exit 1
fi
exec "$project_root/.venv/bin/python" -I -X utf8 "$project_root/scripts/run_omega.py" "$@"
