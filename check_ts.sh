#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ ! -x "$project_root/.venv/bin/python" ]]; then
    echo 'Project Python environment is missing. Run bash setup.sh first.' >&2
    exit 1
fi
exec "$project_root/.venv/bin/python" -I -X utf8 "$project_root/scripts/check_ts.py" "$@"
