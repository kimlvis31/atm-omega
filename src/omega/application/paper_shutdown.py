"""Data-only v1 shutdown identities and financial inspection; no strategy calls."""

# Imports
import json
import re
from hashlib import sha256

from omega.application.paper_state import validate_state
from omega.domain.errors import ValidationError
from omega.domain.models import decimal

# Constants
MARKER = re.compile(r"shutdown:([a-f0-9]{64}):([0-9]{1,15})")


def shutdown_marker(operation_id, issued_at_ms):
    if (
        type(operation_id) is not str
        or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", operation_id) is None
        or type(issued_at_ms) is not int
        or not 0 <= issued_at_ms <= 253_402_300_799_000
    ):
        raise ValidationError("invalid paper shutdown identity or issue time")
    return f"shutdown:{sha256(operation_id.encode()).hexdigest()}:{issued_at_ms}"


def is_shutdown_row(row):
    return (
        row.get("status") == "recovery_required"
        and MARKER.fullmatch(row.get("last_reason", "")) is not None
    )


def pending_identity(name, row):
    pending = row["pending"]
    # Shutdown reason freezes the issue identity even after unrelated display changes.
    body = (
        pending["reason"]
        if MARKER.fullmatch(pending["reason"])
        else json.dumps(pending, sort_keys=True, separators=(",", ":"), allow_nan=False)
    )
    return sha256((row["account_id"] + ":" + name + ":" + body).encode()).hexdigest()


def inspect_finance(data):
    """Validate finance independently of broken or unavailable model checkpoints.

    Portfolio data is already serialized owner state. Projection intentionally
    excludes configuration, analysis/model snapshots and diagnostic metadata.
    """
    rows = {}
    keys = (
        "account_id",
        "instrument",
        "budget",
        "quantity",
        "entry_price",
        "revision",
        "last_changed_ms",
        "pending",
        "last_candle",
        "status",
        "fills",
    )
    for name, row in data["strategies"].items():
        rows[name] = {key: row[key] for key in keys} | {
            "module_id": "shutdown_finance",
            "configuration": {},
            "analysis": {},
        }
    validate_state(
        {
            "schema_version": data["schema_version"],
            "paused": data["paused"],
            "accounts": data["accounts"],
            "strategies": rows,
            "candles": data["candles"],
        }
    )
    positions, pending = [], []
    for name, row in sorted(data["strategies"].items()):
        common = {
            "account_id": row["account_id"],
            "instance": name,
            "instrument": row["instrument"],
        }
        if decimal(row["quantity"]):
            positions.append(common | {"quantity": row["quantity"]})
        if row["pending"] is not None:
            pending.append(
                common
                | {
                    "order_id": pending_identity(name, row),
                    "quantity": row["pending"]["quantity"],
                    "reducing": row["pending"]["reducing"],
                }
            )
    return positions, pending
