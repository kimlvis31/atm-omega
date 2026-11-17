"""Data-only device projections; owner telemetry and plugin state are excluded."""

import re

from omega.domain.errors import ValidationError
from omega.domain.models import decimal


def paper_device_snapshot(value):
    """Never turn a malformed provider packet into a confirmed empty portfolio."""
    fallback = {"accounts": None}
    if type(value) is not dict:
        return fallback
    accounts = value.get("accounts")
    if type(accounts) is not list or len(accounts) > 10:
        return fallback
    result, seen = [], set()
    for account in accounts:
        if type(account) is not dict:
            return fallback
        identity = account.get("account_id")
        if (
            type(identity) is not str
            or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", identity) is None
            or identity in seen
        ):
            return fallback
        seen.add(identity)
        source = account.get("positions")
        positions = None
        if type(source) is list and len(source) <= 100:
            positions = []
            for row in source:
                if not _position(row):
                    positions = None
                    break
                positions.append(
                    {key: row[key] for key in ("instrument", "quantity", "entry_price")}
                )
        result.append({"account_id": identity, "positions": positions})
    feed = value.get("feed")
    recording = value.get("recording")
    return {
        "accounts": result,
        "paused": value.get("paused") is not False,
        "feed": {
            "enabled": type(feed) is not dict or feed.get("enabled") is not False,
            "ready": type(feed) is dict and feed.get("ready") is True,
        },
        "recording": {
            "available": type(recording) is dict and recording.get("available") is True,
            "manual_resume_required": type(recording) is not dict
            or recording.get("manual_resume_required") is not False,
            "owner_retired": type(recording) is not dict
            or recording.get("owner_retired") is not False,
        },
    }


def _position(row):
    if type(row) is not dict:
        return False
    instrument = row.get("instrument")
    if (
        type(instrument) is not str
        or re.fullmatch(r"binance:(?:spot|usdm):[A-Z0-9_]{1,64}", instrument) is None
    ):
        return False
    for key in ("quantity", "entry_price"):
        value = row.get(key)
        if type(value) is not str or not 1 <= len(value) <= 128:
            return False
        try:
            amount = decimal(value)
            if abs(amount.adjusted()) > 100 or key == "entry_price" and amount < 0:
                return False
        except (ValidationError, ValueError, ArithmeticError):
            return False
    return True
