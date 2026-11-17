"""Pure bounded projection of confirmed cached virtual ledgers, never authority.

Selection copies only public candidate scalars/position fields. It never walks
ledger/configuration/operation history or calls an owner, clock, model or store.
Unknown observations cannot establish an empty portfolio.
"""

import re
from decimal import Decimal, InvalidOperation

from omega.domain.margin import MarginMode
from omega.domain.models import is_valid_symbol

MAX_ACCOUNTS = 10
MAX_POSITIONS = 100
MAX_TIME = 253402300799000
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_DECIMAL = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:E[+-]?[0-9]+)?\Z")
_POSITION_FIELDS = ("instrument", "quantity", "entry_price", "margin_mode", "leverage")
_ACCOUNT_FIELDS = (
    "account_id",
    "state",
    "entry_paused",
    "new_risk_ready",
    "cursor_ms",
    "pending_order_count",
    "positions",
    "position_status",
    "reason_code",
)
_UNAVAILABLE_REASONS = {
    "service_unavailable",
    "cache_missing",
    "account_unknown",
    "account_conflict",
    "account_recovery_required",
    "account_invalid",
}
_PENDING_REASONS = {"pending_frame", "pending_frame_unknown", "pending_count_unknown"}


def _identity(value):
    return type(value) is str and _ID.fullmatch(value) is not None


def _scalar(value):
    # account_document keeps this explicit StrEnum before HTTP JSON encoding.
    # Convert only the known financial enum, never an arbitrary object repr.
    if type(value) is MarginMode:
        return value.value
    if value is None or type(value) in {bool, int}:
        return value
    return value if type(value) is str and len(value) <= 1024 else None


def _pending_count(value):
    if (
        type(value) is not list
        or len(value) > MAX_POSITIONS
        or any(not _identity(item) for item in value)
        or len(set(value)) != len(value)
    ):
        return None
    return len(value)


def select_cached_margin_view(declared_accounts, cached_views, *, blocked_accounts=()):
    """Take a bounded scalar-only copy under the caller's short cache lock.

    Registry absence/corruption returns None, distinct from declared empty {}.
    Account values and any undeclared cache entries are deliberately untouched.
    """
    if (
        type(declared_accounts) is not dict
        or len(declared_accounts) > MAX_ACCOUNTS
        or any(not _identity(key) for key in declared_accounts)
        or type(cached_views) is not dict
        or type(blocked_accounts) not in {tuple, set, frozenset}
    ):
        return None
    selected = []
    for identity in sorted(declared_accounts):
        row = cached_views.get(identity)
        if type(row) is not dict:
            selected.append({"declared_id": identity, "cache_present": False})
            continue
        packet = row.get("account")
        packet = packet if type(packet) is dict else {}
        account = packet.get("account")
        account = account if type(account) is dict else {}
        raw_positions = account.get("positions")
        positions = None
        if type(raw_positions) is list and len(raw_positions) <= MAX_POSITIONS:
            positions = [
                {key: _scalar(position.get(key)) for key in _POSITION_FIELDS}
                if type(position) is dict
                else None
                for position in raw_positions
            ]
        selected.append(
            {
                "declared_id": identity,
                "cache_present": True,
                "runtime_blocked": identity in blocked_accounts,
                "cached_id": _scalar(row.get("account_id")),
                "cached_state": _scalar(row.get("state")),
                "entry_paused": _scalar(row.get("paused")),
                "new_risk_ready": _scalar(row.get("new_risk_ready")),
                "cursor_ms": _scalar(row.get("cursor_ms")),
                "cursor_present": "cursor_ms" in row,
                "pending_frame": _scalar(row.get("pending_frame")),
                "pending_order_count": _pending_count(row.get("pending_orders")),
                "account_schema": _scalar(packet.get("schema_version")),
                "account_id": _scalar(account.get("account_id")),
                "settlement_asset": _scalar(account.get("settlement_asset")),
                "positions": positions,
            }
        )
    return tuple(selected)


def _number(value):
    if type(value) is not str or len(value) > 1024 or _DECIMAL.fullmatch(value) is None:
        raise ValueError("invalid public decimal")
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise ValueError("invalid public decimal") from None
    if (
        not number.is_finite()
        or len(number.as_tuple().digits) > 256
        or not -256 <= number.as_tuple().exponent <= 256
    ):
        raise ValueError("public decimal exceeds bounded exact capacity")
    return number


def _positions(value):
    if type(value) is not list or len(value) > MAX_POSITIONS:
        raise ValueError("invalid public positions")
    result, seen = [], set()
    for row in value:
        if type(row) is not dict or set(row) != set(_POSITION_FIELDS):
            raise ValueError("invalid public position fields")
        instrument = row["instrument"]
        if (
            type(instrument) is not str
            or not instrument.startswith("binance:usdm:")
            or not is_valid_symbol(instrument[len("binance:usdm:") :])
            or not instrument.endswith("USDT")
            or instrument in seen
            or type(row["margin_mode"]) is not str
            or row["margin_mode"] not in {"cross", "isolated"}
            or type(row["leverage"]) is not int
            or not 1 <= row["leverage"] <= 1000
        ):
            raise ValueError("invalid public position identity/configuration")
        quantity, price = _number(row["quantity"]), _number(row["entry_price"])
        if price < 0 or (quantity == 0) != (price == 0):
            raise ValueError("position quantity and entry price disagree")
        seen.add(instrument)
        if quantity != 0:
            result.append(dict(row))
    return result


def _project(row, service_ready):
    result = {
        "account_id": row["declared_id"],
        "state": "unavailable",
        "entry_paused": None,
        "new_risk_ready": False,
        "cursor_ms": None,
        "pending_order_count": None,
        "positions": None,
        "position_status": "unknown",
        "reason_code": "service_unavailable",
    }
    if service_ready is not True:
        return result
    if row.get("cache_present") is not True:
        result["reason_code"] = "cache_missing"
        return result
    state = row.get("cached_state")
    state = state if type(state) is str else None
    if row.get("runtime_blocked") is True or state != "ready":
        result["reason_code"] = (
            "account_conflict"
            if state == "conflict"
            else "account_unknown"
            if state == "unknown" or row.get("runtime_blocked") is True
            else "account_recovery_required"
            if state in {"financial_only", "shutdown_recovery_required"}
            else "account_invalid"
        )
        return result
    try:
        cursor = row["cursor_ms"]
        if (
            row["cached_id"] != row["declared_id"]
            or row["account_id"] != row["declared_id"]
            or row["settlement_asset"] != "USDT"
            or type(row["account_schema"]) is not int
            or row["account_schema"] not in {1, 2}
            or type(row["entry_paused"]) is not bool
            or type(row["new_risk_ready"]) is not bool
            or row["cursor_present"] is not True
            or cursor is not None
            and (type(cursor) is not int or not 0 <= cursor <= MAX_TIME or cursor % 60000)
        ):
            raise ValueError("invalid public account identity/control fields")
        positions = _positions(row["positions"])
    except (ValueError, TypeError, KeyError):
        result["reason_code"] = "account_invalid"
        return result
    pending = row.get("pending_order_count")
    if type(pending) is not int or not 0 <= pending <= MAX_POSITIONS:
        pending = None
    frame = row.get("pending_frame")
    incomplete = pending is None or type(frame) is not bool or frame
    result.update(
        state="pending" if incomplete else "ready",
        entry_paused=row["entry_paused"],
        new_risk_ready=row["new_risk_ready"] and not row["entry_paused"] and not incomplete,
        cursor_ms=cursor,
        pending_order_count=pending,
        positions=positions,
        position_status="confirmed",
        reason_code="pending_count_unknown"
        if pending is None
        else "pending_frame_unknown"
        if type(frame) is not bool
        else "pending_frame"
        if frame
        else "confirmed_cached_ledger",
    )
    return result


def mobile_margin_portfolio(selected, *, service_ready):
    """No clock/IO/authority: only confirmed cached virtual positions, not quotes."""
    valid = (
        type(selected) is tuple
        and len(selected) <= MAX_ACCOUNTS
        and all(type(row) is dict and _identity(row.get("declared_id")) for row in selected)
        and len({row["declared_id"] for row in selected}) == len(selected)
    )
    accounts = [] if not valid else [_project(row, service_ready) for row in selected]
    complete = valid and service_ready is True and all(row["state"] == "ready" for row in accounts)
    known = sum(row["positions"] is not None for row in accounts)
    return {
        "schema_version": 1,
        "scope": "virtual_margin_v2",
        "state": "ready" if complete else "partial" if known else "unavailable",
        "accounts": accounts,
        "summary": {
            "known_open_positions": sum(len(row["positions"] or ()) for row in accounts),
            "known_pending_orders": sum(row["pending_order_count"] or 0 for row in accounts),
            "unknown_accounts": sum(row["positions"] is None for row in accounts),
            "complete": complete,
        },
    }


def unavailable_margin_portfolio():
    """Unknown provider/registry, never an authoritative empty-account claim."""
    return mobile_margin_portfolio(None, service_ready=False)


def sanitize_margin_portfolio(value):
    """Validate/copy only public wire fields, never copy arbitrary provider data.

    This validates observation shape, not provider financial authority. Counts
    and completeness are recomputed and conflicting declarations are rejected.
    """
    try:
        if (
            type(value) is not dict
            or type(value.get("schema_version")) is not int
            or value["schema_version"] != 1
            or type(value.get("scope")) is not str
            or value.get("scope") != "virtual_margin_v2"
            or type(value.get("state")) is not str
            or value["state"] not in {"ready", "partial", "unavailable"}
            or type(value.get("accounts")) is not list
            or len(value["accounts"]) > MAX_ACCOUNTS
        ):
            raise ValueError("invalid public portfolio identity")
        accounts, identities = [], set()
        for row in value["accounts"]:
            if type(row) is not dict or not set(_ACCOUNT_FIELDS) <= row.keys():
                raise ValueError("invalid public account fields")
            if not _identity(row["account_id"]) or row["account_id"] in identities:
                raise ValueError("invalid public account identity")
            identities.add(row["account_id"])
            state, reason = row["state"], row["reason_code"]
            if (
                type(state) is not str
                or type(reason) is not str
                or type(row["position_status"]) is not str
            ):
                raise ValueError("invalid public account status")
            if state == "unavailable":
                if (
                    reason not in _UNAVAILABLE_REASONS
                    or row["positions"] is not None
                    or row["position_status"] != "unknown"
                    or row["entry_paused"] is not None
                    or row["new_risk_ready"] is not False
                    or row["cursor_ms"] is not None
                    or row["pending_order_count"] is not None
                ):
                    raise ValueError("unavailable account claims known values")
                positions = None
            else:
                if (
                    state not in {"ready", "pending"}
                    or row["position_status"] != "confirmed"
                    or type(row["entry_paused"]) is not bool
                    or type(row["new_risk_ready"]) is not bool
                    or row["new_risk_ready"]
                    and (state != "ready" or row["entry_paused"])
                ):
                    raise ValueError("invalid public known-account control")
                cursor = row["cursor_ms"]
                if cursor is not None and (
                    type(cursor) is not int or not 0 <= cursor <= MAX_TIME or cursor % 60000
                ):
                    raise ValueError("invalid public account cursor")
                pending = row["pending_order_count"]
                if pending is None:
                    if state != "pending" or reason != "pending_count_unknown":
                        raise ValueError("missing pending count claims completeness")
                elif type(pending) is not int or not 0 <= pending <= MAX_POSITIONS:
                    raise ValueError("invalid public pending count")
                if (
                    state == "ready"
                    and reason != "confirmed_cached_ledger"
                    or state == "pending"
                    and reason not in _PENDING_REASONS
                    or reason == "pending_count_unknown"
                    and pending is not None
                ):
                    raise ValueError("invalid public account reason")
                if type(row["positions"]) is not list or len(row["positions"]) > MAX_POSITIONS:
                    raise ValueError("invalid public position list")
                selected_positions = []
                for position in row["positions"]:
                    if type(position) is not dict:
                        raise ValueError("invalid public position packet")
                    selected_positions.append({key: position[key] for key in _POSITION_FIELDS})
                positions = _positions(selected_positions)
                if len(positions) != len(row["positions"]):
                    raise ValueError("public positions must contain open positions only")
            public = {key: row[key] for key in _ACCOUNT_FIELDS if key != "positions"}
            public["positions"] = positions
            accounts.append(public)
        complete = all(row["state"] == "ready" for row in accounts)
        if not accounts:
            complete = value["state"] == "ready"
        known = sum(row["positions"] is not None for row in accounts)
        state = "ready" if complete else "partial" if known else "unavailable"
        summary = {
            "known_open_positions": sum(len(row["positions"] or ()) for row in accounts),
            "known_pending_orders": sum(row["pending_order_count"] or 0 for row in accounts),
            "unknown_accounts": sum(row["positions"] is None for row in accounts),
            "complete": complete,
        }
        supplied = value.get("summary")
        if (
            value["state"] != state
            or type(supplied) is not dict
            or not summary.keys() <= supplied.keys()
            or type(supplied["complete"]) is not bool
            or any(type(supplied[key]) is not int for key in summary if key != "complete")
            or any(supplied[key] != member for key, member in summary.items())
        ):
            raise ValueError("public portfolio summary differs from actual observations")
        return {
            "schema_version": 1,
            "scope": "virtual_margin_v2",
            "state": state,
            "accounts": accounts,
            "summary": summary,
        }
    except (ValueError, TypeError, KeyError, InvalidOperation):
        return unavailable_margin_portfolio()
