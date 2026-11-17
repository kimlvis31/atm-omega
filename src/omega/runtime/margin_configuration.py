"""Bounded registered-configuration copies, never financial or model authority."""

import json
import math
import re
from hashlib import sha256

from omega.domain.errors import ValidationError

MAX_CONFIGURATION_BYTES = 131072
MAX_CONFIGURATION_DEPTH = 32
MAX_CONFIGURATION_NODES = 20000
_IDENTITY = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_REQUIRED = {"account", "strategies", "start_ms", "brackets"}
_OPTIONAL = {"fee_rate", "external_feature_wait_ms", "funding", "cost_policy"}
_STRATEGY = {"id", "instrument", "module", "configuration", "analysis", "constraints"}


def _copy_json(value, remaining, depth=0):
    # Native types only: do not invoke cached objects' deepcopy/repr/JSON hooks.
    remaining[0] -= 1
    if remaining[0] < 0 or depth > MAX_CONFIGURATION_DEPTH:
        raise ValidationError("registered configuration exceeds structural capacity")
    kind = type(value)
    if value is None or kind in {bool, int}:
        return value
    if kind is float and math.isfinite(value):
        return value
    if kind is str:
        remaining[1] -= len(value)
        if remaining[1] < 0:
            raise ValidationError("registered configuration string exceeds capacity")
        return value
    if kind is list:
        if len(value) > remaining[0]:
            raise ValidationError("registered configuration exceeds structural capacity")
        return [_copy_json(item, remaining, depth + 1) for item in value]
    if kind is dict:
        if len(value) * 2 > remaining[0] or any(type(key) is not str for key in value):
            raise ValidationError("registered configuration needs bounded native JSON keys")
        return {
            _copy_json(key, remaining, depth + 1): _copy_json(item, remaining, depth + 1)
            for key, item in value.items()
        }
    raise ValidationError("registered configuration needs finite native JSON")


def configuration_parameters(value, *, account_id=None):
    """Copy/hash raw settings without resolving modules, files, clocks or numbers.

    This validates the registered wrappers, not financial creation permission.
    Module/analysis data stays intact and must be revalidated before new use.
    Numeric strings retain their exact representation and canonical JSON uses
    the runtime registry's default ensure_ascii=True encoding.
    """
    copied = _copy_json(value, [MAX_CONFIGURATION_NODES, MAX_CONFIGURATION_BYTES])
    if type(copied) is not dict or not _REQUIRED <= copied.keys() <= _REQUIRED | _OPTIONAL:
        raise ValidationError("invalid registered configuration fields")
    account = copied["account"]
    if type(account) is not dict or set(account) != {"account_id", "balance", "positions"}:
        raise ValidationError("invalid registered account configuration")
    identity = account["account_id"]
    if (
        type(identity) is not str
        or _IDENTITY.fullmatch(identity) is None
        or (account_id is not None and identity != account_id)
    ):
        raise ValidationError("registered account identity mismatch")
    positions = account["positions"]
    strategies = copied["strategies"]
    if (
        type(positions) is not list
        or not 1 <= len(positions) <= 100
        or type(strategies) is not list
        or not 1 <= len(strategies) <= 100
        or type(copied["brackets"]) is not list
        or len(copied["brackets"]) > 100
    ):
        raise ValidationError("invalid registered configuration rows")
    position_fields = {"instrument", "owner_id", "margin_mode", "leverage", "budget_margin"}
    for row in positions:
        if type(row) is not dict or set(row) - {"isolated_collateral"} != position_fields:
            raise ValidationError("invalid registered position configuration")
    for row in strategies:
        if (
            type(row) is not dict
            or set(row) - {"external_features", "analysis_execution"} != _STRATEGY
        ):
            raise ValidationError("invalid registered strategy configuration")
    try:
        raw = json.dumps(copied, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError, UnicodeError, OverflowError):
        raise ValidationError("invalid canonical registered configuration") from None
    if len(raw) > MAX_CONFIGURATION_BYTES:
        raise ValidationError("registered configuration exceeds byte capacity")
    return copied, sha256(raw).hexdigest()


def configuration_response(account_id, *, parameters=None, reason_code=None):
    """Create the exact owner DTO; unavailable never resembles empty settings."""
    copied, digest = (
        configuration_parameters(parameters, account_id=account_id)
        if parameters is not None and reason_code is None
        else (None, None)
    )
    return {
        "schema_version": 1,
        "scope": "virtual_margin_configuration",
        "state": "ready" if copied is not None else "unavailable",
        "account_id": account_id,
        "configuration_sha256": digest,
        "parameters": copied,
        "reason_code": None if copied is not None else reason_code or "configuration_unconfirmed",
    }
