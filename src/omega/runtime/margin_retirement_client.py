"""Owner-only lifecycle wire contracts shared by the terminal and GUI."""

import re
from urllib.parse import quote

from omega.domain.errors import ValidationError

SCOPE = "virtual_margin_retirement"
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_SHA = re.compile(r"[a-f0-9]{64}\Z")
_REASON = re.compile(r"[A-Za-z0-9_]{1,128}\Z")
_KEYS = frozenset(
    {
        "schema_version",
        "scope",
        "core_epoch",
        "account_id",
        "state",
        "phase",
        "reason_code",
        "observation_sha256",
        "registered_parameters_sha256",
        "session_config_hash",
        "ledger_namespace",
        "paused",
        "open_positions",
        "pending_orders",
        "staged",
        "source_pending",
        "strategy_count",
        "source_record_count",
        "ledger_total",
        "operation_id",
        "receipt_sha256",
    }
)


def _identity(value):
    if type(value) is not str or _ID.fullmatch(value) is None:
        raise ValidationError("invalid retirement identity")
    return value


def _checksum(value):
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise ValidationError("invalid retirement checksum")
    return value


def retirement_url(account_id):
    return "/api/margin-trading/accounts/" + quote(_identity(account_id), safe="") + "/retirement"


def retirement_body(identity, account_id, observation_sha256, core_epoch):
    return {
        "id": _identity(identity),
        "kind": "retire",
        "core_epoch": _identity(core_epoch),
        "parameters": {
            "account_id": _identity(account_id),
            "expected_observation_sha256": _checksum(observation_sha256),
        },
    }


def retirement_response(account_id, core_epoch, *, state="unavailable", reason_code=None, **values):
    result = dict.fromkeys(_KEYS)
    result.update(
        schema_version=1,
        scope=SCOPE,
        core_epoch=core_epoch,
        account_id=account_id,
        state=state,
        reason_code=reason_code,
    )
    result.update(values)
    return validate_retirement_observation(result, account_id=account_id, core_epoch=core_epoch)


def validate_retirement_observation(value, *, account_id, core_epoch):
    _identity(account_id)
    _identity(core_epoch)
    if (
        type(value) is not dict
        or set(value) != _KEYS
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["scope"] != SCOPE
        or value["account_id"] != account_id
        or value["core_epoch"] != core_epoch
        or type(value["state"]) is not str
        or value["state"] not in {"ready", "unavailable", "retiring", "retired"}
        or (value["phase"] is not None and type(value["phase"]) is not str)
        or value["phase"] not in {None, "fenced", "prepared", "closed", "retired", "unknown"}
    ):
        raise ValidationError("invalid retirement response or stale identity")
    for name in (
        "observation_sha256",
        "registered_parameters_sha256",
        "session_config_hash",
        "ledger_namespace",
        "receipt_sha256",
    ):
        if value[name] is not None:
            _checksum(value[name])
    if value["operation_id"] is not None:
        _identity(value["operation_id"])
    reason = value["reason_code"]
    if reason is not None and (type(reason) is not str or _REASON.fullmatch(reason) is None):
        raise ValidationError("invalid retirement reason")
    for name in ("paused", "source_pending"):
        if value[name] is not None and type(value[name]) is not bool:
            raise ValidationError("retirement flags require native booleans")
    for name, upper in (
        ("open_positions", 100),
        ("pending_orders", 10000),
        ("staged", 10000),
        ("strategy_count", 100),
        ("source_record_count", 1000000),
        ("ledger_total", 10000),
    ):
        if value[name] is not None and (
            type(value[name]) is not int or not 0 <= value[name] <= upper
        ):
            raise ValidationError("retirement counters require bounded native integers")
    if value["state"] == "ready":
        if (
            value["reason_code"] is not None
            or value["phase"] is not None
            or value["paused"] is not True
            or value["source_pending"] is not False
            or any(value[name] != 0 for name in ("open_positions", "pending_orders", "staged"))
            or any(
                value[name] is None
                for name in (
                    "observation_sha256",
                    "registered_parameters_sha256",
                    "session_config_hash",
                    "ledger_namespace",
                    "strategy_count",
                    "source_record_count",
                    "ledger_total",
                )
            )
            or value["strategy_count"] < 1
            or value["source_record_count"] < 1
            or value["operation_id"] is not None
            or value["receipt_sha256"] is not None
        ):
            raise ValidationError("retirement readiness lacks confirmed flat paused evidence")
    elif value["state"] == "unavailable":
        if reason is None or value["observation_sha256"] is not None:
            raise ValidationError("unavailable retirement cannot authorize an operation")
    elif value["state"] == "retired":
        if value["phase"] != "retired" or any(
            value[name] is None
            for name in (
                "receipt_sha256",
                "operation_id",
                "registered_parameters_sha256",
                "session_config_hash",
                "ledger_namespace",
                "ledger_total",
            )
        ):
            raise ValidationError("retired account lacks its durable receipt")
    elif (
        value["phase"] not in {"fenced", "prepared", "closed", "unknown"}
        or value["operation_id"] is None
    ):
        raise ValidationError("retiring account lacks its original operation")
    return dict(value)


def history_load_url():
    return "/api/margin-trading/history"


def history_job_url(identity):
    return history_load_url() + "/" + quote(_identity(identity), safe="")


def history_load_body(identity, account_id, receipt_sha256, core_epoch):
    return {
        "id": _identity(identity),
        "account_id": _identity(account_id),
        "expected_receipt_sha256": _checksum(receipt_sha256),
        "core_epoch": _identity(core_epoch),
    }


def validate_retired_history(value, *, identity, core_epoch):
    _identity(identity)
    _identity(core_epoch)
    keys = {
        "schema_version",
        "scope",
        "job_id",
        "account_id",
        "receipt_sha256",
        "state",
        "reason_code",
        "cached",
        "ledger_total",
        "core_epoch",
    }
    if (
        type(value) is not dict
        or set(value) != keys
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["scope"] != "virtual_margin_retired_history"
        or value["job_id"] != identity
        or value["core_epoch"] != core_epoch
        or type(value["state"]) is not str
        or value["state"] not in {"queued", "running", "completed", "failed", "unavailable"}
        or type(value["cached"]) is not bool
    ):
        raise ValidationError("invalid retired-history response or stale identity")
    if value["account_id"] is not None:
        _identity(value["account_id"])
    if value["receipt_sha256"] is not None:
        _checksum(value["receipt_sha256"])
    if value["reason_code"] is not None and (
        type(value["reason_code"]) is not str or _REASON.fullmatch(value["reason_code"]) is None
    ):
        raise ValidationError("invalid retired-history reason")
    if value["ledger_total"] is not None and (
        type(value["ledger_total"]) is not int or not 0 <= value["ledger_total"] <= 10000
    ):
        raise ValidationError("invalid archived ledger count")
    if value["state"] == "completed" and (
        value["account_id"] is None
        or value["receipt_sha256"] is None
        or value["ledger_total"] is None
        or value["reason_code"] is not None
    ):
        raise ValidationError("completed history lacks confirmed archive identity")
    if value["cached"] and value["state"] != "completed":
        raise ValidationError("pending history cannot claim a loaded cache")
    return dict(value)
