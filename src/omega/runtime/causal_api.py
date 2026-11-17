"""Owner-only routing for committed inference; no model or market IO here."""

import re

from omega.domain.errors import ValidationError

PATH = "/api/causal-inference"
BINDINGS_PATH = "/api/external-feature-bindings"
RECOVERY_PATH = PATH + "/recovery"
ADOPTION_PATH = RECOVERY_PATH + "/actions/adopt"


def causal_request(provider, method, path, query, body, *, core_epoch):
    if query:
        raise ValidationError("committed inference does not accept query parameters")
    external = getattr(provider, "external_features", None)
    if path == ADOPTION_PATH and method == "GET":
        read = getattr(provider, "adoption_snapshot", None)
        if not callable(read):
            raise ValidationError("cold adoption is unavailable")
        result = {**read()}
        result["adoption_scope"] = result.get("scope")
    elif path == ADOPTION_PATH and method == "POST":
        submit = getattr(provider, "submit_adoption", None)
        if (
            not callable(submit)
            or type(body) is not dict
            or set(body) != {"id", "parameters"}
            or type(body["id"]) is not str
            or re.fullmatch(r"[A-Za-z0-9_-]{1,96}", body["id"]) is None
            or type(body["parameters"]) is not dict
            or body["parameters"].get("core_epoch") != core_epoch
        ):
            raise ValidationError("invalid cold adoption request")
        result = submit(body["id"], body["parameters"])
    elif path == RECOVERY_PATH and method == "GET":
        read = getattr(provider, "recovery_snapshot", None)
        if not callable(read):
            raise ValidationError("committed inference recovery is unavailable")
        result = {**read()}
        result["recovery_scope"] = result.get("scope")
    elif path == RECOVERY_PATH and method == "POST":
        submit = getattr(provider, "submit_recovery", None)
        if not callable(submit):
            raise ValidationError("committed inference recovery is unavailable")
        if (
            type(body) is not dict
            or set(body) != {"id", "parameters"}
            or type(body["id"]) is not str
            or re.fullmatch(r"[A-Za-z0-9_-]{1,96}", body["id"]) is None
            or type(body["parameters"]) is not dict
            or body["parameters"].get("core_epoch") != core_epoch
        ):
            raise ValidationError("invalid committed inference recovery request")
        result = submit(body["id"], body["parameters"])
    elif path.startswith(RECOVERY_PATH + "/") and method == "GET":
        read = getattr(provider, "recovery_status", None)
        recovery_id = path[len(RECOVERY_PATH) + 1 :]
        if not callable(read) or re.fullmatch(r"[A-Za-z0-9_-]{1,96}", recovery_id) is None:
            raise ValidationError("invalid committed inference recovery operation")
        result = read(recovery_id)
        if result is None:
            raise ValidationError("committed inference recovery operation not found")
    elif path == BINDINGS_PATH and method == "GET":
        result = {
            "bindings": []
            if external is None
            else [row.document() for row in external.list_bindings()],
            "server_time_ms": provider.snapshot().get("server_time_ms"),
        }
    elif path == PATH and method == "GET":
        result = provider.snapshot()
    elif (
        path == PATH
        and method == "POST"
        and type(body) is dict
        and set(body) == {"id", "kind", "parameters"}
    ):
        if (
            type(body["id"]) is not str
            or re.fullmatch(r"[A-Za-z0-9_-]{1,96}", body["id"]) is None
            or type(body["kind"]) is not str
            or not 1 <= len(body["kind"]) <= 48
        ):
            raise ValidationError("invalid committed inference command identity")
        if (
            type(body["parameters"]) is not dict
            or body["parameters"].get("core_epoch") != core_epoch
        ):
            raise ValidationError("committed inference operation belongs to another core epoch")
        result = provider.submit(body["id"], body["kind"], body["parameters"])
    elif path.startswith(PATH + "/") and method == "GET":
        identity = path[len(PATH) + 1 :]
        if re.fullmatch(r"[A-Za-z0-9_-]{1,96}", identity) is None:
            raise ValidationError("invalid committed inference operation ID")
        result = provider.status(identity)
    else:
        raise ValidationError("invalid committed inference operation")
    origin = (
        {"operation_core_epoch": result["core_epoch"]}
        if "id" in result and "core_epoch" in result
        else {}
    )
    return {
        **result,
        **origin,
        "core_epoch": core_epoch,
        "scope": "committed_inference_service",
        "live_orders_enabled": False,
        "strategy_delivery_enabled": external is not None,
        "strategy_delivery_scope": "virtual_margin_v2_opt_in" if external is not None else None,
    }
