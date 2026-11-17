"""Core routing authenticates, fences epochs and reports owner cleanup failures."""

from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.runtime.service import CoreService

PATH = "/api/causal-inference"


class Backend:
    def __init__(self):
        self.calls = []

    def snapshot(self):
        return {"state": "disabled", "revision": 0, "active_deployment_id": None}

    def submit(self, identity, kind, parameters):
        self.calls.append((identity, kind, parameters))
        return {"id": identity, "state": "queued", "core_epoch": parameters["core_epoch"]}

    def status(self, identity):
        return {"id": identity, "state": "unknown", "core_epoch": "old-core"}

    def close(self):
        self.calls.append("closed")
        return {"closed": True, "durable": True, "worker_alive": False, "actor_alive": False}


def command(core, kind="activate"):
    return {
        "id": "explicit-operation",
        "kind": kind,
        "parameters": {"core_epoch": core.control.epoch, "revision": 0},
    }


def test_owner_routing_preserves_historical_epoch_without_authorizing_trading(tmp_path):
    backend = Backend()
    core = CoreService(tmp_path, causal_inference=backend)
    try:
        snapshot = core.request("GET", PATH, core.token)
        assert snapshot["state"] == "disabled" and not snapshot["strategy_delivery_enabled"]
        assert not snapshot["live_orders_enabled"]
        body = command(core)
        queued = core.request("POST", PATH, core.token, body)
        assert queued["state"] == "queued"
        assert backend.calls == [(body["id"], body["kind"], body["parameters"])]
        historical = core.request("GET", PATH + "/older", core.token)
        assert historical["operation_core_epoch"] == "old-core"
        assert historical["core_epoch"] == core.control.epoch
        assert core.state()["core"]["capabilities"]["causal_inference"]
    finally:
        core.close()
    assert backend.calls[-1] == "closed"


def test_binding_listing_uses_cache_only_and_never_accepts_a_mutation(tmp_path):
    from omega.domain.external_features import ExternalFeatureBinding
    from omega.domain.models import Instrument, Market

    binding = ExternalFeatureBinding(
        "causal_nn",
        "deployment",
        "primary",
        Instrument("binance", Market.USDM, "BTCUSDT"),
        "a" * 64,
        "b" * 64,
        ("NN.score",),
    )
    backend = Backend()
    backend.external_features = SimpleNamespace(list_bindings=lambda: (binding,))
    core = CoreService(tmp_path, causal_inference=backend)
    try:
        path = "/api/external-feature-bindings"
        result = core.request("GET", path, core.token)
        assert result["bindings"] == [binding.document()]
        assert result["strategy_delivery_enabled"] is True
        assert result["strategy_delivery_scope"] == "virtual_margin_v2_opt_in"
        assert not result["live_orders_enabled"] and backend.calls == []
        for suffix in ("", "?unknown=1"):
            with pytest.raises(ValidationError):
                core.request("POST", path + suffix, core.token, {})
        with pytest.raises(ValidationError):
            core.request("GET", path + "?unknown=1", core.token)
        assert backend.calls == []
    finally:
        core.close()


@pytest.mark.parametrize(
    "change", ["epoch", "top_extra", "missing", "not_object", "bad_kind", "bad_id"]
)
def test_bad_or_old_epoch_submission_never_reaches_backend(tmp_path, change):
    backend = Backend()
    core = CoreService(tmp_path, causal_inference=backend)
    try:
        body = command(core)
        if change == "epoch":
            body["parameters"]["core_epoch"] = "old"
        elif change == "top_extra":
            body["input_sha256"] = "a" * 64
        elif change == "missing":
            del body["parameters"]["core_epoch"]
        elif change == "bad_kind":
            body["kind"] = []
        elif change == "bad_id":
            body["id"] = "../escape"
        else:
            body["parameters"] = []
        with pytest.raises(ValidationError):
            core.request("POST", PATH, core.token, body)
        assert not backend.calls
    finally:
        core.close()


@pytest.mark.parametrize("suffix", ["?x=1", "/nested/id", "/"])
def test_query_or_noncanonical_operation_path_rejected(tmp_path, suffix):
    core = CoreService(tmp_path, causal_inference=Backend())
    try:
        with pytest.raises(ValidationError):
            core.request("GET", PATH + suffix, core.token)
    finally:
        core.close()


def test_mobile_cannot_read_model_state_or_submit_inference(tmp_path):
    backend = Backend()
    core = CoreService(tmp_path, causal_inference=backend)
    try:
        paired = core.request(
            "POST",
            "/api/commands",
            core.token,
            {
                "id": "pair",
                "kind": "issue_pairing",
                "payload": {"name": "phone", "permissions": ["read", "pause"]},
            },
        )
        device = core.request(
            "POST", "/api/pair", "", {"code": paired["result"]["code"], "name": "phone"}
        )
        for path in (PATH, "/api/external-feature-bindings"):
            for method in ("GET", "POST"):
                with pytest.raises(PermissionError):
                    core.request(method, path, device["token"], command(core))
        assert not backend.calls
    finally:
        core.close()


def test_shutdown_blocks_new_inference_but_allows_pause_control(tmp_path):
    backend = Backend()
    core = CoreService(tmp_path, causal_inference=backend)
    core.shutdown_management = SimpleNamespace(blocks_new_risk=True)
    try:
        with pytest.raises(ValidationError, match="shutdown"):
            core.request("POST", PATH, core.token, command(core))
        core.request("POST", PATH, core.token, command(core, "pause"))
        assert len(backend.calls) == 1 and backend.calls[0][1] == "pause"
    finally:
        core.shutdown_management = None
        core.close()


def test_optional_start_failure_keeps_paused_core_and_hides_exception_text(tmp_path):
    class Unavailable(Backend):
        def start(self):
            raise RuntimeError("private file/model details")

    core = CoreService(tmp_path, causal_inference=Unavailable())
    try:
        core._start_optional_causal()
        state = core.state()
        assert state["core"]["paused"] and state["core"]["causal_start_error"] == "RuntimeError"
        assert "private file/model details" not in str(state)
        assert any(row["code"] == "CAUSAL_INFERENCE_UNAVAILABLE" for row in state["incidents"])
    finally:
        core.close()


@pytest.mark.parametrize(
    "receipt",
    [
        None,
        {},
        {"closed": True, "durable": False},
        {
            "closed": True,
            "durable": True,
            "worker_alive": True,
            "actor_alive": False,
        },
    ],
)
def test_unconfirmed_actor_close_does_not_skip_other_core_cleanup(tmp_path, receipt):
    cleaned = []
    core = CoreService(tmp_path)
    core.causal_inference = SimpleNamespace(close=lambda: receipt)
    core.research = SimpleNamespace(close=lambda: cleaned.append("research"))
    with pytest.raises(StorageError, match="causal inference shutdown"):
        core.close()
    assert cleaned == ["research"]
    assert any(
        row["code"] == "CAUSAL_SHUTDOWN_UNCONFIRMED" and row["severity"] == "critical"
        for row in core.control.snapshot()["incidents"]
    )
    # A failed child shutdown cannot admit a replacement core into the namespace.
    with pytest.raises(RuntimeError, match="already owns"):
        CoreService(tmp_path)
    core.causal_inference.close = lambda: {
        "closed": True,
        "durable": True,
        "worker_alive": False,
        "actor_alive": False,
    }
    core.close()
    restarted = CoreService(tmp_path)
    restarted.close()


def test_missing_optional_service_has_explicit_unavailable_response(tmp_path):
    core = CoreService(tmp_path)
    try:
        with pytest.raises(ValidationError, match="unavailable"):
            core.request("GET", PATH, core.token)
    finally:
        core.close()
