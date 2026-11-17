"""Shadow API dispatch stays owner-only and never runs a model in the HTTP host."""

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.runtime.service import CoreService


class Shadow:
    def __init__(self):
        self.calls = []

    def snapshot(self):
        return {"state": "ready", "paused": True}

    def submit(self, identity, kind, parameters):
        self.calls.append((identity, kind, parameters))
        return {"id": identity, "state": "queued"}

    def status(self, identity):
        return {"id": identity, "state": "completed", "core_epoch": "historical-core"}

    def close(self):
        self.calls.append("close")


def test_owner_shadow_routes_submit_only_to_the_bounded_service(tmp_path):
    backend = Shadow()
    core = CoreService(tmp_path, shadow_inference=backend)
    try:
        snapshot = core.request("GET", "/api/shadow-models", core.token)
        assert snapshot["scope"] == "paper_shadow_only" and snapshot["paused"]
        result = core.request(
            "POST",
            "/api/shadow-models",
            core.token,
            {"id": "fixture", "kind": "prepare_fixture", "parameters": {"training_job_id": "x"}},
        )
        assert result["state"] == "queued" and result["core_epoch"] == snapshot["core_epoch"]
        assert backend.calls == [("fixture", "prepare_fixture", {"training_job_id": "x"})]
        historical = core.request("GET", "/api/shadow-models/fixture", core.token)
        assert historical["state"] == "completed"
        assert historical["operation_core_epoch"] == "historical-core"
        assert historical["core_epoch"] == snapshot["core_epoch"]
        assert core.state()["core"]["capabilities"]["shadow_inference"]
        for path, body in (
            ("/api/shadow-models?x=1", None),
            ("/api/shadow-models/nested/id", None),
        ):
            with pytest.raises(ValidationError):
                core.request("GET", path, core.token, body)
        with pytest.raises(ValidationError):
            core.request("POST", "/api/shadow-models", core.token, {"kind": "activate"})
    finally:
        core.close()
    assert backend.calls[-1] == "close"


def test_registered_mobile_device_cannot_prepare_or_activate_nn_candidate(tmp_path):
    backend = Shadow()
    core = CoreService(tmp_path, shadow_inference=backend)
    try:
        issue = core.request(
            "POST",
            "/api/commands",
            core.token,
            {
                "id": "pair",
                "kind": "issue_pairing",
                "payload": {"name": "phone", "permissions": ["read", "pause"]},
            },
        )
        paired = core.request(
            "POST", "/api/pair", "", {"code": issue["result"]["code"], "name": "phone"}
        )
        for method in ("GET", "POST"):
            with pytest.raises(PermissionError):
                core.request(
                    method,
                    "/api/shadow-models",
                    paired["token"],
                    {"id": "one", "kind": "activate", "parameters": {}},
                )
        assert backend.calls == []
    finally:
        core.close()


def test_unavailable_optional_shadow_does_not_stop_the_paused_paper_core(tmp_path):
    class Unavailable(Shadow):
        def start(self):
            raise RuntimeError("private metadata details must not enter public state")

    backend = Unavailable()
    core = CoreService(tmp_path, shadow_inference=backend)
    try:
        core._start_optional_shadow()
        state = core.state()
        assert state["core"]["paused"] and not state["core"]["live_orders_enabled"]
        assert state["core"]["shadow_start_error"] == "RuntimeError"
        assert "private metadata" not in str(state)
        assert any(row["code"] == "SHADOW_INFERENCE_UNAVAILABLE" for row in state["incidents"])
    finally:
        core.close()


@pytest.mark.parametrize(
    "receipt",
    [
        {"closed": True, "durable": False, "worker_alive": False},
        {"closed": True, "durable": True, "worker_alive": True},
    ],
)
def test_unconfirmed_shadow_close_is_reported_and_other_cleanup_still_runs(tmp_path, receipt):
    from types import SimpleNamespace

    core = CoreService(tmp_path)
    cleaned = []
    core.shadow_inference = SimpleNamespace(close=lambda: receipt)
    core.research = SimpleNamespace(close=lambda: cleaned.append("research"))
    with pytest.raises(StorageError, match="not confirmed"):
        core.close()
    assert cleaned == ["research"]
    # An uncertain child cannot release the namespace to a replacement owner.
    with pytest.raises(RuntimeError, match="already owns"):
        CoreService(tmp_path)
    assert core.instance.released is False
    core.shadow_inference.close = lambda: {
        "closed": True,
        "durable": True,
        "worker_alive": False,
    }
    core.close()
    assert core.instance.released is True
    restarted = CoreService(tmp_path)
    restarted.close()
