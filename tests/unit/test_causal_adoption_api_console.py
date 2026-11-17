"""Owner control transport/consent checks; injected backend is not a resource proof."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.causal_adoption_client import PATH, adoption_body
from omega.runtime.causal_recovery_client import recovery_reset_body
from omega.runtime.console import ControlConsole
from omega.runtime.service import CoreService


def view(epoch="core"):
    return {
        "schema_version": 1,
        "scope": "cold_startup_observation",
        "core_epoch": epoch,
        "prior_phase": "candidate_prepared",
        "registry_sha256": "a" * 64,
        "candidate_generation": 1,
        "candidate_actor_token": "candidate",
        "startup_certificate_sha256": "b" * 64,
        "heads_sha256": "c" * 64,
        "observation_sha256": "d" * 64,
        "blockers": [],
        "eligible": True,
        "requires_confirmation": True,
        "factory_authority": False,
        "physical_close_claimed": False,
        "automatic_inference_started": False,
        "financial_resumed": False,
        "old_uncertainty_cleared": False,
    }


def api_view(epoch="core"):
    return {
        **view(epoch),
        "scope": "committed_inference_service",
        "adoption_scope": "cold_startup_observation",
        "live_orders_enabled": False,
    }


class Backend:
    def __init__(self):
        self.calls, self.epoch = [], None

    def adoption_snapshot(self):
        self.calls.append("cached")
        return view(self.epoch)

    def submit_adoption(self, identity, parameters):
        self.calls.append((identity, deepcopy(parameters)))
        return {"id": identity, "core_epoch": self.epoch, "state": "admitted", "durable": False}

    def recovery_status(self, identity):
        self.calls.append(("original", identity))
        return {"id": identity, "state": "unknown", "durable": False}

    def snapshot(self):
        return {"state": "disabled", "revision": 0, "active_deployment_id": None}

    def close(self):
        return {"closed": True, "durable": True, "worker_alive": False, "actor_alive": False}


@pytest.fixture
def core(tmp_path):
    backend = Backend()
    core = CoreService(tmp_path, causal_inference=backend)
    backend.epoch = core.control.epoch
    yield core, backend
    core.shutdown_management = None
    core.close()


def test_adoption_action_keeps_original_recovery_ids_and_owner_scope(core):
    core, backend = core
    result = core.request("GET", PATH, core.token)
    assert result["adoption_scope"] == "cold_startup_observation"
    assert result["scope"] == "committed_inference_service"
    body = adoption_body("explicit", result)
    admitted = core.request("POST", PATH, core.token, body)
    assert admitted["state"] == "admitted" and admitted["durable"] is False
    old = core.request("GET", "/api/causal-inference/recovery/adoption", core.token)
    assert old["id"] == "adoption"
    assert backend.calls == ["cached", ("explicit", body["parameters"]), ("original", "adoption")]


@pytest.mark.parametrize("mutation", ["epoch", "identity", "extra", "parameters"])
def test_invalid_adoption_envelope_never_reaches_backend(core, mutation):
    core, backend = core
    body = adoption_body("explicit", api_view(core.control.epoch))
    if mutation == "epoch":
        body["parameters"]["core_epoch"] = "another-core"
    elif mutation == "identity":
        body["id"] = "../path"
    elif mutation == "extra":
        body["factory"] = "override"
    else:
        body["parameters"] = []
    with pytest.raises(ValidationError):
        core.request("POST", PATH, core.token, body)
    assert backend.calls == []


def test_shutdown_denies_adoption_write_but_keeps_cached_read(core):
    core, backend = core
    core.shutdown_management = SimpleNamespace(blocks_new_risk=True)
    with pytest.raises(ValidationError, match="shutdown"):
        core.request(
            "POST", PATH, core.token, adoption_body("explicit", api_view(core.control.epoch))
        )
    core.request("GET", PATH, core.token)
    assert backend.calls == ["cached"]


def test_mobile_never_receives_adoption_permissions(core):
    core, backend = core
    pairing = core.request(
        "POST",
        "/api/commands",
        core.token,
        {
            "id": "pair",
            "kind": "issue_pairing",
            "payload": {"name": "phone", "permissions": ["read"]},
        },
    )
    paired = core.request("POST", "/api/pair", "", {"code": pairing["result"]["code"]})
    for method in ("GET", "POST"):
        with pytest.raises(PermissionError):
            core.request(
                method,
                PATH,
                paired["token"],
                adoption_body("explicit", api_view(core.control.epoch)),
            )
    assert backend.calls == []


def test_console_lost_adoption_response_preserves_id_without_retry(tmp_path):
    output, calls = [], []
    console = ControlConsole(tmp_path, output=output.append)

    def transport(method, path, body=None):
        calls.append((method, path, deepcopy(body)))
        if method == "POST":
            raise TimeoutError("lost response after admission")
        return api_view() if path == PATH else {"id": path.rsplit("/", 1)[1], "state": "unknown"}

    console.request = transport
    with pytest.raises(TimeoutError):
        console.execute("causal-adopt --confirm")
    identity = calls[1][2]["id"]
    assert identity in output[0] and "causal-adoption " + identity in output[1]
    assert console.execute("causal-adoption " + identity)["state"] == "unknown"
    assert [call[0] for call in calls] == ["GET", "POST", "GET"]


@pytest.mark.parametrize(
    "text", ["causal-adopt", "causal-adopt --confirm yes", "causal-adoption ../bad"]
)
def test_console_rejects_implicit_consent_or_bad_identity_without_transport(tmp_path, text):
    console = ControlConsole(tmp_path)
    console.request = lambda *_args: pytest.fail("invalid input cannot reach transport")
    with pytest.raises(ValueError):
        console.execute(text)


def test_regular_recovery_client_accepts_checked_historical_carry_scope():
    snapshot = {
        "schema_version": 1,
        "scope": "committed_inference_service",
        "core_epoch": "core",
        "actor_token": "actor",
        "generation": 1,
        "control_revision": 3,
        "retirement_sha256": None,
        "live_orders_enabled": False,
        "recovery_scope": "verified_historical_pin_carry",
        "historical_carry_available": True,
        "cold_interrupted_adoption_available": True,
        "automatic_inference_started": False,
        "financial_resumed": False,
        "state": "HELD",
        "blockers": [],
        "current_recovery_id": None,
    }
    assert recovery_reset_body("explicit", snapshot)["parameters"]["expected_generation"] == 1
    for name in ("recovery_scope", "state"):
        bad = {**snapshot, name: []}
        with pytest.raises(ValueError):
            recovery_reset_body("explicit", bad)
