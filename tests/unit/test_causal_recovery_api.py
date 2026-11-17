"""Owner-only recovery admission routing; fake NN, actual core control boundary."""

import threading
from types import SimpleNamespace

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.causal_api import RECOVERY_PATH
from omega.runtime.service import CoreService


class RecoveryBackend:
    def __init__(self):
        self.calls = []

    def snapshot(self):
        return {"state": "disabled", "revision": 0, "active_deployment_id": None}

    def status(self, identity):
        pytest.fail("recovery routes must not enter the old operation catch-all")

    def recovery_snapshot(self):
        self.calls.append("cached")
        return {
            "state": "HELD",
            "generation": 1,
            "control_revision": 2,
            "scope": "verified_pins0_reservations0",
        }

    def submit_recovery(self, identity, parameters):
        self.calls.append((identity, parameters))
        return {
            "id": identity,
            "core_epoch": parameters["core_epoch"],
            "state": "admitted",
            "durable": False,
        }

    def recovery_status(self, identity):
        self.calls.append(("status", identity))
        return None if identity == "missing" else {"id": identity, "state": "HELD"}

    def close(self):
        # Transport fixture has no NN resources; never a real-resource proof.
        return {"closed": True, "durable": True, "actor_alive": False, "worker_alive": False}


def body(core):
    return {
        "id": "explicit-recovery",
        "parameters": {
            "core_epoch": core.control.epoch,
            "expected_generation": 1,
            "expected_control_revision": 2,
            "expected_actor_token": "a" * 32,
            "expected_retirement_sha256": None,
            "state_policy": "reset",
            "confirm": True,
        },
    }


def test_recovery_routes_are_separate_and_admission_is_not_reported_durable(tmp_path):
    backend = RecoveryBackend()
    core = CoreService(tmp_path, causal_inference=backend)
    try:
        view = core.request("GET", RECOVERY_PATH, core.token)
        assert view["state"] == "HELD" and view["core_epoch"] == core.control.epoch
        assert view["recovery_scope"] == "verified_pins0_reservations0"
        assert view["scope"] == "committed_inference_service"
        request = body(core)
        admitted = core.request("POST", RECOVERY_PATH, core.token, request)
        assert admitted["state"] == "admitted" and admitted["durable"] is False
        assert backend.calls == ["cached", (request["id"], request["parameters"])]
        status = core.request("GET", RECOVERY_PATH + "/explicit-recovery", core.token)
        assert status["state"] == "HELD" and not status["live_orders_enabled"]
        with pytest.raises(ValidationError, match="not found"):
            core.request("GET", RECOVERY_PATH + "/missing", core.token)
    finally:
        core.close()


@pytest.mark.parametrize("mutation", ["epoch", "id", "extra", "parameters", "missing"])
def test_recovery_bad_envelopes_never_reach_backend(tmp_path, mutation):
    backend = RecoveryBackend()
    core = CoreService(tmp_path, causal_inference=backend)
    try:
        request = body(core)
        if mutation == "epoch":
            request["parameters"]["core_epoch"] = "previous-core"
        elif mutation == "id":
            request["id"] = "../path"
        elif mutation == "extra":
            request["factory"] = "untrusted"
        elif mutation == "parameters":
            request["parameters"] = []
        else:
            del request["parameters"]
        with pytest.raises(ValidationError):
            core.request("POST", RECOVERY_PATH, core.token, request)
        assert backend.calls == []
    finally:
        core.close()


def test_mobile_permissions_do_not_grant_generation_recovery(tmp_path):
    backend = RecoveryBackend()
    core = CoreService(tmp_path, causal_inference=backend)
    try:
        pairing = core.request(
            "POST",
            "/api/commands",
            core.token,
            {
                "id": "pair",
                "kind": "issue_pairing",
                "payload": {"name": "phone", "permissions": ["read", "pause"]},
            },
        )
        paired = core.request("POST", "/api/pair", "", {"code": pairing["result"]["code"]})
        for path in (RECOVERY_PATH, RECOVERY_PATH + "/existing"):
            for method in ("GET", "POST"):
                with pytest.raises(PermissionError):
                    core.request(method, path, paired["token"], body(core))
        assert backend.calls == []
    finally:
        core.close()


def test_active_core_shutdown_blocks_recovery_admission_but_keeps_cached_reads(tmp_path):
    backend = RecoveryBackend()
    core = CoreService(tmp_path, causal_inference=backend)
    core.shutdown_management = SimpleNamespace(blocks_new_risk=True)
    try:
        with pytest.raises(ValidationError, match="shutdown"):
            core.request("POST", RECOVERY_PATH, core.token, body(core))
        core.request("GET", RECOVERY_PATH, core.token)
        assert backend.calls == ["cached"]
    finally:
        core.shutdown_management = None
        core.close()


def test_shutdown_fences_before_manager_admission_and_cancel_waits_for_completion(tmp_path):
    backend, events = RecoveryBackend(), []
    backend.notify_shutdown_requested = lambda identity, active=True: events.append(
        (identity, active)
    )
    core = CoreService(tmp_path, causal_inference=backend)
    original = {
        "id": "stop",
        "core_epoch": core.control.epoch,
        "expected_revision": 0,
        "kind": "flatten_exit",
        "parameters": {},
        "state": "queued",
    }
    cancel = {
        "id": "cancel",
        "core_epoch": core.control.epoch,
        "expected_revision": 1,
        "kind": "cancel",
        "parameters": {},
        "state": "queued",
    }

    class Manager:
        core_epoch = core.control.epoch
        blocks_new_risk = False

        def submit(self, identity, *_args):
            if identity == "stop":
                assert events == [("stop", True)]
                self.blocks_new_risk = True
            return deepcopy(original if identity == "stop" else cancel)

        def snapshot(self):
            return {"operations": [deepcopy(original), deepcopy(cancel)]}

    from copy import deepcopy

    manager = Manager()
    core.shutdown_management = manager
    try:
        core.request(
            "POST",
            "/api/shutdown",
            core.token,
            {key: original[key] for key in original if key != "state"},
        )
        core.request(
            "POST",
            "/api/shutdown",
            core.token,
            {key: cancel[key] for key in cancel if key != "state"},
        )
        core.request("GET", "/api/shutdown", core.token)
        assert events == [("stop", True)]
        cancel["state"], manager.blocks_new_risk = "completed", False
        core.request("GET", "/api/shutdown", core.token)
        assert events == [("stop", True), ("stop", False)]
        # A later cached GET cannot repeat a cancel or implicitly resume inference.
        core.request("GET", "/api/shutdown", core.token)
        assert len(events) == 2
    finally:
        core.shutdown_management = None
        core.close()


def test_cached_cancel_cannot_clear_a_reserved_new_shutdown_fence(tmp_path):
    backend, events = RecoveryBackend(), []
    backend.notify_shutdown_requested = lambda identity, active=True: events.append(
        (identity, active)
    )
    core = CoreService(tmp_path, causal_inference=backend)
    reserved, release = threading.Event(), threading.Event()
    manager = SimpleNamespace(blocks_new_risk=False)
    core.shutdown_management = manager
    core._fence_causal_shutdown("original")
    stale = {
        "operations": [
            {"id": "original", "state": "canceled"},
        ]
    }

    def new_shutdown_admission():
        with core.owner_mutation_lock:
            core._fence_causal_shutdown("new-shutdown")
            reserved.set()
            assert release.wait(2)
            manager.blocks_new_risk = True

    worker = threading.Thread(target=new_shutdown_admission)
    worker.start()
    try:
        assert reserved.wait(2)
        core._sync_causal_shutdown_cancel(stale)
        assert all(active for _, active in events)
        release.set()
        worker.join(2)
        assert not worker.is_alive()
        core._sync_causal_shutdown_cancel(stale)
        assert all(active for _, active in events)
    finally:
        release.set()
        worker.join(2)
        core.shutdown_management = None
        core.close()


@pytest.mark.parametrize("suffix", ["?x=1", "/", "/nested/id"])
def test_noncanonical_recovery_paths_do_not_reach_backend(tmp_path, suffix):
    backend = RecoveryBackend()
    core = CoreService(tmp_path, causal_inference=backend)
    try:
        with pytest.raises(ValidationError):
            core.request("GET", RECOVERY_PATH + suffix, core.token)
        assert backend.calls == []
    finally:
        core.close()
