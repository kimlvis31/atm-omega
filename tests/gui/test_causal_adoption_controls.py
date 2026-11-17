"""Cold adoption GUI with fake transport; existing717 tests remain unchanged."""

import importlib.util
import os
import sys
import time
from copy import deepcopy

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["omega-cold-adoption-controls-tests"])


@pytest.fixture
def slot_errors(monkeypatch):
    errors = []
    monkeypatch.setattr(sys, "excepthook", lambda kind, value, trace: errors.append((kind, value)))
    yield errors
    assert errors == [], f"Unhandled Qt slot errors: {errors!r}"


@pytest.fixture
def client(qt_app):
    from PySide6.QtCore import QObject, Signal

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        stateReceived = Signal(dict)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls = []
            self.state = {"core": {"epoch": "core-new", "capabilities": {"causal_inference": True}}}

        def request(self, method, path, payload=None, *, tag):
            self.calls.append((method, path, deepcopy(payload), tag))
            return tag

    return Client()


@pytest.fixture
def panel(client, slot_errors):
    from omega.gui.causal_controls import CausalControls

    widget = CausalControls(client)
    widget.timer.stop()
    widget.show()
    widget.setChecked(True)
    yield widget
    widget.close()
    widget.deleteLater()


def observation(**changes):
    return {
        "schema_version": 1,
        "scope": "committed_inference_service",
        "adoption_scope": "cold_startup_observation",
        "live_orders_enabled": False,
        "core_epoch": "core-new",
        "prior_phase": "restored_manual",
        "eligible": True,
        "candidate_generation": 2,
        "candidate_actor_token": "old-candidate",
        "registry_sha256": "a" * 64,
        "startup_certificate_sha256": "b" * 64,
        "heads_sha256": "c" * 64,
        "observation_sha256": "d" * 64,
        "blockers": [],
        "requires_confirmation": True,
        "factory_authority": False,
        "physical_close_claimed": False,
        "automatic_inference_started": False,
        "financial_resumed": False,
        "old_uncertainty_cleared": False,
        **changes,
    }


def accept(panel, value=None):
    adoption = panel.adoption
    adoption.setChecked(True)
    adoption.refresh()
    tag = next(tag for tag, row in adoption._requests.items() if row["context"] == "snapshot")
    panel.client.resultReceived.emit(tag, value or observation())
    return adoption


def submit(panel):
    adoption = accept(panel)
    adoption.confirm.setChecked(True)
    adoption.adopt_button.click()
    return adoption


def operation(adoption, state="running", durable=True):
    return {
        "id": adoption._target["id"],
        "core_epoch": adoption.epoch,
        "kind": "cold_adoption",
        "state": state,
        "durable": durable,
        "request_digest": "1" * 64,
        "result": None,
        "error": None,
    }


def completion(adoption, **changes):
    value = operation(adoption, "succeeded")
    value["result"] = {
        "core_epoch": "core-new",
        "prior_core_epoch": "core-old",
        "generation": 3,
        "new_actor_token": "actor-new",
        "manual_required": True,
        "receipt_sha256": "2" * 64,
        "automatic_inference_started": False,
        "financial_resumed": False,
        "old_error_cleared": False,
        "old_request_replayed": False,
        "old_physical_close_claimed": False,
        **changes,
    }
    return value


def normal_snapshot(**changes):
    return {
        "schema_version": 1,
        "scope": "committed_inference_service",
        "core_epoch": "core-new",
        "live_orders_enabled": False,
        "strategy_delivery_enabled": False,
        "revision": 0,
        "state": "manual_required",
        "ready": True,
        "storage_available": True,
        "actor_alive": True,
        "server_time_ms": 1_700_000_000_000,
        "active_deployment_id": None,
        "deployments": [],
        "streams": [],
        "operations": [],
        "actor_generation": 3,
        "current_actor_token": "actor-new",
        **changes,
    }


def accept_normal(panel, value=None):
    panel.refresh()
    tag = next(tag for tag, row in panel._requests.items() if row["context"] == "snapshot")
    panel.client.resultReceived.emit(tag, value or normal_snapshot())


def old_request(epoch="core-old"):
    return {
        "id": "lost-original",
        "kind": "predict_committed",
        "epoch": epoch,
        "generation": 2,
        "actor_token": "old-candidate",
        "unknown": True,
    }


def test_folded_adoption_is_inert_and_has_no_idle_timer(panel):
    adoption = panel.adoption
    assert not adoption.isChecked() and not hasattr(adoption, "timer")
    assert not [call for call in panel.client.calls if "/actions/adopt" in call[1]]
    adoption.confirm.setChecked(True)
    adoption.adopt()
    assert not [call for call in panel.client.calls if call[0] == "POST"]


def test_explicit_click_posts_only_fresh_epoch_and_observation_with_new_uuid(panel):
    adoption = submit(panel)
    method, path, body, _ = panel.client.calls[-1]
    assert (method, path) == ("POST", "/api/causal-inference/recovery/actions/adopt")
    assert len(body["id"]) == 32 and body["id"] == adoption._target["id"]
    assert body["parameters"] == {
        "core_epoch": "core-new",
        "expected_observation_sha256": "d" * 64,
        "state_policy": "reset",
        "confirm": True,
    }
    assert not adoption.confirm.isChecked() and not adoption.adopt_button.isEnabled()
    adoption.adopt_button.click()
    assert len([call for call in panel.client.calls if call[0] == "POST"]) == 1


@pytest.mark.parametrize("condition", ["no_consent", "expired", "held", "collapsed", "detached"])
def test_no_post_without_fresh_ready_visible_attached_consent(panel, condition):
    adoption = accept(
        panel,
        observation(eligible=False, blockers=["COLD_STARTUP_CHANGED"])
        if condition == "held"
        else None,
    )
    adoption.confirm.setChecked(condition != "no_consent")
    if condition == "expired":
        adoption._received_at -= 11
    elif condition == "collapsed":
        panel.setChecked(False)
    elif condition == "detached":
        panel.set_attached(False)
    adoption.adopt()
    assert not [call for call in panel.client.calls if call[0] == "POST"]


def test_lost_post_retains_same_id_and_never_automatically_reposts(panel):
    adoption = submit(panel)
    original = adoption._target["id"]
    panel.client.failed.emit(panel.client.calls[-1][3], "lost response")
    adoption._last_poll = 0
    adoption.tick()
    assert adoption._target["unknown"] and original in adoption.request_label.text()
    adoption.query_button.click()
    assert panel.client.calls[-1][:2] == (
        "GET",
        "/api/causal-inference/recovery/" + original,
    )
    panel.client.resultReceived.emit(panel.client.calls[-1][3], operation(adoption, "unknown"))
    adoption.adopt()
    assert len([call for call in panel.client.calls if call[0] == "POST"]) == 1


def test_pending_polls_stop_at32_with_manual_same_id_available(panel):
    adoption = submit(panel)
    panel.client.resultReceived.emit(panel.client.calls[-1][3], operation(adoption))
    original = adoption._target["id"]
    for _ in range(34):
        adoption._last_poll = 0
        panel._tick()
        if adoption._requests:
            tag = next(iter(adoption._requests))
            panel.client.resultReceived.emit(tag, operation(adoption))
    polls = [call for call in panel.client.calls if call[1].endswith("/" + original)]
    assert len(polls) == 32 and adoption._polls == 32
    adoption.query_button.click()
    assert len([call for call in panel.client.calls if call[1].endswith("/" + original)]) == 33
    assert len([call for call in panel.client.calls if call[0] == "POST"]) == 1


@pytest.mark.parametrize("hidden", ["folded", "hidden", "detached"])
def test_pending_poll_requires_expansion_visibility_and_connection(panel, hidden):
    adoption = submit(panel)
    panel.client.resultReceived.emit(panel.client.calls[-1][3], operation(adoption))
    if hidden == "folded":
        adoption.setChecked(False)
    elif hidden == "hidden":
        panel.hide()
    else:
        panel.set_attached(False)
    count = len(panel.client.calls)
    adoption._last_poll = 0
    adoption.tick()
    assert len(panel.client.calls) == count


def test_epoch_change_discards_late_completion_and_preserves_original_id(panel):
    adoption = submit(panel)
    late_tag, late_value = panel.client.calls[-1][3], completion(adoption)
    original = adoption._target["id"]
    panel.update_state({"core": {"epoch": "core-next", "capabilities": {"causal_inference": True}}})
    panel.client.resultReceived.emit(late_tag, late_value)
    assert adoption.snapshot is None and adoption.receipt is None
    assert adoption._target["id"] == original and adoption._target["unknown"]
    assert not panel._adoption_releases


def test_changed_observation_clears_consent_and_age_includes_transport_time(panel):
    adoption = accept(panel)
    adoption.confirm.setChecked(True)
    adoption.refresh()
    tag, request = next(iter(adoption._requests.items()))
    request["started_at"] = time.monotonic() - 11
    panel.client.resultReceived.emit(tag, observation(observation_sha256="e" * 64))
    assert not adoption.confirm.isChecked() and not adoption._fresh()
    assert not adoption.adopt_button.isEnabled()


@pytest.mark.parametrize("change", ["nonbool", "live", "wrong_scope"])
def test_invalid_observation_keeps_adoption_disabled_without_slot_error(panel, change):
    value = observation()
    if change == "nonbool":
        value["eligible"] = 1
    elif change == "live":
        value["live_orders_enabled"] = True
    else:
        value["adoption_scope"] = "anything"
    adoption = accept(panel, value)
    assert adoption.snapshot is None and not adoption.adopt_button.isEnabled()


def test_cold_receipt_requires_new_fresh_exact_binding_before_history_partition(panel):
    panel._pending = old_request()
    adoption = submit(panel)
    prior_snapshot_tag = next(iter(panel._requests))
    panel.client.resultReceived.emit(panel.client.calls[-1][3], completion(adoption))
    assert panel.snapshot is None and not panel._historical
    assert panel._minimum_revision == 0
    panel.client.resultReceived.emit(prior_snapshot_tag, normal_snapshot(revision=99))
    assert panel.snapshot is None and not panel._historical
    accept_normal(panel)
    assert "lost-original" in panel._historical and not panel._unresolved
    assert panel._historical["lost-original"]["unknown"] and panel._pending is None
    assert panel._writable() and panel._minimum_revision == 0
    assert panel.operations.rowCount() == 1 and panel.reconcile_button.isEnabled()
    panel.reconcile_button.click()
    assert panel.client.calls[-1][1].endswith("/lost-original")
    assert not panel.trusted.isChecked() and not panel.shape_confirm.isChecked()
    assert len([call for call in panel.client.calls if call[0] == "POST"]) == 1


@pytest.mark.parametrize("change", ["no_receipt", "unknown", "wrong_token", "other_epoch"])
def test_no_old_history_release_without_exact_positive_cold_receipt(panel, change):
    panel._pending = old_request(epoch="unrelated-core" if change == "other_epoch" else "core-old")
    adoption = submit(panel)
    if change != "no_receipt":
        value = operation(adoption, "unknown") if change == "unknown" else completion(adoption)
        panel.client.resultReceived.emit(panel.client.calls[-1][3], value)
    accept_normal(
        panel,
        normal_snapshot(current_actor_token="wrong" if change == "wrong_token" else "actor-new"),
    )
    assert not panel._historical and "lost-original" in panel._unresolved
    assert not panel._writable()


def test_slot_error_capture_for_sync_transport_failure_and_native_boolean_controls(
    panel,
    monkeypatch,
    slot_errors,
):
    adoption = accept(panel)
    adoption.confirm.setChecked(True)

    def failed_transport(*_args, **_kwargs):
        raise RuntimeError("transport unavailable")

    monkeypatch.setattr(panel.client, "request", failed_transport)
    adoption.adopt_button.click()
    assert adoption._target["unknown"] and not adoption.adopt_button.isEnabled()
    adoption.query_button.click()
    panel.set_attached(False)
    panel.set_attached(True)
    panel.set_presentation("dark", "en")
    assert "automatically" in adoption.hint.text() and slot_errors == []


@pytest.mark.parametrize("capability", [False, True, 1, None])
def test_normal_recovery_accepts_only_native_cold_capability_without_granting_authority(
    capability,
):
    from omega.gui.causal_recovery_controls import _summary

    value = {
        "schema_version": 1,
        "scope": "committed_inference_service",
        "live_orders_enabled": False,
        "core_epoch": "core-new",
        "state": "HELD",
        "generation": None,
        "actor_token": None,
        "control_revision": 0,
        "held": True,
        "current_recovery_id": None,
        "blockers": ["COLD_MANUAL_REQUIRED"],
        "retirement_sha256": None,
        "recovery_scope": "verified_pins0_reservations0",
        "historical_carry_available": False,
        "cold_interrupted_adoption_available": capability,
        "automatic_inference_started": False,
        "financial_resumed": False,
    }
    if type(capability) is bool:
        assert _summary(value)["cold_interrupted_adoption_available"] is capability
    else:
        with pytest.raises(ValueError):
            _summary(value)
