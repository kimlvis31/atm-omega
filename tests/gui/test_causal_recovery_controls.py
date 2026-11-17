"""Qt fake-transport recovery checks; no model, account, database or host execution."""

import importlib.util
import os
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

    return QApplication.instance() or QApplication(["omega-nn-recovery-controls-tests"])


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
            self.state = {"core": {"epoch": "core-1", "capabilities": {"causal_inference": True}}}

        def request(self, method, path, payload=None, *, tag):
            self.calls.append((method, path, deepcopy(payload), tag))
            return tag

    return Client()


@pytest.fixture
def panel(client, qt_app):
    from omega.gui.causal_controls import CausalControls

    widget = CausalControls(client)
    widget.timer.stop()
    widget.show()
    widget.setChecked(True)
    yield widget
    widget.close()
    widget.deleteLater()
    qt_app.sendPostedEvents()
    qt_app.processEvents()


def summary(**changes):
    return {
        "schema_version": 1,
        "scope": "committed_inference_service",
        "live_orders_enabled": False,
        "core_epoch": "core-1",
        "state": "MANUAL_REQUIRED",
        "generation": 0,
        "actor_token": "actor-0",
        "control_revision": 1,
        "held": False,
        "current_recovery_id": None,
        "blockers": [],
        "retirement_sha256": None,
        "recovery_scope": "verified_pins0_reservations0",
        "historical_carry_available": False,
        "cold_interrupted_adoption_available": False,
        "automatic_inference_started": False,
        "financial_resumed": False,
        **changes,
    }


def accept(panel, value=None):
    recovery = panel.recovery
    recovery.setChecked(True)
    recovery.refresh()
    request = next(tag for tag, row in recovery._requests.items() if row["kind"] == "summary")
    panel.client.resultReceived.emit(request, value or summary())
    return recovery


def operation(recovery, state="running", durable=True):
    return {
        "id": recovery._target["id"],
        "core_epoch": recovery.epoch,
        "state": state,
        "durable": durable,
        "request_digest": "a" * 64,
        "result": None,
        "error": None,
    }


def submit(panel):
    recovery = accept(panel)
    recovery.confirm.setChecked(True)
    recovery.recover_button.click()
    return recovery


def normal_snapshot(generation=0, revision=0):
    return {
        "schema_version": 1,
        "scope": "committed_inference_service",
        "core_epoch": "core-1",
        "live_orders_enabled": False,
        "strategy_delivery_enabled": False,
        "revision": revision,
        "state": "disabled",
        "ready": True,
        "storage_available": True,
        "actor_alive": True,
        "server_time_ms": 1_700_000_000_000,
        "active_deployment_id": None,
        "deployments": [],
        "streams": [],
        "operations": [],
        "actor_generation": generation,
        "current_actor_token": f"actor-{generation}",
    }


def accept_normal(panel, value):
    panel.refresh()
    tag = next(tag for tag, row in panel._requests.items() if row["context"] == "snapshot")
    panel.client.resultReceived.emit(tag, value)


def test_recovery_starts_collapsed_and_reuses_parent_timer(panel):
    recovery = panel.recovery
    assert not recovery.isChecked() and not hasattr(recovery, "timer")
    assert not [call for call in panel.client.calls if "/recovery" in call[1]]
    recovery.confirm.setChecked(True)
    recovery.recover()
    assert not [call for call in panel.client.calls if call[0] == "POST"]


@pytest.mark.parametrize("condition", ["consent", "collapsed", "detached", "expired", "blocked"])
def test_recovery_never_posts_without_current_explicit_ready_consent(panel, condition):
    recovery = accept(
        panel,
        summary(blockers=["HISTORICAL_PINS_REQUIRE_CARRY"]) if condition == "blocked" else None,
    )
    recovery.confirm.setChecked(condition != "consent")
    if condition == "collapsed":
        panel.setChecked(False)
    elif condition == "detached":
        panel.set_attached(False)
    elif condition == "expired":
        recovery._received_at -= 11
    recovery.recover()
    assert not [call for call in panel.client.calls if call[0] == "POST"]


def test_explicit_reset_payload_and_progress_never_grant_summary_or_resume(panel):
    recovery = submit(panel)
    method, path, payload, tag = panel.client.calls[-1]
    assert method == "POST" and path.endswith("/recovery")
    assert len(payload["id"]) == 32
    assert payload["parameters"] == {
        "core_epoch": "core-1",
        "expected_generation": 0,
        "expected_control_revision": 1,
        "expected_actor_token": "actor-0",
        "expected_retirement_sha256": None,
        "state_policy": "reset",
        "confirm": True,
    }
    panel.client.resultReceived.emit(tag, operation(recovery))
    assert recovery.snapshot["generation"] == 0 and not recovery._fresh()
    assert recovery.receipt["state"] == "running"
    assert not recovery.recover_button.isEnabled()
    assert "자동 재개하지" in recovery.hint.text()
    assert [call[1] for call in panel.client.calls if call[0] == "POST"] == [path]


def test_pending_reset_disables_consent_without_an_unhandled_qt_slot_error(panel, monkeypatch):
    slot_errors = []
    monkeypatch.setattr("sys.excepthook", lambda kind, error, traceback: slot_errors.append(error))
    recovery = submit(panel)
    assert recovery._target is not None
    assert not recovery.confirm.isEnabled() and not recovery.recover_button.isEnabled()
    assert slot_errors == []


def test_lost_post_keeps_original_id_manual_get_never_reposts(panel):
    recovery = submit(panel)
    original = recovery._target["id"]
    panel.client.failed.emit(panel.client.calls[-1][3], "connection lost")
    assert recovery._target["unknown"]
    recovery._last_poll = 0
    recovery.tick()
    assert len([call for call in panel.client.calls if call[0] == "POST"]) == 1
    recovery.query_button.click()
    assert panel.client.calls[-1][:2] == (
        "GET",
        "/api/causal-inference/recovery/" + original,
    )
    assert original in recovery.request_label.text()


def test_pending_poll_budget_stops_at32_manual_same_id_remains_available(panel):
    recovery = submit(panel)
    panel.client.resultReceived.emit(panel.client.calls[-1][3], operation(recovery))
    original = recovery._target["id"]
    for _ in range(34):
        before = len(panel.client.calls)
        recovery._last_poll = 0
        panel._tick()
        if len(panel.client.calls) > before and recovery._requests:
            tag = next(iter(recovery._requests))
            panel.client.resultReceived.emit(tag, operation(recovery))
    polls = [call for call in panel.client.calls if call[1].endswith("/recovery/" + original)]
    assert len(polls) == 32 and recovery._polls == 32
    recovery.query_button.click()
    assert len([call for call in panel.client.calls if call[1].endswith(original)]) == 33
    assert len([call for call in panel.client.calls if call[0] == "POST"]) == 1


@pytest.mark.parametrize("hidden", ["collapsed", "hidden", "detached"])
def test_pending_poll_only_runs_while_expanded_visible_and_attached(panel, hidden):
    recovery = submit(panel)
    panel.client.resultReceived.emit(panel.client.calls[-1][3], operation(recovery))
    if hidden == "collapsed":
        recovery.setChecked(False)
    elif hidden == "hidden":
        panel.hide()
    else:
        panel.set_attached(False)
    count = len(panel.client.calls)
    recovery._last_poll = 0
    recovery.tick()
    assert len(panel.client.calls) == count


def test_epoch_change_drops_late_summary_and_retains_unknown_id(panel):
    recovery = submit(panel)
    original, late = recovery._target["id"], panel.client.calls[-1][3]
    panel.update_state({"core": {"epoch": "core-2", "capabilities": {"causal_inference": True}}})
    panel.client.resultReceived.emit(late, operation(recovery, "succeeded"))
    assert recovery.snapshot is None and recovery.receipt is None
    assert recovery._target["id"] == original and recovery._target["unknown"]
    assert not recovery.recover_button.isEnabled()


def test_new_generation_drops_old_summary_and_invalidates_normal_control_view(panel):
    accept_normal(panel, normal_snapshot(revision=20))
    recovery = accept(panel)
    recovery.refresh()
    late = next(iter(recovery._requests))
    # A separately accepted authority notification makes the old GET stale.
    panel._recovery_authority(summary(generation=1, actor_token="actor-1"))
    accept_normal(panel, normal_snapshot(generation=1, revision=0))
    panel.client.resultReceived.emit(late, summary())
    assert panel._actor_generation == 1 and panel.snapshot["revision"] == 0
    panel.refresh()
    late_normal = next(iter(panel._requests))
    panel.client.resultReceived.emit(late_normal, normal_snapshot(generation=0, revision=99))
    assert panel.snapshot["actor_generation"] == 1 and panel._minimum_revision == 0


def test_old_generation_complete_receipt_cannot_select_deployment_or_set_revision(panel):
    accept_normal(panel, normal_snapshot())
    panel._pending = {
        "id": "old-request",
        "kind": "activate",
        "epoch": "core-1",
        "unknown": False,
        "generation": 0,
        "actor_token": "actor-0",
    }
    panel._recovery_authority(summary(generation=1, actor_token="actor-1"))
    old = {
        "id": "old-request",
        "kind": "activate",
        "core_epoch": "core-1",
        "operation_actor_generation": 0,
        "operation_actor_token": "actor-0",
        "state": "complete",
        "durable": True,
        "revision": 99,
        "result": {"deployment_id": "old-deployment"},
    }
    panel._receipt(old)
    assert panel._minimum_revision == 0 and panel._deploy_selection is None
    assert panel._unresolved["old-request"]["unknown"]
    panel.reconcile()
    assert panel.client.calls[-1][1].endswith("/old-request")


def test_only_durable_manual_recovery_plus_fresh_binding_releases_old_history_hold(panel):
    accept_normal(panel, normal_snapshot())
    panel._pending = {
        "id": "lost-original",
        "kind": "predict_committed",
        "epoch": "core-1",
        "generation": 0,
        "actor_token": "actor-0",
        "unknown": True,
    }
    recovery = submit(panel)
    value = operation(recovery, "succeeded")
    value["result"] = {
        "generation": 1,
        "manual_required": True,
        "receipt_sha256": "b" * 64,
        "historical_carry_available": False,
    }
    panel.client.resultReceived.emit(panel.client.calls[-1][3], value)
    summary_tag = next(iter(recovery._requests))
    panel.client.resultReceived.emit(summary_tag, summary(generation=1, actor_token="actor-1"))
    assert "lost-original" in panel._unresolved and not panel._historical
    assert not panel._writable()
    accept_normal(panel, normal_snapshot(generation=1))
    assert not panel._unresolved and "lost-original" in panel._historical
    assert panel._writable() and panel._historical["lost-original"]["unknown"]
    assert panel.reconcile_button.isEnabled()
    panel.reconcile_button.click()
    assert panel.client.calls[-1][1].endswith("/lost-original")
    assert panel._minimum_revision == 0
    assert len([call for call in panel.client.calls if call[0] == "POST"]) == 1


def test_generation_change_without_positive_recovery_keeps_unresolved_hold(panel):
    accept_normal(panel, normal_snapshot())
    panel._pending = {
        "id": "lost-original",
        "kind": "predict_committed",
        "epoch": "core-1",
        "generation": 0,
        "actor_token": "actor-0",
        "unknown": True,
    }
    accept_normal(panel, normal_snapshot(generation=1))
    assert "lost-original" in panel._unresolved and not panel._historical
    assert not panel._writable()


def test_old_generation_late_operation_get_is_history_only(panel):
    recovery = submit(panel)
    panel.client.resultReceived.emit(panel.client.calls[-1][3], operation(recovery))
    recovery.query()
    tag = panel.client.calls[-1][3]
    panel._recovery_authority(summary(generation=1, actor_token="actor-1"))
    panel.client.resultReceived.emit(tag, operation(recovery, "succeeded"))
    assert recovery.receipt is None and recovery.snapshot is None
    assert recovery._target["unknown"] and recovery._target["historical"]
    assert not recovery.recover_button.isEnabled()


def test_legacy_old_epoch_receipt_is_not_current_control_authority(panel):
    panel._pending = {"id": "old-request", "kind": "activate", "epoch": "core-old", "unknown": True}
    panel._receipt(
        {
            "id": "old-request",
            "kind": "activate",
            "core_epoch": "core-old",
            "state": "complete",
            "durable": True,
            "revision": 99,
            "result": {"deployment_id": "old-deployment"},
        }
    )
    assert panel._pending["unknown"] and panel._deploy_selection is None
    assert panel._minimum_revision == 0


def test_legacy_snapshot_without_host_headers_still_accepts(panel):
    value = normal_snapshot()
    del value["actor_generation"], value["current_actor_token"]
    accept_normal(panel, value)
    assert panel._fresh() and not panel._generation_known


def test_summary_rejects_live_scope_and_unknown_carry_claim(panel):
    recovery = accept(panel, summary(live_orders_enabled=True))
    assert recovery.snapshot is None
    recovery.refresh()
    tag = next(iter(recovery._requests))
    panel.client.resultReceived.emit(tag, summary(historical_carry_available=True))
    assert recovery.snapshot is None and not recovery._fresh()


def test_summary_age_uses_request_start_and_new_generation_resets_consent(panel):
    recovery = accept(panel)
    recovery.confirm.setChecked(True)
    recovery.refresh()
    tag, row = next(iter(recovery._requests.items()))
    row["started_at"] = time.monotonic() - 11
    panel.client.resultReceived.emit(tag, summary(generation=1, actor_token="actor-1"))
    assert not recovery._fresh() and not recovery.confirm.isChecked()
    assert recovery.generation == 1 and not recovery.recover_button.isEnabled()
