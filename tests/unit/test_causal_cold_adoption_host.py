"""Actual same-Root no-model interruption and explicit new-owner adoption.

The child intentionally exits without managed shutdown after a certified manual
startup. This exercises parent exclusion, not an arbitrary worker-close claim.
"""

import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.inference import decoded
from omega.runtime.causal_feature_delivery_store import CausalFeatureDeliveryStore
from omega.runtime.causal_inference_service import CausalInferenceService
from omega.runtime.causal_recovery import CausalInferenceHost
from omega.runtime.causal_recovery_records import RecoveryJournal
from omega.runtime.causal_root_identity import capture_managed_root_identity
from omega.runtime.control import InstanceLock

CHILD = r"""
import json, os, sys, time
from pathlib import Path
from omega.runtime.causal_recovery import CausalInferenceHost
from omega.runtime.control import InstanceLock
root=Path(sys.argv[1])
lease=InstanceLock(root/'state/core.lock')
host=CausalInferenceHost(root,core_epoch='before',training_job_provider=lambda _:None,
 core_instance=lease)
host.start()
v=host.recovery_snapshot()
host.submit_recovery('original_request',{
 'core_epoch':'before','expected_generation':v['generation'],
 'expected_control_revision':v['control_revision'],'expected_actor_token':v['actor_token'],
 'expected_retirement_sha256':v['retirement_sha256'],'state_policy':'reset','confirm':True})
end=time.monotonic()+10
while time.monotonic()<end:
 row=host.recovery_status('original_request')
 if row['state'] not in ('admitted','running'):
  assert row['state']=='succeeded',row
  print(json.dumps({'generation':host.recovery_snapshot()['generation']}),flush=True)
  os._exit(0)
 time.sleep(.005)
raise RuntimeError('manual successor startup was not observed')
"""


def wait_for(predicate, timeout=6):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(0.005)
    raise AssertionError("bounded adoption host condition was not observed")


def crashed_namespace(root):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(__file__).parents[2] / "src")
    result = subprocess.run(
        [sys.executable, "-c", CHILD, str(root)],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["generation"] == 1
    return json.loads((root / "state/causal-recovery.json").read_text())["snapshot"]["host"]


def start(root, *, lease=True, journal_factory=RecoveryJournal, factory=None):
    owned = InstanceLock(root / "state/core.lock") if lease else None
    calls = []

    def actor_factory(*args, **kwargs):
        calls.append(kwargs["generation_context"])
        return (factory or CausalInferenceService)(*args, **kwargs)

    host = CausalInferenceHost(
        root,
        core_epoch="after",
        training_job_provider=lambda _: None,
        core_instance=owned,
        journal_factory=journal_factory,
        actor_factory=actor_factory,
    )
    host.start()
    return host, owned, calls


def request(host):
    return {
        "core_epoch": host.core_epoch,
        "expected_observation_sha256": host.adoption_snapshot()["observation_sha256"],
        "state_policy": "reset",
        "confirm": True,
    }


def terminal(host, key):
    return wait_for(
        lambda: (
            row
            if (row := host.recovery_status(key))["state"] not in {"admitted", "running"}
            else None
        )
    )


def stop_held(host, lease):
    try:
        with pytest.raises(StorageError):
            host.close()
    finally:
        if lease is not None:
            lease.close()


def test_actual_same_root_untouched_startup_adopts_new_request_and_manual_generation(tmp_path):
    prior = crashed_namespace(tmp_path)
    paths = [
        tmp_path / "state/causal-inference.json",
        tmp_path / "state/causal-feature-delivery.json",
    ]
    before = [path.read_bytes() for path in paths]
    host, lease, calls = start(tmp_path)
    try:
        view = host.adoption_snapshot()
        assert view["eligible"] and view["factory_authority"] is False
        assert calls == [] and before == [path.read_bytes() for path in paths]
        with pytest.raises(ValidationError):
            host.submit_adoption("original_request", request(host))
        body = request(host)
        admitted = host.submit_adoption("new_owner", body)
        assert admitted["state"] == "admitted" and admitted["durable"] is False
        result = terminal(host, "new_owner")
        assert result["state"] == "succeeded", result
        assert result["kind"] == "cold_adoption" and result["result"]["manual_required"]
        assert len(calls) == 1 and calls[0].generation == 2
        assert host.snapshot()["core_epoch"] == "after" and host.external_features.status().held
        assert host._current.actor.owner is None and host._current.actor.source is None
        assert result["result"]["old_physical_close_claimed"] is False
        assert result["result"]["old_request_replayed"] is False
        assert host._host_document["prior_operation_id"] == "original_request"
        assert host._host_document["prior_registry_sha256"] != ""
        assert prior["actor_token"] != host._current.context.actor_token
        assert host.submit_adoption("new_owner", body) == result
        with pytest.raises(ValidationError):
            host.submit_adoption("new_owner", {**body, "expected_observation_sha256": "f" * 64})
        assert host.close()["durable"]
    finally:
        lease.close()


@pytest.mark.parametrize("change", ["copied_root", "missing_lease", "changed_head"])
def test_unconfirmed_exclusion_or_head_keeps_namespace_held_and_factory_zero(tmp_path, change):
    source = tmp_path / "original"
    prior = crashed_namespace(source)
    root = source
    if change == "copied_root":
        root = tmp_path / "copy"
        shutil.copytree(source, root)
    elif change == "changed_head":
        store = CausalFeatureDeliveryStore(root)
        store.close()
    host, lease, calls = start(root, lease=change != "missing_lease")
    try:
        assert not host.adoption_snapshot()["eligible"] and calls == []
        with pytest.raises(TradingBlocked):
            host.submit_adoption("forbidden", request(host))
        assert host._host_document["phase"] == prior["phase"]
    finally:
        stop_held(host, lease)


def test_admission_is_memory_only_and_reclassifies_changed_heads_before_factory(tmp_path):
    crashed_namespace(tmp_path)
    entered, release = threading.Event(), threading.Event()

    class GateJournal(RecoveryJournal):
        def read_host(self):
            value = super().read_host()
            if getattr(self, "gate", False):
                entered.set()
                assert release.wait(4)
            return value

    host, lease, calls = start(tmp_path, journal_factory=GateJournal)
    host._journal.gate = True
    try:
        body = request(host)
        assert host.submit_adoption("changed_late", body)["state"] == "admitted"
        assert entered.wait(2) and calls == []
        store = CausalFeatureDeliveryStore(tmp_path)
        store.close()
        release.set()
        assert terminal(host, "changed_late")["state"] == "blocked"
        assert calls == []
    finally:
        release.set()
        stop_held(host, lease)


def test_lost_final_adoption_ack_keeps_same_candidate_and_original_id(tmp_path):
    crashed_namespace(tmp_path)

    class LostFinal(RecoveryJournal):
        def save_host(self, raw, *, expected_revision):
            ack = super().save_host(raw, expected_revision=expected_revision)
            if decoded(raw).get("phase") == "adopted_manual":
                raise StorageError("synthetic lost final new-owner receipt ACK")
            return ack

    host, lease, calls = start(tmp_path, journal_factory=LostFinal)
    try:
        body = request(host)
        host.submit_adoption("lost", body)
        result = terminal(host, "lost")
        assert result["state"] == "unknown" and len(calls) == 1
        candidate = host._current
        assert candidate is not None and host.external_features.status().held
        assert host.submit_adoption("lost", body)["state"] == "unknown"
        with pytest.raises(TradingBlocked):
            host.submit_adoption("another", body)
        assert host._current is candidate and len(calls) == 1
    finally:
        stop_held(host, lease)


def test_shutdown_wins_fresh_cold_binding_after_actual_candidate_acceptance(tmp_path):
    crashed_namespace(tmp_path)
    entered, release = threading.Event(), threading.Event()

    class GatePrepared(RecoveryJournal):
        def save_host(self, raw, *, expected_revision):
            ack = super().save_host(raw, expected_revision=expected_revision)
            if decoded(raw).get("phase") == "adoption_prepared":
                entered.set()
                assert release.wait(4)
            return ack

    host, lease, calls = start(tmp_path, journal_factory=GatePrepared)
    try:
        host.submit_adoption("shutdown_race", request(host))
        assert entered.wait(3) and len(calls) == 1
        host.notify_shutdown_requested("shutdown")
        release.set()
        assert terminal(host, "shutdown_race")["state"] == "unknown"
        assert host._current is None and host.external_features.status().held
        assert len(host._retained_candidates) == 1
    finally:
        release.set()
        stop_held(host, lease)


def test_current_clean_closed_manifest_restores_checked_pins_after_carry(tmp_path, monkeypatch):
    import test_causal_historical_carry_host as fixture

    lease = InstanceLock(tmp_path / "state/core.lock")
    original_host_class = fixture.CausalInferenceHost
    monkeypatch.setattr(
        fixture,
        "CausalInferenceHost",
        lambda *a, **k: original_host_class(*a, core_instance=lease, **k),
    )
    before, saved = fixture.create(tmp_path)
    before.submit_recovery("before_close", fixture.request(before))
    assert fixture.terminal(before, "before_close")["state"] == "succeeded"
    current = before._current.context
    assert before.close()["durable"]
    closed = before._host_document
    assert closed["actor_token"] == current.actor_token and closed["generation"] == 1
    assert closed["proof"]["actor_token"] == current.actor_token
    assert closed["current_carry_manifest_sha256"]
    lease.close()
    after, new_lease, calls = start(tmp_path)
    try:
        assert len(calls) == 1 and calls[0].generation == 2
        assert after._current.actor.owner is None and after._current.actor.source is None
        assert after.external_features.status().held and not after._cold_settlement_blocked
        assert after.external_features.settlement_pin_documents()
        after.external_features.release(saved["pin"], consumer_receipt_sha256="e" * 64)
        assert after.external_features.settlement_pin_documents() == ()
        assert after.close()["durable"]
    finally:
        new_lease.close()


def test_every_current_checkpoint_overwrites_a_preserved_foreign_root_stamp(tmp_path):
    lease = InstanceLock(tmp_path / "state/core.lock")
    host = CausalInferenceHost(
        tmp_path, core_epoch="core", training_job_provider=lambda _: None, core_instance=lease
    )
    try:
        host.start()
        actual = capture_managed_root_identity(tmp_path, lease, original_stream=lease.stream)
        assert actual is not None and host._host_document["managed_root_identity_sha256"] == actual
        host._host_document["managed_root_identity_sha256"] = "f" * 64
        assert host.close()["durable"]
        assert host._host_document["managed_root_identity_sha256"] == actual
    finally:
        lease.close()


@pytest.mark.parametrize("default_attempt_first", [False, True])
def test_explicit_preservation_closes_only_current_scope_without_blessing_namespace(
    tmp_path, default_attempt_first
):
    crashed_namespace(tmp_path)
    store = CausalFeatureDeliveryStore(tmp_path)
    store.close()  # Actual changed head deliberately makes adoption unavailable.
    path = tmp_path / "state/causal-recovery.json"
    original = path.read_bytes()
    host, lease, calls = start(tmp_path)
    try:
        assert not host.adoption_snapshot()["eligible"] and calls == []
        assert host.can_close_preserving_cold_namespace()
        if default_attempt_first:
            with pytest.raises(StorageError):
                host.close()
        receipt = host.close_preserving_cold_namespace()
        assert receipt["namespace_preserved"] and receipt["namespace_clean"] is False
        assert receipt["durable"] is False
        assert receipt["current_required_recording_state"] == "none_attempted"
        assert receipt["current_physical_cleanup_confirmed"] is True
        assert receipt["old_resource_close_claimed"] is False
        assert receipt["actor_alive"] is False and receipt["worker_alive"] is False
        assert path.read_bytes() == original and not host.thread.is_alive()
        assert host.close_preserving_cold_namespace() == receipt
        with pytest.raises(StorageError):
            host.close()
    finally:
        lease.close()


def test_unreturned_factory_attempt_never_becomes_no_factory_preservation(tmp_path):
    observed = []

    def fail_factory(*args, **kwargs):
        observed.append(host._factory_attempted)
        raise StorageError("synthetic unreturned factory acquisition uncertainty")

    host = CausalInferenceHost(
        tmp_path,
        core_epoch="core",
        training_job_provider=lambda _: None,
        actor_factory=fail_factory,
    )
    try:
        with pytest.raises(StorageError):
            host.start()
        assert observed == [1] and host._retained_candidates == []
        assert not host.can_close_preserving_cold_namespace()
        with pytest.raises(StorageError):
            host.close_preserving_cold_namespace()
    finally:
        with pytest.raises(StorageError):
            host.close()
