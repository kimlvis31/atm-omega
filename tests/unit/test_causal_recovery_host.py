"""Host seam cases; default no-model actors and actual temporary journal stores.

No market source, database, model child, financial account, or carry is started.
The explicit provider fake is used only for borrower/coordinator scheduling.
"""

import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_causal_generation_facade import FRAME, RECEIPT, Provider
from test_causal_inference_service import activate, predict, reset
from test_causal_inference_service import create as create_actor

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.external_features import ExternalFeaturePin
from omega.domain.inference import decoded, digest, encoded
from omega.runtime.causal_generation_facade import GenerationBinding
from omega.runtime.causal_generation_outcomes import GenerationContext
from omega.runtime.causal_recovery import CausalInferenceHost
from omega.runtime.causal_recovery_proofs import (
    RecoveryProofArchive,
    pending_unknown_evidence,
    verified_journal_head,
)
from omega.runtime.causal_recovery_records import FeatureMutationUnknown, RecoveryJournal
from omega.runtime.control import InstanceLock


def wait_for(predicate, timeout=4):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = predicate()
        if result:
            return result
        time.sleep(0.005)
    raise AssertionError("bounded host condition was not observed")


def request(host):
    view = host.recovery_snapshot()
    return {
        "core_epoch": view["core_epoch"],
        "expected_generation": view["generation"],
        "expected_control_revision": view["control_revision"],
        "expected_actor_token": view["actor_token"],
        "expected_retirement_sha256": view["retirement_sha256"],
        "state_policy": "reset",
        "confirm": True,
    }


def operation(host, key):
    return wait_for(
        lambda: (
            value
            if (value := host.recovery_status(key))["state"] not in {"admitted", "running"}
            else None
        )
    )


def create(tmp_path, **kwargs):
    host = CausalInferenceHost(
        tmp_path, core_epoch="core", training_job_provider=lambda _: None, **kwargs
    )
    host.start()
    return host


def test_default_no_model_replacement_stable_port_exact_barrier_and_manual_start(tmp_path):
    host = create(tmp_path)
    stable, old = host.external_features, host._current
    try:
        assert host.recovery_snapshot()["state"] == "MANUAL_REQUIRED"
        assert stable.status().held and stable.list_bindings() == ()
        admitted = host.submit_recovery("replacement", request(host))
        assert admitted["state"] == "admitted" and admitted["durable"] is False
        result = operation(host, "replacement")
        assert result["state"] == "succeeded", result
        assert result["durable"] and result["result"]["manual_required"]
        assert host.external_features is stable and host._current is not old
        assert stable.status().held and stable.status().generation == 1
        assert old.actor.retirement_snapshot().physical_confirmed
        assert old.actor.actor_join_observation().original_thread_dead
        assert len(host._history) == 1
        assert host._host_document["barrier"]["mode"] == "old_pause_ack"
        receipt = host._host_document["receipt"]
        assert receipt["automatic_inference_started"] is False
        assert receipt["financial_resumed"] is False and receipt["old_error_cleared"] is False
        assert host.recovery_snapshot()["historical_carry_available"] is False
    finally:
        assert host.close()["durable"]


def test_recovery_memory_admission_does_not_wait_for_journal_and_same_id_is_status(tmp_path):
    entered, release = threading.Event(), threading.Event()

    class SlowJournal(RecoveryJournal):
        def save_host(self, raw, *, expected_revision):
            if decoded(raw).get("phase") == "intent":
                entered.set()
                assert release.wait(3)
            return super().save_host(raw, expected_revision=expected_revision)

    host = create(tmp_path, journal_factory=SlowJournal)
    body = request(host)
    try:
        admitted = host.submit_recovery("once", body)
        assert admitted["state"] == "admitted"
        assert entered.wait(1)
        assert host.submit_recovery("once", body)["request_digest"] == admitted["request_digest"]
        with pytest.raises(ValidationError):
            host.submit_recovery("once", {**body, "expected_control_revision": 9})
        assert len(host._retained_candidates) == 1
        release.set()
        assert operation(host, "once")["state"] == "succeeded"
        assert len(host._retained_candidates) == 2
        host.submit_recovery("once", body)
        assert len(host._retained_candidates) == 2
    finally:
        release.set()
        host.close()


@pytest.mark.parametrize(
    "blocker", ["capture_failed", "recording_unknown", "journal_write_pending"]
)
def test_physical_or_recording_blocker_makes_zero_candidate_factory_calls(
    tmp_path, monkeypatch, blocker
):
    host = create(tmp_path)
    old = host._current.actor
    original = old.retirement_snapshot

    def blocked():
        snapshot = original()
        return replace(snapshot, **{blocker: True}) if snapshot is not None else None

    monkeypatch.setattr(old, "retirement_snapshot", blocked)
    try:
        host.submit_recovery("blocked", request(host))
        assert operation(host, "blocked")["state"] == "unknown"
        assert len(host._retained_candidates) == 1
        with pytest.raises(TradingBlocked):
            host.submit_recovery("another", request(host))
    finally:
        monkeypatch.setattr(old, "retirement_snapshot", original)
        # Unknown recovery publication cannot become a strict durable close ACK.
        with pytest.raises(StorageError):
            host.close()


def test_lost_final_receipt_ack_holds_same_candidate_without_second_factory(tmp_path):
    class LostReceipt(RecoveryJournal):
        def save_host(self, raw, *, expected_revision):
            result = super().save_host(raw, expected_revision=expected_revision)
            if decoded(raw).get("phase") == "restored_manual":
                raise StorageError("injected lost receipt ACK")
            return result

    host = create(tmp_path, journal_factory=LostReceipt)
    body = request(host)
    host.submit_recovery("receipt", body)
    assert operation(host, "receipt")["state"] == "unknown"
    candidate = host._current
    assert candidate.context.generation == 1 and host.external_features.status().held
    assert len(host._retained_candidates) == 2
    host.submit_recovery("receipt", body)
    with pytest.raises(TradingBlocked):
        host.submit_recovery("new", request(host))
    assert host._current is candidate and len(host._retained_candidates) == 2
    with pytest.raises(StorageError):
        host.close()


def test_close_wins_candidate_install_cas(tmp_path):
    prepared, release = threading.Event(), threading.Event()

    class PausedPrepared(RecoveryJournal):
        def save_host(self, raw, *, expected_revision):
            ack = super().save_host(raw, expected_revision=expected_revision)
            if decoded(raw).get("phase") == "candidate_prepared":
                prepared.set()
                assert release.wait(3)
            return ack

    host = create(tmp_path, journal_factory=PausedPrepared)
    old = host._current
    host.submit_recovery("closing", request(host))
    assert prepared.wait(2)
    errors = []

    def close():
        try:
            host.close()
        except StorageError as error:
            errors.append(error)

    closer = threading.Thread(target=close)
    closer.start()
    wait_for(lambda: host.external_features.status().closing)
    release.set()
    closer.join(4)
    assert not closer.is_alive() and host._current is old
    assert host.recovery_status("closing")["state"] == "unknown"
    assert errors and host.external_features.status().held


def test_coordinator_services_preparation_and_terminal_while_recovery_waits_borrow(
    tmp_path, monkeypatch
):
    host = create(tmp_path)
    provider = Provider()
    binding = host._current
    # Explicit synthetic feature provider, never a physical-retirement proof.
    fake_binding = GenerationBinding("core", 0, binding.context.actor_token, provider)
    host.external_features._binding = fake_binding
    host.external_features._bindings = provider.list_bindings()
    host.external_features.enable_current(
        0, binding.context.actor_token, expected_control_revision=0
    )
    dispatch_entered, allow_dispatch = threading.Event(), threading.Event()
    provider.on_dispatch = lambda _: (dispatch_entered.set(), allow_dispatch.wait(3))
    monkeypatch.setattr(host, "_ack_owner", lambda *_: None)
    monkeypatch.setattr(host, "_consume_terminal", lambda *_: True)
    results, errors = [], []

    def pin():
        try:
            results.append(
                host.external_features.pin(
                    "consumer", FRAME, provider.snapshot, provider.candle, decision_at_ms=60000
                )
            )
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=pin)
    worker.start()
    assert dispatch_entered.wait(1)
    host.submit_recovery("drain", request(host))
    wait_for(lambda: host.recovery_snapshot()["state"] == "RECOVERY_HELD")
    assert host.external_features.status().borrows == 1
    allow_dispatch.set()
    worker.join(2)
    assert not worker.is_alive() and not errors and len(results) == 1
    # The coordinator has persisted terminal acknowledgement rather than
    # deadlocking on the borrower that was waiting for that acknowledgement.
    assert operation(host, "drain")["state"] == "blocked"
    assert len(host._retained_candidates) == 1
    host.external_features.release(results[0], consumer_receipt_sha256=RECEIPT)
    assert host.close()["durable"]


def test_wait_timeout_is_rpc_unknown_without_recording_failure(tmp_path):
    prepare_entered, release = threading.Event(), threading.Event()

    class DelayedPrepare(RecoveryJournal):
        def prepare_mutation(self, ref, args):
            prepare_entered.set()
            assert release.wait(2)
            return super().prepare_mutation(ref, args)

    host = create(tmp_path, journal_factory=DelayedPrepare)
    normal_timeout = host.ack_timeout_s
    host.ack_timeout_s = 0.05
    provider = Provider()
    fake = GenerationBinding("core", 0, host._current.context.actor_token, provider)
    host.external_features._binding = fake
    host.external_features._bindings = provider.list_bindings()
    host.external_features.enable_current(0, fake.actor_token, expected_control_revision=0)
    errors = []

    def pin():
        try:
            host.external_features.pin(
                "consumer", FRAME, provider.snapshot, provider.candle, decision_at_ms=60000
            )
        except FeatureMutationUnknown as error:
            errors.append(error)

    worker = threading.Thread(target=pin)
    try:
        worker.start()
        assert prepare_entered.wait(1)
        worker.join(1)
        assert not worker.is_alive() and errors and errors[0].recording_unknown is False
        assert provider.calls == []
        release.set()
        host.ack_timeout_s = normal_timeout
        wait_for(lambda: host.external_features.status().borrows == 0)
        for ref in host.external_features.pending_mutations():
            assert host.reconcile_mutation(ref)
        assert host.external_features.status().hot_mutations == 0
    finally:
        release.set()
        host.ack_timeout_s = normal_timeout
        worker.join(2)
        assert host.close()["durable"]


def test_clean_restart_adopts_new_manual_generation_under_current_root_lease(tmp_path):
    lease = InstanceLock(tmp_path / "state" / "core.lock")
    first = create(tmp_path, core_instance=lease)
    first.close()
    lease.close()
    second_lease = InstanceLock(tmp_path / "state" / "core.lock")
    second = CausalInferenceHost(
        tmp_path,
        core_epoch="newcore",
        training_job_provider=lambda _: None,
        core_instance=second_lease,
    )
    try:
        second.start()
        assert second.recovery_snapshot()["state"] == "MANUAL_REQUIRED"
        assert second.external_features.status().generation == 1
        assert second.external_features.status().held
        assert second._host_document["past_close_ack_claimed"] is False
        assert second.snapshot()["core_epoch"] == "newcore"
        assert second.close()["durable"]
    finally:
        second_lease.close()


def test_interrupted_registry_cannot_become_clean_by_shutdown_or_factory(tmp_path):
    journal = RecoveryJournal(tmp_path)
    revision, _ = journal.read_host()
    journal.save_host(
        encoded(
            {
                "phase": "candidate_prepared",
                "generation": 4,
                "actor_token": "original",
                "manual_required": True,
            }
        ),
        expected_revision=revision,
    )
    journal.close()
    factories = []
    host = create(tmp_path, actor_factory=lambda *a, **k: factories.append((a, k)))
    assert host.recovery_snapshot()["blockers"] == ["COLD_REGISTRY_MANUAL_RECONCILIATION"]
    assert factories == []
    with pytest.raises(StorageError):
        host.close()
    assert host._host_document["phase"] == "candidate_prepared"


def test_fault_old_ready_head_is_bound_as_new_host_fault_barrier(tmp_path, monkeypatch):
    host = create(tmp_path)
    old = host._current.actor

    def fail_suspend(*, retire=False):
        raise StorageError("synthetic normal-cleanup fault")

    monkeypatch.setattr(old, "_suspend", fail_suspend)
    try:
        host.submit_recovery("fault", request(host))
        result = operation(host, "fault")
        assert result["state"] == "succeeded", result
        barrier = host._host_document["barrier"]
        assert barrier["mode"] == "host_fault_barrier"
        assert barrier["old_pause_ack_sha256"] is None
        assert host._host_document["proof"]["original_errors"]
        assert host._host_document["proof"]["normal_model_cleanup"] == "unconfirmed"
        assert host._host_document["proof"]["all_kernel"] is False
    finally:
        host.close()


def test_public_cached_views_do_not_call_journal_or_change_generation(tmp_path, monkeypatch):
    host = create(tmp_path)
    try:
        original = host._current

        def forbidden(*args, **kwargs):
            raise AssertionError("cached GET performed journal IO")

        for method in ("read_host", "get_mutation", "save_host"):
            monkeypatch.setattr(host._journal, method, forbidden)
        assert host.snapshot()["core_epoch"] == "core"
        assert host.recovery_snapshot()["state"] == "MANUAL_REQUIRED"
        assert host.external_features.list_bindings() == ()
        with pytest.raises(ValidationError):
            host.recovery_status("unknown")
        assert host._current is original
        monkeypatch.undo()
    finally:
        host.close()


def test_generation_overlays_preserve_old_receipt_and_ignore_old_success(tmp_path, monkeypatch):
    host = create(tmp_path)
    old = host._current
    try:
        host.submit_recovery("generation", request(host))
        assert operation(host, "generation")["state"] == "succeeded"
        current = host._current
        host._controls["late"] = (old, "reset_stream")
        old_row = {"id": "late", "state": "complete", "revision": 7}
        monkeypatch.setattr(old.actor, "status", lambda _: old_row)
        result = host.status("late")
        assert result["operation_actor_generation"] == 0
        assert result["operation_actor_token"] == old.context.actor_token
        assert result["current_actor_generation"] == 1
        assert old_row == {"id": "late", "state": "complete", "revision": 7}
        snapshot = host.snapshot()
        assert snapshot["actor_generation"] == 1
        assert snapshot["current_actor_token"] == current.context.actor_token
        assert host.external_features.status().held
        assert host.recovery_snapshot()["state"] == "MANUAL_REQUIRED"
        unknown = host._operation_overlay(old_row, None)
        assert unknown["operation_actor_generation"] is None
    finally:
        host.close()


def test_core_shutdown_interlock_is_memory_only_matching_and_keeps_release_open(
    tmp_path, monkeypatch
):
    host = create(tmp_path)
    try:
        initial = host.external_features.status()
        original_save = host._journal.save_host
        monkeypatch.setattr(host._journal, "save_host", lambda *a, **k: pytest.fail("interlock IO"))
        assert host.notify_shutdown_requested("shutdown")
        held = host.external_features.status()
        assert held.held and held.settlement_open == initial.settlement_open
        assert held.shutdown_serial > initial.shutdown_serial
        assert host.recovery_snapshot()["core_shutdown_operation_id"] == "shutdown"
        with pytest.raises(TradingBlocked):
            host.submit_recovery("blocked", request(host))
        with pytest.raises(ValidationError):
            host.notify_shutdown_requested("different", active=False)
        assert host.notify_shutdown_requested("shutdown", active=False)
        canceled = host.external_features.status()
        assert canceled.held and canceled.settlement_open
        assert canceled.shutdown_serial > held.shutdown_serial
        assert host.recovery_snapshot()["core_shutdown_operation_id"] is None
        assert "CORE_SHUTDOWN_REQUESTED" not in host.recovery_snapshot()["blockers"]
        assert host.recovery_snapshot()["state"] == "MANUAL_REQUIRED"
        monkeypatch.setattr(host._journal, "save_host", original_save)
    finally:
        host.close()


def test_immutable_lifecycle_proofs_survive_later_replacement(tmp_path):
    host = create(tmp_path)
    try:
        first_sha = host._host_document["proof_artifact_sha256"]
        first_path = tmp_path / "state" / "causal-recovery-proofs" / (first_sha + ".json")
        original = first_path.read_bytes()
        host.submit_recovery("first", request(host))
        assert operation(host, "first")["state"] == "succeeded"
        first_receipt_sha = host._host_document["proof_artifact_sha256"]
        first_receipt = first_path.with_name(first_receipt_sha + ".json").read_bytes()
        host.submit_recovery("second", request(host))
        assert operation(host, "second")["state"] == "succeeded"
        assert first_path.read_bytes() == original
        assert first_path.with_name(first_receipt_sha + ".json").read_bytes() == first_receipt
        assert host._host_document["proof_record_count"] >= 10
    finally:
        host.close()


def test_proof_reservation_precedes_artifact_and_failed_write_blocks_cold_factory(
    tmp_path, monkeypatch
):
    host = create(tmp_path)

    def fail_write(self, record):
        raise StorageError("injected immutable artifact write uncertainty")

    monkeypatch.setattr(RecoveryProofArchive, "write", fail_write)
    host.submit_recovery("artifact", request(host))
    assert operation(host, "artifact")["state"] == "unknown"
    assert len(host._retained_candidates) == 1
    with pytest.raises(StorageError):
        host.close()
    monkeypatch.undo()
    factories = []
    restarted = create(tmp_path, actor_factory=lambda *a, **k: factories.append((a, k)))
    assert factories == []
    assert restarted._host_document["phase"] == "proof_pending"
    assert restarted.recovery_snapshot()["state"] == "HELD"
    with pytest.raises(StorageError):
        restarted.close()


def test_never_started_close_has_no_write_or_physical_resource_claim(tmp_path):
    host = CausalInferenceHost(tmp_path, core_epoch="core", training_job_provider=lambda _: None)
    result = host.close()
    assert result["closed"] and result["durable"]
    assert result["evidence_basis"] == "never_started_no_acquisitions"
    assert result["durable_recording_required"] is False
    assert result["physical_cleanup_required"] is False
    assert host.thread is None and host._journal is None and host._retained_candidates == []
    assert not (tmp_path / "state").exists()
    assert host.close() == result
    with pytest.raises(ValidationError):
        host.start()
    uncertain = CausalInferenceHost(
        tmp_path, core_epoch="core", training_job_provider=lambda _: None
    )
    uncertain._journal_unknown = True
    with pytest.raises(StorageError):
        uncertain.close()
    assert not uncertain.closed and uncertain._journal_unknown


def test_actual_actor_complete_codec_enables_only_current_explicit_reset(tmp_path, monkeypatch):
    context = GenerationContext("core", 0, "original", "host")
    actor, _ = create_actor(tmp_path, monkeypatch, epoch="core", generation_context=context)
    host = CausalInferenceHost(tmp_path, core_epoch="core", training_job_provider=lambda _: None)
    facade_binding = GenerationBinding("core", 0, "original", actor.external_features)
    binding = SimpleNamespace(context=context, actor=actor, facade_binding=facade_binding)
    host._current = binding
    host.external_features.bind_initial(facade_binding, held=True)
    try:
        deployment = activate(actor)
        host._controls["activate"] = (binding, "activate")
        assert host.status("activate")["state"] == "complete"
        assert host.external_features.status().held
        result = reset(actor, deployment)
        assert result["state"] == "complete" and result["result"]["stream_epoch"]
        host._controls["reset"] = (binding, "reset_stream")
        assert host.status("reset")["state"] == "complete"
        assert not host.external_features.status().held
        assert host.recovery_snapshot()["state"] == "ACTIVE"
    finally:
        actor.close()


def test_actual_retired_journal_pending_prediction_and_stream_are_bound(tmp_path, monkeypatch):
    actor, state = create_actor(tmp_path, monkeypatch)
    try:
        deployment = activate(actor)
        reset(actor, deployment)
        state["auto"] = False
        predict(actor, deployment)
        wait_for(lambda: state["workers"][-1].requests)
    finally:
        actor.close()
    retirement = actor.retirement_snapshot()
    head = verified_journal_head(tmp_path, retirement.last_journal_ack)
    pending = pending_unknown_evidence(head)
    prediction = next(row for row in pending["operations"] if row["id"] == "predict")
    assert prediction["state"] == "unknown" and prediction["request_id"]
    assert prediction["request_digest"] == head["operations"]["predict"]["request_digest"]
    assert pending["streams"] == [
        {
            "deployment_id": deployment,
            "stream": "stream",
            "stream_epoch": head["deployments"][deployment]["streams"]["stream"]["stream_epoch"],
            "instrument": "binance:usdm:BTCUSDT",
            "pending_operation_id": None,
            "uncertain": True,
        }
    ]
    assert digest(pending) != digest({"operations": [], "streams": []})


@pytest.mark.parametrize("lost_phase", ["owner_ack", "terminal_publication", "consume_publication"])
def test_actual_terminal_ack_loss_reconciles_same_effect_once(tmp_path, monkeypatch, lost_phase):
    from test_causal_feature_delivery import ready

    host = create(tmp_path)
    original = host._current
    original.actor.close()
    service, state, provider, _, candle, cutoff, snapshot = ready(
        tmp_path,
        monkeypatch,
        epoch="core",
        actor_token=original.context.actor_token,
        generation_context=original.context,
    )
    facade_binding = GenerationBinding("core", 0, original.context.actor_token, provider)
    attached = replace(original, actor=service, facade_binding=facade_binding)
    host._current = attached
    host._retained_candidates.append(attached)
    host.external_features._binding = facade_binding
    host.external_features._bindings = provider.list_bindings()
    host.external_features._clock_ms = lambda: state["clock"][0]
    host.external_features.enable_current(
        0, original.context.actor_token, expected_control_revision=0
    )
    release, entered = threading.Event(), threading.Event()
    normal_timeout, effects, errors = host.ack_timeout_s, [], []
    original_dispatch = provider.dispatch_feature_mutation

    def dispatch(ref, arguments):
        effects.append(ref)
        return original_dispatch(ref, arguments)

    monkeypatch.setattr(provider, "dispatch_feature_mutation", dispatch)
    if lost_phase == "owner_ack":
        ack_owner = service.ack_feature_settlement
        lost = []

        def lose_owner_ack(*args, **kwargs):
            result = ack_owner(*args, **kwargs)
            if not lost:
                lost.append(True)
                raise StorageError("synthetic lost cache-retirement RPC acknowledgement")
            return result

        monkeypatch.setattr(service, "ack_feature_settlement", lose_owner_ack)
    else:
        publish = host._publish_ticket
        delayed_kind = "terminal" if lost_phase == "terminal_publication" else "consume"

        def delay_publication(ticket):
            if ticket.kind == delayed_kind and not entered.is_set():
                entered.set()
                assert release.wait(3)
            publish(ticket)

        monkeypatch.setattr(host, "_publish_ticket", delay_publication)
        request_call = host._request

        def bounded_wait(kind, *args):
            if kind == delayed_kind:
                host.ack_timeout_s = 0.05
            return request_call(kind, *args)

        monkeypatch.setattr(host, "_request", bounded_wait)

    def pin():
        try:
            host.external_features.pin("consumer", FRAME, snapshot, candle, decision_at_ms=cutoff)
        except FeatureMutationUnknown as error:
            errors.append(error)

    worker = threading.Thread(target=pin)
    try:
        worker.start()
        if lost_phase != "owner_ack":
            assert entered.wait(2)
        worker.join(2)
        assert not worker.is_alive() and errors and errors[0].recording_unknown is False
        assert len(effects) == 1
        release.set()
        host.ack_timeout_s = normal_timeout
        if lost_phase != "owner_ack":
            monkeypatch.setattr(host, "_request", request_call)
        ref = errors[0].ref
        assert host.reconcile_mutation(ref)
        assert host.external_features.status().hot_mutations == 0 and len(effects) == 1
        outcome = service.read_feature_settlement(ref, host_token=original.context.host_token)
        # The actual result is the immutable pin from the original effect.
        pin_result = (
            next(iter(host.external_features._pins.values()))[1]
            if outcome is None
            else ExternalFeaturePin.parse(decoded(outcome.result_json)["pin"])
        )
        host.external_features.release(pin_result, consumer_receipt_sha256=RECEIPT)
    finally:
        release.set()
        host.ack_timeout_s = normal_timeout
        worker.join(3)
        # Fake source/worker ports intentionally cannot grant physical release.
        with pytest.raises(StorageError):
            host.close()
        assert not service.thread.is_alive()
