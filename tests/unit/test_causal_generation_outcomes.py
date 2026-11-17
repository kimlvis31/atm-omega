"""Original actor effects with real temporary file stores and explicit fake model IO.

These tests grant no source, model-child, OS-close or replacement authority.
The existing actor fixture supplies the explicitly synthetic worker/source only.
"""

import threading
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_causal_feature_delivery import ready
from test_causal_feature_delivery_store import pin_for, recorded
from test_causal_inference_service import command, create, terminal, wait

from omega.domain.errors import StorageError, ValidationError
from omega.domain.external_features import (
    ExternalFeaturePin,
    exact_decimal_text,
    semantic_candle_sha256,
)
from omega.domain.inference import decoded, digest, encoded
from omega.runtime import causal_nn
from omega.runtime.causal_feature_delivery_store import CausalFeatureDeliveryStore
from omega.runtime.causal_generation_outcomes import GenerationContext
from omega.runtime.causal_inference_service import CausalInferenceService
from omega.runtime.causal_recovery_records import (
    FeatureMutationRef,
    FeatureMutationRejected,
    FeatureMutationUnknown,
    RecoveryJournal,
)


def context(journal=None):
    return GenerationContext(
        "core1",
        7,
        "reserved-actor",
        "private-host",
        None if journal is None else journal.mutation_ack_authority(),
    )


def pin_arguments(snapshot, candle, cutoff, *, consumer="consumer", frame="a" * 64):
    arguments = {
        "consumer_id": consumer,
        "frame_ref": frame,
        "snapshot": snapshot.document(),
        "decision_at_ms": cutoff,
        "candle": {
            "instrument": candle.instrument.key,
            "open_time_ms": candle.open_time_ms,
            "close_time_ms": candle.close_time_ms,
            **{
                name: exact_decimal_text(getattr(candle, name))
                for name in ("open", "high", "low", "close", "volume")
            },
        },
        "candle_sha256": semantic_candle_sha256(candle),
    }
    pin_id = ExternalFeaturePin(
        snapshot.binding.provider_id,
        consumer,
        frame,
        snapshot.binding.binding_sha256,
        snapshot.snapshot_sha256,
        cutoff,
    ).pin_id
    return pin_id, encoded(arguments)


def ref_for(binding, kind, pin_id, arguments_json, token="original-mutation"):
    return FeatureMutationRef(
        binding.core_epoch,
        binding.generation,
        binding.actor_token,
        token,
        kind,
        pin_id,
        digest(decoded(arguments_json)),
    )


def close_actor(service):
    try:
        service.close()
    except StorageError:
        assert service.closed


def lose_one_control_response(monkeypatch, service, operation):
    enqueue, armed = service._enqueue_feature_command, [True]

    class LostResponseEvent:
        def __init__(self, original):
            self.original = original

        def set(self):
            self.original.set()

        def wait(self, timeout):
            assert self.original.wait(timeout), "owner did not finish the injected lost response"
            return False

    def delayed_response(ticket):
        if armed[0] and ticket["kind"] == "generation_control" and ticket["operation"] == operation:
            armed[0] = False
            ticket["event"] = LostResponseEvent(ticket["event"])
        return enqueue(ticket)

    monkeypatch.setattr(service, "_enqueue_feature_command", delayed_response)


@pytest.mark.parametrize("lost_operation", ["ack", "confirm"])
def test_lost_ack_or_confirmation_response_retains_exact_idempotent_original_outcome(
    tmp_path, monkeypatch, lost_operation
):
    journal = RecoveryJournal(tmp_path)
    binding = context(journal)
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    actual_pin, effects = provider.store.pin, []

    def counted(pin):
        effects.append(pin)
        return actual_pin(pin)

    provider.store.pin = counted
    try:
        pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
        ref = ref_for(binding, "pin", pin_id, arguments)
        journal.prepare_mutation(ref, arguments)
        pin = provider.dispatch_feature_mutation(ref, arguments)
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        ack = journal.finish_mutation(ref, outcome.result_json)
        lose_one_control_response(monkeypatch, service, lost_operation)
        if lost_operation == "ack":
            with pytest.raises(StorageError, match="unconfirmed"):
                service.ack_feature_settlement(ref, ack, host_token=binding.host_token)
            original = service.read_feature_settlement(ref, host_token=binding.host_token)
            assert original.ref == ref and original.result_json == outcome.result_json
            assert ref.mutation_token not in provider._generation_outcomes
            assert not provider._generation_acknowledged[ref.mutation_token].confirmed
            final = service.seal_feature_settlement(host_token=binding.host_token)
            assert len(final.outcomes) == 1 and final.outcomes[0].ref == ref
        else:
            assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token)
            with pytest.raises(StorageError, match="unconfirmed"):
                service.confirm_feature_settlement_ack(ref, ack, host_token=binding.host_token)
            assert provider._generation_acknowledged[ref.mutation_token].confirmed
        with pytest.raises(ValidationError):
            service.ack_feature_settlement(ref, replace(ack), host_token=binding.host_token)
        with pytest.raises(ValidationError):
            service.confirm_feature_settlement_ack(ref, replace(ack), host_token=binding.host_token)
        with pytest.raises(ValidationError):
            service.ack_feature_settlement(
                replace(ref, arguments_sha256="f" * 64), ack, host_token=binding.host_token
            )
        assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token) is True
        assert provider.dispatch_feature_mutation(ref, arguments) == pin
        assert len(effects) == 1
        assert service.confirm_feature_settlement_ack(ref, ack, host_token=binding.host_token)
        assert journal.retire_mutation_ack(ref, ack)
        assert service.read_feature_settlement(ref, host_token=binding.host_token) is None
        # A prior original positive remains a cached fact after issuer retirement.
        assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token) is True
        assert service.confirm_feature_settlement_ack(ref, ack, host_token=binding.host_token)
    finally:
        close_actor(service)
        journal.close()


def test_ack_tombstones_are_bounded_never_evict_unconfirmed_and_keep_capacity_charged(
    tmp_path, monkeypatch
):
    import omega.runtime.causal_feature_delivery as delivery_module

    assert delivery_module.MAX_HOT_MUTATIONS == 128
    assert delivery_module.RELEASE_RESERVED == 32
    # Scale the same finite policy for this cache regression; every ACK below is
    # still issued by a real temporary RecoveryJournal/FileInferenceStateStore.
    monkeypatch.setattr(delivery_module, "MAX_HOT_MUTATIONS", 3)
    monkeypatch.setattr(delivery_module, "RELEASE_RESERVED", 1)
    journal = RecoveryJournal(tmp_path)
    binding = context(journal)
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    try:
        pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
        ref = ref_for(binding, "pin", pin_id, arguments)
        journal.prepare_mutation(ref, arguments)
        pin = provider.dispatch_feature_mutation(ref, arguments)
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        ack = journal.finish_mutation(ref, outcome.result_json)
        assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token)
        assert service.confirm_feature_settlement_ack(ref, ack, host_token=binding.host_token)
        assert journal.retire_mutation_ack(ref, ack)
        oldest = ref.mutation_token
        pending = []
        arguments = encoded({"pin": pin.document(), "consumer_receipt_sha256": "b" * 64})
        for number in range(3):
            ref = ref_for(binding, "release", pin.pin_id, arguments, f"release{number}")
            journal.prepare_mutation(ref, arguments)
            assert provider.dispatch_feature_mutation(ref, arguments) is None
            outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
            ack = journal.finish_mutation(ref, outcome.result_json)
            assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token)
            pending.append((ref, ack))
        assert oldest not in provider._generation_acknowledged
        assert len(provider._generation_acknowledged) == 3
        assert all(not row.confirmed for row in provider._generation_acknowledged.values())
        with pytest.raises(FeatureMutationRejected):
            provider.dispatch_feature_mutation(
                ref_for(binding, "release", pin.pin_id, arguments, "over-capacity"), arguments
            )
        for ref, ack in pending:
            assert service.read_feature_settlement(ref, host_token=binding.host_token).ref == ref
            assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token)
        ref, ack = pending[0]
        assert service.confirm_feature_settlement_ack(ref, ack, host_token=binding.host_token)
        assert journal.retire_mutation_ack(ref, ack)
        extra = ref_for(binding, "release", pin.pin_id, arguments, "after-confirm")
        journal.prepare_mutation(extra, arguments)
        assert provider.dispatch_feature_mutation(extra, arguments) is None
        outcome = service.read_feature_settlement(extra, host_token=binding.host_token)
        extra_ack = journal.finish_mutation(extra, outcome.result_json)
        assert service.ack_feature_settlement(extra, extra_ack, host_token=binding.host_token)
        assert len(provider._generation_acknowledged) == 3
        assert ref.mutation_token not in provider._generation_acknowledged
        assert all(row.outcome.ref != ref for row in provider._generation_acknowledged.values())
    finally:
        close_actor(service)
        journal.close()


def test_contains_pin_is_io0_settlement_membership_after_expiry_and_pause_until_release(
    tmp_path, monkeypatch
):
    binding = context()
    service, state, provider, actual_binding, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    try:
        pin = provider.pin("consumer", "a" * 64, snapshot, candle, decision_at_ms=cutoff)
        assert provider.contains_pin(pin) is True
        assert provider.authorize(pin, snapshot, candle, decision_at_ms=cutoff)
        state["clock"][0] = snapshot.valid_until_ms
        assert not provider.authorize(pin, snapshot, candle, decision_at_ms=cutoff)
        assert provider.contains_pin(pin) is True
        command(service, "pause", "pause", deployment_id=actual_binding.instance_id)
        assert terminal(service, "pause")["state"] == "complete"
        assert not provider.authorize(pin, snapshot, candle, decision_at_ms=cutoff)
        assert provider.contains_pin(replace(pin, created_at_ms=pin.created_at_ms + 1)) is False

        def forbidden(*_, **__):
            raise AssertionError("cached contains_pin attempted filesystem IO")

        with monkeypatch.context() as cached:
            cached.setattr(Path, "open", forbidden)
            for _ in range(100):
                assert provider.contains_pin(pin) is True
                assert not provider.authorize(pin, snapshot, candle, decision_at_ms=cutoff)
        provider.release(pin, consumer_receipt_sha256="b" * 64)
        assert provider.contains_pin(pin) is False
    finally:
        close_actor(service)


def test_actual_reissued_terminal_ack_requires_original_issuer_and_same_terminal_record(
    tmp_path, monkeypatch
):
    journal = RecoveryJournal(tmp_path)
    binding = context(journal)
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    try:
        pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
        ref = ref_for(binding, "pin", pin_id, arguments)
        journal.prepare_mutation(ref, arguments)
        pin = provider.dispatch_feature_mutation(ref, arguments)
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        original_ack = journal.finish_mutation(ref, outcome.result_json)
        assert service.ack_feature_settlement(ref, original_ack, host_token=binding.host_token)
        assert service.confirm_feature_settlement_ack(
            ref, original_ack, host_token=binding.host_token
        )
        assert journal.retire_mutation_ack(ref, original_ack)
        reissued = journal.finish_mutation(ref, outcome.result_json)
        assert reissued is not original_ack
        assert reissued.record_sha256 == original_ack.record_sha256
        assert binding.verifies_terminal_ack(ref, reissued, outcome.result_json)
        with pytest.raises(ValidationError):
            service.ack_feature_settlement(ref, replace(reissued), host_token=binding.host_token)
        assert service.ack_feature_settlement(ref, reissued, host_token=binding.host_token)
        assert provider._generation_acknowledged[ref.mutation_token].ack is reissued
        assert not provider._generation_acknowledged[ref.mutation_token].confirmed
        assert provider.dispatch_feature_mutation(ref, arguments) == pin
        assert service.confirm_feature_settlement_ack(ref, reissued, host_token=binding.host_token)
        assert journal.retire_mutation_ack(ref, reissued)
        assert len(provider._generation_acknowledged) == 1
    finally:
        close_actor(service)
        journal.close()


def test_dead_actor_keeps_original_positive_ack_and_unconfirmed_outcome_without_new_confirmation(
    tmp_path, monkeypatch
):
    journal = RecoveryJournal(tmp_path)
    binding = context(journal)
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    try:
        pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
        ref = ref_for(binding, "pin", pin_id, arguments)
        journal.prepare_mutation(ref, arguments)
        provider.dispatch_feature_mutation(ref, arguments)
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        ack = journal.finish_mutation(ref, outcome.result_json)
        assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token)
        close_actor(service)
        frozen = service.read_feature_settlement(ref, host_token=binding.host_token)
        assert frozen.ref == ref and frozen.result_json == outcome.result_json
        assert service.generation_settlement_snapshot().outcomes == (frozen,)
        assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token) is True
        with pytest.raises(ValidationError):
            service.ack_feature_settlement(ref, replace(ack), host_token=binding.host_token)
        with pytest.raises(StorageError):
            service.confirm_feature_settlement_ack(ref, ack, host_token=binding.host_token)
        assert not provider._generation_acknowledged[ref.mutation_token].confirmed
    finally:
        close_actor(service)
        journal.close()


def test_actual_pin_release_outcomes_exact_terminal_handshake_and_final_membership(
    tmp_path, monkeypatch
):
    journal = RecoveryJournal(tmp_path)
    binding = context(journal)
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    try:
        pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
        ref = ref_for(binding, "pin", pin_id, arguments)
        assert journal.prepare_mutation(ref, arguments).state == "prepared"
        pin = provider.dispatch_feature_mutation(ref, arguments)
        assert type(pin) is ExternalFeaturePin
        assert provider.authorize(pin, snapshot, candle, decision_at_ms=cutoff)
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        assert outcome.ref == ref and outcome.arguments_json == arguments
        assert outcome.phase == "complete" and not outcome.recording_unknown
        assert outcome.current_write_confirmed and not outcome.capture_failed
        assert decoded(outcome.result_json) == {"pin": pin.document()}
        assert outcome.head_revision == provider.store.revision
        assert outcome.head_sha256 == digest(provider.store.data)
        assert outcome.last_exact_ack is provider.store.store._last_ack
        ack = journal.finish_mutation(ref, outcome.result_json)
        with pytest.raises(ValidationError):
            service.ack_feature_settlement(ref, replace(ack), host_token=binding.host_token)
        with pytest.raises(ValidationError):
            service.ack_feature_settlement(ref, ack, host_token="foreign-host")
        assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token) is True
        assert service.confirm_feature_settlement_ack(ref, ack, host_token=binding.host_token)
        assert journal.retire_mutation_ack(ref, ack) is True
        assert service.read_feature_settlement(ref, host_token=binding.host_token) is None

        arguments = encoded({"pin": pin.document(), "consumer_receipt_sha256": "b" * 64})
        ref = ref_for(binding, "release", pin.pin_id, arguments, "release-original")
        journal.prepare_mutation(ref, arguments)
        assert provider.dispatch_feature_mutation(ref, arguments) is None
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        assert decoded(outcome.result_json) == {
            "released": True,
            "pin_sha256": digest(pin.document()),
            "consumer_receipt_sha256": "b" * 64,
        }
        ack = journal.finish_mutation(ref, outcome.result_json)
        assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token)
        assert service.confirm_feature_settlement_ack(ref, ack, host_token=binding.host_token)
        assert journal.retire_mutation_ack(ref, ack)
        final = service.seal_feature_settlement(host_token=binding.host_token)
        assert final.sealed and final.pins == final.reservations == final.outcomes == ()
        assert final.current_write_confirmed and not final.recording_unknown
        assert final.store_scope_token == provider.store.store._lease_scope.token
        assert final.store_binding_token == provider.store.store._lease_binding.token
    finally:
        close_actor(service)
        journal.close()
    assert service.generation_settlement_snapshot().sealed
    assert service.generation_settlement_snapshot().pins == ()


def test_wait_timeout_retains_original_ref_then_late_actual_effect_without_replay(
    tmp_path, monkeypatch
):
    binding = context()
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    normal_timeout = service.ack_timeout_s
    # The deliberate50ms deadline targets the pending mutation, not unrelated
    # filesystem startup/activation/reset performed by ready().
    service.ack_timeout_s = 0.05
    entered, release = threading.Event(), threading.Event()
    actual, calls = provider.store.pin, []

    def delayed(pin):
        calls.append(pin)
        entered.set()
        assert release.wait(3)
        return actual(pin)

    provider.store.pin = delayed
    pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
    ref = ref_for(binding, "pin", pin_id, arguments)
    try:
        with pytest.raises(FeatureMutationUnknown) as failure:
            provider.dispatch_feature_mutation(ref, arguments)
        assert entered.is_set() and failure.value.ref is ref
        assert failure.value.phase == "dispatch" and failure.value.recording_unknown is False
        changed = decoded(arguments)
        changed["decision_at_ms"] += 1
        changed_json = encoded(changed)
        with pytest.raises(ValidationError):
            provider.dispatch_feature_mutation(
                replace(ref, arguments_sha256=digest(changed)), changed_json
            )
        release.set()
        wait(lambda: provider._generation_tickets[ref.mutation_token]["event"].is_set())
        pin = provider.dispatch_feature_mutation(ref, arguments)
        assert pin.pin_id == pin_id and len(calls) == 1
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        assert outcome.phase == "complete" and not outcome.recording_unknown
        assert decoded(outcome.result_json) == {"pin": pin.document()}
    finally:
        release.set()
        service.ack_timeout_s = normal_timeout
        close_actor(service)


def test_actual_immutable_pin_write_failure_is_distinct_from_rpc_wait_timeout(
    tmp_path, monkeypatch
):
    binding = context()
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    actual_put = provider.store._put

    def fail_pin(path, document):
        if path.parent.name == "pins":
            raise OSError("explicit immutable pin write fault")
        return actual_put(path, document)

    provider.store._put = fail_pin
    pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
    ref = ref_for(binding, "pin", pin_id, arguments)
    try:
        with pytest.raises(FeatureMutationUnknown) as failure:
            provider.dispatch_feature_mutation(ref, arguments)
        assert failure.value.recording_unknown is True
        wait(lambda: service.closed)
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        assert outcome.phase == "unknown" and outcome.recording_unknown
        final = service.generation_settlement_snapshot()
        assert final.sealed and final.recording_unknown
        assert final.reservations == (("pin:" + pin_id, final.reservations[0][1]),)
        assert final.pins == () and not final.current_write_confirmed
    finally:
        close_actor(service)


def test_actual_file_store_commit_fault_keeps_original_failed_and_pending_sticky(
    tmp_path, monkeypatch
):
    binding = context()
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    actual = causal_nn.atomic_json
    raw = provider.store.store

    def unavailable(path, document):
        if Path(path) == raw.path:
            raise OSError("explicit actual state commit fault")
        return actual(path, document)

    monkeypatch.setattr(causal_nn, "atomic_json", unavailable)
    pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
    ref = ref_for(binding, "pin", pin_id, arguments)
    try:
        with pytest.raises(FeatureMutationUnknown) as failure:
            provider.dispatch_feature_mutation(ref, arguments)
        assert failure.value.recording_unknown
        wait(lambda: service.closed)
        assert raw.failed is True
        assert provider.store._settlement_write_pending
        assert provider.store._accounting_confirmed is False
        final = service.generation_settlement_snapshot()
        assert final.recording_unknown and not final.current_write_confirmed
    finally:
        close_actor(service)


@pytest.mark.parametrize("mode", ["missing", "duck"])
def test_actual_pin_bytes_with_missing_or_duck_current_ack_stay_recording_unknown(
    tmp_path, monkeypatch, mode
):
    binding = context()
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    actual_save = provider.store.store.save_snapshot

    def uncertain(snapshot_json, **kwargs):
        ack = actual_save(snapshot_json, **kwargs)
        if not decoded(snapshot_json)["pins"]:
            return ack
        return (
            None
            if mode == "missing"
            else SimpleNamespace(
                durable=ack.durable, revision=ack.revision, snapshot_sha256=ack.snapshot_sha256
            )
        )

    provider.store.store.save_snapshot = uncertain
    pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
    ref = ref_for(binding, "pin", pin_id, arguments)
    try:
        with pytest.raises(FeatureMutationUnknown) as failure:
            provider.dispatch_feature_mutation(ref, arguments)
        assert failure.value.recording_unknown
        wait(lambda: service.closed)
        final = service.generation_settlement_snapshot()
        assert final.sealed and final.recording_unknown
        # Actual bytes may exist. A missing return never proves write failure.
        assert len(final.pins) == 1 and final.pins[0].pin_id == pin_id
        assert not final.current_write_confirmed
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        assert outcome.phase == "unknown" and outcome.recording_unknown
    finally:
        close_actor(service)


def test_final_original_actor_marks_queued_mutation_known_not_dispatched(tmp_path, monkeypatch):
    binding = context()
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding, ack_timeout_s=1
    )
    entered, release = threading.Event(), threading.Event()
    actual_pin = provider.store.pin
    failures, legacy_results = [], []

    def delayed(pin):
        entered.set()
        assert release.wait(3)
        return actual_pin(pin)

    provider.store.pin = delayed

    def legacy():
        legacy_results.append(
            provider.pin("legacy", "a" * 64, snapshot, candle, decision_at_ms=cutoff)
        )

    old_waiter = threading.Thread(target=legacy)
    old_waiter.start()
    assert entered.wait(1)
    pin_id, arguments = pin_arguments(snapshot, candle, cutoff, consumer="new-queued")
    ref = ref_for(binding, "pin", pin_id, arguments)

    def queued():
        try:
            provider.dispatch_feature_mutation(ref, arguments)
        except Exception as error:
            failures.append(error)

    new_waiter = threading.Thread(target=queued)
    new_waiter.start()
    closer = threading.Thread(target=lambda: close_actor(service))
    try:
        wait(lambda: len(service._feature_commands) == 1)
        closer.start()
        wait(lambda: service.closing)
        release.set()
        for thread in (old_waiter, new_waiter, closer):
            thread.join(timeout=3)
            assert not thread.is_alive()
        assert len(failures) == 1 and type(failures[0]) is FeatureMutationRejected
        final = service.generation_settlement_snapshot()
        assert final.sealed and final.pins == tuple(legacy_results)
        assert len(final.outcomes) == 1 and final.outcomes[0].phase == "rejected"
        assert final.outcomes[0].ref == ref and not final.outcomes[0].recording_unknown
    finally:
        release.set()
        close_actor(service)


@pytest.mark.parametrize("primary", [KeyboardInterrupt, SystemExit])
def test_baseexception_on_entered_effect_never_wakes_as_none_pin_success(
    tmp_path, monkeypatch, primary
):
    binding = context()
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )

    def interrupted(_):
        raise primary("explicit effect interruption")

    provider.store.pin = interrupted
    pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
    ref = ref_for(binding, "pin", pin_id, arguments)
    try:
        with pytest.raises(FeatureMutationUnknown) as failure:
            provider.dispatch_feature_mutation(ref, arguments)
        assert failure.value.recording_unknown is True
        wait(lambda: service.closed)
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        assert outcome.phase == "unknown" and outcome.result_json is None
        assert outcome.recording_unknown
        assert service.error["code"] == "CAUSAL_INFERENCE_UNAVAILABLE"
        assert service.generation_settlement_snapshot().sealed
    finally:
        close_actor(service)


def test_positive_pre_effect_validation_rejection_has_exact_no_dispatch_terminal(
    tmp_path, monkeypatch
):
    journal = RecoveryJournal(tmp_path)
    binding = context(journal)
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    try:
        _, arguments = pin_arguments(snapshot, candle, cutoff)
        ref = ref_for(binding, "pin", "f" * 64, arguments)
        journal.prepare_mutation(ref, arguments)
        before = provider.store.revision
        with pytest.raises(FeatureMutationRejected):
            provider.dispatch_feature_mutation(ref, arguments)
        outcome = service.read_feature_settlement(ref, host_token=binding.host_token)
        assert outcome.phase == "rejected" and not outcome.recording_unknown
        assert provider.store.revision == before and outcome.current_write_confirmed
        ack = journal.finish_mutation(ref, encoded({"not_dispatched": True}))
        assert service.ack_feature_settlement(ref, ack, host_token=binding.host_token)
        assert service.confirm_feature_settlement_ack(ref, ack, host_token=binding.host_token)
        assert journal.retire_mutation_ack(ref, ack)
    finally:
        close_actor(service)
        journal.close()


def test_exact_head_then_duck_save_keeps_legacy_success_but_withholds_new_coverage(tmp_path):
    store = CausalFeatureDeliveryStore(tmp_path)
    try:
        first = store._settlement_last_exact_ack
        revision = store.revision

        def duck(snapshot_json, **_):
            document = decoded(snapshot_json)
            return SimpleNamespace(
                durable=True, revision=document["revision"], snapshot_sha256=digest(document)
            )

        store.store.save_snapshot = duck
        assert store._save() is None
        assert store.revision == revision + 1 and store._accounting_confirmed is True
        assert store._settlement_last_exact_ack is first
        assert store._settlement_recording_unknown
        assert not store._settlement_current_write_confirmed
        assert not store._settlement_write_pending
    finally:
        store.close()


@pytest.mark.parametrize("primary", [KeyboardInterrupt, SystemExit])
def test_actual_commit_then_observer_baseexception_preserves_primary_and_pending_unknown(
    tmp_path, primary
):
    store = CausalFeatureDeliveryStore(tmp_path)
    first = store._settlement_last_exact_ack

    def observer(_):
        raise primary("original observer primary")

    store._on_change = observer
    try:
        with pytest.raises(primary, match="original observer primary"):
            store._save()
        assert store.store._last_ack.revision == first.revision + 1
        assert store._settlement_write_pending and store._settlement_recording_unknown
        assert not store._settlement_current_write_confirmed
        assert store._accounting_confirmed is True
    finally:
        store.close()


def test_completed_original_outcomes_keep_96_pin_slots_and_32_release_reserve(
    tmp_path, monkeypatch
):
    binding = context()
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    pins = []
    try:
        for number in range(96):
            pin_id, arguments = pin_arguments(
                snapshot, candle, cutoff, consumer=f"consumer{number}"
            )
            ref = ref_for(binding, "pin", pin_id, arguments, f"pin{number}")
            pins.append(provider.dispatch_feature_mutation(ref, arguments))
        pin_id, arguments = pin_arguments(snapshot, candle, cutoff, consumer="extra")
        with pytest.raises(FeatureMutationRejected):
            provider.dispatch_feature_mutation(
                ref_for(binding, "pin", pin_id, arguments), arguments
            )
        for number, pin in enumerate(pins[:32]):
            arguments = encoded({"pin": pin.document(), "consumer_receipt_sha256": "b" * 64})
            ref = ref_for(binding, "release", pin.pin_id, arguments, f"release{number}")
            assert provider.dispatch_feature_mutation(ref, arguments) is None
        assert len(provider._generation_outcomes) == 128
        pin = pins[32]
        arguments = encoded({"pin": pin.document(), "consumer_receipt_sha256": "b" * 64})
        with pytest.raises(FeatureMutationRejected):
            provider.dispatch_feature_mutation(
                ref_for(binding, "release", pin.pin_id, arguments, "release-extra"), arguments
            )
        assert len(provider._generation_outcomes) == 128
        final = service.seal_feature_settlement(host_token=binding.host_token)
        assert len(final.outcomes) == 128 and len(final.pins) == 64
        assert not final.capture_failed and not final.recording_unknown
    finally:
        close_actor(service)


def test_sealed_original_export_is_cached_io0_and_blocks_fresh_dispatch(tmp_path, monkeypatch):
    binding = context()
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
    ref = ref_for(binding, "pin", pin_id, arguments)
    pin = provider.dispatch_feature_mutation(ref, arguments)
    first = service.seal_feature_settlement(host_token=binding.host_token)
    assert first.pins == (pin,) and first.sealed
    other_id, other_args = pin_arguments(snapshot, candle, cutoff, consumer="other")
    with pytest.raises(FeatureMutationRejected):
        provider.dispatch_feature_mutation(
            ref_for(binding, "pin", other_id, other_args, "other"), other_args
        )
    close_actor(service)
    final = service.generation_settlement_snapshot()

    def forbidden(*_, **__):
        raise AssertionError("cached settlement inspection attempted filesystem IO")

    monkeypatch.setattr(Path, "open", forbidden)
    for _ in range(100):
        assert service.generation_settlement_snapshot() is final
        assert provider.generation_settlement_snapshot() is final
        assert service.read_feature_settlement(ref, host_token=binding.host_token).ref == ref
    assert final.sealed and final.pins == (pin,) and not final.capture_failed


def test_foreign_generation_and_store_substitution_are_fenced_before_original_read_io(
    tmp_path, monkeypatch
):
    binding = context()
    service, _, provider, _, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, generation_context=binding
    )
    pin_id, arguments = pin_arguments(snapshot, candle, cutoff)
    ref = ref_for(binding, "pin", pin_id, arguments)
    actual_store = provider.store
    try:
        with pytest.raises(ValidationError):
            provider.dispatch_feature_mutation(replace(ref, actor_token="foreign"), arguments)
        provider.store = SimpleNamespace()
        with pytest.raises(FeatureMutationUnknown) as failure:
            provider.dispatch_feature_mutation(ref, arguments)
        assert failure.value.recording_unknown
        wait(lambda: service.closed)
        assert service.generation_settlement_snapshot().capture_failed
    finally:
        provider.store = actual_store
        close_actor(service)


def test_reserved_context_conflicts_fail_before_any_actor_or_file_creation(tmp_path):
    with pytest.raises(ValidationError):
        CausalInferenceService(
            tmp_path,
            core_epoch="core1",
            training_job_provider=lambda _: None,
            generation_context=context(),
            actor_token="different",
        )
    assert list(tmp_path.iterdir()) == []


def test_startup_reserves_original_token_and_caches_actual_journal_ack_without_child(
    tmp_path, monkeypatch
):
    binding = context()
    service, _ = create(tmp_path, monkeypatch, generation_context=binding)
    try:
        initial = service.retirement_snapshot()
        assert initial.actor_token == binding.actor_token == service._actor_token
        assert initial.core_epoch == binding.core_epoch
        assert initial.last_journal_ack is service.store._last_ack
        assert initial.last_journal_ack.snapshot_sha256 == digest(service._data)
        assert not initial.sealed and not initial.journal_write_pending
        assert not initial.recording_unknown and service.owner is None
        membership = service.generation_settlement_snapshot()
        assert membership is not None and not membership.sealed
        assert membership.pins == membership.reservations == membership.outcomes == ()
        assert membership.current_write_confirmed and not membership.capture_failed
        assert not membership.recording_unknown
        assert service.external_features._generation_gate_closed is False
    finally:
        close_actor(service)


def test_startup_unsealed_export_reads_actual_persisted_pin_and_reservation_membership(
    tmp_path, monkeypatch
):
    original_store, snapshot = recorded(tmp_path)
    original_pin = original_store.pin(pin_for(snapshot))
    original_store._charge("interrupted-reservation", 17)
    expected_reservations = tuple(sorted(original_store.data["reservations"].items()))
    original_store.close()
    binding = context()
    service, _ = create(tmp_path, monkeypatch, generation_context=binding)
    try:
        membership = service.generation_settlement_snapshot()
        assert membership is not None and not membership.sealed
        assert membership.pins == (original_pin,)
        assert membership.reservations == expected_reservations
        assert membership.outcomes == ()
        assert membership.head_revision == service.external_features.store.revision
        assert membership.head_sha256 == digest(service.external_features.store.data)
        assert membership.last_exact_ack is service.external_features.store.store._last_ack
        assert membership.current_write_confirmed and not membership.recording_unknown
        assert not membership.capture_failed
        assert service.external_features._generation_gate_closed is False
        assert service.owner is None and service.external_features.list_bindings() == ()
    finally:
        close_actor(service)


def test_legacy_provider_public_pin_release_and_actor_token_defaults_stay_unchanged(
    tmp_path, monkeypatch
):
    service, _, provider, _, candle, cutoff, snapshot = ready(tmp_path, monkeypatch)
    try:
        assert service._generation_context is None
        assert service._actor_token != "reserved-actor"
        pin = provider.pin("consumer", "a" * 64, snapshot, candle, decision_at_ms=cutoff)
        assert type(pin) is ExternalFeaturePin
        assert provider.release(pin, consumer_receipt_sha256="b" * 64) is None
        assert service.generation_settlement_snapshot() is None
    finally:
        service.close()


def test_no_model_actor_fault_still_seals_actual_empty_membership_before_file_close(
    tmp_path, monkeypatch
):
    binding = context()
    service, _ = create(tmp_path, monkeypatch, generation_context=binding)
    try:
        with service.condition:
            service.error = {"code": "EXPLICIT_TEST_FAULT"}
            service.ready = False
            service.condition.notify_all()
        wait(lambda: service.closed)
        final = service.generation_settlement_snapshot()
        assert final.sealed and final.pins == final.reservations == final.outcomes == ()
        assert final.current_write_confirmed and not final.recording_unknown
        assert not final.capture_failed
    finally:
        close_actor(service)
