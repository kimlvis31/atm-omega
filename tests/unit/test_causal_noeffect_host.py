"""Default temporary stores and synthetic immutable pins; no model or database."""

import threading
from dataclasses import replace

import pytest
from test_causal_historical_carry_host import RECEIPT, create, registry, request, terminal

from omega.domain.errors import StorageError, ValidationError
from omega.domain.inference import encoded
from omega.runtime.causal_recovery_records import FeatureMutationRejected, FeatureMutationUnknown


def release_pin(host, saved):
    return host.external_features.release(saved["pin"], consumer_receipt_sha256=RECEIPT)


def test_naturally_closed_provider_no_effect_preserves_pin_and_host_cleanup(tmp_path):
    host, saved = create(tmp_path, reservation=None)
    actor = host._current.actor
    actor.close()
    before = registry(tmp_path)
    try:
        with pytest.raises(FeatureMutationRejected, match="original_settlement_gate_closed"):
            release_pin(host, saved)
        assert host.external_features.pending_mutations() == ()
        assert host.external_features.status().pins == 1
        assert registry(tmp_path) == before
        assert actor.generation_settlement_snapshot().outcomes == ()
        assert not host._not_dispatched and not host._completed_terminal
    finally:
        result = host.close()
        assert result["closed"] and result["durable"] and not result["actor_alive"]
    assert registry(tmp_path) == before


def test_actual_actor_rejection_keeps_original_ack_and_confirmation(tmp_path, monkeypatch):
    host, saved = create(tmp_path, reservation=None)
    actor, provider = host._current.actor, host._current.actor.external_features
    before, acknowledgements, confirmations = registry(tmp_path), [], []
    original_ack = actor.ack_feature_settlement
    original_confirm = actor.confirm_feature_settlement_ack

    def reject(_):
        raise ValidationError("synthetic original pre-effect validation rejection")

    def ack(ref, value, **options):
        acknowledgements.append((ref, value))
        return original_ack(ref, value, **options)

    def confirm(ref, value, **options):
        confirmations.append((ref, value))
        return original_confirm(ref, value, **options)

    monkeypatch.setattr(provider, "_generation_arguments_actor", reject)
    monkeypatch.setattr(actor, "ack_feature_settlement", ack)
    monkeypatch.setattr(actor, "confirm_feature_settlement_ack", confirm)
    try:
        with pytest.raises(FeatureMutationRejected):
            release_pin(host, saved)
        assert len(acknowledgements) == len(confirmations) == 1
        assert acknowledgements[0][0] == confirmations[0][0]
        assert acknowledgements[0][1] is confirmations[0][1]
        assert host.external_features.pending_mutations() == ()
        assert registry(tmp_path) == before
    finally:
        assert host.close()["durable"]


@pytest.mark.parametrize("lost_kind", ["not_dispatched", "consume"])
def test_lost_no_effect_response_uses_same_ack_without_dispatch_replay(
    tmp_path, monkeypatch, lost_kind
):
    host, saved = create(tmp_path, reservation=None)
    actor = host._current.actor
    actor.close()
    before, calls, errors = registry(tmp_path), [], []
    dispatch = actor.external_features.dispatch_feature_mutation
    entered, resume = threading.Event(), threading.Event()
    publish, rpc, normal_timeout = host._publish_ticket, host._request, host.ack_timeout_s

    def counted(ref, arguments):
        calls.append(ref)
        return dispatch(ref, arguments)

    def delay(ticket):
        if ticket.kind == lost_kind and not entered.is_set():
            entered.set()
            assert resume.wait(3)
        publish(ticket)

    def bounded(kind, *args):
        if kind == lost_kind:
            host.ack_timeout_s = 0.05
        return rpc(kind, *args)

    def attempt():
        try:
            release_pin(host, saved)
        except FeatureMutationUnknown as error:
            errors.append(error)

    monkeypatch.setattr(actor.external_features, "dispatch_feature_mutation", counted)
    monkeypatch.setattr(host, "_publish_ticket", delay)
    monkeypatch.setattr(host, "_request", bounded)
    worker = threading.Thread(target=attempt)
    try:
        worker.start()
        assert entered.wait(2)
        worker.join(1)
        assert not worker.is_alive() and len(errors) == 1
        assert errors[0].recording_unknown is False and len(calls) == 1
        ref = errors[0].ref
        if lost_kind == "not_dispatched":
            retained = host._not_dispatched[ref.sha256]
            assert retained[1] == ref
            assert host.external_features.verify_no_effect_witness(retained[3], ref)
        resume.set()
        host.ack_timeout_s = normal_timeout
        monkeypatch.setattr(host, "_request", rpc)
        assert host.reconcile_mutation(ref)
        assert host.external_features.pending_mutations() == () and len(calls) == 1
        assert registry(tmp_path) == before
    finally:
        resume.set()
        host.ack_timeout_s = normal_timeout
        monkeypatch.setattr(host, "_request", rpc)
        worker.join(3)
        assert host.close()["durable"]


def test_wrong_witness_ref_generation_copied_ack_and_json_marker_fail_closed(tmp_path, monkeypatch):
    host, saved = create(tmp_path, reservation=None)
    host._current.actor.close()
    finish, consume, observed = host.finish_not_dispatched_mutation, host.consume_terminal, []

    def checked_finish(ref, witness):
        for wrong_ref, wrong_witness in (
            (ref, object()),
            (ref, replace(witness)),
            (replace(ref, generation=ref.generation + 1), witness),
            (replace(ref, mutation_token="foreign"), witness),
        ):
            with pytest.raises(ValidationError):
                finish(wrong_ref, wrong_witness)
        assert host.lookup_mutation(ref).ack.state == "prepared"
        with pytest.raises(ValidationError):
            host.finish_mutation(ref, encoded({"not_dispatched": True}))
        assert host.lookup_mutation(ref).ack.state == "prepared"
        value = finish(ref, witness)
        assert finish(ref, witness) is value
        observed.append(value)
        return value

    def checked_consume(ref, ack, witness):
        assert consume(ref, replace(ack), witness) is False
        assert consume(ref, ack, object()) is False
        return consume(ref, ack, witness)

    monkeypatch.setattr(host, "finish_not_dispatched_mutation", checked_finish)
    monkeypatch.setattr(host, "consume_terminal", checked_consume)
    try:
        with pytest.raises(FeatureMutationRejected):
            release_pin(host, saved)
        assert len(observed) == 1 and host.external_features.pending_mutations() == ()
    finally:
        assert host.close()["durable"]


@pytest.mark.parametrize("recording_unknown", [False, True])
def test_unknown_dispatch_cannot_gain_no_effect_authority(tmp_path, monkeypatch, recording_unknown):
    host, saved = create(tmp_path, reservation=None)
    before = registry(tmp_path)

    def unknown(ref, _):
        raise FeatureMutationUnknown(ref, "dispatch", recording_unknown)

    monkeypatch.setattr(host._current.actor.external_features, "dispatch_feature_mutation", unknown)
    try:
        with pytest.raises(FeatureMutationUnknown) as caught:
            release_pin(host, saved)
        ref = caught.value.ref
        assert caught.value.recording_unknown is recording_unknown
        original_unknown = host.lookup_mutation(ref)
        assert original_unknown.ref == ref and original_unknown.ack.state == "unknown"
        assert original_unknown.ack.code == "dispatch_unknown"
        assert original_unknown.ack.result_json is None
        assert host.external_features.no_effect_witness(ref) is None
        with pytest.raises(ValidationError):
            host.finish_not_dispatched_mutation(ref, object())
        assert host.lookup_mutation(ref) == original_unknown
        assert not host.reconcile_mutation(ref) and not host._not_dispatched
        assert host.lookup_mutation(ref) == original_unknown
        assert host.external_features.pending_mutations() == (ref,)
        assert host.external_features.status().recording_unknown == int(recording_unknown)
        assert host.external_features.status().pins == 1 and registry(tmp_path) == before
    finally:
        # The deliberately unresolved facade row keeps Host shutdown held;
        # close the actual original actor explicitly before observing that hold.
        host._current.actor.close()
        with pytest.raises(StorageError):
            host.close()
        assert not host.thread.is_alive() and not host._current.actor.thread.is_alive()


def test_resolved_rejected_release_keeps_obligation_for_explicit_manual_carry(tmp_path):
    host, saved = create(tmp_path, reservation=None)
    host._current.actor.close()
    try:
        with pytest.raises(FeatureMutationRejected):
            release_pin(host, saved)
        host.submit_recovery("after-no-effect", request(host))
        result = terminal(host, "after-no-effect")
        assert result["state"] == "succeeded", result
        assert host._current.context.generation == 1 and host.external_features.status().held
        assert host.external_features.settlement_pin_documents() == (
            encoded(saved["pin"].document()),
        )
        assert host._current.actor.external_features.contains_pin(saved["pin"])
    finally:
        assert host.close()["durable"]


def test_dead_cached_rejected_outcome_keeps_exact_coverage_and_original_pin_on_close(
    tmp_path, monkeypatch
):
    host, saved = create(tmp_path, reservation=None)
    actor = host._current.actor
    original_ack = actor.ack_feature_settlement
    before = registry(tmp_path)

    def reject(_):
        raise ValidationError("synthetic original pre-effect rejection")

    def die_before_ack(ref, ack, **options):
        actor.close()
        return original_ack(ref, ack, **options)

    monkeypatch.setattr(actor.external_features, "_generation_arguments_actor", reject)
    monkeypatch.setattr(actor, "ack_feature_settlement", die_before_ack)
    try:
        with pytest.raises(FeatureMutationUnknown) as caught:
            release_pin(host, saved)
        ref = caught.value.ref
        assert not caught.value.recording_unknown
        outcome = actor.read_feature_settlement(ref, host_token=host._host_token)
        assert outcome.phase == "rejected" and outcome.result_json is None
        assert host.reconcile_mutation(ref)
        assert host.external_features.pending_mutations() == ()
        assert host._terminal_coverage[ref.sha256] == outcome.sha256
        assert actor.generation_settlement_snapshot().outcomes == (outcome,)
        assert host.external_features.status().pins == 1
    finally:
        assert host.close()["durable"]
        assert host.closed and not host.thread.is_alive() and not actor.thread.is_alive()
    assert actor.generation_settlement_snapshot().outcomes == (outcome,)
    assert outcome.phase == "rejected" and outcome.result_json is None
    assert registry(tmp_path) == before


@pytest.mark.parametrize("barrier", ["capacity", "original_recording_unknown"])
def test_no_effect_capacity_or_original_unknown_blocks_before_terminal_write(
    tmp_path, monkeypatch, barrier
):
    host, saved = create(tmp_path, reservation=None)
    actor = host._current.actor
    actor.close()
    membership_getter = actor.generation_settlement_snapshot
    before, resolved = registry(tmp_path), False

    class FullEvidence(dict):
        def __len__(self):
            return 128

    if barrier == "capacity":
        host._not_dispatched = FullEvidence()
    else:
        monkeypatch.setattr(
            actor,
            "generation_settlement_snapshot",
            lambda: replace(membership_getter(), recording_unknown=True),
        )
    try:
        with pytest.raises(FeatureMutationUnknown) as caught:
            release_pin(host, saved)
        ref = caught.value.ref
        # Both barriers reject before this Host attempts a terminal write.
        # The facade still durably records its unresolved terminal attempt.
        assert caught.value.recording_unknown is False
        original_unknown = host.lookup_mutation(ref)
        assert original_unknown.ack.state == "unknown"
        assert original_unknown.ack.code == "terminal_unknown"
        assert original_unknown.ack.result_json is None
        assert ref.sha256 not in host._not_dispatched
        assert registry(tmp_path) == before
        witness = host.external_features.no_effect_witness(ref)
        assert witness is not None
        with pytest.raises(FeatureMutationUnknown) as blocked:
            host.finish_not_dispatched_mutation(ref, witness)
        assert blocked.value.recording_unknown is False
        assert host.lookup_mutation(ref) == original_unknown
        assert registry(tmp_path) == before
        if barrier == "capacity":
            assert type(host._not_dispatched) is FullEvidence
            assert list(host._not_dispatched.items()) == []
            host._not_dispatched = {}  # Remove only the artificial zero-entry capacity fixture.
        monkeypatch.setattr(actor, "generation_settlement_snapshot", membership_getter)
        ack = host.finish_not_dispatched_mutation(ref, witness)
        assert host._not_dispatched[ref.sha256][2] is ack
        assert host.reconcile_mutation(ref)
        assert host.external_features.pending_mutations() == ()
        assert host.external_features.status().pins == 1 and registry(tmp_path) == before
        resolved = True
    finally:
        monkeypatch.setattr(actor, "generation_settlement_snapshot", membership_getter)
        if resolved:
            assert host.close()["durable"]
        else:
            # A failed assertion must not manufacture a clean shutdown receipt.
            with pytest.raises(StorageError):
                host.close()
            assert not host.thread.is_alive() and not actor.thread.is_alive()
