"""Exact no-effect capabilities; default-provider integration belongs to Host tests."""

from dataclasses import replace

import pytest
from test_causal_generation_facade import RECEIPT, SHA, Broker, Provider, pin

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.inference import decoded, digest, encoded
from omega.runtime.causal_generation_facade import GenerationBinding, GenerationFeatureProvider
from omega.runtime.causal_recovery_records import FeatureMutationRejected, FeatureMutationUnknown


class OriginalGateProvider(Provider):
    def __init__(self):
        super().__init__()
        self.allow_release = True
        self.allow_pin = True
        self.unknown_write = False

    def dispatch_feature_mutation(self, ref, arguments_json):
        if self.unknown_write:
            raise FeatureMutationUnknown(ref, "dispatch", True)
        if ref.kind == "release" and not self.allow_release:
            raise FeatureMutationRejected("original_settlement_gate_closed")
        if ref.kind == "pin" and not self.allow_pin:
            raise FeatureMutationRejected("original_pin_admission_closed")
        return super().dispatch_feature_mutation(ref, arguments_json)


class CheckedBroker(Broker):
    def __init__(self):
        super().__init__()
        self.facade = None
        self.no_effect_calls = []
        self.no_effect_caps = {}
        self.lose_terminal_response = False
        self.lose_consume_response = False
        self.recording_failure = False

    def finish_not_dispatched_mutation(self, ref, witness):
        assert self.facade.verify_no_effect_witness(witness, ref)
        self.no_effect_calls.append((ref, witness))
        if self.recording_failure:
            raise FeatureMutationUnknown(ref, "terminal", True)
        self.no_effect_caps[ref.sha256] = witness
        ack = super().finish_mutation(ref, encoded({"not_dispatched": True}))
        if self.lose_terminal_response:
            raise TimeoutError("actual negative-effect ACK response lost")
        return ack

    def consume_terminal(self, ref, ack, witness):
        if decoded(ack.result_json) == {"not_dispatched": True}:
            cap = self.no_effect_caps.get(ref.sha256)
            assert cap is not None and self.facade.verify_no_effect_witness(cap, ref)
            assert self.facade.verify_applied_mutation(witness, ref, ack)
        consumed = super().consume_terminal(ref, ack, witness)
        if self.lose_consume_response:
            raise TimeoutError("positive consumption response lost")
        return consumed


def setup_checked():
    broker, provider = CheckedBroker(), OriginalGateProvider()
    facade = GenerationFeatureProvider(broker, lambda: 60001)
    broker.facade = facade
    binding = GenerationBinding("core", 0, "actor0", provider)
    facade.bind_initial(binding)
    return facade, broker, provider, binding


def reject_release(facade, broker, provider):
    original = pin(facade, provider)
    provider.allow_release = False
    broker.lose_terminal_response = True
    with pytest.raises(FeatureMutationUnknown) as caught:
        facade.release(original, consumer_receipt_sha256=RECEIPT)
    ref = caught.value.ref
    return original, ref, facade.no_effect_witness(ref), broker.records[ref.sha256]


def test_known_stopped_settlement_rejection_preserves_pin_and_uses_dedicated_port():
    facade, broker, provider, _ = setup_checked()
    original = pin(facade, provider)
    provider.allow_release = False
    with pytest.raises(FeatureMutationRejected):
        facade.release(original, consumer_receipt_sha256=RECEIPT)
    assert len(broker.no_effect_calls) == 1
    assert facade.status().hot_mutations == 0 and facade.status().pins == 1
    assert provider.contains_pin(original) and facade._released == {}
    ref, cap = broker.no_effect_calls[0]
    assert facade.no_effect_witness(ref) is None
    assert not facade.verify_no_effect_witness(cap, ref)  # Consumed original row is retired.


def test_lost_negative_terminal_reply_settles_only_original_rejected_row_without_release():
    facade, broker, provider, _ = setup_checked()
    original, ref, cap, ack = reject_release(facade, broker, provider)
    assert cap is not None and facade.verify_no_effect_witness(cap, ref)
    assert facade.status().unknown_mutations == 1 and facade.status().recording_unknown == 0
    assert not facade.settle_not_dispatched(ref, ack)
    assert not facade.settle_not_dispatched(ref, ack, witness=replace(cap))
    assert facade.settle_not_dispatched(ref, ack, witness=cap)
    assert facade.status().hot_mutations == 0 and facade.status().pins == 1
    assert provider.contains_pin(original) and not facade._released
    assert len(broker.no_effect_calls) == 1
    assert len([row for row in provider.calls if type(row) is tuple]) == 1  # Original pin only.


def test_wrong_ref_generation_binding_result_and_foreign_capability_are_rejected():
    facade, broker, provider, _ = setup_checked()
    _, ref, cap, ack = reject_release(facade, broker, provider)
    for foreign in (
        replace(ref, generation=1),
        replace(ref, actor_token="actor1"),
        replace(ref, arguments_sha256=SHA),
        replace(ref, mutation_token="foreign"),
    ):
        assert not facade.verify_no_effect_witness(cap, foreign)
    assert not facade.verify_no_effect_witness(
        replace(cap, result_json=encoded({"released": True})), ref
    )
    other, other_broker, other_provider, _ = setup_checked()
    _, same_ref, other_cap, _ = reject_release(other, other_broker, other_provider)
    assert same_ref == ref and other_cap is not cap
    assert not facade.verify_no_effect_witness(other_cap, ref)
    conflicting = replace(ack, result_json=encoded({"released": True}))
    with pytest.raises(StorageError):
        facade.settle_not_dispatched(ref, conflicting, witness=cap)
    assert facade.verify_no_effect_witness(cap, ref) and facade.status().pins == 1
    assert facade.settle_not_dispatched(ref, ack, witness=cap)


def test_lost_consume_reply_keeps_exact_no_effect_and_applied_witnesses():
    facade, broker, provider, _ = setup_checked()
    original = pin(facade, provider)
    provider.allow_release = False
    broker.lose_consume_response = True
    with pytest.raises(FeatureMutationUnknown) as caught:
        facade.release(original, consumer_receipt_sha256=RECEIPT)
    ref = caught.value.ref
    cap = facade.no_effect_witness(ref)
    ack = broker.records[ref.sha256]
    applied = facade.applied_mutation(ref, ack)
    assert cap is not None and facade.verify_applied_mutation(applied, ref, ack)
    assert not facade.settle_not_dispatched(ref, replace(ack), witness=cap)
    broker.lose_consume_response = False
    assert facade.settle_not_dispatched(ref, ack, witness=cap)
    assert facade.status().hot_mutations == 0 and provider.contains_pin(original)


@pytest.mark.parametrize("unknown_write", [False, True])
def test_unknown_dispatch_cannot_be_promoted_by_not_dispatched_json(unknown_write):
    facade, broker, provider, _ = setup_checked()
    original = pin(facade, provider)
    if unknown_write:
        provider.unknown_write = True
    else:
        provider.dispatch_error = ValidationError("unclassified original callback failure")
    with pytest.raises(FeatureMutationUnknown) as caught:
        facade.release(original, consumer_receipt_sha256=RECEIPT)
    ref = caught.value.ref
    assert facade.no_effect_witness(ref) is None and broker.no_effect_calls == []
    forged = broker.ack(ref, "terminal", result_json=encoded({"not_dispatched": True}))
    assert not facade.settle_not_dispatched(ref, forged)
    if unknown_write:
        assert facade.status().recording_unknown == 1
        assert not facade.reconcile_mutation(ref, forged)
    else:
        with pytest.raises(FeatureMutationUnknown):
            facade.reconcile_mutation(ref, forged)
    assert facade.status().pins == 1 and facade.status().hot_mutations == 1


def test_unconfirmed_negative_effect_recording_stays_sticky_with_original_pin():
    facade, broker, provider, _ = setup_checked()
    original = pin(facade, provider)
    provider.allow_release = False
    broker.recording_failure = True
    with pytest.raises(FeatureMutationUnknown) as caught:
        facade.release(original, consumer_receipt_sha256=RECEIPT)
    ref = caught.value.ref
    cap = broker.no_effect_calls[-1][1]
    assert caught.value.recording_unknown and not facade.verify_no_effect_witness(cap, ref)
    forged = broker.ack(ref, "terminal", result_json=encoded({"not_dispatched": True}))
    assert not facade.settle_not_dispatched(ref, forged, witness=cap)
    assert facade.status().pins == 1 and provider.contains_pin(original)


def test_preparation_rejection_never_mints_dispatch_no_effect_capability():
    facade, broker, provider, _ = setup_checked()
    broker.prepare_error = FeatureMutationRejected()
    with pytest.raises(FeatureMutationRejected):
        pin(facade, provider)
    assert broker.no_effect_calls == [] and provider.calls == []
    assert facade.pending_mutations() == ()


def test_bounded_rejected_rows_are_not_evicted_to_admit_more_new_risk():
    facade, broker, provider, _ = setup_checked()
    provider.allow_pin = False
    broker.lose_terminal_response = True
    refs = []
    for index in range(96):
        with pytest.raises(FeatureMutationUnknown) as caught:
            pin(facade, provider, frame=digest({"rejected": index}))
        refs.append(caught.value.ref)
    with pytest.raises(TradingBlocked):
        pin(facade, provider, frame=digest({"rejected": 96}))
    assert len(facade.pending_mutations()) == 96
    assert all(facade.no_effect_witness(ref) is not None for ref in refs)


def test_no_effect_resolution_does_not_let_pins0_install_forget_the_old_pin():
    facade, broker, provider, _ = setup_checked()
    original, ref, cap, ack = reject_release(facade, broker, provider)
    assert facade.settle_not_dispatched(ref, ack, witness=cap)
    fence = facade.hold(SHA)
    assert facade.close_settlement(fence)
    assert not facade.install_cas(fence, GenerationBinding("core", 1, "actor1", Provider()))
    facade.fence_shutdown()
    assert facade.status().pins == 1 and provider.contains_pin(original)
