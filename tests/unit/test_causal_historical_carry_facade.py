"""Carry CAS and successor settlement, with checked original-provider fakes.

Physical retirement and default-file provenance are exercised by backend/Host
tests. These tests never promote their data-only manifest into physical proof.
"""

from dataclasses import replace

import pytest
from test_causal_generation_facade import FRAME, RECEIPT, SHA, Broker, Provider, pin, setup

from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.external_features import ExternalFeaturePin
from omega.domain.inference import CAUSAL_SCOPE, InferenceCommitAck, decoded, digest, encoded
from omega.runtime.causal_generation_facade import GenerationBinding, GenerationFeatureProvider
from omega.runtime.causal_historical_carry import CarryAcceptedAck, CarryManifest
from omega.runtime.causal_recovery_records import FeatureMutationUnknown


def manifest_for(binding, pins, *, reservations=(("unknown:retained-charge", 32),)):
    pins = tuple(sorted(pins, key=lambda value: value.pin_id))
    registry = {
        "scope": CAUSAL_SCOPE,
        "schema_version": 1,
        "revision": binding.generation + 7,
        "records": 2 * len(pins) + len(reservations),
        "bytes": 4096 * len(pins) + 32,
        "latest": {},
        "pins": {
            value.pin_id: {
                "snapshot": value.snapshot_sha256,
                "proof_sha256": digest(value.document()),
            }
            for value in pins
        },
        "reservations": dict(reservations),
    }
    provenance = {
        "snapshots": [],
        "pin_records": [
            {"pin": value.document(), "membership_present": True, "consumed": None}
            for value in pins
        ],
        "reservations": [
            {
                "key": key,
                "size": size,
                "kind": "unknown",
                "state": "unknown",
                "evidence_sha256": None,
            }
            for key, size in reservations
        ],
    }
    return CarryManifest(
        binding.core_epoch,
        binding.generation,
        binding.actor_token,
        "original-provider",
        "original-store-scope",
        "original-store-binding",
        SHA,
        3,
        SHA,
        SHA,
        encoded(registry),
        encoded(provenance),
        pins,
        reservations,
        False,
        False,
    )


class CarryProvider(Provider):
    def __init__(self, manifest):
        super().__init__()
        self.members = {value.pin_id: value for value in manifest.pins}
        self.acceptance = None
        self.on_acceptance = None

    def carry_acceptance_snapshot(self):
        if self.on_acceptance:
            self.on_acceptance()
        return self.acceptance


def prepare(facade, original, pins, *, manifest=None):
    manifest = manifest_for(original, pins) if manifest is None else manifest
    provider = CarryProvider(manifest)
    candidate = GenerationBinding(
        "core", original.generation + 1, "actor" + str(original.generation + 1), provider
    )
    head = decoded(manifest.registry_json) | {"revision": manifest.delivery_revision + 1}
    provider.acceptance = CarryAcceptedAck(
        manifest.sha256,
        candidate.core_epoch,
        candidate.generation,
        candidate.actor_token,
        "new-store-scope" + str(candidate.generation),
        "new-store-binding" + str(candidate.generation),
        manifest.delivery_revision,
        manifest.delivery_sha256,
        InferenceCommitAck(manifest.delivery_revision + 1, digest(head), True),
    )
    fence = facade.hold(digest({"carry": original.generation}))
    assert facade.close_settlement(fence)
    return fence, candidate, manifest, provider.acceptance


def install(facade, original, pins):
    prepared = prepare(facade, original, pins)
    assert facade.install_carry_cas(*prepared)
    return prepared[1], prepared[2]


def test_carry_preserves_full_pin_bytes_and_grants_successor_release_only():
    facade, broker, old_provider, original = setup()
    old_pin = pin(facade, old_provider)
    old_bytes = encoded(old_pin.document())
    candidate, manifest = install(facade, original, (old_pin,))
    assert facade.is_current_binding(candidate)
    assert not facade.is_current_binding(replace(candidate))
    assert not facade.is_current_binding(original)
    assert facade.settlement_pin_documents() == (old_bytes,)
    assert facade.status().pins == 1 and facade.status().held
    assert manifest.charged_records == 3 and manifest.charged_bytes == 4128
    assert manifest.reservations == (("unknown:retained-charge", 32),)
    status = facade.status()
    assert facade.enable_current(1, "actor1", expected_control_revision=status.control_revision)
    assert (
        facade.lookup(old_provider.binding, old_provider.candle, decision_at_ms=60000).disposition
        == "unavailable"
    )
    assert facade.offer_committed(old_provider.binding, old_provider.candle).snapshot is None
    assert not facade.authorize(
        old_pin, old_provider.snapshot, old_provider.candle, decision_at_ms=60000
    )
    with facade.entry_lease(
        old_pin, old_provider.snapshot, old_provider.candle, decision_at_ms=60000
    ) as allowed:
        assert not allowed
    with pytest.raises(TradingBlocked):
        pin(facade, candidate.provider)
    with pytest.raises(TradingBlocked):
        facade.pin(
            "different-consumer",
            FRAME,
            old_provider.snapshot,
            old_provider.candle,
            decision_at_ms=60000,
        )
    facade.release(ExternalFeaturePin.parse(old_pin.document()), consumer_receipt_sha256=RECEIPT)
    assert not candidate.provider.contains_pin(old_pin)
    assert old_provider.contains_pin(old_pin)  # The retired provider is never dispatched.
    assert encoded(old_pin.document()) == old_bytes
    assert facade.status().pins == 0 and facade.settlement_pin_documents() == ()
    release_ref = next(
        row[1] for row in broker.calls if row[0] == "prepare" and row[1].kind == "release"
    )
    assert (release_ref.generation, release_ref.actor_token) == (1, "actor1")
    release_args = next(
        row[2] for row in broker.calls if row[0] == "prepare" and row[1] == release_ref
    )
    assert encoded(decoded(release_args)["pin"]) == old_bytes
    facade.release(old_pin, consumer_receipt_sha256=RECEIPT)
    with pytest.raises(ValidationError):
        facade.release(old_pin, consumer_receipt_sha256=SHA)
    assert manifest.reservations == (("unknown:retained-charge", 32),)


def test_exact_original_acceptance_and_full_old_member_set_are_required():
    facade, _, provider, original = setup()
    old_pin = pin(facade, provider)
    fence, candidate, manifest, acceptance = prepare(facade, original, (old_pin,))
    assert not facade.install_carry_cas(fence, candidate, manifest, replace(acceptance))
    assert facade.is_current_binding(original)
    changed = replace(old_pin, created_at_ms=60001)
    changed_manifest = manifest_for(original, (changed,))
    changed_args = prepare(facade, original, (changed,), manifest=changed_manifest)
    assert not facade.install_carry_cas(*changed_args)
    assert facade.settlement_pin_documents() == (encoded(old_pin.document()),)
    missing_args = prepare(facade, original, (), manifest=manifest_for(original, ()))
    assert not facade.install_carry_cas(*missing_args)
    candidate.provider.members.clear()
    assert not facade.install_carry_cas(fence, candidate, manifest, acceptance)


def test_two_successive_carries_preserve_old_and_new_obligations_without_risk_revival():
    facade, broker, provider0, binding0 = setup()
    first = pin(facade, provider0)
    binding1, manifest1 = install(facade, binding0, (first,))
    assert facade.enable_current(
        1, "actor1", expected_control_revision=facade.status().control_revision
    )
    binding1.provider.snapshot = replace(
        binding1.provider.snapshot, stream_epoch="new-manual-stream"
    )
    second = pin(facade, binding1.provider, frame=SHA)
    binding2, manifest2 = install(facade, binding1, (first, second))
    assert manifest1.origin_generation == 0 and manifest2.origin_generation == 1
    assert facade.status().pins == 2
    assert facade.status().held  # Settlement does not require model activation/reset.
    assert not facade.install_cas(
        facade.hold(SHA), GenerationBinding("core", 3, "actor3", Provider())
    )
    # Reopen a separate carry transition only; HELD still allows exact old releases.
    assert facade.status().settlement_open
    facade.release(first, consumer_receipt_sha256=RECEIPT)
    facade.release(second, consumer_receipt_sha256=SHA)
    refs = [row[1] for row in broker.calls if row[0] == "prepare" and row[1].kind == "release"]
    assert len(refs) == 2 and all(
        ref.generation == 2 and ref.actor_token == "actor2" for ref in refs
    )
    assert facade.status().pins == 0 and binding2.provider.members == {}
    assert provider0.contains_pin(first) and binding1.provider.contains_pin(second)


def test_lost_successor_consume_response_blocks_next_install_until_same_original_ack():
    facade, broker, provider, original = setup()
    old_pin = pin(facade, provider)
    successor, _ = install(facade, original, (old_pin,))
    broker.consume_error = TimeoutError()
    with pytest.raises(FeatureMutationUnknown) as caught:
        facade.release(old_pin, consumer_receipt_sha256=RECEIPT)
    ref, ack, witness = broker.consume_calls[-1]
    assert ref == caught.value.ref and ref.generation == 1
    assert facade.verify_applied_mutation(witness, ref, ack)
    assert facade.status().pins == 0 and facade.status().hot_mutations == 1
    next_args = prepare(facade, successor, ())
    assert not facade.install_carry_cas(*next_args)
    assert facade.is_current_binding(successor)
    assert not facade.reconcile_mutation(ref, replace(ack))
    broker.consume_error = None
    assert facade.reconcile_mutation(ref, ack)
    assert facade.status().hot_mutations == 0
    assert facade.install_carry_cas(*next_args)
    assert len([value for value in successor.provider.calls if type(value) is tuple]) == 1


def test_old_unknown_cannot_be_replaced_by_a_new_carry_ref():
    facade, _, provider, original = setup()
    old_pin = pin(facade, provider)
    provider.dispatch_error = TimeoutError()
    with pytest.raises(FeatureMutationUnknown) as caught:
        facade.release(old_pin, consumer_receipt_sha256=RECEIPT)
    prepared = prepare(facade, original, (old_pin,))
    assert not facade.install_carry_cas(*prepared)
    assert facade.pending_mutations() == (caught.value.ref,)
    assert facade.status().pins == 1 and facade.is_current_binding(original)


@pytest.mark.parametrize("mode", ["interlock", "shutdown"])
def test_shutdown_wins_candidate_observation_before_short_carry_cas(mode):
    facade, _, provider, original = setup()
    old_pin = pin(facade, provider)
    prepared = prepare(facade, original, (old_pin,))
    candidate = prepared[1]

    def fence_during_cached_observation():
        assert facade._lock.acquire(blocking=False)
        facade._lock.release()
        if mode == "interlock":
            facade.fence_install("core-close")
        else:
            facade.fence_shutdown()

    candidate.provider.on_acceptance = fence_during_cached_observation
    assert not facade.install_carry_cas(*prepared)
    assert facade.is_current_binding(original)
    assert facade.settlement_pin_documents() == (encoded(old_pin.document()),)


def test_manifest_unknown_recording_and_unresolved_effects_stay_blocked():
    for flag in ("recording_unknown", "unresolved_outcomes"):
        facade, _, provider, original = setup()
        old_pin = pin(facade, provider)
        manifest = replace(manifest_for(original, (old_pin,)), **{flag: True})
        args = prepare(facade, original, (old_pin,), manifest=manifest)
        assert not facade.install_carry_cas(*args)
        assert facade.is_current_binding(original) and facade.status().pins == 1


def test_carry_membership_table_is_an_active_capacity_not_a_lifetime_counter():
    broker, provider = Broker(), Provider()
    facade = GenerationFeatureProvider(broker, lambda: 60001, pin_capacity=1)
    original = GenerationBinding("core", 0, "actor0", provider)
    facade.bind_initial(original)
    old_pin = pin(facade, provider)
    successor, _ = install(facade, original, (old_pin,))
    facade.release(old_pin, consumer_receipt_sha256=RECEIPT)
    assert facade.enable_current(
        1, "actor1", expected_control_revision=facade.status().control_revision
    )
    successor.provider.snapshot = replace(
        successor.provider.snapshot, stream_epoch="new-manual-stream"
    )
    for index in range(140):
        current = pin(facade, successor.provider, frame=digest({"cycle": index}))
        facade.release(current, consumer_receipt_sha256=RECEIPT)
    assert facade.status().pins == 0
    assert len(facade._historical_pins) == 0 and len(facade._released) == 128
