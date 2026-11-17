"""Fresh new-owner pointer CAS; Root/physical authority belongs to Host tests."""

from dataclasses import replace

import pytest
from test_causal_generation_facade import RECEIPT, SHA, Broker, Provider, pin, setup
from test_causal_historical_carry_facade import CarryProvider, manifest_for

from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.inference import InferenceCommitAck, decoded, digest, encoded
from omega.runtime.causal_generation_facade import GenerationBinding, GenerationFeatureProvider
from omega.runtime.causal_historical_carry import CarryAcceptedAck


def cold_values(*, empty=False, two_pins=False):
    old_facade, _, old_provider, _ = setup()
    pins = () if empty else (pin(old_facade, old_provider),)
    if two_pins:
        pins += (pin(old_facade, old_provider, frame=SHA),)
    # Pin creation may predate the certified prior candidate by many generations.
    prior = GenerationBinding("prior-core", 7, "prior-candidate", old_provider)
    manifest = manifest_for(prior, pins)
    provider = CarryProvider(manifest)
    binding = GenerationBinding("new-core", 8, "new-candidate", provider)
    new_head = decoded(manifest.registry_json) | {"revision": manifest.delivery_revision + 1}
    acceptance = CarryAcceptedAck(
        manifest.sha256,
        binding.core_epoch,
        binding.generation,
        binding.actor_token,
        "current-store-scope",
        "current-store-binding",
        manifest.delivery_revision,
        manifest.delivery_sha256,
        InferenceCommitAck(manifest.delivery_revision + 1, digest(new_head), True),
    )
    provider.acceptance = acceptance
    return binding, manifest, acceptance, old_provider


def fresh(*, pin_capacity=1024):
    broker = Broker()
    return GenerationFeatureProvider(broker, lambda: 60001, pin_capacity=pin_capacity), broker


def test_fresh_cold_binding_preserves_old_bytes_and_allows_only_successor_settlement():
    facade, broker = fresh()
    binding, manifest, acceptance, old_provider = cold_values()
    original = manifest.pins[0]
    before = encoded(original.document())
    assert facade.bind_cold_initial(binding, manifest, acceptance, control_revision=11)
    status = facade.status()
    assert (status.core_epoch, status.generation, status.actor_token) == (
        "new-core",
        8,
        "new-candidate",
    )
    assert status.control_revision == 11 and status.held and status.settlement_open
    assert status.pins == 1 and facade.is_current_binding(binding)
    assert facade.settlement_pin_documents() == (before,)
    assert not facade.authorize(
        original, old_provider.snapshot, old_provider.candle, decision_at_ms=60000
    )
    with facade.entry_lease(
        original, old_provider.snapshot, old_provider.candle, decision_at_ms=60000
    ) as allowed:
        assert not allowed
    assert (
        facade.lookup(old_provider.binding, old_provider.candle, decision_at_ms=60000).disposition
        == "unavailable"
    )
    with pytest.raises(TradingBlocked):
        pin(facade, binding.provider)
    assert binding.provider.calls == []
    facade.release(original, consumer_receipt_sha256=RECEIPT)
    assert facade.status().pins == 0 and binding.provider.members == {}
    assert old_provider.contains_pin(original)
    ref = next(row[1] for row in broker.calls if row[0] == "prepare")
    assert (ref.core_epoch, ref.generation, ref.actor_token, ref.kind) == (
        "new-core",
        8,
        "new-candidate",
        "release",
    )
    assert encoded(decoded(broker.calls[0][2])["pin"]) == before
    assert manifest.reservations == (("unknown:retained-charge", 32),)
    assert manifest.charged_records == 3 and manifest.charged_bytes == 4128


def test_explicit_enable_does_not_promote_historical_membership_to_risk():
    facade, _ = fresh()
    binding, manifest, acceptance, old_provider = cold_values()
    assert facade.bind_cold_initial(binding, manifest, acceptance)
    assert facade.enable_current(8, "new-candidate", expected_control_revision=0)
    original = manifest.pins[0]
    assert not facade.authorize(
        original, old_provider.snapshot, old_provider.candle, decision_at_ms=60000
    )
    assert facade.offer_committed(old_provider.binding, old_provider.candle).snapshot is None
    with facade.entry_lease(
        original, old_provider.snapshot, old_provider.candle, decision_at_ms=60000
    ) as allowed:
        assert not allowed
    with pytest.raises(TradingBlocked):
        pin(facade, binding.provider)
    assert facade.status().pins == 1


def test_binding_is_initial_only_and_cannot_replace_even_an_equal_original_pointer():
    facade, _ = fresh()
    args = cold_values()[:3]
    assert facade.bind_cold_initial(*args)
    assert not facade.bind_cold_initial(*args)
    assert not facade.bind_cold_initial(replace(args[0]), args[1], args[2])
    ordinary, _ = fresh()
    ordinary.bind_initial(GenerationBinding("core", 0, "ordinary", Provider()), held=True)
    assert not ordinary.bind_cold_initial(*args)


def test_exact_actual_acceptance_and_matching_reserved_successor_are_required():
    binding, manifest, acceptance, _ = cold_values()
    facade, _ = fresh()
    assert not facade.bind_cold_initial(binding, manifest, replace(acceptance))
    assert facade.status().generation is None
    wrong_binding = replace(binding, generation=9)
    assert not facade.bind_cold_initial(wrong_binding, manifest, acceptance)
    wrong_origin = replace(manifest, origin_generation=6)
    assert not facade.bind_cold_initial(binding, wrong_origin, acceptance)
    wrong_head = replace(acceptance, observed_sha256=SHA)
    binding.provider.acceptance = wrong_head
    assert not facade.bind_cold_initial(binding, manifest, wrong_head)
    assert facade.status().generation is None


def test_original_member_readback_and_bounded_full_membership_are_required():
    facade, _ = fresh()
    binding, manifest, acceptance, _ = cold_values()
    binding.provider.members.clear()
    assert not facade.bind_cold_initial(binding, manifest, acceptance)
    assert facade.status().pins == 0
    tiny, _ = fresh(pin_capacity=1)
    args = cold_values(two_pins=True)[:3]
    assert not tiny.bind_cold_initial(*args)
    assert tiny.status().generation is None


@pytest.mark.parametrize("flag", ["recording_unknown", "unresolved_outcomes"])
def test_original_unknown_is_not_cleared_by_fresh_binding(flag):
    facade, _ = fresh()
    binding, manifest, _, _ = cold_values()
    changed = replace(manifest, **{flag: True})
    acceptance = replace(binding.provider.acceptance, manifest_sha256=changed.sha256)
    binding.provider.acceptance = acceptance
    assert not facade.bind_cold_initial(binding, changed, acceptance)
    assert facade.status().generation is None


@pytest.mark.parametrize("mode", ["terminal", "interlock", "interlock-cancel"])
def test_shutdown_memory_transition_wins_cold_binding_outside_lock_observations(mode):
    facade, _ = fresh()
    binding, manifest, acceptance, _ = cold_values()

    def fence_before_cas():
        assert facade._lock.acquire(blocking=False)
        facade._lock.release()
        if mode == "terminal":
            facade.fence_shutdown()
        else:
            assert facade.fence_install("core-close")
            if mode == "interlock-cancel":
                assert facade.clear_install_fence("core-close")

    binding.provider.on_acceptance = fence_before_cas
    assert not facade.bind_cold_initial(binding, manifest, acceptance)
    assert facade.status().generation is None and facade.status().pins == 0


def test_competing_initial_binding_wins_without_candidate_pointer_or_pin_publication():
    facade, _ = fresh()
    binding, manifest, acceptance, _ = cold_values()
    winner = GenerationBinding("other-core", 2, "other-actor", Provider())
    binding.provider.on_acceptance = lambda: facade.bind_initial(winner, held=True)
    assert not facade.bind_cold_initial(binding, manifest, acceptance)
    assert facade.is_current_binding(winner)
    assert facade.status().pins == 0 and facade.settlement_pin_documents() == ()


def test_empty_clean_closed_obligations_still_bind_manual_and_native_revision_only():
    facade, _ = fresh()
    args = cold_values(empty=True)[:3]
    for invalid in (True, -1, 2**63):
        with pytest.raises(ValidationError):
            facade.bind_cold_initial(*args, control_revision=invalid)
    assert facade.bind_cold_initial(*args, control_revision=4)
    assert facade.status().held and facade.status().pins == 0
    assert facade.list_bindings() == ()
    assert facade.enable_current(8, "new-candidate", expected_control_revision=4)
