"""Real temporary carry bytes/leases, with the existing explicit fake model/source.

The fixture's physical-proof SHA is test metadata, not an actual physical-close
claim. Host tests separately validate original default retirement authority.
"""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_causal_feature_delivery import ready
from test_causal_inference_service import create

from omega.domain.errors import StorageError, ValidationError
from omega.domain.inference import InferenceCommitAck, decoded, digest, encoded
from omega.runtime.causal_feature_delivery_store import CausalFeatureDeliveryStore
from omega.runtime.causal_generation_outcomes import GenerationContext
from omega.runtime.causal_historical_carry import (
    CarryAcceptedAck,
    CarryInstallRequest,
    CarryManifest,
    build_carry_manifest,
    read_default_carry_heads,
)
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_recovery_records import FeatureMutationRef


def retired_manifest(root, monkeypatch, *, reservation=False, consumed_pending=False):
    """Shared data fixture for new carry tests; no physical authority is minted."""
    context = GenerationContext("core1", 0, "original-actor", "host")
    service, _, provider, _, candle, cutoff, snapshot = ready(
        root, monkeypatch, generation_context=context
    )
    if reservation or consumed_pending:
        actual = provider.store.pin

        def charged(pin):
            result = actual(pin)
            if reservation:
                provider.store._charge("snapshot:" + "f" * 64, 17)
            if consumed_pending:
                body = {
                    "scope": "consumed_external_feature_pin",
                    "pin_id": pin.pin_id,
                    "consumer_receipt_sha256": "b" * 64,
                }
                consumed = body | {"record_sha256": digest(body)}
                provider.store._charge(
                    "release:" + pin.pin_id, len(encoded(consumed).encode()), release=True
                )
                provider.store._put(provider.store._path("consumed", pin.pin_id), consumed)
            return result

        provider.store.pin = charged
    pin = provider.pin("consumer", "a" * 64, snapshot, candle, decision_at_ms=cutoff)
    service.close()
    manifest = build_carry_manifest(
        root,
        context=context,
        physical_proof_sha256="c" * 64,
        journal_ack=service._retirement_last_ack,
        settlement=service.generation_settlement_snapshot(),
    )
    return manifest, pin, context


def request_for(manifest, *, token="successor-actor"):
    return CarryInstallRequest(manifest, "core1", manifest.origin_generation + 1, token)


def test_actual_manifest_binds_full_pin_snapshot_input_receipt_and_canonical_codecs(
    tmp_path, monkeypatch
):
    manifest, pin, context = retired_manifest(tmp_path, monkeypatch)
    assert manifest.pins == (pin,) and manifest.origin_generation == context.generation
    assert manifest.charged_records > 0 and manifest.charged_bytes > 0
    provenance = decoded(manifest.provenance_json)
    assert provenance["pin_records"][0] == {
        "pin": pin.document(),
        "membership_present": True,
        "consumed": None,
    }
    snapshot = provenance["snapshots"][0]
    assert snapshot["snapshot_sha256"] == pin.snapshot_sha256
    assert len(snapshot["input_document_sha256"]) == len(snapshot["receipt_sha256"]) == 64
    assert CarryManifest.parse(manifest.document()) == manifest
    assert CarryManifest.parse(manifest.document()).sha256 == manifest.sha256
    with pytest.raises(ValidationError):
        replace(manifest, origin_generation=True)
    with pytest.raises(ValidationError):
        CarryInstallRequest(manifest, "core1", True, "other")
    with pytest.raises(ValidationError):
        request_for(replace(manifest, recording_unknown=True))
    with pytest.raises(ValidationError):
        request_for(replace(manifest, unresolved_outcomes=True))


def test_default_cold_heads_validate_actual_envelopes_without_any_writer_construction(
    tmp_path, monkeypatch
):
    manifest, _, _ = retired_manifest(tmp_path, monkeypatch)

    def forbidden(*_, **__):
        raise AssertionError("cold carry reader constructed a writer")

    monkeypatch.setattr(FileInferenceStateStore, "__init__", forbidden)
    journal, delivery = read_default_carry_heads(tmp_path)
    assert journal["revision"] == manifest.journal_revision
    assert journal["snapshot_sha256"] == manifest.journal_sha256
    assert delivery["revision"] == manifest.delivery_revision
    assert delivery["snapshot_sha256"] == manifest.delivery_sha256
    assert (
        set(journal)
        == set(delivery)
        == {"schema_version", "revision", "snapshot_sha256", "snapshot"}
    )


def test_consumed_file_with_original_membership_is_explicit_not_silently_filtered(
    tmp_path, monkeypatch
):
    manifest, pin, _ = retired_manifest(tmp_path, monkeypatch, consumed_pending=True)
    assert manifest.pins == ()
    assert pin.pin_id in decoded(manifest.registry_json)["pins"]
    record = decoded(manifest.provenance_json)["pin_records"][0]
    assert record["pin"] == pin.document() and record["membership_present"] is True
    assert record["consumed"]["consumer_receipt_sha256"] == "b" * 64
    reservation = decoded(manifest.provenance_json)["reservations"][0]
    assert reservation["kind"] == "release" and reservation["state"] == "present"
    store = CausalFeatureDeliveryStore(tmp_path, carry_install=request_for(manifest))
    try:
        assert store.load_pins() == {}
        assert store.release(pin, "b" * 64) is None
        with pytest.raises(ValidationError):
            store.release(pin, "d" * 64)
        assert store.data["reservations"] == dict(manifest.reservations)
    finally:
        store.close()


def test_new_original_store_accepts_once_then_releases_old_full_pin_without_model(
    tmp_path, monkeypatch
):
    manifest, pin, _ = retired_manifest(tmp_path, monkeypatch)
    request = request_for(manifest)
    store = CausalFeatureDeliveryStore(tmp_path, carry_install=request)
    try:
        acceptance = store.carry_acceptance_snapshot()
        assert type(acceptance) is CarryAcceptedAck
        assert acceptance.manifest_sha256 == manifest.sha256
        assert acceptance.observed_revision == manifest.delivery_revision
        assert acceptance.observed_sha256 == manifest.delivery_sha256
        assert acceptance.commit_ack is store.store._last_ack
        assert type(acceptance.commit_ack) is InferenceCommitAck
        assert acceptance.commit_ack.revision == manifest.delivery_revision + 1
        assert store.data["bytes"] == manifest.charged_bytes
        assert store.data["records"] == manifest.charged_records
        assert store.load_pins() == {pin.pin_id: pin}
        store.release(pin, "b" * 64)
        after_first = store.data["bytes"], store.data["records"]
        assert store.release(pin, "b" * 64) is None
        assert (store.data["bytes"], store.data["records"]) == after_first
        with pytest.raises(ValidationError):
            store.release(pin, "d" * 64)
        assert store.load_pins() == {}
        assert store.carry_acceptance_snapshot() is acceptance
    finally:
        store.close()


def test_current_generation_release_uses_new_ref_unchanged_old_pin_and_no_activation(
    tmp_path, monkeypatch
):
    manifest, pin, _ = retired_manifest(tmp_path, monkeypatch)
    request = request_for(manifest)
    context = GenerationContext("core1", 1, request.new_actor_token, "new-host")
    service, state = create(
        tmp_path, monkeypatch, generation_context=context, carry_install=request
    )
    try:
        provider = service.external_features
        acceptance = service.carry_acceptance_snapshot()
        assert acceptance is provider.carry_acceptance_snapshot()
        assert acceptance is provider.store.carry_acceptance_snapshot()
        assert provider.contains_pin(pin) is True
        assert provider.list_bindings() == () and service.owner is service.source is None
        assert state["workers"] == [] and state["source_calls"] == 0
        arguments = encoded({"pin": pin.document(), "consumer_receipt_sha256": "b" * 64})
        ref = FeatureMutationRef(
            "core1",
            1,
            request.new_actor_token,
            "new-release",
            "release",
            pin.pin_id,
            digest(decoded(arguments)),
        )
        assert provider.dispatch_feature_mutation(ref, arguments) is None
        assert not provider.contains_pin(pin)
        outcome = service.read_feature_settlement(ref, host_token=context.host_token)
        assert outcome.ref == ref and outcome.phase == "complete"
        assert decoded(outcome.result_json)["pin_sha256"] == digest(pin.document())
        assert not outcome.recording_unknown
    finally:
        service.close()
    next_manifest = build_carry_manifest(
        tmp_path,
        context=context,
        physical_proof_sha256="e" * 64,
        journal_ack=service._retirement_last_ack,
        settlement=service.generation_settlement_snapshot(),
    )
    assert next_manifest.pins == ()
    second = CausalFeatureDeliveryStore(
        tmp_path, carry_install=request_for(next_manifest, token="third-actor")
    )
    try:
        assert second.carry_acceptance_snapshot().new_generation == 2
        assert second.data["records"] == next_manifest.charged_records
        assert second.data["bytes"] == next_manifest.charged_bytes
        assert second.release(pin, "b" * 64) is None
    finally:
        second.close()


def test_unknown_missing_reservation_evidence_remains_charged_and_classified_unknown(
    tmp_path, monkeypatch
):
    manifest, pin, _ = retired_manifest(tmp_path, monkeypatch, reservation=True)
    assert manifest.has_unknown_reservations
    assert manifest.reservations == (("snapshot:" + "f" * 64, 17),)
    assert decoded(manifest.provenance_json)["reservations"] == [
        {
            "key": "snapshot:" + "f" * 64,
            "size": 17,
            "kind": "snapshot",
            "state": "unknown",
            "evidence_sha256": None,
        }
    ]
    store = CausalFeatureDeliveryStore(tmp_path, carry_install=request_for(manifest))
    try:
        assert tuple(sorted(store.data["reservations"].items())) == manifest.reservations
        assert store.data["bytes"] == manifest.charged_bytes
        assert store.data["records"] == manifest.charged_records
        store.release(pin, "b" * 64)
        assert store.data["reservations"]["snapshot:" + "f" * 64] == 17
    finally:
        store.close()


@pytest.mark.parametrize("changed", ["head", "pin", "snapshot", "input"])
def test_changed_actual_head_or_immutable_evidence_rejects_before_constructor_save(
    tmp_path, monkeypatch, changed
):
    manifest, pin, _ = retired_manifest(tmp_path, monkeypatch)
    if changed == "head":
        intervening = CausalFeatureDeliveryStore(tmp_path)
        intervening.close()
    elif changed == "pin":
        path = tmp_path / "artifacts/causal-feature-delivery/pins" / (pin.pin_id + ".json")
        path.write_text(encoded(replace(pin, created_at_ms=pin.created_at_ms + 1).document()))
    elif changed == "snapshot":
        path = (
            tmp_path
            / "artifacts/causal-feature-delivery/snapshots"
            / (pin.snapshot_sha256 + ".json")
        )
        path.write_text("{}")
    else:
        ref = decoded(manifest.provenance_json)["snapshots"][0]["input_ref"]
        (tmp_path / ref).write_text("{}")
    head_path = tmp_path / "state/causal-feature-delivery.json"
    before = head_path.read_bytes()
    saves = []
    actual_save = CausalFeatureDeliveryStore._save

    def observed(store):
        saves.append(True)
        return actual_save(store)

    monkeypatch.setattr(CausalFeatureDeliveryStore, "_save", observed)
    with pytest.raises((StorageError, ValidationError, ValueError)):
        CausalFeatureDeliveryStore(tmp_path, carry_install=request_for(manifest))
    assert saves == [] and head_path.read_bytes() == before


def test_actual_new_commit_with_duck_return_is_not_original_carry_acceptance(tmp_path, monkeypatch):
    manifest, _, _ = retired_manifest(tmp_path, monkeypatch)
    actual = FileInferenceStateStore.save_snapshot

    def duck(store, raw, **kwargs):
        ack = actual(store, raw, **kwargs)
        return SimpleNamespace(
            durable=ack.durable, revision=ack.revision, snapshot_sha256=ack.snapshot_sha256
        )

    monkeypatch.setattr(FileInferenceStateStore, "save_snapshot", duck)
    with pytest.raises(StorageError, match="startup ACK"):
        CausalFeatureDeliveryStore(tmp_path, carry_install=request_for(manifest))


def test_carry_acceptance_cached_inspection_has_io0_and_rejects_scalar_store_substitution(
    tmp_path, monkeypatch
):
    manifest, pin, _ = retired_manifest(tmp_path, monkeypatch)
    request = request_for(manifest)
    context = GenerationContext("core1", 1, request.new_actor_token, "new-host")
    service, _ = create(tmp_path, monkeypatch, generation_context=context, carry_install=request)
    provider = service.external_features
    try:
        acceptance = service.carry_acceptance_snapshot()

        def forbidden(*_, **__):
            raise AssertionError("cached acceptance inspection attempted IO")

        with monkeypatch.context() as cached:
            cached.setattr(Path, "open", forbidden)
            for _ in range(100):
                assert service.carry_acceptance_snapshot() is acceptance
                assert provider.contains_pin(pin)
        original = provider.store
        original_ack = original._carry_acceptance
        original._carry_acceptance = replace(original_ack)
        try:
            assert service.carry_acceptance_snapshot() is None
        finally:
            original._carry_acceptance = original_ack
        provider.store = SimpleNamespace()
        try:
            assert service.carry_acceptance_snapshot() is None
        finally:
            provider.store = original
    finally:
        service.close()


def test_absent_carry_option_preserves_default_store_and_no_acceptance(tmp_path):
    store = CausalFeatureDeliveryStore(tmp_path)
    try:
        assert store.revision == 1 and store.carry_acceptance_snapshot() is None
        assert store.data["pins"] == store.data["reservations"] == {}
    finally:
        store.close()
