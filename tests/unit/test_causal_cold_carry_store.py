"""Cold carry DATA from real files and actual default no-model startup evidence.

These tests deliberately supply a data-only exclusion SHA. They do not claim
Root exclusion or grant a Host factory; those predicates belong to Host tests.
Only the older pin-creator fixture uses the explicit fake model/source.
"""

from dataclasses import replace

import pytest
from test_causal_historical_carry_store import retired_manifest
from test_causal_inference_service import command, terminal

from omega.domain.errors import StorageError, ValidationError
from omega.domain.inference import decoded, digest
from omega.runtime import causal_historical_carry as carry_module
from omega.runtime.causal_capacity import EvidenceBudgetLimits
from omega.runtime.causal_feature_delivery_store import CausalFeatureDeliveryStore
from omega.runtime.causal_generation_outcomes import GenerationContext
from omega.runtime.causal_historical_carry import (
    CarryInstallRequest,
    build_cold_carry_manifest,
    verify_clean_closed_carry_manifest,
)
from omega.runtime.causal_inference_service import CausalInferenceService
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_startup_policy import ManualStartupCertificate


def candidate_observation(root, monkeypatch, *, reservation=False):
    older, pin, _ = retired_manifest(root, monkeypatch, reservation=reservation)
    context = GenerationContext("certified-core", 1, "certified-candidate", "host-token")
    install = CarryInstallRequest(
        older, context.core_epoch, context.generation, context.actor_token
    )
    candidate = CausalInferenceService(
        root,
        core_epoch=context.core_epoch,
        training_job_provider=lambda _: None,
        generation_context=context,
        carry_install=install,
    )
    candidate.start()
    certificate = candidate.manual_startup_certificate()
    assert type(certificate) is ManualStartupCertificate
    assert candidate.owner is candidate.source is candidate.activation is None
    assert candidate.external_features.list_bindings() == ()
    return candidate, certificate, pin, older


def close_candidate(candidate):
    try:
        candidate.close()
    except StorageError:
        assert candidate.closed


@pytest.mark.parametrize("reservation", [False, True])
def test_cold_manifest_origin_is_certified_prior_candidate_and_old_pin_bytes_are_unchanged(
    tmp_path, monkeypatch, reservation
):
    candidate, certificate, pin, older = candidate_observation(
        tmp_path, monkeypatch, reservation=reservation
    )
    try:
        historical = ManualStartupCertificate.parse(certificate.document())
        manifest = build_cold_carry_manifest(
            tmp_path, historical, exclusion_sha256="e" * 64, limits=EvidenceBudgetLimits()
        )
        assert manifest.origin_core_epoch == certificate.core_epoch != older.origin_core_epoch
        assert manifest.origin_generation == certificate.generation == older.origin_generation + 1
        assert manifest.origin_actor_token == certificate.actor_token != older.origin_actor_token
        assert manifest.origin_provider_token == certificate.provider_token
        assert manifest.origin_store_scope_token == certificate.store_scope_token
        assert manifest.origin_store_binding_token == certificate.store_binding_token
        assert manifest.settlement_sha256 == certificate.settlement_sha256
        assert manifest.pins == (pin,) and manifest.pins[0].document() == pin.document()
        assert manifest.physical_proof_sha256 == "e" * 64
        assert manifest.physical_proof_sha256 != older.physical_proof_sha256
        assert manifest.charged_bytes == older.charged_bytes
        assert manifest.charged_records == older.charged_records
        assert manifest.reservations == older.reservations
        assert manifest.has_unknown_reservations is reservation
        assert not manifest.recording_unknown and not manifest.unresolved_outcomes
        assert candidate.external_features.contains_pin(pin)
        assert candidate.owner is candidate.source is None
    finally:
        close_candidate(candidate)


def test_cold_data_builder_constructs_no_writer_old_context_or_old_typed_ack(tmp_path, monkeypatch):
    candidate, certificate, pin, _ = candidate_observation(tmp_path, monkeypatch)
    try:

        def forbidden(*_, **__):
            raise AssertionError("cold data builder minted a writer or old resource value")

        with monkeypatch.context() as no_capabilities:
            no_capabilities.setattr(FileInferenceStateStore, "__init__", forbidden)
            no_capabilities.setattr(carry_module, "GenerationContext", forbidden)
            no_capabilities.setattr(carry_module, "InferenceCommitAck", forbidden)
            manifest = build_cold_carry_manifest(
                tmp_path, certificate, exclusion_sha256="e" * 64, limits=EvidenceBudgetLimits()
            )
        assert manifest.pins == (pin,)
        assert manifest.settlement_sha256 == certificate.settlement_sha256
    finally:
        close_candidate(candidate)


@pytest.mark.parametrize(
    "field,value",
    [
        ("provider_token", "foreign-provider"),
        ("store_scope_token", "foreign-store-scope"),
        ("store_binding_token", "foreign-store-binding"),
        ("actor_token", "foreign-candidate"),
        ("settlement_sha256", "f" * 64),
        ("delivery_revision", 999999),
    ],
)
def test_cold_data_rejects_changed_certified_identity_or_settlement_hash(
    tmp_path, monkeypatch, field, value
):
    candidate, certificate, _, _ = candidate_observation(tmp_path, monkeypatch)
    try:
        changed = replace(certificate, **{field: value})
        with pytest.raises(StorageError):
            build_cold_carry_manifest(
                tmp_path, changed, exclusion_sha256="e" * 64, limits=EvidenceBudgetLimits()
            )
    finally:
        close_candidate(candidate)


def test_new_actual_control_journal_head_invalidates_untouched_startup_observation(
    tmp_path, monkeypatch
):
    candidate, certificate, _, _ = candidate_observation(tmp_path, monkeypatch)
    try:
        deployment = candidate.snapshot()["deployments"][0]["id"]
        command(candidate, "new-pause", "pause", deployment_id=deployment)
        assert terminal(candidate, "new-pause")["state"] == "complete"
        with pytest.raises(StorageError):
            build_cold_carry_manifest(
                tmp_path, certificate, exclusion_sha256="e" * 64, limits=EvidenceBudgetLimits()
            )
        assert candidate.owner is candidate.source is None
    finally:
        close_candidate(candidate)


def test_changed_actual_input_provenance_rejects_even_when_fixed_startup_heads_match(
    tmp_path, monkeypatch
):
    candidate, certificate, _, older = candidate_observation(tmp_path, monkeypatch)
    try:
        input_ref = decoded(older.provenance_json)["snapshots"][0]["input_ref"]
        path = tmp_path / input_ref
        original = path.read_bytes()
        path.write_text("{}")
        try:
            with pytest.raises((StorageError, ValidationError, ValueError)):
                build_cold_carry_manifest(
                    tmp_path,
                    certificate,
                    exclusion_sha256="e" * 64,
                    limits=EvidenceBudgetLimits(),
                )
        finally:
            path.write_bytes(original)
    finally:
        close_candidate(candidate)


def test_clean_closed_verifier_preserves_exact_manifest_and_old_physical_observation(
    tmp_path, monkeypatch
):
    manifest, pin, _ = retired_manifest(tmp_path, monkeypatch, reservation=True)
    before = (tmp_path / "state/causal-feature-delivery.json").read_bytes()

    def forbidden(*_, **__):
        raise AssertionError("closed data verifier constructed a writer or past ACK")

    monkeypatch.setattr(FileInferenceStateStore, "__init__", forbidden)
    monkeypatch.setattr(carry_module, "InferenceCommitAck", forbidden)
    monkeypatch.setattr(carry_module, "GenerationContext", forbidden)
    result = verify_clean_closed_carry_manifest(tmp_path, manifest, limits=EvidenceBudgetLimits())
    assert result is manifest and result.pins == (pin,)
    assert result.physical_proof_sha256 == manifest.physical_proof_sha256
    assert result.sha256 == digest(manifest.document())
    assert result.has_unknown_reservations
    assert (tmp_path / "state/causal-feature-delivery.json").read_bytes() == before


def test_clean_closed_rejects_changed_current_head_and_unresolved_original_flags(
    tmp_path, monkeypatch
):
    manifest, _, _ = retired_manifest(tmp_path, monkeypatch)
    for flags in ({"recording_unknown": True}, {"unresolved_outcomes": True}):
        with pytest.raises(StorageError):
            verify_clean_closed_carry_manifest(
                tmp_path, replace(manifest, **flags), limits=EvidenceBudgetLimits()
            )
    intervening = CausalFeatureDeliveryStore(tmp_path)
    intervening.close()
    actual = (tmp_path / "state/causal-feature-delivery.json").read_bytes()
    with pytest.raises(StorageError):
        verify_clean_closed_carry_manifest(tmp_path, manifest, limits=EvidenceBudgetLimits())
    assert (tmp_path / "state/causal-feature-delivery.json").read_bytes() == actual
