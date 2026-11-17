"""Pure cold classification vectors from real file-actor observations.

Root ownership is a synthetic input here. These tests never grant a factory
permission or prove a real crashed child's death; Host integration owns that.
"""

from copy import deepcopy
from dataclasses import asdict, replace
from hashlib import sha256

import pytest

from omega.domain.errors import ValidationError
from omega.domain.inference import encoded
from omega.runtime.causal_cold_adoption import classify_interrupted_cold_adoption
from omega.runtime.causal_generation_outcomes import GenerationContext
from omega.runtime.causal_historical_carry import read_default_carry_heads
from omega.runtime.causal_inference_service import CausalInferenceService
from omega.runtime.causal_recovery_proofs import retired_generation_proof
from omega.runtime.causal_startup_policy import (
    ManualStartupCertificate,
    manual_startup_policy_sha256,
)


def observation_sha(document):
    return sha256(encoded(document, maximum=2 * 1024 * 1024).encode()).hexdigest()


@pytest.fixture
def vector(tmp_path):
    context = GenerationContext("old-core", 0, "old-actor", "old-host")
    old = CausalInferenceService(
        tmp_path,
        core_epoch="old-core",
        training_job_provider=lambda _: None,
        generation_context=context,
    )
    old.start()
    try:
        certificate = old.manual_startup_certificate()
        assert type(certificate) is ManualStartupCertificate
        assert old.owner is None and old.source is None and old._unattached_worker is None
        assert certificate.physical_close_claimed is False
        assert certificate.inference_child_started is False
    finally:
        old.close()
    retired, joined, settlement = (
        old.retirement_snapshot(),
        old.actor_join_observation(timeout_s=0),
        old.generation_settlement_snapshot(),
    )
    proof = retired_generation_proof(context, retired, joined, settlement)
    observations = {
        "original_retirement": asdict(retired),
        "original_join": asdict(joined),
        "original_settlement": settlement.document(),
    }
    candidate = CausalInferenceService(
        tmp_path,
        core_epoch="old-core",
        training_job_provider=lambda _: None,
        generation_context=GenerationContext("old-core", 1, "candidate", "old-host"),
    )
    candidate.start()
    try:
        certificate = candidate.manual_startup_certificate()
        assert type(certificate) is ManualStartupCertificate
        journal, delivery = read_default_carry_heads(tmp_path)
        # Preserve observed startup DATA only, then really close the actor. Never
        # roll the actual files back or pretend a current writer has disappeared.
        document = {
            "phase": "candidate_prepared",
            "core_epoch": "old-core",
            "generation": 0,
            "actor_token": "old-actor",
            "candidate_actor_token": "candidate",
            "candidate_startup_ack": asdict(candidate.retirement_snapshot().last_journal_ack),
            "candidate_startup_certificate": certificate.document(),
            "proof": proof.document(),
        }
        data = {
            "host_document": deepcopy(document),
            "journal_envelope": deepcopy(journal),
            "delivery_envelope": deepcopy(delivery),
            "current_core_epoch": "new-core",
            "expected_policy_sha256": manual_startup_policy_sha256(),
            "current_root_owned": True,
            "original_observations": deepcopy(observations),
        }
    finally:
        candidate.close()
    return data


@pytest.mark.parametrize("phase", ["candidate_prepared", "restored_manual"])
def test_untouched_default_manual_startup_is_only_data_eligible(vector, phase):
    vector["host_document"]["phase"] = phase
    before = deepcopy(vector)
    observed = classify_interrupted_cold_adoption(**vector)
    assert observed.eligible, observed.blockers
    assert observed.factory_authority is False and observed.physical_close_claimed is False
    assert observed.candidate_generation == 1 and observed.candidate_actor_token == "candidate"
    assert observed.document()["requires_confirmation"] is True
    assert vector == before
    with pytest.raises(ValidationError):
        replace(observed, factory_authority=True)


@pytest.mark.parametrize(
    "mutation",
    [
        "intent",
        "proof_pending",
        "fault_barrier",
        "root",
        "integer_root",
        "policy",
        "journal",
        "delivery",
        "same_core",
        "token",
        "missing_observations",
        "unconfirmed_retirement",
        "forged_authority",
        "boolean_generation",
        "unhashable_phase",
    ],
)
def test_changed_or_unconfirmed_cold_inputs_remain_held(vector, mutation):
    if mutation in {"intent", "proof_pending", "fault_barrier"}:
        vector["host_document"]["phase"] = mutation
    elif mutation == "root":
        vector["current_root_owned"] = False
    elif mutation == "integer_root":
        vector["current_root_owned"] = 1
    elif mutation == "policy":
        vector["expected_policy_sha256"] = "f" * 64
    elif mutation in {"journal", "delivery"}:
        envelope = vector[mutation + "_envelope"]
        envelope["revision"] += 1
        envelope["snapshot"]["revision"] += 1
        envelope["snapshot_sha256"] = observation_sha(envelope["snapshot"])
    elif mutation == "same_core":
        vector["current_core_epoch"] = "old-core"
    elif mutation == "token":
        vector["host_document"]["candidate_actor_token"] = "another-candidate"
    elif mutation == "missing_observations":
        vector["original_observations"] = None
    elif mutation in {"unconfirmed_retirement", "forged_authority"}:
        record = vector["original_observations"]["original_retirement"]
        record[
            "physical_confirmed"
            if mutation == "unconfirmed_retirement"
            else "actor_recovery_authorized"
        ] = mutation == "forged_authority"
        vector["host_document"]["proof"]["retirement_sha256"] = observation_sha(record)
    elif mutation == "boolean_generation":
        vector["host_document"]["generation"] = True
    else:
        vector["host_document"]["phase"] = []
    result = classify_interrupted_cold_adoption(**vector)
    assert not result.eligible and result.blockers
    assert result.factory_authority is False and result.automatic_inference_started is False


def test_custom_worker_startup_has_no_default_policy_certificate(tmp_path):
    actor = CausalInferenceService(
        tmp_path,
        core_epoch="core",
        training_job_provider=lambda _: None,
        generation_context=GenerationContext("core", 0, "actor", "host"),
        worker_factory=lambda *_args: pytest.fail("startup must not construct a worker"),
    )
    actor.start()
    try:
        assert actor.manual_startup_certificate() is None
    finally:
        actor.close()


def test_policy_fingerprint_ignores_location_and_line_endings_but_binds_later_helpers(
    tmp_path,
    monkeypatch,
):
    import omega.runtime.causal_startup_policy as policy

    values = []
    for index, ending in enumerate(("\n", "\r\n")):
        package = tmp_path / str(index) / "omega"
        for name in (
            "runtime/causal_a.py",
            "runtime/control.py",
            "domain/inference.py",
            "domain/external_features.py",
            "domain/models.py",
        ):
            path = package / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(("value = 1" + ending).encode())
        monkeypatch.setattr(policy, "__file__", str(package / "runtime/causal_startup_policy.py"))
        values.append(policy.manual_startup_policy_sha256())
    assert values[0] == values[1]
    (package / "runtime/causal_later.py").write_text("value = 2\n", encoding="utf-8")
    assert policy.manual_startup_policy_sha256() != values[1]
