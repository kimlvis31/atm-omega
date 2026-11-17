"""Original default-provider rejected outcomes remain intact across retirement."""

from dataclasses import replace

import pytest
from test_causal_historical_carry_host import create, registry, request, terminal
from test_causal_noeffect_host import release_pin

from omega.domain.errors import StorageError, ValidationError
from omega.domain.inference import encoded
from omega.runtime.causal_generation_outcomes import covered_rejected_outcome
from omega.runtime.causal_recovery_proofs import retired_generation_carry_proof
from omega.runtime.causal_recovery_records import FeatureMutationUnknown


def rejected(host, monkeypatch):
    actor = host._current.actor
    original_ack = actor.ack_feature_settlement

    def reject(_):
        raise ValidationError("synthetic original pre-effect rejection")

    def stop_before_ack(ref, ack, **options):
        actor.close()
        return original_ack(ref, ack, **options)

    monkeypatch.setattr(actor.external_features, "_generation_arguments_actor", reject)
    monkeypatch.setattr(actor, "ack_feature_settlement", stop_before_ack)
    return actor


def test_actual_dead_rejection_reconciles_then_carries_original_pin_and_closes(
    tmp_path, monkeypatch
):
    host, saved = create(tmp_path, reservation=None)
    before = registry(tmp_path)
    actor = rejected(host, monkeypatch)
    try:
        with pytest.raises(FeatureMutationUnknown) as caught:
            release_pin(host, saved)
        ref = caught.value.ref
        outcome = actor.read_feature_settlement(ref, host_token=host._host_token)
        assert outcome.phase == "rejected" and outcome.result_json is None
        assert host.reconcile_mutation(ref)
        assert registry(tmp_path) == before
        host.submit_recovery("carry-original-rejection", request(host))
        result = terminal(host, "carry-original-rejection")
        assert result["state"] == "succeeded", result
        assert host._current.context.generation == 1
        assert host.external_features.status().held
        assert host.external_features.settlement_pin_documents() == (
            encoded(saved["pin"].document()),
        )
        assert host._current.actor.external_features.contains_pin(saved["pin"])
        assert actor.generation_settlement_snapshot().outcomes == (outcome,)
        after = registry(tmp_path)
        for key in ("records", "bytes", "pins", "reservations", "latest"):
            assert after[key] == before[key]
    finally:
        assert host.close()["durable"]


def test_rejection_coverage_keeps_all_original_identity_and_uncertainty_guards(
    tmp_path, monkeypatch
):
    host, saved = create(tmp_path, reservation=None)
    actor = rejected(host, monkeypatch)
    try:
        with pytest.raises(FeatureMutationUnknown) as caught:
            release_pin(host, saved)
        assert host.reconcile_mutation(caught.value.ref)
        context = host._current.context
        settlement = actor.generation_settlement_snapshot()
        outcome = settlement.outcomes[0]
        coverage = tuple(host._terminal_coverage.items())
        assert covered_rejected_outcome(context, settlement, outcome, coverage)
        assert not covered_rejected_outcome(context, settlement, outcome, ())
        assert not covered_rejected_outcome(context, settlement, outcome, coverage * 2)
        assert not covered_rejected_outcome(
            context, settlement, outcome, ((outcome.ref.sha256, "0" * 64),)
        )
        for changed in (
            replace(outcome, phase="unknown"),
            replace(outcome, error_code="other"),
            replace(outcome, arguments_json="{}"),
            replace(outcome, result_json="{}"),
            replace(outcome, recording_unknown=True),
            replace(outcome, capture_failed=True),
            replace(outcome, last_exact_ack=replace(outcome.last_exact_ack)),
            replace(outcome, current_write_confirmed=False),
        ):
            # Even recomputing a forged digest cannot repair original authority.
            forged = ((changed.ref.sha256, changed.sha256),)
            assert not covered_rejected_outcome(context, settlement, changed, forged)
        with pytest.raises(StorageError):
            retired_generation_carry_proof(
                context,
                actor.retirement_snapshot(),
                actor.actor_join_observation(timeout_s=0),
                settlement,
                (),
            )
        assert (
            retired_generation_carry_proof(
                context,
                actor.retirement_snapshot(),
                actor.actor_join_observation(timeout_s=0),
                settlement,
                coverage,
            ).terminal_coverage
            == coverage
        )
    finally:
        assert host.close()["durable"]
