"""Synthetic causal headers and real module output shapes; no exchange/order access."""

import csv
import json
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path

import pytest

from omega.application.inference_gate import CausalInferenceGate
from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.domain.inference import (
    CAUSAL_SCOPE,
    CausalInferenceFrame,
    InferenceCommitAck,
    InferenceModelIdentity,
    InferenceOutputBinding,
    InferenceReceipt,
    decoded,
    digest,
    encoded,
)
from omega.domain.models import Instrument, Market

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def model(**changes):
    if changes.get("stateful") is False:
        changes.setdefault("state_schema_sha256", None)
    return replace(
        InferenceModelIdentity(
            "fixture.rnn",
            "a" * 64,
            "b" * 64,
            "model1",
            "c" * 64,
            540000,
            540100,
            540200,
            "d" * 64,
            raw_lookback=2,
            stateful=True,
            state_schema_sha256="e" * 64,
        ),
        **changes,
    )


def frame(index=10, **changes):
    return replace(
        CausalInferenceFrame(
            BTC,
            (index - 1) * 60000,
            index * 60000,
            (index + 1) * 60000,
            (index + 1) * 60000 + 100,
            digest({"fixture_minute": index}),
            2,
        ),
        **changes,
    )


def gate(*, active_model=None, bindings=None, **kwargs):
    owner = CausalInferenceGate(
        active_model or model(),
        bindings or (InferenceOutputBinding("NN.score", ("values", 0)),),
        core_epoch="core1",
        deployment_epoch="deployment1",
        **kwargs,
    )
    owner.open_stream("shared", BTC, stream_epoch="stream1")
    return owner


def request(owner, value=None, **changes):
    value = value or frame()
    return owner.request(
        "shared",
        value,
        **{
            "decision_at_ms": value.available_at_ms,
            "now_ms": value.available_at_ms + 50,
            "deadline_ms": value.close_time_ms + 10000,
        }
        | changes,
    )


def receipt(req, *, prediction=None, status="complete", completed_at_ms=None, **changes):
    return replace(
        InferenceReceipt(
            CAUSAL_SCOPE,
            req.request_id,
            req.request_id,
            completed_at_ms or req.issued_at_ms + 50,
            status,
            encoded(prediction if prediction is not None else {"values": [0.75], "state": [1, 2]}),
        ),
        **changes,
    )


def ack(transition, durable=True):
    return InferenceCommitAck(transition.revision, transition.snapshot_sha256, durable)


def commit(owner, req, result=None):
    transition = owner.stage(result or receipt(req), now_ms=req.issued_at_ms + 60)
    return owner.commit(transition, ack(transition), now_ms=req.issued_at_ms + 70)


def test_staging_publishes_nothing_and_one_durable_state_is_shared_by_consumers():
    owner = gate()
    req = request(owner)
    before = owner.snapshot()
    transition = owner.stage(receipt(req), now_ms=660210)
    assert owner.snapshot() == before
    assert owner.previous_state_json("shared") == "null"
    assert owner.lookup("shared", frame(), now_ms=660220) is None
    with pytest.raises(TradingBlocked, match="acknowledgement"):
        request(owner, frame(11))
    actual = owner.commit(transition, ack(transition), now_ms=660220)
    assert actual.values == (("NN.score", D("0.75")),)
    assert owner.previous_state_json("shared") == "[1,2]"
    snapshot = owner.snapshot()
    for _ in range(100):
        assert owner.lookup("shared", frame(), now_ms=660230) == actual
    assert owner.snapshot() == snapshot
    with pytest.raises(ProtocolError, match="already committed"):
        owner.commit(transition, ack(transition), now_ms=660230)
    with pytest.raises(ProtocolError, match="duplicate"):
        owner.stage(receipt(req), now_ms=660230)
    next_request = request(owner, frame(11))
    assert next_request.sequence == 2
    assert next_request.previous_state_sha256 == digest([1, 2])


def test_unknown_commit_never_advances_memory_and_actual_disk_candidate_requires_manual_reset():
    owner = gate()
    req = request(owner)
    transition = owner.stage(receipt(req), now_ms=660210)
    with pytest.raises(StorageError, match="unconfirmed"):
        owner.commit(transition, ack(transition, False), now_ms=660220)
    assert owner.previous_state_json("shared") == "null"
    assert owner.lookup("shared", frame(), now_ms=660230) is None
    with pytest.raises(TradingBlocked, match="recording"):
        request(owner, frame(11))
    # This candidate is what an actual durable read could find after a lost ACK.
    recovered = CausalInferenceGate.restore(
        transition.snapshot_json,
        core_epoch="core2",
        deployment_epoch="deployment2",
        expected_model=model(),
    )
    assert recovered.previous_state_json("shared") == "[1,2]"
    assert recovered.lookup("shared", frame(), now_ms=660230) is None
    with pytest.raises(TradingBlocked, match="recovery"):
        request(recovered, frame(11))
    with pytest.raises(ProtocolError):
        recovered.stage(receipt(req), now_ms=660230)
    recovered.open_stream(
        "shared", BTC, stream_epoch="stream2", initial_state_json="[1,2]", after_open_ms=600000
    )
    resumed = request(recovered, frame(11))
    assert resumed.core_epoch == "core2" and resumed.stream_epoch == "stream2"
    assert resumed.sequence == 1 and resumed.previous_state_sha256 == digest([1, 2])


def test_wrong_ack_and_known_slow_commit_never_publish_an_expired_feature():
    owner = gate()
    req = request(owner)
    transition = owner.stage(receipt(req), now_ms=660210)
    with pytest.raises(ProtocolError, match="acknowledgement"):
        owner.commit(transition, replace(ack(transition), snapshot_sha256="e" * 64), now_ms=660220)
    assert owner.previous_state_json("shared") == "null"
    assert owner.commit(transition, ack(transition), now_ms=req.deadline_ms) is None
    assert owner.previous_state_json("shared") == "[1,2]"
    assert owner.lookup("shared", frame(), now_ms=req.deadline_ms) is None
    assert request(owner, frame(11)).sequence == 2


def test_pending_request_is_idempotent_and_restart_does_not_replay_it():
    owner = gate()
    req = request(owner)
    snapshot = owner.snapshot()
    assert request(owner, now_ms=660190) == req
    assert owner.snapshot() == snapshot
    with pytest.raises(TradingBlocked, match="outstanding"):
        request(owner, frame(11))
    recovered = CausalInferenceGate.restore(
        snapshot, core_epoch="core2", deployment_epoch="dep2", expected_model=model()
    )
    assert decoded(recovered.snapshot())["pending"] == {}
    assert recovered.previous_state_json("shared") == "null"
    with pytest.raises(TradingBlocked):
        request(recovered)
    with pytest.raises(ProtocolError):
        recovered.stage(receipt(req), now_ms=660220)


@pytest.mark.parametrize(
    "changes",
    [
        {"origin": "retrospective"},
        {"origin": "monte_carlo"},
        {"origin": "interpolated"},
        {"quality": "partial"},
        {"quality": "missing"},
        {"quality": "unsupported"},
        {"coverage": D(".999")},
        {"observed_rows": 1},
        {"window_start_ms": 480000},
    ],
)
def test_repaired_research_gaps_and_incomplete_windows_never_become_causal_features(changes):
    owner = gate()
    before = owner.snapshot()
    with pytest.raises(TradingBlocked):
        request(owner, frame(**changes))
    assert owner.snapshot() == before


@pytest.mark.parametrize(
    "active_model,kwargs",
    [
        (
            model(
                training_cutoff_ms=600001, training_available_at_ms=600010, available_at_ms=600020
            ),
            {},
        ),
        (model(available_at_ms=660120), {}),
        (model(), {"decision_at_ms": 660050}),
        (model(), {"now_ms": 900000, "deadline_ms": 910000}),
    ],
)
def test_future_training_model_availability_and_old_data_are_blocked(active_model, kwargs):
    owner = gate(active_model=active_model)
    with pytest.raises(TradingBlocked):
        request(owner, **kwargs)
    assert decoded(owner.snapshot())["pending"] == {}


@pytest.mark.parametrize("status", ["failed", "unknown", "cancelled"])
def test_failed_unknown_and_cancelled_results_latch_stream_without_advancing_state(status):
    owner = gate()
    req = request(owner)
    with pytest.raises(TradingBlocked):
        owner.stage(receipt(req, status=status), now_ms=660220)
    assert owner.previous_state_json("shared") == "null"
    assert decoded(owner.snapshot())["streams"]["shared"]["health"] == "blocked"
    with pytest.raises(TradingBlocked):
        request(owner)


def test_timeout_and_late_result_require_reset_and_never_apply_new_state():
    owner = gate()
    req = request(owner)
    assert owner.expire(now_ms=req.deadline_ms) == (req.request_id,)
    assert owner.previous_state_json("shared") == "null"
    with pytest.raises(ProtocolError):
        owner.stage(receipt(req), now_ms=req.deadline_ms + 1)
    owner.open_stream("shared", BTC, stream_epoch="stream2")
    new = request(owner)
    assert new.request_id != req.request_id
    with pytest.raises(ProtocolError):
        owner.stage(receipt(req), now_ms=660220)
    assert commit(owner, new).values == (("NN.score", D(".75")),)


def test_stage_that_crosses_frame_freshness_is_blocked_before_deadline():
    owner = gate(max_frame_age_ms=1000)
    req = request(owner, now_ms=660900, deadline_ms=670000)
    with pytest.raises(TradingBlocked, match="fresh"):
        owner.stage(receipt(req, completed_at_ms=661100), now_ms=661110)
    assert decoded(owner.snapshot())["streams"]["shared"]["reason"] == "inference_input_stale"


@pytest.mark.parametrize(
    "prediction",
    [
        {"values": [True], "state": [1]},
        {"values": ["0.4"], "state": [1]},
        {"values": [None], "state": [1]},
        {"values": [], "state": [1]},
        {"values": [1e101], "state": [1]},
        {"values": [1]},
        {"values": [1], "state": [1], "trade_intent": "BUY"},
    ],
)
def test_invalid_or_ambiguous_module_outputs_do_not_publish_features(prediction):
    owner = gate()
    req = request(owner)
    with pytest.raises(ValidationError):
        owner.stage(receipt(req, prediction=prediction), now_ms=660220)
    assert owner.previous_state_json("shared") == "null"
    assert owner.lookup("shared", frame(), now_ms=660230) is None


def test_stateless_contract_and_nested_module_owned_output_mapping():
    owner = gate(
        active_model=model(stateful=False),
        bindings=(InferenceOutputBinding("NN.long_probability", ("values", 0, "probability")),),
    )
    req = request(owner)
    actual = commit(owner, req, receipt(req, prediction={"values": [{"probability": 0.125}]}))
    assert actual.values == (("NN.long_probability", D(".125")),)
    with pytest.raises(ValidationError, match="stateless"):
        owner.open_stream("bad", BTC, stream_epoch="s1", initial_state_json="[1]")


def test_model_binding_request_hash_and_full_frame_identity_prevent_cache_aliases():
    owner = gate()
    req = request(owner)
    actual = commit(owner, req)
    assert owner.lookup("shared", frame(available_at_ms=660101), now_ms=660230) is None
    assert owner.lookup("shared", frame(input_sha256="e" * 64), now_ms=660230) is None
    other = gate(bindings=(InferenceOutputBinding("NN.other", ("values", 0)),))
    other_req = request(other)
    assert req.request_id != other_req.request_id
    with pytest.raises(ProtocolError):
        other.stage(receipt(req), now_ms=660230)
    wrong_model = gate(active_model=model(checkpoint_sha256="e" * 64))
    assert request(wrong_model).request_id != req.request_id
    assert actual.input_sha256 == frame().input_sha256


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d.update(schema_version=True),
        lambda d: d.update(extra="unexpected"),
        lambda d: d["streams"]["shared"].update(state_sha256="e" * 64),
        lambda d: d["streams"]["shared"].update(last_open_ms=600001),
        lambda d: d["streams"]["shared"].update(receipt_sha256=None),
        lambda d: d["streams"]["shared"]["feature"].update(instrument="binance:usdm:ETHUSDT"),
        lambda d: d["bindings"][0].update(unexpected=True),
    ],
)
def test_restore_rejects_corruption_instead_of_reconstructing_state_or_success(mutation):
    owner = gate()
    commit(owner, request(owner))
    document = decoded(owner.snapshot())
    mutation(document)
    with pytest.raises((ValidationError, ProtocolError)):
        CausalInferenceGate.restore(
            encoded(document), core_epoch="new", deployment_epoch="dep", expected_model=model()
        )


def test_restore_rejects_changed_model_and_outstanding_epoch_corruption():
    owner = gate()
    req = request(owner)
    with pytest.raises(ProtocolError, match="model"):
        CausalInferenceGate.restore(
            owner.snapshot(),
            core_epoch="new",
            deployment_epoch="dep",
            expected_model=model(revision="newmodel"),
        )
    document = decoded(owner.snapshot())
    document["pending"][req.request_id]["stream_epoch"] = "forged"
    with pytest.raises(ProtocolError):
        CausalInferenceGate.restore(
            encoded(document), core_epoch="new", deployment_epoch="dep", expected_model=model()
        )


def test_declared_no_signal_commits_recurrent_state_without_making_a_trade_feature():
    owner = gate(active_model=model(allow_no_signal=True))
    req = request(owner)
    result = receipt(req, prediction={"values": [None], "state": [3, 4]})
    transition = owner.stage(result, now_ms=660210)
    assert owner.commit(transition, ack(transition), now_ms=660220) is None
    assert owner.previous_state_json("shared") == "[3,4]"
    assert owner.lookup("shared", frame(), now_ms=660230) is None
    snapshot = owner.snapshot()
    assert decoded(snapshot)["streams"]["shared"]["reason"] == "no_signal"
    restored = CausalInferenceGate.restore(
        snapshot,
        core_epoch="new",
        deployment_epoch="dep",
        expected_model=model(allow_no_signal=True),
    )
    assert restored.previous_state_json("shared") == "[3,4]"
    assert request(owner, frame(11)).previous_state_sha256 == digest([3, 4])


def test_no_signal_does_not_bypass_the_actual_commit_clock_check():
    owner = gate(active_model=model(allow_no_signal=True))
    req = request(owner)
    transition = owner.stage(
        receipt(req, prediction={"values": [None], "state": [3]}), now_ms=660210
    )
    with pytest.raises(ValidationError, match="backwards"):
        owner.commit(transition, ack(transition), now_ms=660200)
    assert owner.previous_state_json("shared") == "null"


def test_request_intent_recording_uncertainty_blocks_dispatch_and_cached_values():
    owner = gate()
    commit(owner, request(owner))
    request(owner, frame(11))
    previous = owner.previous_state_json("shared")
    owner.recording_uncertain()
    assert owner.previous_state_json("shared") == previous
    assert owner.lookup("shared", frame(), now_ms=660230) is None
    with pytest.raises(TradingBlocked, match="recording"):
        request(owner, frame(11))


def test_one_failed_stream_does_not_advance_or_stop_another_prepared_stream():
    owner = gate()
    owner.open_stream("second", BTC, stream_epoch="second-1")
    failed = request(owner)
    healthy = owner.request(
        "second", frame(), decision_at_ms=660100, now_ms=660150, deadline_ms=670000
    )
    owner.fail(failed.request_id, "model_failed")
    assert commit(owner, healthy).stream == "second"
    assert owner.previous_state_json("shared") == "null"
    assert owner.previous_state_json("second") == "[1,2]"


def test_state_schema_and_capacity_are_explicit_frozen_contracts():
    with pytest.raises(ValidationError):
        model(state_schema_sha256=None)
    owner = gate(max_streams=1)
    with pytest.raises(ValidationError, match="capacity"):
        owner.open_stream("second", BTC, stream_epoch="second1")
    commit(owner, request(owner))
    with pytest.raises(ProtocolError, match="model"):
        CausalInferenceGate.restore(
            owner.snapshot(),
            core_epoch="new",
            deployment_epoch="dep",
            expected_model=model(state_schema_sha256="f" * 64),
        )


@pytest.mark.parametrize("value", [True, -1, 1.0, "660000"])
def test_native_clock_types_and_field_mapping_are_not_coerced(value):
    with pytest.raises(ValidationError):
        frame(available_at_ms=value)
    with pytest.raises(ValidationError):
        gate().request("shared", frame(), decision_at_ms=660100, now_ms=value, deadline_ms=670000)


def test_real_prediction_session_envelope_is_offline_until_a_host_proves_a_separate_causal_request(
    tmp_path,
):
    from omega.research import demo_instance
    from omega.research.datasets import FIELDS, export_dataset
    from omega.research.neural import train
    from omega.research.prediction import PredictionSession

    source = tmp_path / "fixture.csv"
    with source.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        for i in range(60):
            writer.writerow(
                {
                    "instrument": BTC.key,
                    "open_time_ms": i * 60000,
                    "open": "100",
                    "high": "102",
                    "low": "99",
                    "close": "101",
                    "volume": str(i + 1),
                }
            )
    dataset = tmp_path / "dataset"
    export_dataset(source, dataset, {})
    training = tmp_path / "training"
    train(dataset, Path(demo_instance.__file__), training, {"config": {"epochs": 1}})
    session = PredictionSession(training, tmp_path / "prediction", trusted_module=True)
    try:
        actual = session.predict([0.5])
    finally:
        session.close()
    assert actual["scope"].startswith("offline_research")
    owner = gate(active_model=model(stateful=False))
    req = request(owner)
    with pytest.raises(ValidationError, match="host-wrapped"):
        receipt(req, scope=actual["scope"], prediction=actual["prediction"])
    # Synthetic host evidence exercises only the new data contract, not a claim
    # that this retrospective training fixture has real source-time evidence.
    result = commit(owner, req, receipt(req, prediction=actual["prediction"]))
    assert result.values[0][1] == D(str(actual["prediction"]["values"][0]))
    assert json.loads((training / "run.json").read_text())["state"] == "complete"
