"""Portable pure proofs do not imply live provider membership or authorization."""

from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.domain.errors import ValidationError
from omega.domain.external_features import (
    ExternalFeatureBinding,
    ExternalFeatureDelivery,
    ExternalFeaturePin,
    ExternalFeatureSnapshot,
    exact_decimal_text,
    semantic_candle_sha256,
)
from omega.domain.inference import digest
from omega.domain.models import Candle, Instrument, Market

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def candle():
    return Candle(
        BTC,
        600000,
        660000,
        D("100.123456789012345678901"),
        D("102"),
        D("99"),
        D("101"),
        D("2.123456789012345678901"),
    )


def binding():
    return ExternalFeatureBinding(
        "causal_nn", "deployment", "stream", BTC, "a" * 64, "b" * 64, ("NN.score",)
    )


def snapshot(**changes):
    values = {
        "binding": binding(),
        "core_epoch": "core",
        "deployment_epoch": "deployment",
        "stream_epoch": "epoch",
        "request_id": "c" * 64,
        "input_sha256": "d" * 64,
        "input_frame_sha256": "e" * 64,
        "receipt_sha256": "f" * 64,
        "evidence_sha256": "1" * 64,
        "candle_sha256": semantic_candle_sha256(candle()),
        "open_time_ms": 600000,
        "decision_at_ms": 660010,
        "accepted_at_ms": 660020,
        "valid_until_ms": 661000,
        "disposition": "ready",
        "values": (("NN.score", D("0.2500")),),
    }
    return ExternalFeatureSnapshot(**(values | changes))


def delivery(value=None, **changes):
    value = value or snapshot()
    return ExternalFeatureDelivery(
        **(
            {
                "binding": value.binding,
                "disposition": value.disposition,
                "snapshot": value,
                "core_epoch": "core",
                "deployment_epoch": "deployment",
                "stream_epoch": "epoch",
                "candle_sha256": value.candle_sha256,
                "open_time_ms": value.open_time_ms,
                "decision_at_ms": 660030,
                "observed_at_ms": 660040,
                "reason": "committed",
            }
            | changes
        )
    )


def test_pure_frozen_roundtrip_copies_without_callbacks_or_runtime_handles():
    values = [
        binding(),
        snapshot(),
        delivery(),
        ExternalFeaturePin(
            "causal_nn",
            "consumer",
            "2" * 64,
            binding().binding_sha256,
            snapshot().snapshot_sha256,
            660050,
        ),
    ]
    for value in values:
        assert type(value).parse(value.document()) == value
        assert deepcopy(value) == value
        with pytest.raises(FrozenInstanceError):
            value.new_field = "mutable"


def test_semantic_candle_exactness_ignores_decimal_scale_without_context_rounding():
    original = candle()
    equivalent = replace(
        original,
        open=D("100.12345678901234567890100"),
        high=D("102.000"),
        volume=D("2.123456789012345678901000"),
    )
    with localcontext() as context:
        context.prec = 2
        assert semantic_candle_sha256(original) == semantic_candle_sha256(equivalent)
        assert exact_decimal_text(D("100.00")) == exact_decimal_text(D("1E2")) == "1e2"
        assert exact_decimal_text(D("-0E999999999")) == "0"
        assert (
            snapshot().snapshot_sha256
            == replace(snapshot(), values=(("NN.score", D(".25")),)).snapshot_sha256
        )
    assert semantic_candle_sha256(original) != semantic_candle_sha256(
        replace(original, volume=D("2.123456789012345678902"))
    )
    assert not snapshot().matches_candle(replace(original, close=D("100.9")))
    assert snapshot().matches_candle(equivalent)
    assert delivery().matches_candle(equivalent)


@pytest.mark.parametrize(
    "changes",
    [
        {"fields": ("score",)},
        {"fields": ("NN.score", "NN.score")},
        {"fields": ["NN.score"]},
        {"producer_sha256": "version-name"},
        {"instrument": BTC.key},
        {"fields": ([],)},
    ],
)
def test_binding_rejects_unregistered_field_shape_or_mutable_configuration(changes):
    with pytest.raises(ValidationError):
        replace(binding(), **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"values": (("NN.other", D(".25")),)},
        {"values": (("NN.score", 0.25),)},
        {"values": (("NN.score", D("NaN")),)},
        {"values": (("NN.score", D("1E101")),)},
        {"accepted_at_ms": True},
        {"accepted_at_ms": 660000},
        {"valid_until_ms": 660020},
        {"open_time_ms": 600001},
        {"disposition": "pending"},
        {"request_id": "not-a-hash"},
    ],
)
def test_snapshot_rejects_noncausal_or_nonexact_values(changes):
    with pytest.raises(ValidationError):
        snapshot(**changes)


def test_financial_cutoff_and_current_observation_both_gate_ready_delivery():
    with pytest.raises(ValidationError):
        delivery(decision_at_ms=660019)
    with pytest.raises(ValidationError):
        delivery(observed_at_ms=661000)
    with pytest.raises(ValidationError):
        delivery(core_epoch="retired")
    unavailable = delivery(snapshot=None, disposition="pending", reason="not_available_at_cutoff")
    assert unavailable.snapshot is None
    assert ExternalFeatureDelivery.parse(unavailable.document()) == unavailable
    expired = delivery(disposition="expired", observed_at_ms=661000)
    assert expired.snapshot == snapshot()
    with pytest.raises(ValidationError):
        delivery(disposition="expired")
    with pytest.raises(ValidationError):
        delivery(disposition="unavailable")  # No values disclosed by a missing authority.


def test_no_signal_is_normal_committed_state_without_values():
    value = snapshot(disposition="no_signal", values=())
    assert delivery(value).disposition == "no_signal"
    assert ExternalFeatureSnapshot.parse(value.document()) == value
    with pytest.raises(ValidationError):
        snapshot(disposition="no_signal")


def test_field_capacity_preserves_legacy_nn_maximum_without_renaming():
    from omega.domain.inference import InferenceOutputBinding

    name = "NN." + "a" * 124
    assert InferenceOutputBinding(name, ("values", 0)).name == name
    assert replace(binding(), fields=(name,)).fields == (name,)
    with pytest.raises(ValidationError):
        replace(binding(), fields=("NN." + "a" * 126,))


def test_pin_idempotent_identity_is_independent_of_new_attempt_timestamp():
    first = ExternalFeaturePin(
        "causal_nn",
        "consumer",
        "2" * 64,
        binding().binding_sha256,
        snapshot().snapshot_sha256,
        660050,
    )
    assert replace(first, created_at_ms=660060).pin_id == first.pin_id
    assert replace(first, frame_ref="3" * 64).pin_id != first.pin_id
    assert ExternalFeaturePin.parse(first.document()) == first
    for durable in (False, 1, "true", None):
        with pytest.raises(ValidationError):
            replace(first, durable=durable)


@pytest.mark.parametrize("factory", [binding, snapshot, delivery])
def test_display_dictionary_or_mutated_document_cannot_parse_as_a_proof(factory):
    value = factory()
    document = value.document()
    document["private_path"] = "/not/a/proof"
    with pytest.raises(ValidationError):
        type(value).parse(document)
    with pytest.raises(ValidationError):
        type(value).parse({"values": {"NN.score": ".25"}})


def test_even_rehashed_snapshot_rejects_nonfinite_text_and_false_pin_ack():
    document = snapshot().document()
    document["values"] = [["NN.score", "NaN"]]
    body = {key: value for key, value in document.items() if key != "snapshot_sha256"}
    document["snapshot_sha256"] = digest(body)
    with pytest.raises(ValidationError):
        ExternalFeatureSnapshot.parse(document)
    pin = ExternalFeaturePin(
        "causal_nn",
        "consumer",
        "2" * 64,
        binding().binding_sha256,
        snapshot().snapshot_sha256,
        660050,
    ).document()
    pin["durable"] = False
    pin["proof_sha256"] = digest(
        {key: value for key, value in pin.items() if key != "proof_sha256"}
    )
    with pytest.raises(ValidationError):
        ExternalFeaturePin.parse(pin)
