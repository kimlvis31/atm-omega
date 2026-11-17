"""Actual proof/pin files with explicitly synthetic request and receipt metadata."""

from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.external_features import (
    ExternalFeatureBinding,
    ExternalFeaturePin,
    ExternalFeatureSnapshot,
    semantic_candle_sha256,
)
from omega.domain.inference import CAUSAL_SCOPE, digest, encoded
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.causal_feature_delivery_store import (
    CausalFeatureDeliveryStore,
    producer_fingerprint,
)
from omega.runtime.causal_nn_proof import exclusive_json

BINDINGS = [{"name": "NN.score", "path": ["values", 0]}]


def proof(root):
    instrument = Instrument("binance", Market.USDM, "BTCUSDT")
    candle = Candle(instrument, 600000, 660000, D(100), D(102), D(99), D(101), D("2.0"))
    rows = {
        "schema_id": "omega.ohlcv.1m.v1",
        "observations": [
            {
                "row": {
                    "instrument": instrument.key,
                    "open_time_ms": 600000,
                    "open": "100.00",
                    "high": "102",
                    "low": "99",
                    "close": "101",
                    "volume": "2.000",
                },
                "available_at_ms": 660010,
                "commit_sha256": "a" * 64,
                "origin": "committed_market",
                "quality": "observed",
            }
        ],
    }
    frame = {"instrument": instrument.key, "open_time_ms": 600000, "input_sha256": digest(rows)}
    request = {
        "scope": CAUSAL_SCOPE,
        "core_epoch": "core",
        "deployment_epoch": "deployment",
        "stream_epoch": "epoch",
        "stream": "stream",
        "decision_at_ms": 660020,
        "deadline_ms": 661000,
        "frame": frame,
        "model": {"synthetic": True},
        "output_bindings_sha256": digest(BINDINGS),
    }
    key = digest(request)
    evidence = {
        "scope": "committed_inference_input",
        "request_id": key,
        "request": request,
        "input_sha256": digest(rows),
        "snapshot": rows,
    }
    receipt = {
        "scope": CAUSAL_SCOPE,
        "request_id": key,
        "request_sha256": key,
        "status": "complete",
        "prediction_json": encoded({"values": [0.25], "state": None}),
    }
    binding = ExternalFeatureBinding(
        "causal_nn",
        "deployment",
        "stream",
        instrument,
        producer_fingerprint(request["model"], "c" * 64, request["output_bindings_sha256"]),
        "c" * 64,
        ("NN.score",),
    )
    snapshot = ExternalFeatureSnapshot(
        binding,
        "core",
        "deployment",
        "epoch",
        key,
        digest(rows),
        digest(frame),
        digest(receipt),
        digest(evidence),
        semantic_candle_sha256(candle),
        600000,
        660020,
        660030,
        661000,
        "ready",
        (("NN.score", D(".25")),),
    )
    path = root / "input.json"
    exclusive_json(path, evidence)
    return snapshot, receipt, path


def recorded(root, **options):
    snapshot, receipt, path = proof(root)
    store = CausalFeatureDeliveryStore(root, **options)
    store.record(
        snapshot, input_ref=path.relative_to(root), receipt=receipt, output_bindings=BINDINGS
    )
    return store, snapshot


def pin_for(snapshot, **changes):
    return ExternalFeaturePin(
        **(
            {
                "provider_id": "causal_nn",
                "consumer_id": "consumer",
                "frame_ref": "d" * 64,
                "binding_sha256": snapshot.binding.binding_sha256,
                "snapshot_sha256": snapshot.snapshot_sha256,
                "created_at_ms": 660040,
            }
            | changes
        )
    )


def test_real_snapshot_membership_pin_retry_and_consumed_receipt_never_delete_evidence(tmp_path):
    store, snapshot = recorded(tmp_path)
    try:
        assert store.load_snapshot(snapshot.snapshot_sha256) == snapshot
        first = store.pin(pin_for(snapshot))
        before = store.data["records"], store.data["bytes"]
        assert store.pin(pin_for(snapshot, created_at_ms=660050)) == first
        assert before == (store.data["records"], store.data["bytes"])
        assert store.load_pins() == {first.pin_id: first}
        store.release(first, "e" * 64)
        assert store.load_pins() == {}
        store.release(first, "e" * 64)
        assert store.pin(first) == first and not store.load_pins()
        assert store.load_snapshot(snapshot.snapshot_sha256) == snapshot
        assert (tmp_path / "input.json").exists()
        with pytest.raises(ValidationError):
            store.release(first, "f" * 64)
    finally:
        store.close()


def test_pin_actual_post_write_ack_loss_reopens_same_membership_without_duplicate_charge(tmp_path):
    store, snapshot = recorded(tmp_path)
    pin = pin_for(snapshot)
    save = store.store.save_snapshot

    def lose_ack(raw, **kwargs):
        from omega.domain.inference import decoded

        ack = save(raw, **kwargs)
        return replace(ack, durable=False) if pin.pin_id in decoded(raw)["pins"] else ack

    store.store.save_snapshot = lose_ack
    with pytest.raises(StorageError):
        store.pin(pin)
    store.close()
    reopened = CausalFeatureDeliveryStore(tmp_path)
    try:
        before = reopened.data["records"], reopened.data["bytes"]
        assert reopened.pin(replace(pin, created_at_ms=660060)) == pin
        assert reopened.load_pins() == {pin.pin_id: pin}
        assert before == (reopened.data["records"], reopened.data["bytes"])
    finally:
        reopened.close()


def test_capacity_and_corrupt_actual_proof_fail_closed_preserving_existing_bytes(tmp_path):
    store, snapshot = recorded(tmp_path, max_records=1)
    try:
        with pytest.raises(ValidationError, match="capacity"):
            store.preflight()
        with pytest.raises(ValidationError, match="capacity"):
            store.pin(pin_for(snapshot))
        path = store._path("snapshots", snapshot.snapshot_sha256)
        before = path.read_bytes()
        (tmp_path / "input.json").write_text("{}", encoding="utf-8")
        with pytest.raises(StorageError):
            store.load_snapshot(snapshot.snapshot_sha256)
        assert path.read_bytes() == before
    finally:
        store.close()


def test_false_pin_or_modified_immutable_pin_cannot_become_membership(tmp_path):
    store, snapshot = recorded(tmp_path)
    try:
        pin = store.pin(pin_for(snapshot))
        path = store._path("pins", pin.pin_id)
        path.write_text("{}", encoding="utf-8")
        with pytest.raises((ValidationError, StorageError)):
            store.load_pins()
        assert path.read_text() == "{}"
    finally:
        store.close()


def test_recorded_values_mapping_semantic_candle_and_filename_must_match_actual_evidence(tmp_path):
    snapshot, receipt, path = proof(tmp_path)
    store = CausalFeatureDeliveryStore(tmp_path)
    try:
        for forged in (
            replace(snapshot, values=(("NN.score", D(".26")),)),
            replace(snapshot, candle_sha256="f" * 64),
        ):
            with pytest.raises(StorageError):
                store.record(
                    forged,
                    input_ref=path.relative_to(tmp_path),
                    receipt=receipt,
                    output_bindings=BINDINGS,
                )
        assert store.data["records"] == 0
        store.record(
            snapshot,
            input_ref=path.relative_to(tmp_path),
            receipt=receipt,
            output_bindings=BINDINGS,
        )
        actual = store._path("snapshots", snapshot.snapshot_sha256)
        false_key = "0" * 64
        fake_path = store._path("snapshots", false_key)
        fake_path.write_bytes(actual.read_bytes())
        with pytest.raises(StorageError, match="source/receipt"):
            store.load_snapshot(false_key)
        assert actual.read_bytes() == fake_path.read_bytes()
    finally:
        store.close()
