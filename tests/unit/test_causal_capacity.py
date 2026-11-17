"""Startup budget/cache observations; actual files, explicitly fake model/source ports."""

import threading
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest
import test_causal_inference_service as actor
from test_causal_feature_delivery import ready
from test_causal_feature_delivery_store import BINDINGS, pin_for, proof, recorded

from omega.domain.errors import StorageError, ValidationError
from omega.domain.inference import encoded
from omega.runtime.causal_capacity import EvidenceBudgetLimits
from omega.runtime.causal_feature_delivery_store import RESERVE_BYTES, CausalFeatureDeliveryStore
from omega.runtime.causal_inference_service import CausalInferenceService


@pytest.mark.parametrize(
    "name,value",
    [
        ("max_bytes", True),
        ("max_bytes", 4194303),
        ("max_bytes", 8589934593),
        ("max_bytes", 536870912.0),
        ("max_records", False),
        ("max_records", 15),
        ("max_records", 1000001),
        ("max_records", "100000"),
        ("max_pins", 0),
        ("max_pins", 65537),
        ("max_pins", True),
        ("warning_percent", 0),
        ("warning_percent", 100),
        ("warning_percent", True),
    ],
)
def test_limits_reject_non_native_or_outside_startup_bounds(name, value):
    with pytest.raises(ValidationError, match=name):
        EvidenceBudgetLimits(**{name: value})


def test_limits_defaults_bounds_immutable_and_document_copy():
    value = EvidenceBudgetLimits()
    assert value.document() == {
        "max_bytes": 536870912,
        "max_records": 100000,
        "max_pins": 2048,
        "warning_percent": 80,
    }
    value.document()["max_pins"] = 1
    assert value.max_pins == 2048
    with pytest.raises(FrozenInstanceError):
        value.max_bytes = 1
    for options in (
        dict(max_bytes=4194304, max_records=16, max_pins=1, warning_percent=1),
        dict(max_bytes=8589934592, max_records=1000000, max_pins=65536, warning_percent=99),
    ):
        assert EvidenceBudgetLimits(**options).document() == options


def test_service_constructor_io_zero_and_prestart_unknown_counts(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("constructor/status must not read/create files")

    monkeypatch.setattr(Path, "mkdir", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    service = CausalInferenceService(
        tmp_path, core_epoch="core", training_job_provider=lambda key: None
    )
    status = service.snapshot()["evidence_storage"]
    assert status["state"] == "unavailable" and not status["ready"]
    assert status["charged_bytes"] is status["charged_records"] is status["active_pins"] is None
    assert status["limits"] == EvidenceBudgetLimits().document()
    assert service.close()["durable"] is True


@pytest.mark.parametrize("options", [{"evidence_limits": {}}, {"on_storage_warning": True}])
def test_service_requires_parsed_budget_and_callable_warning(tmp_path, options):
    with pytest.raises(ValidationError):
        CausalInferenceService(
            tmp_path, core_epoch="core", training_job_provider=lambda key: None, **options
        )


def test_default_explicit_budget_keeps_original_registry_and_proof_bytes(tmp_path):
    documents = []
    for name, options in (("old", {}), ("new", {"limits": EvidenceBudgetLimits()})):
        root = tmp_path / name
        root.mkdir()
        snapshot, receipt, input_path = proof(root)
        store = CausalFeatureDeliveryStore(root, **options)
        try:
            assert store.store.read() == encoded(store.data)
            assert set(store.data) == {
                "scope",
                "schema_version",
                "revision",
                "records",
                "bytes",
                "latest",
                "pins",
                "reservations",
            }
            store.record(
                snapshot,
                input_ref=input_path.relative_to(root),
                receipt=receipt,
                output_bindings=BINDINGS,
            )
            documents.append(
                (
                    store.store.read(),
                    store._path("snapshots", snapshot.snapshot_sha256).read_bytes(),
                )
            )
        finally:
            store.close()
    assert documents[0] == documents[1]


def test_tiny_store_override_stays_supported_and_full_does_not_delete(tmp_path):
    store, snapshot = recorded(tmp_path, max_records=1)
    try:
        document = store._path("snapshots", snapshot.snapshot_sha256)
        before = document.read_bytes()
        status = store.capacity_snapshot()
        assert status["state"] == "full" and status["charged_records"] == 1
        assert not status["accepts_new_reservation"]
        with pytest.raises(ValidationError, match="capacity"):
            store.preflight()
        assert (
            document.read_bytes() == before
            and store.load_snapshot(snapshot.snapshot_sha256) == snapshot
        )
    finally:
        store.close()


def test_uncertain_charge_stays_accounted_and_survives_restart(tmp_path):
    store = CausalFeatureDeliveryStore(tmp_path)
    store._charge("input:unknown", RESERVE_BYTES)
    status = store.capacity_snapshot()
    assert status["uncertain_reservations"] == 1
    assert status["uncertain_reserved_bytes"] == status["charged_bytes"] == RESERVE_BYTES
    store.close()
    reopened = CausalFeatureDeliveryStore(tmp_path)
    try:
        assert reopened.capacity_snapshot() == status
    finally:
        reopened.close()


def test_headroom_can_block_before_zero_bytes_and_uncertain_slots_are_not_free(tmp_path):
    limits = EvidenceBudgetLimits(max_bytes=4194304)
    store = CausalFeatureDeliveryStore(tmp_path, limits=limits)
    try:
        store._charge("input:first", RESERVE_BYTES)
        store._charge("input:second", 1)
        status = store.capacity_snapshot()
        assert status["charged_bytes"] == RESERVE_BYTES + 1
        assert status["remaining_bytes"] == RESERVE_BYTES - 1
        assert status["uncertain_reserved_bytes"] == RESERVE_BYTES + 1
        assert status["state"] == "full" and not status["accepts_new_reservation"]
        with pytest.raises(ValidationError, match="capacity"):
            store.preflight()
    finally:
        store.close()


@pytest.mark.parametrize("field,value", [("bytes", 9), ("records", 0)])
def test_restart_rejects_counters_omitting_uncertain_charges_without_rewriting(
    tmp_path, field, value
):
    store = CausalFeatureDeliveryStore(tmp_path)
    store._charge("input:unknown", 10)
    store.data[field] = value
    store._save()
    store.close()
    path = tmp_path / "state/causal-feature-delivery.json"
    before = path.read_bytes()
    with pytest.raises(StorageError, match="omit uncertain"):
        CausalFeatureDeliveryStore(tmp_path)
    assert path.read_bytes() == before


def test_restart_rejects_records_outside_consumed_allowance_preserving_registry(tmp_path):
    limits = EvidenceBudgetLimits(max_records=16, max_pins=1)
    store = CausalFeatureDeliveryStore(tmp_path)
    store.data["records"] = 18
    store._save()
    store.close()
    path = tmp_path / "state/causal-feature-delivery.json"
    before = path.read_bytes()
    with pytest.raises(StorageError, match="bounded membership"):
        CausalFeatureDeliveryStore(tmp_path, limits=limits)
    assert path.read_bytes() == before


def test_ack_loss_status_unknown_then_actual_restart_restores_counters(tmp_path):
    store = CausalFeatureDeliveryStore(tmp_path)
    save = store.store.save_snapshot

    def lost(*args, **kwargs):
        return replace(save(*args, **kwargs), durable=False)

    store.store.save_snapshot = lost
    with pytest.raises(StorageError, match="ACK"):
        store._charge("input:unknown", 100)
    view = store.capacity_snapshot()
    assert view["state"] == "unavailable"
    assert view["charged_bytes"] is view["uncertain_reservations"] is None
    store.close()
    reopened = CausalFeatureDeliveryStore(tmp_path)
    try:
        assert reopened.capacity_snapshot()["charged_bytes"] == 100
        assert reopened.capacity_snapshot()["uncertain_reservations"] == 1
    finally:
        reopened.close()


def test_consumption_extra_allowance_survives_default_restart_without_refund(tmp_path):
    store, snapshot = recorded(tmp_path, max_records=2)
    pin = store.pin(pin_for(snapshot))
    store.release(pin, "e" * 64)
    assert store.data["records"] == 3
    before = store.capacity_snapshot()
    store.close()
    reopened = CausalFeatureDeliveryStore(tmp_path, max_records=2)
    try:
        assert reopened.capacity_snapshot() == before
        assert before["state"] == "full" and before["charged_records"] == 3
        assert before["remaining_records"] == 0
        assert (
            reopened.load_pins() == {}
            and reopened.load_snapshot(snapshot.snapshot_sha256) == snapshot
        )
        with pytest.raises(ValidationError, match="capacity"):
            reopened.preflight()
    finally:
        reopened.close()


def test_lower_ordinary_budget_full_preserves_pin_but_below_allowance_is_rejected(tmp_path):
    store, snapshot = recorded(tmp_path)
    pin = store.pin(pin_for(snapshot))
    store.data["bytes"] = 4194305
    store._save()
    store.close()
    limits = EvidenceBudgetLimits(max_bytes=4194304)
    reopened = CausalFeatureDeliveryStore(tmp_path, limits=limits)
    try:
        assert reopened.capacity_snapshot()["state"] == "full"
        assert reopened.load_pins() == {pin.pin_id: pin}
        assert reopened.load_snapshot(snapshot.snapshot_sha256) == snapshot
        with pytest.raises(ValidationError, match="capacity"):
            reopened.preflight()
        reopened.data["bytes"] = limits.max_bytes + limits.max_pins * 2048 + 1
        reopened._save()
    finally:
        reopened.close()
    before = (tmp_path / "state/causal-feature-delivery.json").read_bytes()
    with pytest.raises(StorageError, match="bounded membership"):
        CausalFeatureDeliveryStore(tmp_path, limits=limits)
    assert (tmp_path / "state/causal-feature-delivery.json").read_bytes() == before


def test_lower_pin_limit_rejects_without_rewriting_existing_membership(tmp_path):
    store, snapshot = recorded(tmp_path)
    first = store.pin(pin_for(snapshot))
    store.pin(pin_for(snapshot, frame_ref="a" * 64))
    store.close()
    before = (tmp_path / "state/causal-feature-delivery.json").read_bytes()
    with pytest.raises(StorageError):
        CausalFeatureDeliveryStore(tmp_path, limits=EvidenceBudgetLimits(max_pins=1))
    assert (tmp_path / "state/causal-feature-delivery.json").read_bytes() == before
    reopened = CausalFeatureDeliveryStore(tmp_path)
    try:
        assert first.pin_id in reopened.load_pins()
    finally:
        reopened.close()


def test_startup_full_budget_rejects_new_input_before_any_source_read(tmp_path, monkeypatch):
    limits = EvidenceBudgetLimits(max_records=16)
    store = CausalFeatureDeliveryStore(tmp_path, limits=limits)
    for index in range(16):
        store._charge(f"input:unknown-{index}", 1)
    store.close()
    warnings = []
    service, state = actor.create(
        tmp_path, monkeypatch, evidence_limits=limits, on_storage_warning=warnings.append
    )
    try:
        actor.wait(lambda: bool(warnings))
        assert warnings == ["NN_EVIDENCE_CAPACITY_FULL"]
        deployment = actor.activate(service)
        assert actor.reset(service, deployment)["state"] == "complete"
        actor.predict(service, deployment)
        assert actor.terminal(service, "predict")["state"] == "failed"
        assert state["source_calls"] == 0
        assert service.snapshot()["evidence_storage"]["uncertain_reservations"] == 16
        assert service.snapshot()["evidence_storage"]["charged_records"] == 16
        assert service.error is None
        assert service.owner.gate.previous_state_json("stream") == "null"
    finally:
        service.close()


def test_actor_cache_only_get_warning_dedup_and_pin_ack_before_callback_lock(tmp_path, monkeypatch):
    fence = threading.Lock()
    warnings, critical = [], []

    def warning(code):
        with fence:
            warnings.append(code)

    service, state, provider, binding, candle, cutoff, snapshot = ready(
        tmp_path,
        monkeypatch,
        evidence_limits=EvidenceBudgetLimits(max_records=16, max_pins=1, warning_percent=1),
        on_storage_warning=warning,
        on_incident=critical.append,
    )
    try:
        actor.wait(lambda: len(warnings) == 1)
        assert warnings == ["NN_EVIDENCE_CAPACITY_WARNING"] and not critical
        status = service.snapshot()["evidence_storage"]
        assert status["state"] == "warning" and status["charged_records"] == 2
        with fence:
            pin = provider.pin("consumer", "d" * 64, snapshot, candle, decision_at_ms=cutoff)
        actor.wait(lambda: len(warnings) == 2)
        assert warnings[-1] == "NN_EVIDENCE_CAPACITY_FULL"
        assert service.snapshot()["evidence_storage"]["active_pins"] == 1
        provider.release(pin, consumer_receipt_sha256="e" * 64)
        assert service.snapshot()["evidence_storage"]["active_pins"] == 0
        assert len(warnings) == 2 and not critical and service.error is None
        with monkeypatch.context() as patch:

            def forbidden(*args, **kwargs):
                raise AssertionError("GET must stay cached")

            patch.setattr(provider.store, "capacity_snapshot", forbidden)
            patch.setattr(Path, "open", forbidden)
            for _ in range(3):
                view = service.snapshot()
                assert view["evidence_storage"]["charged_records"] == 4
                view["evidence_storage"]["charged_records"] = 0
            assert service.snapshot()["evidence_storage"]["charged_records"] == 4
    finally:
        service.close()
    assert service.snapshot()["evidence_storage"]["state"] == "unavailable"
    assert service.snapshot()["evidence_storage"]["charged_bytes"] is None


def test_recording_fault_masks_previously_known_budget_and_warning_failure_is_advisory(
    tmp_path, monkeypatch
):
    calls = []

    def warning(code):
        calls.append(code)
        raise RuntimeError("notification unavailable")

    service, _, _, _, _, _, _ = ready(
        tmp_path,
        monkeypatch,
        evidence_limits=EvidenceBudgetLimits(max_records=16, warning_percent=1),
        on_storage_warning=warning,
    )
    try:
        actor.wait(lambda: bool(calls))
        assert service.error is None
        assert service.snapshot()["evidence_storage"]["charged_bytes"] > 0
        # Fault is normally raised by the actor. No financial/model mutation is involved.
        service._fault("TEST_RECORDING_UNKNOWN")
        assert service.snapshot()["evidence_storage"]["charged_bytes"] is None
        assert service.snapshot()["evidence_storage"]["state"] == "unavailable"
    finally:
        with pytest.raises(StorageError):
            service.close()
