"""Default no-child actors carry actual temporary immutable obligations.

Seeded request/receipt metadata is explicitly synthetic. No model, market source,
financial account, database or order is started by these seam cases.
"""

import json
import threading
import time
from dataclasses import asdict, replace
from decimal import Decimal
from types import SimpleNamespace

import pytest
from test_causal_feature_delivery_store import BINDINGS, pin_for, proof

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.inference import decoded, digest, encoded
from omega.domain.models import Candle
from omega.runtime.causal_inference_service import CausalInferenceService
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_recovery import CausalInferenceHost
from omega.runtime.causal_recovery_proofs import (
    RecoveryProofArchive,
    retired_generation_carry_proof,
    retired_generation_proof,
)
from omega.runtime.causal_recovery_records import RecoveryJournal

RECEIPT = "e" * 64


def wait_for(predicate, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(0.005)
    raise AssertionError("bounded carry host condition was not observed")


def request(host):
    view = host.recovery_snapshot()
    return {
        "core_epoch": view["core_epoch"],
        "expected_generation": view["generation"],
        "expected_control_revision": view["control_revision"],
        "expected_actor_token": view["actor_token"],
        "expected_retirement_sha256": view["retirement_sha256"],
        "state_policy": "reset",
        "confirm": True,
    }


def terminal(host, key):
    return wait_for(
        lambda: (
            row
            if (row := host.recovery_status(key))["state"] not in {"admitted", "running"}
            else None
        )
    )


def registry(root):
    return json.loads((root / "state" / "causal-feature-delivery.json").read_text())["snapshot"]


def create(root, *, reservation="snapshot", journal_factory=RecoveryJournal, candidate_hook=None):
    saved = {"factories": [], "candidate_hook": candidate_hook}

    class SeededActor(CausalInferenceService):
        def _publish(self):
            if not getattr(self, "_seed_done", False):
                self.ready = False
            return super()._publish()

        def _initialize(self):
            super()._initialize()
            snapshot, receipt, source = proof(self.root)
            store = self.external_features.store
            store.register_input(snapshot.request_id, source.relative_to(self.root))
            store.record(
                snapshot,
                input_ref=source.relative_to(self.root),
                receipt=receipt,
                output_bindings=BINDINGS,
            )
            pin = store.pin(pin_for(snapshot))
            if reservation == "snapshot":
                artifact = store._path("snapshots", snapshot.snapshot_sha256)
                store._charge("snapshot:" + snapshot.snapshot_sha256, artifact.stat().st_size)
            elif reservation == "unknown":
                store._charge("input:" + "f" * 64, 64)
            elif reservation is not None:
                raise AssertionError("unknown fixture reservation")
            saved.update(
                pin=pin,
                snapshot=snapshot,
                candle=Candle(
                    snapshot.binding.instrument,
                    600000,
                    660000,
                    Decimal(100),
                    Decimal(102),
                    Decimal(99),
                    Decimal(101),
                    Decimal("2.0"),
                ),
            )
            self.external_features._capture_generation_settlement_actor(sealed=False)
            self._cache_retirement(sealed=False)
            self._seed_done, self.ready = True, True
            self._publish()

    def factory(path, **options):
        generation = options["generation_context"].generation
        saved["factories"].append((generation, options.get("carry_install")))
        actor = (SeededActor if generation == 0 else CausalInferenceService)(path, **options)
        if generation and candidate_hook is not None:
            candidate_hook(actor)
        return actor

    host = CausalInferenceHost(
        root,
        core_epoch="core",
        training_job_provider=lambda _: None,
        actor_factory=factory,
        journal_factory=journal_factory,
    )
    host.start()
    # Represent the existing consumer's exact original membership; fixture writes
    # were performed by its original actor on the actual default store.
    binding, pin = host._current.facade_binding, saved["pin"]
    host.external_features._pins[host.external_features._pin_key(pin, 0)] = (binding, pin)
    return host, saved


def test_actual_old_pin_carries_release_only_without_model_or_account_resume(tmp_path):
    host, saved = create(tmp_path)
    original = host._current
    pin = saved["pin"]
    before = registry(tmp_path)
    paths = [
        tmp_path / "input.json",
        tmp_path / "artifacts/causal-feature-delivery/pins" / (pin.pin_id + ".json"),
        tmp_path / "artifacts/causal-feature-delivery/snapshots" / (pin.snapshot_sha256 + ".json"),
    ]
    immutable = {path: path.read_bytes() for path in paths}
    try:
        host.submit_recovery("carry", request(host))
        result = terminal(host, "carry")
        assert result["state"] == "succeeded", result
        assert result["result"]["historical_carry_available"]
        current = host._current
        assert current.context.generation == 1 and host.external_features.status().held
        assert current.actor.owner is None and current.actor.source is None
        assert current.actor.carry_acceptance_snapshot() is not None
        assert host.external_features.settlement_pin_documents() == (encoded(pin.document()),)
        assert not host.external_features.authorize(
            pin, saved["snapshot"], saved["candle"], decision_at_ms=660040
        )
        assert current.actor.external_features.contains_pin(pin)
        after = registry(tmp_path)
        for key in ("records", "bytes", "pins", "reservations", "latest"):
            assert after[key] == before[key]
        for path in paths:
            assert path.read_bytes() == immutable[path]
        old = host._history[0]
        with pytest.raises(StorageError):
            retired_generation_proof(original.context, old[1], old[2], old[3])
        assert retired_generation_carry_proof(original.context, old[1], old[2], old[3]) == old[4]
        host.external_features.release(pin, consumer_receipt_sha256=RECEIPT)
        consumed = registry(tmp_path)
        assert pin.pin_id not in consumed["pins"]
        assert consumed["reservations"] == before["reservations"]
        host.external_features.release(pin, consumer_receipt_sha256=RECEIPT)
        assert registry(tmp_path) == consumed
        with pytest.raises(ValidationError):
            host.external_features.release(pin, consumer_receipt_sha256="b" * 64)
        assert host._host_document["receipt"]["financial_resumed"] is False
    finally:
        assert host.close()["durable"]


def test_two_carries_preserve_full_original_pin_and_each_generation_acceptance(tmp_path):
    host, saved = create(tmp_path)
    pin = saved["pin"]
    try:
        manifests = []
        for index in range(2):
            key = "carry" + str(index)
            host.submit_recovery(key, request(host))
            result = terminal(host, key)
            assert result["state"] == "succeeded", result
            manifests.append(result["result"]["carry_manifest_sha256"])
            assert host._current.context.generation == index + 1
            assert host.external_features.settlement_pin_documents() == (encoded(pin.document()),)
            assert registry(tmp_path)["pins"][pin.pin_id]["proof_sha256"] == digest(pin.document())
        assert manifests[0] != manifests[1]
        host.external_features.release(pin, consumer_receipt_sha256=RECEIPT)
        assert host.external_features.settlement_pin_documents() == ()
        assert len(saved["factories"]) == 3
    finally:
        host.close()


def test_manifest_and_original_observations_are_only_in_checked_immutable_attachment(tmp_path):
    host, saved = create(tmp_path)
    try:
        host.submit_recovery("attachment", request(host))
        assert terminal(host, "attachment")["state"] == "succeeded"
        document = host._host_document
        archive = RecoveryProofArchive(tmp_path, host.evidence_limits)
        attachment = archive.read_attachment(document["original_observations_artifact_sha256"])
        assert set(attachment) == {
            "carry_manifest",
            "original_retirement",
            "original_join",
            "original_settlement",
        }
        physical = document["proof"]
        assert digest(attachment["original_retirement"]) == physical["retirement_sha256"]
        assert digest(attachment["original_join"]) == physical["join_sha256"]
        assert digest(attachment["original_settlement"]) == physical["settlement_sha256"]
        assert attachment["carry_manifest"]["pins"] == [saved["pin"].document()]
        assert "carry_manifest" not in document
        assert len(encoded(document).encode()) < 65536
        assert host.recovery_snapshot()["scope"] == "verified_historical_pin_carry"
    finally:
        host.close()


def test_unknown_reservation_stays_charged_and_explicitly_unknown(tmp_path):
    host, _ = create(tmp_path, reservation="unknown")
    before = registry(tmp_path)
    try:
        host.submit_recovery("charged", request(host))
        result = terminal(host, "charged")
        assert result["state"] == "succeeded", result
        after = registry(tmp_path)
        assert after["reservations"] == before["reservations"] == {"input:" + "f" * 64: 64}
        assert after["bytes"] == before["bytes"] and after["records"] == before["records"]
        assert host.recovery_snapshot()["last_carry_counts"]["unknown_reservations"] == 1
        attachment = RecoveryProofArchive(tmp_path, host.evidence_limits).read_attachment(
            host._host_document["original_observations_artifact_sha256"]
        )
        provenance = attachment["carry_manifest"]["provenance"]
        assert provenance["reservations"][0]["state"] == "unknown"
        assert host.external_features.status().held
    finally:
        host.close()


@pytest.mark.parametrize("kind", ["pin", "snapshot", "input"])
def test_changed_actual_old_evidence_stops_before_successor_factory(tmp_path, kind):
    host, saved = create(tmp_path)
    pin = saved["pin"]
    path = (
        tmp_path / "input.json"
        if kind == "input"
        else tmp_path
        / "artifacts/causal-feature-delivery"
        / ("pins" if kind == "pin" else "snapshots")
        / ((pin.pin_id if kind == "pin" else pin.snapshot_sha256) + ".json")
    )
    raw = path.read_bytes()
    try:
        path.write_bytes(raw + b" ")
        # Semantic equality is not corruption. Replace a required evidence field.
        body = json.loads(path.read_text())
        body["unrecognized_fixture_field"] = True
        path.write_text(json.dumps(body))
        host.submit_recovery("changed", request(host))
        assert terminal(host, "changed")["state"] == "unknown"
        assert len(saved["factories"]) == 1
    finally:
        path.write_bytes(raw)
        with pytest.raises(StorageError):
            host.close()


def test_copied_candidate_acceptance_is_not_actual_new_store_ack(tmp_path):
    def hook(actor):
        getter = actor.carry_acceptance_snapshot
        actor.carry_acceptance_snapshot = lambda: None if getter() is None else replace(getter())

    host, saved = create(tmp_path, candidate_hook=hook)
    try:
        host.submit_recovery("copied", request(host))
        assert terminal(host, "copied")["state"] == "unknown"
        assert len(saved["factories"]) == 2 and host._current.context.generation == 0
    finally:
        with pytest.raises(StorageError):
            host.close()


def test_actual_new_save_then_duck_ack_keeps_same_candidate_unknown(tmp_path, monkeypatch):
    active = []

    def hook(actor):
        active.append(actor)

    host, saved = create(tmp_path, candidate_hook=hook)
    save = FileInferenceStateStore.save_snapshot

    def duck(store, raw, *, expected_revision):
        ack = save(store, raw, expected_revision=expected_revision)
        if active and store.path.name == "causal-feature-delivery.json":
            return SimpleNamespace(**asdict(ack))
        return ack

    monkeypatch.setattr(FileInferenceStateStore, "save_snapshot", duck)
    try:
        host.submit_recovery("duck", request(host))
        assert terminal(host, "duck")["state"] == "unknown"
        assert len(saved["factories"]) == 2 and host.external_features.status().held
        with pytest.raises(TradingBlocked):
            host.submit_recovery("another", request(host))
    finally:
        with pytest.raises(StorageError):
            host.close()


def test_close_fence_wins_after_actual_carry_acceptance_before_install(tmp_path):
    entered, release = threading.Event(), threading.Event()

    class PausedPrepared(RecoveryJournal):
        def save_host(self, raw, *, expected_revision):
            ack = super().save_host(raw, expected_revision=expected_revision)
            if decoded(raw).get("phase") == "candidate_prepared":
                entered.set()
                assert release.wait(4)
            return ack

    host, saved = create(tmp_path, journal_factory=PausedPrepared)
    old, errors = host._current, []

    def close():
        try:
            host.close()
        except StorageError as error:
            errors.append(error)

    closer = threading.Thread(target=close)
    try:
        host.submit_recovery("closing", request(host))
        assert entered.wait(3)
        closer.start()
        wait_for(lambda: host.external_features.status().closing)
        release.set()
        closer.join(5)
        assert not closer.is_alive() and errors
        assert host._current is old and len(saved["factories"]) == 2
        assert host.recovery_status("closing")["state"] == "unknown"
    finally:
        release.set()
        if closer.ident is not None:
            closer.join(5)
        if host.thread.is_alive():
            with pytest.raises(StorageError):
                host.close()
