"""Real actor/gate/file authority; source and inference worker are explicit fakes."""

import threading
import time
from dataclasses import replace
from decimal import Decimal as D

import pytest
import test_causal_inference_service as actor_fixture
from test_causal_inference_service import activate, command, create, predict, reset, terminal, wait

from omega.domain.errors import StorageError, TradingBlocked
from omega.domain.external_features import ExternalFeaturePin, ExternalFeatureSnapshot
from omega.domain.inference import encoded
from omega.domain.models import Candle, Instrument, Market


def test_partial_no_signal_commits_actual_state_and_restorable_authority(tmp_path, monkeypatch):
    state = {
        "clock": [660150],
        "workers": [],
        "events": [],
        "auto": True,
        "provider_calls": 0,
        "source_calls": 0,
        "verify_calls": 0,
        "no_signal": True,
    }
    monkeypatch.setattr(
        actor_fixture,
        "BINDINGS",
        [
            {"name": "NN.score", "path": ["values", 0]},
            {"name": "NN.other", "path": ["values", 1]},
        ],
    )
    original = actor_fixture.Worker.receipt

    def partial(worker, key):
        receipt = original(worker, key)
        if receipt is None:
            return None
        import json

        payload = json.loads(receipt.prediction_json)
        payload["values"] = [None, 0.25]
        return replace(receipt, prediction_json=encoded(payload))

    monkeypatch.setattr(actor_fixture.Worker, "receipt", partial)
    service, state = create(tmp_path, monkeypatch, state=state)
    try:
        deployment = activate(
            service, contract=replace(actor_fixture.CONTRACT, allow_no_signal=True)
        )
        assert reset(service, deployment)["state"] == "complete"
        predict(service, deployment)
        operation = terminal(service, "predict")
        assert operation["state"] == "complete" and operation["result"]["no_signal"] is True
        provider = service.external_features
        binding = provider.list_bindings()[0]
        bar = Candle(binding.instrument, 600000, 660000, D(100), D(102), D(99), D(101), D(2))
        delivery = provider.lookup(binding, bar, decision_at_ms=state["clock"][0])
        assert delivery.disposition == "no_signal" and delivery.snapshot.values == ()
        assert provider.store.load_snapshot(delivery.snapshot.snapshot_sha256) == delivery.snapshot
        assert service.owner.gate.previous_state_json("stream") == "[1]"
        assert reset(service, deployment, key="preserve", policy="preserve")["state"] == "complete"
        state["clock"][0] += 60000
        predict(service, deployment, key="next", opened=660000)
        assert terminal(service, "next")["state"] == "complete"
        assert service.owner.gate.previous_state_json("stream") == "[2]"
        assert service.error is None
    finally:
        service.close()


def ready(root, monkeypatch, **options):
    service, state = create(root, monkeypatch, **options)
    deployment = activate(service)
    assert reset(service, deployment)["state"] == "complete"
    predict(service, deployment)
    assert terminal(service, "predict")["state"] == "complete"
    provider = service.external_features
    binding = provider.list_bindings()[0]
    candle = Candle(
        Instrument("binance", Market.USDM, "BTCUSDT"),
        600000,
        660000,
        D("100.00"),
        D(102),
        D(99),
        D(101),
        D("2.000"),
    )
    cutoff = state["clock"][0]
    delivery = provider.lookup(binding, candle, decision_at_ms=cutoff)
    assert delivery.disposition == "ready"
    return service, state, provider, binding, candle, cutoff, delivery.snapshot


def test_manual_actual_receipt_registers_typed_cache_pin_and_consumed_receipt(
    tmp_path, monkeypatch
):
    service, state, provider, binding, candle, cutoff, snapshot = ready(tmp_path, monkeypatch)
    try:
        assert snapshot.matches_candle(candle)
        assert (
            provider.describe(
                binding.provider_id, binding.instance_id, binding.stream, binding.instrument
            )
            == binding
        )
        before = state["source_calls"], state["verify_calls"]
        for _ in range(100):
            assert provider.lookup(binding, candle, decision_at_ms=cutoff).snapshot == snapshot
        assert before == (state["source_calls"], state["verify_calls"])
        pin = provider.pin("consumer", "a" * 64, snapshot, candle, decision_at_ms=cutoff)
        assert provider.authorize(pin, snapshot, candle, decision_at_ms=cutoff)
        assert provider.pin("consumer", "a" * 64, snapshot, candle, decision_at_ms=cutoff) == pin
        provider.release(pin, consumer_receipt_sha256="b" * 64)
        assert not provider.authorize(pin, snapshot, candle, decision_at_ms=cutoff)
        assert provider.store.load_snapshot(snapshot.snapshot_sha256) == snapshot
        assert provider.lookup(binding, candle, decision_at_ms=cutoff).snapshot == snapshot
    finally:
        service.close()


def test_parsed_proofs_or_display_values_cannot_create_actual_authority(tmp_path, monkeypatch):
    service, state, provider, binding, candle, cutoff, snapshot = ready(tmp_path, monkeypatch)
    try:
        original = ExternalFeatureSnapshot.parse(snapshot.document())
        forged = replace(original, values=(("NN.score", D(".99")),))
        with pytest.raises(TradingBlocked):
            provider.pin("consumer", "a" * 64, forged, candle, decision_at_ms=cutoff)
        invented = ExternalFeaturePin(
            "causal_nn",
            "unregistered-consumer",
            "a" * 64,
            binding.binding_sha256,
            snapshot.snapshot_sha256,
            cutoff,
        )
        assert not provider.authorize(
            ExternalFeaturePin.parse(invented.document()), original, candle, decision_at_ms=cutoff
        )
        pin = provider.pin("consumer", "a" * 64, original, candle, decision_at_ms=cutoff)
        assert not provider.authorize(
            replace(pin, created_at_ms=cutoff + 1), snapshot, candle, decision_at_ms=cutoff
        )
        changed = replace(candle, volume=D("2.001"))
        assert provider.lookup(binding, changed, decision_at_ms=cutoff).disposition == "unavailable"
        assert not provider.authorize(pin, snapshot, changed, decision_at_ms=cutoff)
        state["clock"][0] = snapshot.valid_until_ms
        assert provider.lookup(binding, candle, decision_at_ms=cutoff).disposition == "expired"
        assert not provider.authorize(pin, snapshot, candle, decision_at_ms=cutoff)
    finally:
        service.close()


def test_pin_inflight_timeout_retry_reuses_first_created_at_and_actual_membership(
    tmp_path, monkeypatch
):
    service, state, provider, binding, candle, cutoff, snapshot = ready(
        tmp_path, monkeypatch, ack_timeout_s=0.1
    )
    entered, release = threading.Event(), threading.Event()
    original = provider.store.pin
    outcomes, calls = [], []

    def delayed(pin):
        entered.set()
        assert release.wait(2)
        calls.append(pin)
        return original(pin)

    provider.store.pin = delayed
    try:
        with pytest.raises(StorageError, match="unconfirmed"):
            provider.pin("consumer", "a" * 64, snapshot, candle, decision_at_ms=cutoff)
        assert entered.is_set()
        state["clock"][0] += 1

        def retry():
            try:
                outcomes.append(
                    provider.pin("consumer", "a" * 64, snapshot, candle, decision_at_ms=cutoff)
                )
            except Exception as error:
                outcomes.append(error)

        thread = threading.Thread(target=retry)
        thread.start()
        time.sleep(0.02)
        release.set()
        thread.join(timeout=2)
        assert not thread.is_alive() and len(outcomes) == 1
        assert isinstance(outcomes[0], ExternalFeaturePin), outcomes
        assert outcomes[0].created_at_ms == cutoff
        assert len(calls) == 1
        assert provider.authorize(outcomes[0], snapshot, candle, decision_at_ms=cutoff)
    finally:
        release.set()
        service.close()


def test_entry_lease_serializes_pause_invalidation_without_model_or_file_io(tmp_path, monkeypatch):
    service, state, provider, binding, candle, cutoff, snapshot = ready(tmp_path, monkeypatch)
    pin = provider.pin("consumer", "a" * 64, snapshot, candle, decision_at_ms=cutoff)
    started, finished = threading.Event(), threading.Event()

    def pause():
        started.set()
        command(service, "pause", "pause", deployment_id=binding.instance_id)
        terminal(service, "pause")
        finished.set()

    thread = threading.Thread(target=pause)
    try:
        with provider.entry_lease(pin, snapshot, candle, decision_at_ms=cutoff) as allowed:
            assert allowed
            thread.start()
            assert started.wait(1)
            assert not finished.wait(0.03)
            # Financial submission would occur here. Do not call actor APIs,
            # pin/release or wait for a provider mutation under this lease.
        thread.join(timeout=3)
        assert finished.is_set()
        assert not provider.authorize(pin, snapshot, candle, decision_at_ms=cutoff)
    finally:
        service.close()


def test_pin_filesystem_failure_wakes_waiter_then_blocks_actual_authority(tmp_path, monkeypatch):
    service, state, provider, binding, candle, cutoff, snapshot = ready(tmp_path, monkeypatch)
    original = provider.store._put

    def unavailable(path, document):
        if path.parent.name == "pins":
            raise OSError("synthetic disk failure")
        return original(path, document)

    provider.store._put = unavailable
    with pytest.raises(StorageError):
        provider.pin("consumer", "a" * 64, snapshot, candle, decision_at_ms=cutoff)
    wait(lambda: service.closed)
    assert provider.lookup(binding, candle, decision_at_ms=cutoff).disposition == "unavailable"
    with pytest.raises(StorageError):
        service.close()


def test_snapshot_record_disk_failure_blocks_all_authority_after_gate_ack(tmp_path, monkeypatch):
    service, state = create(tmp_path, monkeypatch)
    deployment = activate(service)
    assert reset(service, deployment)["state"] == "complete"
    provider = service.external_features
    original = provider.store._put

    def unavailable(path, document):
        if path.parent.name == "snapshots":
            raise OSError("synthetic evidence write failure")
        return original(path, document)

    provider.store._put = unavailable
    predict(service, deployment)
    wait(lambda: service.closed)
    assert service.error is not None
    assert provider.list_bindings() == ()
    assert terminal(service, "predict")["state"] == "unknown"
    with pytest.raises(StorageError):
        service.close()
