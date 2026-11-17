"""Automatic source events use real gate/files and explicit fake NN/source ports."""

import threading
import time
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D

import pytest
from test_causal_inference_service import (
    CONTRACT,
    JOB,
    activate,
    command,
    create,
    reset,
    wait,
)

from omega.domain.errors import ValidationError
from omega.domain.external_features import ExternalFeatureDelivery
from omega.domain.models import Candle, Instrument, Market


def candle(opened=600000, volume="2.000"):
    return Candle(
        Instrument("binance", Market.USDM, "BTCUSDT"),
        opened,
        opened + 60000,
        D("100.00"),
        D(102),
        D(99),
        D(101),
        D(volume),
    )


def setup(root, monkeypatch, *, state=None, no_signal=False, streams=1):
    state = state or {
        "clock": [660150],
        "workers": [],
        "events": [],
        "auto": True,
        "provider_calls": 0,
        "source_calls": 0,
        "verify_calls": 0,
        "allow_internal": True,
        "no_signal": no_signal,
    }
    service, state = create(root, monkeypatch, state=state)
    deployment = activate(
        service, contract=replace(CONTRACT, allow_no_signal=True) if no_signal else CONTRACT
    )
    for index in range(streams):
        assert (
            reset(service, deployment, stream=f"stream{index}", key=f"reset{index}")["state"]
            == "complete"
        )
    provider = service.external_features
    return service, state, provider, provider.list_bindings()


def settled(provider, binding, bar, state, expected="ready"):
    def read():
        delivery = provider.lookup(binding, bar, decision_at_ms=state["clock"][0])
        return delivery if delivery.disposition == expected else None

    return wait(read, seconds=8)


def test_offer_does_not_wait_for_financial_authority_lease(tmp_path, monkeypatch):
    service, state, provider, bindings = setup(tmp_path, monkeypatch)
    entered, release = threading.Event(), threading.Event()

    def hold():
        with provider.lock:
            entered.set()
            assert release.wait(3)

    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert entered.wait(2)
        started = time.monotonic()
        delivery = provider.offer_committed(bindings[0], candle())
        assert time.monotonic() - started < 0.2
        assert delivery.disposition == "unavailable" and delivery.reason == "authority_cache_busy"
        assert state["source_calls"] == 0
    finally:
        release.set()
        thread.join(2)
        service.close()


def test_two_minutes_offered_before_dispatch_cannot_share_recurrent_previous_state(
    tmp_path, monkeypatch
):
    service, state, provider, bindings = setup(tmp_path, monkeypatch)
    state["auto"] = False
    state["clock"][0] = 720150
    try:
        with service.condition:
            assert provider.offer_committed(bindings[0], candle()).disposition == "pending"
            second = provider.offer_committed(bindings[0], candle(660000))
            assert second.disposition == "unavailable" and second.reason == "stream_event_busy"
            assert state["source_calls"] == 0
        wait(lambda: len(service._pending) == 1)
        assert state["source_calls"] == 1
        state["auto"] = True
        settled(provider, bindings[0], candle(), state)
        assert provider.offer_committed(bindings[0], candle(660000)).disposition == "pending"
        settled(provider, bindings[0], candle(660000), state)
        assert service.owner.gate.previous_state_json("stream0") == "[2]"
        assert state["source_calls"] == 2
        assert service.snapshot()["streams"][0]["manual_required"] is False
    finally:
        service.close()


def test_pending_internal_event_is_not_automatically_replayed_after_restart(tmp_path, monkeypatch):
    service, state, provider, bindings = setup(tmp_path, monkeypatch)
    state["auto"] = False
    provider.offer_committed(bindings[0], candle())
    wait(lambda: len(service._pending) == 1)
    assert service.close()["durable"] is True
    before = state["source_calls"], len(state["workers"])
    restored, _ = create(tmp_path, monkeypatch, epoch="core2", state=state)
    try:
        assert restored.external_features.list_bindings() == ()
        assert (
            restored.external_features.offer_committed(bindings[0], candle()).disposition
            == "unavailable"
        )
        assert before == (state["source_calls"], len(state["workers"]))
        assert restored.snapshot()["deployments"][0]["manual_required"] is True
        assert (
            reset(restored, bindings[0].instance_id, stream="stream0", policy="preserve")["state"]
            == "failed"
        )
    finally:
        restored.close()


def test_duplicate_event_is_rejected_before_source_read_and_consumes_no_control_history(
    tmp_path, monkeypatch
):
    service, state, provider, bindings = setup(tmp_path, monkeypatch)
    binding, bar = bindings[0], candle()
    state["auto"] = False
    try:
        before = len(service.snapshot()["operations"]), service.snapshot()["revision"]
        assert provider.offer_committed(binding, bar).disposition == "pending"
        wait(lambda: len(service._pending) == 1)
        assert state["source_calls"] == 1
        for _ in range(100):
            assert provider.offer_committed(binding, bar).disposition == "pending"
        assert state["source_calls"] == 1
        assert len(state["workers"][0].requests) == 1
        state["auto"] = True
        result = settled(provider, binding, bar, state)
        assert result.snapshot.values == (("NN.score", D(".25")),)
        assert provider.offer_committed(binding, bar).snapshot == result.snapshot
        assert before == (len(service.snapshot()["operations"]), service.snapshot()["revision"])
        assert state["source_calls"] == 1
        state["clock"][0] = result.snapshot.valid_until_ms
        assert provider.offer_committed(binding, bar).disposition == "expired"
        assert state["source_calls"] == 1
    finally:
        service.close()


def test_financial_grace_expiry_never_inserts_a_late_result_into_the_frozen_cutoff(
    tmp_path, monkeypatch
):
    service, state, provider, bindings = setup(tmp_path, monkeypatch)
    binding, bar = bindings[0], candle()
    state["auto"] = False
    try:
        provider.offer_committed(binding, bar)
        wait(lambda: len(service._pending) == 1)
        cutoff = state["clock"][0]
        frozen = provider.lookup(binding, bar, decision_at_ms=cutoff).document()
        assert frozen["disposition"] == "pending"
        state["clock"][0] += 1100
        state["auto"] = True
        settled(provider, binding, bar, state)
        assert provider.lookup(binding, bar, decision_at_ms=cutoff).disposition == "pending"
        assert ExternalFeatureDelivery.parse(frozen).document() == frozen
        assert service.owner.gate.previous_state_json("stream0") == "[1]"
    finally:
        service.close()


def test_no_signal_auto_progress_and_fifo_all_streams_with_only_eight_active(tmp_path, monkeypatch):
    service, state, provider, bindings = setup(tmp_path, monkeypatch, streams=10, no_signal=True)
    state["auto"] = False
    try:
        before = len(service.snapshot()["operations"])
        for binding in bindings:
            assert provider.offer_committed(binding, candle()).disposition == "pending"
        wait(lambda: len(service._pending) == 8)
        assert state["source_calls"] == 8
        assert state["submitted_streams"] == [f"stream{index}" for index in range(8)]
        state["auto"] = True
        for binding in bindings:
            result = settled(provider, binding, candle(), state, "no_signal")
            assert result.snapshot.values == ()
        assert state["submitted_streams"] == [f"stream{index}" for index in range(10)]
        assert state["source_calls"] == 10
        assert len(service.snapshot()["operations"]) == before
        state["clock"][0] += 60000
        provider.offer_committed(bindings[-1], candle(660000))
        settled(provider, bindings[-1], candle(660000), state, "no_signal")
        assert service.owner.gate.previous_state_json("stream9") == "[2]"
    finally:
        service.close()


def test_event_candle_conflict_never_executes_model_and_gap_requires_explicit_reset(
    tmp_path, monkeypatch
):
    service, state, provider, bindings = setup(tmp_path, monkeypatch)
    binding = bindings[0]
    try:
        provider.offer_committed(binding, candle(volume="3"))
        wait(lambda: service.snapshot()["streams"][0]["manual_required"])
        assert not state["workers"][0].requests
        assert state["source_calls"] == 1
        assert (
            reset(service, binding.instance_id, stream="stream0", key="unsafe", policy="preserve")[
                "state"
            ]
            == "failed"
        )
        assert (
            reset(service, binding.instance_id, stream="stream0", key="explicit")["state"]
            == "complete"
        )
        binding = provider.list_bindings()[0]
        provider.offer_committed(binding, candle())
        settled(provider, binding, candle(), state)
        state["clock"][0] += 120000
        before = state["source_calls"]
        assert provider.offer_committed(binding, candle(720000)).disposition == "unavailable"
        wait(lambda: service.snapshot()["streams"][0]["manual_required"])
        assert state["source_calls"] == before
    finally:
        service.close()


def test_auto_admission_survives_synthetic_full_manual_history_without_appending_ids(
    tmp_path, monkeypatch
):
    service, state, provider, bindings = setup(tmp_path, monkeypatch)
    original = provider.store.record
    armed = [True]

    def fill_history(snapshot, **kwargs):
        if armed[0]:
            armed[0] = False
            seed = deepcopy(service._data["operations"]["activate"])
            # Synthetic validated old terminal history, not a claim that these
            # activations ran. Injection occurs on the actual owner thread.
            for index in range(252):
                row = deepcopy(seed)
                row.update(id=f"old{index}", state="failed", result=None)
                service._data["operations"][row["id"]] = row
        return original(snapshot, **kwargs)

    provider.store.record = fill_history
    try:
        binding = bindings[0]
        revision = service.snapshot()["revision"]
        provider.offer_committed(binding, candle())
        settled(provider, binding, candle(), state)
        wait(lambda: len(service.snapshot()["operations"]) == 254)
        with pytest.raises(ValidationError, match="capacity"):
            command(
                service,
                "full-manual",
                "activate",
                training_job_id=JOB,
                deployment_id=None,
                trusted_module=True,
                contract=CONTRACT.document(),
                bindings=[{"name": "NN.score", "path": ["values", 0]}],
            )
        state["clock"][0] += 60000
        provider.offer_committed(binding, candle(660000))
        settled(provider, binding, candle(660000), state)
        assert len(service.snapshot()["operations"]) == 254
        assert service.snapshot()["revision"] == revision
    finally:
        service.close()
