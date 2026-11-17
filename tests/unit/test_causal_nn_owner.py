"""Actual state files and fake evidence/model ports; no causal market claim or orders."""

import json
import os
import threading
from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.application.inference_gate import CausalInferenceGate
from omega.domain.errors import ProtocolError, StorageError, TradingBlocked
from omega.domain.inference import (
    CAUSAL_SCOPE,
    CausalInferenceFrame,
    InferenceEvidenceReceipt,
    InferenceModelIdentity,
    InferenceOutputBinding,
    InferenceReceipt,
    decoded,
    digest,
    encoded,
)
from omega.domain.models import Instrument, Market
from omega.runtime import causal_nn as module
from omega.runtime.causal_nn import CausalNNFeatureOwner, FileInferenceStateStore

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
BINDINGS = (InferenceOutputBinding("NN.score", ("values", 0)),)


def model():
    return InferenceModelIdentity(
        "fixture",
        "a" * 64,
        "b" * 64,
        "v1",
        "c" * 64,
        540000,
        540100,
        540200,
        "d" * 64,
        raw_lookback=2,
        stateful=True,
        state_schema_sha256="e" * 64,
    )


def frame(index=10):
    return CausalInferenceFrame(
        BTC,
        (index - 1) * 60000,
        index * 60000,
        (index + 1) * 60000,
        (index + 1) * 60000 + 100,
        digest({"synthetic_frame": index}),
        2,
    )


class Evidence:
    def __init__(self, clock):
        self.clock, self.calls, self.failure = clock, [], None

    def verify(self, request):
        self.calls.append(request.request_id)
        if self.failure:
            raise self.failure
        return InferenceEvidenceReceipt(
            "committed_inference_evidence",
            request.request_id,
            request.frame.input_sha256,
            self.clock[0],
        )


class Worker:
    def __init__(self):
        self.requests, self.cancelled, self.results = [], [], {}
        self.closed, self.observer, self.failure = False, None, None

    def submit(self, request, previous_state_json):
        if self.observer:
            self.observer(request)
        self.requests.append((request, previous_state_json))
        if self.failure:
            raise self.failure

    def receipt(self, request_id):
        return self.results.get(request_id)

    def cancel(self, request_id):
        self.cancelled.append(request_id)

    def close(self):
        self.closed = True


def create(root, *, epoch="core1", worker=None, evidence=None, clock=None, **kwargs):
    clock = clock if clock is not None else [660150]
    worker = worker or Worker()
    evidence = evidence or Evidence(clock)
    owner = CausalNNFeatureOwner(
        root,
        model(),
        BINDINGS,
        worker=worker,
        evidence=evidence,
        core_epoch=epoch,
        deployment_epoch="deployment",
        clock_ms=lambda: clock[0],
        **kwargs,
    )
    return owner, worker, evidence, clock


def submit(owner, value=None):
    value = value or frame()
    return owner.request(
        "shared",
        value,
        decision_at_ms=value.available_at_ms,
        deadline_ms=value.close_time_ms + 10000,
    )


def finish(owner, worker, clock, request):
    clock[0] = request.issued_at_ms + 60
    worker.results[request.request_id] = InferenceReceipt(
        CAUSAL_SCOPE,
        request.request_id,
        request.request_id,
        request.issued_at_ms + 50,
        "complete",
        encoded({"values": [0.75], "state": [1, 2]}),
    )
    return owner.poll(request.request_id)


def test_real_file_intent_precedes_dispatch_and_shared_lookup_does_no_worker_or_file_io(tmp_path):
    owner, worker, evidence, clock = create(tmp_path)
    try:
        owner.open_stream("shared", BTC, stream_epoch="stream1")

        def observe(request):
            saved = decoded(owner.store.read())
            assert saved["pending"][request.request_id] == request.document()
            assert owner.gate.previous_state_json("shared") == "null"

        worker.observer = observe
        req = submit(owner)
        assert len(worker.requests) == 1 and evidence.calls == [req.request_id]
        assert submit(owner) == req and len(worker.requests) == 1
        assert owner.lookup("shared", frame()) is None
        result = finish(owner, worker, clock, req)
        assert result.values == (("NN.score", D(".75")),)
        before = owner.store.path.read_bytes()
        for _ in range(100):
            assert owner.lookup("shared", frame()) == result
        assert owner.store.path.read_bytes() == before and len(worker.requests) == 1
        assert owner.snapshot()["outstanding_dispatches"] == 0
        assert owner.poll(req.request_id) is None
    finally:
        owner.close()
    assert worker.closed


@pytest.mark.parametrize("after_write", [False, True])
def test_intent_commit_ack_loss_never_dispatches_or_overwrites_actual_new_file(
    tmp_path, monkeypatch, after_write
):
    owner, worker, _, _ = create(tmp_path)
    owner.open_stream("shared", BTC, stream_epoch="stream1")
    before = owner.store.path.read_bytes()
    original = module.atomic_json

    def lose_ack(path, data):
        if after_write:
            original(path, data)
        raise OSError("synthetic intent recording failure")

    monkeypatch.setattr(module, "atomic_json", lose_ack)
    with pytest.raises(StorageError):
        submit(owner)
    actual = owner.store.path.read_bytes()
    assert worker.requests == [] and owner.gate.previous_state_json("shared") == "null"
    assert (actual != before) == after_write
    with pytest.raises(TradingBlocked):
        submit(owner)
    assert owner.store.path.read_bytes() == actual
    with pytest.raises(StorageError, match="cleanup"):
        owner.close()
    monkeypatch.setattr(module, "atomic_json", original)
    recovered, fresh_worker, _, _ = create(tmp_path, epoch="core2")
    try:
        assert recovered.gate.previous_state_json("shared") == "null"
        assert recovered.snapshot()["state"]["pending"] == {}
        assert fresh_worker.requests == []
        with pytest.raises(TradingBlocked):
            submit(recovered)
    finally:
        recovered.close()


def test_completed_state_ack_loss_recovers_actual_new_rnn_state_without_replaying_request(tmp_path):
    owner, worker, _, clock = create(tmp_path)
    owner.open_stream("shared", BTC, stream_epoch="stream1")
    req = submit(owner)
    original = owner.store.commit

    def lose_ack(transition):
        original(transition)
        raise OSError("synthetic response ACK loss")

    owner.store.commit = lose_ack
    with pytest.raises(OSError, match="ACK loss"):
        finish(owner, worker, clock, req)
    actual = owner.store.path.read_bytes()
    assert decoded(owner.store.read())["streams"]["shared"]["state_json"] == "[1,2]"
    assert owner.gate.previous_state_json("shared") == "null"
    assert owner.lookup("shared", frame()) is None
    with pytest.raises(TradingBlocked):
        submit(owner)
    assert owner.store.path.read_bytes() == actual
    with pytest.raises(StorageError):
        owner.close()
    recovered, next_worker, _, next_clock = create(tmp_path, epoch="core2")
    try:
        assert recovered.gate.previous_state_json("shared") == "[1,2]"
        assert recovered.poll(req.request_id) is None
        assert next_worker.requests == []
        recovered.open_stream(
            "shared", BTC, stream_epoch="stream2", initial_state_json="[1,2]", after_open_ms=600000
        )
        next_clock[0] = 720150
        second = submit(recovered, frame(11))
        assert second.previous_state_sha256 == digest([1, 2])
        assert next_worker.requests[0][1] == "[1,2]"
    finally:
        recovered.close()


@pytest.mark.parametrize("mode", ["raise", "wrong_hash", "future", "offline"])
def test_missing_or_mismatched_evidence_blocks_before_worker_dispatch(tmp_path, mode):
    owner, worker, evidence, clock = create(tmp_path)
    try:
        owner.open_stream("shared", BTC, stream_epoch="stream1")
        original = evidence.verify

        def wrong(request):
            if mode == "raise":
                raise ProtocolError("actual source is not verified")
            proof = original(request)
            if mode == "wrong_hash":
                return replace(proof, source_commit_sha256="f" * 64)
            if mode == "future":
                return replace(proof, verified_at_ms=clock[0] + 1)
            return {"scope": "offline_retrospective_shadow_only"}

        evidence.verify = wrong
        with pytest.raises(ProtocolError):
            submit(owner)
        assert worker.requests == []
        assert owner.snapshot()["state"]["streams"]["shared"]["health"] == "blocked"
    finally:
        owner.close()


def test_worker_outcome_unknown_is_not_retried_and_expired_work_is_cancelled(tmp_path):
    owner, worker, _, clock = create(tmp_path)
    try:
        owner.open_stream("shared", BTC, stream_epoch="stream1")
        worker.failure = OSError("worker pipe ACK unknown")
        with pytest.raises(OSError):
            submit(owner)
        assert len(worker.requests) == 1 and len(worker.cancelled) == 1
        with pytest.raises(TradingBlocked):
            submit(owner)
        owner.open_stream("shared", BTC, stream_epoch="stream2")
        worker.failure = None
        req = submit(owner)
        clock[0] = req.deadline_ms
        assert owner.poll(req.request_id) is None
        assert req.request_id in worker.cancelled
        assert owner.gate.previous_state_json("shared") == "null"
    finally:
        owner.close()


def test_old_or_offline_worker_receipt_cannot_mutate_another_stream(tmp_path):
    owner, worker, _, clock = create(tmp_path)
    try:
        owner.open_stream("shared", BTC, stream_epoch="stream1")
        req = submit(owner)
        worker.results[req.request_id] = {
            "scope": "offline_research",
            "prediction": {"values": [1]},
        }
        with pytest.raises(ProtocolError, match="offline"):
            owner.poll(req.request_id)
        owner.open_stream("shared", BTC, stream_epoch="stream2")
        fresh = submit(owner)
        assert owner.poll(req.request_id) is None
        assert finish(owner, worker, clock, fresh).request_id == fresh.request_id
    finally:
        owner.close()


def test_actual_file_cas_and_second_owner_preserve_the_first_owner(tmp_path):
    path = tmp_path / "state.json"
    store = FileInferenceStateStore(path)
    initial = CausalInferenceGate(model(), BINDINGS, core_epoch="core1", deployment_epoch="dep")
    try:
        store.save_snapshot(initial.snapshot(), expected_revision=None)
        with pytest.raises(RuntimeError, match="already owns"):
            FileInferenceStateStore(path)
        initial.open_stream("shared", BTC, stream_epoch="stream1")
        store.save_snapshot(initial.snapshot(), expected_revision=0)
        saved = path.read_bytes()
        initial.request("shared", frame(), decision_at_ms=660100, now_ms=660150, deadline_ms=670000)
        with pytest.raises(ProtocolError, match="changed"):
            store.save_snapshot(initial.snapshot(), expected_revision=0)
        assert path.read_bytes() == saved
    finally:
        store.close()


def test_wrong_thread_does_not_dispatch_or_write_and_cleanup_attempts_both_ports(tmp_path):
    owner, worker, _, _ = create(tmp_path)
    before = owner.store.path.read_bytes()
    failures = []

    def mutate():
        try:
            owner.open_stream("shared", BTC, stream_epoch="stream1")
        except Exception as error:
            failures.append(error)

    thread = threading.Thread(target=mutate)
    thread.start()
    thread.join(2)
    assert not thread.is_alive() and isinstance(failures[0], StorageError)
    assert owner.store.path.read_bytes() == before and worker.requests == []
    worker.close = lambda: (_ for _ in ()).throw(RuntimeError("synthetic cleanup failure"))
    with pytest.raises(StorageError, match="cleanup"):
        owner.close()
    assert owner.store.closed


def test_corrupt_or_missing_snapshot_never_silently_initializes_existing_history(tmp_path):
    owner, _, _, _ = create(tmp_path)
    owner.open_stream("shared", BTC, stream_epoch="stream1")
    path = owner.store.path
    owner.close()
    previous = path.read_bytes()
    document = json.loads(previous)
    document["snapshot"]["revision"] += 1
    path.write_text(json.dumps(document))
    bad = path.read_bytes()
    with pytest.raises(StorageError, match="integrity"):
        create(tmp_path, epoch="new")
    assert path.read_bytes() == bad


@pytest.mark.parametrize("missing", ["state", "anchor"])
def test_missing_registered_state_or_origin_cannot_become_a_fresh_model(tmp_path, missing):
    owner, _, _, _ = create(tmp_path)
    owner.open_stream("shared", BTC, stream_epoch="stream1")
    state, anchor = owner.store.path, owner.store.anchor_path
    owner.close()
    deleted, remaining = (state, anchor) if missing == "state" else (anchor, state)
    deleted.unlink()
    before = remaining.read_bytes()
    with pytest.raises(StorageError, match="missing|integrity"):
        create(tmp_path, epoch="core2")
    assert not deleted.exists() and remaining.read_bytes() == before


def test_damaged_origin_anchor_is_preserved_and_never_replaced(tmp_path):
    owner, _, _, _ = create(tmp_path)
    path = owner.store.anchor_path
    owner.close()
    path.write_text('{"data":{},"sha256":"bad"}')
    before = path.read_bytes()
    with pytest.raises(StorageError, match="anchor"):
        create(tmp_path, epoch="core2")
    assert path.read_bytes() == before


def test_fifo_state_is_rejected_without_blocking(tmp_path):
    if not hasattr(os, "mkfifo"):
        pytest.skip("POSIX FIFO boundary requires Linux")
    path = tmp_path / "state.json"
    os.mkfifo(path)
    store = FileInferenceStateStore(path)
    try:
        with pytest.raises(StorageError):
            store.read()
    finally:
        store.close()
