"""Actual child/model execution with explicitly synthetic committed-source evidence.

The source fixture below is not a real DB provenance integration. Logical model
activation is backdated only by a test monkeypatch, avoiding a minute-long wait;
production activation always uses actual wall time and has a separate check.
"""

import csv
import importlib.util
import json
import multiprocessing
import os
import time
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path

import pytest

from omega.application.inference_gate import CausalInferenceGate
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.inference import InferenceOutputBinding, digest
from omega.domain.models import Instrument, Market
from omega.research import demo_instance
from omega.research.datasets import FIELDS, MarketRow, export_dataset
from omega.research.deployment import ShadowModelContract
from omega.research.neural import train
from omega.runtime import causal_nn_proof as proof
from omega.runtime import causal_nn_worker as worker_module
from omega.runtime.causal_nn import CausalNNFeatureOwner
from omega.runtime.causal_nn_worker import CausalInferenceProcessWorker

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
CONTRACT = ShadowModelContract("omega.ohlcv.1m.v1", ("volume",), 1, (None,))
BINDINGS = (InferenceOutputBinding("NN.score", ("values", 0)),)


@pytest.fixture(scope="module")
def training(tmp_path_factory):
    root = tmp_path_factory.mktemp("causal-training")
    source = root / "source.csv"
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
    export_dataset(source, root / "dataset", {})
    train(root / "dataset", Path(demo_instance.__file__), root / "run", {"config": {"epochs": 1}})
    return root / "run"


class SyntheticSource:
    def __init__(self):
        self.calls, self.change = 0, None

    def read_window(self, instrument, start_ms, end_ms):
        self.calls += 1
        result = []
        for opened in range(start_ms, end_ms, 60000):
            row = MarketRow(instrument.key, opened, D(100), D(102), D(99), D(101), D(2))
            observation = proof.CommittedInferenceObservation(
                row, opened + 60000, digest({"synthetic_commit": opened})
            )
            result.append(self.change(observation) if self.change else observation)
        return tuple(result)


def setup(tmp_path, training, monkeypatch):
    opened = (proof.wall_ms() // 60000 - 1) * 60000
    with monkeypatch.context() as patch:
        patch.setattr(proof, "wall_ms", lambda: opened - 1)
        activation = proof.prepare_causal_activation(
            training, tmp_path / "activation", CONTRACT, trusted_module=True
        )
    source = SyntheticSource()
    registry = proof.InferenceInputRegistry(tmp_path / "inputs", activation, source)
    worker = CausalInferenceProcessWorker(activation, registry.root, tmp_path / "execution")
    return activation, source, registry, worker, opened


def request(activation, registry, opened, *, stream="shared", deadline=None):
    gate = CausalInferenceGate(
        activation.model, BINDINGS, core_epoch="core", deployment_epoch="deployment"
    )
    gate.open_stream(stream, BTC, stream_epoch="epoch")
    frame = registry.frame(BTC, opened)
    now = proof.wall_ms()
    value = gate.request(
        stream, frame, decision_at_ms=now, now_ms=now, deadline_ms=deadline or now + 15000
    )
    registry.verify(value)
    return gate, value


def wait_receipt(worker, key, *, seconds=15):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        result = worker.receipt(key)
        if result is not None:
            return result
        time.sleep(0.02)
    pytest.fail(f"child returned no receipt: {worker.errors}")


def test_activation_is_data_only_and_knowledge_cannot_be_backdated(tmp_path, training):
    before = proof.wall_ms()
    activation = proof.prepare_causal_activation(
        training, tmp_path / "activation", CONTRACT, trusted_module=True
    )
    assert (
        before
        <= activation.model.training_cutoff_ms
        <= activation.model.available_at_ms
        <= proof.wall_ms()
    )
    manifest = activation.verify()
    assert manifest["verified_at_ms"] >= before
    assert manifest["all_source_end_ms"] == 3600000
    assert manifest["scope"] != "paper_shadow_only"
    assert not (activation.root / "prediction").exists()


@pytest.mark.parametrize(
    "auxiliary",
    [
        {"scope": "retrospective_training_only", "sha256": "a" * 64},
        {},
    ],
)
def test_retrospective_auxiliary_training_is_never_promoted_to_causal_activation(
    tmp_path, training, auxiliary
):
    source = tmp_path / "retrospective_run"
    source.mkdir()
    document = json.loads((training / "run.json").read_text(encoding="utf-8"))
    document["dataset"]["auxiliary"] = auxiliary
    (source / "run.json").write_text(json.dumps(document), encoding="utf-8")
    destination = tmp_path / "must_not_activate"
    with pytest.raises(ValueError, match="retrospective auxiliary"):
        proof.prepare_causal_activation(source, destination, CONTRACT, trusted_module=True)
    assert not destination.exists()


def test_actual_reused_child_demo_values_to_durable_owner_feature(tmp_path, training, monkeypatch):
    activation, source, registry, worker, opened = setup(tmp_path, training, monkeypatch)
    owner = CausalNNFeatureOwner(
        tmp_path / "owner",
        activation.model,
        BINDINGS,
        worker=worker,
        evidence=registry,
        core_epoch="core",
        deployment_epoch="deployment",
    )
    child_pid = None
    try:
        for stream in ("shared", "other"):
            owner.open_stream(stream, BTC, stream_epoch="epoch")
            frame = registry.frame(BTC, opened)
            now = proof.wall_ms()
            req = owner.request(stream, frame, decision_at_ms=now, deadline_ms=now + 15000)
            assert owner.lookup(stream, frame) is None
            end = time.monotonic() + 15
            feature = None
            while feature is None and time.monotonic() < end:
                feature = owner.poll(req.request_id)
                time.sleep(0.01)
            assert feature is not None, worker.errors
            assert feature.values[0][0] == "NN.score"
            assert feature.values[0][1].is_finite()
            assert owner.lookup(stream, frame) == feature
            assert source.calls <= 2
            if child_pid is None:
                child_pid = worker._process.pid
            assert child_pid == worker._process.pid != os.getpid()
        assert len(list((tmp_path / "execution").iterdir())) == 1
    finally:
        owner.close()


def test_duplicate_id_not_reexecuted(tmp_path, training, monkeypatch):
    activation, _, registry, worker, opened = setup(tmp_path, training, monkeypatch)
    _, req = request(activation, registry, opened)
    try:
        worker.submit(req, "null")
        assert wait_receipt(worker, req.request_id).status == "complete"
        with pytest.raises(ProtocolError, match="replayed"):
            worker.submit(req, "null")
    finally:
        worker.close()


def test_expired_duplicate_records_do_not_exhaust_long_lived_worker(
    tmp_path, training, monkeypatch
):
    activation, _, registry, worker, opened = setup(tmp_path, training, monkeypatch)
    _, req = request(activation, registry, opened)
    worker._seen = {
        digest({"expired_request": index}): req.issued_at_ms - 1 for index in range(4096)
    }
    try:
        worker.submit(req, "null")
        assert wait_receipt(worker, req.request_id).status == "complete"
        assert len(worker._seen) == 1
    finally:
        worker.close()


@pytest.mark.parametrize("kind", ["mc", "repair", "future", "commit", "instrument"])
def test_registry_rejects_inconsistent_source_evidence(tmp_path, training, monkeypatch, kind):
    _, source, registry, worker, opened = setup(tmp_path, training, monkeypatch)
    changes = {
        "mc": lambda row: replace(row, origin="monte_carlo"),
        "repair": lambda row: replace(row, quality="interpolated"),
        "future": lambda row: replace(row, available_at_ms=proof.wall_ms() + 1000),
        "commit": lambda row: replace(row, commit_sha256="not-a-commit"),
        "instrument": lambda row: replace(
            row, row=replace(row.row, instrument="binance:usdm:ETHUSDT")
        ),
    }
    source.change = changes[kind]
    try:
        with pytest.raises((ValueError, ValidationError)):
            registry.frame(BTC, opened)
        assert worker._process is None
    finally:
        worker.close()


@pytest.mark.parametrize("kind", ["request", "snapshot", "oversize", "offline", "state"])
def test_dispatch_rejects_changed_frozen_input_before_model_import(
    tmp_path, training, monkeypatch, kind
):
    activation, _, registry, worker, opened = setup(tmp_path, training, monkeypatch)
    _, req = request(activation, registry, opened)
    path = registry.root / (req.request_id + ".json")
    document = proof.read_document(path)
    if kind == "request":
        document["request"]["stream_epoch"] = "other"
    elif kind == "snapshot":
        document["snapshot"]["observations"][0]["row"]["volume"] = "3"
    elif kind == "offline":
        document["scope"] = "offline_retrospective_shadow_only"
    elif kind == "oversize":
        document["extra"] = "x" * (proof.MAX_INPUT_BYTES + 1)
    path.write_text(json.dumps(document), encoding="utf-8")
    try:
        with pytest.raises(ValueError):
            worker.submit(req, "[]" if kind == "state" else "null")
        assert worker._process is None
    finally:
        worker.close()


def test_cancel_retires_all_peer_work_without_retry(tmp_path, training, monkeypatch):
    modified = custom_training(tmp_path, training, "        import time\n        time.sleep(0.5)\n")
    activation, _, registry, worker, opened = setup(tmp_path, modified, monkeypatch)
    _, first = request(activation, registry, opened, stream="a")
    _, second = request(activation, registry, opened, stream="b")
    try:
        worker.submit(first, "null")
        worker.submit(second, "null")
        worker.cancel(first.request_id)
        assert worker.receipt(first.request_id).status == "unknown"
        assert worker.receipt(second.request_id).status == "unknown"
        with pytest.raises(ProtocolError, match="retired"):
            worker.submit(second, "null")
    finally:
        worker.close()


def test_expired_input_cannot_be_replaced_by_fresh_window(tmp_path, training, monkeypatch):
    activation, _, registry, worker, opened = setup(tmp_path, training, monkeypatch)
    _, req = request(activation, registry, opened)
    before = proof.read_document(registry.root / (req.request_id + ".json"))
    registry.clock_ms = lambda: req.deadline_ms + 1
    monkeypatch.setattr(worker_module, "wall_ms", lambda: req.deadline_ms + 1)
    registry.frame(BTC, opened)  # New observation lookup does not refresh old request.
    try:
        with pytest.raises(ValueError, match="deadline"):
            registry.verify(req)
        assert proof.read_document(registry.root / (req.request_id + ".json")) == before
        with pytest.raises(ValueError, match="deadline"):
            worker.submit(req, "null")
    finally:
        worker.close()


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO available on Linux only")
def test_fifo_input_rejected_without_blocking(tmp_path, training, monkeypatch):
    activation, _, registry, worker, opened = setup(tmp_path, training, monkeypatch)
    _, req = request(activation, registry, opened)
    path = registry.root / (req.request_id + ".json")
    path.unlink()
    os.mkfifo(path)
    try:
        with pytest.raises(ValueError):
            worker.submit(req, "null")
        assert worker._process is None
    finally:
        worker.close()


def test_changed_model_snapshot_preserves_bytes_and_blocks_activation(
    tmp_path, training, monkeypatch
):
    activation, _, registry, worker, opened = setup(tmp_path, training, monkeypatch)
    _, req = request(activation, registry, opened)
    path = activation.root / "instance_module.py"
    path.write_text("raise RuntimeError('must never execute')", encoding="utf-8")
    try:
        with pytest.raises(ValueError, match="artifact"):
            registry.verify(req)
        # Parent dispatch itself checks the input; child rechecks the activation
        # before loading. No result from a modified model may reach the gate.
        worker.submit(req, "null")
        assert wait_receipt(worker, req.request_id).status == "failed"
        assert "artifact" in worker.errors[req.request_id]
    finally:
        worker.close()


def custom_training(tmp_path, training, code):
    source = tmp_path / "custom_instance.py"
    source.write_text(
        Path(demo_instance.__file__)
        .read_text(encoding="utf-8")
        .replace(
            "    def prepare_inference(self, rows, request, context):\n",
            "    def prepare_inference(self, rows, request, context):\n" + code,
        ),
        encoding="utf-8",
    )
    result = tmp_path / "custom_training"
    train(training.parent / "dataset", source, result, {"config": {"epochs": 1}})
    return result


def test_child_detects_input_replacement_by_trusted_preprocessor(tmp_path, training, monkeypatch):
    modified = custom_training(
        tmp_path,
        training,
        "        import os\n"
        "        from pathlib import Path\n"
        "        Path(os.environ['OMEGA_TEST_CAUSAL_INPUT']).write_text('{}', encoding='utf-8')\n",
    )
    activation, _, registry, worker, opened = setup(tmp_path, modified, monkeypatch)
    _, req = request(activation, registry, opened)
    monkeypatch.setenv("OMEGA_TEST_CAUSAL_INPUT", str(registry.root / (req.request_id + ".json")))
    try:
        worker.submit(req, "null")
        assert wait_receipt(worker, req.request_id).status == "failed"
        assert "replaced during preprocessing" in worker.errors[req.request_id]
    finally:
        worker.close()


def test_actual_model_result_cannot_publish_after_deadline(tmp_path, training, monkeypatch):
    modified = custom_training(tmp_path, training, "        import time\n        time.sleep(1.1)\n")
    activation, _, registry, worker, opened = setup(tmp_path, modified, monkeypatch)
    _, req = request(activation, registry, opened, deadline=proof.wall_ms() + 1000)
    try:
        worker.submit(req, "null")
        result = wait_receipt(worker, req.request_id)
        assert result.status == "failed"
        assert "deadline" in worker.errors[req.request_id]
    finally:
        worker.close()


def test_real_child_cleanup_failure_is_not_reported_as_confirmed(tmp_path, training, monkeypatch):
    modified = custom_training(
        tmp_path,
        training,
        "        def failed_close(context):\n"
        "            raise RuntimeError('synthetic cleanup failure')\n"
        "        self.close = failed_close\n",
    )
    activation, _, registry, worker, opened = setup(tmp_path, modified, monkeypatch)
    _, req = request(activation, registry, opened)
    worker.submit(req, "null")
    assert wait_receipt(worker, req.request_id).status == "complete"
    with pytest.raises(StorageError, match="cleanup unconfirmed"):
        worker.close()
    assert worker.closed


def test_symlink_input_cannot_redirect_child_to_another_record(tmp_path, training, monkeypatch):
    activation, _, registry, worker, opened = setup(tmp_path, training, monkeypatch)
    _, req = request(activation, registry, opened)
    path = registry.root / (req.request_id + ".json")
    target = tmp_path / "actual_input.json"
    path.rename(target)
    try:
        path.symlink_to(target)
    except OSError:
        # Windows junction creation does not require symlink privilege and still
        # exercises the same reparse ancestor guard, without changing other files.
        if os.name != "nt":
            raise
        import subprocess

        redirected = tmp_path / "redirected"
        redirected.mkdir()
        target.rename(redirected / path.name)
        link = tmp_path / "junction"
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(redirected)],
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            worker.close()
            pytest.skip("Windows host denies both symlink and junction creation")
        worker.input_root = link
    try:
        with pytest.raises(ValueError, match="reparse|symlink"):
            worker.submit(req, "null")
        assert worker._process is None
    finally:
        worker.close()


def _train_pytorch(dataset, module, destination):
    # This function itself is invoked by spawn: torch is never imported or
    # executed by the host fixture process.
    train(
        dataset,
        module,
        destination,
        {
            "device": "cpu",
            "config": {
                "architecture": "gru",
                "lookback": 2,
                "hidden_size": 2,
                "epochs": 1,
                "batch_size": 16,
            },
        },
    )


@pytest.mark.skipif(
    importlib.util.find_spec("torch") is None, reason="optional CPU PyTorch not installed"
)
def test_actual_pytorch_child_state_is_owned_by_host_receipt(tmp_path, training, monkeypatch):
    from omega.research import pytorch_instance

    process = multiprocessing.get_context("spawn").Process(
        target=_train_pytorch,
        args=(training.parent / "dataset", Path(pytorch_instance.__file__), tmp_path / "torch_run"),
    )
    process.start()
    process.join(timeout=30)
    if process.is_alive():
        process.terminate()
        process.join(timeout=3)
        pytest.fail("CPU PyTorch child training exceeded 30 seconds")
    assert process.exitcode == 0
    process.close()
    contract = ShadowModelContract(
        "omega.ohlcv.1m.v1",
        ("close/open-1", "high/open-1", "low/open-1", "log_volume"),
        2,
        (None, 2, 4),
        state_shape=(1, 1, 2),
        state_version="gru-hidden-v1",
    )
    opened = (proof.wall_ms() // 60000 - 1) * 60000
    with monkeypatch.context() as patch:
        patch.setattr(proof, "wall_ms", lambda: opened - 1)
        activation = proof.prepare_causal_activation(
            tmp_path / "torch_run", tmp_path / "activation", contract, trusted_module=True
        )
    registry = proof.InferenceInputRegistry(tmp_path / "inputs", activation, SyntheticSource())
    worker = CausalInferenceProcessWorker(activation, registry.root, tmp_path / "execution")
    _, req = request(activation, registry, opened)
    try:
        worker.submit(req, "null")
        result = wait_receipt(worker, req.request_id)
        assert result.status == "complete", worker.errors
        prediction = json.loads(result.prediction_json)
        assert len(prediction["state"]) == 1
        assert len(prediction["state"][0][0]) == 2
        assert result.request_sha256 == req.request_id
    finally:
        worker.close()


def test_actual_child_result_ack_loss_preserves_disk_and_requires_manual_restart(
    tmp_path, training, monkeypatch
):
    activation, _, registry, worker, opened = setup(tmp_path, training, monkeypatch)
    root = tmp_path / "owner"
    owner = CausalNNFeatureOwner(
        root,
        activation.model,
        BINDINGS,
        worker=worker,
        evidence=registry,
        core_epoch="core1",
        deployment_epoch="deployment",
    )
    owner.open_stream("shared", BTC, stream_epoch="epoch1")
    frame = registry.frame(BTC, opened)
    now = proof.wall_ms()
    req = owner.request("shared", frame, decision_at_ms=now, deadline_ms=now + 15000)
    real_commit = owner.store.commit

    def lose_ack(transition):
        return replace(real_commit(transition), durable=False)

    monkeypatch.setattr(owner.store, "commit", lose_ack)
    end = time.monotonic() + 15
    failed = False
    while not failed and time.monotonic() < end:
        try:
            owner.poll(req.request_id)
        except StorageError:
            failed = True
        time.sleep(0.01)
    assert failed
    assert owner.lookup("shared", frame) is None
    assert owner.snapshot()["state"]["streams"]["shared"]["last_request_id"] is None
    saved = json.loads(owner.store.read())
    assert saved["streams"]["shared"]["last_request_id"] == req.request_id
    with pytest.raises(StorageError):
        owner.close()
    next_worker = CausalInferenceProcessWorker(activation, registry.root, tmp_path / "execution2")
    restored = CausalNNFeatureOwner(
        root,
        activation.model,
        BINDINGS,
        worker=next_worker,
        evidence=registry,
        core_epoch="core2",
        deployment_epoch="deployment",
    )
    try:
        state = restored.snapshot()["state"]["streams"]["shared"]
        assert state["last_request_id"] == req.request_id
        assert state["health"] == "blocked"
        assert state["feature"] is None
        assert next_worker._process is None
        assert restored.poll(req.request_id) is None
    finally:
        restored.close()
