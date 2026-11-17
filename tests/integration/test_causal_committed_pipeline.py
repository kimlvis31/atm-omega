"""Real DB -> causal evidence -> durable owner -> actual child -> feature ACK.

Explicit --database-tests only. This waits for one newly eligible minute to close
after actual model activation (normally 60..120 seconds). Market OHLCV are test
fixtures, committed after that actual boundary, never exchange observations.
The external stdlib demo adds an explicit test counter state, not a PyTorch RNN.
No historical clock monkeypatch, account, TS or trading execution is involved.
"""

import csv
import json
import multiprocessing
import os
import time
import tomllib
from decimal import Decimal as D
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.errors import TradingBlocked
from omega.domain.inference import InferenceOutputBinding, digest
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.collection import MinuteObservation
from omega.research import demo_instance
from omega.research.datasets import FIELDS, export_dataset
from omega.research.deployment import ShadowModelContract
from omega.runtime.causal_market_source import CommittedInferenceMarketSource
from omega.runtime.causal_nn import CausalNNFeatureOwner
from omega.runtime.causal_nn_proof import (
    InferenceInputRegistry,
    prepare_causal_activation,
    read_document,
)
from omega.runtime.causal_nn_worker import CausalInferenceProcessWorker

pytestmark = pytest.mark.database
MINUTE = 60000


def _training_child(dataset, module, destination):
    from omega.research.neural import train

    train(dataset, module, destination, {"config": {"epochs": 1}})


@pytest.fixture
def pipeline_database(request, tmp_path):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated database opt-in")
    path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not path:
        pytest.fail("OMEGA_TEST_DB_CONFIG must explicitly select the isolated test DB")
    path = Path(path).resolve()
    settings = load_database_settings(path)
    if (settings.host, settings.port, settings.dbname, settings.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing causal pipeline test outside 127.0.0.1:55433/omega_test/omega_app")
    with connect_database(settings) as connection:
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    secret = Path(raw["password_file"])
    if not secret.is_absolute():
        secret = path.parent / secret
    public = {
        "host": settings.host,
        "port": settings.port,
        "dbname": settings.dbname,
        "user": settings.user,
        "password_file": secret.resolve().as_posix(),
    }
    (tmp_path / "database.config.toml").write_text(
        "\n".join(f"{key} = {json.dumps(value)}" for key, value in public.items()), encoding="utf-8"
    )
    key = Instrument("binance", Market.USDM, "TEST_CAUSAL_" + uuid4().hex[:20].upper())
    try:
        yield settings, key
    finally:
        with connect_database(settings) as connection:
            assert key.symbol.startswith("TEST_CAUSAL_")
            connection.execute("DELETE FROM market.candles_1m WHERE instrument=%s", (key.key,))


def train_counter_demo(root, instrument):
    """Training and external module code execute only in a real child process."""
    source = root / "training.csv"
    with source.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        for index in range(60):
            writer.writerow(
                {
                    "instrument": instrument.key,
                    "open_time_ms": index * MINUTE,
                    "open": "100",
                    "high": "102",
                    "low": "99",
                    "close": "101",
                    "volume": str(index + 1),
                }
            )
    dataset = root / "training_dataset"
    export_dataset(source, dataset, {})
    text = Path(demo_instance.__file__).read_text(encoding="utf-8")
    text = text.replace(
        '"module_id": "omega.demo.linear_neuron"', '"module_id": "omega.test.causal_counter_demo"'
    )
    text = text.replace('"stateful_inference": False', '"stateful_inference": True')
    old = 'return {"values": [self.weight * value + self.bias for value in batch], "state": state}'
    new = (
        'return {"values": [self.weight * value + self.bias for value in batch], '
        '"state": [1 if state is None else state[0] + 1]}'
    )
    assert old in text
    text = text.replace(old, new)
    module = root / "counter_demo.py"
    module.write_text(text, encoding="utf-8")
    process = multiprocessing.get_context("spawn").Process(
        target=_training_child, args=(dataset, module, root / "training_run")
    )
    process.start()
    process.join(timeout=20)
    if process.is_alive():
        process.terminate()
        process.join(timeout=3)
        pytest.fail("isolated stdlib demo training exceeded its 20 second deadline")
    assert process.exitcode == 0
    assert process.pid != os.getpid()
    process.close()
    return root / "training_run"


def test_actual_committed_pipeline_durable_feature_and_restart_manual_gate(
    pipeline_database, tmp_path
):
    settings, key = pipeline_database
    training = train_counter_demo(tmp_path, key)
    contract = ShadowModelContract(
        "omega.ohlcv.1m.v1",
        ("volume",),
        1,
        (None,),
        state_shape=(1,),
        state_version="test-counter-v1",
    )
    activation = prepare_causal_activation(
        training, tmp_path / "activation", contract, trusted_module=True
    )
    assert activation.model.available_at_ms >= activation.model.training_cutoff_ms
    target_open = (activation.model.training_cutoff_ms // MINUTE + 1) * MINUTE
    target_close = target_open + MINUTE
    end_wait = time.monotonic() + 150
    while time.time_ns() // 1_000_000 < target_close:
        if time.monotonic() >= end_wait:
            pytest.fail("actual newly eligible minute did not close within 150 seconds")
        time.sleep(0.2)
    # All three rows are synthetic fixtures accepted after the actual target bar
    # closes. Their local reception times are not forged candle-close timestamps.
    accepted = time.time_ns() // 1_000_000
    writer = PostgresCandleStore(settings)
    try:
        for index in range(3):
            opened, price = target_open - (2 - index) * MINUTE, D(100 + index)
            assert writer.put(
                MinuteObservation(
                    Candle(
                        key,
                        opened,
                        opened + MINUTE,
                        price,
                        price + 1,
                        price - 1,
                        price,
                        D(10 + index),
                    ),
                    "rest",
                    accepted,
                )
            )
    finally:
        writer.close()
    source = CommittedInferenceMarketSource(tmp_path)
    stored = source.read_window(key, target_open - 2 * MINUTE, target_close)
    assert len(stored) == 3
    assert all(value.available_at_ms >= accepted for value in stored)
    registry = InferenceInputRegistry(tmp_path / "inputs", activation, source)
    worker = CausalInferenceProcessWorker(activation, registry.root, tmp_path / "execution")
    submitted = []
    submit = worker.submit

    def submit_once(request, previous_state_json):
        submitted.append(request.request_id)
        return submit(request, previous_state_json)

    worker.submit = submit_once
    bindings = (InferenceOutputBinding("NN.score", ("values", 0)),)
    owner_root = tmp_path / "owner"
    owner = CausalNNFeatureOwner(
        owner_root,
        activation.model,
        bindings,
        worker=worker,
        evidence=registry,
        core_epoch="core1",
        deployment_epoch="deployment",
    )
    try:
        owner.open_stream("committed", key, stream_epoch="stream1")
        frame = registry.frame(key, target_open)
        decision = time.time_ns() // 1_000_000
        assert decision >= frame.available_at_ms >= accepted
        deadline = decision + 20000
        request = owner.request("committed", frame, decision_at_ms=decision, deadline_ms=deadline)
        duplicate = owner.request("committed", frame, decision_at_ms=decision, deadline_ms=deadline)
        assert duplicate.request_id == request.request_id
        assert submitted == [request.request_id]
        assert owner.lookup("committed", frame) is None
        evidence = read_document(registry.root / (request.request_id + ".json"))
        assert evidence["input_sha256"] == digest(evidence["snapshot"]) == frame.input_sha256
        observed = evidence["snapshot"]["observations"][0]
        assert observed["commit_sha256"] == stored[-1].commit_sha256
        assert observed["available_at_ms"] >= accepted
        assert observed["row"]["open_time_ms"] == target_open
        feature, end_predict = None, time.monotonic() + 20
        while feature is None and time.monotonic() < end_predict:
            feature = owner.poll(request.request_id)
            if feature is None:
                time.sleep(0.01)
        assert feature is not None, worker.errors
        assert feature.request_id == request.request_id
        assert feature.input_sha256 == frame.input_sha256
        assert feature.values[0][0] == "NN.score" and feature.values[0][1].is_finite()
        assert worker._process.pid != os.getpid()
        actual = json.loads(owner.store.read())
        state = actual["streams"]["committed"]
        assert json.loads(state["state_json"]) == [1]
        assert state["last_request_id"] == request.request_id
        assert state["feature"]["request_id"] == request.request_id
        for _ in range(100):
            assert owner.lookup("committed", frame) == feature
        assert owner.poll(request.request_id) is None
        assert submitted == [request.request_id]
    finally:
        owner.close()
    restarted_worker = CausalInferenceProcessWorker(
        activation, registry.root, tmp_path / "execution2"
    )
    restarted = CausalNNFeatureOwner(
        owner_root,
        activation.model,
        bindings,
        worker=restarted_worker,
        evidence=registry,
        core_epoch="core2",
        deployment_epoch="deployment",
    )
    try:
        restored = restarted.snapshot()["state"]["streams"]["committed"]
        assert json.loads(restored["state_json"]) == [1]
        assert restored["health"] == "blocked"
        assert restored["feature"] is None
        assert restarted.lookup("committed", frame) is None
        assert restarted.poll(request.request_id) is None
        assert restarted_worker._process is None
        with pytest.raises(TradingBlocked):
            restarted.request(
                "committed",
                frame,
                decision_at_ms=time.time_ns() // 1_000_000,
                deadline_ms=time.time_ns() // 1_000_000 + 20000,
            )
    finally:
        restarted.close()
