"""Real actor/files with explicitly fake source/model ports; no actual NN or DB."""

import json
import threading
import time
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.inference import (
    CAUSAL_SCOPE,
    InferenceModelIdentity,
    InferenceReceipt,
    digest,
    encoded,
)
from omega.domain.model_contract import ShadowModelContract
from omega.research.datasets import MarketRow
from omega.runtime import causal_inference_service as module
from omega.runtime.causal_inference_service import CausalInferenceService
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_nn_proof import CommittedInferenceObservation

JOB = "a" * 32
INSTRUMENT = "binance:usdm:BTCUSDT"
CONTRACT = ShadowModelContract(
    "omega.ohlcv.1m.v1", ("volume",), 1, (None,), state_shape=(1,), state_version="fixture-v1"
)
BINDINGS = [{"name": "NN.score", "path": ["values", 0]}]


def wait(predicate, seconds=4):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        result = predicate()
        if result:
            return result
        time.sleep(0.005)
    pytest.fail("causal test actor did not reach the expected state")


def terminal(service, key):
    def read():
        row = service.status(key)
        return row if row["state"] in {"complete", "failed", "unknown", "cancelled"} else None

    return wait(read)


def fixture_job(root):
    final = root / "artifacts" / JOB
    training = final / "training"
    training.mkdir(parents=True)
    files = {
        "training/run.json": json.dumps({"model": {"file": "model.json"}}).encode(),
        "training/instance_module.py": b"# never execute this test fixture\n",
        "training/model.json": b"{}",
    }
    for name, raw in files.items():
        (final / name).write_bytes(raw)
    (final / "artifact.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "job_id": JOB,
                "kind": "train",
                "state": "complete",
                "result_file": "training/run.json",
                "files": [
                    {"file": name, "sha256": sha256(raw).hexdigest()} for name, raw in files.items()
                ],
            }
        ),
        encoding="utf-8",
    )
    return {
        "id": JOB,
        "kind": "train",
        "state": "complete",
        "artifact": str(final),
        "result_file": str(training / "run.json"),
    }


class Worker:
    def __init__(self, state, activation, input_root, execution_root):
        self.state, self.activation = state, activation
        self.requests, self.cancelled, self.results = {}, [], {}
        self.closed = False
        self.thread = threading.get_ident()
        state["workers"].append(self)

    def submit(self, request, previous_state_json):
        assert threading.get_ident() == self.thread
        journal = json.loads((self.state["root"] / "state/causal-inference.json").read_text())[
            "snapshot"
        ]
        if not self.state.get("allow_internal"):
            assert any(
                (row["result"] or {}).get("request_id") == request.request_id
                for row in journal["operations"].values()
            )
        else:
            owner_path = (
                self.state["root"]
                / "state/causal-nn"
                / (digest(request.deployment_epoch) + ".json")
            )
            assert request.request_id in json.loads(owner_path.read_text())["snapshot"]["pending"]
        self.state.setdefault("submitted_streams", []).append(request.stream)
        self.requests[request.request_id] = request, previous_state_json
        self.state["events"].append(("submit", threading.get_ident()))
        if self.state.get("dispatch_failure"):
            raise RuntimeError("worker dispatch outcome unknown")

    def receipt(self, key):
        assert threading.get_ident() == self.thread
        if key not in self.requests or not self.state["auto"]:
            return self.results.pop(key, None)
        request, old = self.requests.pop(key)
        state = json.loads(old)
        return InferenceReceipt(
            CAUSAL_SCOPE,
            key,
            key,
            self.state["clock"][0],
            "complete",
            encoded(
                {
                    "values": [None if self.state.get("no_signal") else 0.25],
                    "state": [1 if state is None else state[0] + 1],
                }
            ),
        )

    def cancel(self, key):
        assert threading.get_ident() == self.thread
        self.cancelled.append(key)

    def close(self):
        assert threading.get_ident() == self.thread
        self.state["close_calls"] = self.state.get("close_calls", 0) + 1
        self.closed = True
        if self.state.get("close_failure"):
            raise RuntimeError("must-not-leak-secret")

    def retirement_status(self):
        alive = self.state.get("physical_alive", False)
        return {
            "closed": self.closed,
            "worker_alive": alive,
            "confirmed": self.closed and not alive,
        }


def create(root, monkeypatch, *, epoch="core1", state=None, **options):
    state = state or {
        "clock": [660150],
        "workers": [],
        "events": [],
        "auto": True,
        "provider_calls": 0,
        "source_calls": 0,
        "verify_calls": 0,
    }
    status = (
        fixture_job(root)
        if not (root / "artifacts" / JOB).exists()
        else {
            "id": JOB,
            "kind": "train",
            "state": "complete",
            "artifact": str(root / "artifacts" / JOB),
            "result_file": str(root / "artifacts" / JOB / "training/run.json"),
        }
    )
    state["root"] = root
    contract_fixture = (
        replace(CONTRACT, allow_no_signal=True) if state.get("no_signal") else CONTRACT
    )
    model = InferenceModelIdentity(
        "fixture",
        "b" * 64,
        "c" * 64,
        "v1",
        "d" * 64,
        540000,
        540100,
        540200,
        digest(contract_fixture.document()),
        stateful=True,
        state_schema_sha256="e" * 64,
        allow_no_signal=contract_fixture.allow_no_signal,
    )

    def activation(root_path, manifest_sha256=None, active_model=None, contract=None, **_):
        def verify():
            state["verify_calls"] += 1
            if state.get("verify_failure"):
                raise ValueError("fixture evidence verification failed")

        return SimpleNamespace(
            root=Path(root_path),
            manifest_sha256=manifest_sha256 or "f" * 64,
            model=active_model or model,
            contract=contract or CONTRACT,
            verify=verify,
        )

    def prepare(source, destination, contract, **kwargs):
        state["events"].append(("activate", threading.get_ident()))
        # The service's real durable queued/running intent exists first.
        journal = json.loads((root / "state/causal-inference.json").read_text())["snapshot"]
        assert any(
            row["kind"] == "activate" and row["state"] == "running"
            for row in journal["operations"].values()
        )
        Path(destination).mkdir(parents=True)
        return activation(destination, contract=contract)

    monkeypatch.setattr(module, "prepare_causal_activation", prepare)
    monkeypatch.setattr(module, "InferenceActivation", activation)

    class Source:
        def read_window(self, instrument, start, end):
            state["source_calls"] += 1
            state["events"].append(("read", threading.get_ident()))
            if state.get("read_entered"):
                state["read_entered"].set()
                assert state["read_release"].wait(3)
            state["clock"][0] += 7
            return tuple(
                CommittedInferenceObservation(
                    MarketRow(instrument.key, value, D(100), D(102), D(99), D(101), D(2)),
                    state["clock"][0],
                    digest({"fixture": value}),
                )
                for value in range(start, end, 60000)
            )

        def close(self):
            state["events"].append(("source_close", threading.get_ident()))

    def provider(key):
        state["provider_calls"] += 1
        assert key == JOB
        return status

    service = CausalInferenceService(
        root,
        core_epoch=epoch,
        training_job_provider=provider,
        source_factory=lambda _: Source(),
        worker_factory=lambda *args: Worker(state, *args),
        clock_ms=lambda: state["clock"][0],
        **options,
    )
    service.start()
    return service, state


def command(service, key, kind, **parameters):
    view = service.snapshot()
    return service.submit(
        key, kind, {"core_epoch": view["core_epoch"], "revision": view["revision"], **parameters}
    )


def activate(service, *, contract=CONTRACT):
    command(
        service,
        "activate",
        "activate",
        training_job_id=JOB,
        deployment_id=None,
        trusted_module=True,
        contract=contract.document(),
        bindings=BINDINGS,
    )
    result = terminal(service, "activate")
    assert result["state"] == "complete", result
    return result["result"]["deployment_id"]


def reset(service, deployment, *, policy="reset", key="reset", stream="stream"):
    command(
        service,
        key,
        "reset_stream",
        deployment_id=deployment,
        stream=stream,
        instrument=INSTRUMENT,
        state_policy=policy,
    )
    return terminal(service, key)


def predict(service, deployment, *, key="predict", timeout=1000, opened=600000):
    return command(
        service,
        key,
        "predict_committed",
        deployment_id=deployment,
        stream="stream",
        instrument=INSTRUMENT,
        open_time_ms=opened,
        timeout_ms=timeout,
    )


def test_disabled_startup_cached_get_and_no_auto_python_or_db(tmp_path, monkeypatch):
    service, state = create(tmp_path, monkeypatch)
    try:
        view = service.snapshot()
        assert view["state"] == "disabled" and view["ready"]
        assert not view["live_orders_enabled"] and not view["strategy_delivery_enabled"]
        assert not state["workers"] and not state["provider_calls"] and not state["source_calls"]
        for _ in range(100):
            assert service.snapshot()["active_deployment_id"] is None
        assert not state["verify_calls"]
    finally:
        receipt = service.close()
    assert receipt["durable"] and not receipt["actor_alive"] and not receipt["worker_alive"]


def test_explicit_activation_reset_predict_durable_values_and_thread_ownership(
    tmp_path, monkeypatch
):
    service, state = create(tmp_path, monkeypatch)
    try:
        deployment = activate(service)
        assert not state["source_calls"] and not state["workers"][0].requests
        assert reset(service, deployment)["state"] == "complete"
        row = predict(service, deployment)
        assert row["durable"]
        done = terminal(service, "predict")
        assert done["state"] == "complete", done
        assert done["result"]["feature"]["values"] == {"NN.score": "0.25"}
        view = service.snapshot()
        stream = view["streams"][0]
        assert stream["feature"]["decision_at_ms"] == state["clock"][0]
        assert stream["feature"]["request_id"] == done["result"]["request_id"]
        raw = json.dumps(view)
        assert "state_json" not in raw and "parameters" not in raw and str(tmp_path) not in raw
        assert {thread for _, thread in state["events"]} == {service.thread.ident}
        assert service.thread.ident != threading.get_ident()
        before = state["source_calls"], state["verify_calls"]
        for _ in range(100):
            assert service.snapshot()["streams"][0]["feature"] is not None
        assert before == (state["source_calls"], state["verify_calls"])
        state["clock"][0] += 1000
        assert service.snapshot()["streams"][0]["feature"] is None
    finally:
        service.close()
    assert all(worker.closed for worker in state["workers"])


def test_same_operation_id_reuses_receipt_but_changed_payload_and_stale_controls_reject(
    tmp_path, monkeypatch
):
    service, state = create(tmp_path, monkeypatch)
    try:
        deployment = activate(service)
        original = service._data["operations"]["activate"]["parameters"]
        assert service.submit("activate", "activate", original)["state"] == "complete"
        assert state["provider_calls"] == 1
        with pytest.raises(ValidationError, match="conflicts"):
            service.submit("activate", "activate", {**original, "training_job_id": "b" * 32})
        with pytest.raises(ValidationError, match="revision"):
            service.submit(
                "old", "pause", {"core_epoch": "core1", "revision": 0, "deployment_id": deployment}
            )
        with pytest.raises(ValidationError, match="epoch"):
            service.submit(
                "wrong",
                "pause",
                {
                    "core_epoch": "retired",
                    "revision": service.snapshot()["revision"],
                    "deployment_id": deployment,
                },
            )
    finally:
        service.close()


def test_pause_invalidates_feature_and_explicit_preserve_uses_actual_saved_state(
    tmp_path, monkeypatch
):
    service, state = create(tmp_path, monkeypatch)
    try:
        deployment = activate(service)
        reset(service, deployment)
        predict(service, deployment)
        assert terminal(service, "predict")["state"] == "complete"
        command(service, "pause", "pause", deployment_id=deployment)
        assert terminal(service, "pause")["state"] == "complete"
        assert service.snapshot()["streams"][0]["feature"] is None
        assert reset(service, deployment, policy="preserve", key="preserve")["state"] == "complete"
        assert service.owner.gate.previous_state_json("stream") == "[1]"
        assert len(state["workers"]) == 2
    finally:
        service.close()


def test_restart_is_disabled_preserves_state_and_never_replays_pending_operations(
    tmp_path, monkeypatch
):
    service, state = create(tmp_path, monkeypatch)
    deployment = activate(service)
    reset(service, deployment)
    predict(service, deployment)
    assert terminal(service, "predict")["state"] == "complete"
    service.close()
    before = len(state["workers"]), state["provider_calls"]
    restored, _ = create(tmp_path, monkeypatch, epoch="core2", state=state)
    try:
        assert restored.snapshot()["active_deployment_id"] is None
        assert before == (len(state["workers"]), state["provider_calls"])
        assert restored.snapshot()["streams"][0]["feature"] is None
        assert restored.status("predict")["state"] == "complete"
        assert reset(restored, deployment, policy="preserve", key="manual")["state"] == "complete"
        assert restored.owner.gate.previous_state_json("stream") == "[1]"
    finally:
        restored.close()


def test_unknown_pending_cannot_be_preserved_and_close_has_no_replay(tmp_path, monkeypatch):
    service, state = create(tmp_path, monkeypatch)
    try:
        deployment = activate(service)
        reset(service, deployment)
        state["auto"] = False
        predict(service, deployment)
        wait(lambda: len(service._pending) == 1)
        command(service, "pause", "pause", deployment_id=deployment)
        assert terminal(service, "pause")["state"] == "complete"
        assert terminal(service, "predict")["state"] == "unknown"
        assert reset(service, deployment, policy="preserve", key="unsafe")["state"] == "failed"
        assert reset(service, deployment, policy="reset", key="explicit")["state"] == "complete"
    finally:
        service.close()


def test_deadline_and_late_worker_result_never_publish_feature(tmp_path, monkeypatch):
    service, state = create(tmp_path, monkeypatch)
    try:
        deployment = activate(service)
        reset(service, deployment)
        state["auto"] = False
        predict(service, deployment)
        wait(lambda: len(service._pending) == 1)
        request_id = next(iter(service._pending))
        request = state["workers"][0].requests[request_id][0]
        state["clock"][0] = request.deadline_ms + 1
        assert terminal(service, "predict")["state"] == "unknown"
        state["workers"][0].results[request_id] = InferenceReceipt(
            CAUSAL_SCOPE,
            request_id,
            request_id,
            request.deadline_ms,
            "complete",
            encoded({"values": [1], "state": [1]}),
        )
        assert service.snapshot()["streams"][0]["feature"] is None
        assert request_id in state["workers"][0].cancelled
    finally:
        service.close()


def test_intent_ack_loss_before_activation_prevents_all_external_actions(tmp_path, monkeypatch):
    state = {"armed": False}

    def store_factory(path):
        store = FileInferenceStateStore(path)
        save = store.save_snapshot

        def lose_ack(raw, **kwargs):
            ack = save(raw, **kwargs)
            return replace(ack, durable=False) if state["armed"] else ack

        store.save_snapshot = lose_ack
        return store

    service, ports = create(tmp_path, monkeypatch, store_factory=store_factory)
    state["armed"] = True
    with pytest.raises(StorageError):
        command(
            service,
            "activate",
            "activate",
            training_job_id=JOB,
            deployment_id=None,
            trusted_module=True,
            contract=CONTRACT.document(),
            bindings=BINDINGS,
        )
    wait(lambda: service.closed)
    assert not ports["provider_calls"] and not ports["workers"]
    assert service.snapshot()["state"] == "recording_unconfirmed"
    with pytest.raises(StorageError):
        service.close()


def test_partial_journal_is_preserved_and_start_does_not_load_python(tmp_path, monkeypatch):
    path = tmp_path / "state/causal-inference.json"
    path.parent.mkdir()
    path.write_text("{", encoding="utf-8")
    service = CausalInferenceService(
        tmp_path,
        core_epoch="core",
        training_job_provider=lambda _: pytest.fail("must not call provider"),
    )
    with pytest.raises(StorageError):
        service.start()
    assert path.read_text() == "{"
    wait(lambda: service.closed)
    with pytest.raises(StorageError):
        service.close()


def test_close_before_start_and_prestart_get_have_no_io(tmp_path):
    service = CausalInferenceService(
        tmp_path,
        core_epoch="core",
        training_job_provider=lambda _: pytest.fail("no provider calls"),
    )
    assert service.snapshot()["state"] == "disabled"
    with pytest.raises(ValidationError):
        service.status("missing")
    assert service.close()["durable"]
    assert list(tmp_path.iterdir()) == []


def test_admission_timeout_is_unknown_and_never_executes_after_late_intent_ack(
    tmp_path, monkeypatch
):
    entered, release = threading.Event(), threading.Event()
    armed = [False]

    def store_factory(path):
        store = FileInferenceStateStore(path)
        original = store.save_snapshot

        def delayed(raw, **kwargs):
            if armed[0]:
                armed[0] = False
                entered.set()
                assert release.wait(2)
            return original(raw, **kwargs)

        store.save_snapshot = delayed
        return store

    service, ports = create(tmp_path, monkeypatch, store_factory=store_factory, ack_timeout_s=0.1)
    try:
        armed[0] = True
        parameters = {
            "core_epoch": "core1",
            "revision": service.snapshot()["revision"],
            "training_job_id": JOB,
            "deployment_id": None,
            "trusted_module": True,
            "contract": CONTRACT.document(),
            "bindings": BINDINGS,
        }
        result = service.submit("slow", "activate", parameters)
        assert entered.is_set()
        assert result["state"] == "unknown" and not result["durable"]
        assert not ports["provider_calls"] and not ports["workers"]
        release.set()

        def durable_status():
            row = service.status("slow")
            return row if row["durable"] else None

        saved = wait(durable_status)
        assert saved["state"] == "unknown"
        assert service.submit("slow", "activate", parameters)["state"] == "unknown"
        assert not ports["provider_calls"]
    finally:
        release.set()
        service.close()


def test_storage_failure_ack_wakes_submit_before_incident_takes_core_fence(tmp_path, monkeypatch):
    fence = threading.Lock()
    callback_entered, callback_done = threading.Event(), threading.Event()
    armed = [False]

    def callback(code):
        callback_entered.set()
        with fence:
            callback_done.set()

    def store_factory(path):
        store = FileInferenceStateStore(path)
        original = store.save_snapshot

        def lose(raw, **kwargs):
            ack = original(raw, **kwargs)
            return replace(ack, durable=False) if armed[0] else ack

        store.save_snapshot = lose
        return store

    service, ports = create(
        tmp_path, monkeypatch, store_factory=store_factory, on_incident=callback, ack_timeout_s=2
    )
    armed[0] = True
    with fence:
        started = time.monotonic()
        with pytest.raises(StorageError):
            command(
                service,
                "activate",
                "activate",
                training_job_id=JOB,
                deployment_id=None,
                trusted_module=True,
                contract=CONTRACT.document(),
                bindings=BINDINGS,
            )
        assert time.monotonic() - started < 1
    assert callback_entered.wait(1) and callback_done.wait(1)
    assert not ports["workers"]
    with pytest.raises(StorageError):
        service.close()


def test_explicit_reset_cancels_queued_peers_and_uses_a_new_child_port(tmp_path, monkeypatch):
    service, state = create(tmp_path, monkeypatch)
    try:
        deployment = activate(service)
        reset(service, deployment)
        reset(service, deployment, stream="peer", key="peer-reset")
        state["auto"] = False
        predict(service, deployment, key="one")
        command(
            service,
            "two",
            "predict_committed",
            deployment_id=deployment,
            stream="peer",
            instrument=INSTRUMENT,
            open_time_ms=600000,
            timeout_ms=1000,
        )
        wait(lambda: len(service._pending) == 2)
        assert reset(service, deployment, key="reset-active")["state"] == "complete"
        assert terminal(service, "one")["state"] == "unknown"
        assert terminal(service, "two")["state"] == "unknown"
        assert len(state["workers"]) == 2 and state["workers"][0].closed
        assert not state["workers"][1].requests
        views = {row["stream"]: row for row in service.snapshot()["streams"]}
        assert not views["stream"]["manual_required"] and views["peer"]["manual_required"]
    finally:
        service.close()


def test_partial_activation_and_modified_registered_artifact_never_load_python(
    tmp_path, monkeypatch
):
    service, ports = create(tmp_path, monkeypatch)

    def fail(source, destination, contract, **kwargs):
        Path(destination).mkdir(parents=True)
        (Path(destination) / "partial.bin").write_bytes(b"preserve me")
        raise RuntimeError("secret path must not escape")

    monkeypatch.setattr(module, "prepare_causal_activation", fail)
    try:
        command(
            service,
            "failed",
            "activate",
            training_job_id=JOB,
            deployment_id=None,
            trusted_module=True,
            contract=CONTRACT.document(),
            bindings=BINDINGS,
        )
        assert terminal(service, "failed")["state"] == "failed"
        assert service.snapshot()["deployments"][0]["state"] == "failed"
        assert not ports["workers"]
        assert (
            list((tmp_path / "artifacts/causal-inference").rglob("partial.bin"))[0].read_bytes()
            == b"preserve me"
        )
        (tmp_path / "artifacts" / JOB / "training/model.json").write_bytes(b"changed")
        command(
            service,
            "modified",
            "activate",
            training_job_id=JOB,
            deployment_id=None,
            trusted_module=True,
            contract=CONTRACT.document(),
            bindings=BINDINGS,
        )
        assert terminal(service, "modified")["state"] == "failed"
        assert "secret" not in json.dumps(service.snapshot())
        assert len(service.snapshot()["deployments"]) == 1
    finally:
        service.close()


def test_journal_os_lock_survives_close_timeout_then_releases_after_real_retirement(
    tmp_path, monkeypatch
):
    service, state = create(tmp_path, monkeypatch, close_timeout_s=0.05)
    deployment = activate(service)
    reset(service, deployment)
    state["read_entered"], state["read_release"] = threading.Event(), threading.Event()
    predict(service, deployment)
    assert state["read_entered"].wait(1)
    with pytest.raises(StorageError, match="deadline"):
        service.close()
    assert service.thread.is_alive()
    second = CausalInferenceService(
        tmp_path,
        core_epoch="core2",
        training_job_provider=lambda _: pytest.fail("must not run provider"),
    )
    try:
        with pytest.raises(StorageError):
            second.start()
        with pytest.raises(StorageError):
            second.close()
    finally:
        state["read_release"].set()
    wait(lambda: service.closed)
    assert service.close()["durable"]
    assert not state["workers"][0].requests
    third, _ = create(tmp_path, monkeypatch, epoch="core3", state=state)
    assert third.snapshot()["active_deployment_id"] is None
    third.close()


def test_cleanup_failure_is_critical_and_original_store_lock_is_released(tmp_path, monkeypatch):
    service, state = create(tmp_path, monkeypatch)
    activate(service)
    state["close_failure"] = True
    with pytest.raises(StorageError):
        service.close()
    assert service.closed and not service.thread.is_alive()
    assert any(kind == "source_close" for kind, _ in state["events"])
    assert service.snapshot()["state"] == "recording_unconfirmed"
    store = FileInferenceStateStore(tmp_path / "state/causal-inference.json")
    store.close()


@pytest.mark.parametrize("kind", ["model", "result", "feature", "epoch"])
def test_malformed_but_checksummed_registry_is_preserved_without_private_get_leak(
    tmp_path, monkeypatch, kind
):
    service, ports = create(tmp_path, monkeypatch)
    deployment = activate(service)
    reset(service, deployment)
    predict(service, deployment)
    assert terminal(service, "predict")["state"] == "complete"
    service.close()
    path = tmp_path / "state/causal-inference.json"
    store = FileInferenceStateStore(path)
    data = json.loads(store.read())
    previous = data["revision"]
    data["revision"] += 1
    if kind == "model":
        data["deployments"][deployment]["model"]["secret_path"] = "C:/private/key"
    elif kind == "result":
        data["operations"]["activate"]["result"]["private_path"] = "C:/private/key"
    elif kind == "feature":
        data["operations"]["predict"]["result"]["feature"]["values"]["NN.score"] = "C:/private/key"
    else:
        data["operations"]["predict"]["core_epoch"] = "mismatched"
    store.save_snapshot(encoded(data), expected_revision=previous)
    store.close()
    original = path.read_bytes()
    fresh = CausalInferenceService(
        tmp_path, core_epoch="core2", training_job_provider=lambda _: pytest.fail("no provider")
    )
    with pytest.raises(StorageError):
        fresh.start()
    wait(lambda: fresh.closed)
    assert path.read_bytes() == original
    assert "C:/private/key" not in json.dumps(fresh.snapshot())
    assert fresh.snapshot()["active_deployment_id"] is None
    assert ports["provider_calls"] == 1
    with pytest.raises(StorageError):
        fresh.close()


def test_bounded_history_never_discards_ids_and_reserves_protective_pause_retire(
    tmp_path, monkeypatch
):
    from copy import deepcopy

    service, ports = create(tmp_path, monkeypatch)
    deployment = activate(service)
    service.close()
    path = tmp_path / "state/causal-inference.json"
    store = FileInferenceStateStore(path)
    data = json.loads(store.read())
    seed = data["operations"]["activate"]
    data["operations"] = {}
    # Synthetic validated completed history supplies the capacity boundary; no
    # claim that 254 model activations actually ran is made by this fixture.
    for index in range(254):
        row = deepcopy(seed)
        row.update(
            id=f"old{index:03d}",
            submitted_revision=index + 1,
            revision=index + 2,
            state="failed",
            result=None,
        )
        row["parameters"]["revision"] = index + 1
        row["request_digest"] = digest({"kind": row["kind"], "parameters": row["parameters"]})
        data["operations"][row["id"]] = row
    data["control_revision"] = 255
    previous = data["revision"]
    data["revision"] += 1
    store.save_snapshot(encoded(data), expected_revision=previous)
    store.close()
    restored, _ = create(tmp_path, monkeypatch, epoch="core2", state=ports)
    try:
        with pytest.raises(ValidationError, match="capacity"):
            command(
                restored,
                "full",
                "activate",
                training_job_id=JOB,
                deployment_id=None,
                trusted_module=True,
                contract=CONTRACT.document(),
                bindings=BINDINGS,
            )
        command(restored, "protect-pause", "pause", deployment_id=deployment)
        assert terminal(restored, "protect-pause")["state"] == "complete"
        command(restored, "protect-retire", "retire", deployment_id=deployment)
        assert terminal(restored, "protect-retire")["state"] == "complete"
        assert len(restored.snapshot()["operations"]) == 256
        assert restored.status("old000")["state"] == "failed"
        assert ports["provider_calls"] == 1
    finally:
        restored.close()


def test_normal_no_signal_commits_state_once_and_preserve_continues_with_next_minute(
    tmp_path, monkeypatch
):
    ports = {
        "clock": [660150],
        "workers": [],
        "events": [],
        "auto": True,
        "provider_calls": 0,
        "source_calls": 0,
        "verify_calls": 0,
        "no_signal": True,
    }
    service, state = create(tmp_path, monkeypatch, state=ports)
    try:
        deployment = activate(service, contract=replace(CONTRACT, allow_no_signal=True))
        reset(service, deployment)
        first = predict(service, deployment)
        done = terminal(service, first["id"])
        assert done["state"] == "complete", done
        assert done["result"]["no_signal"] is True and done["result"]["feature"] is None
        view = service.snapshot()["streams"][0]
        assert view["state"] == "ready" and view["reason"] == "no_signal"
        assert not view["manual_required"] and view["last_open_ms"] == 600000
        assert service.owner.gate.previous_state_json("stream") == "[1]"
        original = service._data["operations"]["predict"]["parameters"]
        assert service.submit("predict", "predict_committed", original)["result"]["no_signal"]
        assert service.snapshot()["streams"][0]["sequence"] == 1
        assert (
            reset(service, deployment, policy="preserve", key="preserve-known")["state"]
            == "complete"
        )
        state["clock"][0] += 60000
        state["no_signal"] = False
        predict(service, deployment, key="next-minute", opened=660000)
        assert terminal(service, "next-minute")["state"] == "complete"
        assert service.owner.gate.previous_state_json("stream") == "[2]"
        assert service.snapshot()["streams"][0]["feature"]["values"] == {"NN.score": "0.25"}
    finally:
        service.close()


def test_live_unknown_cleanup_retains_owner_and_journal_lock_until_explicit_retry(
    tmp_path, monkeypatch
):
    service, state = create(tmp_path, monkeypatch, close_timeout_s=0.05)
    activate(service)
    state["close_failure"], state["physical_alive"] = True, True
    try:
        with pytest.raises(StorageError, match="deadline"):
            service.close()
        assert service.thread.is_alive() and service._quarantined
        assert service.snapshot()["state"] == "recording_unconfirmed"
        assert not service.snapshot()["ready"]
        with pytest.raises((StorageError, RuntimeError)):
            FileInferenceStateStore(tmp_path / "state/causal-inference.json")
        before = state["close_calls"]
        time.sleep(0.05)
        assert state["close_calls"] == before  # Condition wait, no retry polling.
    finally:
        state["close_failure"], state["physical_alive"] = False, False
        with pytest.raises(StorageError):  # Critical uncertainty remains recorded.
            service.close()
        wait(lambda: service.closed)
    assert not service.thread.is_alive() and not service._quarantined
    store = FileInferenceStateStore(tmp_path / "state/causal-inference.json")
    store.close()


@pytest.mark.parametrize("fault", ["dispatch_failure", "verify_failure"])
def test_request_failure_refreshes_blocked_stream_and_prevents_preserve_escape(
    tmp_path, monkeypatch, fault
):
    service, state = create(tmp_path, monkeypatch)
    try:
        deployment = activate(service)
        reset(service, deployment)
        predict(service, deployment)
        assert terminal(service, "predict")["state"] == "complete"
        state["clock"][0] += 60000
        state[fault] = True
        predict(service, deployment, key="uncertain", opened=660000)
        done = terminal(service, "uncertain")
        assert done["state"] == "failed"
        view = service.snapshot()["streams"][0]
        assert view["state"] == "blocked" and view["manual_required"]
        assert view["feature"] is None
        if fault == "dispatch_failure":
            assert len(done["result"]["request_id"]) == 64
            assert done["result"]["request_id"] in state["workers"][0].cancelled
        assert (
            reset(service, deployment, policy="preserve", key="must-not-preserve")["state"]
            == "failed"
        )
        assert service.owner.gate.previous_state_json("stream") == "[1]"
        state[fault] = False
        assert reset(service, deployment, key="explicit-reset")["state"] == "complete"
        assert len(state["workers"]) == 2
        predict(service, deployment, key="manual-retry", opened=660000)
        assert terminal(service, "manual-retry")["state"] == "complete"
        assert service.owner.gate.previous_state_json("stream") == "[1]"
    finally:
        state[fault] = False
        service.close()


def test_no_signal_with_late_durable_ack_is_unknown_not_a_normal_no_signal(tmp_path, monkeypatch):
    ports = {
        "clock": [660150],
        "workers": [],
        "events": [],
        "auto": True,
        "provider_calls": 0,
        "source_calls": 0,
        "verify_calls": 0,
        "no_signal": True,
    }

    def owner_factory(*args, **kwargs):
        owner = module.CausalNNFeatureOwner(*args, **kwargs)
        original = owner.store.commit

        def delayed_ack(transition):
            ack = original(transition)
            ports["clock"][0] += 1000
            return ack

        owner.store.commit = delayed_ack
        return owner

    service, state = create(tmp_path, monkeypatch, state=ports, owner_factory=owner_factory)
    try:
        deployment = activate(service, contract=replace(CONTRACT, allow_no_signal=True))
        reset(service, deployment)
        predict(service, deployment)
        done = terminal(service, "predict")
        assert done["state"] == "unknown"
        assert "no_signal" not in done["result"]
        assert service.owner.gate.previous_state_json("stream") == "[1]"
        assert service.snapshot()["streams"][0]["manual_required"]
        assert (
            reset(service, deployment, policy="preserve", key="late-preserve")["state"] == "failed"
        )
    finally:
        service.close()
