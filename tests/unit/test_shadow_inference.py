"""Shadow owner boundaries plus an actual offline research-child pipeline."""

import csv
import json
import multiprocessing
import os
import shutil
import threading
import time
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.research.datasets import FIELDS, export_dataset
from omega.research.deployment import ShadowModelContract
from omega.research.service import ResearchService
from omega.runtime import shadow_inference
from omega.runtime.shadow_inference import ShadowInferenceService

INSTRUMENT = "binance:usdm:BTCUSDT"


def await_value(read, check, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = read()
        if check(value):
            return value
        time.sleep(0.01)
    raise AssertionError(f"shadow/research operation did not finish: {value}")


def completed(service, identity):
    return await_value(
        lambda: service.status(identity),
        lambda row: row["state"] not in {"queued", "running", "waiting"},
    )


@pytest.fixture(scope="module")
def actual_training(tmp_path_factory):
    root = tmp_path_factory.mktemp("shadow-actual-training")
    source = root / "source.csv"
    with source.open("w", newline="", encoding="utf8") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        for index in range(40):
            writer.writerow(
                dict(
                    instrument=INSTRUMENT,
                    open_time_ms=index * 60000,
                    open=100 + index,
                    high=102 + index,
                    low=99 + index,
                    close=101 + index,
                    volume=10 + index,
                )
            )
    dataset = root / "dataset"
    export_dataset(source, dataset, {})
    research = ResearchService(root / "research")
    try:
        job = research.submit(
            "train",
            {
                "dataset": str(dataset),
                "demo": True,
                "config": {"epochs": 1},
                "device": "cpu",
                "timeout_seconds": 15,
            },
        )
        trained = completed(research, job["id"])
        assert trained["state"] == "complete", trained
        assert (
            Path(trained["result_file"])
            == research.root / "artifacts" / job["id"] / "training/run.json"
        )
        yield research, trained
    finally:
        research.close()


class FakeResearch:
    """Fake scheduler writes real candidate-compatible result files; no model import."""

    def __init__(self, root, training):
        actual, job = training
        self.root = root.resolve()
        shutil.copytree(
            Path(job["result_file"]).parent, self.root / "artifacts" / job["id"] / "training"
        )
        self.training = deepcopy(job)
        self.training["result_file"] = str(
            self.root / "artifacts" / job["id"] / "training/run.json"
        )
        self.jobs = {job["id"]: self.training}
        self.submissions, self.cancellations = [], []
        self.running = self.ack_loss = self.invalid_result = False
        self.on_submit = None

    def status(self, identity):
        return deepcopy(self.jobs[identity])

    def submit(self, kind, parameters):
        assert kind == "nn_predict"
        identity = uuid4().hex
        run = Path(parameters["training_run"])
        metadata = json.loads((run / "run.json").read_text())
        model = json.loads((run / "model.json").read_text())
        # The fixture is a stateless linear demo; this fake supplies artifact IO only.
        result = {
            "schema_version": 1,
            "module": metadata["module"],
            "checkpoint_sha256": sha256((run / "model.json").read_bytes()).hexdigest(),
            "scope": "offline_research; no account/trading integration",
            "prediction": {
                "values": [model["weight"] * x + model["bias"] for x in parameters["batch"]],
                "state": None,
            },
        }
        if self.invalid_result:
            result["prediction"]["values"] = [[1]]
        path = self.root / "artifacts" / identity / "prediction/prediction.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(result), encoding="utf8")
        job = dict(
            id=identity,
            kind=kind,
            parameters=deepcopy(parameters),
            state="running" if self.running else "complete",
            result_file=str(path),
        )
        self.jobs[identity] = job
        self.submissions.append(deepcopy(job))
        if self.on_submit:
            self.on_submit()
        if self.ack_loss:
            raise OSError("synthetic child ACK loss")
        return deepcopy(job)

    def cancel(self, identity):
        self.cancellations.append(identity)
        self.jobs[identity]["state"] = "cancelled"
        return self.status(identity)


@pytest.fixture
def research(tmp_path, actual_training):
    return FakeResearch(tmp_path / "research", actual_training)


def ready(research, epoch="core-a", clock=None):
    service = ShadowInferenceService(
        research.root, research, core_epoch=epoch, clock_ms=clock or (lambda: 120001)
    )
    service.start()
    await_value(service.snapshot, lambda row: row["state"] in {"ready", "unavailable"})
    assert service.snapshot()["state"] == "ready", service.snapshot()
    return service


def revision(service):
    return service.snapshot()["deployment"]["document"]["revision"]


def operation(service, identity, kind, parameters):
    service.submit(identity, kind, parameters)
    return completed(service, identity)


def fixture_parameters(research):
    return {
        "training_job_id": research.training["id"],
        "contract": ShadowModelContract(
            "omega.ohlcv.1m.v1", ("normalized_volume",), 1, (None,)
        ).document(),
        "fixture_batch": [0.25],
        "trusted_module": True,
    }


def activate(service, research):
    row = operation(service, "fixture", "prepare_fixture", fixture_parameters(research))
    assert row["state"] == "completed", row
    candidate_id = row["result"]["candidate_id"]
    assert (
        operation(
            service,
            "activate",
            "activate",
            {"candidate_id": candidate_id, "expected_revision": revision(service)},
        )["state"]
        == "completed"
    )
    assert (
        operation(
            service,
            "stream",
            "reset_stream",
            {
                "stream": "preview",
                "instrument": INSTRUMENT,
                "reason": "explicit_start",
                "expected_revision": revision(service),
            },
        )["state"]
        == "completed"
    )
    return candidate_id


def test_completed_training_metadata_preview_uses_owner_worker(actual_training):
    research, trained = actual_training
    service = ShadowInferenceService(research.root, research, core_epoch=uuid4().hex)
    try:
        service.start()
        await_value(service.snapshot, lambda value: value["state"] == "ready")
        before = sorted(p.name for p in (research.root / "jobs").glob("*.json"))
        row = operation(
            service, "inspect-run", "inspect_training", {"training_job_id": trained["id"]}
        )
        assert row["state"] == "completed", row
        preview = row["result"]
        assert preview["training_job_id"] == trained["id"]
        assert preview["module"]["module_id"] == "omega.demo.linear_neuron"
        assert preview["contract_defaults"]["features"] == ["normalized_volume"]
        assert sorted(p.name for p in (research.root / "jobs").glob("*.json")) == before
        assert service.snapshot()["deployment"]["document"]["candidate"] is None
    finally:
        service.close()


def prediction(service):
    return dict(
        expected_revision=revision(service),
        stream="preview",
        instrument=INSTRUMENT,
        sequence=1,
        open_time_ms=60000,
        as_of_ms=120000,
        deadline_ms=170000,
        input_digest="c" * 64,
        batch=[0.5],
        schema_id="omega.ohlcv.1m.v1",
        features=["normalized_volume"],
        lookback=1,
    )


def test_actual_research_training_fixture_activation_and_prediction(actual_training):
    research, trained = actual_training
    # This test uses actual train + two actual nn_predict children, no fake artifacts.
    service = ready(research)
    try:
        params = fixture_parameters(type("Source", (), {"training": trained})())
        row = operation(service, "actual-fixture", "prepare_fixture", params)
        assert row["state"] == "completed", row
        candidate = row["candidate_id"]
        assert (
            operation(
                service,
                "actual-activate",
                "activate",
                {"candidate_id": candidate, "expected_revision": revision(service)},
            )["state"]
            == "completed"
        )
        operation(
            service,
            "actual-stream",
            "reset_stream",
            {
                "stream": "preview",
                "instrument": INSTRUMENT,
                "expected_revision": revision(service),
                "reason": "explicit_start",
            },
        )
        result = operation(service, "actual-predict", "predict", prediction(service))
        assert result["state"] == "completed", result
        assert result["result"]["scope"] == "paper_shadow_only"
        assert len(result["result"]["values"]) == 1
        assert Path(research.status(result["job_id"])["result_file"]).is_file()
    finally:
        service.close()


def test_candidate_hashing_is_worker_only_and_get_is_memory_only(research, monkeypatch):
    caller = threading.get_ident()
    threads = []
    original = shadow_inference.prepare_candidate
    monkeypatch.setattr(
        shadow_inference,
        "prepare_candidate",
        lambda *a, **k: (threads.append(threading.get_ident()), original(*a, **k))[1],
    )
    service = ready(research)
    try:
        activate(service, research)
        monkeypatch.setattr(service, "_read", lambda: pytest.fail("GET must not read files"))
        monkeypatch.setattr(research, "status", lambda _: pytest.fail("GET must not poll children"))
        assert service.status("fixture")["state"] == "completed"
        assert service.snapshot()["candidates"][0]["fixture_verified"]
        assert threads and all(item != caller for item in threads)
    finally:
        service.close()


@pytest.mark.parametrize("change", ["wrong_kind", "wrong_path", "untrusted"])
def test_fixture_source_binding_and_explicit_trust(research, change):
    service = ready(research)
    try:
        params = fixture_parameters(research)
        if change == "wrong_kind":
            research.training["kind"] = "nn_predict"
        elif change == "wrong_path":
            research.training["result_file"] = str(
                research.root / "artifacts" / research.training["id"] / "train/run.json"
            )
        else:
            params["trusted_module"] = False
            with pytest.raises(ValidationError):
                service.submit("bad", "prepare_fixture", params)
            return
        assert operation(service, "bad", "prepare_fixture", params)["state"] == "failed"
        assert not research.submissions
    finally:
        service.close()


def test_duplicate_request_and_stale_revision_do_not_change_controller(research):
    service = ready(research)
    try:
        params = fixture_parameters(research)
        row = operation(service, "fixture", "prepare_fixture", params)
        assert service.submit("fixture", "prepare_fixture", params) == row
        with pytest.raises(ValidationError, match="conflict"):
            service.submit("fixture", "prepare_fixture", {**params, "fixture_batch": [0.5]})
        before = service.snapshot()["deployment"]
        assert (
            operation(service, "stale", "pause", {"expected_revision": 99, "reason": "manual"})[
                "state"
            ]
            == "failed"
        )
        assert service.snapshot()["deployment"] == before
        assert len(research.submissions) == 1
    finally:
        service.close()


def test_submit_ack_loss_is_unknown_and_never_automatically_resubmitted(research):
    service = ready(research)
    try:
        research.ack_loss = True
        row = operation(service, "fixture", "prepare_fixture", fixture_parameters(research))
        assert row["state"] == "unknown" and row["job_id"] is None
        assert row["intent"]["parameters"] == research.submissions[0]["parameters"]
        operation(service, "refresh", "refresh", {})
        assert len(research.submissions) == 1
    finally:
        service.close()
    restored = ready(research, "core-b")
    try:
        assert restored.status("fixture")["state"] == "unknown"
        assert restored.snapshot()["deployment"]["document"]["paused"]
        assert len(research.submissions) == 1
    finally:
        restored.close()


def test_job_bind_commit_failure_keeps_dispatch_intent_and_cancels_child(research, monkeypatch):
    service = ready(research)
    original = shadow_inference.atomic_json

    def fail_bind(path, envelope):
        if any(row["job_id"] for row in envelope["data"]["operations"].values()):
            raise OSError("synthetic disk failure")
        original(path, envelope)

    monkeypatch.setattr(shadow_inference, "atomic_json", fail_bind)
    try:
        row = operation(service, "fixture", "prepare_fixture", fixture_parameters(research))
        assert row["state"] == "unknown", row
        assert service.snapshot()["state"] == "unavailable"
        disk = json.loads(service.path.read_text())["data"]["operations"]["fixture"]
        assert disk["intent"] and disk["job_id"] is None
        assert len(research.submissions) == 1 and research.cancellations
    finally:
        service.close()


def test_dispatch_intent_write_failure_never_submits_child(research, monkeypatch):
    service = ready(research)
    original = shadow_inference.atomic_json

    def fail_intent(path, envelope):
        if any(row["intent"] for row in envelope["data"]["operations"].values()):
            raise OSError("synthetic intent fsync failure")
        original(path, envelope)

    monkeypatch.setattr(shadow_inference, "atomic_json", fail_intent)
    try:
        row = operation(service, "fixture", "prepare_fixture", fixture_parameters(research))
        assert row["state"] == "unknown"
        assert not research.submissions
        saved = json.loads(service.path.read_text())["data"]["operations"]["fixture"]
        assert saved["intent"] is None
    finally:
        service.close()


def test_close_fences_irreversible_child_submission_and_retains_known_job(research):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def block_submit():
        entered.set()
        assert release.wait(5)

    research.on_submit = block_submit
    service = ready(research)
    service.submit("fixture", "prepare_fixture", fixture_parameters(research))
    assert entered.wait(3)

    def close():
        service.close()
        finished.set()

    closer = threading.Thread(target=close)
    closer.start()
    try:
        assert not finished.wait(0.05)
        release.set()
        closer.join(3)
        assert finished.is_set()
        assert len(research.submissions) == 1
        job_id = research.submissions[0]["id"]
        assert research.cancellations == [job_id]
        saved = json.loads(service.path.read_text())["data"]["operations"]["fixture"]
        assert saved["job_id"] == job_id and saved["state"] == "unknown"
    finally:
        release.set()
        closer.join(3)
        service.close()


def test_repeated_close_keeps_unjoined_worker_visible_until_it_retires(research, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = shadow_inference.prepare_candidate

    def held_hash(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(shadow_inference, "prepare_candidate", held_hash)
    service = ready(research)
    try:
        service.submit("held-fixture", "prepare_fixture", fixture_parameters(research))
        assert entered.wait(3)
        assert service.close()["worker_alive"] is True
        assert service.close()["worker_alive"] is True
        assert not research.submissions
    finally:
        release.set()
        service._thread.join(3)
        assert service.close()["worker_alive"] is False


def test_restart_requires_manual_reset_resume_and_rejects_old_child(research):
    service = ready(research)
    activate(service, research)
    research.running = True
    service.submit("predict", "predict", prediction(service))
    await_value(lambda: service.status("predict"), lambda row: row["state"] == "waiting")
    service.close()
    restored = ready(research, "core-b")
    try:
        assert restored.status("predict")["state"] == "unknown"
        doc = restored.snapshot()["deployment"]["document"]
        assert doc["paused"] and not doc["requests"]
        assert (
            operation(
                restored, "premature-resume", "resume", {"expected_revision": revision(restored)}
            )["state"]
            == "failed"
        )
        operation(
            restored,
            "reset",
            "reset_stream",
            {
                "stream": "preview",
                "instrument": INSTRUMENT,
                "reason": "manual_restart",
                "expected_revision": revision(restored),
            },
        )
        assert (
            operation(restored, "resume", "resume", {"expected_revision": revision(restored)})[
                "state"
            ]
            == "completed"
        )
        assert len(research.submissions) == 2
    finally:
        restored.close()


def test_deadline_expires_and_confirms_child_cancellation(research):
    now = [120001]
    service = ready(research, clock=lambda: now[0])
    try:
        activate(service, research)
        research.running = True
        service.submit("predict", "predict", prediction(service))
        await_value(lambda: service.status("predict"), lambda row: row["state"] == "waiting")
        now[0] = 170000
        row = completed(service, "predict")
        assert row["state"] == "expired" and row["cancellation"] == "confirmed"
        assert research.cancellations == [row["job_id"]]
        assert not service.snapshot()["deployment"]["document"]["requests"]
    finally:
        service.close()


def test_invalid_prediction_artifact_blocks_stream_without_acceptance(research):
    service = ready(research)
    try:
        activate(service, research)
        research.invalid_result = True
        row = operation(service, "predict", "predict", prediction(service))
        assert row["state"] == "failed"
        streams = service.snapshot()["deployment"]["document"]["streams"]
        assert streams["preview|" + INSTRUMENT]["health"] == "blocked"
        assert streams["preview|" + INSTRUMENT]["sequence"] == 0
    finally:
        service.close()


def test_corrupt_metadata_is_preserved_and_never_replaced_with_empty_state(research):
    service = ready(research)
    service.close()
    raw = b'{"corrupt":true}'
    service.path.write_bytes(raw)
    restored = ShadowInferenceService(research.root, research, core_epoch="core-b")
    assert restored.snapshot()["state"] == "unavailable"
    with pytest.raises((StorageError, RuntimeError)):
        restored.start()
    assert restored.path.read_bytes() == raw
    restored.close()


def test_retired_candidate_worker_cannot_write_new_core_metadata(research, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = shadow_inference.prepare_candidate

    def delayed(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(shadow_inference, "prepare_candidate", delayed)
    service = ready(research)
    service.submit("fixture", "prepare_fixture", fixture_parameters(research))
    assert entered.wait(3)
    service.close()
    restored = ready(research, "core-b")
    try:
        before = restored.path.read_bytes()
        release.set()
        service._thread.join(3)
        assert not service._thread.is_alive()
        assert restored.path.read_bytes() == before
        assert not research.submissions
    finally:
        release.set()
        restored.close()


def test_same_content_candidate_reuses_verified_registry_path_across_restart(research):
    service = ready(research)
    candidate_id = activate(service, research)
    before = service.snapshot()["candidates"][0]
    active_path = service.snapshot()["deployment"]["document"]["candidate"]["path"]
    try:
        row = operation(service, "fixture-again", "prepare_fixture", fixture_parameters(research))
        assert row["state"] == "completed" and row["candidate_id"] == candidate_id
        assert service.snapshot()["candidates"] == [before]
        assert before["path"] == active_path
        assert len(research.submissions) == 1
        assert len(list((research.root / "state/shadow-candidates").glob("*/candidate"))) == 2
    finally:
        service.close()
    restored = ready(research, "core-b")
    try:
        assert restored.snapshot()["deployment"]["document"]["candidate"]["path"] == active_path
    finally:
        restored.close()


def test_duplicate_fixture_inflight_keeps_original_path_for_both_jobs(research):
    service = ready(research)
    research.running = True
    try:
        params = fixture_parameters(research)
        service.submit("first", "prepare_fixture", params)
        await_value(lambda: service.status("first"), lambda row: row["state"] == "waiting")
        path = service.snapshot()["candidates"][0]["path"]
        service.submit("second", "prepare_fixture", params)
        await_value(lambda: service.status("second"), lambda row: row["state"] == "waiting")
        assert service.snapshot()["candidates"][0]["path"] == path
        assert len(research.submissions) == 1
        assert service.status("first")["job_id"] == service.status("second")["job_id"]
        for job in research.jobs.values():
            if job["kind"] == "nn_predict":
                job["state"] = "complete"
        assert completed(service, "first")["state"] == "completed"
        assert completed(service, "second")["state"] == "completed"
    finally:
        service.close()


def test_deadline_is_rechecked_after_large_result_verification(research, monkeypatch):
    now = [120001]
    service = ready(research, clock=lambda: now[0])
    try:
        activate(service, research)
        original = shadow_inference.ShadowDeployment.accept

        def slow_accept(self, *args, **kwargs):
            result = original(self, *args, **kwargs)
            now[0] = 170000
            return result

        monkeypatch.setattr(shadow_inference.ShadowDeployment, "accept", slow_accept)
        row = operation(service, "predict", "predict", prediction(service))
        assert row["state"] == "expired" and row["result"] is None
        assert research.cancellations == [row["job_id"]]
        stream = service.snapshot()["deployment"]["document"]["streams"]["preview|" + INSTRUMENT]
        assert stream["sequence"] == 0 and stream["health"] == "blocked"
        assert stream["accepted_at_ms"] is None
    finally:
        service.close()


def _nonregular_reader(root, result):
    from types import SimpleNamespace

    service = ShadowInferenceService(root, SimpleNamespace(root=root), core_epoch="fifo-core")
    result.put(service.snapshot()["state"])


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO boundary")
def test_fifo_metadata_never_blocks_constructor(tmp_path):
    root = tmp_path / "fifo-research"
    (root / "state").mkdir(parents=True)
    os.mkfifo(root / "state/shadow-inference.json")
    context = multiprocessing.get_context("spawn")
    result = context.Queue()
    process = context.Process(target=_nonregular_reader, args=(root, result))
    process.start()
    process.join(5)
    try:
        assert not process.is_alive(), "metadata open blocked on a FIFO"
        assert process.exitcode == 0 and result.get(timeout=1) == "unavailable"
    finally:
        if process.is_alive():
            process.kill()
            process.join(2)
        result.close()


@pytest.mark.parametrize(
    "field,value",
    [("job_id", True), ("core_epoch", []), ("intent", {"parameters": {}, "request_id": False})],
)
def test_valid_checksum_malformed_operation_metadata_is_fail_closed(research, field, value):
    service = ready(research)
    operation(service, "refresh", "refresh", {})
    service.close()
    document = json.loads(service.path.read_text())
    document["data"]["operations"]["refresh"][field] = value
    document["checksum"] = sha256(shadow_inference._json(document["data"])).hexdigest()
    raw = json.dumps(document).encode()
    service.path.write_bytes(raw)
    restored = ShadowInferenceService(research.root, research, core_epoch="core-b")
    assert restored.snapshot()["state"] == "unavailable"
    assert restored.path.read_bytes() == raw
    restored.close()


def test_unchanged_metadata_error_does_not_fsync_or_busy_poll_and_new_command_wakes(
    research, monkeypatch
):
    now = [120001]
    service = ready(research, clock=lambda: now[0])
    try:
        activate(service, research)
        research.running = True
        service.submit("predict", "predict", prediction(service))
        pending = await_value(
            lambda: service.status("predict"), lambda row: row["state"] == "waiting"
        )
        original_status = research.status
        attempts = []

        def unavailable(identity):
            if identity == pending["job_id"]:
                attempts.append(identity)
                raise OSError("persistent metadata error")
            return original_status(identity)

        monkeypatch.setattr(research, "status", unavailable)
        monkeypatch.setattr(
            research, "cancel", lambda _: (_ for _ in ()).throw(OSError("unknown cancellation"))
        )
        now[0] = 170000
        row = await_value(
            lambda: service.status("predict"),
            lambda item: item["error"] == "research_metadata_unavailable",
        )
        assert row["state"] == "unknown" and row["cancellation"] == "unknown"
        writes = []
        original_write = shadow_inference.atomic_json

        def counted_write(*args, **kwargs):
            writes.append(True)
            return original_write(*args, **kwargs)

        monkeypatch.setattr(shadow_inference, "atomic_json", counted_write)
        before = service.path.read_bytes()
        for _ in range(5):
            service._poll_one(row)
        assert not writes and service.path.read_bytes() == before
        count = len(attempts)
        threading.Event().wait(0.6)
        assert len(attempts) == count  # Cancellation-only work does not poll every .25 s.
        started = time.monotonic()
        assert operation(service, "wake", "refresh", {})["state"] == "completed"
        assert time.monotonic() - started < 1.5
        assert len(attempts) > count
    finally:
        service.close()


class RawResearch(FakeResearch):
    """Deterministic scheduler/artifact fake; actual child coverage is separate."""

    def __init__(self, root, training, now):
        super().__init__(root, training)
        self.now = now
        self.hold_prepare = self.hold_predict = self.lose_prepare = self.lose_predict = False

    def submit(self, kind, parameters):
        if kind != "nn_prepare":
            self.running, self.ack_loss = self.hold_predict, self.lose_predict
            return super().submit(kind, parameters)
        from omega.research.inference_input import build_prepared, load_window

        identity = uuid4().hex
        run = Path(parameters["training_run"])
        metadata = json.loads((run / "run.json").read_text())
        model = json.loads((run / "model.json").read_text())
        rows, rows_hash, lineage = load_window(parameters["dataset"], parameters["request"])
        result = build_prepared(
            parameters["request"],
            metadata["module"],
            rows,
            rows_hash,
            lineage,
            {"batch": [float(rows[0].volume) / model["volume_scale"]], "used_rows": [0]},
            snapshot_available_at_ms=self.now[0],
            prepared_at_ms=self.now[0],
        )
        path = self.root / "artifacts" / identity / "preparation/prepared.json"
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps(result), encoding="utf8")
        job = dict(
            id=identity,
            kind=kind,
            parameters=deepcopy(parameters),
            state="running" if self.hold_prepare else "complete",
            result_file=str(path),
        )
        self.jobs[identity] = job
        self.submissions.append(deepcopy(job))
        if self.lose_prepare:
            raise OSError("synthetic prepare ACK loss")
        return deepcopy(job)


@pytest.fixture
def raw_owner(tmp_path, actual_training):
    now = [120001]
    research = RawResearch(tmp_path / "research", actual_training, now)
    service = ready(research, clock=lambda: now[0])
    activate(service, research)
    values = {
        "expected_revision": revision(service),
        "stream": "preview",
        "instrument": INSTRUMENT,
        "sequence": 1,
        "open_time_ms": 60000,
        "deadline_ms": 180001,
        "dataset": actual_training[1]["parameters"]["dataset"],
        "retrospective_confirmed": True,
    }
    try:
        yield service, research, values, now
    finally:
        service.close()


def test_raw_preparation_then_frozen_prediction_advances_only_after_accept(raw_owner):
    service, research, values, _ = raw_owner
    research.hold_predict = True
    service.submit("raw", "predict_raw", values)
    waiting = await_value(
        lambda: service.status("raw"),
        lambda row: bool(row["state"] == "waiting" and row["intent"]["request_id"] is not None),
    )
    streams = service.snapshot()["deployment"]["document"]["streams"]
    assert streams["preview|" + INSTRUMENT]["sequence"] == 0
    assert [row["kind"] for row in research.submissions][-2:] == ["nn_prepare", "nn_predict"]
    assert waiting["result"]["preparation"]["job_id"] != waiting["job_id"]
    assert waiting["result"]["preparation"]["scope"] == "offline_retrospective_shadow_only"
    research.jobs[waiting["job_id"]]["state"] = "complete"
    result = completed(service, "raw")
    assert result["state"] == "completed", result
    assert result["result"]["preparation"]["reference_time_ms"] == 120000
    assert result["result"]["preparation"]["as_of_ms"] == 120001
    assert (
        service.snapshot()["deployment"]["document"]["streams"]["preview|" + INSTRUMENT]["sequence"]
        == 1
    )


@pytest.mark.parametrize("stage", ["prepare", "predict"])
def test_raw_stage_ack_loss_never_reuses_previous_child_or_automatically_submits(raw_owner, stage):
    service, research, values, _ = raw_owner
    setattr(research, "lose_" + stage, True)
    row = operation(service, "raw", "predict_raw", values)
    assert row["state"] == "unknown", row
    assert row["job_id"] is None
    before = len(research.submissions)
    assert service.submit("raw", "predict_raw", values) == service.status("raw")
    assert operation(service, "refresh", "refresh", {})["state"] == "completed"
    assert len(research.submissions) == before
    assert (
        service.snapshot()["deployment"]["document"]["streams"]["preview|" + INSTRUMENT]["sequence"]
        == 0
    )


@pytest.mark.parametrize("action", ["pause", "deadline", "reset"])
def test_raw_preparation_is_invalidated_without_cursor_advance(raw_owner, action):
    service, research, values, now = raw_owner
    research.hold_prepare = True
    service.submit("raw", "predict_raw", values)
    row = await_value(lambda: service.status("raw"), lambda item: item["state"] == "waiting")
    with pytest.raises(ValidationError, match="already owns"):
        service.submit("duplicate", "predict_raw", values)
    if action == "deadline":
        now[0] = values["deadline_ms"]
    elif action == "pause":
        operation(
            service,
            "pause-raw",
            "pause",
            {
                "expected_revision": revision(service),
                "reason": "test_pause",
            },
        )
    else:
        operation(
            service,
            "reset-raw",
            "reset_stream",
            {
                "expected_revision": revision(service),
                "reason": "test_reset",
                "stream": "preview",
                "instrument": INSTRUMENT,
            },
        )
    final = completed(service, "raw")
    assert final["state"] in {"failed", "expired"}, final
    assert row["job_id"] in research.cancellations
    assert (
        service.snapshot()["deployment"]["document"]["streams"]["preview|" + INSTRUMENT]["sequence"]
        == 0
    )
    assert [item["kind"] for item in research.submissions].count("nn_predict") == 1


def test_raw_prepared_artifact_tamper_before_prediction_accept_is_rejected(raw_owner):
    service, research, values, _ = raw_owner
    research.hold_predict = True
    service.submit("raw", "predict_raw", values)
    row = await_value(
        lambda: service.status("raw"),
        lambda item: bool(item["state"] == "waiting" and item["intent"]["request_id"] is not None),
    )
    path = Path(row["result"]["preparation"]["file"])
    artifact = json.loads(path.read_text())
    artifact["window"][0]["volume"] = "12345"
    path.write_text(json.dumps(artifact), encoding="utf8")
    research.jobs[row["job_id"]]["state"] = "complete"
    final = completed(service, "raw")
    assert final["state"] == "failed", final
    stream = service.snapshot()["deployment"]["document"]["streams"]["preview|" + INSTRUMENT]
    assert stream["sequence"] == 0 and stream["health"] != "ready"


def test_raw_restart_retains_unknown_operation_without_child_resubmit(raw_owner):
    service, research, values, now = raw_owner
    research.hold_prepare = True
    service.submit("raw", "predict_raw", values)
    await_value(lambda: service.status("raw"), lambda row: row["state"] == "waiting")
    service.close()
    count = len(research.submissions)
    restarted = ready(research, epoch="core-b", clock=lambda: now[0])
    try:
        assert restarted.snapshot()["deployment"]["document"]["paused"]
        assert restarted.status("raw")["state"] == "unknown"
        restarted.submit("raw", "predict_raw", values)
        assert len(research.submissions) == count
    finally:
        restarted.close()


def test_actual_raw_owner_demo_children_freeze_prepare_then_accept_once(actual_training):
    import sys
    from types import SimpleNamespace

    research, trained = actual_training
    before_modules = set(sys.modules)
    service = ShadowInferenceService(research.root, research, core_epoch=uuid4().hex)
    try:
        service.start()
        await_value(service.snapshot, lambda value: value["state"] == "ready")
        activate(service, SimpleNamespace(training=trained))
        before_jobs = {p.name for p in (research.root / "jobs").glob("*.json")}
        row = operation(
            service,
            "actual-raw",
            "predict_raw",
            {
                "expected_revision": revision(service),
                "stream": "preview",
                "instrument": INSTRUMENT,
                "sequence": 1,
                "open_time_ms": 60000,
                "deadline_ms": int(time.time() * 1000) + 60000,
                "dataset": trained["parameters"]["dataset"],
                "retrospective_confirmed": True,
            },
        )
        assert row["state"] == "completed", row
        assert row["result"]["scope"] == "offline_retrospective_shadow_only"
        proof = row["result"]["preparation"]
        assert proof["as_of_ms"] > proof["reference_time_ms"] == 120000
        assert proof["rows_sha256"] != proof["dataset_sha256"]
        assert research.status(proof["job_id"])["kind"] == "nn_prepare"
        assert research.status(row["job_id"])["kind"] == "nn_predict"
        after_jobs = {p.name for p in (research.root / "jobs").glob("*.json")}
        assert len(after_jobs - before_jobs) == 2
        assert service.submit("actual-raw", "predict_raw", row["parameters"]) == row
        assert {p.name for p in (research.root / "jobs").glob("*.json")} == after_jobs
        stream = service.snapshot()["deployment"]["document"]["streams"]["preview|" + INSTRUMENT]
        assert stream["sequence"] == 1 and stream["input_digest"] == proof["prepared_sha256"]
        assert not {"torch", "numpy", "scipy"} & (set(sys.modules) - before_modules)
        assert not any(
            name.startswith("omega_external_") for name in set(sys.modules) - before_modules
        )
    finally:
        service.close()


@pytest.mark.parametrize("ack_lost", [False, True])
def test_raw_second_stage_owner_write_failure_creates_no_prediction_child(
    raw_owner, monkeypatch, ack_lost
):
    service, research, values, now = raw_owner
    original = shadow_inference.atomic_json
    faulted = []

    def fail_second_intent(path, envelope):
        row = envelope["data"]["operations"].get("raw")
        if (
            not faulted
            and row
            and row["intent"]
            and row["intent"]["request_id"] is not None
            and row["job_id"] is None
        ):
            faulted.append(True)
            if ack_lost:
                original(path, envelope)
            raise OSError("synthetic second intent write/ACK failure")
        return original(path, envelope)

    monkeypatch.setattr(shadow_inference, "atomic_json", fail_second_intent)
    service.submit("raw", "predict_raw", values)
    row = completed(service, "raw")
    assert faulted and row["state"] == "unknown", row
    assert [job["kind"] for job in research.submissions].count("nn_prepare") == 1
    assert [job["kind"] for job in research.submissions].count("nn_predict") == 1
    assert (
        service.snapshot()["deployment"]["document"]["streams"]["preview|" + INSTRUMENT]["sequence"]
        == 0
    )
    service.close()
    count = len(research.submissions)
    restarted = ready(research, epoch="core-b", clock=lambda: now[0])
    try:
        assert restarted.snapshot()["deployment"]["document"]["paused"]
        assert restarted.status("raw")["state"] == "unknown"
        restarted.submit("raw", "predict_raw", values)
        assert len(research.submissions) == count
    finally:
        restarted.close()
