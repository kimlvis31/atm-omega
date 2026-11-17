"""Actual research-child prediction plus data-only Paper-shadow ownership tests."""

import json
import multiprocessing
import os
import time
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from omega.research import demo_instance
from omega.research.artifacts import read_json, write_json
from omega.research.deployment import (
    ShadowDeployment,
    ShadowModelContract,
    _hash,
    load_candidate,
    prepare_candidate,
    verify_fixture,
)
from omega.research.neural import train
from omega.research.service import ResearchService

INSTRUMENT = "binance:usdm:BTCUSDT"


def _fifo_readers(path, result):
    """A child timeout makes a future blocking-open regression fail, not hang pytest."""
    from omega.research.deployment import _artifact_hash, _copy_artifact, _read

    rejected = []
    for operation in (
        lambda: _read(Path(path)),
        lambda: _artifact_hash(Path(path)),
        lambda: _copy_artifact(Path(path), Path(path).with_suffix(".copy")),
    ):
        try:
            operation()
        except (ValueError, OSError):
            rejected.append(True)
        else:
            rejected.append(False)
    result.put(rejected)


def completed(service, job):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        result = service.status(job["id"])
        if result["state"] in {"complete", "failed", "cancelled", "interrupted"}:
            assert result["state"] == "complete", result
            return result
        time.sleep(0.02)
    raise AssertionError("actual shadow prediction child did not finish")


@pytest.fixture
def trained(dataset, tmp_path):
    source = tmp_path / "training"
    train(dataset, Path(demo_instance.__file__), source, {"config": {"epochs": 1}, "device": "cpu"})
    return source


def contract(**changes):
    return ShadowModelContract(
        schema_id="omega.ohlcv.1m.v1",
        features=("normalized_volume",),
        lookback=1,
        input_shape=(None,),
        **changes,
    )


@pytest.fixture
def candidate(trained, tmp_path):
    return prepare_candidate(
        trained, tmp_path / "candidate", contract(), fixture_batch=[0.25], trusted_module=True
    )


@pytest.fixture
def verified(candidate, tmp_path):
    service = ResearchService(tmp_path / "research")
    job = completed(service, service.submit("nn_predict", candidate.parameters()))
    verify_fixture(candidate, job, research_root=service.root)
    yield candidate, service
    service.close()


def active(candidate):
    book = ShadowDeployment("core-a")
    book.activate(candidate, expected_revision=0)
    book.reset_stream(
        "paper-preview", INSTRUMENT, expected_revision=book.revision, reason="explicit_start"
    )
    return book


def request(book, **changes):
    values = {
        "sequence": 1,
        "open_time_ms": 60000,
        "as_of_ms": 120000,
        "now_ms": 120001,
        "deadline_ms": 170000,
        "input_digest": "c" * 64,
        "batch": [0.5],
        "schema_id": "omega.ohlcv.1m.v1",
        "features": ("normalized_volume",),
        "lookback": 1,
    }
    values.update(changes)
    return book.request("paper-preview", INSTRUMENT, **values)


def submit(book, service, req):
    job = service.submit("nn_predict", req["parameters"])
    book.bind_job(req["id"], job, research_root=service.root)
    return completed(service, job)


def test_prepare_is_data_only_and_training_never_activates(candidate, trained, monkeypatch):
    import omega.research.prediction as prediction

    assert candidate.verified is False
    book = ShadowDeployment("core-a")
    assert book.candidate is None and book.paused
    before = {
        path.relative_to(trained): path.read_bytes()
        for path in trained.rglob("*")
        if path.is_file()
    }
    monkeypatch.setattr(
        prediction,
        "PredictionSession",
        lambda *a, **kw: pytest.fail("host must not execute a model"),
    )
    second = prepare_candidate(
        trained, trained.parent / "another", contract(), fixture_batch=[0.25], trusted_module=True
    )
    assert not second.verified
    assert all((trained / key).read_bytes() == value for key, value in before.items())
    with pytest.raises(ValueError, match="fixture"):
        book.activate(candidate, expected_revision=0)
    assert book.candidate is None


def test_actual_child_fixture_activation_shadow_prediction_and_json_checkpoint(verified):
    candidate, service = verified
    assert load_candidate(candidate.path).verified
    book = active(candidate)
    req = request(book)
    job = submit(book, service, req)
    result = book.accept(req["id"], job, now_ms=120100)
    model = read_json(Path(candidate.path) / "bundle/model.json")
    assert result["values"] == [model["weight"] * 0.5 + model["bias"]]
    assert result["scope"] == "paper_shadow_only" and not result["no_signal"]
    assert result["input_digest"] == "c" * 64
    with pytest.raises(ValueError, match="duplicate"):
        book.accept(req["id"], job, now_ms=120101)
    snapshot = json.loads(json.dumps(book.snapshot()))
    restored = ShadowDeployment.restore(snapshot, core_epoch="core-b")
    assert restored.paused and restored.epoch != book.epoch
    assert restored.candidate.candidate_id == candidate.candidate_id
    with pytest.raises(ValueError, match="reset"):
        restored.resume(expected_revision=restored.revision)
    restored.reset_stream(
        "paper-preview", INSTRUMENT, expected_revision=restored.revision, reason="core_restart"
    )
    restored.resume(expected_revision=restored.revision)
    assert not restored.paused


@pytest.mark.parametrize(
    "options",
    [{"trusted_module": False}, {"device": "gpu"}, {"device": "auto"}, {"timeout_seconds": True}],
)
def test_candidate_requires_explicit_trust_cpu_and_bounded_timeout(trained, tmp_path, options):
    with pytest.raises(ValueError):
        prepare_candidate(
            trained,
            tmp_path / "rejected",
            contract(),
            fixture_batch=[0.25],
            **({"trusted_module": True} | options),
        )
    assert not (tmp_path / "rejected").exists()


def test_checkpoint_tampering_rejected_before_worker_or_activation(candidate):
    path = Path(candidate.path) / "bundle/model.json"
    path.write_text("{}")
    with pytest.raises(ValueError, match="changed"):
        load_candidate(candidate.path)


def test_fixture_expected_shape_and_result_identity_are_checked(trained, tmp_path):
    candidate = prepare_candidate(
        trained,
        tmp_path / "candidate",
        contract(),
        fixture_batch=[0.25],
        fixture_expected=[999],
        trusted_module=True,
    )
    service = ResearchService(tmp_path / "research")
    try:
        job = completed(service, service.submit("nn_predict", candidate.parameters()))
        with pytest.raises(ValueError, match="output mismatch"):
            verify_fixture(candidate, job, research_root=service.root)
        assert not candidate.verified
        wrong = deepcopy(job)
        wrong["parameters"]["batch"] = [0.5]
        with pytest.raises(ValueError, match="parameters"):
            verify_fixture(candidate, wrong, research_root=service.root)
    finally:
        service.close()


@pytest.mark.parametrize(
    "changes",
    [
        {"features": ("other",)},
        {"lookback": 2},
        {"schema_id": "different"},
        {"batch": [[0.5]]},
        {"batch": [True]},
        {"batch": [float("nan")]},
        {"sequence": True},
        {"open_time_ms": 60001},
        {"as_of_ms": 119999},
        {"deadline_ms": 120001},
        {"input_digest": "invalid"},
    ],
)
def test_request_rejects_schema_shape_clock_and_ambiguous_numbers(verified, changes):
    candidate, _service = verified
    book = active(candidate)
    before = book.snapshot()
    with pytest.raises((ValueError, TypeError)):
        request(book, **changes)
    assert book.snapshot() == before


def test_timeout_keeps_committed_cursor_and_requires_explicit_reset(verified):
    candidate, service = verified
    book = active(candidate)
    req = request(book)
    job = submit(book, service, req)
    with pytest.raises(ValueError, match="deadline"):
        book.accept(req["id"], job, now_ms=170000)
    row = next(iter(book.streams.values()))
    assert row["sequence"] == 0 and row["open_ms"] is None and row["health"] == "blocked"
    with pytest.raises(ValueError, match="unknown"):
        request(book)


def test_owner_tick_expires_queued_request_without_any_result(verified):
    candidate, _service = verified
    book = active(candidate)
    req = request(book)
    assert book.expire(now_ms=169999) == []
    assert book.expire(now_ms=170000) == [
        {"request_id": req["id"], "job_id": None, "research_root": None}
    ]
    assert not book.requests
    row = next(iter(book.streams.values()))
    assert row["sequence"] == 0 and row["health"] == "blocked"


def test_manual_pause_invalidates_inflight_result_before_explicit_reset(verified):
    candidate, service = verified
    book = active(candidate)
    req = request(book)
    job = submit(book, service, req)
    book.pause(expected_revision=book.revision, reason="user_pause")
    with pytest.raises(ValueError, match="late"):
        book.accept(req["id"], job, now_ms=120100)
    assert book.paused and next(iter(book.streams.values()))["sequence"] == 0
    with pytest.raises(ValueError, match="reset"):
        book.resume(expected_revision=book.revision)


@pytest.mark.parametrize(
    "prediction",
    [
        {"values": [True]},
        {"values": ["0.1"]},
        {"values": [[0.1]]},
        {"values": [None]},
        {"values": [0.1], "state": [0.1]},
    ],
)
def test_invalid_actual_artifact_gates_stream_without_advancing(verified, prediction):
    candidate, service = verified
    book = active(candidate)
    req = request(book)
    job = submit(book, service, req)
    path = Path(job["result_file"])
    result = read_json(path)
    result["prediction"] = prediction
    write_json(path, result)
    with pytest.raises(ValueError):
        book.accept(req["id"], job, now_ms=120100)
    row = next(iter(book.streams.values()))
    assert row["sequence"] == 0 and row["health"] == "blocked"


def test_nan_output_in_actual_child_artifact_is_blocked(verified):
    candidate, service = verified
    book = active(candidate)
    req = request(book)
    job = submit(book, service, req)
    path = Path(job["result_file"])
    result = read_json(path)
    result["prediction"]["values"] = [float("nan")]
    path.write_text(json.dumps(result), encoding="utf-8")
    with pytest.raises(ValueError):
        book.accept(req["id"], job, now_ms=120100)
    assert next(iter(book.streams.values()))["sequence"] == 0


def test_activation_and_reset_fence_already_running_jobs(verified):
    candidate, service = verified
    book = active(candidate)
    req = request(book)
    job = submit(book, service, req)
    book.activate(candidate, expected_revision=book.revision)
    with pytest.raises(ValueError, match="late"):
        book.accept(req["id"], job, now_ms=120100)
    assert book.streams == {}


def test_verified_fixture_artifact_hash_and_declared_shape_revalidated(verified):
    candidate, _service = verified
    path = Path(candidate.path) / "fixture-result.json"
    result = read_json(path)
    result["prediction"]["values"] = ["bad"]
    write_json(path, result)
    with pytest.raises(ValueError, match="fixture result changed"):
        active(candidate)


def test_pending_checkpoint_new_epoch_is_manual_and_late_response_cannot_publish(verified):
    candidate, service = verified
    book = active(candidate)
    req = request(book)
    job = submit(book, service, req)
    restored = ShadowDeployment.restore(
        json.loads(json.dumps(book.snapshot())), core_epoch="core-b"
    )
    assert restored.paused and restored.requests == {}
    with pytest.raises(ValueError, match="unknown"):
        restored.accept(req["id"], job, now_ms=120100)
    assert next(iter(restored.streams.values()))["sequence"] == 0


@pytest.mark.parametrize(
    "mutation",
    ["bool_sequence", "batch", "state", "digest", "deadline", "owner", "duplicate_stream"],
)
def test_valid_checksum_pending_corruption_cannot_be_restored(verified, mutation):
    candidate, _service = verified
    book = active(candidate)
    req = request(book)
    checkpoint = book.snapshot()
    pending = checkpoint["document"]["requests"][req["id"]]
    if mutation == "bool_sequence":
        pending["sequence"] = True
    elif mutation == "batch":
        pending["parameters"]["batch"] = [0.75]
    elif mutation == "state":
        pending["parameters"]["state"] = [[[1.0]]]
    elif mutation == "digest":
        pending["input_digest"] = "bad"
    elif mutation == "deadline":
        pending["deadline_ms"] = pending["issued_at_ms"]
    elif mutation == "owner":
        pending["research_root"] = "/changed-owner"
    else:
        another = deepcopy(pending)
        another["id"] = "f" * 32
        checkpoint["document"]["requests"][another["id"]] = another
    checkpoint["checksum"] = _hash(checkpoint["document"])
    with pytest.raises(ValueError):
        ShadowDeployment.restore(checkpoint, core_epoch="core-b")


def test_revision_conflict_and_external_mutation_cannot_change_controller(verified):
    candidate, _service = verified
    book = active(candidate)
    snapshot = book.snapshot()
    with pytest.raises(ValueError, match="revision"):
        book.activate(candidate, expected_revision=0)
    assert book.snapshot() == snapshot
    req = request(book)
    req["parameters"]["batch"][0] = 999
    assert book.requests[req["id"]]["parameters"]["batch"] == [0.5]


def test_gap_requires_reset_and_streams_do_not_share_rnn_state(verified):
    candidate, service = verified
    book = active(candidate)
    req = request(book)
    job = submit(book, service, req)
    book.accept(req["id"], job, now_ms=120100)
    with pytest.raises(ValueError, match="sequence"):
        request(
            book,
            sequence=2,
            open_time_ms=180000,
            as_of_ms=240000,
            now_ms=240001,
            deadline_ms=290000,
        )
    book.reset_stream(
        "another-preview", INSTRUMENT, expected_revision=book.revision, reason="explicit_start"
    )
    assert book.streams["another-preview|" + INSTRUMENT]["sequence"] == 0
    assert book.streams["paper-preview|" + INSTRUMENT]["sequence"] == 1


@pytest.mark.parametrize(
    "field,value", [("sequence", True), ("state_hash", "0" * 64), ("open_ms", 120001)]
)
def test_valid_checksum_stream_tampering_rejected(verified, field, value):
    candidate, _service = verified
    book = active(candidate)
    checkpoint = book.snapshot()
    row = next(iter(checkpoint["document"]["streams"].values()))
    row[field] = value
    checkpoint["checksum"] = _hash(checkpoint["document"])
    with pytest.raises(ValueError):
        ShadowDeployment.restore(checkpoint, core_epoch="core-b")


def test_null_no_signal_must_be_explicit_and_cannot_mix_partial_null():
    schema = contract(allow_no_signal=True)
    assert schema.validate_prediction({"values": [None]}, 1)
    assert not schema.validate_prediction({"values": [0.0]}, 1)
    with pytest.raises(ValueError, match="partial null"):
        schema.validate_prediction({"values": [None, 0.0]}, 2)
    with pytest.raises(ValueError):
        contract().validate_prediction({"values": [None]}, 1)


def test_explicit_shape_contract_matches_feature_width_and_lookback():
    with pytest.raises(ValueError, match="lookback"):
        replace(contract(), input_shape=(None, 2, 1))
    with pytest.raises(ValueError):
        replace(contract(), state_shape=(1, 1, 1))


@pytest.mark.parametrize("field,value", [("cleanup_completed", False), ("state", "running")])
def test_unfinished_training_is_never_a_candidate(trained, tmp_path, field, value):
    path = trained / "run.json"
    run = read_json(path)
    run[field] = value
    write_json(path, run)
    with pytest.raises(ValueError, match="completed"):
        prepare_candidate(
            trained, tmp_path / "bad", contract(), fixture_batch=[0.25], trusted_module=True
        )


@pytest.mark.parametrize("mutation", ["core_version", "feature_order", "lookback"])
def test_candidate_metadata_rejects_version_feature_and_lookback_mismatch(
    trained, tmp_path, mutation
):
    path = trained / "run.json"
    run = read_json(path)
    if mutation == "core_version":
        run["module"]["core_version"] = "incompatible"
    elif mutation == "feature_order":
        run["module"]["input"]["features"] = ["other_feature"]
    else:
        run["config"]["lookback"] = 2
    write_json(path, run)
    with pytest.raises(ValueError):
        prepare_candidate(
            trained, tmp_path / "rejected", contract(), fixture_batch=[0.25], trusted_module=True
        )


def test_actual_child_no_signal_is_normal_only_with_declared_null_contract(dataset, tmp_path):
    module = tmp_path / "no_signal.py"
    text = (
        Path(demo_instance.__file__)
        .read_text()
        .replace(
            'return {"values": [self.weight * value + self.bias for value in batch], '
            '"state": state}',
            'return {"values": [None for value in batch], "state": None}',
        )
    )
    module.write_text(text)
    source = tmp_path / "training"
    train(dataset, module, source, {"config": {"epochs": 1}, "device": "cpu"})
    candidate = prepare_candidate(
        source,
        tmp_path / "candidate",
        contract(allow_no_signal=True),
        fixture_batch=[0.25],
        fixture_expected=[None],
        trusted_module=True,
    )
    service = ResearchService(tmp_path / "research")
    try:
        job = completed(service, service.submit("nn_predict", candidate.parameters()))
        verify_fixture(candidate, job, research_root=service.root)
        book = active(candidate)
        req = request(book)
        result = book.accept(req["id"], submit(book, service, req), now_ms=120100)
        assert result["no_signal"] and result["values"] == [None]
        row = next(iter(book.streams.values()))
        assert row["sequence"] == 1 and row["reason"] == "no_signal"
        assert row["input_digest"] == "c" * 64 and row["as_of_ms"] == 120000
    finally:
        service.close()


def test_stateful_actual_child_state_commit_reset_and_contiguous_cursor(dataset, tmp_path):
    module = tmp_path / "recurrent.py"
    text = Path(demo_instance.__file__).read_text()
    text = text.replace('"stateful_inference": False', '"stateful_inference": True')
    text = text.replace(
        'return {"values": [self.weight * value + self.bias for value in batch], "state": state}',
        'return {"values": [self.weight * value + self.bias for value in batch], '
        '"state": [[[1.0 if state is None else state[0][0][0] + 1.0]]]}',
    )
    module.write_text(text)
    source = tmp_path / "training"
    train(dataset, module, source, {"config": {"epochs": 1}, "device": "cpu"})
    schema = contract(state_shape=(1, 1, 1), state_version="hidden.v1")
    candidate = prepare_candidate(
        source, tmp_path / "candidate", schema, fixture_batch=[0.25], trusted_module=True
    )
    service = ResearchService(tmp_path / "research")
    try:
        job = completed(service, service.submit("nn_predict", candidate.parameters()))
        verify_fixture(candidate, job, research_root=service.root)
        book = active(candidate)
        first = request(book)
        assert first["parameters"]["state"] is None
        job = submit(book, service, first)
        book.accept(first["id"], job, now_ms=120100)
        assert next(iter(book.streams.values()))["state"] == [[[1.0]]]
        second = request(
            book,
            sequence=2,
            open_time_ms=120000,
            as_of_ms=180000,
            now_ms=180001,
            deadline_ms=230000,
        )
        assert second["parameters"]["state"] == [[[1.0]]]
        job = submit(book, service, second)
        book.accept(second["id"], job, now_ms=180100)
        assert next(iter(book.streams.values()))["state"] == [[[2.0]]]
        with pytest.raises(ValueError, match="sequence"):
            request(
                book,
                sequence=3,
                open_time_ms=240000,
                as_of_ms=300000,
                now_ms=300001,
                deadline_ms=350000,
            )
        epoch = next(iter(book.streams.values()))["epoch"]
        book.reset_stream(
            "paper-preview", INSTRUMENT, expected_revision=book.revision, reason="gap_reset"
        )
        assert next(iter(book.streams.values()))["state"] is None
        assert next(iter(book.streams.values()))["epoch"] != epoch
    finally:
        service.close()


def test_metadata_reader_uses_shared_actual_fd_reader(tmp_path, monkeypatch):
    from omega.research.deployment import _read

    path = tmp_path / "metadata.json"
    path.write_text('{"known": 1}')
    monkeypatch.setattr(
        Path, "read_text", lambda *a, **kw: pytest.fail("path reader reopens metadata")
    )
    assert _read(path) == {"known": 1}


@pytest.mark.parametrize("operation", ["metadata", "hash", "copy"])
def test_nonregular_directory_is_rejected_without_creating_candidate(tmp_path, operation):
    from omega.research.deployment import _artifact_hash, _copy_artifact, _read

    source = tmp_path / "directory"
    source.mkdir()
    target = tmp_path / "copy"
    operations = {
        "metadata": lambda: _read(source),
        "hash": lambda: _artifact_hash(source),
        "copy": lambda: _copy_artifact(source, target),
    }
    with pytest.raises((ValueError, OSError)):
        operations[operation]()
    assert source.is_dir() and not target.exists()


def test_actual_fd_metadata_byte_bound_survives_incorrect_stat_size(tmp_path, monkeypatch):
    from omega.research import artifacts
    from omega.research.deployment import _read

    path = tmp_path / "metadata.json"
    path.write_bytes(b" " * 2_097_153)
    original = artifacts.os.fstat

    def stale_size(descriptor):
        details = original(descriptor)
        return SimpleNamespace(
            st_mode=details.st_mode,
            st_size=0,
            st_file_attributes=getattr(details, "st_file_attributes", 0),
        )

    monkeypatch.setattr(artifacts.os, "fstat", stale_size)
    with pytest.raises(ValueError, match="capacity"):
        _read(path)


@pytest.mark.parametrize("operation", ["hash", "copy"])
def test_artifact_actual_read_bound_survives_incorrect_stat_size(tmp_path, monkeypatch, operation):
    from omega.research import deployment

    source, target = tmp_path / "artifact", tmp_path / "copy"
    source.write_bytes(b"x" * 64)
    original = deployment.os.fstat

    def stale_size(descriptor):
        details = original(descriptor)
        return SimpleNamespace(
            st_mode=details.st_mode,
            st_size=0,
            st_file_attributes=getattr(details, "st_file_attributes", 0),
        )

    monkeypatch.setattr(deployment, "MAX_ARTIFACT_BYTES", 32)
    monkeypatch.setattr(deployment.os, "fstat", stale_size)
    with pytest.raises(ValueError, match="bounded"):
        deployment._artifact_hash(source) if operation == "hash" else deployment._copy_artifact(
            source, target
        )
    assert source.read_bytes() == b"x" * 64


def test_copy_is_exclusive_and_preserves_existing_bytes(tmp_path):
    from omega.research.deployment import _copy_artifact

    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"original")
    target.write_bytes(b"retained")
    with pytest.raises(FileExistsError):
        _copy_artifact(source, target)
    assert target.read_bytes() == b"retained" and source.read_bytes() == b"original"


@pytest.mark.parametrize("operation", ["hash", "copy"])
def test_hash_and_copy_read_the_opened_descriptor_after_path_replacement(
    tmp_path, monkeypatch, operation
):
    from hashlib import sha256

    from omega.research import deployment

    source, retained, target = tmp_path / "source", tmp_path / "retained", tmp_path / "copy"
    source.write_bytes(b"original")
    original = deployment._status_descriptor

    def replace_after_open(path):
        descriptor = original(path)
        source.rename(retained)
        source.write_bytes(b"replacement")
        return descriptor

    monkeypatch.setattr(deployment, "_status_descriptor", replace_after_open)
    if operation == "hash":
        assert deployment._artifact_hash(source) == sha256(b"original").hexdigest()
    else:
        deployment._copy_artifact(source, target)
        assert target.read_bytes() == b"original"
    assert source.read_bytes() == b"replacement" and retained.read_bytes() == b"original"


@pytest.mark.skipif(
    not hasattr(os, "mkfifo"), reason="OS has no POSIX FIFO; Linux validation required"
)
def test_fifo_metadata_hash_and_copy_reject_without_waiting_for_writer(tmp_path):
    fifo = tmp_path / "metadata.json"
    os.mkfifo(fifo)
    context = multiprocessing.get_context("spawn")
    result = context.Queue()
    process = context.Process(target=_fifo_readers, args=(str(fifo), result))
    process.start()
    try:
        process.join(5)
        assert not process.is_alive(), "regular-file validation must precede any blocking FIFO read"
        assert process.exitcode == 0
        assert result.get(timeout=1) == [True, True, True]
        assert fifo.exists() and not fifo.with_suffix(".copy").exists()
    finally:
        if process.is_alive():
            process.kill()
            process.join(5)
        result.close()
        result.join_thread()
