"""Actual artifact IO boundaries; no accounts, market network or fake framework."""

import os
import time
from hashlib import sha256
from pathlib import Path

import pytest

from omega.research import artifacts, demo_instance, prediction
from omega.research.artifacts import (
    MAX_ARTIFACT_BYTES,
    bounded_artifact_hash,
    copy_bounded_artifact,
    validate_artifact_path,
)
from omega.research.neural import train
from omega.research.prediction import PredictionSession
from omega.research.service import ResearchService


@pytest.fixture
def trained(dataset, tmp_path):
    source = tmp_path / "training"
    run = train(dataset, Path(demo_instance.__file__), source, {"config": {"epochs": 1}})
    return source, run


def test_bounded_hash_and_exclusive_copy_preserve_existing_bytes(tmp_path):
    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"12345678")
    expected = sha256(source.read_bytes()).hexdigest()
    assert bounded_artifact_hash(source, max_bytes=8) == expected
    assert copy_bounded_artifact(source, target, max_bytes=8) == expected
    assert source.read_bytes() == target.read_bytes() == b"12345678"
    with pytest.raises(FileExistsError):
        copy_bounded_artifact(source, target, max_bytes=8)
    assert target.read_bytes() == b"12345678"
    for invalid in (True, 0, MAX_ARTIFACT_BYTES + 1):
        with pytest.raises(ValueError):
            bounded_artifact_hash(source, max_bytes=invalid)


def test_oversize_is_rejected_before_destination_creation(tmp_path):
    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"123456789")
    with pytest.raises(ValueError, match="capacity"):
        copy_bounded_artifact(source, target, max_bytes=8)
    assert not target.exists() and source.read_bytes() == b"123456789"


@pytest.mark.parametrize("operation", ["hash", "copy"])
def test_actual_source_growth_is_bounded_during_streaming(tmp_path, monkeypatch, operation):
    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"12345")
    original = artifacts._bounded_artifact_chunks

    def growing(stream, maximum):
        for chunk in original(stream, maximum):
            yield chunk
            with source.open("ab") as writer:
                writer.write(b"6789")

    monkeypatch.setattr(artifacts, "_bounded_artifact_chunks", growing)
    with pytest.raises(ValueError, match="grew"):
        if operation == "hash":
            bounded_artifact_hash(source, max_bytes=8)
        else:
            copy_bounded_artifact(source, target, max_bytes=8)
    if operation == "copy":
        assert target.read_bytes() == b"12345"


def test_source_replacement_is_detected_after_copy(tmp_path, monkeypatch):
    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"original")
    replacement = tmp_path / "replacement"
    replacement.write_bytes(b"replaced")
    original = artifacts._bounded_artifact_chunks

    def replacing(stream, maximum):
        yield from original(stream, maximum)
        try:
            os.replace(replacement, source)
        except PermissionError as error:
            if os.name == "nt" and error.winerror == 5:
                pytest.skip("Windows active-handle file replacement unavailable")
            raise

    monkeypatch.setattr(artifacts, "_bounded_artifact_chunks", replacing)
    with pytest.raises(ValueError, match="changed"):
        copy_bounded_artifact(source, target, max_bytes=16)
    assert target.read_bytes() == b"original"
    assert source.read_bytes() == b"replaced"


@pytest.mark.parametrize("field", ["module", "checkpoint", "run"])
def test_prediction_rejects_oversize_before_snapshot_or_plugin(
    trained, tmp_path, monkeypatch, field
):
    source, run = trained
    destination = tmp_path / "denied"
    if field == "module":
        (source / "instance_module.py").write_bytes(b"x" * (prediction.MAX_MODULE_BYTES + 1))
    elif field == "checkpoint":
        monkeypatch.setattr(prediction, "MAX_CHECKPOINT_BYTES", 1)
    else:
        (source / "run.json").write_bytes(b" " * (prediction.MAX_RUN_BYTES + 1))
    monkeypatch.setattr(
        prediction, "load_module", lambda *a: pytest.fail("oversize reached plugin")
    )
    with pytest.raises(ValueError, match="capacity"):
        PredictionSession(source, destination, trusted_module=True)
    assert not destination.exists()
    assert prediction.MAX_MODULE_BYTES == 1024 * 1024
    assert run["model"]["file"]


def make_link(link, target, *, directory=False):
    try:
        link.symlink_to(target, target_is_directory=directory)
    except OSError:
        pytest.skip("Windows symlink creation unavailable")


@pytest.mark.parametrize("field", ["module", "checkpoint", "run", "parent"])
def test_prediction_rejects_link_before_resolving_or_plugin(trained, tmp_path, monkeypatch, field):
    source, run = trained
    if field == "parent":
        link = tmp_path / "linked"
        make_link(link, source, directory=True)
        source = link
    else:
        path = (
            source
            / {
                "module": "instance_module.py",
                "checkpoint": run["model"]["file"],
                "run": "run.json",
            }[field]
        )
        target = path.with_name(path.name + ".original")
        path.rename(target)
        make_link(path, target)
    monkeypatch.setattr(prediction, "load_module", lambda *a: pytest.fail("symlink reached plugin"))
    with pytest.raises(ValueError, match="symlinks"):
        PredictionSession(source, tmp_path / "denied", trusted_module=True)
    assert not (tmp_path / "denied").exists()


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO validation")
@pytest.mark.parametrize("field", ["module", "checkpoint", "run"])
def test_prediction_fifo_never_blocks_before_plugin(trained, tmp_path, monkeypatch, field):
    source, run = trained
    path = (
        source
        / {"module": "instance_module.py", "checkpoint": run["model"]["file"], "run": "run.json"}[
            field
        ]
    )
    path.unlink()
    os.mkfifo(path)
    monkeypatch.setattr(prediction, "load_module", lambda *a: pytest.fail("FIFO reached plugin"))
    started = time.monotonic()
    with pytest.raises(ValueError, match="regular"):
        PredictionSession(source, tmp_path / "denied", trusted_module=True)
    assert time.monotonic() - started < 2
    assert not (tmp_path / "denied").exists()


def test_original_change_after_copy_blocks_plugin(trained, tmp_path, monkeypatch):
    source, _ = trained
    original = prediction.copy_bounded_artifact

    def changing(path, destination, **kwargs):
        result = original(path, destination, **kwargs)
        if Path(path).name == "instance_module.py":
            with Path(path).open("ab") as writer:
                writer.write(b"\n")
        return result

    monkeypatch.setattr(prediction, "copy_bounded_artifact", changing)
    monkeypatch.setattr(
        prediction, "load_module", lambda *a: pytest.fail("changed original reached plugin")
    )
    with pytest.raises(ValueError, match="changed while snapshotting"):
        PredictionSession(source, tmp_path / "denied", trusted_module=True)


def test_link_parent_traversal_is_not_hidden_by_normalization(tmp_path):
    with pytest.raises(ValueError, match="traversal"):
        validate_artifact_path(tmp_path / "anything" / ".." / "file")


def test_scheduler_does_not_resolve_link_away_before_prediction(trained, tmp_path):
    source, _ = trained
    link = tmp_path / "linked"
    make_link(link, source, directory=True)
    service = ResearchService(tmp_path / "research")
    try:
        job = service.submit(
            "nn_predict", {"training_run": str(link), "batch": [0], "trusted_module": True}
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            row = service.status(job["id"])
            if row["state"] in {"complete", "failed", "cancelled", "interrupted"}:
                break
            time.sleep(0.02)
        assert row["state"] == "failed" and "symlinks" in row["error"]
        assert not (Path(row.get("artifact", "")) / "prediction/prediction.json").is_file()
    finally:
        service.close()
