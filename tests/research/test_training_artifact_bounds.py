"""Training/resume reuse actual bounded artifact snapshots; fixture data only."""

import json
import os
import time
from pathlib import Path

import pytest

from omega.research import demo_instance, neural
from omega.research.artifacts import content_hash
from omega.research.neural import snapshot_module, train
from omega.research.service import ResearchService


@pytest.fixture
def original(dataset, tmp_path):
    source = tmp_path / "original"
    train(dataset, Path(demo_instance.__file__), source, {"config": {"epochs": 1}})
    return source


def test_train_oversize_module_rejected_before_dataset_or_plugin(dataset, tmp_path, monkeypatch):
    module = tmp_path / "large.py"
    module.write_bytes(b"x" * (neural.MAX_MODULE_BYTES + 1))
    monkeypatch.setattr(
        neural, "load_training_inputs", lambda *a: pytest.fail("oversize reached dataset")
    )
    monkeypatch.setattr(neural, "load_module", lambda *a: pytest.fail("oversize reached plugin"))
    with pytest.raises(ValueError, match="capacity"):
        train(dataset, module, tmp_path / "denied", {})
    assert not (tmp_path / "denied").exists()


def test_snapshot_source_mutation_rejected_before_execution(tmp_path, monkeypatch):
    module = tmp_path / "module.py"
    module.write_bytes(b"# original\n")
    before = module.read_bytes()
    copy = neural.copy_bounded_artifact

    def changed(source, destination, **kwargs):
        digest = copy(source, destination, **kwargs)
        with module.open("ab") as writer:
            writer.write(b"# modified\n")
        return digest

    monkeypatch.setattr(neural, "copy_bounded_artifact", changed)
    with pytest.raises(ValueError, match="changed while"):
        snapshot_module(module, tmp_path / "snapshot.py")
    assert (tmp_path / "snapshot.py").read_bytes() == before


@pytest.mark.parametrize("field", ["run", "checkpoint"])
def test_resume_oversize_rejected_before_restore(dataset, original, tmp_path, monkeypatch, field):
    if field == "run":
        monkeypatch.setattr(neural, "MAX_RUN_BYTES", 1)
    else:
        monkeypatch.setattr(neural, "MAX_CHECKPOINT_BYTES", 1)
    with pytest.raises(ValueError, match="capacity"):
        train(
            dataset,
            Path(demo_instance.__file__),
            tmp_path / "denied",
            {"config": {"epochs": 2}, "resume": {"run": str(original), "checkpoint": "model.json"}},
        )
    assert not (tmp_path / "denied/resume/model.json").exists()
    assert not (tmp_path / "denied/run.json").exists()


def link_or_skip(link, target, *, directory=False):
    try:
        link.symlink_to(target, target_is_directory=directory)
    except OSError:
        pytest.skip("Windows symlink creation unavailable")


@pytest.mark.parametrize("field", ["module", "run", "checkpoint", "root"])
def test_training_resume_links_rejected_before_resolution(dataset, original, tmp_path, field):
    module = Path(demo_instance.__file__)
    if field == "module":
        linked = tmp_path / "module.py"
        link_or_skip(linked, module)
        module = linked
    elif field == "root":
        linked = tmp_path / "linked"
        link_or_skip(linked, original, directory=True)
        original = linked
    else:
        path = original / ("run.json" if field == "run" else "model.json")
        target = path.with_name(path.name + ".original")
        path.rename(target)
        link_or_skip(path, target)
    with pytest.raises(ValueError, match="symlinks"):
        train(
            dataset,
            module,
            tmp_path / "denied",
            {"config": {"epochs": 2}, "resume": {"run": str(original), "checkpoint": "model.json"}},
        )
    assert not (tmp_path / "denied/run.json").exists()


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO validation")
@pytest.mark.parametrize("field", ["module", "run", "checkpoint"])
def test_training_resume_fifo_is_nonblocking(dataset, original, tmp_path, field):
    module = Path(demo_instance.__file__)
    if field == "module":
        module = tmp_path / "module.py"
        os.mkfifo(module)
    else:
        path = original / ("run.json" if field == "run" else "model.json")
        path.unlink()
        os.mkfifo(path)
    started = time.monotonic()
    with pytest.raises(ValueError, match="regular"):
        train(
            dataset,
            module,
            tmp_path / "denied",
            {"config": {"epochs": 2}, "resume": {"run": str(original), "checkpoint": "model.json"}},
        )
    assert time.monotonic() - started < 2
    assert not (tmp_path / "denied/run.json").exists()


def test_resume_source_changed_after_copy_is_never_restored(
    dataset, original, tmp_path, monkeypatch
):
    original_copy = neural.copy_bounded_artifact
    before = (original / "model.json").read_bytes()

    def changed(source, destination, **kwargs):
        copied = original_copy(source, destination, **kwargs)
        if Path(source) == original / "model.json":
            with Path(source).open("ab") as writer:
                writer.write(b" ")
        return copied

    monkeypatch.setattr(neural, "copy_bounded_artifact", changed)
    with pytest.raises(ValueError, match="resume checkpoint changed"):
        train(
            dataset,
            Path(demo_instance.__file__),
            tmp_path / "denied",
            {"config": {"epochs": 2}, "resume": {"run": str(original), "checkpoint": "model.json"}},
        )
    assert (tmp_path / "denied/resume/model.json").read_bytes() == before
    assert not (tmp_path / "denied/run.json").exists()


@pytest.mark.parametrize("kind", ["train", "nn_inspect"])
def test_scheduler_external_nn_source_is_not_resolved_before_validation(dataset, tmp_path, kind):
    module = tmp_path / "linked.py"
    link_or_skip(module, Path(demo_instance.__file__))
    service = ResearchService(tmp_path / "research")
    try:
        job = service.submit(
            kind, {"module": str(module), "dataset": str(dataset), "config": {"epochs": 1}}
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            row = service.status(job["id"])
            if row["state"] in {"complete", "failed", "cancelled", "interrupted"}:
                break
            time.sleep(0.02)
        assert row["state"] == "failed" and "symlinks" in row["error"]
    finally:
        service.close()


def test_inspection_uses_child_snapshot_and_preserves_original(tmp_path):
    module = tmp_path / "module.py"
    module.write_bytes(Path(demo_instance.__file__).read_bytes())
    before = module.read_bytes()
    service = ResearchService(tmp_path / "research")
    try:
        job = service.submit("nn_inspect", {"module": str(module)})
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            row = service.status(job["id"])
            if row["state"] in {"complete", "failed", "cancelled", "interrupted"}:
                break
            time.sleep(0.02)
        assert row["state"] == "complete", row
        frozen = Path(row["artifact"]) / "instance_module.py"
        assert frozen.read_bytes() == module.read_bytes() == before
        assert content_hash(frozen) == content_hash(module)
        assert (
            json.loads(Path(row["result_file"]).read_text())["module_id"]
            == "omega.demo.linear_neuron"
        )
    finally:
        service.close()
