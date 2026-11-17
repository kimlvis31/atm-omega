import json
from pathlib import Path

import pytest

from omega.research import demo_instance
from omega.research.artifacts import child_path
from omega.research.neural import TrainingContext, inspect_module, read_training_run, train


def test_demo_training_records_and_read_only_viewer(dataset, tmp_path):
    root = tmp_path / "training"
    run = train(dataset, Path(demo_instance.__file__), root, {"config": {"epochs": 3}, "seed": 1})
    assert run["state"] == "complete"
    assert run["fit_scope"] == "train_only"
    assert run["evaluation"]["samples"] == 18
    view = read_training_run(root)
    assert len(view["metrics"]) == 7
    assert len(view["structures"]) == 1
    assert len(view["evaluations"]) == 18
    assert len(view["tensor_summaries"]) == 3
    assert (root / "metrics.csv").is_file()
    assert (root / "input.rows.csv").is_file()
    # A viewer reads data only, even if the selected plugin is removed.
    (root / "instance_module.py").unlink()
    assert read_training_run(root)["run"]["module"]["module_id"] == "omega.demo.linear_neuron"


def test_nn_compatibility_online_option_and_hash(dataset, tmp_path):
    class Incompatible:
        @staticmethod
        def describe():
            return demo_instance.describe() | {"api_version": 999}

    with pytest.raises(ValueError, match="incompatible"):
        inspect_module(Incompatible)
    with pytest.raises(ValueError, match="online learning"):
        train(dataset, Path(demo_instance.__file__), tmp_path / "online", {"online_learning": True})
    root = tmp_path / "training"
    train(dataset, Path(demo_instance.__file__), root, {"config": {"epochs": 1}})
    (root / "model.json").write_text("{}")
    with pytest.raises(ValueError, match="hash mismatch"):
        read_training_run(root)


def test_record_profiles_and_core_failure_policy(tmp_path):
    context = TrainingContext(tmp_path, recording={"profile": "lightweight"})
    context.emit_tensor_summary("a", {"value": 1})
    assert not (tmp_path / "tensor_summaries.jsonl").exists()
    context.emit_metric("loss", 1, step=0)
    assert (tmp_path / "metrics.jsonl").exists()
    with pytest.raises(ValueError):
        context.emit_metric("loss", float("nan"), step=1)
    standard = TrainingContext(tmp_path)
    standard.emit_tensor_summary("a", {"value": float("nan")})
    assert standard.warnings
    strict = TrainingContext(tmp_path, recording={"optional_detail_failure": "stop"})
    with pytest.raises(ValueError):
        strict.emit_tensor_summary("a", {"value": float("nan")})


def test_artifact_escape_and_unknown_graph_node(tmp_path):
    context = TrainingContext(tmp_path)
    for name in ("../escape", str(tmp_path.parent / "absolute")):
        with pytest.raises(ValueError, match="below"):
            child_path(tmp_path, name)
    with pytest.raises(ValueError, match="unknown node"):
        context.emit_structure({"nodes": [{"id": "A"}], "edges": [{"source": "A", "target": "B"}]})


def test_external_plugin_preparation_does_not_receive_test(dataset, tmp_path):
    source = Path(demo_instance.__file__).read_text()
    # Instrument the external module boundary, rather than relying on a mock host.
    source = source.replace(
        "self.scale = max(float(row.volume) for row in rows) or 1.0",
        "assert max(row.open_time_ms for row in rows) < 42 * 60000\n"
        "        self.scale = max(float(row.volume) for row in rows) or 1.0",
    )
    source = source.replace(
        "for epoch in range(self.completed_epochs, self.epochs):",
        "assert len(train) == 84 and len(validation) == 18\n"
        "        for epoch in range(self.completed_epochs, self.epochs):",
    )
    plugin = tmp_path / "plugin.py"
    plugin.write_text(source)
    run = train(dataset, plugin, tmp_path / "trained", {"config": {"epochs": 1}})
    assert run["state"] == "complete"
    events = [
        json.loads(line) for line in (tmp_path / "trained/events.jsonl").read_text().splitlines()
    ]
    assert any(
        event["kind"] == "preprocessing_fit" and event["data"]["split"] == "train"
        for event in events
    )


def test_cleanup_failure_is_never_published_as_complete(dataset, tmp_path):
    source = (
        Path(demo_instance.__file__)
        .read_text()
        .replace(
            "    def close(self, context):\n        pass",
            "    def close(self, context):\n        raise RuntimeError('synthetic cleanup error')",
        )
    )
    plugin = tmp_path / "close_failure.py"
    plugin.write_text(source)
    root = tmp_path / "run"
    with pytest.raises(RuntimeError, match="cleanup error"):
        train(dataset, plugin, root, {"config": {"epochs": 1}})
    assert read_training_run(root)["run"]["state"] == "failed"


def test_resume_preserves_original_and_matches_uninterrupted_training(dataset, tmp_path):
    plugin = Path(demo_instance.__file__)
    first = tmp_path / "first"
    train(dataset, plugin, first, {"config": {"epochs": 2}, "seed": 8})
    original = {
        path.relative_to(first): path.read_bytes() for path in first.rglob("*") if path.is_file()
    }
    resumed = tmp_path / "resumed"
    result = train(
        dataset,
        plugin,
        resumed,
        {
            "config": {"epochs": 5},
            "seed": 8,
            "resume": {"run": str(first), "checkpoint": "model.json"},
        },
    )
    complete = tmp_path / "complete"
    train(dataset, plugin, complete, {"config": {"epochs": 5}, "seed": 8})
    assert json.loads((resumed / "model.json").read_text()) == json.loads(
        (complete / "model.json").read_text()
    )
    assert result["resume"]["scope"] == "same_dataset_and_module_epoch_boundary"
    assert (resumed / "resume/model.json").read_bytes() == original[Path("model.json")]
    assert all((first / name).read_bytes() == value for name, value in original.items())
    assert len(read_training_run(resumed)["metrics"]) == 7  # Three more epochs and held-out test.


@pytest.mark.parametrize(
    "change,error",
    [
        ({"seed": 2}, "seed mismatch"),
        ({"config": {"epochs": 4, "learning_rate": 0.2}}, "config mismatch"),
        ({"online_learning": 1}, "boolean"),
    ],
)
def test_resume_rejects_incompatible_training_state(dataset, tmp_path, change, error):
    first = tmp_path / "first"
    train(dataset, Path(demo_instance.__file__), first, {"config": {"epochs": 1}, "seed": 1})
    parameters = {
        "config": {"epochs": 4},
        "seed": 1,
        "resume": {"run": str(first), "checkpoint": "model.json"},
    } | change
    with pytest.raises(ValueError, match=error):
        train(dataset, Path(demo_instance.__file__), tmp_path / "next", parameters)


@pytest.mark.parametrize(
    "fault,error",
    [
        ("tamper", "hash mismatch"),
        ("running", "writer cleanup"),
        ("unregistered", "registered checkpoint"),
        ("source", "identity/source mismatch"),
        ("dataset", "dataset/split/provenance mismatch"),
        ("cleanup", "writer cleanup is not confirmed"),
    ],
)
def test_resume_validates_provenance_before_restore(dataset, tmp_path, fault, error):
    first = tmp_path / "first"
    plugin = tmp_path / "instance.py"
    plugin.write_bytes(Path(demo_instance.__file__).read_bytes())
    train(dataset, plugin, first, {"config": {"epochs": 1}})
    metadata = json.loads((first / "run.json").read_text())
    if fault == "tamper":
        (first / "model.json").write_text("{}")
    elif fault == "running":
        metadata["state"] = "running"
    elif fault == "unregistered":
        metadata["artifacts"] = []
    elif fault == "source":
        plugin.write_text(plugin.read_text() + "\n# changed source\n")
    elif fault == "cleanup":
        metadata["cleanup_completed"] = False
    else:
        metadata["dataset"]["splits"]["train"][1] -= 60000
    (first / "run.json").write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match=error):
        train(
            dataset,
            plugin,
            tmp_path / "next",
            {
                "config": {"epochs": 3},
                "resume": {"run": str(first), "checkpoint": "model.json"},
            },
        )


def test_invalid_final_result_records_failure_instead_of_running(dataset, tmp_path):
    source = (
        Path(demo_instance.__file__)
        .read_text()
        .replace('"epochs": self.epochs,', '"epochs": float("nan"),')
    )
    plugin = tmp_path / "nonfinite.py"
    plugin.write_text(source)
    root = tmp_path / "run"
    with pytest.raises(ValueError):
        train(dataset, plugin, root, {"config": {"epochs": 1}})
    assert json.loads((root / "run.json").read_text())["state"] == "failed"


def test_missing_instance_method_still_closes_created_plugin(dataset, tmp_path):
    source = (
        Path(demo_instance.__file__)
        .read_text()
        .replace("    def evaluate(self, rows, context):", "    def unused(self, rows, context):")
    )
    source = source.replace(
        "    def close(self, context):\n        pass",
        "    def close(self, context):\n"
        "        context.emit_event('closed_after_validation_error')",
    )
    plugin = tmp_path / "incomplete.py"
    plugin.write_text(source)
    root = tmp_path / "run"
    with pytest.raises(ValueError, match="missing evaluate"):
        train(dataset, plugin, root, {"config": {"epochs": 1}})
    assert any(
        row["kind"] == "closed_after_validation_error" for row in read_training_run(root)["events"]
    )
