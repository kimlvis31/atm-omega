"""Real optional PyTorch tests, never a fake tensor implementation."""

import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from omega.research import pytorch_instance
from omega.research.artifacts import read_json
from omega.research.datasets import load_dataset, select_split
from omega.research.neural import TrainingContext, inspect_module, train
from omega.research.service import ResearchService


def test_optional_torch_descriptor_does_not_import_framework():
    descriptor = inspect_module(pytorch_instance)
    assert descriptor["framework"] == "pytorch"
    assert descriptor["capabilities"]["resume_training"]
    assert descriptor["input"]["fit_scope"] == "train_only"


@pytest.fixture
def torch():
    return pytest.importorskip(
        "torch", reason="optional actual PyTorch environment is not installed"
    )


@pytest.mark.parametrize("architecture", ["mlp", "gru"])
def test_actual_torch_epoch_resume_matches_uninterrupted_cpu(
    dataset, tmp_path, torch, architecture
):
    config = {
        "architecture": architecture,
        "epochs": 2,
        "batch_size": 7,
        "lookback": 3,
        "hidden_size": 5,
        "checkpoint_every": 1,
    }
    plugin = Path(pytorch_instance.__file__)
    first = tmp_path / "first"
    parameters = {"config": config, "seed": 11, "device": "cpu"}
    initial = train(dataset, plugin, first, parameters)
    assert initial["device"]["used"] == "cpu"
    assert initial["evaluation"]["samples"] == 12  # 9 bars per instrument minus lookback 3.
    resumed = tmp_path / "resumed"
    resumed_run = train(
        dataset,
        plugin,
        resumed,
        parameters
        | {
            "config": config | {"epochs": 4},
            "resume": {"run": str(first), "checkpoint": "checkpoints/epoch_00002.pt"},
        },
    )
    complete = tmp_path / "complete"
    train(dataset, plugin, complete, parameters | {"config": config | {"epochs": 4}})
    actual = torch.load(resumed / "checkpoints/epoch_00004.pt", weights_only=True)
    expected = torch.load(complete / "checkpoints/epoch_00004.pt", weights_only=True)
    for name in ("network", "head"):
        if actual[name] is not None:
            assert actual[name].keys() == expected[name].keys()
            for key in actual[name]:
                assert torch.equal(actual[name][key], expected[name][key]), key
    assert torch.equal(actual["sampler_rng"], expected["sampler_rng"])
    assert actual["completed_epochs"] == 4
    assert resumed_run["evaluation"] == read_json(complete / "run.json")["evaluation"]
    assert resumed_run["resume"]["checkpoint"]["metadata"]["boundary"] == "epoch"


def test_gru_predict_state_matches_chunked_sequence_and_validates_shapes(tmp_path, torch):
    context = TrainingContext(tmp_path)
    context.device_requested = "cpu"
    instance = pytorch_instance.create(
        {"architecture": "gru", "lookback": 4, "hidden_size": 3}, context
    )
    batch = [
        [[0.1, 0.2, -0.1, 0.3], [0.2, 0.3, -0.2, 0.4], [0.3, 0.4, -0.1, 0.5], [0.4, 0.5, -0.2, 0.6]]
    ]
    full = instance.predict(batch)
    first = instance.predict([batch[0][:2]])
    second = instance.predict([batch[0][2:]], first["state"])
    assert second["values"] == pytest.approx(full["values"], abs=1e-7)
    assert torch.allclose(torch.tensor(second["state"]), torch.tensor(full["state"]), atol=1e-7)
    with pytest.raises(ValueError, match="state.*shape"):
        instance.predict(batch, [[[1.0]]])
    with pytest.raises(ValueError, match="finite"):
        instance.predict([[[float("nan"), 0, 0, 0]]])
    with pytest.raises(ValueError, match="shape"):
        instance.predict([[1, 2, 3, 4]])
    instance.close(context)


def test_windows_never_cross_instrument_gaps_or_heldout_split(dataset, tmp_path, torch):
    manifest, rows = load_dataset(dataset)
    context = TrainingContext(tmp_path)
    context.device_requested = "cpu"
    instance = pytorch_instance.create({"lookback": 3}, context)
    training = select_split(rows, manifest["splits"], "train")
    instance.prepare(training, context)
    maximum = instance.volume_scale
    train_samples = instance.transform(training, "train", context)
    validation = instance.transform(
        select_split(rows, manifest["splits"], "validation"), "validation", context
    )
    assert all(sample[3] < manifest["splits"]["train"][1] for sample in train_samples)
    assert all(
        manifest["splits"]["validation"][0] + 3 * 60000 <= sample[3] for sample in validation
    )
    assert instance.volume_scale == maximum
    gapped = [row for row in training if row.open_time_ms != 600000]
    samples = instance.transform(gapped, "train", context)
    assert not any(600000 <= sample[3] <= 780000 for sample in samples)
    instance.close(context)


def test_explicit_gpu_is_not_silently_cpu_fallback(tmp_path, torch):
    if torch.cuda.is_available():
        pytest.skip("this rejection case requires a CPU-only runtime")
    context = TrainingContext(tmp_path)
    context.device_requested = "gpu"
    with pytest.raises(ValueError, match="CUDA is unavailable"):
        pytorch_instance.create({}, context)


def test_failed_training_retains_registered_epoch_for_resume(dataset, tmp_path, torch):
    plugin = tmp_path / "fails_once.py"
    source = (
        Path(pytorch_instance.__file__)
        .read_text()
        .replace(
            "            self.completed_epochs = epoch + 1",
            "            if epoch == 1 and context.root.name == 'failed':\n"
            "                raise RuntimeError('injected after first durable epoch')\n"
            "            self.completed_epochs = epoch + 1",
        )
    )
    plugin.write_text(source)
    failed = tmp_path / "failed"
    config = {"epochs": 2, "lookback": 3}
    with pytest.raises(RuntimeError, match="durable epoch"):
        train(dataset, plugin, failed, {"config": config, "device": "cpu"})
    metadata = json.loads((failed / "run.json").read_text())
    assert metadata["state"] == "failed"
    assert metadata["artifacts"][0]["file"] == "checkpoints/epoch_00001.pt"
    result = train(
        dataset,
        plugin,
        tmp_path / "resumed",
        {
            "config": config,
            "device": "cpu",
            "resume": {"run": str(failed), "checkpoint": "checkpoints/epoch_00001.pt"},
        },
    )
    assert result["state"] == "complete"


def test_module_preprocessing_does_not_fit_validation(tmp_path, torch):
    context = TrainingContext(tmp_path)
    context.device_requested = "cpu"
    instance = pytorch_instance.create({"lookback": 2}, context)
    rows = [
        SimpleNamespace(
            instrument="A", open_time_ms=i * 60000, open=10, high=11, low=9, close=10, volume=i + 1
        )
        for i in range(4)
    ]
    instance.prepare(rows, context)
    scale = instance.volume_scale
    later = [SimpleNamespace(**(vars(row) | {"volume": 1000000})) for row in rows]
    instance.transform(later, "validation", context)
    assert instance.volume_scale == scale
    assert instance.features(later)[0][-1] > 1
    instance.close(context)


def test_actual_torch_background_jobs_train_resume_and_predict(dataset, tmp_path, torch):
    service = ResearchService(tmp_path / "service")

    def completed(job):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            status = service.status(job["id"])
            if status["state"] in {"complete", "failed", "cancelled", "interrupted"}:
                assert status["state"] == "complete", status
                return status
            time.sleep(0.05)
        raise AssertionError(f"actual torch background job did not finish: {status}")

    parameters = {
        "dataset": str(dataset),
        "builtin": "pytorch_return",
        "device": "cpu",
        "config": {"epochs": 1, "lookback": 3, "architecture": "gru"},
    }
    try:
        first = completed(service.submit("train", parameters))
        training_run = str(Path(first["result_file"]).parent)
        resumed = completed(
            service.submit(
                "train",
                parameters
                | {
                    "config": parameters["config"] | {"epochs": 2},
                    "resume": {"run": training_run, "checkpoint": "checkpoints/epoch_00001.pt"},
                },
            )
        )
        assert resumed["result"]["module_id"] == "omega.example.pytorch_return"
        result = completed(
            service.submit(
                "nn_predict",
                {
                    "training_run": str(Path(resumed["result_file"]).parent),
                    "trusted_module": True,
                    "batch": [[[0.01, 0.02, -0.01, 0.5], [0.01, 0.02, -0.01, 0.6]]],
                },
            )
        )
        assert result["result"]["count"] == 1
        prediction = read_json(Path(result["result_file"]))["prediction"]
        assert len(prediction["values"]) == 1
        assert prediction["state"] is not None
    finally:
        service.close()
