import json
import time
from pathlib import Path

import pytest

from omega.research import demo_instance
from omega.research.neural import train
from omega.research.prediction import PredictionSession, predict_run
from omega.research.service import ResearchService


def completed(service, job):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        result = service.status(job["id"])
        if result["state"] in {"complete", "failed", "cancelled", "interrupted"}:
            return result
        time.sleep(0.02)
    raise AssertionError(f"prediction worker did not finish: {result}")


def test_verified_offline_prediction_is_explicit_and_preserves_training_run(dataset, tmp_path):
    source = tmp_path / "training"
    train(dataset, Path(demo_instance.__file__), source, {"config": {"epochs": 2}})
    before = {
        path.relative_to(source): path.read_bytes() for path in source.rglob("*") if path.is_file()
    }
    with pytest.raises(ValueError, match="trusted_module"):
        PredictionSession(source, tmp_path / "untrusted")
    assert not (tmp_path / "untrusted").exists()
    session = PredictionSession(source, tmp_path / "prediction", trusted_module=True)
    try:
        result = session.predict([0, 0.5, 1.0])
        model = json.loads((source / "model.json").read_text())
        assert result["prediction"]["values"] == [
            model["weight"] * value + model["bias"] for value in [0, 0.5, 1.0]
        ]
        with pytest.raises(ValueError, match="recurrent state"):
            session.predict([0], {"hidden": [0]})
        with pytest.raises(ValueError):
            session.predict([float("nan")])
    finally:
        session.close()
    session.close()
    with pytest.raises(RuntimeError, match="closed"):
        session.predict([0])
    assert all((source / name).read_bytes() == value for name, value in before.items())


def test_corrupt_prediction_checkpoint_rejected_before_plugin_execution(dataset, tmp_path):
    source = tmp_path / "training"
    train(dataset, Path(demo_instance.__file__), source, {"config": {"epochs": 1}})
    (source / "model.json").write_text("{}")
    with pytest.raises(ValueError, match="checkpoint hash"):
        PredictionSession(source, tmp_path / "prediction", trusted_module=True)
    assert not (tmp_path / "prediction").exists()


def test_prediction_is_actual_background_job_and_untrusted_request_isolated(dataset, tmp_path):
    source = tmp_path / "training"
    train(dataset, Path(demo_instance.__file__), source, {"config": {"epochs": 1}})
    service = ResearchService(tmp_path / "service")
    parameters = {"training_run": str(source), "batch": [0.1, 0.2]}
    try:
        denied = completed(service, service.submit("nn_predict", parameters))
        assert denied["state"] == "failed"
        assert "trusted_module" in denied["error"]
        result = completed(
            service, service.submit("nn_predict", parameters | {"trusted_module": True})
        )
        assert result["state"] == "complete", result
        assert result["result"]["count"] == 2
        assert Path(result["result_file"]).is_file()
        assert json.loads(Path(result["result_file"]).read_text())["scope"].startswith(
            "offline_research"
        )
    finally:
        service.close()


def test_builtin_torch_inspection_needs_no_framework_in_scheduler(tmp_path):
    service = ResearchService(tmp_path / "service")
    try:
        inspected = completed(service, service.submit("nn_inspect", {"builtin": "pytorch_return"}))
        assert inspected["state"] == "complete", inspected
        assert inspected["result"]["framework"] == "pytorch"
    finally:
        service.close()


def test_predict_result_file_only_published_after_cleanup(dataset, tmp_path):
    source = tmp_path / "training"
    run = train(dataset, Path(demo_instance.__file__), source, {"config": {"epochs": 1}})
    # A valid but failing frozen trusted module must propagate cleanup failure.
    module = source / "instance_module.py"
    text = module.read_text().replace(
        "    def close(self, context):\n        pass",
        "    def close(self, context):\n        raise RuntimeError('close failed')",
    )
    module.write_text(text)
    from omega.research.artifacts import content_hash, write_json

    run["module_source_sha256"] = content_hash(module)
    write_json(source / "run.json", run)
    destination = tmp_path / "prediction"
    with pytest.raises(RuntimeError, match="close failed"):
        predict_run(source, destination, {"batch": [0], "trusted_module": True})
    assert not (destination / "prediction.json").exists()
