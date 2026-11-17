"""Explicit offline prediction from a verified run; no live trading dependency.

Opening a session executes the frozen trusted Python instance. Reading a training
report with read_training_run() remains data-only and never executes that code.
Callers own state streams and deadlines; this is not a realtime execution guard.
"""

# Imports
import json
import time
from pathlib import Path
from typing import Any

from omega.research.artifacts import (
    bounded_artifact_hash,
    child_path,
    copy_bounded_artifact,
    read_status_json,
    validate_artifact_path,
    write_json,
)
from omega.research.neural import (
    MAX_RECORD_BYTES,
    TrainingContext,
    inspect_module,
    load_module,
    release_module,
)

# Constants
MAX_MODULE_BYTES = 1024 * 1024
MAX_CHECKPOINT_BYTES = 512 * 1024 * 1024
MAX_RUN_BYTES = 2 * 1024 * 1024


class PredictionSession:
    def __init__(
        self,
        source: Path,
        destination: Path,
        *,
        checkpoint: str | None = None,
        device: str = "cpu",
        cancel: Any = None,
        trusted_module: bool = False,
    ):
        if trusted_module is not True:
            raise ValueError("prediction executes frozen Python; trusted_module=true is required")
        if device not in {"auto", "cpu", "gpu"}:
            raise ValueError("prediction device must be auto, cpu or gpu")
        self.instance = None
        self.module = None
        self.closed = False
        source = validate_artifact_path(source)
        run_path = validate_artifact_path(source / "run.json")
        run_hash = bounded_artifact_hash(run_path, max_bytes=MAX_RUN_BYTES)
        run = read_status_json(run_path)
        if bounded_artifact_hash(run_path, max_bytes=MAX_RUN_BYTES) != run_hash:
            raise ValueError("prediction run changed while reading")
        if run.get("state") != "complete":
            raise ValueError("prediction requires a completed training run")
        model = run.get("model")
        selected = checkpoint or (model.get("file") if isinstance(model, dict) else None)
        candidates = [
            item
            for item in run.get("artifacts", [])
            if item.get("file") == selected and item.get("kind") == "checkpoint"
        ]
        if len(candidates) != 1:
            raise ValueError("prediction requires one registered checkpoint")
        self.artifact = candidates[0]
        self.module_source_sha256 = run["module_source_sha256"]
        module = source / "instance_module.py"
        if bounded_artifact_hash(module, max_bytes=MAX_MODULE_BYTES) != run["module_source_sha256"]:
            raise ValueError("prediction module source hash mismatch")
        validate_artifact_path(source / selected)
        original = child_path(source, selected)
        if (
            bounded_artifact_hash(original, max_bytes=MAX_CHECKPOINT_BYTES)
            != self.artifact["sha256"]
        ):
            raise ValueError("prediction checkpoint hash mismatch")
        destination = validate_artifact_path(destination, allow_missing=True)
        destination.mkdir(parents=True, exist_ok=False)
        validate_artifact_path(destination)
        frozen_module = destination / "instance_module.py"
        frozen_checkpoint = destination / "checkpoint.snapshot"
        copied_module = copy_bounded_artifact(module, frozen_module, max_bytes=MAX_MODULE_BYTES)
        copied_checkpoint = copy_bounded_artifact(
            original, frozen_checkpoint, max_bytes=MAX_CHECKPOINT_BYTES
        )
        if (
            copied_module != run["module_source_sha256"]
            or copied_checkpoint != self.artifact["sha256"]
            or bounded_artifact_hash(frozen_module, max_bytes=MAX_MODULE_BYTES)
            != run["module_source_sha256"]
            or bounded_artifact_hash(module, max_bytes=MAX_MODULE_BYTES)
            != run["module_source_sha256"]
            or bounded_artifact_hash(frozen_checkpoint, max_bytes=MAX_CHECKPOINT_BYTES)
            != self.artifact["sha256"]
            or bounded_artifact_hash(original, max_bytes=MAX_CHECKPOINT_BYTES)
            != self.artifact["sha256"]
            or bounded_artifact_hash(run_path, max_bytes=MAX_RUN_BYTES) != run_hash
        ):
            raise ValueError("prediction source changed while snapshotting")
        try:
            self.module = load_module(frozen_module, scope_owned=False)
            self.descriptor = inspect_module(self.module)
            if self.descriptor != run["module"] or not self.descriptor["capabilities"].get(
                "predict", False
            ):
                raise ValueError("prediction module descriptor/capability mismatch")
            self.context = TrainingContext(destination, cancel, {"profile": "lightweight"})
            self.context.seed = run["seed"]
            self.context.device_requested = device
            self.context.dataset_identity = run["dataset"]
            self.context.check_cancelled()
            self.instance = self.module.create(run["config"], self.context)
            for name in ("restore", "predict", "close"):
                if not callable(getattr(self.instance, name, None)):
                    raise ValueError(f"prediction instance missing {name}()")
            self.instance.restore(frozen_checkpoint, self.context)
        except BaseException:
            self.close()
            raise

    def predict(self, batch: Any, state: Any = None) -> dict:
        if self.closed:
            raise RuntimeError("prediction session is closed")
        self.context.check_cancelled()
        for value in (batch, state):
            encoded = json.dumps(value, allow_nan=False)
            if len(encoded.encode("utf-8")) > MAX_RECORD_BYTES:
                raise ValueError("prediction input/state exceeds record budget")
        if state is not None and not self.descriptor["capabilities"].get(
            "stateful_inference", False
        ):
            raise ValueError("selected model does not support recurrent state")
        result = self.instance.predict(batch, state)
        if not isinstance(result, dict) or not isinstance(result.get("values"), list):
            raise ValueError("prediction must return a JSON object with values list")
        encoded = json.dumps(result, allow_nan=False)
        if len(encoded.encode("utf-8")) > MAX_RECORD_BYTES:
            raise ValueError("prediction output exceeds record budget")
        self.context.check_cancelled()
        return {
            "schema_version": 1,
            "module": self.descriptor,
            "checkpoint_sha256": self.artifact["sha256"],
            "prediction": result,
            "scope": "offline_research; no account/trading integration",
        }

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            if self.instance is not None and callable(getattr(self.instance, "close", None)):
                self.instance.close(self.context)
        finally:
            release_module(self.module)
            self.module = None


def predict_run(source: Path, destination: Path, parameters: dict, cancel: Any = None) -> dict:
    session = PredictionSession(
        source,
        destination,
        checkpoint=parameters.get("checkpoint"),
        device=parameters.get("device", "cpu"),
        cancel=cancel,
        trusted_module=parameters.get("trusted_module", False),
    )
    try:
        result = session.predict(parameters["batch"], parameters.get("state"))
    finally:
        session.close()
    write_json(destination / "prediction.json", result)
    return result


def prepare_run(
    source: Path, destination: Path, parameters: dict, cancel: Any = None, *, clock_ms=None
) -> dict:
    """Restore preprocessing in the existing research child; no training/refit."""
    from omega.research.inference_input import (
        MAX_BYTES,
        _decode,
        _read_bytes,
        build_prepared,
        encoded,
        load_window,
        parse_parameters,
        raw_window_size,
    )

    parameters = parse_parameters(parameters)
    request = parameters["request"]
    now = clock_ms or (lambda: int(time.time() * 1000))
    previous_clock = None

    def check_deadline():
        nonlocal previous_clock
        value = now()
        if (
            type(value) is not int
            or not 0 <= value < request["deadline_ms"]
            or previous_clock is not None
            and value < previous_clock
        ):
            raise ValueError("raw preparation deadline exceeded or clock invalid")
        previous_clock = value
        return value

    check_deadline()
    rows, rows_sha256, lineage = load_window(parameters["dataset"], request, cancel)
    snapshot_ms = check_deadline()
    if rows[-1].open_time_ms + 60000 > snapshot_ms:
        raise ValueError("raw preparation cannot receive future/unclosed input bars")
    # Reject source/capability identity before executing the frozen trusted module.
    run = _decode(_read_bytes(Path(source) / "run.json", MAX_BYTES))
    selected = parameters.get("checkpoint") or (run.get("model") or {}).get("file")
    artifacts = [
        item
        for item in run.get("artifacts", [])
        if item.get("file") == selected and item.get("kind") == "checkpoint"
    ]
    descriptor = run.get("module", {})
    raw_window_size(request, descriptor)
    if (
        run.get("module_source_sha256") != request["module_sha256"]
        or len(artifacts) != 1
        or artifacts[0].get("sha256") != request["checkpoint_sha256"]
        or descriptor.get("capabilities", {}).get("prepare_inference") is not True
        or descriptor.get("input", {}).get("schema_id") != request["contract"]["schema_id"]
        or descriptor.get("input", {}).get("features", request["contract"]["features"])
        != request["contract"]["features"]
        or "lookback" in run.get("config", {})
        and run["config"]["lookback"] != request["contract"]["lookback"]
    ):
        raise ValueError("raw preparation frozen source/capability/contract mismatch")
    session = PredictionSession(
        source, destination, checkpoint=selected, device="cpu", cancel=cancel, trusted_module=True
    )
    try:
        if (
            session.module_source_sha256 != request["module_sha256"]
            or session.artifact["sha256"] != request["checkpoint_sha256"]
            or session.descriptor != descriptor
        ):
            raise ValueError("raw preparation source changed during restore")
        hook = getattr(session.instance, "prepare_inference", None)
        if not callable(hook):
            raise ValueError("selected instance has no prepare_inference hook")
        check_deadline()
        session.context.check_cancelled()
        result = hook(rows, _decode(encoded(request)), session.context)
        session.context.check_cancelled()
        document = build_prepared(
            request,
            session.descriptor,
            rows,
            rows_sha256,
            lineage,
            result,
            snapshot_available_at_ms=snapshot_ms,
            prepared_at_ms=check_deadline(),
        )
    finally:
        session.close()
    # Cleanup can take time or fail; publish no result until both have completed.
    check_deadline()
    if (
        len(
            (json.dumps(document, allow_nan=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
        )
        > MAX_BYTES
    ):
        raise ValueError("prepared artifact exceeds its 256 KiB file capacity")
    write_json(destination / "prepared.json", document)
    return document
