"""External Python NN instance contract and portable training records.

Plugins are trusted local code executed in a worker process, not a security
sandbox. They own preprocessing, framework, model, optimizer and evaluation.
"""

# Imports
import csv
import hashlib
import importlib.util
import json
import platform
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import ModuleType
from typing import Any
from uuid import uuid4

from omega import __version__
from omega.research.artifacts import (
    Cancelled,
    bounded_artifact_hash,
    check_cancel,
    child_path,
    content_hash,
    copy_bounded_artifact,
    read_json,
    read_status_json,
    validate_artifact_path,
    write_json,
)
from omega.research.datasets import SPLITS, select_split, write_rows
from omega.research.training_inputs import load_training_inputs, snapshot_training_inputs

# Constants
NN_API_VERSION = 1
MAX_RECORD_BYTES = 256 * 1024
MAX_MODULE_BYTES = 1024 * 1024
MAX_RUN_BYTES = 2 * 1024 * 1024
MAX_CHECKPOINT_BYTES = 512 * 1024 * 1024
_external_modules: ContextVar[tuple[ModuleType, ...] | None] = ContextVar(
    "omega_external_modules", default=None
)


def _json_identity(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, allow_nan=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def release_module(module: ModuleType | None) -> bool:
    """Release this exact registration only; never sweep another owner's modules."""
    if not isinstance(module, ModuleType):
        return False
    name = module.__dict__.get("_omega_registration_name")
    if type(name) is str and name.startswith("omega_external_") and sys.modules.get(name) is module:
        del sys.modules[name]
        return True
    return False


@contextmanager
def external_module_scope():
    """Own this synchronous call's imports through its final plugin cleanup.

    Context-local immutable registration lists keep nested/concurrent calls
    separate. Long-lived PredictionSession instances use explicit ownership.
    """
    token = _external_modules.set(())
    try:
        yield
    finally:
        try:
            for module in reversed(_external_modules.get()):
                release_module(module)
        finally:
            _external_modules.reset(token)


def load_module(path: Path, *, scope_owned: bool = True) -> ModuleType:
    """Load trusted Python, retaining registration for dataclasses/typing.

    Bare callers outside external_module_scope must call release_module after
    their final use. scope_owned=False is reserved for explicit session owners.
    """
    if type(scope_owned) is not bool:
        raise ValueError("scope_owned must be boolean")
    if path.suffix.lower() != ".py" or not path.is_file():
        raise ValueError("NN/strategy module must be an existing .py file")
    spec = importlib.util.spec_from_file_location(f"omega_external_{uuid4().hex}", path)
    if spec is None or spec.loader is None:
        raise ValueError("cannot load external Python module")
    module = importlib.util.module_from_spec(spec)
    module._omega_registration_name = spec.name
    # Dataclass/type introspection needs the module registered during execution.
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        release_module(module)
        raise
    if scope_owned and _external_modules.get() is not None:
        _external_modules.set((*_external_modules.get(), module))
    return module


def snapshot_module(source: Path, destination: Path, *, expected_hash: str | None = None) -> str:
    """Freeze one bounded external Python module before inspection/execution."""
    source = validate_artifact_path(source)
    if source.suffix.lower() != ".py":
        raise ValueError("NN instance module must be a .py file")
    digest = bounded_artifact_hash(source, max_bytes=MAX_MODULE_BYTES)
    if expected_hash is not None and digest != expected_hash:
        raise ValueError("NN module source changed before snapshotting")
    copied = copy_bounded_artifact(source, destination, max_bytes=MAX_MODULE_BYTES)
    if (
        copied != digest
        or bounded_artifact_hash(destination, max_bytes=MAX_MODULE_BYTES) != digest
        or bounded_artifact_hash(source, max_bytes=MAX_MODULE_BYTES) != digest
    ):
        raise ValueError("NN module source changed while snapshotting")
    return digest


def inspect_module(module: ModuleType) -> dict:
    descriptor = module.describe()
    if not isinstance(descriptor, dict):
        raise ValueError("describe() must return a JSON object")
    for name in ("module_id", "module_version", "api_version", "core_version", "capabilities"):
        if name not in descriptor:
            raise ValueError(f"NN descriptor missing {name}")
    if (
        type(descriptor["api_version"]) is not int
        or descriptor["api_version"] != NN_API_VERSION
        or descriptor["core_version"] != __version__
    ):
        raise ValueError("NN module API/core version is incompatible")
    if (
        not isinstance(descriptor["capabilities"], dict)
        or any(type(value) is not bool for value in descriptor["capabilities"].values())
        or not isinstance(descriptor["module_id"], str)
        or not descriptor["module_id"]
        or not isinstance(descriptor["module_version"], str)
        or not descriptor["module_version"]
    ):
        raise ValueError("invalid NN capability/identity descriptor")
    if not callable(getattr(module, "create", None)):
        raise ValueError("NN module must export create(config, context)")
    inputs = descriptor.get("input")
    if (
        isinstance(inputs, dict)
        and "raw_lookback" in inputs
        and (type(inputs["raw_lookback"]) is not int or not 1 <= inputs["raw_lookback"] <= 4096)
    ):
        raise ValueError("module input.raw_lookback must be an integer within [1,4096]")
    json.dumps(descriptor, allow_nan=False)
    return descriptor


class TrainingContext:
    """Host-managed records and artifact paths; data manipulation remains module-owned."""

    def __init__(self, root: Path, cancel: Any = None, recording: dict | None = None):
        self.root = root.resolve()
        self.cancel = cancel
        self.recording = {
            "profile": "standard",
            "optional_detail_failure": "warn_continue",
            "max_records": 100_000,
        } | (recording or {})
        if self.recording["profile"] not in {"lightweight", "standard", "detailed"}:
            raise ValueError("recording profile must be lightweight, standard or detailed")
        if self.recording["optional_detail_failure"] not in {"warn_continue", "stop"}:
            raise ValueError("invalid optional detail failure policy")
        maximum = self.recording["max_records"]
        if type(maximum) is not int or not 1 <= maximum <= 10_000_000:
            raise ValueError("max_records must be a positive bounded integer")
        self.seed = 0
        self.device_requested = "auto"
        self.warnings: list[str] = []
        self.artifacts: list[dict] = []
        self.started = time.monotonic()
        self.count = 0

    def check_cancelled(self) -> None:
        check_cancel(self.cancel)

    def artifact_path(self, name: str) -> Path:
        self.check_cancelled()
        path = child_path(self.root, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _emit(self, stream: str, payload: dict, *, optional: bool = False) -> None:
        self.check_cancelled()
        if optional and self.recording["profile"] == "lightweight":
            return
        record = {
            "schema_version": 1,
            "elapsed_ms": int((time.monotonic() - self.started) * 1000),
        } | payload
        try:
            if self.count >= self.recording["max_records"]:
                raise ValueError("training record budget exhausted")
            encoded = json.dumps(record, ensure_ascii=False, allow_nan=False)
            if len(encoded.encode("utf-8")) > MAX_RECORD_BYTES:
                raise ValueError("training record too large; store tensor as a separate artifact")
            with self.artifact_path(f"{stream}.jsonl").open(
                "a", encoding="utf-8", newline="\n"
            ) as output:
                output.write(encoded + "\n")
                output.flush()
            self.count += 1
        except (OSError, ValueError) as exc:
            if not optional or self.recording["optional_detail_failure"] == "stop":
                raise
            self.warnings.append(f"optional {stream} omitted: {exc}")

    def emit_metric(
        self, name: str, value: float, *, step: int, split: str = "train", **metadata: Any
    ) -> None:
        if split not in SPLITS or type(step) is not int or step < 0:
            raise ValueError("invalid metric step/split")
        self._emit(
            "metrics",
            {"metric": name, "value": value, "step": step, "split": split, "metadata": metadata},
        )

    def emit_structure(self, structure: dict, *, step: int = 0) -> None:
        ids = [node["id"] for node in structure.get("nodes", [])]
        if len(ids) != len(set(ids)) or not ids:
            raise ValueError("structure requires unique stable node IDs")
        for edge in structure.get("edges", []):
            if edge["source"] not in ids or edge["target"] not in ids:
                raise ValueError("structure edge references an unknown node")
        self._emit("structures", {"step": step, "structure": structure})

    def emit_tensor_summary(self, layer_id: str, summary: dict, *, step: int = 0) -> None:
        self._emit(
            "tensor_summaries",
            {"layer_id": layer_id, "step": step, "summary": summary},
            optional=True,
        )

    def emit_evaluation(self, row: dict, *, split: str = "test") -> None:
        if split not in SPLITS:
            raise ValueError("invalid evaluation split")
        self._emit("evaluations", {"split": split, "sample": row})

    def emit_event(self, kind: str, **data: Any) -> None:
        self._emit("events", {"kind": kind, "data": data})

    def register_artifact(self, name: str, *, kind: str, **metadata: Any) -> dict:
        path = child_path(self.root, name)
        if not path.is_file():
            raise ValueError("artifact must exist before registration")
        item = {"file": name, "kind": kind, "sha256": content_hash(path), "metadata": metadata}
        json.dumps(item, allow_nan=False)
        self.artifacts.append(item)
        self.emit_event("artifact_registered", artifact=item)
        return item

    def checkpoint_json(self, name: str, data: dict, **metadata: Any) -> dict:
        write_json(self.artifact_path(name), data)
        return self.register_artifact(name, kind="checkpoint", **metadata)


def _resume_snapshot(
    options: Any, destination: Path, descriptor: dict, module_hash: str, run: dict
) -> tuple[Path, dict] | None:
    """Copy and verify the chosen immutable checkpoint before plugin deserialization.

    A still-running/unknown source is deliberately excluded: its writer must be
    reconciled first. A failed/cancelled run can expose its last completed epoch.
    """
    if options is None:
        return None
    if not isinstance(options, dict) or set(options) != {"run", "checkpoint"}:
        raise ValueError("resume requires exactly run directory and checkpoint file")
    if not descriptor["capabilities"].get("resume_training", False):
        raise ValueError("selected NN module does not support training resume")
    previous_root = validate_artifact_path(Path(options["run"]))
    previous_path = validate_artifact_path(previous_root / "run.json")
    previous_hash = bounded_artifact_hash(previous_path, max_bytes=MAX_RUN_BYTES)
    previous = read_status_json(previous_path)
    if bounded_artifact_hash(previous_path, max_bytes=MAX_RUN_BYTES) != previous_hash:
        raise ValueError("resume source run changed while reading")
    if previous.get("state") not in {"complete", "failed", "cancelled"}:
        raise ValueError("resume source must be complete, failed or cancelled after writer cleanup")
    if previous.get("cleanup_completed") is not True:
        raise ValueError("resume source writer cleanup is not confirmed")
    if previous.get("module") != descriptor or previous.get("module_source_sha256") != module_hash:
        raise ValueError("resume module identity/source mismatch")
    if previous.get("dataset") != run["dataset"]:
        raise ValueError("resume dataset/split/provenance mismatch")
    for name in ("seed", "online_learning"):
        if previous.get(name) != run[name]:
            raise ValueError(f"resume {name} mismatch")
    overrides = descriptor.get("resume_config_overrides", [])
    if not isinstance(overrides, list) or any(not isinstance(name, str) for name in overrides):
        raise ValueError("invalid resume config override declaration")
    old_config = {
        name: value for name, value in previous.get("config", {}).items() if name not in overrides
    }
    new_config = {name: value for name, value in run["config"].items() if name not in overrides}
    if old_config != new_config:
        raise ValueError("resume config mismatch outside declared overrides")
    name = options["checkpoint"]
    matches = [
        item
        for item in previous.get("artifacts", [])
        if item.get("file") == name and item.get("kind") == "checkpoint"
    ]
    if len(matches) != 1:
        raise ValueError("resume checkpoint must be one registered checkpoint artifact")
    item = matches[0]
    validate_artifact_path(previous_root / name)
    original = child_path(previous_root, name)
    if bounded_artifact_hash(original, max_bytes=MAX_CHECKPOINT_BYTES) != item["sha256"]:
        raise ValueError("resume checkpoint hash mismatch")
    snapshot_name = f"resume/{Path(name).name}"
    validate_artifact_path(destination / snapshot_name, allow_missing=True)
    frozen = child_path(destination, snapshot_name)
    frozen.parent.mkdir(parents=True, exist_ok=True)
    copied = copy_bounded_artifact(original, frozen, max_bytes=MAX_CHECKPOINT_BYTES)
    if (
        copied != item["sha256"]
        or bounded_artifact_hash(frozen, max_bytes=MAX_CHECKPOINT_BYTES) != item["sha256"]
        or bounded_artifact_hash(original, max_bytes=MAX_CHECKPOINT_BYTES) != item["sha256"]
        or bounded_artifact_hash(previous_path, max_bytes=MAX_RUN_BYTES) != previous_hash
    ):
        raise ValueError("resume checkpoint changed while snapshotting")
    provenance = {
        "source_run_sha256": _json_identity(previous),
        "checkpoint": item,
        "snapshot_file": frozen.relative_to(destination).as_posix(),
        "scope": "same_dataset_and_module_epoch_boundary",
    }
    return frozen, provenance


@external_module_scope()
def train(
    source: Path, module_path: Path, destination: Path, parameters: dict, cancel: Any = None
) -> dict:
    module_path = validate_artifact_path(module_path)
    module_hash = bounded_artifact_hash(module_path, max_bytes=MAX_MODULE_BYTES)
    manifest, rows, auxiliary_identity = load_training_inputs(source, cancel)
    destination = validate_artifact_path(destination, allow_missing=True)
    destination.mkdir(parents=True, exist_ok=False)
    validate_artifact_path(destination)
    if auxiliary_identity is not None:
        frozen_inputs = snapshot_training_inputs(source, destination, cancel)
        if frozen_inputs != (manifest, rows, auxiliary_identity):
            raise ValueError("training observation dataset changed while snapshotting")
        manifest, rows, auxiliary_identity = frozen_inputs
    write_json(destination / "input.dataset.json", manifest)
    write_rows(destination / "input.rows.csv", rows, cancel)
    # Snapshot the selected module before loading; record dependencies/environment separately.
    frozen = destination / "instance_module.py"
    snapshot_module(module_path, frozen, expected_hash=module_hash)
    module = load_module(frozen)
    descriptor = inspect_module(module)
    capabilities = descriptor["capabilities"]
    if not capabilities.get("train", False):
        raise ValueError("selected NN module does not support training")
    online_learning = parameters.get("online_learning", False)
    if type(online_learning) is not bool:
        raise ValueError("online_learning must be boolean")
    if online_learning and not capabilities.get("online_learning", False):
        raise ValueError("selected NN module does not support online learning")
    context = TrainingContext(destination, cancel, parameters.get("recording"))
    context.seed = parameters.get("seed", 0)
    if type(context.seed) is not int:
        raise ValueError("training seed must be an integer")
    context.device_requested = parameters.get("device", "auto")
    if context.device_requested not in {"auto", "cpu", "gpu"}:
        raise ValueError("NN device must be auto, cpu or gpu")
    config = parameters.get("config", {})
    if not isinstance(config, dict):
        raise ValueError("NN config must be a JSON object")
    json.dumps(config, allow_nan=False)
    run = {
        "schema_version": 1,
        "state": "running",
        "cleanup_completed": False,
        "module": descriptor,
        "module_source_sha256": module_hash,
        "config": config,
        "seed": context.seed,
        "dataset": {
            "rows_sha256": manifest["rows"]["sha256"],
            "splits": manifest["splits"],
            "transform": manifest.get("transform"),
            "repair": manifest.get("repair"),
            "quality": manifest["quality"],
        },
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "core_version": __version__,
            "interpreter": sys.executable,
        },
        "recording": context.recording,
        "online_learning": online_learning,
        "fit_scope": "train_only",
        "device": {"requested": context.device_requested, "used": "not_reported_by_module"},
        "test_available_after_training": True,
        "plugin_trust": "local trusted Python; process isolation is not a security sandbox",
    }
    packages = {}
    for name in ("torch", "triton", "numpy"):
        try:
            packages[name] = version(name)
        except PackageNotFoundError:
            packages[name] = None
    run["environment"]["packages"] = packages
    run["environment_sha256"] = hashlib.sha256(
        json.dumps(run["environment"], sort_keys=True).encode("utf-8")
    ).hexdigest()
    if auxiliary_identity is not None:
        run["dataset"]["auxiliary"] = auxiliary_identity
        context.warnings.append(
            "Auxiliary inputs are a retrospective training view with actual availability metadata; "
            "this is not a causal trading receipt."
        )
    resume = _resume_snapshot(
        parameters.get("resume"), destination, descriptor, run["module_source_sha256"], run
    )
    run["resume"] = resume[1] if resume else None
    context.dataset_identity = run["dataset"]
    write_json(destination / "run.json", run)
    operation_error = None
    instance = None
    try:
        context.emit_event("training_started", resumed=resume is not None)
        instance = module.create(config, context)
        for name in ("prepare", "transform", "train", "evaluate", "save", "close"):
            if not callable(getattr(instance, name, None)):
                raise ValueError(f"NN instance missing {name}()")
        # [1]: Fit preparation can see train only. Test is not handed to train/validation.
        train_rows = tuple(select_split(rows, manifest["splits"], "train"))
        if resume:
            restore = getattr(instance, "restore", None)
            if not callable(restore):
                raise ValueError("resumable NN instance missing restore(checkpoint, context)")
            restore(resume[0], context)
            context.emit_event("checkpoint_restored", **resume[1])
            prepared_train = instance.transform(train_rows, "train", context)
        else:
            prepared_train = instance.prepare(train_rows, context)
        validation = instance.transform(
            tuple(select_split(rows, manifest["splits"], "validation")), "validation", context
        )
        context.check_cancelled()
        outcome = instance.train(
            prepared_train, validation, config | {"online_learning": online_learning}, context
        )
        if not isinstance(outcome, dict):
            raise ValueError("NN train() must return a JSON object")
        run["device"]["used"] = outcome.get("device", "not_reported_by_module")
        run["device"]["reason"] = outcome.get("device_reason", "module_reported_without_reason")
        context.check_cancelled()
        test = instance.transform(
            tuple(select_split(rows, manifest["splits"], "test")), "test", context
        )
        evaluation = instance.evaluate(test, context)
        model = instance.save(context)
    except BaseException as exc:
        operation_error = exc
        run.update(
            state="cancelled" if isinstance(exc, Cancelled) else "failed",
            error=f"{type(exc).__name__}: {exc}",
            artifacts=context.artifacts,
            warnings=context.warnings,
        )
        try:
            write_json(destination / "run.json", run)
        except OSError:
            pass  # Preserve the original failure when even failure metadata cannot be saved.
        raise
    finally:
        try:
            if instance is not None and callable(getattr(instance, "close", None)):
                instance.close(context)
        except BaseException as exc:
            context.warnings.append(f"instance close failed: {type(exc).__name__}: {exc}")
            if operation_error is None:
                run.update(
                    state="failed",
                    error=f"instance close failed: {type(exc).__name__}: {exc}",
                    artifacts=context.artifacts,
                )
            run["warnings"] = context.warnings
            try:
                write_json(destination / "run.json", run)
            except OSError:
                pass
            if operation_error is None:
                raise
        else:
            run["cleanup_completed"] = True
            if operation_error is not None:
                try:
                    write_json(destination / "run.json", run)
                except OSError:
                    pass
    # Success is published only after framework/plugin cleanup and final hashing.
    try:
        context.check_cancelled()
        context.emit_event("training_completed")
        _metrics_csv(destination)
        for artifact in context.artifacts:
            if content_hash(child_path(destination, artifact["file"])) != artifact["sha256"]:
                raise ValueError("registered training artifact changed before finalization")
        run.update(
            state="complete",
            outcome=outcome,
            evaluation=evaluation,
            model=model,
            artifacts=context.artifacts,
            warnings=context.warnings,
        )
        json.dumps(run, allow_nan=False)
        write_json(destination / "run.json", run)
    except BaseException as exc:
        run.update(
            state="cancelled" if isinstance(exc, Cancelled) else "failed",
            error=f"{type(exc).__name__}: {exc}",
            artifacts=context.artifacts,
            warnings=context.warnings,
        )
        try:
            # Invalid plugin results must not prevent recording host failure state.
            run.pop("outcome", None)
            run.pop("evaluation", None)
            run.pop("model", None)
            write_json(destination / "run.json", run)
        except OSError:
            pass
        raise
    return run


def _metrics_csv(root: Path) -> None:
    source = root / "metrics.jsonl"
    if not source.exists():
        return
    with (root / "metrics.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=("metric", "split", "step", "value", "elapsed_ms"),
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        for line in source.read_text(encoding="utf-8").splitlines():
            writer.writerow(json.loads(line))


def read_training_run(root: Path) -> dict:
    """Read-only viewer data source; never imports an external instance module."""
    run = read_json(root / "run.json")
    result = {"run": run}
    for name in ("metrics", "structures", "tensor_summaries", "evaluations", "events"):
        path = root / f"{name}.jsonl"
        result[name] = (
            [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            if path.exists()
            else []
        )
    for artifact in run.get("artifacts", []):
        if content_hash(child_path(root, artifact["file"])) != artifact["sha256"]:
            raise ValueError("training artifact hash mismatch")
    return result
