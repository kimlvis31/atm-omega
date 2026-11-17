"""Bounded offline job scheduler; each heavy job has an independent process."""

# Imports
import json
import multiprocessing
import os
import re
import threading
import time
import traceback
from copy import deepcopy
from hashlib import sha256
from pathlib import Path, PurePosixPath, PureWindowsPath
from uuid import uuid4

from omega.ports.research import ResearchJobHandler
from omega.research.artifacts import (
    Cancelled,
    check_cancel,
    child_path,
    content_hash,
    read_json,
    read_status_json,
    write_json,
)
from omega.research.datasets import export_dataset, load_dataset, monte_carlo, write_rows
from omega.research.neural import (
    external_module_scope,
    inspect_module,
    load_module,
    snapshot_module,
    train,
)
from omega.research.prediction import predict_run, prepare_run
from omega.research.simulation import OrderRequest, SimulationSpec, simulate

# Constants
JOB_KINDS = {
    "export",
    "monte_carlo",
    "simulate",
    "train",
    "nn_inspect",
    "nn_predict",
    "nn_prepare",
    "inspect_dataset",
}
TERMINAL_STATES = {"complete", "failed", "cancelled", "interrupted"}


class ResearchMetadataUnavailable(RuntimeError):
    """A registered job's actual metadata could not be read; its outcome is unknown."""


def _close_metadata_fingerprint(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(encoded) > 2_097_152:
        raise ResearchMetadataUnavailable("research close metadata exceeds capacity")
    return sha256(encoded).hexdigest()


def _nn_module_path(parameters: dict) -> Path:
    builtin = parameters.get("builtin")
    if builtin is not None:
        if builtin != "pytorch_return":
            raise ValueError("unknown builtin NN instance")
        if parameters.get("demo", False) or parameters.get("module"):
            raise ValueError("select builtin, external module or demo exclusively")
        return Path(__file__).with_name("pytorch_instance.py")
    return (
        Path(__file__).with_name("demo_instance.py")
        if parameters.get("demo", False)
        else Path(parameters["module"])
    )


def _valid_id(value) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(char in "0123456789abcdef" for char in value)
    )


def _recover_final(root: Path, status: dict) -> bool:
    try:
        return _inspect_final(root, status)
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _inspect_final(root: Path, status: dict) -> bool:
    identity = status.get("id")
    if not _valid_id(identity):
        return False
    final = root / "artifacts" / status["id"]
    marker = final / "artifact.json"
    if not marker.is_file():
        return False
    index = read_json(marker)
    if index.get("job_id") != status["id"] or index.get("state") != "complete":
        return False
    for item in index["files"]:
        if content_hash(child_path(final, item["file"])) != item["sha256"]:
            return False
    result = child_path(final, index["result_file"])
    if not result.is_file():
        return False
    status.update(
        state="complete",
        artifact=str(final),
        result_file=str(result),
        result=index["summary"],
        finished_at_ms=index["finished_at_ms"],
    )
    return True


@external_module_scope()
def _run_job(root: str, job_id: str, kind: str, parameters: dict, cancel, handler=None) -> None:
    # Conservative initial CPU budget. A plugin must still declare/manage its own
    # native workers; these defaults are not a hard security/resource sandbox.
    for variable in (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
    ):
        os.environ[variable] = "1"
    root_path = Path(root)
    job_path = root_path / "jobs" / f"{job_id}.json"
    partial = root_path / "artifacts" / f".partial-{job_id}"
    final = root_path / "artifacts" / job_id
    status = read_json(job_path)
    try:
        partial.mkdir(exist_ok=False)
        if handler is not None:
            outcome = handler(partial, final, parameters, cancel)
            if type(outcome) is not dict or set(outcome) != {"result_file", "summary"}:
                raise ValueError("invalid composed research result")
            relative, summary = outcome["result_file"], outcome["summary"]
            if (
                type(relative) is not str
                or not 1 <= len(relative) <= 512
                or PurePosixPath(relative).is_absolute()
                or PureWindowsPath(relative).drive
                or "\\" in relative
                or ".." in PurePosixPath(relative).parts
                or PurePosixPath(relative).as_posix() != relative
                or type(summary) is not dict
                or len(json.dumps(summary, allow_nan=False).encode()) > 65536
                or not child_path(partial, relative).is_file()
            ):
                raise ValueError("invalid composed research artifact")
        elif kind == "inspect_dataset":
            from omega.research.auxiliary_datasets import SCHEMA_ID_V2, load_auxiliary_dataset

            source = Path(parameters["dataset"])
            header_path = source / "dataset.json" if source.is_dir() else source
            if read_status_json(header_path).get("schema_id") == SCHEMA_ID_V2:
                from omega.research.auxiliary_bundle_v2 import inspection_identity

                manifest, rows, _ = load_auxiliary_dataset(source, cancel)
                auxiliary = inspection_identity(manifest)
            else:
                from omega.research.training_inputs import load_training_inputs

                manifest, rows, auxiliary = load_training_inputs(source, cancel)
            instrument_ranges = {}
            for row in rows:
                check_cancel(cancel)
                bounds = instrument_ranges.setdefault(
                    row.instrument, [row.open_time_ms, row.open_time_ms + 60000]
                )
                bounds[0] = min(bounds[0], row.open_time_ms)
                bounds[1] = max(bounds[1], row.open_time_ms + 60000)
            summary = {
                "schema_id": auxiliary["schema_id"] if auxiliary else manifest["schema_id"],
                "rows": len(rows),
                "instruments": sorted({row.instrument for row in rows}),
                "time_range": [
                    min(row.open_time_ms for row in rows),
                    max(row.open_time_ms for row in rows) + 60000,
                ],
                "auxiliary": auxiliary,
                "instrument_ranges": dict(sorted(instrument_ranges.items())),
            }
            relative = "inspection.json"
            write_json(partial / relative, summary)
        elif kind == "export":
            result = export_dataset(
                Path(parameters["source_file"]).resolve(), partial / "dataset", parameters, cancel
            )
            relative = "dataset/dataset.json"
            summary = {"rows": result["rows"]["count"], "instruments": result["instruments"]}
            if "interpolated" in result.get("variants", {}):
                relative = "dataset/interpolated/dataset.json"
                repaired = read_json(partial / relative)
                summary.update(
                    rows=repaired["rows"]["count"],
                    original_dataset_file=str(final / "dataset/dataset.json"),
                    interpolation={
                        "rows": repaired["quality"]["interpolated_rows"],
                        "remaining_gaps": repaired["quality"]["missing_minutes"],
                        "causal": repaired["repair"]["causal"],
                    },
                )
        elif kind == "monte_carlo":
            result = monte_carlo(
                Path(parameters["dataset"]).resolve(), partial / "dataset", parameters, cancel
            )
            relative = "dataset/dataset.json"
            if result.get("schema_id") == "omega.market-observations.1m.v1":
                from omega.research.auxiliary_datasets import load_auxiliary_dataset

                _, generated_rows, _ = load_auxiliary_dataset(partial / "dataset", cancel)
                summary = {
                    "rows": len(generated_rows),
                    "schema_id": result["schema_id"],
                    "transform": result["transform"] | {"provenance": "recorded in manifest"},
                }
            else:
                summary = {
                    "rows": result["rows"]["count"],
                    "transform": result["transform"] | {"provenance": "recorded in manifest"},
                }
        elif kind == "train":
            module_path = _nn_module_path(parameters)
            training_dataset = Path(parameters["dataset"]).resolve()
            mc_options = parameters.get("monte_carlo", {})
            if not isinstance(mc_options, dict):
                raise ValueError("monte_carlo options must be an object")
            if mc_options.get("enabled", False):
                monte_carlo(training_dataset, partial / "generated_dataset", mc_options, cancel)
                training_dataset = partial / "generated_dataset"
            result = train(
                training_dataset,
                module_path,
                partial / "training",
                parameters,
                cancel,
            )
            relative = "training/run.json"
            summary = {
                "module_id": result["module"]["module_id"],
                "evaluation": result["evaluation"],
                "warnings": result["warnings"],
                "checkpoint": (
                    result["model"].get("file") if isinstance(result.get("model"), dict) else None
                ),
                "completed_epochs": result.get("outcome", {}).get("epochs"),
                "training_dataset_file": str(
                    final / training_dataset.relative_to(partial)
                    if training_dataset.is_relative_to(partial)
                    else training_dataset
                ),
            }
        elif kind == "nn_inspect":
            module_path = _nn_module_path(parameters)
            frozen_module = partial / "instance_module.py"
            snapshot_module(module_path, frozen_module)
            result = inspect_module(load_module(frozen_module))
            relative = "module.json"
            write_json(partial / relative, result)
            summary = result
        elif kind == "nn_prepare":
            result = prepare_run(
                Path(parameters["training_run"]),
                partial / "preparation",
                parameters,
                cancel,
            )
            relative = "preparation/prepared.json"
            summary = {
                "scope": result["scope"],
                "prepared_sha256": result["prepared_sha256"],
                "as_of_ms": result["as_of_ms"],
                "count": len(result["batch"]),
            }
        elif kind == "nn_predict":
            result = predict_run(
                Path(parameters["training_run"]),
                partial / "prediction",
                parameters,
                cancel,
            )
            relative = "prediction/prediction.json"
            summary = {
                "module_id": result["module"]["module_id"],
                "count": len(result["prediction"]["values"]),
                "scope": "offline_research",
            }
        else:
            manifest, rows = load_dataset(Path(parameters["dataset"]).resolve(), cancel)
            allow_noncausal = parameters.get("allow_noncausal_research_input", False)
            if type(allow_noncausal) is not bool:
                raise ValueError("allow_noncausal_research_input must be boolean")
            noncausal_input = (
                manifest.get("repair")
                and not manifest["repair"]["causal"]
                and manifest["quality"]["interpolated_rows"] > 0
            )
            if noncausal_input and not allow_noncausal:
                raise ValueError(
                    "noncausal interpolated input requires explicit "
                    "allow_noncausal_research_input for offline stress replay"
                )
            strategy_name = parameters.get("strategy", "buy_and_hold_demo")
            if "ts_module_id" in parameters:
                from omega.research.strategy_binding import TradeStrategyBinding, build_features
                from omega.strategies.registry import create

                strategy_name = parameters["ts_module_id"]
                strategy = TradeStrategyBinding(
                    lambda instrument, config: create(strategy_name, instrument, config),
                    parameters.get("strategy_config", {}),
                    feature_factory=lambda instrument: build_features(
                        instrument, parameters.get("feature_config", {})
                    ),
                )
            elif "strategy_module" in parameters:
                source_module = Path(parameters["strategy_module"]).resolve()
                frozen = partial / "strategy_module.py"
                frozen.write_bytes(source_module.read_bytes())
                module = load_module(frozen)
                config = parameters.get("strategy_config", {})
                if callable(getattr(module, "create_strategy", None)):
                    strategy = module.create_strategy(config)
                else:
                    from omega.research.strategy_binding import TradeStrategyBinding

                    strategy = TradeStrategyBinding(
                        module.create,
                        config,
                        feature_factory=getattr(module, "create_features", None),
                    )
                strategy_name = str(source_module)
            elif strategy_name == "buy_and_hold_demo":
                from decimal import Decimal

                quantity = Decimal(str(parameters.get("quantity", "1")))
                if not quantity.is_finite() or quantity <= 0:
                    raise ValueError("demo quantity must be finite and positive")
                seen = set()

                def strategy(view):
                    orders = []
                    for candle in view.candles:
                        if candle.instrument not in seen:
                            orders.append(
                                OrderRequest(candle.instrument, quantity, tag="buy_and_hold_demo")
                            )
                            seen.add(candle.instrument)
                    return orders
            else:
                raise ValueError("unknown simulation strategy; provide strategy_module")
            result = simulate(
                rows, strategy, SimulationSpec.parse(parameters.get("spec", {})), cancel
            )
            result["input"] = {
                "rows_sha256": manifest["rows"]["sha256"],
                "strategy": strategy_name,
                "strategy_config": parameters.get("strategy_config", {}),
                "feature_config": parameters.get("feature_config", {}),
                "repair": manifest.get("repair"),
            }
            if noncausal_input:
                result["assumptions"].append(
                    "noncausal bidirectional repaired input; not a causal historical backtest"
                )
            write_json(partial / "input.dataset.json", manifest)
            write_rows(partial / "input.rows.csv", rows, cancel)
            relative = "simulation.json"
            write_json(partial / relative, result)
            summary = {
                "fills": len(result["fills"]),
                "wallet": result["wallet"],
                "backend": result["backend"],
            }
        check_cancel(cancel)
        finished_at_ms = int(time.time() * 1000)
        file_index = []
        for path in sorted(partial.rglob("*")):
            check_cancel(cancel)
            if (
                path.is_symlink()
                or path.is_junction()
                or not path.resolve().is_relative_to(partial.resolve())
            ):
                raise ValueError("worker artifacts cannot escape the job directory")
            if path.is_file():
                with path.open("r+b") as stream:
                    os.fsync(stream.fileno())
                file_index.append(
                    {"file": path.relative_to(partial).as_posix(), "sha256": content_hash(path)}
                )
        write_json(
            partial / "artifact.json",
            {
                "schema_version": 1,
                "job_id": job_id,
                "kind": kind,
                "state": "complete",
                "result_file": relative,
                "summary": summary,
                "finished_at_ms": finished_at_ms,
                "files": file_index,
            },
        )
        # Only finalized directories are exposed as successful artifacts.
        partial.rename(final)
        status.update(
            state="complete",
            finished_at_ms=finished_at_ms,
            artifact=str(final),
            result_file=str(final / relative),
            result=summary,
        )
    except Cancelled as exc:
        status.update(state="cancelled", error=str(exc), finished_at_ms=int(time.time() * 1000))
    except BaseException as exc:
        status.update(
            state="failed",
            error=f"{type(exc).__name__}: {exc}",
            finished_at_ms=int(time.time() * 1000),
        )
        if partial.exists():
            try:
                write_json(
                    partial / "failure.json",
                    {"error": status["error"], "traceback": traceback.format_exc()},
                )
            except OSError:
                pass
    write_json(job_path, status)


class ResearchService:
    """One offline worker at a time keeps live/control capacity independent.

    CSV/JSONL inputs use instrument/open_time_ms/open/high/low/close/volume.
    export: source_file, split_ratios?, purge_bars?
    monte_carlo: dataset (directory or dataset.json), seed?, block_bars?, splits?
    train: dataset, module (.py), builtin=pytorch_return or demo=true,
           config?, recording?, seed?, resume?
    simulate: dataset, spec?, strategy_module? (create_strategy), quantity?
    nn_inspect: module (.py) or demo=true
    nn_predict: training_run, batch, checkpoint?, state?, device? (trusted frozen module execution)
    nn_prepare: training_run, dataset, request, trusted_module (retrospective raw snapshot only)
    """

    def __init__(self, root: Path, *, handlers: dict[str, ResearchJobHandler] | None = None):
        if handlers is not None and (
            type(handlers) is not dict
            or len(handlers) > 8
            or any(
                type(key) is not str
                or re.fullmatch(r"[a-z][a-z_]{0,63}", key) is None
                or key in JOB_KINDS
                or not callable(value)
                for key, value in handlers.items()
            )
        ):
            raise ValueError("invalid bounded composed research handlers")
        self._handlers = dict(handlers or {})
        self.root = Path(root).resolve()
        (self.root / "jobs").mkdir(parents=True, exist_ok=True)
        (self.root / "artifacts").mkdir(exist_ok=True)
        for directory in (self.root / "jobs", self.root / "artifacts"):
            if directory.is_symlink() or directory.is_junction():
                raise ValueError("research output directories cannot be symlinks/junctions")
        self._condition = threading.Condition()
        self._pending: list[str] = []
        self._requests: dict[str, tuple[str, dict]] = {}
        self._closed = False
        self._close_lock = threading.RLock()
        self._close_initialized = False
        self._cancel_intents = {}
        self._close_capacity_error = False
        self._degraded: str | None = None
        self._volatile_failures: dict[str, dict] = {}
        self._last_job_id: str | None = None
        self._active: tuple[str, object] | None = None
        self._running_process: tuple[str, object] | None = None
        self._unconfirmed_process = None
        self._quarantine: dict | None = None
        self._quarantine_invalid = False
        self._quarantine_path = self.root / "state" / "research-unconfirmed.json"
        self._context = multiprocessing.get_context("spawn")
        self._load_quarantine()
        for path in (self.root / "jobs").glob("*.json"):
            if self._quarantine or self._quarantine_invalid:
                # A possibly living former worker can still publish here. Never
                # reconcile or rename its files merely because this owner restarted.
                break
            if not _valid_id(path.stem) or path.is_symlink():
                continue
            try:
                old = read_json(path)
                if (
                    not isinstance(old, dict)
                    or old.get("id") != path.stem
                    or not {"kind", "state", "created_at_ms"} <= old.keys()
                    or type(old["created_at_ms"]) is not int
                ):
                    raise ValueError("job metadata identity mismatch")
            except (OSError, ValueError):
                backup = path.with_suffix(f".{uuid4().hex}.corrupt")
                path.rename(backup)
                old = {
                    "schema_version": 1,
                    "id": path.stem,
                    "kind": "unknown",
                    "created_at_ms": int(time.time() * 1000),
                    "state": "failed",
                    "error": f"corrupt job metadata preserved as {backup.name}",
                }
                write_json(path, old)
            if old.get("state") not in TERMINAL_STATES:
                if not _recover_final(self.root, old):
                    old.update(
                        state="interrupted", error="service restarted before job finalization"
                    )
                write_json(path, old)
        self._thread = threading.Thread(
            target=self._loop, name="omega-research-supervisor", daemon=True
        )
        self._thread.start()

    def _load_quarantine(self):
        path = self._quarantine_path
        try:
            if (
                path.parent.is_symlink()
                or path.parent.is_junction()
                or not path.parent.resolve().is_relative_to(self.root)
            ):
                raise ValueError("unsafe quarantine directory")
            try:
                details = path.lstat()
            except FileNotFoundError:
                return
            if path.is_symlink() or path.is_junction() or details.st_size > 4096:
                raise ValueError("unsafe quarantine marker")
            with path.open("rb") as stream:
                raw = stream.read(4097)
            if len(raw) > 4096:
                raise ValueError("quarantine marker exceeds capacity")
            marker = json.loads(raw.decode("utf-8"))
            if (
                not isinstance(marker, dict)
                or set(marker) != {"schema_version", "job_id", "pid", "reason", "recorded_at_ms"}
                or type(marker["schema_version"]) is not int
                or marker["schema_version"] != 1
                or not _valid_id(marker["job_id"])
                or (
                    marker["pid"] is not None
                    and (type(marker["pid"]) is not int or marker["pid"] <= 0)
                )
                or marker["reason"] != "WORKER_EXIT_UNCONFIRMED"
                or type(marker["recorded_at_ms"]) is not int
                or marker["recorded_at_ms"] < 0
            ):
                raise ValueError("invalid quarantine marker")
            self._quarantine = marker
            self._degraded = "worker exit unconfirmed; manual quarantine verification required"
        except (OSError, ValueError, TypeError):
            self._quarantine_invalid = True
            self._degraded = "unreadable research quarantine; manual verification required"

    def _unknown_status(self, job_id):
        # Read only descriptive metadata. Do not expose a late complete/result path
        # or overwrite the worker's potentially still-changing record.
        try:
            old = read_status_json(self._job_path(job_id))
            if not isinstance(old, dict):
                old = {}
        except (OSError, ValueError):
            old = {}
        created = old.get("created_at_ms")
        return {
            "schema_version": 1,
            "id": job_id,
            "kind": old.get("kind") if isinstance(old.get("kind"), str) else "unknown",
            "state": "unknown",
            "created_at_ms": created if type(created) is int else 0,
            "error": "WORKER_EXIT_UNCONFIRMED; manual quarantine verification required",
            "computation_state": "unknown",
            "persistence": "quarantine_overlay",
        }

    def _quarantine_worker(self, job_id, process):
        marker = {
            "schema_version": 1,
            "job_id": job_id,
            "pid": process.pid if process is not None else None,
            "reason": "WORKER_EXIT_UNCONFIRMED",
            "recorded_at_ms": int(time.time() * 1000),
        }
        with self._condition:
            self._unconfirmed_process = process
            self._quarantine = marker
            self._degraded = "worker exit unconfirmed; manual quarantine verification required"
            self._condition.notify_all()
            try:
                path = self._quarantine_path
                if (
                    path.parent.is_symlink()
                    or path.parent.is_junction()
                    or not path.parent.resolve().is_relative_to(self.root)
                    or path.is_symlink()
                    or path.is_junction()
                ):
                    raise ValueError("unsafe quarantine path")
                write_json(path, marker)
            except (OSError, ValueError):
                # Remain blocked in memory. No durable protection can be promised
                # across restart when the filesystem refuses the marker itself.
                self._degraded += "; quarantine persistence unavailable"

    @staticmethod
    def _stop_process(process):
        # A terminate/kill call is a request, never proof that the child exited.
        try:
            process.terminate()
            process.join(2)
        except Exception:
            pass
        if process.is_alive():
            try:
                process.kill()
                process.join(2)
            except Exception:
                pass
        return not process.is_alive()

    def _publish_status(self, job_id, status):
        with self._condition:
            if self._quarantine or self._quarantine_invalid:
                return
            write_json(self._job_path(job_id), status)

    def supports(self, kind: str) -> bool:
        return type(kind) is str and kind in JOB_KINDS | self._handlers.keys()

    def handler_scope(self, kind: str, feature: str) -> str | None:
        """Read explicit installed-handler metadata; never infer an optional feature."""
        if type(kind) is not str or type(feature) is not str:
            return None
        scopes = getattr(self._handlers.get(kind), "omega_feature_scopes", None)
        if type(scopes) is not dict:
            return None
        value = scopes.get(feature)
        return value if type(value) is str and 1 <= len(value) <= 128 else None

    def submit(self, kind: str, parameters: dict) -> dict:
        if kind not in JOB_KINDS | self._handlers.keys() or not isinstance(parameters, dict):
            raise ValueError("unsupported research job kind/parameters")
        # Freeze caller-owned dictionaries; do not let later settings mutate queued jobs.
        encoded = json.dumps(parameters, allow_nan=False)
        if len(encoded.encode("utf-8")) > 1024 * 1024:
            raise ValueError("job parameters exceed one MiB")
        parameters = json.loads(encoded)
        if kind == "nn_prepare":
            from omega.research.inference_input import parse_parameters

            parameters = parse_parameters(parameters)
        timeout = parameters.get("timeout_seconds", 3600)
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < timeout <= 86400
        ):
            raise ValueError("timeout_seconds must be within (0, 86400]")
        with self._condition:
            if self._closed:
                raise RuntimeError("research service is closed")
            if self._degraded:
                raise RuntimeError(f"research persistence/supervisor unavailable: {self._degraded}")
            if (
                len(
                    set(self._pending)
                    | {
                        identity
                        for identity, plan in self._cancel_intents.items()
                        if not plan["confirmed"]
                    }
                )
                >= 32
            ):
                raise ValueError("research queue is full")
            job_id = uuid4().hex
            status = {
                "schema_version": 1,
                "id": job_id,
                "kind": kind,
                "state": "queued",
                "created_at_ms": int(time.time() * 1000),
                "parameters": parameters,
            }
            write_json(self._job_path(job_id), status)
            self._requests[job_id] = kind, parameters
            self._pending.append(job_id)
            self._condition.notify_all()
            return status

    def _job_path(self, job_id: str) -> Path:
        if not _valid_id(job_id):
            raise ValueError("invalid job ID")
        return self.root / "jobs" / f"{job_id}.json"

    def status(self, job_id: str) -> dict:
        path = self._job_path(job_id)
        with self._condition:
            quarantined = self._quarantine_invalid or (
                self._quarantine and self._quarantine["job_id"] == job_id
            )
            volatile = self._volatile_failures.get(job_id)
            registered = (
                job_id in self._requests
                or job_id in self._pending
                or (self._active is not None and self._active[0] == job_id)
            )
        if quarantined:
            return self._unknown_status(job_id)
        if volatile is not None:
            return dict(volatile)
        try:
            deadline = time.monotonic() + 0.05
            for attempt in range(3):
                try:
                    result = read_status_json(path)
                    break
                except FileNotFoundError as exc:
                    if not registered:
                        raise KeyError(job_id) from exc
                    with self._condition:
                        retired = self._closed or self._degraded
                    if retired or attempt == 2 or time.monotonic() >= deadline:
                        raise ResearchMetadataUnavailable(
                            "research job metadata unavailable"
                        ) from exc
                    time.sleep(min((attempt + 1) * 0.005, max(0, deadline - time.monotonic())))
            if not isinstance(result, dict):
                raise ValueError("job metadata must be an object")
            if self._quarantine and (
                result.get("id") != job_id
                or not isinstance(result.get("kind"), str)
                or not isinstance(result.get("state"), str)
                or type(result.get("created_at_ms")) is not int
            ):
                raise ValueError("invalid quarantined job metadata")
        except (OSError, ValueError, TypeError):
            if not self._quarantine:
                raise
            result = self._unknown_status(job_id)
            result["error"] = "RESEARCH_QUARANTINE; unreadable job metadata preserved"
            return result
        with self._condition:
            quarantined = self._quarantine_invalid or (
                self._quarantine and self._quarantine["job_id"] == job_id
            )
            volatile = self._volatile_failures.get(job_id)
        if quarantined:
            return self._unknown_status(job_id)
        if volatile is not None:
            return dict(volatile)
        if self._quarantine and result.get("state") not in TERMINAL_STATES:
            result.update(
                dispatch_state="blocked",
                error="RESEARCH_QUARANTINE; dispatch disabled pending manual verification",
            )
            if result.get("state") != "queued":
                result.update(state="unknown", computation_state="unknown")
        return result

    def list_jobs(self) -> list[dict]:
        return sorted(
            (
                self.status(path.stem)
                for path in (self.root / "jobs").glob("*.json")
                if _valid_id(path.stem) and not path.is_symlink()
            ),
            key=lambda value: value["created_at_ms"],
            reverse=True,
        )

    def list_job_summaries(self, limit: int = 100) -> list[dict]:
        """Bound JSON reads by recent file activity; never expose large job parameters."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("job summary limit must be within [1, 1000]")
        with self._condition:
            registered = set(self._requests) | set(self._pending)
            active = self._active[0] if self._active else None
            if active is not None:
                registered.add(active)
        candidates, listed = [], set()
        for path in (self.root / "jobs").glob("*.json"):
            if _valid_id(path.stem) and not path.is_symlink():
                try:
                    candidates.append((path.stat().st_mtime_ns, path))
                    listed.add(path.stem)
                except OSError:
                    continue
        # A registered job can lose its directory entry as well as an open.
        # Show those unavailable owners first (active before queued), without
        # inventing an mtime or a created_at timestamp. Keep normal mtime order.
        missing = [
            self._job_path(identity)
            for identity in sorted(registered - listed, key=lambda value: (value != active, value))
        ]
        paths = missing + [path for _, path in sorted(candidates, reverse=True)]
        result = []
        fields = {
            "id",
            "kind",
            "state",
            "created_at_ms",
            "started_at_ms",
            "finished_at_ms",
            "result_file",
            "dispatch_state",
            "computation_state",
        }
        for path in paths[:limit]:
            try:
                status = self.status(path.stem)
                summary = {key: status[key] for key in fields if key in status}
                if "error" in status:
                    summary["error"] = str(status["error"])[:512]
                result.append(summary)
            except (ResearchMetadataUnavailable, KeyError) as exc:
                if isinstance(exc, KeyError) and (
                    path.stem not in registered or exc.args != (path.stem,)
                ):
                    raise
                result.append(
                    {
                        "id": path.stem,
                        "state": "unknown",
                        "computation_state": "unknown",
                        "persistence": "unavailable",
                        "error": "job metadata unavailable",
                    }
                )
            except (OSError, ValueError, TypeError):
                result.append(
                    {
                        "id": path.stem,
                        "state": "unknown",
                        "computation_state": "unknown",
                        "persistence": "unavailable",
                        "error": "unreadable job metadata",
                    }
                )
        return result

    def cancel(self, job_id: str) -> dict:
        with self._close_lock:
            return self._cancel(job_id)

    def _cancel(self, job_id: str) -> dict:
        with self._condition:
            plan = self._cancel_intents.get(job_id)
            quarantined = bool(self._quarantine or self._quarantine_invalid)
        if plan is not None:
            if quarantined:
                raise ResearchMetadataUnavailable(
                    "research quarantine blocks cancellation acknowledgement"
                )
            self._confirm_close_cancellation(job_id, plan)
            if not self._closed:
                with self._condition:
                    self._cancel_intents.pop(job_id)
            return deepcopy(plan["target"])
        status = self.status(job_id)
        with self._condition:
            if status["state"] in TERMINAL_STATES:
                if job_id in self._pending:
                    raise ResearchMetadataUnavailable(
                        "queued metadata has a foreign terminal state"
                    )
                return status
            if job_id in self._pending and not (
                self._degraded or self._quarantine or self._quarantine_invalid
            ):
                if len(self._cancel_intents) >= 32:
                    raise ValueError("research cancellation acknowledgement budget is full")
                # Confirm the original and reserve its fixed target before the
                # accepted job loses its dispatch slot or request provenance.
                plan = self._cancellation_plan(job_id)
                self._capture_close_cancellation(job_id, plan, original=status)
                self._cancel_intents[job_id] = plan
                self._pending.remove(job_id)
                self._requests.pop(job_id, None)
            elif self._active and self._active[0] == job_id:
                self._active[1].set()
            else:
                # The worker may have completed while status was being read.
                # Never publish the earlier queued view over its terminal JSON.
                self._condition.notify_all()
                status = None
            self._condition.notify_all()
        if plan is not None:
            try:
                self._confirm_close_cancellation(job_id, plan)
            except Exception as error:
                plan["error_type"] = plan["error_type"] or type(error).__name__
                raise
            with self._condition:
                self._cancel_intents.pop(job_id)
            return deepcopy(plan["target"])
        return self.status(job_id) if status is None else status

    def health(self) -> dict:
        return {
            "state": "closed" if self._closed else "degraded" if self._degraded else "ready",
            "supervisor_alive": self._thread.is_alive(),
            "error": self._degraded,
            "quarantined": bool(self._quarantine or self._quarantine_invalid),
        }

    def _loop(self) -> None:
        try:
            self._schedule()
        except Exception as exc:
            # Storage can fail while transitioning a dequeued job, including before
            # any process exists. Keep the API honest instead of leaving dead queued work.
            with self._condition:
                self._degraded = f"{type(exc).__name__}: {exc}"
                identities = list(self._pending)
                if self._last_job_id:
                    identities.append(self._last_job_id)
                for identity in identities:
                    kind = self._requests.get(identity, ("unknown", {}))[0]
                    self._volatile_failures[identity] = {
                        "schema_version": 1,
                        "id": identity,
                        "kind": kind,
                        "state": "failed",
                        "created_at_ms": int(time.time() * 1000),
                        "error": f"research supervisor/storage failure: {self._degraded}",
                        "persistence": "memory_only",
                        "computation_state": "unknown",
                    }
                if self._active:
                    self._active[1].set()
                if self._unconfirmed_process is None:
                    self._active = None
                self._pending.clear()
                self._requests.clear()
                self._condition.notify_all()

    def _schedule(self) -> None:
        while True:
            with self._condition:
                self._condition.wait_for(
                    lambda: self._closed or self._degraded or bool(self._pending)
                )
                if self._closed or self._degraded:
                    return
                job_id = self._pending.pop(0)
                self._last_job_id = job_id
                kind, parameters = self._requests.pop(job_id)
                cancel = self._context.Event()
                self._active = job_id, cancel
            status = self.status(job_id)
            with self._condition:
                if self._closed or self._degraded or cancel.is_set():
                    if not self._quarantine and not self._quarantine_invalid:
                        status.update(state="cancelled", finished_at_ms=int(time.time() * 1000))
                        self._publish_status(job_id, status)
                    if self._unconfirmed_process is None:
                        self._active = None
                    self._condition.notify_all()
                    if self._closed or self._degraded:
                        return
                    continue
                status.update(state="running", started_at_ms=int(time.time() * 1000))
                write_json(self._job_path(job_id), status)
            process = self._context.Process(
                target=_run_job,
                args=(str(self.root), job_id, kind, parameters, cancel)
                + ((self._handlers[kind],) if kind in self._handlers else ()),
                name=f"omega-research-{kind}",
            )
            with self._condition:
                self._running_process = job_id, process
            started = time.monotonic()
            cancelled_at = None
            timed_out = False
            try:
                with self._condition:
                    if self._closed or self._degraded or cancel.is_set():
                        if not self._quarantine and not self._quarantine_invalid:
                            status.update(state="cancelled", finished_at_ms=int(time.time() * 1000))
                            self._publish_status(job_id, status)
                        if self._closed or self._degraded:
                            return
                        continue
                    process.start()
                while process.is_alive():
                    process.join(0.1)
                    if time.monotonic() - started > parameters.get("timeout_seconds", 3600):
                        timed_out = True
                        cancel.set()
                    if cancel.is_set():
                        if cancelled_at is None:
                            cancelled_at = time.monotonic()
                        elif time.monotonic() - cancelled_at > 2:
                            if not self._stop_process(process):
                                self._quarantine_worker(job_id, process)
                                return
                            break
                if self._quarantine or self._quarantine_invalid:
                    return
                status = self.status(job_id)
                if status["state"] not in TERMINAL_STATES:
                    if not _recover_final(self.root, status):
                        state = "failed" if timed_out or not cancel.is_set() else "cancelled"
                        status.update(
                            state=state,
                            error="worker deadline exceeded"
                            if timed_out
                            else f"worker exit {process.exitcode}; partial output is uncommitted",
                            finished_at_ms=int(time.time() * 1000),
                        )
                    self._publish_status(job_id, status)
                elif timed_out and status["state"] == "cancelled":
                    status.update(state="failed", error="worker deadline exceeded")
                    self._publish_status(job_id, status)
            except Exception as exc:
                if self._quarantine or self._quarantine_invalid:
                    return
                if process.pid is not None and process.is_alive():
                    cancel.set()
                    if not self._stop_process(process):
                        self._quarantine_worker(job_id, process)
                        return
                status = self.status(job_id)
                status.update(state="failed", error=f"worker start/supervisor error: {exc}")
                self._publish_status(job_id, status)
            finally:
                if process.pid is not None:
                    if process.is_alive():
                        cancel.set()
                        if self._unconfirmed_process is not process:
                            if not self._stop_process(process):
                                self._quarantine_worker(job_id, process)
                    if not process.is_alive() and self._unconfirmed_process is not process:
                        process.close()
                with self._condition:
                    if self._unconfirmed_process is None:
                        self._active = None
                        self._running_process = None
                    self._condition.notify_all()

    def _cancellation_plan(self, job_id):
        return {
            "original": None,
            "target": None,
            "request_sha256": _close_metadata_fingerprint(self._requests[job_id]),
            "capture_failed": False,
            "confirmed": False,
            "error_type": None,
        }

    def _capture_close_cancellation(self, job_id, plan, *, original=None):
        if plan["capture_failed"]:
            raise ResearchMetadataUnavailable("original queued metadata was not confirmed")
        path = self._job_path(job_id)
        if plan["original"] is None:
            try:
                original = read_status_json(path) if original is None else deepcopy(original)
                if (
                    type(original) is not dict
                    or set(original)
                    != {"schema_version", "id", "kind", "state", "created_at_ms", "parameters"}
                    or type(original["schema_version"]) is not int
                    or original["schema_version"] != 1
                    or original["id"] != job_id
                    or original["state"] != "queued"
                    or type(original["created_at_ms"]) is not int
                    or original["created_at_ms"] < 0
                    or _close_metadata_fingerprint((original["kind"], original["parameters"]))
                    != plan["request_sha256"]
                ):
                    raise ResearchMetadataUnavailable(
                        "queued metadata differs from its original request"
                    )
                target = dict(original)
                target.update(state="cancelled", finished_at_ms=int(time.time() * 1000))
                # Freeze before the first write, including its original timestamp.
                plan.update(
                    original=original,
                    target=target,
                    original_sha256=_close_metadata_fingerprint(original),
                    target_sha256=_close_metadata_fingerprint(target),
                )
            except Exception:
                plan["capture_failed"] = True
                raise

    def _confirm_close_cancellation(self, job_id, plan):
        self._capture_close_cancellation(job_id, plan)
        path = self._job_path(job_id)
        if plan["confirmed"]:
            return
        if (
            _close_metadata_fingerprint(plan["original"]) != plan["original_sha256"]
            or _close_metadata_fingerprint(plan["target"]) != plan["target_sha256"]
        ):
            raise ResearchMetadataUnavailable("fixed research cancellation intent changed")
        current = read_status_json(path)
        observed = _close_metadata_fingerprint(current)
        if observed == plan["target_sha256"]:
            plan["confirmed"] = True
            return
        if observed != plan["original_sha256"]:
            raise ResearchMetadataUnavailable("research cancellation metadata changed; preserved")
        write_json(path, plan["target"])
        if _close_metadata_fingerprint(read_status_json(path)) != plan["target_sha256"]:
            raise ResearchMetadataUnavailable("research cancellation acknowledgement is unknown")
        plan["confirmed"] = True

    def close(self) -> None:
        with self._close_lock:
            self._close()

    def _close(self) -> None:
        with self._condition:
            pending = (
                list(self._pending)
                if not (self._closed or self._quarantine or self._quarantine_invalid)
                else []
            )
            # Fence dispatch before releasing the lock for status reads. Even a
            # metadata failure below must not leave a live worker un-signalled.
            self._closed = True
            if not self._close_initialized:
                if len(set(pending) | set(self._cancel_intents)) > 32:
                    self._close_capacity_error = True
                for job_id in pending:
                    if job_id in self._cancel_intents:
                        continue
                    if len(self._cancel_intents) >= 32:
                        break
                    try:
                        plan = self._cancellation_plan(job_id)
                    except Exception as error:
                        plan = {
                            "original": None,
                            "capture_failed": True,
                            "confirmed": False,
                            "error_type": type(error).__name__,
                        }
                    self._cancel_intents[job_id] = plan
                self._close_initialized = True
            self._pending.clear()
            self._requests.clear()
            if self._active:
                self._active[1].set()
            self._condition.notify_all()
        failure = None
        try:
            for job_id, plan in self._cancel_intents.items():
                try:
                    if self._quarantine or self._quarantine_invalid:
                        raise ResearchMetadataUnavailable(
                            "research quarantine blocks cancellation acknowledgement"
                        )
                    self._confirm_close_cancellation(job_id, plan)
                except Exception as exc:
                    plan["error_type"] = plan["error_type"] or type(exc).__name__
                    if failure is None:
                        failure = exc
        finally:
            with self._condition:
                self._pending.clear()
                self._requests.clear()
                if self._active:
                    self._active[1].set()
                self._condition.notify_all()
        self._thread.join(6)
        if self._thread.is_alive():
            with self._condition:
                if self._active and not self._quarantine:
                    process = self._running_process[1] if self._running_process else None
                    self._quarantine_worker(self._active[0], process)
            raise RuntimeError("research worker did not stop within its shutdown deadline")
        with self._condition:
            process = self._unconfirmed_process
            if process is not None:
                if process.is_alive():
                    raise RuntimeError("research child exit remains unconfirmed")
                process.close()
                self._unconfirmed_process = None
                self._active = None
                self._running_process = None
        if failure is not None:
            raise RuntimeError("research shutdown metadata unavailable") from failure
        if self._close_capacity_error:
            raise ResearchMetadataUnavailable("research close cancellation capacity exceeded")
