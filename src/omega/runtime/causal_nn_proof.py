"""Frozen causal inputs and model activation; external Python remains trusted.

The provider must read committed original observations with real local acceptance
times. Hashes bind that assertion; they cannot establish an untrusted provider's
honesty. Retrospective exports, MC and repaired rows are deliberately unsupported.
Training metadata also cannot prove what trusted Python learned from: activation
uses the actual verification time as a conservative lower bound for knowledge.
"""

# Imports
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from omega import __version__
from omega.domain.inference import (
    CausalInferenceFrame,
    InferenceEvidenceReceipt,
    InferenceModelIdentity,
    checksum,
    digest,
    encoded,
    timestamp,
)
from omega.research.artifacts import (
    bounded_artifact_hash,
    copy_bounded_artifact,
    read_status_json,
    validate_artifact_path,
)
from omega.research.datasets import MarketRow
from omega.research.deployment import ShadowModelContract

MAX_INPUT_BYTES = 1024 * 1024
MAX_RUN_BYTES = 2 * 1024 * 1024
MAX_MODULE_BYTES = 1024 * 1024
MAX_CHECKPOINT_BYTES = 512 * 1024 * 1024


def wall_ms():
    return time.time_ns() // 1_000_000


def exclusive_json(path, document, *, maximum=MAX_INPUT_BYTES):
    """Exclusive regular-file write followed by an actual readback ACK."""
    path = validate_artifact_path(Path(path), allow_missing=True)
    raw = encoded(document, maximum=maximum).encode("utf-8")
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    if os.name != "nt":
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    if read_document(path, maximum=maximum) != document:
        raise ValueError("causal snapshot readback mismatch")


def read_document(path, *, maximum=MAX_INPUT_BYTES):
    path = validate_artifact_path(Path(path))
    # Both reads inspect actual regular descriptors; the bounded hash rejects a
    # FIFO before read_status_json and never reads an unbounded growing file.
    before = bounded_artifact_hash(path, max_bytes=maximum)
    result = read_status_json(path)
    if bounded_artifact_hash(path, max_bytes=maximum) != before:
        raise ValueError("causal snapshot changed during read")
    encoded(result, maximum=maximum)
    return result


def checkpoint_path(root, name):
    if (
        type(name) is not str
        or not name
        or Path(name).is_absolute()
        or any(part in {".", ".."} for part in Path(name).parts)
        or ":" in name
        or "\\" in name
    ):
        raise ValueError("checkpoint must be a relative artifact path")
    return validate_artifact_path(Path(root) / name)


@dataclass(frozen=True)
class InferenceActivation:
    root: Path
    manifest_sha256: str
    model: InferenceModelIdentity
    contract: ShadowModelContract

    def verify(self):
        manifest = read_document(self.root / "activation.json")
        if (
            digest(manifest) != self.manifest_sha256
            or manifest["scope"] != "causal_model_activation"
            or manifest["model"] != self.model.document()
            or manifest["contract"] != self.contract.document()
            or manifest["verified_at_ms"] > self.model.available_at_ms
            or manifest["verified_at_ms"] > self.model.training_cutoff_ms
        ):
            raise ValueError("causal activation identity mismatch")
        run_path = self.root / "run.json"
        if bounded_artifact_hash(run_path, max_bytes=MAX_RUN_BYTES) != manifest["run_sha256"]:
            raise ValueError("activated training run changed")
        run = read_document(run_path, maximum=MAX_RUN_BYTES)
        if run.get("dataset", {}).get("auxiliary") is not None:
            raise ValueError(
                "retrospective auxiliary training identity cannot become causal activation"
            )
        if (
            digest(run["dataset"]) != self.model.training_source_sha256
            or run["module_source_sha256"] != self.model.module_sha256
            or run["module"]["module_id"] != self.model.module_id
            or bounded_artifact_hash(self.root / "instance_module.py", max_bytes=MAX_MODULE_BYTES)
            != self.model.module_sha256
            or bounded_artifact_hash(
                checkpoint_path(self.root, manifest["checkpoint"]),
                max_bytes=MAX_CHECKPOINT_BYTES,
            )
            != self.model.checkpoint_sha256
        ):
            raise ValueError("activated model artifact changed")
        return manifest


def prepare_causal_activation(source, destination, contract, *, trusted_module=False):
    """Data-only snapshot. No module import, model restoration or framework here."""
    if trusted_module is not True or not isinstance(contract, ShadowModelContract):
        raise ValueError("explicit trusted Python and tensor contract required")
    if contract.schema_id != "omega.ohlcv.1m.v1":
        raise ValueError("causal inputs currently support OHLCV only")
    source = validate_artifact_path(Path(source))
    run_path = source / "run.json"
    run_hash = bounded_artifact_hash(run_path, max_bytes=MAX_RUN_BYTES)
    run = read_document(run_path, maximum=MAX_RUN_BYTES)
    descriptor = run["module"]
    capabilities = descriptor["capabilities"]
    if run.get("dataset", {}).get("auxiliary") is not None:
        raise ValueError(
            "retrospective auxiliary training identity cannot become causal activation"
        )
    if (
        run.get("state") != "complete"
        or run.get("cleanup_completed") is not True
        or run.get("environment", {}).get("core_version") != __version__
        or descriptor.get("core_version") != __version__
        or descriptor.get("api_version") != 1
        or capabilities.get("predict") is not True
        or capabilities.get("prepare_inference") is not True
        or capabilities.get("online_learning") is True
        or capabilities.get("dynamic_topology") is True
        or run.get("online_learning") is True
        or run.get("fit_scope") != "train_only"
        or bool(capabilities.get("stateful_inference")) != (contract.state_shape is not None)
        or descriptor.get("input", {}).get("schema_id") != contract.schema_id
        or descriptor["input"].get("preprocessing_owner") != "module"
        or descriptor["input"].get("fit_scope") != "train_only"
        or descriptor["input"].get("features", list(contract.features)) != list(contract.features)
        or "lookback" in run.get("config", {})
        and run["config"]["lookback"] != contract.lookback
    ):
        raise ValueError("completed compatible frozen model contract required")
    splits = run["dataset"]["splits"]
    if type(splits) is not dict or set(splits) != {"train", "validation", "test"}:
        raise ValueError("all training/validation/test source bounds required")
    previous = -1
    for name in ("train", "validation", "test"):
        bounds = splits[name]
        if type(bounds) is not list or len(bounds) != 2:
            raise ValueError("invalid training source bounds")
        for value in bounds:
            timestamp(value)
        if not previous <= bounds[0] < bounds[1] or any(v % 60000 for v in bounds):
            raise ValueError("invalid ordered training source bounds")
        previous = bounds[1]
    selected = (run.get("model") or {}).get("file")
    original = checkpoint_path(source, selected)
    matches = [
        item
        for item in run.get("artifacts", [])
        if item.get("file") == selected and item.get("kind") == "checkpoint"
    ]
    if len(matches) != 1:
        raise ValueError("one registered training checkpoint required")
    checkpoint_hash = checksum(matches[0]["sha256"])
    module_hash = checksum(run["module_source_sha256"])
    destination = validate_artifact_path(Path(destination), allow_missing=True)
    destination.mkdir(parents=True, exist_ok=False)
    target_checkpoint = destination / selected
    target_checkpoint.parent.mkdir(parents=True, exist_ok=True)
    for original_path, target, maximum, expected in (
        (run_path, destination / "run.json", MAX_RUN_BYTES, run_hash),
        (
            source / "instance_module.py",
            destination / "instance_module.py",
            MAX_MODULE_BYTES,
            module_hash,
        ),
        (original, target_checkpoint, MAX_CHECKPOINT_BYTES, checkpoint_hash),
    ):
        if (
            copy_bounded_artifact(original_path, target, max_bytes=maximum) != expected
            or bounded_artifact_hash(original_path, max_bytes=maximum) != expected
            or bounded_artifact_hash(target, max_bytes=maximum) != expected
        ):
            raise ValueError("training source changed during activation")
    now = wall_ms()
    if previous > now:
        raise ValueError("training source contains future knowledge")
    # A run's historical finished_at is never treated as deployment knowledge.
    # Future eligible bars must begin after this actual verification timestamp.
    model = InferenceModelIdentity(
        descriptor["module_id"],
        module_hash,
        checkpoint_hash,
        run_hash,
        digest(run["dataset"]),
        now,
        now,
        now,
        digest(contract.document()),
        raw_lookback=descriptor["input"].get("raw_lookback", contract.lookback),
        stateful=contract.state_shape is not None,
        state_schema_sha256=None
        if contract.state_shape is None
        else digest(
            {
                "shape": list(contract.state_shape),
                "version": contract.state_version,
            }
        ),
        allow_no_signal=contract.allow_no_signal,
    )
    manifest = {
        "scope": "causal_model_activation",
        "schema_version": 1,
        "run_sha256": run_hash,
        "checkpoint": selected,
        "verified_at_ms": now,
        "all_source_end_ms": previous,
        "model": model.document(),
        "contract": contract.document(),
        "trust": "local trusted Python; metadata assertions are not training honesty proofs",
    }
    exclusive_json(destination / "activation.json", manifest)
    result = InferenceActivation(destination, digest(manifest), model, contract)
    result.verify()
    return result


@dataclass(frozen=True)
class CommittedInferenceObservation:
    row: MarketRow
    available_at_ms: int
    commit_sha256: str
    origin: str = "committed_market"
    quality: str = "observed"

    def document(self):
        if not isinstance(self.row, MarketRow):
            raise ValueError("typed committed OHLCV observation required")
        timestamp(self.available_at_ms)
        checksum(self.commit_sha256)
        if (
            self.origin != "committed_market"
            or self.quality != "observed"
            or self.available_at_ms < self.row.open_time_ms + 60000
        ):
            raise ValueError("original observed closed OHLCV and real availability required")
        return {
            "row": self.row.as_dict(),
            "available_at_ms": self.available_at_ms,
            "commit_sha256": self.commit_sha256,
            "origin": self.origin,
            "quality": self.quality,
        }


class CommittedInferenceSource(Protocol):
    def read_window(
        self, instrument, start_ms: int, end_ms: int
    ) -> tuple[CommittedInferenceObservation, ...]:
        """Read actual committed rows/acceptance times; no export/repair fallback."""
        ...


class InferenceInputRegistry:
    """One owner freezes verified windows; child reads only request-bound records."""

    def __init__(self, root, activation, source, *, clock_ms=wall_ms):
        self.root = validate_artifact_path(Path(root), allow_missing=True)
        self.root.mkdir(parents=True, exist_ok=False)
        self.activation, self.source, self.clock_ms = activation, source, clock_ms
        self._windows = {}
        activation.verify()

    def frame(self, instrument, open_time_ms):
        timestamp(open_time_ms)
        start = open_time_ms - (self.activation.model.raw_lookback - 1) * 60000
        observations = self.source.read_window(instrument, start, open_time_ms + 60000)
        if (
            type(observations) is not tuple
            or len(observations) != self.activation.model.raw_lookback
        ):
            raise ValueError("committed inference window is incomplete")
        rows = [value.document() for value in observations]
        snapshot = {"schema_id": "omega.ohlcv.1m.v1", "observations": rows}
        encoded(snapshot, maximum=MAX_INPUT_BYTES)
        if [value["row"]["open_time_ms"] for value in rows] != list(
            range(start, open_time_ms + 60000, 60000)
        ) or any(value["row"]["instrument"] != instrument.key for value in rows):
            raise ValueError("committed window identity/continuity mismatch")
        available = max(value["available_at_ms"] for value in rows)
        if available > self.clock_ms():
            raise ValueError("committed inference observation has future availability")
        key = digest(snapshot)
        if key not in self._windows and len(self._windows) >= 128:
            raise ValueError("unconsumed inference snapshot capacity exceeded")
        self._windows[key] = snapshot
        return CausalInferenceFrame(
            instrument, start, open_time_ms, open_time_ms + 60000, available, key, len(rows)
        )

    def verify(self, request):
        self.activation.verify()
        if request.model != self.activation.model:
            raise ValueError("request model differs from verified activation")
        path = self.root / (request.request_id + ".json")
        if path.exists():
            document = read_document(path)
            if (
                document["request"] != request.document()
                or document["input_sha256"] != request.frame.input_sha256
            ):
                raise ValueError("existing causal input request mismatch")
            verified = document["verified_at_ms"]
        else:
            snapshot = self._windows.get(request.frame.input_sha256)
            if snapshot is None:
                raise ValueError("unknown committed inference window")
            verified = self.clock_ms()
            document = {
                "scope": "committed_inference_input",
                "request": request.document(),
                "request_id": request.request_id,
                "input_sha256": request.frame.input_sha256,
                "activation_sha256": self.activation.manifest_sha256,
                "verified_at_ms": verified,
                "snapshot": snapshot,
            }
            validate_input_document(document, request, self.activation, now_ms=verified)
            exclusive_json(path, document)
            self._windows.pop(request.frame.input_sha256, None)
        validate_input_document(document, request, self.activation, now_ms=self.clock_ms())
        return InferenceEvidenceReceipt(
            "committed_inference_evidence", request.request_id, request.frame.input_sha256, verified
        )


def validate_input_document(document, request, activation, *, now_ms):
    """Shared verifier/child check; no mutable DB or raw-shadow files consulted."""
    frame = request.frame
    if (
        type(document) is not dict
        or set(document)
        != {
            "scope",
            "request",
            "request_id",
            "input_sha256",
            "activation_sha256",
            "verified_at_ms",
            "snapshot",
        }
        or document["scope"] != "committed_inference_input"
        or document["request"] != request.document()
        or document["request_id"] != request.request_id
        or document["activation_sha256"] != activation.manifest_sha256
        or request.model != activation.model
        or document["input_sha256"] != frame.input_sha256
        or digest(document["snapshot"]) != frame.input_sha256
        or not request.issued_at_ms <= document["verified_at_ms"] <= now_ms < request.deadline_ms
        or not request.model.available_at_ms <= request.decision_at_ms
        or request.model.training_cutoff_ms > frame.open_time_ms
        or frame.origin != "committed_market"
        or frame.quality != "observed"
        or frame.coverage != 1
    ):
        raise ValueError("causal input identity, availability or deadline mismatch")
    snapshot = document["snapshot"]
    if (
        type(snapshot) is not dict
        or set(snapshot) != {"schema_id", "observations"}
        or snapshot["schema_id"] != "omega.ohlcv.1m.v1"
    ):
        raise ValueError("only observed OHLCV input snapshots supported")
    observations = snapshot["observations"]
    if (
        type(observations) is not list
        or len(observations) != request.model.raw_lookback
        or len(observations) != frame.observed_rows
    ):
        raise ValueError("causal raw window size mismatch")
    rows = []
    for index, value in enumerate(observations):
        if type(value) is not dict or set(value) != {
            "row",
            "available_at_ms",
            "commit_sha256",
            "origin",
            "quality",
        }:
            raise ValueError("invalid observed OHLCV record")
        row = MarketRow.parse(value["row"])
        observed = CommittedInferenceObservation(
            row, value["available_at_ms"], value["commit_sha256"], value["origin"], value["quality"]
        )
        observed.document()
        if (
            row.instrument != frame.instrument.key
            or row.open_time_ms != frame.window_start_ms + index * 60000
            or observed.available_at_ms > request.decision_at_ms
        ):
            raise ValueError("causal observed row identity or availability mismatch")
        rows.append(row)
    if (
        rows[-1].open_time_ms != frame.open_time_ms
        or max(v["available_at_ms"] for v in observations) != frame.available_at_ms
    ):
        raise ValueError("causal window boundary mismatch")
    return tuple(rows)
