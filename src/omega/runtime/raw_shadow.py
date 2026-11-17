"""Data-only binding between a raw preparation child and the shadow controller.

The snapshot cutoff is actual preparation time, not historical availability.
No plugin or framework is imported by this owner-side adapter.
"""

# Imports
import re
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market

MAX_TIME = 253_402_300_799_000
RAW_KEYS = frozenset(
    {
        "expected_revision",
        "stream",
        "instrument",
        "sequence",
        "open_time_ms",
        "deadline_ms",
        "dataset",
        "retrospective_confirmed",
    }
)


def parameters(value):
    if type(value) is not dict or set(value) != RAW_KEYS:
        raise ValidationError("raw shadow parameters mismatch")
    if (
        value["retrospective_confirmed"] is not True
        or type(value["expected_revision"]) is not int
        or value["expected_revision"] < 0
        or type(value["sequence"]) is not int
        or not 1 <= value["sequence"] <= 2**63 - 1
        or type(value["stream"]) is not str
        or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value["stream"]) is None
        or type(value["dataset"]) is not str
        or not 1 <= len(value["dataset"]) <= 4096
        or "\x00" in value["dataset"]
        or any(
            type(value[k]) is not int or not 0 <= value[k] <= MAX_TIME
            for k in ("open_time_ms", "deadline_ms")
        )
        or value["open_time_ms"] % 60000
    ):
        raise ValidationError("raw shadow needs explicit retrospective data and valid clocks")
    try:
        provider, market, symbol = value["instrument"].split(":")
        if Instrument(provider, Market(market), symbol).key != value["instrument"]:
            raise ValueError("noncanonical instrument")
    except (AttributeError, TypeError, ValueError) as error:
        raise ValidationError("raw shadow instrument mismatch") from error
    return deepcopy(value)


def context(deployment, values, now_ms):
    """Check actual controller state before both submission and prepared acceptance."""
    values = parameters(values)
    row = deployment.streams.get(values["stream"] + "|" + values["instrument"])
    if (
        deployment.candidate is None
        or deployment.paused
        or deployment.revision != values["expected_revision"]
        or row is None
        or row["health"] != "ready"
        or row["sequence"] + 1 != values["sequence"]
        or row["open_ms"] is not None
        and values["open_time_ms"] != row["open_ms"] + 60000
        or row["accepted_at_ms"] is not None
        and now_ms < row["accepted_at_ms"]
        or not values["open_time_ms"] + 60000 <= now_ms < values["deadline_ms"]
        or values["deadline_ms"] > now_ms + 60000
        or any(
            item["stream"] == values["stream"] + "|" + values["instrument"]
            for item in deployment.requests.values()
        )
    ):
        raise ValidationError("raw shadow stream/context changed, paused or expired")
    return row


def prepare(deployment, values, now_ms):
    from omega.research.inference_input import manifest_identity, parse_request

    row = context(deployment, values, now_ms)
    candidate = deployment.candidate
    metadata = candidate.metadata()
    if not metadata["module"]["capabilities"].get("prepare_inference", False):
        raise ValidationError("active frozen module has no raw inference preparation capability")
    request = {
        "candidate_id": candidate.candidate_id,
        "module_sha256": metadata["module_sha256"],
        "checkpoint_sha256": metadata["checkpoint_sha256"],
        "dataset_sha256": manifest_identity(values["dataset"]),
        "contract": metadata["contract"],
        "instrument": values["instrument"],
        "open_time_ms": values["open_time_ms"],
        "stream": values["stream"],
        "sequence": values["sequence"],
        "state_hash": row["state_hash"],
        "core_epoch": deployment.core_epoch,
        "deployment_epoch": deployment.epoch,
        "stream_epoch": row["epoch"],
        "deadline_ms": values["deadline_ms"],
    }
    declared = metadata["module"].get("input", {})
    if type(declared) is not dict:
        raise ValidationError("raw module input descriptor must be an object")
    if "raw_lookback" in declared:
        request["raw_lookback"] = declared["raw_lookback"]
    request = parse_request(request)
    return candidate, {
        "training_run": str(Path(candidate.path) / "bundle"),
        "dataset": str(Path(values["dataset"]).absolute()),
        "request": request,
        "checkpoint": metadata["checkpoint"],
        "trusted_module": True,
        "timeout_seconds": metadata["timeout_seconds"],
    }


def current(deployment, operation, now_ms):
    row = context(deployment, operation["parameters"], now_ms)
    request = operation["intent"]["parameters"]["request"]
    if (
        deployment.candidate.candidate_id != request["candidate_id"]
        or deployment.epoch != request["deployment_epoch"]
        or deployment.core_epoch != request["core_epoch"]
        or row["epoch"] != request["stream_epoch"]
        or row["state_hash"] != request["state_hash"]
    ):
        raise ValidationError("raw preparation belongs to a retired context")


def accept_prepared(deployment, operation, job, root, now_ms):
    from omega.research.inference_input import encoded, read_prepared

    current(deployment, operation, now_ms)
    if (
        job.get("id") != operation["job_id"]
        or job.get("kind") != "nn_prepare"
        or job.get("state") != "complete"
        or job.get("parameters") != operation["intent"]["parameters"]
    ):
        raise ValidationError("raw preparation job identity changed")
    path = Path(root) / "artifacts" / job["id"] / "preparation/prepared.json"
    if Path(job.get("result_file", "")).absolute() != path.absolute():
        raise ValidationError("raw result does not belong to its actual preparation job")
    expected = operation["intent"]["parameters"]["request"]
    prepared = read_prepared(path, expected)
    metadata = deployment.candidate.metadata()
    if prepared["module"] != metadata["module"]:
        raise ValidationError("raw preparation module descriptor changed")
    values = operation["parameters"]
    request = deployment.request(
        values["stream"],
        values["instrument"],
        sequence=values["sequence"],
        open_time_ms=values["open_time_ms"],
        as_of_ms=prepared["as_of_ms"],
        now_ms=now_ms,
        deadline_ms=values["deadline_ms"],
        input_digest=prepared["prepared_sha256"],
        batch=prepared["batch"],
        schema_id=expected["contract"]["schema_id"],
        features=tuple(expected["contract"]["features"]),
        lookback=expected["contract"]["lookback"],
    )
    proof = {
        "job_id": job["id"],
        "request": expected,
        "file": str(path),
        "document_sha256": sha256(encoded(prepared)).hexdigest(),
        **{
            key: prepared[key]
            for key in (
                "scope",
                "prepared_sha256",
                "dataset_sha256",
                "rows_sha256",
                "window_sha256",
                "batch_sha256",
                "reference_time_ms",
                "snapshot_available_at_ms",
                "as_of_ms",
                "lineage",
            )
        },
    }
    return request, proof


def verify_proof(proof, request, root):
    from omega.research.inference_input import encoded, read_prepared

    identity = proof.get("job_id")
    if type(identity) is not str or re.fullmatch("[a-f0-9]{32}", identity) is None:
        raise ValidationError("raw preparation proof job identity mismatch")
    path = Path(root) / "artifacts" / identity / "preparation/prepared.json"
    if Path(proof["file"]).absolute() != path.absolute():
        raise ValidationError("raw preparation proof path changed")
    prepared = read_prepared(path, proof["request"])
    if (
        sha256(encoded(prepared)).hexdigest() != proof["document_sha256"]
        or prepared["prepared_sha256"] != request["input_digest"]
        or prepared["batch_sha256"] != request["batch_hash"]
    ):
        raise ValidationError("prepared shadow lineage changed after frozen dispatch")
