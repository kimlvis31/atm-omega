"""Data-only validation for the causal service's explicit control boundary."""

import re
from decimal import Decimal
from pathlib import Path

from omega.domain.errors import ValidationError
from omega.domain.inference import (
    InferenceFeatures,
    InferenceModelIdentity,
    InferenceOutputBinding,
    checksum,
    digest,
    encoded,
    identity,
    timestamp,
)
from omega.domain.model_contract import ShadowModelContract
from omega.infrastructure.market_data import instrument_key
from omega.research.artifacts import bounded_artifact_hash, validate_artifact_path
from omega.runtime.causal_nn_proof import read_document

KINDS = {"activate", "reset_stream", "predict_committed", "pause", "retire"}
SCOPE = "committed_inference_service"
FLAGS = {"scope": SCOPE, "live_orders_enabled": False, "strategy_delivery_enabled": False}
TERMINAL = {"complete", "failed", "unknown", "cancelled"}


def _error(value):
    if value is not None:
        if (
            type(value) is not dict
            or set(value) != {"code", "message", "manual_required"}
            or value["message"] != "Committed inference needs explicit verification or recovery."
            or value["manual_required"] is not True
        ):
            raise ValidationError("invalid persisted causal error record")
        identity(value["code"])


def _feature(value, stream, instrument):
    keys = {
        "request_id",
        "input_sha256",
        "frame_sha256",
        "open_time_ms",
        "decision_at_ms",
        "accepted_at_ms",
        "valid_until_ms",
        "values",
    }
    if (
        type(value) is not dict
        or set(value) != keys
        or type(value["values"]) is not dict
        or not 1 <= len(value["values"]) <= 64
    ):
        raise ValidationError("invalid persisted causal feature view")
    values = []
    for name, text in value["values"].items():
        if type(text) is not str or not 1 <= len(text) <= 256:
            raise ValidationError("invalid persisted exact NN value")
        number = Decimal(text)
        if (
            not number.is_finite()
            or len(number.as_tuple().digits) > 128
            or abs(number.adjusted()) > 100
        ):
            raise ValidationError("invalid persisted finite NN value")
        values.append((name, number))
    return InferenceFeatures(
        value["request_id"],
        stream,
        instrument_key(instrument),
        value["open_time_ms"],
        value["input_sha256"],
        value["frame_sha256"],
        value["decision_at_ms"],
        tuple(values),
        value["accepted_at_ms"],
        value["valid_until_ms"],
    )


def _stream(value, stream, deployment):
    keys = {
        "deployment_id",
        "stream",
        "stream_epoch",
        "instrument",
        "state",
        "reason",
        "last_open_ms",
        "sequence",
        "pending_operation_id",
        "feature",
        "manual_required",
    }
    if (
        type(value) is not dict
        or set(value) != keys
        or value["stream"] != stream
        or value["deployment_id"] != deployment
        or value["state"] not in {"ready", "blocked", "manual_required"}
        or type(value["manual_required"]) is not bool
        or type(value["sequence"]) is not int
        or not 0 <= value["sequence"] <= 2**53
    ):
        raise ValidationError("invalid persisted causal stream view")
    identity(stream)
    identity(value["stream_epoch"])
    identity(value["reason"])
    instrument_key(value["instrument"])
    if value["last_open_ms"] is not None:
        timestamp(value["last_open_ms"])
        if value["last_open_ms"] % 60000:
            raise ValidationError("invalid persisted stream minute")
    if value["pending_operation_id"] is not None:
        operation_id(value["pending_operation_id"])
    if value["feature"] is not None:
        _feature(value["feature"], stream, value["instrument"])


def operation_id(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,96}", value) is None:
        raise ValidationError("invalid causal inference operation ID")
    return value


def parse_request(kind, parameters):
    if type(kind) is not str or kind not in KINDS or type(parameters) is not dict:
        raise ValidationError("unsupported causal inference operation")
    encoded(parameters, maximum=65536)
    common = {"core_epoch", "revision"}
    fields = {
        "activate": {"training_job_id", "deployment_id", "trusted_module", "contract", "bindings"},
        "reset_stream": {"deployment_id", "stream", "instrument", "state_policy"},
        "predict_committed": {
            "deployment_id",
            "stream",
            "instrument",
            "open_time_ms",
            "timeout_ms",
        },
        "pause": {"deployment_id"},
        "retire": {"deployment_id"},
    }
    if set(parameters) != common | fields[kind]:
        raise ValidationError("unexpected or missing causal inference parameters")
    identity(parameters["core_epoch"])
    if type(parameters["revision"]) is not int or parameters["revision"] < 0:
        raise ValidationError("invalid causal inference control revision")
    deployment = parameters["deployment_id"]
    if deployment is not None:
        identity(deployment)
    elif kind != "activate":
        raise ValidationError("causal inference deployment ID required")
    if kind == "activate":
        if (
            type(parameters["training_job_id"]) is not str
            or re.fullmatch("[0-9a-f]{32}", parameters["training_job_id"]) is None
            or parameters["trusted_module"] is not True
        ):
            raise ValidationError("explicit trusted completed training job required")
        ShadowModelContract.parse(parameters["contract"])
        parse_bindings(parameters["bindings"])
    if kind in {"reset_stream", "predict_committed"}:
        identity(parameters["stream"])
        instrument_key(parameters["instrument"])
    if kind == "reset_stream" and (
        type(parameters["state_policy"]) is not str
        or parameters["state_policy"] not in {"reset", "preserve"}
    ):
        raise ValidationError("explicit reset or preserve state policy required")
    if kind == "predict_committed":
        timestamp(parameters["open_time_ms"])
        if (
            parameters["open_time_ms"] % 60000
            or type(parameters["timeout_ms"]) is not int
            or not 1000 <= parameters["timeout_ms"] <= 30000
        ):
            raise ValidationError("aligned minute and 1000..30000ms prediction timeout required")
    return digest({"kind": kind, "parameters": parameters})


def parse_bindings(value):
    if type(value) is not list or not 1 <= len(value) <= 64:
        raise ValidationError("explicit bounded NN output bindings required")
    bindings = []
    for row in value:
        if type(row) is not dict or set(row) != {"name", "path"} or type(row["path"]) is not list:
            raise ValidationError("invalid causal output binding schema")
        bindings.append(InferenceOutputBinding(row["name"], tuple(row["path"])))
    if len({row.name for row in bindings}) != len(bindings):
        raise ValidationError("duplicate causal NN output field")
    return tuple(bindings)


def registered_training(root, job_id, provider):
    """Only a complete registered job's actual bounded immutable artifacts qualify."""
    status = provider(job_id)
    final = validate_artifact_path(Path(root) / "artifacts" / job_id)
    source = final / "training"
    run_path = source / "run.json"
    if (
        type(status) is not dict
        or status.get("id") != job_id
        or status.get("kind") != "train"
        or status.get("state") != "complete"
        or Path(status.get("artifact", "")) != final
        or Path(status.get("result_file", "")) != run_path
    ):
        raise ValidationError("completed registered training job under this project required")
    marker = read_document(final / "artifact.json")
    if (
        marker.get("schema_version") != 1
        or marker.get("job_id") != job_id
        or marker.get("kind") != "train"
        or marker.get("state") != "complete"
        or marker.get("result_file") != "training/run.json"
        or type(marker.get("files")) is not list
        or len(marker["files"]) > 10000
    ):
        raise ValidationError("registered training artifact marker mismatch")
    run = read_document(run_path, maximum=2 * 1024 * 1024)
    selected = (run.get("model") or {}).get("file")
    if (
        type(selected) is not str
        or Path(selected).is_absolute()
        or ".." in Path(selected).parts
        or "\\" in selected
        or ":" in selected
    ):
        raise ValidationError("registered training checkpoint must be a relative artifact")
    required = {
        "training/run.json": 2 * 1024 * 1024,
        "training/instance_module.py": 1024 * 1024,
        "training/" + selected: 512 * 1024 * 1024,
    }
    for name, maximum in required.items():
        matches = [row for row in marker["files"] if type(row) is dict and row.get("file") == name]
        if len(matches) != 1 or bounded_artifact_hash(final / name, max_bytes=maximum) != matches[
            0
        ].get("sha256"):
            raise ValidationError("registered training artifact content mismatch")
    return source


def validate_journal(data):
    keys = {
        "scope",
        "schema_version",
        "revision",
        "core_epoch",
        "control_revision",
        "deployments",
        "operations",
    }
    if (
        type(data) is not dict
        or set(data) != keys
        or data["scope"] != "committed_1m_inference"
        or type(data["schema_version"]) is not int
        or data["schema_version"] != 1
        or type(data["revision"]) is not int
        or data["revision"] < 0
        or type(data["control_revision"]) is not int
        or data["control_revision"] < 0
        or type(data["deployments"]) is not dict
        or len(data["deployments"]) > 32
        or type(data["operations"]) is not dict
        or len(data["operations"]) > 256
    ):
        raise ValidationError("invalid bounded causal service registry; original preserved")
    identity(data["core_epoch"])
    for key, row in data["operations"].items():
        operation_id(key)
        required = {
            "id",
            "kind",
            "parameters",
            "request_digest",
            "core_epoch",
            "submitted_revision",
            "revision",
            "state",
            "created_at_ms",
            "started_at_ms",
            "finished_at_ms",
            "durable",
            "result",
            "error",
        }
        if (
            type(row) is not dict
            or set(row) != required
            or row["id"] != key
            or row["state"] not in TERMINAL | {"queued", "running"}
            or row["durable"] is not True
            or row["request_digest"] != parse_request(row["kind"], row["parameters"])
        ):
            raise ValidationError("invalid persisted causal operation")
        identity(row["core_epoch"])
        if (
            row["core_epoch"] != row["parameters"]["core_epoch"]
            or row["submitted_revision"] != row["parameters"]["revision"]
            or type(row["revision"]) is not int
            or not row["submitted_revision"] < row["revision"] <= data["control_revision"]
        ):
            raise ValidationError("invalid persisted operation revision")
        for name in ("created_at_ms", "started_at_ms", "finished_at_ms"):
            if row[name] is not None:
                timestamp(row[name])
        _error(row["error"])
        result = row["result"]
        if result is not None:
            expected = {
                "activate": {"deployment_id"},
                "reset_stream": {"deployment_id", "stream", "stream_epoch"},
                "pause": {"deployment_id"},
                "retire": {"deployment_id"},
                "predict_committed": {"deployment_id", "stream", "request_id"},
            }[row["kind"]]
            if type(result) is not dict or set(result) not in (
                expected,
                expected | {"feature"} if row["kind"] == "predict_committed" else expected,
                expected | {"feature", "no_signal"}
                if row["kind"] == "predict_committed"
                else expected,
            ):
                raise ValidationError("invalid persisted causal result schema")
            identity(result["deployment_id"])
            for name in ("stream", "stream_epoch"):
                if name in result:
                    identity(result[name])
            if "request_id" in result:
                checksum(result["request_id"])
            if "no_signal" in result:
                if (
                    row["state"] != "complete"
                    or result["no_signal"] is not True
                    or result.get("feature") is not None
                    or not data["deployments"]
                    .get(result["deployment_id"], {})
                    .get("model", {})
                    .get("allow_no_signal")
                ):
                    raise ValidationError("invalid persisted NoSignal acceptance")
            elif "feature" in result:
                _feature(result["feature"], result["stream"], row["parameters"]["instrument"])
    for key, row in data["deployments"].items():
        identity(key)
        required = {
            "id",
            "state",
            "training_job_id",
            "model",
            "manifest_sha256",
            "contract",
            "bindings",
            "streams",
            "uncertain_streams",
            "error",
        }
        if (
            type(row) is not dict
            or set(row) != required
            or row["id"] != key
            or row["state"]
            not in {"preparing", "ready", "paused", "manual_required", "retired", "failed"}
            or type(row["streams"]) is not dict
            or len(row["streams"]) > 128
            or type(row["uncertain_streams"]) is not list
            or len(row["uncertain_streams"]) > 128
        ):
            raise ValidationError("invalid persisted causal deployment")
        ShadowModelContract.parse(row["contract"])
        parse_bindings(row["bindings"])
        if (
            type(row["training_job_id"]) is not str
            or re.fullmatch("[a-f0-9]{32}", row["training_job_id"]) is None
        ):
            raise ValidationError("invalid persisted training job identity")
        _error(row["error"])
        if row["model"] is not None:
            model = InferenceModelIdentity(**row["model"])
            if model.input_schema_sha256 != digest(row["contract"]):
                raise ValidationError("persisted model and input contract disagree")
            checksum(row["manifest_sha256"])
        elif row["state"] not in {"preparing", "failed", "retired"}:
            raise ValidationError("incomplete causal activation is not resumable")
        for stream, value in row["streams"].items():
            _stream(value, stream, key)
        if (
            len(set(row["uncertain_streams"])) != len(row["uncertain_streams"])
            or not set(row["uncertain_streams"]) <= row["streams"].keys()
        ):
            raise ValidationError("invalid uncertain-stream evidence")
    return data
