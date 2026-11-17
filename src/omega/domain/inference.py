"""Data-only causal inference contracts; source evidence is verified by a host port.

Hashes identify records, not their truth. Training cutoffs include all feature and
label availability used by training. This module cannot discover or verify those
facts from a model file; a trusted composition owner must supply actual evidence.
No receipt authorizes an order or suspends existing position protection.
"""

# Imports
import json
import re
from dataclasses import asdict, dataclass
from decimal import Decimal
from hashlib import sha256

from omega.domain.errors import ValidationError
from omega.domain.models import Instrument

# Constants
MAX_TIME = 253_402_300_799_000
MAX_DOCUMENT_BYTES = 1_048_576
CAUSAL_SCOPE = "committed_1m_inference"


def timestamp(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("inference timestamps require bounded native integers")
    return value


def identity(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValidationError("invalid inference identity")
    return value


def checksum(value):
    if type(value) is not str or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise ValidationError("inference identity requires a SHA256 digest")
    return value


def encoded(value, *, maximum=MAX_DOCUMENT_BYTES):
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(raw.encode("utf-8")) > maximum:
            raise ValidationError("inference record exceeds capacity")
        return raw
    except (TypeError, ValueError, RecursionError) as error:
        raise ValidationError("inference record must be bounded finite JSON") from error


def decoded(value, *, maximum=MAX_DOCUMENT_BYTES):
    if type(value) is not str or len(value.encode("utf-8")) > maximum:
        raise ValidationError("inference record exceeds capacity")

    def pairs(items):
        result = {}
        for key, item in items:
            if key in result:
                raise ValidationError("duplicate inference JSON key")
            result[key] = item
        return result

    try:
        result = json.loads(
            value,
            object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValidationError("nonfinite JSON")),
        )
        encoded(result, maximum=maximum)
        return result
    except (ValueError, TypeError, RecursionError) as error:
        raise ValidationError("invalid inference JSON") from error


def digest(value):
    return sha256(encoded(value).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class InferenceModelIdentity:
    module_id: str
    module_sha256: str
    checkpoint_sha256: str
    revision: str
    training_source_sha256: str
    training_cutoff_ms: int
    training_available_at_ms: int
    available_at_ms: int
    input_schema_sha256: str
    raw_lookback: int = 1
    stateful: bool = False
    state_schema_sha256: str | None = None
    allow_no_signal: bool = False

    def __post_init__(self):
        identity(self.module_id)
        identity(self.revision)
        for value in (
            self.module_sha256,
            self.checkpoint_sha256,
            self.training_source_sha256,
            self.input_schema_sha256,
        ):
            checksum(value)
        for value in (self.training_cutoff_ms, self.training_available_at_ms, self.available_at_ms):
            timestamp(value)
        if (
            not self.training_cutoff_ms <= self.training_available_at_ms <= self.available_at_ms
            or type(self.raw_lookback) is not int
            or not 1 <= self.raw_lookback <= 4096
            or type(self.stateful) is not bool
            or type(self.allow_no_signal) is not bool
        ):
            raise ValidationError("invalid inference model availability/shape contract")
        if self.stateful:
            checksum(self.state_schema_sha256)
        elif self.state_schema_sha256 is not None:
            raise ValidationError("stateless inference cannot declare a recurrent state schema")

    def document(self):
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CausalInferenceFrame:
    """Host-verified facts; a count/hash alone does not prove raw-window continuity."""

    instrument: Instrument
    window_start_ms: int
    open_time_ms: int
    close_time_ms: int
    available_at_ms: int
    input_sha256: str
    observed_rows: int
    origin: str = "committed_market"
    quality: str = "observed"
    coverage: Decimal = Decimal(1)

    def __post_init__(self):
        if not isinstance(self.instrument, Instrument):
            raise ValidationError("inference frame needs a typed instrument")
        for value in (
            self.window_start_ms,
            self.open_time_ms,
            self.close_time_ms,
            self.available_at_ms,
        ):
            timestamp(value)
        checksum(self.input_sha256)
        if (
            self.window_start_ms % 60000
            or self.open_time_ms % 60000
            or self.close_time_ms != self.open_time_ms + 60000
            or self.window_start_ms > self.open_time_ms
            or self.available_at_ms < self.close_time_ms
            or type(self.observed_rows) is not int
            or not 0 <= self.observed_rows <= 4096
            or type(self.origin) is not str
            or self.origin
            not in {"committed_market", "retrospective", "monte_carlo", "interpolated"}
            or type(self.quality) is not str
            or self.quality not in {"observed", "partial", "missing", "unsupported"}
            or type(self.coverage) is not Decimal
            or not self.coverage.is_finite()
            or not 0 <= self.coverage <= 1
        ):
            raise ValidationError("invalid inference frame range, availability or quality")

    def document(self):
        return {
            "instrument": self.instrument.key,
            "window_start_ms": self.window_start_ms,
            "open_time_ms": self.open_time_ms,
            "close_time_ms": self.close_time_ms,
            "available_at_ms": self.available_at_ms,
            "input_sha256": self.input_sha256,
            "observed_rows": self.observed_rows,
            "origin": self.origin,
            "quality": self.quality,
            "coverage": str(self.coverage),
        }


@dataclass(frozen=True, slots=True)
class InferenceOutputBinding:
    name: str
    path: tuple[str | int, ...]

    def __post_init__(self):
        if (
            type(self.name) is not str
            or re.fullmatch(r"NN\.[A-Za-z][A-Za-z0-9_.]{0,123}", self.name) is None
            or type(self.path) is not tuple
            or not 2 <= len(self.path) <= 8
            or self.path[0] != "values"
            or any(
                not (
                    type(item) is int
                    and 0 <= item < 4096
                    or type(item) is str
                    and re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", item)
                )
                for item in self.path[1:]
            )
        ):
            raise ValidationError("invalid namespaced inference output path")


@dataclass(frozen=True, slots=True)
class InferenceRequest:
    core_epoch: str
    deployment_epoch: str
    stream_epoch: str
    stream: str
    sequence: int
    model: InferenceModelIdentity
    frame: CausalInferenceFrame
    decision_at_ms: int
    issued_at_ms: int
    deadline_ms: int
    previous_state_sha256: str
    output_bindings_sha256: str

    def __post_init__(self):
        for value in (self.core_epoch, self.deployment_epoch, self.stream_epoch, self.stream):
            identity(value)
        if type(self.sequence) is not int or not 1 <= self.sequence <= 2**53:
            raise ValidationError("invalid inference sequence")
        if not isinstance(self.model, InferenceModelIdentity) or not isinstance(
            self.frame, CausalInferenceFrame
        ):
            raise ValidationError("typed inference model and frame required")
        checksum(self.previous_state_sha256)
        checksum(self.output_bindings_sha256)
        for value in (self.decision_at_ms, self.issued_at_ms, self.deadline_ms):
            timestamp(value)
        if (
            not self.decision_at_ms
            <= self.issued_at_ms
            < self.deadline_ms
            <= self.issued_at_ms + 60000
        ):
            raise ValidationError("invalid inference decision/deadline order")

    def document(self):
        return {
            "scope": CAUSAL_SCOPE,
            "core_epoch": self.core_epoch,
            "deployment_epoch": self.deployment_epoch,
            "stream_epoch": self.stream_epoch,
            "stream": self.stream,
            "sequence": self.sequence,
            "model": self.model.document(),
            "frame": self.frame.document(),
            "decision_at_ms": self.decision_at_ms,
            "issued_at_ms": self.issued_at_ms,
            "deadline_ms": self.deadline_ms,
            "previous_state_sha256": self.previous_state_sha256,
            "output_bindings_sha256": self.output_bindings_sha256,
        }

    @property
    def request_id(self):
        return digest(self.document())


@dataclass(frozen=True, slots=True)
class InferenceReceipt:
    scope: str
    request_id: str
    request_sha256: str
    completed_at_ms: int
    status: str
    prediction_json: str

    def __post_init__(self):
        checksum(self.request_id)
        checksum(self.request_sha256)
        timestamp(self.completed_at_ms)
        if (
            self.scope != CAUSAL_SCOPE
            or type(self.status) is not str
            or self.status not in {"complete", "failed", "unknown", "cancelled"}
        ):
            raise ValidationError("causal inference requires an explicit host-wrapped receipt")
        decoded(self.prediction_json, maximum=256 * 1024)

    def document(self):
        return asdict(self)


@dataclass(frozen=True, slots=True)
class InferenceEvidenceReceipt:
    scope: str
    request_sha256: str
    source_commit_sha256: str
    verified_at_ms: int

    def __post_init__(self):
        if self.scope != "committed_inference_evidence":
            raise ValidationError("explicit committed inference evidence required")
        checksum(self.request_sha256)
        checksum(self.source_commit_sha256)
        timestamp(self.verified_at_ms)


@dataclass(frozen=True, slots=True)
class InferenceFeatures:
    request_id: str
    stream: str
    instrument: Instrument
    open_time_ms: int
    input_sha256: str
    frame_sha256: str
    decision_at_ms: int
    values: tuple[tuple[str, Decimal], ...]
    accepted_at_ms: int
    valid_until_ms: int

    def __post_init__(self):
        for value in (self.request_id, self.input_sha256, self.frame_sha256):
            checksum(value)
        identity(self.stream)
        for value in (
            self.open_time_ms,
            self.decision_at_ms,
            self.accepted_at_ms,
            self.valid_until_ms,
        ):
            timestamp(value)
        if (
            not isinstance(self.instrument, Instrument)
            or self.open_time_ms % 60000
            or not self.open_time_ms + 60000
            <= self.decision_at_ms
            <= self.accepted_at_ms
            < self.valid_until_ms
            or type(self.values) is not tuple
            or not 1 <= len(self.values) <= 64
            or any(type(row) is not tuple or len(row) != 2 for row in self.values)
        ):
            raise ValidationError("invalid committed inference features")
        names = set()
        for name, value in self.values:
            InferenceOutputBinding(name, ("values", 0))
            if name in names or type(value) is not Decimal or not value.is_finite():
                raise ValidationError("invalid committed NN feature values")
            names.add(name)


@dataclass(frozen=True, slots=True)
class InferenceTransition:
    request_id: str
    base_revision: int
    revision: int
    snapshot_json: str
    snapshot_sha256: str

    def __post_init__(self):
        checksum(self.request_id)
        checksum(self.snapshot_sha256)
        if (
            type(self.base_revision) is not int
            or self.base_revision < 0
            or type(self.revision) is not int
            or self.revision != self.base_revision + 1
        ):
            raise ValidationError("invalid inference transition revision")
        document = decoded(self.snapshot_json)
        if (
            type(document) is not dict
            or digest(document) != self.snapshot_sha256
            or document.get("revision") != self.revision
        ):
            raise ValidationError("inference transition does not identify its candidate snapshot")


@dataclass(frozen=True, slots=True)
class InferenceCommitAck:
    revision: int
    snapshot_sha256: str
    durable: bool

    def __post_init__(self):
        checksum(self.snapshot_sha256)
        if type(self.revision) is not int or self.revision < 0 or type(self.durable) is not bool:
            raise ValidationError("invalid inference commit acknowledgement")
