"""Pure causal-feature owner with staged state commits and no trading authority.

The host supplies verified market/model evidence and a real durable-store ACK.
This class checks their consistency; a hash or a caller-created ACK does not
prove filesystem durability, training causality or exchange availability.
"""

# Imports
from decimal import Decimal, InvalidOperation
from functools import wraps

from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.domain.inference import (
    CAUSAL_SCOPE,
    CausalInferenceFrame,
    InferenceCommitAck,
    InferenceFeatures,
    InferenceModelIdentity,
    InferenceOutputBinding,
    InferenceReceipt,
    InferenceRequest,
    InferenceTransition,
    checksum,
    decoded,
    digest,
    encoded,
    identity,
    timestamp,
)
from omega.domain.models import Instrument, Market


def _checkpoint_guard(function):
    @wraps(function)
    def checked(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (KeyError, TypeError, AttributeError, InvalidOperation) as error:
            raise ValidationError("malformed inference checkpoint") from error

    return checked


def _instrument(value):
    try:
        provider, market, symbol = value.split(":")
        result = Instrument(provider, Market(market), symbol)
    except (TypeError, ValueError, AttributeError) as error:
        raise ValidationError("invalid inference instrument document") from error
    if result.key != value:
        raise ValidationError("noncanonical inference instrument")
    return result


def _number(value):
    if type(value) not in {int, float}:
        raise ValidationError("NN feature outputs must be native finite numbers")
    try:
        result = Decimal(str(value))
    except InvalidOperation as error:
        raise ValidationError("invalid NN feature number") from error
    if (
        not result.is_finite()
        or len(result.as_tuple().digits) > 128
        or abs(result.adjusted()) > 100
    ):
        raise ValidationError("NN feature output exceeds its finite display/calculation bound")
    return result


def _feature(document):
    expected = {
        "request_id",
        "stream",
        "instrument",
        "open_time_ms",
        "input_sha256",
        "frame_sha256",
        "decision_at_ms",
        "values",
        "accepted_at_ms",
        "valid_until_ms",
    }
    if type(document) is not dict or set(document) != expected:
        raise ValidationError("invalid inference feature document")
    checksum(document["request_id"])
    checksum(document["input_sha256"])
    checksum(document["frame_sha256"])
    identity(document["stream"])
    for key in ("open_time_ms", "decision_at_ms", "accepted_at_ms", "valid_until_ms"):
        timestamp(document[key])
    if document["open_time_ms"] % 60000 or document["accepted_at_ms"] >= document["valid_until_ms"]:
        raise ValidationError("invalid inference feature interval")
    values = []
    if type(document["values"]) is not list or not 1 <= len(document["values"]) <= 64:
        raise ValidationError("invalid inference feature capacity")
    for name, text in document["values"]:
        InferenceOutputBinding(name, ("values", 0))
        if type(text) is not str or len(text) > 256:
            raise ValidationError("inference feature checkpoint requires bounded Decimal strings")
        value = Decimal(text)
        if (
            not value.is_finite()
            or len(value.as_tuple().digits) > 128
            or abs(value.adjusted()) > 100
        ):
            raise ValidationError("invalid checkpoint feature Decimal")
        values.append((name, value))
    if len({name for name, _ in values}) != len(values):
        raise ValidationError("duplicate inference checkpoint feature")
    return InferenceFeatures(
        document["request_id"],
        document["stream"],
        _instrument(document["instrument"]),
        document["open_time_ms"],
        document["input_sha256"],
        document["frame_sha256"],
        document["decision_at_ms"],
        tuple(values),
        document["accepted_at_ms"],
        document["valid_until_ms"],
    )


class CausalInferenceGate:
    def __init__(
        self,
        model,
        bindings,
        *,
        core_epoch,
        deployment_epoch,
        max_frame_age_ms=120000,
        max_streams=32,
    ):
        if not isinstance(model, InferenceModelIdentity):
            raise ValidationError("typed inference model identity required")
        identity(core_epoch)
        identity(deployment_epoch)
        if (
            type(bindings) is not tuple
            or not 1 <= len(bindings) <= 64
            or any(not isinstance(row, InferenceOutputBinding) for row in bindings)
            or len({row.name for row in bindings}) != len(bindings)
            or type(max_frame_age_ms) is not int
            or not 1 <= max_frame_age_ms <= 86400000
            or type(max_streams) is not int
            or not 1 <= max_streams <= 128
        ):
            raise ValidationError("invalid bounded causal inference gate configuration")
        self.model, self.bindings = model, bindings
        self.core_epoch, self.deployment_epoch = core_epoch, deployment_epoch
        self.max_frame_age_ms, self.max_streams = max_frame_age_ms, max_streams
        self._document = {
            "schema_version": 1,
            "scope": CAUSAL_SCOPE,
            "model": model.document(),
            "bindings": [{"name": row.name, "path": list(row.path)} for row in bindings],
            "core_epoch": core_epoch,
            "deployment_epoch": deployment_epoch,
            "max_frame_age_ms": max_frame_age_ms,
            "max_streams": max_streams,
            "revision": 0,
            "recording_unconfirmed": False,
            "streams": {},
            "pending": {},
        }
        self._staged = None

    @property
    def revision(self):
        return self._document["revision"]

    @property
    def output_bindings_sha256(self):
        return digest(self._document["bindings"])

    def snapshot(self):
        return encoded(self._document)

    def _mutable(self):
        if self._document["recording_unconfirmed"]:
            raise TradingBlocked(
                "inference recording is unconfirmed; read actual state before recovery"
            )
        if self._staged is not None:
            raise TradingBlocked("an inference transition awaits its exact durable acknowledgement")

    def _replace(self, candidate):
        encoded(candidate)
        self._document = candidate

    def open_stream(
        self, stream, instrument, *, stream_epoch, initial_state_json="null", after_open_ms=None
    ):
        self._mutable()
        identity(stream)
        identity(stream_epoch)
        if not isinstance(instrument, Instrument):
            raise ValidationError("typed inference stream instrument required")
        previous = self._document["streams"].get(stream)
        if previous is not None and previous["epoch"] == stream_epoch:
            raise ValidationError("stream recovery must use a new explicit stream epoch")
        if previous is None and len(self._document["streams"]) >= self.max_streams:
            raise ValidationError("inference stream capacity reached")
        state = decoded(initial_state_json, maximum=65536)
        if not self.model.stateful and state is not None:
            raise ValidationError("stateless inference cannot import recurrent state")
        if after_open_ms is not None:
            timestamp(after_open_ms)
            if after_open_ms % 60000:
                raise ValidationError("stream recovery cursor must identify a minute")
        candidate = decoded(self.snapshot())
        candidate["pending"] = {
            key: row for key, row in candidate["pending"].items() if row["stream"] != stream
        }
        candidate["streams"][stream] = {
            "epoch": stream_epoch,
            "instrument": instrument.key,
            "sequence": 0,
            "last_open_ms": after_open_ms,
            "state_json": encoded(state, maximum=65536),
            "state_sha256": digest(state),
            "health": "ready",
            "reason": "explicit_stream_reset",
            "feature": None,
            "receipt_sha256": None,
            "last_request_id": None,
            "accepted_at_ms": None,
        }
        candidate["revision"] += 1
        self._replace(candidate)

    def previous_state_json(self, stream):
        return self._document["streams"][stream]["state_json"]

    def _policy(self, frame, *, decision_at_ms, now_ms):
        if not isinstance(frame, CausalInferenceFrame):
            raise ValidationError("typed causal inference frame required")
        for value in (decision_at_ms, now_ms):
            timestamp(value)
        if (
            frame.origin != "committed_market"
            or frame.quality != "observed"
            or frame.coverage != 1
            or frame.observed_rows != self.model.raw_lookback
            or frame.close_time_ms - frame.window_start_ms != self.model.raw_lookback * 60000
            or not frame.available_at_ms <= decision_at_ms <= now_ms
            or now_ms - frame.close_time_ms > self.max_frame_age_ms
            or self.model.training_cutoff_ms > frame.open_time_ms
            or self.model.available_at_ms > decision_at_ms
        ):
            raise TradingBlocked(
                "inference input/model is not causally available, complete or fresh"
            )

    def request(self, stream, frame, *, decision_at_ms, now_ms, deadline_ms):
        self._mutable()
        self._policy(frame, decision_at_ms=decision_at_ms, now_ms=now_ms)
        row = self._document["streams"].get(stream)
        if row is None or row["health"] != "ready" or row["instrument"] != frame.instrument.key:
            raise TradingBlocked("inference stream needs explicit preparation/recovery")
        for document in self._document["pending"].values():
            if document["stream"] == stream:
                if (
                    document["frame"] != frame.document()
                    or document["decision_at_ms"] != decision_at_ms
                    or document["deadline_ms"] != deadline_ms
                ):
                    raise TradingBlocked(
                        "inference stream already owns a different outstanding frame"
                    )
                if now_ms >= document["deadline_ms"]:
                    self.fail(digest(document), "inference_deadline_exceeded")
                    raise TradingBlocked("inference request expired")
                return self._request_from_document(document)
        if row["last_open_ms"] is not None and frame.open_time_ms != row["last_open_ms"] + 60000:
            raise TradingBlocked(
                "inference frames must advance by exactly one minute; use the committed cache"
            )
        request = InferenceRequest(
            self.core_epoch,
            self.deployment_epoch,
            row["epoch"],
            stream,
            row["sequence"] + 1,
            self.model,
            frame,
            decision_at_ms,
            now_ms,
            deadline_ms,
            row["state_sha256"],
            self.output_bindings_sha256,
        )
        candidate = decoded(self.snapshot())
        candidate["pending"][request.request_id] = request.document()
        candidate["revision"] += 1
        self._replace(candidate)
        return request

    @staticmethod
    def _request_from_document(document):
        try:
            value = dict(document)
            if value.pop("scope") != CAUSAL_SCOPE:
                raise ValidationError("offline inference request cannot be restored as causal")
            value["model"] = InferenceModelIdentity(**value["model"])
            frame = dict(value["frame"])
            frame["instrument"] = _instrument(frame["instrument"])
            frame["coverage"] = Decimal(frame["coverage"])
            value["frame"] = CausalInferenceFrame(**frame)
            return InferenceRequest(**value)
        except (TypeError, KeyError, ValueError) as error:
            raise ValidationError("invalid inference request checkpoint") from error

    def fail(self, request_id, reason="inference_failed"):
        self._mutable()
        identity(reason)
        request = self._document["pending"].get(request_id)
        if request is None:
            raise ValidationError("unknown inference request; no state was advanced")
        candidate = decoded(self.snapshot())
        candidate["pending"].pop(request_id)
        row = candidate["streams"][request["stream"]]
        row.update(health="blocked", reason=reason, feature=None)
        candidate["revision"] += 1
        self._replace(candidate)

    def expire(self, *, now_ms):
        self._mutable()
        timestamp(now_ms)
        expired = tuple(
            key for key, row in self._document["pending"].items() if now_ms >= row["deadline_ms"]
        )
        for key in expired:
            self.fail(key, "inference_deadline_exceeded")
        return expired

    def _outputs(self, payload):
        if (
            type(payload) is not dict
            or type(payload.get("values")) is not list
            or payload.keys() - {"values", "state"}
        ):
            raise ValidationError("inference receipt needs the module prediction.values list")
        state = payload.get("state")
        if self.model.stateful and state is None or not self.model.stateful and state is not None:
            raise ValidationError("prediction recurrent state does not match its frozen model")
        encoded(state, maximum=65536)
        values = []
        no_signal = False
        for binding in self.bindings:
            value = payload
            for component in binding.path:
                if type(component) is int and type(value) is list and component < len(value):
                    value = value[component]
                elif type(component) is str and type(value) is dict and component in value:
                    value = value[component]
                else:
                    raise ValidationError("prediction output mapping is unavailable")
            if value is None and self.model.allow_no_signal:
                no_signal = True
            else:
                values.append([binding.name, str(_number(value))])
        return ([] if no_signal else values), state, no_signal

    def stage(self, receipt, *, now_ms):
        self._mutable()
        timestamp(now_ms)
        if not isinstance(receipt, InferenceReceipt):
            raise ValidationError("typed host-verified causal receipt required")
        document = self._document["pending"].get(receipt.request_id)
        if document is None:
            raise ProtocolError(
                "late, duplicate or unknown inference receipt; no state was advanced"
            )
        request = self._request_from_document(document)
        if receipt.request_sha256 != request.request_id:
            self.fail(receipt.request_id, "inference_request_mismatch")
            raise ProtocolError("inference receipt differs from its bound request")
        if (
            not request.issued_at_ms <= receipt.completed_at_ms <= now_ms < request.deadline_ms
            or receipt.status != "complete"
        ):
            self.fail(receipt.request_id, "inference_failed_or_late")
            raise TradingBlocked("inference receipt failed or missed its deadline")
        try:
            self._policy(request.frame, decision_at_ms=request.decision_at_ms, now_ms=now_ms)
        except TradingBlocked:
            self.fail(receipt.request_id, "inference_input_stale")
            raise
        row = self._document["streams"][request.stream]
        if (
            request.core_epoch != self.core_epoch
            or request.deployment_epoch != self.deployment_epoch
            or request.stream_epoch != row["epoch"]
            or request.model != self.model
            or request.previous_state_sha256 != row["state_sha256"]
            or request.sequence != row["sequence"] + 1
            or request.output_bindings_sha256 != self.output_bindings_sha256
        ):
            self.fail(receipt.request_id, "inference_state_mismatch")
            raise ProtocolError("inference epoch, model or recurrent cursor changed")
        try:
            values, state, no_signal = self._outputs(
                decoded(receipt.prediction_json, maximum=256 * 1024)
            )
        except ValidationError:
            self.fail(receipt.request_id, "inference_output_invalid")
            raise
        candidate = decoded(self.snapshot())
        candidate["pending"].pop(receipt.request_id)
        feature = {
            "request_id": receipt.request_id,
            "stream": request.stream,
            "instrument": request.frame.instrument.key,
            "open_time_ms": request.frame.open_time_ms,
            "input_sha256": request.frame.input_sha256,
            "values": values,
            "frame_sha256": digest(request.frame.document()),
            "decision_at_ms": request.decision_at_ms,
            "accepted_at_ms": now_ms,
            "valid_until_ms": request.deadline_ms,
        }
        candidate["streams"][request.stream].update(
            sequence=request.sequence,
            last_open_ms=request.frame.open_time_ms,
            state_json=encoded(state, maximum=65536),
            state_sha256=digest(state),
            health="ready",
            reason="no_signal" if no_signal else "prediction_accepted",
            feature=None if no_signal else feature,
            receipt_sha256=digest(receipt.document()),
            last_request_id=request.request_id,
            accepted_at_ms=now_ms,
        )
        candidate["revision"] += 1
        snapshot = encoded(candidate)
        transition = InferenceTransition(
            receipt.request_id, self.revision, candidate["revision"], snapshot, digest(candidate)
        )
        self._staged = transition
        return transition

    def commit(self, transition, acknowledgement, *, now_ms):
        timestamp(now_ms)
        if self._staged is None or transition != self._staged:
            raise ProtocolError("inference transition is unknown, stale or already committed")
        if not isinstance(acknowledgement, InferenceCommitAck) or (
            acknowledgement.revision != transition.revision
            or acknowledgement.snapshot_sha256 != transition.snapshot_sha256
            or transition.base_revision != self.revision
        ):
            raise ProtocolError("inference acknowledgement does not identify its staged snapshot")
        if not acknowledgement.durable:
            self.recording_uncertain()
            raise StorageError("inference write is unconfirmed; no feature/state is published")
        candidate = decoded(transition.snapshot_json)
        if digest(candidate) != transition.snapshot_sha256:
            raise ProtocolError("staged inference snapshot is corrupt")
        stream = next(
            row
            for row in candidate["streams"].values()
            if row["last_request_id"] == transition.request_id
        )
        feature = stream["feature"]
        if now_ms < stream["accepted_at_ms"]:
            raise ValidationError("actual inference commit clock moved backwards")
        self._replace(candidate)
        self._staged = None
        return (
            _feature(feature)
            if feature is not None and now_ms < feature["valid_until_ms"]
            else None
        )

    def recording_uncertain(self):
        """Block after an unknown intent/state write without overwriting the actual store."""
        self._staged = None
        self._document["recording_unconfirmed"] = True

    def lookup(self, stream, frame, *, now_ms):
        timestamp(now_ms)
        if not isinstance(frame, CausalInferenceFrame) or self._document["recording_unconfirmed"]:
            return None
        row = self._document["streams"].get(stream)
        if row is None or row["health"] != "ready" or row["feature"] is None:
            return None
        feature = _feature(row["feature"])
        if (
            feature.open_time_ms != frame.open_time_ms
            or feature.instrument != frame.instrument
            or feature.input_sha256 != frame.input_sha256
            or feature.frame_sha256 != digest(frame.document())
            or not feature.accepted_at_ms <= now_ms < feature.valid_until_ms
        ):
            return None
        try:
            self._policy(frame, decision_at_ms=feature.decision_at_ms, now_ms=now_ms)
        except TradingBlocked:
            return None
        return feature

    @classmethod
    @_checkpoint_guard
    def restore(cls, snapshot_json, *, core_epoch, deployment_epoch, expected_model):
        """Restore verified state as blocked; an explicit new stream epoch is mandatory.

        A saved staged candidate may contain the actual new RNN state after an
        ACK loss. Read it rather than replaying the old request or guessing which
        state won. No restored feature is eligible for new risk automatically.
        """
        document = decoded(snapshot_json)
        expected = {
            "schema_version",
            "scope",
            "model",
            "bindings",
            "core_epoch",
            "deployment_epoch",
            "max_frame_age_ms",
            "max_streams",
            "revision",
            "recording_unconfirmed",
            "streams",
            "pending",
        }
        if (
            type(document) is not dict
            or set(document) != expected
            or type(document["schema_version"]) is not int
            or document["schema_version"] != 1
            or document["scope"] != CAUSAL_SCOPE
        ):
            raise ValidationError("unsupported inference owner checkpoint")
        if type(document["bindings"]) is not list:
            raise ValidationError("invalid inference checkpoint bindings")
        if any(
            type(row) is not dict or set(row) != {"name", "path"} or type(row["path"]) is not list
            for row in document["bindings"]
        ):
            raise ValidationError("invalid inference checkpoint output mapping")
        bindings = tuple(
            InferenceOutputBinding(row["name"], tuple(row["path"])) for row in document["bindings"]
        )
        model = InferenceModelIdentity(**document["model"])
        if model != expected_model:
            raise ProtocolError("inference checkpoint belongs to another frozen model")
        gate = cls(
            model,
            bindings,
            core_epoch=core_epoch,
            deployment_epoch=deployment_epoch,
            max_frame_age_ms=document["max_frame_age_ms"],
            max_streams=document["max_streams"],
        )
        identity(document["core_epoch"])
        identity(document["deployment_epoch"])
        if (
            type(document["revision"]) is not int
            or document["revision"] < 0
            or type(document["recording_unconfirmed"]) is not bool
            or type(document["streams"]) is not dict
            or len(document["streams"]) > gate.max_streams
            or type(document["pending"]) is not dict
            or len(document["pending"]) > gate.max_streams
        ):
            raise ValidationError("invalid bounded inference checkpoint")
        for key, row in document["streams"].items():
            identity(key)
            if type(row) is not dict or set(row) != {
                "epoch",
                "instrument",
                "sequence",
                "last_open_ms",
                "state_json",
                "state_sha256",
                "health",
                "reason",
                "feature",
                "receipt_sha256",
                "last_request_id",
                "accepted_at_ms",
            }:
                raise ValidationError("invalid inference stream checkpoint")
            identity(row["epoch"])
            _instrument(row["instrument"])
            identity(row["reason"])
            state = decoded(row["state_json"], maximum=65536)
            if row["state_sha256"] != digest(state) or not model.stateful and state is not None:
                raise ProtocolError("inference recurrent state checksum/schema mismatch")
            if (
                type(row["sequence"]) is not int
                or not 0 <= row["sequence"] < 2**53
                or row["health"] not in {"ready", "blocked"}
            ):
                raise ValidationError("invalid inference stream sequence/health")
            if row["last_open_ms"] is not None:
                timestamp(row["last_open_ms"])
                if row["last_open_ms"] % 60000:
                    raise ValidationError("invalid saved inference cursor")
            if row["accepted_at_ms"] is not None:
                timestamp(row["accepted_at_ms"])
                if (
                    row["last_open_ms"] is None
                    or row["accepted_at_ms"] < row["last_open_ms"] + 60000
                ):
                    raise ValidationError("invalid saved inference acceptance time")
            if row["feature"] is not None:
                feature = _feature(row["feature"])
                if (
                    feature.stream != key
                    or feature.instrument.key != row["instrument"]
                    or feature.open_time_ms != row["last_open_ms"]
                    or row["sequence"] == 0
                ):
                    raise ProtocolError("inference feature differs from its committed stream")
                if tuple(name for name, _ in feature.values) != tuple(
                    binding.name for binding in bindings
                ):
                    raise ProtocolError("inference feature output schema changed")
            if row["receipt_sha256"] is not None:
                checksum(row["receipt_sha256"])
            if row["last_request_id"] is not None:
                checksum(row["last_request_id"])
            if row["sequence"] > 0 and (
                row["last_open_ms"] is None
                or row["receipt_sha256"] is None
                or row["last_request_id"] is None
                or row["accepted_at_ms"] is None
                or model.stateful
                and state is None
            ):
                raise ValidationError("committed inference stream lacks its receipt/cursor/state")
            if (
                row["feature"] is not None
                and row["feature"]["request_id"] != row["last_request_id"]
            ):
                raise ProtocolError("feature differs from its committed request identity")
        busy = set()
        for request_id, value in document["pending"].items():
            request = gate._request_from_document(value)
            row = document["streams"].get(request.stream)
            if (
                request_id != request.request_id
                or row is None
                or request.model != model
                or request.core_epoch != document["core_epoch"]
                or request.deployment_epoch != document["deployment_epoch"]
                or request.stream_epoch != row["epoch"]
                or request.sequence != row["sequence"] + 1
                or request.previous_state_sha256 != row["state_sha256"]
                or request.output_bindings_sha256 != gate.output_bindings_sha256
                or request.frame.instrument.key != row["instrument"]
                or request.stream in busy
            ):
                raise ProtocolError("inference outstanding request checkpoint mismatch")
            gate._policy(
                request.frame, decision_at_ms=request.decision_at_ms, now_ms=request.issued_at_ms
            )
            busy.add(request.stream)
        document.update(
            core_epoch=core_epoch,
            deployment_epoch=deployment_epoch,
            pending={},
            recording_unconfirmed=False,
        )
        for row in document["streams"].values():
            row.update(health="blocked", reason="restart_manual_reset_required", feature=None)
        document["revision"] += 1
        gate._replace(document)
        return gate
