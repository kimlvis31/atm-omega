"""Immutable external-feature proofs; parsing is never present-day authority.

These values contain no provider, process, callback or file handle. A runtime
authority must verify its real cached epochs, lifetime and pin membership before
new risk. Restored proofs remain useful for audit/protection when that authority
is unavailable. Semantic candle identity does not replace the full input proof.
"""

# Imports
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import wraps

from omega.domain.errors import ValidationError
from omega.domain.inference import checksum, digest, encoded, identity, timestamp
from omega.domain.models import Candle, Instrument, Market


def _parse(function):
    @wraps(function)
    def checked(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (KeyError, TypeError, ValueError, AttributeError, InvalidOperation) as error:
            raise ValidationError("malformed external feature proof") from error

    return checked


def _fields(value):
    if type(value) is not tuple or not 1 <= len(value) <= 64:
        raise ValidationError("external feature fields must be a bounded unique tuple")
    for name in value:
        if (
            type(name) is not str
            or len(name) > 128
            or re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,31}\.[A-Za-z][A-Za-z0-9_.]{0,123}", name)
            is None
        ):
            raise ValidationError("external fields require an explicit namespace")
    if len(set(value)) != len(value):
        raise ValidationError("duplicate external feature field")


def _instrument(value):
    provider, market, symbol = value.split(":")
    result = Instrument(provider, Market(market), symbol)
    if result.key != value:
        raise ValidationError("external instrument must be canonical")
    return result


def _number(value):
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValidationError("external feature values require finite Decimal numbers")
    if len(value.as_tuple().digits) > 128 or abs(value.adjusted()) > 100:
        raise ValidationError("external feature value exceeds its exact numeric capacity")


def exact_decimal_text(value):
    """Canonical exact coefficient/exponent; no context.normalize or rounding.

    This also avoids expanding a hostile exponent into a huge fixed-point string.
    Decimal('100.00') and Decimal('1E2') both become '1e2'; every zero is '0'.
    """
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValidationError("semantic values require finite Decimals")
    sign, digits, exponent = value.as_tuple()
    if len(digits) > 4096 or type(exponent) is not int:
        raise ValidationError("semantic decimal exceeds bounded coefficient capacity")
    if value == 0:
        return "0"
    digits = list(digits)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    return ("-" if sign else "") + "".join(str(v) for v in digits) + "e" + str(exponent)


def semantic_candle_sha256(candle):
    if not isinstance(candle, Candle):
        raise ValidationError("semantic candle identity requires an actual typed Candle")
    timestamp(candle.open_time_ms)
    timestamp(candle.close_time_ms)
    if not isinstance(candle.instrument, Instrument):
        raise ValidationError("semantic candle requires a typed instrument")
    return digest(
        {
            "scope": "omega.semantic_closed_candle.v1",
            "instrument": candle.instrument.key,
            "open_time_ms": candle.open_time_ms,
            "close_time_ms": candle.close_time_ms,
            "values": [
                exact_decimal_text(getattr(candle, name))
                for name in ("open", "high", "low", "close", "volume")
            ],
        }
    )


def _document(scope, values):
    return {"schema_version": 1, "scope": scope, **values}


def _body(document, scope, keys, identity_key):
    if (
        type(document) is not dict
        or set(document) != {"schema_version", "scope", identity_key} | set(keys)
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or document["scope"] != scope
    ):
        raise ValidationError("invalid external feature document schema")
    encoded(document, maximum=262144)
    body = {key: document[key] for key in keys}
    if document[identity_key] != digest(_document(scope, body)):
        raise ValidationError("external feature proof checksum mismatch")
    return body


@dataclass(frozen=True, slots=True)
class ExternalFeatureBinding:
    provider_id: str
    instance_id: str
    stream: str
    instrument: Instrument
    producer_sha256: str
    contract_sha256: str
    fields: tuple[str, ...]

    def __post_init__(self):
        for value in (self.provider_id, self.instance_id, self.stream):
            identity(value)
        if not isinstance(self.instrument, Instrument):
            raise ValidationError("typed external binding instrument required")
        checksum(self.producer_sha256)
        checksum(self.contract_sha256)
        _fields(self.fields)

    @property
    def fields_sha256(self):
        return digest(list(self.fields))

    def _body(self):
        return {
            "provider_id": self.provider_id,
            "instance_id": self.instance_id,
            "stream": self.stream,
            "instrument": self.instrument.key,
            "producer_sha256": self.producer_sha256,
            "contract_sha256": self.contract_sha256,
            "fields": list(self.fields),
            "fields_sha256": self.fields_sha256,
        }

    @property
    def binding_sha256(self):
        return digest(_document("external_feature_binding", self._body()))

    def document(self):
        return _document("external_feature_binding", self._body()) | {
            "binding_sha256": self.binding_sha256
        }

    @classmethod
    @_parse
    def parse(cls, document):
        body = _body(
            document,
            "external_feature_binding",
            (
                "provider_id",
                "instance_id",
                "stream",
                "instrument",
                "producer_sha256",
                "contract_sha256",
                "fields",
                "fields_sha256",
            ),
            "binding_sha256",
        )
        if type(body["fields"]) is not list:
            raise ValidationError("serialized external fields require a list")
        fields_sha256 = body.pop("fields_sha256")
        body["fields"] = tuple(body["fields"])
        body["instrument"] = _instrument(body["instrument"])
        result = cls(**body)
        if result.fields_sha256 != fields_sha256:
            raise ValidationError("external field selection checksum mismatch")
        return result


@dataclass(frozen=True, slots=True)
class ExternalFeatureSnapshot:
    binding: ExternalFeatureBinding
    core_epoch: str
    deployment_epoch: str
    stream_epoch: str
    request_id: str
    input_sha256: str
    input_frame_sha256: str
    receipt_sha256: str
    evidence_sha256: str
    candle_sha256: str
    open_time_ms: int
    decision_at_ms: int
    accepted_at_ms: int
    valid_until_ms: int
    disposition: str
    values: tuple[tuple[str, Decimal], ...]

    def __post_init__(self):
        if not isinstance(self.binding, ExternalFeatureBinding):
            raise ValidationError("typed external snapshot binding required")
        for value in (self.core_epoch, self.deployment_epoch, self.stream_epoch):
            identity(value)
        for value in (
            self.request_id,
            self.input_sha256,
            self.input_frame_sha256,
            self.receipt_sha256,
            self.evidence_sha256,
            self.candle_sha256,
        ):
            checksum(value)
        for value in (
            self.open_time_ms,
            self.decision_at_ms,
            self.accepted_at_ms,
            self.valid_until_ms,
        ):
            timestamp(value)
        if (
            self.open_time_ms % 60000
            or not self.open_time_ms + 60000
            <= self.decision_at_ms
            <= self.accepted_at_ms
            < self.valid_until_ms
            or type(self.values) is not tuple
        ):
            raise ValidationError("invalid external snapshot availability or value container")
        if self.disposition == "no_signal":
            if self.values:
                raise ValidationError("NoSignal cannot carry usable external values")
        elif self.disposition == "ready":
            if (
                any(type(row) is not tuple or len(row) != 2 for row in self.values)
                or tuple(row[0] for row in self.values) != self.binding.fields
            ):
                raise ValidationError(
                    "external snapshot must contain exact declared fields in order"
                )
            for _, value in self.values:
                _number(value)
        else:
            raise ValidationError("only committed ready/NoSignal snapshots exist")

    def _body(self):
        return {
            "binding": self.binding.document(),
            "core_epoch": self.core_epoch,
            "deployment_epoch": self.deployment_epoch,
            "stream_epoch": self.stream_epoch,
            "request_id": self.request_id,
            "input_sha256": self.input_sha256,
            "input_frame_sha256": self.input_frame_sha256,
            "receipt_sha256": self.receipt_sha256,
            "evidence_sha256": self.evidence_sha256,
            "candle_sha256": self.candle_sha256,
            "open_time_ms": self.open_time_ms,
            "decision_at_ms": self.decision_at_ms,
            "accepted_at_ms": self.accepted_at_ms,
            "valid_until_ms": self.valid_until_ms,
            "disposition": self.disposition,
            "values": [[name, exact_decimal_text(value)] for name, value in self.values],
        }

    @property
    def snapshot_sha256(self):
        return digest(_document("external_feature_snapshot", self._body()))

    def document(self):
        return _document("external_feature_snapshot", self._body()) | {
            "snapshot_sha256": self.snapshot_sha256
        }

    def matches_candle(self, candle):
        return (
            isinstance(candle, Candle)
            and candle.instrument == self.binding.instrument
            and candle.open_time_ms == self.open_time_ms
            and semantic_candle_sha256(candle) == self.candle_sha256
        )

    @classmethod
    @_parse
    def parse(cls, document):
        keys = tuple(cls.__dataclass_fields__)
        body = _body(document, "external_feature_snapshot", keys, "snapshot_sha256")
        body["binding"] = ExternalFeatureBinding.parse(body["binding"])
        if type(body["values"]) is not list or any(
            type(row) is not list or len(row) != 2 or type(row[1]) is not str or len(row[1]) > 256
            for row in body["values"]
        ):
            raise ValidationError("invalid serialized exact external values")
        body["values"] = tuple((name, Decimal(value)) for name, value in body["values"])
        result = cls(**body)
        if result.snapshot_sha256 != document["snapshot_sha256"]:
            raise ValidationError("external snapshot is not canonical")
        return result


@dataclass(frozen=True, slots=True)
class ExternalFeatureDelivery:
    binding: ExternalFeatureBinding
    disposition: str
    snapshot: ExternalFeatureSnapshot | None
    core_epoch: str
    deployment_epoch: str
    stream_epoch: str
    candle_sha256: str
    open_time_ms: int
    decision_at_ms: int
    observed_at_ms: int
    reason: str

    def __post_init__(self):
        if not isinstance(self.binding, ExternalFeatureBinding):
            raise ValidationError("typed external delivery binding required")
        for value in (self.core_epoch, self.deployment_epoch, self.stream_epoch, self.reason):
            identity(value)
        checksum(self.candle_sha256)
        for value in (self.open_time_ms, self.decision_at_ms, self.observed_at_ms):
            timestamp(value)
        if (
            self.open_time_ms % 60000
            or self.open_time_ms + 60000 > self.decision_at_ms
            or self.decision_at_ms > self.observed_at_ms
        ):
            raise ValidationError("invalid external delivery decision cutoff")
        if type(self.disposition) is not str or self.disposition not in {
            "ready",
            "pending",
            "no_signal",
            "expired",
            "unavailable",
        }:
            raise ValidationError("unsupported external delivery disposition")
        value = self.snapshot
        if value is not None and (
            not isinstance(value, ExternalFeatureSnapshot)
            or value.binding != self.binding
            or value.candle_sha256 != self.candle_sha256
            or value.open_time_ms != self.open_time_ms
        ):
            raise ValidationError("external delivery snapshot differs from its binding/candle")
        if self.disposition in {"ready", "no_signal"}:
            if (
                value is None
                or value.disposition != self.disposition
                or (value.core_epoch, value.deployment_epoch, value.stream_epoch)
                != (self.core_epoch, self.deployment_epoch, self.stream_epoch)
                or not value.accepted_at_ms
                <= self.decision_at_ms
                <= self.observed_at_ms
                < value.valid_until_ms
            ):
                raise ValidationError("external delivery is not available at its actual cutoff")
        elif self.disposition == "expired" and value is None:
            raise ValidationError("expired external delivery must identify its immutable snapshot")
        elif self.disposition == "expired" and self.observed_at_ms < value.valid_until_ms:
            raise ValidationError("external snapshot has not actually expired")
        elif self.disposition in {"pending", "unavailable"} and value is not None:
            raise ValidationError("unavailable external delivery cannot disclose future values")

    def _body(self):
        return {
            "binding": self.binding.document(),
            "disposition": self.disposition,
            "snapshot": None if self.snapshot is None else self.snapshot.document(),
            "core_epoch": self.core_epoch,
            "deployment_epoch": self.deployment_epoch,
            "stream_epoch": self.stream_epoch,
            "candle_sha256": self.candle_sha256,
            "open_time_ms": self.open_time_ms,
            "decision_at_ms": self.decision_at_ms,
            "observed_at_ms": self.observed_at_ms,
            "reason": self.reason,
        }

    @property
    def delivery_sha256(self):
        return digest(_document("external_feature_delivery", self._body()))

    def document(self):
        return _document("external_feature_delivery", self._body()) | {
            "delivery_sha256": self.delivery_sha256
        }

    def matches_candle(self, candle):
        return (
            isinstance(candle, Candle)
            and candle.instrument == self.binding.instrument
            and candle.open_time_ms == self.open_time_ms
            and semantic_candle_sha256(candle) == self.candle_sha256
        )

    @classmethod
    @_parse
    def parse(cls, document):
        body = _body(
            document,
            "external_feature_delivery",
            tuple(cls.__dataclass_fields__),
            "delivery_sha256",
        )
        body["binding"] = ExternalFeatureBinding.parse(body["binding"])
        if body["snapshot"] is not None:
            body["snapshot"] = ExternalFeatureSnapshot.parse(body["snapshot"])
        return cls(**body)


@dataclass(frozen=True, slots=True)
class ExternalFeaturePin:
    provider_id: str
    consumer_id: str
    frame_ref: str
    binding_sha256: str
    snapshot_sha256: str
    created_at_ms: int
    durable: bool = True

    def __post_init__(self):
        identity(self.provider_id)
        identity(self.consumer_id)
        for value in (self.frame_ref, self.binding_sha256, self.snapshot_sha256):
            checksum(value)
        timestamp(self.created_at_ms)
        if self.durable is not True:
            raise ValidationError("an external pin proof requires a native durable ACK")

    @property
    def pin_id(self):
        return digest(
            {
                "provider_id": self.provider_id,
                "consumer_id": self.consumer_id,
                "frame_ref": self.frame_ref,
                "binding_sha256": self.binding_sha256,
                "snapshot_sha256": self.snapshot_sha256,
            }
        )

    def _body(self):
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    def document(self):
        body = _document("external_feature_pin", self._body()) | {"pin_id": self.pin_id}
        return body | {"proof_sha256": digest(body)}

    @classmethod
    @_parse
    def parse(cls, document):
        body = _body(
            document,
            "external_feature_pin",
            (*tuple(cls.__dataclass_fields__), "pin_id"),
            "proof_sha256",
        )
        pin_id = body.pop("pin_id")
        result = cls(**body)
        if result.pin_id != pin_id:
            raise ValidationError("external pin identity mismatch")
        return result
