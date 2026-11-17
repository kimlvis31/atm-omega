"""Canonical typed JSON for derived auxiliary observations, never raw exchange bytes."""

import json
import re
from decimal import Decimal, InvalidOperation
from hashlib import sha256

from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market

MAX_PAYLOAD_BYTES = 256 * 1024
SCHEMA_VERSION = 1
_OBSERVATION_KEYS = {
    "channel",
    "values",
    "window_start_ms",
    "window_end_ms",
    "event_time_ms",
    "available_at_ms",
    "quality",
    "coverage",
    "source",
}
_DECIMAL_TEXT = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


def _bytes(payload):
    if type(payload) is not bytes or not 0 < len(payload) <= MAX_PAYLOAD_BYTES:
        raise ValidationError("auxiliary encoding requires 1..262144 encoded bytes")


def observation_checksum(payload: bytes) -> str:
    """Hash encoded bytes without pretending that the hash validates their schema."""
    _bytes(payload)
    return sha256(payload).hexdigest()


def _keys(value, expected):
    if type(value) is not dict or value.keys() != expected:
        raise ValidationError("invalid auxiliary encoding fields")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError("duplicate JSON field in auxiliary encoding")
        result[key] = value
    return result


def _invalid_constant(_text):
    raise ValidationError("non-finite JSON constant in auxiliary encoding")


def _decimal(value):
    if type(value) is not str or _DECIMAL_TEXT.fullmatch(value) is None:
        raise ValidationError("auxiliary Decimal requires an exact finite number string")
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise ValidationError("invalid auxiliary Decimal encoding") from None
    if not number.is_finite():
        raise ValidationError("non-finite auxiliary Decimal encoding")
    return number


def encode_observation(instrument: Instrument, observation: AuxiliaryInput) -> bytes:
    if not isinstance(instrument, Instrument) or not isinstance(observation, AuxiliaryInput):
        raise ValidationError("typed auxiliary instrument and observation required")
    instrument = Instrument(instrument.provider, instrument.market, instrument.symbol)
    observation = AuxiliaryInput(**{key: getattr(observation, key) for key in _OBSERVATION_KEYS})
    values = [
        {
            "name": name,
            "type": "int" if type(value) is int else "decimal",
            "value": value if type(value) is int else str(value),
        }
        for name, value in observation.values
    ]
    document = {
        "schema_version": SCHEMA_VERSION,
        "instrument": {
            "provider": instrument.provider,
            "market": instrument.market.value,
            "symbol": instrument.symbol,
        },
        "observation": {
            key: getattr(observation, key) for key in _OBSERVATION_KEYS - {"values", "coverage"}
        },
    }
    document["observation"].update(values=values, coverage=str(observation.coverage))
    try:
        encoded = json.dumps(
            document, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ValidationError("auxiliary observation cannot be encoded") from None
    _bytes(encoded)
    # Validate the complete representation even if an object was constructed unsafely.
    decode_observation(encoded)
    return encoded


def decode_observation(payload: bytes) -> tuple[Instrument, AuxiliaryInput]:
    _bytes(payload)
    try:
        document = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_invalid_constant,
        )
        _keys(document, {"schema_version", "instrument", "observation"})
        if (
            type(document["schema_version"]) is not int
            or document["schema_version"] != SCHEMA_VERSION
        ):
            raise ValidationError("unsupported auxiliary encoding schema")
        identity = document["instrument"]
        _keys(identity, {"provider", "market", "symbol"})
        instrument = Instrument(
            identity["provider"], Market(identity["market"]), identity["symbol"]
        )
        row = document["observation"]
        _keys(row, _OBSERVATION_KEYS)
        if type(row["values"]) is not list or len(row["values"]) > 64:
            raise ValidationError("auxiliary encoded fields exceed their limit")
        values = []
        for item in row["values"]:
            _keys(item, {"name", "type", "value"})
            if item["type"] == "int" and type(item["value"]) is int:
                value = item["value"]
            elif item["type"] == "decimal":
                value = _decimal(item["value"])
            else:
                raise ValidationError("invalid auxiliary field dtype or value")
            values.append((item["name"], value))
        observation = AuxiliaryInput(
            **{
                **{key: row[key] for key in _OBSERVATION_KEYS - {"values", "coverage"}},
                "values": tuple(values),
                "coverage": _decimal(row["coverage"]),
            }
        )
        return instrument, observation
    except ValidationError:
        raise
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, OverflowError):
        raise ValidationError("invalid auxiliary observation encoding") from None
