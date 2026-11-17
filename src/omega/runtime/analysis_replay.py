"""Read-only replay of selected facts; no latest selection, new clock or writes."""

import re
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from hashlib import sha256

from omega.application.market_frame_builder import DEPTH_SOURCE_SUFFIX, MarketFrameBuilder
from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.ports.auxiliary import StoredAuxiliary
from omega.runtime.analysis_manifest import (
    InputPolicy,
    ManifestReference,
    frame_digest,
    pin_values,
)

MAX_TIME = 253_402_300_799_000
HEAD_KEYS = frozenset(
    {
        "schema_version",
        "kind",
        "analysis_id",
        "execution_id",
        "generation",
        "policy_hash",
        "start_ms",
        "end_ms",
        "source",
        "previous",
    }
)
CHUNK_KEYS = frozenset({"schema_version", "kind", "policy", "instrument", "pins", "frames"})
PIN_KEYS = frozenset(
    {
        "instrument",
        "channel",
        "source",
        "open_ms",
        "revision",
        "checksum",
        "previous_checksum",
        "available_at_ms",
        "event_time_ms",
    }
)
FRAME_KEYS = frozenset({"candle", "as_of_ms", "reference_time_ms", "pins", "frame_digest"})
CANDLE_KEYS = frozenset({"instrument", "open_ms", "open", "high", "low", "close", "volume"})


def _hex(value, length=64):
    return type(value) is str and re.fullmatch(f"[a-f0-9]{{{length}}}", value) is not None


def _time(value, *, aligned=False):
    return type(value) is int and 0 <= value <= MAX_TIME and (not aligned or value % 60000 == 0)


def _reference(value):
    return value if isinstance(value, ManifestReference) else ManifestReference.from_document(value)


def _head(value, analysis_id, policy, kind="ca_input"):
    if (
        type(value) is not dict
        or set(value) != HEAD_KEYS
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["kind"] != kind
        or value["analysis_id"] != analysis_id
        or not _hex(value["analysis_id"], 32)
        or not _hex(value["execution_id"], 32)
        or type(value["generation"]) is not int
        or not 0 <= value["generation"] < 2**63
        or value["policy_hash"] != policy.checksum
        or not _time(value["start_ms"], aligned=True)
        or not _time(value["end_ms"], aligned=True)
        or not 1 <= (value["end_ms"] - value["start_ms"]) // 60000 <= 512
    ):
        raise ValidationError("analysis input head schema, identity, policy or range changed")
    _reference(value["source"])
    if value["previous"] is not None:
        _reference(value["previous"])
    return value


def validate_head(
    manifest, consumption_ref, *, expected_analysis_id, expected_policy, expected_kind="ca_input"
):
    """Validate this head and immediate chain edge; full-history audit is separate."""
    if (
        not _hex(expected_analysis_id, 32)
        or not isinstance(expected_policy, InputPolicy)
        or expected_kind not in {"ca_input", "paper_input"}
    ):
        raise ValidationError("analysis replay requires its expected CA and fixed input policy")
    reference = _reference(consumption_ref)
    head = _head(manifest.read(reference), expected_analysis_id, expected_policy, expected_kind)
    if head["previous"] is not None:
        previous = _reference(head["previous"])
        if previous == reference:
            raise ValidationError("analysis input chain cannot reference its own head")
        prior = _head(manifest.read(previous), expected_analysis_id, expected_policy, expected_kind)
        if prior["end_ms"] != head["start_ms"] or prior["generation"] > head["generation"]:
            raise ValidationError("analysis input chain is discontinuous or regresses generation")
    return deepcopy(head)


def _instrument(value):
    try:
        if type(value) is not str:
            raise ValueError
        provider, market, symbol = value.split(":")
        return Instrument(provider, Market(market), symbol)
    except (TypeError, ValueError):
        raise ValidationError("invalid manifest instrument") from None


def _candle(value, instrument):
    if (
        type(value) is not dict
        or set(value) != CANDLE_KEYS
        or value["instrument"] != instrument.key
        or not _time(value["open_ms"], aligned=True)
        or value["open_ms"] > MAX_TIME - 60000
    ):
        raise ValidationError("invalid manifest candle identity or minute")
    numbers = []
    for name in ("open", "high", "low", "close", "volume"):
        text = value[name]
        if type(text) is not str or not 1 <= len(text) <= 512:
            raise ValidationError("manifest OHLCV requires bounded original decimal strings")
        try:
            number = Decimal(text)
        except InvalidOperation:
            raise ValidationError("invalid manifest decimal") from None
        if not number.is_finite():
            raise ValidationError("manifest OHLCV must be finite")
        numbers.append(number)
    return Candle(instrument, value["open_ms"], value["open_ms"] + 60000, *numbers)


def _pin(value, instrument, policy):
    if type(value) is not dict or set(value) != PIN_KEYS:
        raise ValidationError("invalid native pin schema")
    channel = value["channel"]
    allowed = {
        "depth": ("depth", DEPTH_SOURCE_SUFFIX),
        "trade_flow": ("trade_flow", "aggTrade"),
        "open_interest": ("metric", policy.open_interest_source_suffix),
        "long_short_ratio": ("metric", policy.long_short_source_suffix),
    }
    if type(channel) is not str or channel not in allowed:
        raise ValidationError("unsupported native pin channel")
    requested, suffix = allowed[channel]
    if (
        requested not in policy.channels
        or value["instrument"] != instrument.key
        or type(value["source"]) is not str
        or not 1 <= len(value["source"]) <= 96
        or value["source"] != instrument.key + ":" + suffix
        or not _time(value["open_ms"], aligned=True)
        or value["open_ms"] > MAX_TIME - 60000
        or type(value["revision"]) is not int
        or not 1 <= value["revision"] < 2**63
        or not _hex(value["checksum"])
        or (value["revision"] == 1) != (value["previous_checksum"] is None)
        or value["previous_checksum"] is not None
        and not _hex(value["previous_checksum"])
        or not _time(value["event_time_ms"])
        or not _time(value["available_at_ms"])
        or value["event_time_ms"] > value["available_at_ms"]
        or channel == "trade_flow"
        and value["event_time_ms"] != value["open_ms"] + 60000
        or channel != "trade_flow"
        and not value["open_ms"] <= value["event_time_ms"] < value["open_ms"] + 60000
    ):
        raise ValidationError("native pin identity, revision or source times changed")
    return value


class _PinnedStore:
    """The builder sees only real, verified facts selected for this particular frame."""

    def __init__(self, pins):
        self.rows = {}
        for pin in pins:
            observation = pin.observation
            key = (
                pin.instrument,
                observation.channel,
                observation.source,
                observation.window_start_ms,
            )
            if key in self.rows:
                raise ValidationError(
                    "one frame cannot select multiple revisions of one native key"
                )
            self.rows[key] = pin

    def get(self, instrument, channel, source, open_time_ms, *, as_of_ms):
        value = self.rows.get((instrument, channel, source, open_time_ms))
        if value is not None and value.observation.available_at_ms > as_of_ms:
            raise ValidationError("pinned observation was unavailable to this frame")
        return value

    def latest_sample(self, instrument, channel, source, *, event_before_ms, as_of_ms, max_age_ms):
        rows = [
            value
            for (key, kind, dataset, _), value in self.rows.items()
            if key == instrument and kind == channel and dataset == source
        ]
        if len(rows) > 1:
            raise ValidationError("frame selected more than one native point per metric")
        if not rows:
            return None
        value = rows[0]
        observation = value.observation
        if (
            not event_before_ms - max_age_ms <= observation.event_time_ms < event_before_ms
            or observation.available_at_ms > as_of_ms
        ):
            raise ValidationError(
                "pinned native point violates this frame's original selection range"
            )
        return value


def replay(
    manifest,
    consumption_ref,
    source,
    *,
    expected_analysis_id,
    expected_policy,
    expected_kind="ca_input",
):
    """Reconstruct exactly one immutable source chunk from its original native pins."""
    head = validate_head(
        manifest,
        consumption_ref,
        expected_analysis_id=expected_analysis_id,
        expected_policy=expected_policy,
        expected_kind=expected_kind,
    )
    policy = expected_policy
    source.check_destination(policy.destination_id)
    chunk = manifest.read(_reference(head["source"]))
    if (
        type(chunk) is not dict
        or set(chunk) != CHUNK_KEYS
        or type(chunk["schema_version"]) is not int
        or chunk["schema_version"] != 1
        or chunk["kind"] != "source_chunk"
        or InputPolicy.from_document(chunk["policy"]) != policy
    ):
        raise ValidationError("analysis source chunk schema or policy changed")
    instrument = _instrument(chunk["instrument"])
    pins, frames = chunk["pins"], chunk["frames"]
    if (
        type(pins) is not list
        or len(pins) > 4096
        or type(frames) is not list
        or not 1 <= len(frames) <= 512
    ):
        raise ValidationError("analysis source chunk exceeds pin/frame bounds")
    checked_pins = [_pin(value, instrument, policy) for value in pins]
    identities = [tuple(value[key] for key in sorted(PIN_KEYS)) for value in checked_pins]
    if len(set(identities)) != len(identities):
        raise ValidationError("analysis native pin table is not deduplicated")
    selected, used, cursor, previous_asof = [], set(), head["start_ms"], None
    for row in frames:
        if (
            type(row) is not dict
            or set(row) != FRAME_KEYS
            or not _hex(row["frame_digest"])
            or type(row["pins"]) is not list
            or len(row["pins"]) > 8
            or any(type(index) is not int or not 0 <= index < len(pins) for index in row["pins"])
            or len(set(row["pins"])) != len(row["pins"])
        ):
            raise ValidationError("invalid analysis frame record or pin indices")
        candle = _candle(row["candle"], instrument)
        if (
            candle.open_time_ms != cursor
            or not _time(row["as_of_ms"])
            or row["as_of_ms"] < candle.close_time_ms
            or row["reference_time_ms"] is not None
            and (
                type(row["reference_time_ms"]) is not int
                or row["reference_time_ms"] != candle.close_time_ms
            )
            or previous_asof is not None
            and row["as_of_ms"] < previous_asof
        ):
            raise ValidationError("manifest frame range or clocks changed")
        for index in row["pins"]:
            pin = checked_pins[index]
            if (
                pin["available_at_ms"] > row["as_of_ms"]
                or pin["channel"] in {"trade_flow", "depth"}
                and pin["open_ms"] != candle.open_time_ms
                or pin["channel"] not in {"trade_flow", "depth"}
                and (
                    instrument.market == Market.SPOT
                    or not candle.close_time_ms - policy.maximum_metric_age_ms
                    <= pin["event_time_ms"]
                    < candle.close_time_ms
                )
            ):
                raise ValidationError("native pin was not eligible for the selected frame")
        used.update(row["pins"])
        selected.append((candle, row))
        cursor, previous_asof = candle.close_time_ms, row["as_of_ms"]
    if cursor != head["end_ms"] or used != set(range(len(pins))):
        raise ValidationError("source chunk range or used native pin set changed")
    observations = []
    for pin in checked_pins:
        value = source.get_auxiliary(
            instrument,
            pin["channel"],
            pin["source"],
            pin["open_ms"],
            as_of_ms=pin["available_at_ms"],
        )
        if (
            not isinstance(value, StoredAuxiliary)
            or pin_values(value) != pin
            or sha256(encode_observation(instrument, value.observation)).hexdigest()
            != value.checksum
        ):
            raise StorageError("pinned native observation is missing or changed")
        observations.append(value)
    result = []
    for candle, row in selected:
        values = tuple(observations[index] for index in row["pins"])
        builder = MarketFrameBuilder(
            _PinnedStore(values),
            maximum_metric_age_ms=policy.maximum_metric_age_ms,
            open_interest_source_suffix=policy.open_interest_source_suffix,
            long_short_source_suffix=policy.long_short_source_suffix,
        )
        prepared = builder.prepare(
            candle,
            as_of_ms=row["as_of_ms"],
            channels=policy.channels,
            retrospective=row["reference_time_ms"] is not None,
        )
        if prepared.pins != values or frame_digest(prepared.frame) != row["frame_digest"]:
            raise StorageError("reconstructed analysis frame differs from its pinned manifest")
        result.append(prepared)
    return tuple(result)
