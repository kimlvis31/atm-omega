"""Frozen native margin inputs; no source or financial IO."""

import json
import re
from decimal import Decimal

from omega.analysis.order_flow import DEPTH_FIELDS, FLOW_FIELDS
from omega.application.auxiliary_codec import (
    decode_observation,
    encode_observation,
    observation_checksum,
)
from omega.application.market_frame_builder import DEPTH_SOURCE_SUFFIX
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.ports.analysis_inputs import PreparedMarketFrame
from omega.ports.auxiliary import StoredAuxiliary
from omega.runtime.margin_metric_inputs import MetricInputPolicy, rebuild_metric, validate_metric

MAX_INPUT_BYTES = 2 * 1024 * 1024
_CHANNELS = {"trade_flow": (FLOW_FIELDS, "aggTrade"), "depth": (DEPTH_FIELDS, DEPTH_SOURCE_SUFFIX)}


def _channels(value):
    if (
        type(value) not in {tuple, list}
        or not 1 <= len(value) <= len(_CHANNELS)
        or any(type(item) is not str or item not in _CHANNELS for item in value)
        or len(set(value)) != len(value)
    ):
        raise ValidationError("margin auxiliary analysis supports trade_flow and depth only")
    return tuple(sorted(value))


def destination_id(value):
    if type(value) is not str or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise ValidationError("margin auxiliary source needs a bound destination identity")
    return value


def _encoded(value):
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ProtocolError("invalid frozen margin auxiliary document") from None
    if len(raw) > MAX_INPUT_BYTES:
        raise ProtocolError("margin auxiliary inputs exceed the account byte budget")
    return raw


def _candle(candle):
    return {
        "instrument": candle.instrument.key,
        "open_time_ms": candle.open_time_ms,
        "close_time_ms": candle.close_time_ms,
        "values": [
            str(getattr(candle, name)) for name in ("open", "high", "low", "close", "volume")
        ],
    }


def unavailable_frames(candles, channels, *, as_of_ms, metric_policies=None):
    """Known candles still support fills/protection when an optional read fails."""
    if any("metric" in required for required in channels.values()):
        if type(metric_policies) is not dict:
            raise ValidationError("missing frozen native metric policy")
        prepared = []
        for candle in candles:
            required = channels.get(candle.instrument.key)
            if required is None:
                continue
            ordinary = tuple(channel for channel in required if channel != "metric")
            inputs = ()
            if ordinary:
                inputs = unavailable_frames(
                    (candle,), {candle.instrument.key: ordinary}, as_of_ms=as_of_ms
                )[0].frame.inputs
            if "metric" in required:
                policy = MetricInputPolicy.from_document(metric_policies[candle.instrument.key])
                metric = rebuild_metric(candle, as_of_ms=as_of_ms, policy=policy, pins=())
                inputs = (*inputs, metric.frame.input("metric"))
            prepared.append(
                PreparedMarketFrame(
                    MarketFeatureFrame(
                        candle, as_of_ms, tuple(sorted(inputs, key=lambda row: row.channel))
                    ),
                    (),
                )
            )
        return tuple(prepared)
    return tuple(
        PreparedMarketFrame(
            MarketFeatureFrame(
                candle,
                as_of_ms,
                tuple(
                    AuxiliaryInput(
                        channel,
                        (),
                        candle.open_time_ms,
                        candle.close_time_ms,
                        candle.close_time_ms,
                        as_of_ms,
                        "missing",
                        Decimal(0),
                        candle.instrument.key + ":" + _CHANNELS[channel][1],
                    )
                    for channel in _channels(channels[candle.instrument.key])
                ),
            ),
            (),
        )
        for candle in candles
        if candle.instrument.key in channels
    )


class MarginSessionInputs:
    def __init__(self, rows, graphs):
        self.channels, self.strategies = {}, set()
        self.metric_policies = {}
        for row in rows:
            required = set(graphs[row["id"]].descriptor.required_fields) - {"OHLCV"}
            if required - (_CHANNELS.keys() | {"metric"}):
                raise ValidationError(
                    "margin auxiliary analysis supports trade_flow and depth only"
                )
            if required:
                key = row["instrument"].key
                if "metric" in required:
                    age = getattr(graphs[row["id"]], "maximum_metric_age_ms", None)
                    if type(age) is not int or not 0 <= age <= 86400000:
                        raise ValidationError(
                            "margin auxiliary analysis supports trade_flow and depth; "
                            "metric requires its configured native age policy"
                        )
                    previous = self.metric_policies.get(key)
                    self.metric_policies[key] = MetricInputPolicy(
                        max(age, previous.maximum_metric_age_ms) if previous is not None else age
                    )
                self.channels[key] = tuple(sorted(set(self.channels.get(key, ())) | required))
                self.strategies.add(row["id"])
        self.enabled = bool(self.channels)

    @staticmethod
    def _prepared(prepared, candle, cutoff, channels=("trade_flow",)):
        channels = _channels(channels)
        if (
            type(prepared) is not PreparedMarketFrame
            or type(prepared.frame) is not MarketFeatureFrame
        ):
            raise ProtocolError("margin auxiliary input must be an original prepared frame")
        frame = prepared.frame
        if (
            frame.candle != candle
            or type(frame.as_of_ms) is not int
            or not candle.close_time_ms <= frame.as_of_ms <= cutoff
            or frame.retrospective
            or type(frame.inputs) is not tuple
            or len(frame.inputs) != len(channels)
            or type(prepared.pins) is not tuple
            or len(prepared.pins) > len(channels)
        ):
            raise ProtocolError("margin auxiliary frame differs from its closed financial minute")
        observations, payloads = {}, {}
        for observation in frame.inputs:
            if (
                type(observation) is not AuxiliaryInput
                or observation.channel not in channels
                or (observation.channel in observations)
            ):
                raise ProtocolError("margin auxiliary frame needs exact distinct native channels")
            payload = encode_observation(candle.instrument, observation)
            decoded_instrument, decoded = decode_observation(payload)
            fields, suffix = _CHANNELS[decoded.channel]
            event_valid = (
                decoded.event_time_ms == candle.close_time_ms
                if decoded.channel == "trade_flow"
                else (
                    candle.open_time_ms <= decoded.event_time_ms < candle.close_time_ms
                    or decoded.quality in {"missing", "unsupported"}
                    and decoded.event_time_ms == candle.close_time_ms
                )
            )
            if (
                decoded_instrument != candle.instrument
                or decoded != observation
                or decoded.source != candle.instrument.key + ":" + suffix
                or decoded.window_start_ms != candle.open_time_ms
                or decoded.window_end_ms != candle.close_time_ms
                or not event_valid
                or not candle.close_time_ms <= decoded.available_at_ms <= frame.as_of_ms
            ):
                raise ProtocolError("margin native source, window or availability differs")
            if decoded.quality == "carried":
                raise ProtocolError("native margin auxiliary observations cannot be carried")
            if decoded.quality in {"observed", "partial"}:
                values = decoded.mapping()
                checked_fields = (
                    tuple(name for name in fields if name in values)
                    if decoded.channel == "depth" and decoded.quality == "partial"
                    else fields
                )
                if any(
                    type(values.get(name)) is not Decimal
                    or not values[name].is_finite()
                    or values[name] < 0
                    for name in checked_fields
                ):
                    raise ProtocolError("margin native channel requires nonnegative Decimal fields")
            observations[decoded.channel], payloads[decoded.channel] = decoded, payload
        # Rebuild immutable values to validate even an unsafely modified dataclass.
        checked = MarketFeatureFrame(
            candle, frame.as_of_ms, tuple(observations[key] for key in channels)
        )
        pins = {}
        for pin in prepared.pins:
            if type(pin) is not StoredAuxiliary:
                raise ProtocolError("margin auxiliary pin must be a native stored observation")
            native = StoredAuxiliary(
                pin.instrument, pin.observation, pin.revision, pin.checksum, pin.previous_checksum
            )
            channel = native.observation.channel
            if (
                native.instrument != candle.instrument
                or channel not in observations
                or channel in pins
                or native.observation != observations[channel]
                or encode_observation(native.instrument, native.observation) != payloads[channel]
                or native.checksum != observation_checksum(payloads[channel])
            ):
                raise ProtocolError("margin auxiliary native pin or payload checksum differs")
            pins[channel] = {
                "revision": native.revision,
                "checksum": native.checksum,
                "previous_checksum": native.previous_checksum,
                "payload": payloads[channel].decode("utf-8"),
            }
        if any(
            key not in pins and observations[key].quality not in {"missing", "unsupported"}
            for key in channels
        ):
            raise ProtocolError("observed or partial margin inputs require their native pin")
        return checked, {
            "candle": _candle(candle),
            "as_of_ms": checked.as_of_ms,
            "inputs": [payloads[key].decode("utf-8") for key in channels],
            "pins": [pins[key] for key in channels if key in pins],
        }

    @staticmethod
    def _prepared_metric(prepared, candle, cutoff, channels, policy):
        if (
            type(prepared) is not PreparedMarketFrame
            or type(prepared.frame) is not MarketFeatureFrame
            or prepared.frame.candle != candle
            or type(prepared.frame.as_of_ms) is not int
            or not candle.close_time_ms <= prepared.frame.as_of_ms <= cutoff
            or prepared.frame.retrospective
            or type(prepared.frame.inputs) is not tuple
            or len(prepared.frame.inputs) != len(channels)
            or type(prepared.pins) is not tuple
            or len(prepared.pins) > len(channels) + 1
        ):
            raise ProtocolError("metric frame differs from its original financial minute")
        observations = {}
        for value in prepared.frame.inputs:
            if (
                type(value) is not AuxiliaryInput
                or value.channel not in channels
                or value.channel in observations
            ):
                raise ProtocolError("metric frame needs its exact distinct required channels")
            observations[value.channel] = value
        native_metric, ordinary_pins = [], []
        ordinary = tuple(channel for channel in channels if channel != "metric")
        for pin in prepared.pins:
            if type(pin) is not StoredAuxiliary or type(pin.observation) is not AuxiliaryInput:
                raise ProtocolError("metric frame needs original typed native pins")
            channel = pin.observation.channel
            if channel in {"open_interest", "long_short_ratio"}:
                native_metric.append(pin)
            elif channel in ordinary:
                ordinary_pins.append(pin)
            else:
                raise ProtocolError("orphan metric frame native pin")
        metric = validate_metric(
            candle,
            observations["metric"],
            as_of_ms=prepared.frame.as_of_ms,
            policy=policy,
            pins=tuple(native_metric),
        )
        payloads, pins = {}, {}
        if ordinary:
            checked, document = MarginSessionInputs._prepared(
                PreparedMarketFrame(
                    MarketFeatureFrame(
                        candle,
                        prepared.frame.as_of_ms,
                        tuple(observations[channel] for channel in ordinary),
                    ),
                    tuple(ordinary_pins),
                ),
                candle,
                cutoff,
                ordinary,
            )
            payloads = {
                value.channel: payload
                for value, payload in zip(checked.inputs, document["inputs"], strict=True)
            }
            pins = {
                decode_observation(row["payload"].encode())[1].channel: row
                for row in document["pins"]
            }
        payloads["metric"] = encode_observation(
            candle.instrument, metric.frame.input("metric")
        ).decode()
        metric_documents = [
            {
                "revision": pin.revision,
                "checksum": pin.checksum,
                "previous_checksum": pin.previous_checksum,
                "payload": encode_observation(pin.instrument, pin.observation).decode(),
            }
            for pin in metric.pins
        ]
        ordered_pins = []
        for channel in channels:
            if channel == "metric":
                ordered_pins.extend(metric_documents)
            elif channel in pins:
                ordered_pins.append(pins[channel])
        checked = MarketFeatureFrame(
            candle,
            prepared.frame.as_of_ms,
            tuple(decode_observation(payloads[channel].encode())[1] for channel in channels),
        )
        return checked, {
            "candle": _candle(candle),
            "as_of_ms": checked.as_of_ms,
            "inputs": [payloads[channel] for channel in channels],
            "pins": ordered_pins,
        }

    def freeze(self, prepared_frames, candles, *, as_of_ms, destination, expected=None):
        destination_id(destination)
        if expected is not None and destination != expected:
            raise ProtocolError("margin auxiliary destination cannot change during a session")
        if type(prepared_frames) is not tuple or len(prepared_frames) != len(self.channels):
            raise ProtocolError(
                "margin auxiliary frames must cover exactly the selected instruments"
            )
        by_key = {bar.instrument.key: bar for bar in candles}
        selected, documents = {}, {}
        for prepared in prepared_frames:
            if (
                type(prepared) is not PreparedMarketFrame
                or type(prepared.frame) is not MarketFeatureFrame
            ):
                raise ProtocolError("invalid prepared margin auxiliary input")
            key = prepared.frame.candle.instrument.key
            if key not in self.channels or key in selected or key not in by_key:
                raise ProtocolError("foreign or repeated margin auxiliary instrument")
            if key in self.metric_policies:
                selected[key], documents[key] = self._prepared_metric(
                    prepared, by_key[key], as_of_ms, self.channels[key], self.metric_policies[key]
                )
            else:
                selected[key], documents[key] = self._prepared(
                    prepared, by_key[key], as_of_ms, self.channels[key]
                )
        document = {
            "destination": destination,
            "frames": [documents[key] for key in sorted(documents)],
        }
        _encoded(document)
        return document, selected

    def restore(self, document, candles, *, as_of_ms, expected=None):
        _encoded(document)
        if type(document) is not dict or set(document) != {"destination", "frames"}:
            raise ProtocolError("invalid frozen margin auxiliary fields")
        if type(document["frames"]) is not list or len(document["frames"]) != len(self.channels):
            raise ProtocolError("invalid frozen margin auxiliary frame count")
        by_key = {bar.instrument.key: bar for bar in candles}
        prepared = []
        for row in document["frames"]:
            key = (
                row.get("candle", {}).get("instrument")
                if type(row) is dict and type(row.get("candle")) is dict
                else None
            )
            if key in self.metric_policies:
                if key not in by_key:
                    raise ProtocolError("foreign frozen metric candle")
                prepared.append(self._restore_metric(row, by_key[key]))
                continue
            if (
                type(row) is not dict
                or set(row) != {"candle", "as_of_ms", "inputs", "pins"}
                or type(row["candle"]) is not dict
                or row["candle"].get("instrument") not in by_key
                or type(row["inputs"]) is not list
                or not 1 <= len(row["inputs"]) <= len(_CHANNELS)
                or type(row["pins"]) is not list
                or len(row["pins"]) > len(_CHANNELS)
            ):
                raise ProtocolError("invalid frozen margin auxiliary frame")
            candle = by_key[row["candle"]["instrument"]]
            if row["candle"] != _candle(candle) or any(
                type(value) is not str for value in row["inputs"]
            ):
                raise ProtocolError("frozen margin auxiliary candle or payload changed")
            observations = {}
            for payload in row["inputs"]:
                instrument, observation = decode_observation(payload.encode("utf-8"))
                if instrument != candle.instrument:
                    raise ProtocolError("frozen margin auxiliary instrument changed")
                observations[payload] = observation
            pins = []
            for pin in row["pins"]:
                if (
                    type(pin) is not dict
                    or set(pin) != {"revision", "checksum", "previous_checksum", "payload"}
                    or pin["payload"] not in observations
                ):
                    raise ProtocolError("frozen margin auxiliary native pin changed")
                pins.append(
                    StoredAuxiliary(
                        candle.instrument,
                        observations[pin["payload"]],
                        pin["revision"],
                        pin["checksum"],
                        pin["previous_checksum"],
                    )
                )
            prepared.append(
                PreparedMarketFrame(
                    MarketFeatureFrame(candle, row["as_of_ms"], tuple(observations.values())),
                    tuple(pins),
                )
            )
        encoded, selected = self.freeze(
            tuple(prepared),
            candles,
            as_of_ms=as_of_ms,
            destination=document["destination"],
            expected=expected,
        )
        if encoded != document:
            raise ProtocolError("frozen margin auxiliary document is not canonical")
        return selected

    @staticmethod
    def _restore_metric(row, candle):
        if (
            set(row) != {"candle", "as_of_ms", "inputs", "pins"}
            or row["candle"] != _candle(candle)
            or type(row["inputs"]) is not list
            or not 1 <= len(row["inputs"]) <= 3
            or type(row["pins"]) is not list
            or len(row["pins"]) > 4
        ):
            raise ProtocolError("invalid frozen native metric fields")
        inputs, pins = [], []
        for payload in row["inputs"]:
            if type(payload) is not str:
                raise ProtocolError("invalid frozen metric input payload")
            instrument, observation = decode_observation(payload.encode())
            if instrument != candle.instrument:
                raise ProtocolError("foreign frozen metric input instrument")
            inputs.append(observation)
        for pin in row["pins"]:
            if (
                type(pin) is not dict
                or set(pin) != {"revision", "checksum", "previous_checksum", "payload"}
                or type(pin["payload"]) is not str
            ):
                raise ProtocolError("invalid frozen metric native pin")
            instrument, observation = decode_observation(pin["payload"].encode())
            if instrument != candle.instrument:
                raise ProtocolError("foreign frozen metric native pin instrument")
            pins.append(
                StoredAuxiliary(
                    instrument,
                    observation,
                    pin["revision"],
                    pin["checksum"],
                    pin["previous_checksum"],
                )
            )
        return PreparedMarketFrame(
            MarketFeatureFrame(candle, row["as_of_ms"], tuple(inputs)), tuple(pins)
        )

    def check_replay(self, prepared_frames, destination, original, candles, *, as_of_ms):
        if prepared_frames is None and destination is None:
            return  # Replay the already persisted inputs; never select a new revision.
        if destination != original["destination"]:
            raise ProtocolError("frozen margin auxiliary destination cannot be replaced")
        if prepared_frames is not None:
            supplied, _ = self.freeze(
                prepared_frames,
                candles,
                as_of_ms=as_of_ms,
                destination=destination,
                expected=original["destination"],
            )
            if supplied != original:
                raise ProtocolError("frozen margin auxiliary inputs cannot be replaced")

    @staticmethod
    def status(document):
        if document is None:
            return {"state": "waiting", "reason": "no_committed_auxiliary_frame"}
        entries = [
            (row, decode_observation(payload.encode("utf-8"))[1])
            for row in document["frames"]
            for payload in row["inputs"]
        ]
        if any(value.channel == "metric" for _row, value in entries):

            def known(row, value):
                if value.channel != "metric":
                    return value.usable_at(row["as_of_ms"])
                values = value.mapping()
                return (
                    value.quality in {"observed", "carried"}
                    and value.coverage == 1
                    and value.available_at_ms <= row["as_of_ms"]
                    and all(
                        type(values.get(name)) is Decimal and values[name] > 0
                        for name in ("METRIC_OPENINTEREST", "METRIC_LONGSHORTRATIO")
                    )
                )

            ready = all(known(row, value) for row, value in entries)
            return {
                "state": "ready" if ready else "unavailable",
                "reason": None if ready else "auxiliary_inputs_unavailable",
                "destination": document["destination"],
                "quality": [value.quality for _row, value in entries],
                "readiness_scope": "native_input_quality_not_strategy_readiness",
                "channel_quality": [
                    {
                        "instrument": row["candle"]["instrument"],
                        "channel": value.channel,
                        "quality": value.quality,
                    }
                    for row, value in entries
                ],
            }
        ready = all(value.usable_at(row["as_of_ms"]) for row, value in entries)
        return {
            "state": "ready" if ready else "unavailable",
            "reason": None if ready else "auxiliary_inputs_unavailable",
            "destination": document["destination"],
            "quality": [value.quality for _row, value in entries],
            **(
                {
                    "channel_quality": [
                        {
                            "instrument": row["candle"]["instrument"],
                            "channel": value.channel,
                            "quality": value.quality,
                        }
                        for row, value in entries
                    ]
                }
                if any(value.channel == "depth" for _row, value in entries)
                else {}
            ),
        }
