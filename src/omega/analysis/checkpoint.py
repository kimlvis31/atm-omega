"""Versioned, data-only checkpoints for the installed calculation graph.

Restoration overlays a freshly validated graph. No class name from a file is
imported, no code is unpickled, and configuration/topology must match exactly.
"""

import json
import math
from collections import deque
from copy import deepcopy
from dataclasses import asdict, is_dataclass
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256

from omega.analysis.aggregation import AggregatedBar
from omega.analysis.market_sentiment import _SentimentBucket
from omega.analysis.order_flow import _OrderFlowBucket
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, decimal

MAX_BYTES = 4_000_000

# Only these mutable calculation fields may be overlaid. New providers must
# explicitly declare a schema here, rather than accidentally serializing IO.
_STATE = {
    "omega.analysis.graph.FeatureGraph": ("_nodes", "_last", "_output"),
    "omega.analysis.market_graph.MarketFeatureGraph": (
        "_nodes",
        "_last",
        "_output",
        "_last_frame",
        "_node_outputs",
    ),
    "omega.analysis.aggregation.CandleAggregation": ("_buckets", "_last", "_result"),
    "omega.analysis.moving_averages.LegacyMaFeatures": (
        "_kernels",
        "_aggregation",
        "_last",
        "_result",
    ),
    "omega.analysis.moving_averages._Kernel": (
        "closes",
        "mas",
        "count",
        "signal",
        "delta",
        "absolute",
    ),
    "omega.analysis.momentum.LegacyMomentumFeatures": (
        "_kernels",
        "_aggregation",
        "_last",
        "_result",
    ),
    "omega.analysis.momentum._MomentumKernel": ("history", "previous_bar", "analysisCount"),
    "omega.analysis.swing.LegacySwingFeatures": (
        "_kernels",
        "_aggregation",
        "_last",
        "_result",
        "_published",
    ),
    "omega.analysis.swing._Kernel": ("search", "confirmed", "confirmed_at_ms"),
    "omega.analysis.parabolic.LegacyParabolicFeatures": (
        "_kernels",
        "_aggregation",
        "_last",
        "_result",
    ),
    "omega.analysis.parabolic._ParabolicKernel": ("history", "previous"),
    "omega.analysis.order_flow.LegacyOrderFlowFeatures": (
        "_kernels",
        "_buckets",
        "_last_frame",
        "_result",
        "_input_quality",
    ),
    "omega.analysis.order_flow._OrderFlowKernel": ("history", "previous"),
    "omega.analysis.market_sentiment.LegacyMarketSentimentFeatures": (
        "_kernels",
        "_buckets",
        "_last_frame",
        "_calculated_asof_ms",
        "_result",
        "_input_quality",
    ),
    "omega.analysis.market_sentiment._SentimentKernel": ("history", "previous"),
    "omega.analysis.short_term_scalar.LegacyShortTermScalarFeatures": (
        "_kernels",
        "_aggregation",
        "_buckets",
        "_baselines",
        "_last_frame",
        "_result",
        "_input_quality",
        "_raw",
    ),
    "omega.analysis.catalog.EmptyFeatures": (),
    "omega.analysis.volume_profile.LegacyVolumeProfileFeatures": (
        "_kernels",
        "_aggregation",
        "_last",
        "_result",
        "_raw",
    ),
    "omega.analysis.volume_profile._ProfileKernel": ("history", "previous"),
    "omega.analysis.temporal_probability.LegacyTemporalProbabilityFeatures": (
        "_kernels",
        "_aggregation",
        "_last",
        "_result",
        "_raw",
    ),
    "omega.analysis.temporal_probability._TemporalKernel": ("closes", "history", "previous"),
    "omega.analysis.price_windows.LegacyPriceWindowFeatures": (
        "_kernels",
        "_aggregation",
        "_last",
        "_result",
        "_raw",
        "_frames",
    ),
    "omega.analysis.price_windows._WindowKernel": (
        "closes",
        "history",
        "previous",
        "count",
        "first_open_ms",
    ),
}


def _name(value):
    return f"{type(value).__module__}.{type(value).__name__}"


def _plain(value, depth=0):
    if depth > 32:
        raise ValidationError("analysis checkpoint is too deeply nested")
    if value is None or type(value) in {bool, int, str}:
        return value
    if isinstance(value, StrEnum):
        return str(value)
    if type(value) is float and math.isfinite(value):
        return value
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, Instrument):
        return {"instrument": value.key}
    if isinstance(value, MarketFeatureFrame):
        frame = {
            "candle": _plain(value.candle, depth + 1),
            "as_of_ms": value.as_of_ms,
            "inputs": _plain(value.inputs, depth + 1),
        }
        # Preserve the exact old live-frame JSON schema when no retrospective
        # clock is requested. Older live checkpoints remain byte-compatible.
        if value.reference_time_ms is not None:
            frame["reference_time_ms"] = value.reference_time_ms
        return {"market_frame": frame}
    if isinstance(value, AuxiliaryInput):
        return {"auxiliary_input": _plain(asdict(value), depth + 1)}
    if isinstance(value, _OrderFlowBucket):
        return {"order_flow_bucket": _plain(asdict(value), depth + 1)}
    if isinstance(value, _SentimentBucket):
        return {"sentiment_bucket": _plain(asdict(value), depth + 1)}
    if isinstance(value, (Candle, AggregatedBar)):
        return {"bar": type(value).__name__, "values": _plain(asdict(value), depth + 1)}
    if is_dataclass(value):
        return {"configuration": _name(value), "values": _plain(asdict(value), depth + 1)}
    if isinstance(value, frozenset):
        return {"frozenset": sorted(value)}
    if isinstance(value, deque):
        return {"deque": [_plain(v, depth + 1) for v in value], "maxlen": value.maxlen}
    if isinstance(value, tuple):
        return {"tuple": [_plain(v, depth + 1) for v in value]}
    if isinstance(value, list):
        return [_plain(v, depth + 1) for v in value]
    if isinstance(value, dict):
        return {"pairs": [[_plain(k, depth + 1), _plain(v, depth + 1)] for k, v in value.items()]}
    raise ValidationError("unsupported analysis checkpoint value")


def _read(value, instrument, depth=0):
    if depth > 32:
        raise ValidationError("analysis checkpoint is too deeply nested")
    if value is None or type(value) in {bool, int, str}:
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if isinstance(value, list):
        return [_read(v, instrument, depth + 1) for v in value]
    if not isinstance(value, dict):
        raise ValidationError("invalid analysis checkpoint value")
    if set(value) == {"decimal"}:
        return decimal(value["decimal"])
    if set(value) == {"instrument"} and value["instrument"] == instrument.key:
        return instrument
    if set(value) == {"market_frame"}:
        row = value["market_frame"]
        if not isinstance(row, dict) or set(row) not in (
            {"candle", "as_of_ms", "inputs"},
            {"candle", "as_of_ms", "inputs", "reference_time_ms"},
        ):
            raise ValidationError("invalid auxiliary frame checkpoint")
        return MarketFeatureFrame(
            candle=_read(row["candle"], instrument, depth + 1),
            as_of_ms=row["as_of_ms"],
            inputs=_read(row["inputs"], instrument, depth + 1),
            reference_time_ms=row.get("reference_time_ms"),
        )
    if set(value) == {"auxiliary_input"}:
        return AuxiliaryInput(**_read(value["auxiliary_input"], instrument, depth + 1))
    if set(value) == {"sentiment_bucket"}:
        row = _read(value["sentiment_bucket"], instrument, depth + 1)
        if not isinstance(row, dict) or set(row) != {
            "open_time_ms",
            "valid_prefix",
            "price",
            "flow",
            "metric",
            "flow_complete",
        }:
            raise ValidationError("invalid sentiment bucket checkpoint")
        return _SentimentBucket(**row)
    if set(value) == {"order_flow_bucket"}:
        row = _read(value["order_flow_bucket"], instrument, depth + 1)
        if (
            not isinstance(row, dict)
            or set(row) != {"open_time_ms", "valid_prefix", "values", "ready"}
            or type(row["open_time_ms"]) is not int
            or row["open_time_ms"] < 0
            or row["open_time_ms"] % 60000
            or type(row["valid_prefix"]) is not bool
            or type(row["ready"]) is not bool
            or (
                row["values"] is not None
                and (
                    type(row["values"]) is not tuple
                    or len(row["values"]) not in {2, 12}
                    or any(
                        type(v) not in {int, float} or not math.isfinite(v) or v < 0
                        for v in row["values"]
                    )
                )
            )
        ):
            raise ValidationError("invalid order-flow bucket checkpoint")
        return _OrderFlowBucket(**row)
    if set(value) == {"tuple"}:
        return tuple(_read(v, instrument, depth + 1) for v in value["tuple"])
    if set(value) == {"deque", "maxlen"}:
        n = value["maxlen"]
        if type(n) is not int or not 1 <= n <= 100000 or len(value["deque"]) > n:
            raise ValidationError("invalid analysis checkpoint history")
        return deque((_read(v, instrument, depth + 1) for v in value["deque"]), maxlen=n)
    if set(value) == {"pairs"}:
        result = {}
        for key, item in value["pairs"]:
            key = _read(key, instrument, depth + 1)
            if key in result:
                raise ValidationError("duplicate checkpoint key")
            result[key] = _read(item, instrument, depth + 1)
        return result
    if set(value) == {"bar", "values"}:
        args = _read(value["values"], instrument, depth + 1)
        # dataclasses.asdict() expanded the nested Instrument: verify it before
        # reconstructing the validated domain identity, never trust a class tag.
        if args["instrument"] != asdict(instrument):
            raise ValidationError("checkpoint instrument mismatch")
        args["instrument"] = instrument
        if value["bar"] == "Candle":
            return Candle(**args)
        if value["bar"] == "AggregatedBar":
            bar = AggregatedBar(**args)
            if (
                type(bar.interval_id) is not int
                or type(bar.open_time_ms) is not int
                or type(bar.close_time_ms) is not int
                or bar.open_time_ms < 0
                or bar.open_time_ms >= bar.close_time_ms
                or not 0
                < bar.low
                <= min(bar.open, bar.close)
                <= max(bar.open, bar.close)
                <= bar.high
                or bar.volume < 0
                or type(bar.complete) is not bool
                or type(bar.valid_prefix) is not bool
            ):
                raise ValidationError("invalid aggregate checkpoint")
            return bar
    raise ValidationError("unknown analysis checkpoint tag")


def _pack(target):
    name = _name(target)
    if name not in _STATE:
        raise ValidationError("feature provider does not support exact checkpoint recovery")
    mutable = _STATE[name]
    fixed = {k: _plain(v) for k, v in vars(target).items() if k not in mutable}
    state = {}
    for key in mutable:
        value = getattr(target, key)
        if key == "_nodes":
            state[key] = [
                {
                    "node": _plain((n.name, n.dependencies, n.public_fields)),
                    "feature": _pack(n.feature),
                }
                for n in value
            ]
        elif key == "_kernels":
            state[key] = [[_plain(k), _pack(v)] for k, v in value.items()]
        elif key == "_aggregation":
            state[key] = _pack(value)
        else:
            state[key] = _plain(value)
    # Frozen configuration records contain data only and remain authoritative.
    return {"type": name, "fixed": fixed, "state": state}


def _overlay(target, raw, instrument):
    name = _name(target)
    expected = _pack(target)
    if (
        not isinstance(raw, dict)
        or set(raw) != {"type", "fixed", "state"}
        or raw["type"] != name
        or raw["fixed"] != expected["fixed"]
        or not isinstance(raw["state"], dict)
        or set(raw["state"]) != set(_STATE[name])
    ):
        raise ValidationError("analysis checkpoint configuration or schema mismatch")
    for key, raw_value in raw["state"].items():
        previous = getattr(target, key)
        if key == "_nodes":
            if len(previous) != len(raw_value):
                raise ValidationError("analysis checkpoint topology mismatch")
            for node, row in zip(previous, raw_value, strict=True):
                if set(row) != {"node", "feature"} or row["node"] != _plain(
                    (node.name, node.dependencies, node.public_fields)
                ):
                    raise ValidationError("analysis checkpoint topology mismatch")
                _overlay(node.feature, row["feature"], instrument)
        elif key == "_kernels":
            if len(previous) != len(raw_value):
                raise ValidationError("analysis checkpoint kernel mismatch")
            seen = set()
            for encoded, state in raw_value:
                identity = _read(encoded, instrument)
                if identity not in previous or identity in seen:
                    raise ValidationError("analysis checkpoint kernel mismatch")
                seen.add(identity)
                _overlay(previous[identity], state, instrument)
        elif key == "_aggregation":
            _overlay(previous, raw_value, instrument)
        else:
            value = _read(raw_value, instrument)
            if isinstance(previous, deque) and (
                not isinstance(value, deque) or value.maxlen != previous.maxlen
            ):
                raise ValidationError("analysis checkpoint history length mismatch")
            if type(previous) in {dict, int} and type(value) is not type(previous):
                raise ValidationError("analysis checkpoint state type mismatch")
            if (
                key in {"_last", "previous_bar"}
                and value is not None
                and not isinstance(value, Candle if key == "_last" else AggregatedBar)
            ):
                raise ValidationError("analysis checkpoint cursor type mismatch")
            if key in {"count", "analysisCount"} and value < 0:
                raise ValidationError("analysis checkpoint count cannot be negative")
            setattr(target, key, value)
    if name in {
        "omega.analysis.parabolic._ParabolicKernel",
        "omega.analysis.order_flow._OrderFlowKernel",
        "omega.analysis.order_flow.LegacyOrderFlowFeatures",
        "omega.analysis.market_graph.MarketFeatureGraph",
        "omega.analysis.market_sentiment._SentimentKernel",
        "omega.analysis.market_sentiment.LegacyMarketSentimentFeatures",
        "omega.analysis.short_term_scalar.LegacyShortTermScalarFeatures",
        "omega.analysis.volume_profile.LegacyVolumeProfileFeatures",
        "omega.analysis.volume_profile._ProfileKernel",
        "omega.analysis.temporal_probability.LegacyTemporalProbabilityFeatures",
        "omega.analysis.temporal_probability._TemporalKernel",
        "omega.analysis.price_windows.LegacyPriceWindowFeatures",
        "omega.analysis.price_windows._WindowKernel",
    }:
        target.validate_checkpoint()


def _json(value):
    try:
        data = json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")).encode()
    except (ValueError, TypeError, RecursionError) as exc:
        raise ValidationError("invalid analysis checkpoint JSON") from exc
    if len(data) > MAX_BYTES:
        raise ValidationError("analysis checkpoint exceeds bounded storage")
    return data


def checkpoint(graph):
    payload = _pack(graph)
    return {"version": 1, "payload": payload, "sha256": sha256(_json(payload)).hexdigest()}


def restore(graph, value, instrument):
    """Return a restored copy; invalid input never changes the active graph."""
    try:
        if (
            not isinstance(value, dict)
            or set(value) != {"version", "payload", "sha256"}
            or type(value["version"]) is not int
            or value["version"] != 1
            or sha256(_json(value["payload"])).hexdigest() != value["sha256"]
        ):
            raise ValidationError("analysis checkpoint checksum or version mismatch")
        candidate = deepcopy(graph)
        _overlay(candidate, value["payload"], instrument)
        return candidate
    except (KeyError, ValueError, TypeError, AttributeError, IndexError, RecursionError) as exc:
        raise ValidationError("malformed analysis checkpoint") from exc
