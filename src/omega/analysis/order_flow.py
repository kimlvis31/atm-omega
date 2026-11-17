"""Eta NES/WOI kernels over explicit minute observations; never fetch their own data."""

# Imports
import math
from collections import deque
from copy import deepcopy
from dataclasses import asdict, dataclass

from omega.analysis.aggregation import INTERVAL_MINUTES, validate_minute
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.ports.modules import ModuleDescriptor

# Constants
FLOW_FIELDS = ("AGGTRADE_NOTIONALBUY", "AGGTRADE_NOTIONALSELL")
DEPTH_FIELDS = tuple(f"DEPTH_BIDS{i}" for i in range(5, -1, -1)) + tuple(
    f"DEPTH_ASKS{i}" for i in range(6)
)
DEPTH_WEIGHTS = (4.5, 3.5, 2.5, 1.5, 0.6, 0.1)


def _finite(value):
    if not math.isfinite(value):
        raise ValidationError("order-flow legacy arithmetic overflow")
    return value


def _scalar(code, values):
    if values is None:
        return None
    if code == "NES":
        return _finite(values[0] - values[1])
    return _finite(
        sum(value / weight for value, weight in zip(values[:6], DEPTH_WEIGHTS, strict=True))
        - sum(
            value / weight
            for value, weight in zip(reversed(values[6:]), DEPTH_WEIGHTS, strict=True)
        )
    )


@dataclass(frozen=True)
class OrderFlowConfiguration:
    interval_id: int
    analysis_code: str
    nSamples: int = 14
    line_index: int = 0
    quote_precision: int = 8

    def __post_init__(self):
        if (
            type(self.interval_id) is not int
            or self.interval_id not in INTERVAL_MINUTES
            or not isinstance(self.analysis_code, str)
            or self.analysis_code not in {"NES", "WOI"}
            or type(self.nSamples) is not int
            or not 2 <= self.nSamples <= 100000
            or type(self.line_index) is not int
            or not 0 <= self.line_index <= 9
            or type(self.quote_precision) is not int
            or not 0 <= self.quote_precision <= 16
        ):
            raise ValidationError("invalid order-flow configuration")


class _OrderFlowKernel:
    def __init__(self, configuration):
        self.config = configuration
        self.history = deque(maxlen=configuration.nSamples - 1)
        self.previous = {}

    def preview(self, raw):
        code, nSamples = self.config.analysis_code, self.config.nSamples
        previous = self.previous
        mode = previous.get("mode", 0)
        if mode == 0:
            records = [raw, *reversed(self.history)]
            value = None
            if len(records) == nSamples and all(item is not None for item in records):
                value = round(_finite(sum(records) / nSamples), self.config.quote_precision)
                mode = 1
        else:
            kValue = 2 / (nSamples + 1)
            value = (
                previous[code]
                if raw is None
                else round(
                    _finite(raw * kValue + previous[code] * (1 - kValue)),
                    self.config.quote_precision,
                )
            )
        absolute = None
        if value is not None and previous.get(code) is not None:
            kValue = 2 / (nSamples * 10 + 1)
            prior = previous[code + "_ABSMA"]
            absolute = _finite(
                abs(value) * kValue
                + (abs(previous[code]) if prior is None else prior) * (1 - kValue)
            )
        relative = None if absolute is None else round(value / absolute, 5) if absolute else 0.0
        if relative is not None:
            _finite(relative)
        return {
            "NETNOTIONAL" if code == "NES" else "IMBALANCE": raw,
            code: value,
            code + "_ABSMA": absolute,
            code + "_ABSMAREL": relative,
            "mode": mode,
        }

    def commit(self, raw, result):
        self.history.append(raw)
        self.previous = dict(result)

    def validate_checkpoint(self):
        code = self.config.analysis_code
        raw_key = "NETNOTIONAL" if code == "NES" else "IMBALANCE"
        if not isinstance(self.history, deque) or self.history.maxlen != self.config.nSamples - 1:
            raise ValidationError("invalid order-flow checkpoint history")
        if any(
            value is not None and (type(value) not in {int, float} or not math.isfinite(value))
            for value in self.history
        ):
            raise ValidationError("invalid order-flow historical scalar")
        previous = self.previous
        if previous == {} and not self.history:
            return
        if (
            not isinstance(previous, dict)
            or set(previous) != {raw_key, code, code + "_ABSMA", code + "_ABSMAREL", "mode"}
            or type(previous["mode"]) is not int
            or previous["mode"] not in {0, 1}
            or not self.history
            or self.history[-1] != previous[raw_key]
        ):
            raise ValidationError("invalid order-flow checkpoint result")
        for key, value in previous.items():
            if (
                key != "mode"
                and value is not None
                and (type(value) not in {int, float} or not math.isfinite(value))
            ):
                raise ValidationError("nonfinite order-flow checkpoint result")
        if previous["mode"] == 0 and any(
            previous[key] is not None for key in (code, code + "_ABSMA", code + "_ABSMAREL")
        ):
            raise ValidationError("invalid order-flow warmup checkpoint")
        if previous["mode"] == 1 and (
            len(self.history) != self.history.maxlen
            or previous[code] is None
            or (previous[code + "_ABSMA"] is None) != (previous[code + "_ABSMAREL"] is None)
            or (previous[code + "_ABSMA"] is not None and previous[code + "_ABSMA"] < 0)
        ):
            raise ValidationError("invalid active order-flow checkpoint")
        absolute, relative = previous[code + "_ABSMA"], previous[code + "_ABSMAREL"]
        if absolute is not None and relative != (
            round(previous[code] / absolute, 5) if absolute else 0.0
        ):
            raise ValidationError("inconsistent order-flow relative checkpoint")


@dataclass
class _OrderFlowBucket:
    open_time_ms: int
    valid_prefix: bool
    values: tuple | None = None
    ready: bool = True


def _effective_raw(code, bucket):
    # Incomplete flow sums are evidence of received minutes, never a full-interval
    # NES sample. Depth deliberately retains its separate latest-snapshot meaning.
    if code == "NES" and not bucket.ready:
        return None
    return _scalar(code, bucket.values)


class LegacyOrderFlowFeatures:
    """Preview from closed kernels; missing values may hold but never become ready."""

    @staticmethod
    def validate_configurations(configurations):
        """Validate the complete definition budget without constructing a provider."""
        configurations = tuple(configurations)
        if any(not isinstance(row, OrderFlowConfiguration) for row in configurations):
            raise ValidationError("typed order-flow configurations required")
        keys = [(row.interval_id, row.analysis_code, row.line_index) for row in configurations]
        if (
            not keys
            or len(keys) > 64
            or len(set(keys)) != len(keys)
            or sum(row.nSamples for row in configurations) > 100000
        ):
            raise ValidationError("unique bounded order-flow configurations required")
        return configurations

    def __init__(self, instrument, configurations):
        configurations = self.validate_configurations(configurations)
        keys = [(row.interval_id, row.analysis_code, row.line_index) for row in configurations]
        self.instrument = instrument
        self._kernels = {
            key: _OrderFlowKernel(config) for key, config in zip(keys, configurations, strict=True)
        }
        channels = {
            "trade_flow" if row.analysis_code == "NES" else "depth" for row in configurations
        }
        self.descriptor = ModuleDescriptor("legacy.order_flow", "1", frozenset(channels), True)
        self._buckets = {}
        self._last_frame = None
        self._result, self._input_quality = {}, {}

    @property
    def input_ready(self):
        return bool(self._input_quality) and all(
            row["ready"] for row in self._input_quality.values()
        )

    @property
    def warmup_alignment_minutes(self):
        return math.lcm(*(INTERVAL_MINUTES[key[0]] for key in self._kernels))

    @property
    def input_quality(self):
        return deepcopy(self._input_quality)

    @property
    def state(self):
        return deepcopy(
            {
                "kernels": {
                    key: {"history": tuple(kernel.history), "previous": kernel.previous}
                    for key, kernel in self._kernels.items()
                },
                "buckets": {key: asdict(bucket) for key, bucket in self._buckets.items()},
                "input_quality": self._input_quality,
                "last_open_time_ms": self._last_frame.candle.open_time_ms
                if self._last_frame
                else None,
            }
        )

    @staticmethod
    def _observation(frame, channel, fields):
        observation = frame.input(channel)
        if observation is None:
            return None, "missing"
        if not observation.usable_at(frame.as_of_ms):
            return None, observation.quality if observation.quality != "observed" else "incomplete"
        if not frame.candle.open_time_ms <= observation.event_time_ms <= frame.candle.close_time_ms:
            return None, "stale"
        values = observation.mapping()
        if any(field not in values for field in fields):
            return None, "missing_fields"
        try:
            result = tuple(float(values[field]) for field in fields)
        except (ValueError, TypeError, OverflowError):
            raise ValidationError("invalid order-flow notional") from None
        if any(not math.isfinite(value) or value < 0 for value in result):
            raise ValidationError("order-flow notionals must be finite and nonnegative")
        return result, "observed"

    def update(self, candle):
        return self.update_frame(MarketFeatureFrame(candle=candle, as_of_ms=candle.close_time_ms))

    def update_frame(self, frame):
        if not isinstance(frame, MarketFeatureFrame):
            raise ValidationError("order-flow analysis requires an explicit market frame")
        candle = frame.candle
        if (
            type(candle.open_time_ms) is not int
            or type(candle.close_time_ms) is not int
            or candle.open_time_ms % 60000
            or candle.close_time_ms - candle.open_time_ms != 60000
        ):
            raise ValidationError("order-flow analysis requires exact integer minute times")
        previous = self._last_frame
        validate_minute(self.instrument, previous.candle if previous else None, candle)
        if previous is not None and candle == previous.candle:
            if frame.inputs != previous.inputs:
                raise ValidationError("analysis needs immutable auxiliary minute observations")
            return dict(self._result)
        kernels, buckets = deepcopy(self._kernels), deepcopy(self._buckets)
        result, quality = {}, {}
        aggregates = {}
        # [1]: Sum minute flow; depth is the latest usable snapshot in this bucket.
        for kernel in kernels.values():
            config = kernel.config
            aggregate_key = (config.interval_id, config.analysis_code, config.quote_precision)
            if aggregate_key in aggregates:
                continue
            duration = INTERVAL_MINUTES[config.interval_id] * 60000
            opened = candle.open_time_ms // duration * duration
            bucket = buckets.get(aggregate_key)
            if bucket is None or bucket.open_time_ms != opened:
                bucket = _OrderFlowBucket(opened, candle.open_time_ms == opened)
                buckets[aggregate_key] = bucket
            channel = "trade_flow" if config.analysis_code == "NES" else "depth"
            fields = FLOW_FIELDS if channel == "trade_flow" else DEPTH_FIELDS
            values, reason = self._observation(frame, channel, fields)
            if channel == "trade_flow":
                bucket.ready = bucket.ready and values is not None
                if values is not None:
                    bucket.values = (
                        values
                        if bucket.values is None
                        else tuple(
                            round(_finite(old + new), config.quote_precision)
                            for old, new in zip(bucket.values, values, strict=True)
                        )
                    )
            else:
                bucket.ready = values is not None
                if values is not None:
                    bucket.values = values
            raw = _effective_raw(config.analysis_code, bucket)
            closed = bucket.valid_prefix and candle.close_time_ms == opened + duration
            ready = bucket.valid_prefix and bucket.ready
            aggregates[aggregate_key] = (
                raw,
                closed,
                ready,
                reason if ready or values is None else "incomplete",
            )
        # [2]: Preview all configurations from their last closed state, then publish atomically.
        for (iid, code, line), kernel in kernels.items():
            key = (iid, code, kernel.config.quote_precision)
            raw, closed, ready, reason = aggregates[key]
            bucket = buckets[key]
            values = kernel.preview(raw) if bucket.valid_prefix else None
            prefix = f"{iid}_{code}_{line}_"
            for suffix, name in (
                (code, code),
                (code + "ABSMA", code + "_ABSMA"),
                (code + "ABSMAREL", code + "_ABSMAREL"),
            ):
                result[prefix + suffix] = values[name] if values else None
            result[f"CLOSED_{iid}"] = closed
            quality[prefix[:-1]] = {
                "channel": "trade_flow" if code == "NES" else "depth",
                "ready": ready,
                "reason": reason if bucket.valid_prefix else "partial_prefix",
            }
            if closed:
                kernel.commit(raw, values)
        self._kernels, self._buckets = kernels, buckets
        self._last_frame, self._result, self._input_quality = frame, result, quality
        return dict(result)

    def validate_checkpoint(self):
        """Check restored data against this provider's fixed schema and last frame.

        This checks small state consistency, not a replay proof of every old input.
        It deliberately permits real missing/hold and partial-prefix states.
        """
        for kernel in self._kernels.values():
            kernel.validate_checkpoint()
        if not all(
            isinstance(value, dict) for value in (self._buckets, self._result, self._input_quality)
        ):
            raise ValidationError("invalid order-flow provider checkpoint mappings")
        if self._last_frame is None:
            if (
                self._buckets
                or self._result
                or self._input_quality
                or any(kernel.history or kernel.previous for kernel in self._kernels.values())
            ):
                raise ValidationError("inconsistent uninitialized order-flow checkpoint")
            return
        if not isinstance(self._last_frame, MarketFeatureFrame):
            raise ValidationError("invalid order-flow frame checkpoint")
        candle = self._last_frame.candle
        if (
            candle.instrument != self.instrument
            or type(candle.open_time_ms) is not int
            or type(candle.close_time_ms) is not int
            or candle.open_time_ms < 0
            or candle.open_time_ms % 60000
            or candle.close_time_ms - candle.open_time_ms != 60000
        ):
            raise ValidationError("invalid order-flow minute cursor checkpoint")
        expected_buckets = {
            (iid, code, kernel.config.quote_precision)
            for (iid, code, _), kernel in self._kernels.items()
        }
        if set(self._buckets) != expected_buckets:
            raise ValidationError("order-flow checkpoint bucket configuration mismatch")
        for key, bucket in self._buckets.items():
            if (
                type(key) is not tuple
                or len(key) != 3
                or type(key[0]) is not int
                or type(key[1]) is not str
                or type(key[2]) is not int
                or not isinstance(bucket, _OrderFlowBucket)
                or type(bucket.open_time_ms) is not int
                or bucket.open_time_ms
                != candle.open_time_ms
                // (INTERVAL_MINUTES[key[0]] * 60000)
                * (INTERVAL_MINUTES[key[0]] * 60000)
                or type(bucket.valid_prefix) is not bool
                or type(bucket.ready) is not bool
            ):
                raise ValidationError("invalid order-flow bucket cursor checkpoint")
            if bucket.values is not None and (
                type(bucket.values) is not tuple
                or len(bucket.values) != (2 if key[1] == "NES" else 12)
                or any(
                    type(value) not in {int, float} or not math.isfinite(value) or value < 0
                    for value in bucket.values
                )
            ):
                raise ValidationError("invalid order-flow bucket notional checkpoint")
            if bucket.ready and bucket.values is None:
                raise ValidationError("ready order-flow bucket lacks observations")
        public, quality_keys = set(), set()
        for (iid, code, line), kernel in self._kernels.items():
            bucket = self._buckets[(iid, code, kernel.config.quote_precision)]
            duration = INTERVAL_MINUTES[iid] * 60000
            closed = bucket.valid_prefix and candle.close_time_ms == bucket.open_time_ms + duration
            prefix = f"{iid}_{code}_{line}_"
            fields = (
                (code, code),
                (code + "ABSMA", code + "_ABSMA"),
                (code + "ABSMAREL", code + "_ABSMAREL"),
            )
            public.update(prefix + suffix for suffix, _ in fields)
            public.add(f"CLOSED_{iid}")
            quality_keys.add(prefix[:-1])
            raw = _effective_raw(code, bucket)
            values = None
            if not bucket.valid_prefix:
                if kernel.previous or kernel.history:
                    raise ValidationError("partial initial bucket has closed order-flow state")
            elif closed:
                values = kernel.previous
                raw_key = "NETNOTIONAL" if code == "NES" else "IMBALANCE"
                if not values or values[raw_key] != raw:
                    raise ValidationError("closed order-flow result and bucket disagree")
            else:
                values = kernel.preview(raw)
            for suffix, name in fields:
                value = self._result.get(prefix + suffix)
                if (
                    value is not None
                    and (type(value) not in {int, float} or not math.isfinite(value))
                ) or (value != (values[name] if values else None)):
                    raise ValidationError("invalid cached order-flow output")
            if (
                type(self._result.get(f"CLOSED_{iid}")) is not bool
                or self._result[f"CLOSED_{iid}"] != closed
            ):
                raise ValidationError("invalid order-flow closure checkpoint")
            channel = "trade_flow" if code == "NES" else "depth"
            observed, reason = self._observation(
                self._last_frame, channel, FLOW_FIELDS if code == "NES" else DEPTH_FIELDS
            )
            if bucket.ready and observed is None:
                raise ValidationError("order-flow ready flag contradicts the final observation")
            if code == "WOI" and (
                bucket.ready != (observed is not None)
                or observed is not None
                and observed != bucket.values
            ):
                raise ValidationError("order-flow depth checkpoint is not the latest snapshot")
            if candle.open_time_ms == bucket.open_time_ms and bucket.values != observed:
                raise ValidationError(
                    "first-minute order-flow bucket disagrees with its observation"
                )
            ready = bucket.valid_prefix and bucket.ready
            expected_reason = (
                "partial_prefix"
                if not bucket.valid_prefix
                else (reason if ready or observed is None else "incomplete")
            )
            row = self._input_quality.get(prefix[:-1])
            if (
                not isinstance(row, dict)
                or set(row) != {"channel", "ready", "reason"}
                or row["channel"] != channel
                or type(row["ready"]) is not bool
                or row["ready"] != ready
                or row["reason"] != expected_reason
            ):
                raise ValidationError("invalid order-flow quality checkpoint")
        if set(self._result) != public or set(self._input_quality) != quality_keys:
            raise ValidationError("unexpected order-flow checkpoint output fields")

    def checkpoint(self):
        from omega.analysis.checkpoint import checkpoint

        return checkpoint(self)

    def restored(self, state, instrument):
        from omega.analysis.checkpoint import restore

        return restore(self, state, instrument)
