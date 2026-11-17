"""Eta MST arithmetic with explicit metric age and causal closed-kernel previews."""

# Imports
import math
from collections import deque
from copy import deepcopy
from dataclasses import asdict, dataclass, replace

from omega.analysis.aggregation import INTERVAL_MINUTES, validate_minute
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.ports.modules import ModuleDescriptor

# Constants
RAW_KEYS = ("PRICE", "OI", "LS", "NB", "NS")
MA_KEYS = ("PRICE_MA", "OI_MA", "LSLOG_MA", "NB_MA", "NS_MA")
OUTPUTS = {
    "MST": "MST",
    "PRICESTR": "PRICE_STR",
    "OISTR": "OI_STR",
    "LSSTR": "LS_STR",
    "TAKERSTR": "TAKER_STR",
    "MSTBASE": "MST_BASE",
}
RESULT_KEYS = frozenset(
    (
        *RAW_KEYS,
        *MA_KEYS,
        "PRICE_DABSMA",
        "OI_DABSMA",
        "LS_DABSMA",
        "PRICE_DREL",
        "OI_DREL",
        "LS_DREL",
        "PRICE_STR",
        "OI_STR",
        "LS_STR",
        "TAKER_IMB",
        "TAKER_ABSMA",
        "TAKER_REL",
        "TAKER_STR",
        "MST_BASE",
        "MST",
        "mode",
    )
)
FLOW_FIELDS = ("AGGTRADE_NOTIONALBUY", "AGGTRADE_NOTIONALSELL")
METRIC_FIELDS = ("METRIC_OPENINTEREST", "METRIC_LONGSHORTRATIO")


def _finite(value):
    if not math.isfinite(value):
        raise ValidationError("MST legacy arithmetic overflow")
    return value


def _number(value):
    try:
        return _finite(float(value))
    except (ValueError, TypeError, OverflowError):
        raise ValidationError("MST input is outside finite legacy arithmetic") from None


def _absolute(delta, previous, kValue):
    if delta is None:
        return None
    return (
        abs(delta) if previous is None else _finite(abs(delta) * kValue + previous * (1 - kValue))
    )


def _relative(delta, absolute):
    if delta is None or absolute is None:
        return None
    return 0.0 if absolute == 0 else _finite(round(delta / absolute, 5))


def _strength(relative):
    return None if relative is None else round(math.tanh(relative), 5)


def _base(price, oi, ls):
    flat = -0.5 * oi * ls
    trend = (
        0.5 + 0.5 * (1 + oi) * (1 - ls) / 4 if price >= 0 else -0.5 - 0.5 * (1 + oi) * (1 + ls) / 4
    )
    return round(max(-1.0, min(1.0, (1 - abs(price)) * flat + abs(price) * trend)), 5)


def _taker(base, strength):
    return round(max(-1.0, min(1.0, base + 0.5 * strength * abs(base))), 5)


@dataclass(frozen=True)
class SentimentConfiguration:
    interval_id: int
    nSamples: int = 14
    line_index: int = 0
    quote_precision: int = 8
    # Five-minute native metrics plus a configurable 30-second receipt allowance;
    # this is an application policy, not a provider freshness guarantee.
    maximum_metric_age_ms: int = 330000

    def __post_init__(self):
        if (
            type(self.interval_id) is not int
            or self.interval_id not in INTERVAL_MINUTES
            or type(self.nSamples) is not int
            or not 2 <= self.nSamples <= 20000
            or type(self.line_index) is not int
            or not 0 <= self.line_index <= 9
            or type(self.quote_precision) is not int
            or not 0 <= self.quote_precision <= 16
            or type(self.maximum_metric_age_ms) is not int
            or not 0 <= self.maximum_metric_age_ms <= 86400000
        ):
            raise ValidationError("invalid bounded MST configuration")


class _SentimentKernel:
    def __init__(self, configuration):
        self.config = configuration
        self.history = deque(maxlen=configuration.nSamples - 1)
        self.previous = {}

    def preview(self, price, metric, flow):
        previous, nSamples = self.previous, self.config.nSamples
        oi_raw, ls_raw = metric if metric is not None else (None, None)
        oi = oi_raw if oi_raw is not None and oi_raw > 0 else previous.get("OI")
        ls = ls_raw if ls_raw is not None and ls_raw > 0 else previous.get("LS")
        nb, ns = flow if flow is not None else (None, None)
        result = dict(zip(RAW_KEYS, (price, oi, ls, nb, ns), strict=True))
        mode = previous.get("mode", 0)
        averages = [None] * 5
        if mode == 0:
            rows = [result, *reversed(self.history)]
            if len(rows) == nSamples and all(
                row[key] is not None for row in rows for key in RAW_KEYS
            ):
                averages = [sum(row[key] for row in rows) / nSamples for key in ("PRICE", "OI")]
                averages += [sum(math.log(row["LS"]) for row in rows) / nSamples]
                averages += [sum(row[key] for row in rows) / nSamples for key in ("NB", "NS")]
                mode = 1
        else:
            kValue = 2 / (nSamples + 1)
            averages = [
                previous[ma]
                if result[key] is None
                else (
                    (math.log(result[key]) if key == "LS" else result[key]) * kValue
                    + previous[ma] * (1 - kValue)
                )
                for key, ma in zip(RAW_KEYS, MA_KEYS, strict=True)
            ]
        result.update(zip(MA_KEYS, averages, strict=True))
        if result["PRICE_MA"] is None or previous.get("PRICE_MA") is None:
            deltas = (None, None, None)
        else:
            deltas = (
                None
                if previous["PRICE_MA"] == 0
                else (result["PRICE_MA"] - previous["PRICE_MA"]) / previous["PRICE_MA"],
                None
                if previous["OI_MA"] == 0
                else (result["OI_MA"] - previous["OI_MA"]) / previous["OI_MA"],
                result["LSLOG_MA"] - previous["LSLOG_MA"],
            )
        kValue = 2 / (nSamples * 10 + 1)
        for name, delta in zip(("PRICE", "OI", "LS"), deltas, strict=True):
            absolute = _absolute(delta, previous.get(name + "_DABSMA"), kValue)
            relative = _relative(delta, absolute)
            result.update(
                {
                    name + "_DABSMA": absolute,
                    name + "_DREL": relative,
                    name + "_STR": _strength(relative),
                }
            )
        if result["NB_MA"] is None or result["NS_MA"] is None:
            imbalance = None
        else:
            total = _finite(result["NB_MA"] + result["NS_MA"])
            imbalance = 0.0 if total == 0 else (result["NB_MA"] - result["NS_MA"]) / total
        absolute = _absolute(imbalance, previous.get("TAKER_ABSMA"), kValue)
        relative = _relative(imbalance, absolute)
        strength = _strength(relative)
        result.update(
            {
                "TAKER_IMB": imbalance,
                "TAKER_ABSMA": absolute,
                "TAKER_REL": relative,
                "TAKER_STR": strength,
            }
        )
        strengths = tuple(result[name + "_STR"] for name in ("PRICE", "OI", "LS"))
        base = None if any(value is None for value in (*strengths, strength)) else _base(*strengths)
        result.update(
            {
                "MST_BASE": base,
                "MST": None if base is None else _taker(base, strength),
                "mode": mode,
            }
        )
        for key, value in result.items():
            if key != "mode" and value is not None:
                _finite(value)
        return result

    def commit(self, result):
        self.history.append({key: result[key] for key in RAW_KEYS})
        self.previous = dict(result)

    def validate_checkpoint(self):
        if not isinstance(self.history, deque) or self.history.maxlen != self.config.nSamples - 1:
            raise ValidationError("invalid MST seed history")
        for row in self.history:
            if not isinstance(row, dict) or set(row) != set(RAW_KEYS):
                raise ValidationError("invalid MST raw history row")
            self._validate_values(row)
        previous = self.previous
        if previous == {} and not self.history:
            return
        if (
            not isinstance(previous, dict)
            or set(previous) != RESULT_KEYS
            or type(previous["mode"]) is not int
            or previous["mode"] not in {0, 1}
            or not self.history
            or self.history[-1] != {key: previous[key] for key in RAW_KEYS}
        ):
            raise ValidationError("invalid MST previous result")
        self._validate_values(previous)
        if previous["mode"] == 0:
            if any(previous[key] is not None for key in RESULT_KEYS - {*RAW_KEYS, "mode"}):
                raise ValidationError("MST seed checkpoint contains active fields")
            return
        if len(self.history) != self.history.maxlen or any(
            previous[key] is None for key in MA_KEYS
        ):
            raise ValidationError("MST active checkpoint lacks seeded averages")
        if (
            previous["PRICE_MA"] <= 0
            or previous["OI_MA"] <= 0
            or previous["NB_MA"] < 0
            or previous["NS_MA"] < 0
        ):
            raise ValidationError("invalid MST average checkpoint")
        if previous["OI"] is None or previous["LS"] is None:
            raise ValidationError("active MST checkpoint lacks last-positive metrics")
        if len({previous[name + "_DABSMA"] is None for name in ("PRICE", "OI", "LS")}) != 1:
            raise ValidationError("inconsistent MST delta readiness checkpoint")
        for name in ("PRICE", "OI", "LS"):
            absolute, relative, strength = (
                previous[name + suffix] for suffix in ("_DABSMA", "_DREL", "_STR")
            )
            if (
                (absolute is None) != (relative is None)
                or (absolute is not None and absolute < 0)
                or (absolute == 0 and relative != 0)
                or strength != _strength(relative)
            ):
                raise ValidationError("invalid MST delta checkpoint")
        total = _finite(previous["NB_MA"] + previous["NS_MA"])
        imbalance = 0.0 if total == 0 else (previous["NB_MA"] - previous["NS_MA"]) / total
        if (
            previous["TAKER_IMB"] != imbalance
            or previous["TAKER_ABSMA"] is None
            or previous["TAKER_ABSMA"] < 0
            or previous["TAKER_REL"] != _relative(imbalance, previous["TAKER_ABSMA"])
            or previous["TAKER_STR"] != _strength(previous["TAKER_REL"])
        ):
            raise ValidationError("invalid MST taker checkpoint")
        strengths = tuple(previous[name + "_STR"] for name in ("PRICE", "OI", "LS"))
        base = None if any(value is None for value in strengths) else _base(*strengths)
        if previous["MST_BASE"] != base or previous["MST"] != (
            None if base is None else _taker(base, previous["TAKER_STR"])
        ):
            raise ValidationError("invalid MST bounded result checkpoint")

    @staticmethod
    def _validate_values(row):
        for key, value in row.items():
            if (
                key != "mode"
                and value is not None
                and (type(value) not in {int, float} or not math.isfinite(value))
            ):
                raise ValidationError("nonfinite MST checkpoint scalar")
        if (
            row["PRICE"] is None
            or any(row.get(key) is not None and row[key] <= 0 for key in ("PRICE", "OI", "LS"))
            or any(row.get(key) is not None and row[key] < 0 for key in ("NB", "NS"))
        ):
            raise ValidationError("invalid MST raw checkpoint values")


@dataclass
class _SentimentBucket:
    open_time_ms: int
    valid_prefix: bool
    price: float
    flow: tuple | None = None
    metric: tuple | None = None
    flow_complete: bool = True


class LegacyMarketSentimentFeatures:
    descriptor = ModuleDescriptor(
        "legacy.mst", "1", frozenset({"OHLCV", "metric", "trade_flow"}), True
    )

    @staticmethod
    def validate_configurations(configurations):
        """Pure collection contract shared by saved settings and actual execution."""
        configurations = tuple(configurations)
        if any(not isinstance(row, SentimentConfiguration) for row in configurations):
            raise ValidationError("typed MST configurations required")
        keys = [(row.interval_id, row.line_index) for row in configurations]
        if (
            not keys
            or len(keys) > 64
            or len(set(keys)) != len(keys)
            or sum(row.nSamples * 5 for row in configurations) > 100000
        ):
            raise ValidationError("unique bounded MST definitions required")
        return configurations

    def __init__(self, instrument, configurations):
        configurations = self.validate_configurations(configurations)
        keys = [(row.interval_id, row.line_index) for row in configurations]
        self.instrument = instrument
        self._kernels = {
            key: _SentimentKernel(row) for key, row in zip(keys, configurations, strict=True)
        }
        self._buckets = {}
        self._last_frame, self._calculated_asof_ms = None, None
        self._result, self._input_quality = {}, {}

    @property
    def maximum_metric_age_ms(self):
        """Frozen shared selection horizon; each configured line still checks its own age."""
        ages = tuple(kernel.config.maximum_metric_age_ms for kernel in self._kernels.values())
        if not ages or any(type(age) is not int or not 0 <= age <= 86400000 for age in ages):
            raise ValidationError("MST requires its configured native metric age policy")
        return max(ages)

    @property
    def state(self):
        return deepcopy(
            {
                "kernels": {
                    key: {"history": tuple(kernel.history), "previous": kernel.previous}
                    for key, kernel in self._kernels.items()
                },
                "buckets": {key: asdict(value) for key, value in self._buckets.items()},
                "input_quality": self._input_quality,
                "last_frame": self._last_frame,
                "calculated_asof_ms": self._calculated_asof_ms,
            }
        )

    @property
    def input_ready(self):
        return bool(self._input_quality) and all(
            row["ready"] for row in self._input_quality.values()
        )

    @property
    def input_quality(self):
        return deepcopy(self._input_quality)

    @property
    def warmup_alignment_minutes(self):
        return math.lcm(*(INTERVAL_MINUTES[key[0]] for key in self._kernels))

    @staticmethod
    def _flow(frame):
        value = frame.input("trade_flow")
        if value is None:
            return None, "missing"
        if not value.usable_at(frame.as_of_ms):
            return None, value.quality if value.quality != "observed" else "incomplete"
        if not frame.candle.open_time_ms <= value.event_time_ms <= frame.candle.close_time_ms:
            return None, "stale"
        mapping = value.mapping()
        if any(key not in mapping for key in FLOW_FIELDS):
            return None, "missing_fields"
        result = tuple(_number(mapping[key]) for key in FLOW_FIELDS)
        if any(value < 0 for value in result):
            raise ValidationError("negative MST taker notional")
        return result, "observed"

    @staticmethod
    def _metric(frame, maximum_age):
        value = frame.input("metric")
        if value is None:
            return None, "missing", None
        age = frame.feature_time_ms - value.event_time_ms
        if value.event_time_ms > frame.candle.close_time_ms or age > maximum_age:
            return None, "stale", age
        if value.quality not in {"observed", "carried"} or value.coverage != 1 or not value.values:
            return None, value.quality if value.quality != "observed" else "incomplete", age
        mapping = value.mapping()
        result = tuple(
            None if key not in mapping else _number(mapping[key]) for key in METRIC_FIELDS
        )
        if any(value is None for value in result):
            reason = "missing_fields"
        elif any(value <= 0 for value in result):
            reason = "nonpositive"
        else:
            reason = (
                "carried"
                if value.quality == "carried" or value.event_time_ms < frame.candle.open_time_ms
                else "observed"
            )
        return result, reason, age

    @staticmethod
    def _bucket_key(configuration):
        return (
            configuration.interval_id,
            configuration.quote_precision,
            configuration.maximum_metric_age_ms,
        )

    def _quality(self, frame, buckets):
        _, flow_reason = self._flow(frame)
        result = {}
        for (iid, line), kernel in self._kernels.items():
            bucket = buckets[self._bucket_key(kernel.config)]
            _, metric_reason, age = self._metric(frame, kernel.config.maximum_metric_age_ms)
            result[f"{iid}_MST_{line}"] = {
                "ready": bucket.valid_prefix
                and bucket.flow_complete
                and flow_reason == "observed"
                and metric_reason in {"observed", "carried"},
                "metric": metric_reason,
                "metric_age_ms": age,
                "trade_flow": flow_reason
                if flow_reason != "observed" or bucket.flow_complete
                else "incomplete",
                "partial_prefix": not bucket.valid_prefix,
            }
        return result

    def update(self, candle):
        return self.update_frame(MarketFeatureFrame(candle=candle, as_of_ms=candle.close_time_ms))

    def update_frame(self, frame):
        if not isinstance(frame, MarketFeatureFrame):
            raise ValidationError("MST requires a closed market frame")
        candle, previous = frame.candle, self._last_frame
        if (
            type(candle.open_time_ms) is not int
            or type(candle.close_time_ms) is not int
            or candle.open_time_ms % 60000
            or candle.close_time_ms - candle.open_time_ms != 60000
        ):
            raise ValidationError("MST requires exact integer minute times")
        validate_minute(self.instrument, previous.candle if previous else None, candle)
        if previous is not None:
            if frame.as_of_ms < previous.as_of_ms:
                raise ValidationError("MST availability cutoff cannot move backwards")
            if candle == previous.candle:
                if (
                    frame.inputs != previous.inputs
                    or frame.reference_time_ms != previous.reference_time_ms
                ):
                    raise ValidationError("MST requires immutable auxiliary observations")
                quality = self._quality(frame, self._buckets)
                self._last_frame, self._input_quality = frame, quality
                return dict(self._result)
        kernels, buckets = deepcopy(self._kernels), deepcopy(self._buckets)
        flow, _ = self._flow(frame)
        price = _number(candle.close)
        if price <= 0:
            raise ValidationError("MST legacy price must be positive")
        for kernel in kernels.values():
            key = self._bucket_key(kernel.config)
            if key in buckets and buckets[key].open_time_ms == candle.open_time_ms // (
                INTERVAL_MINUTES[key[0]] * 60000
            ) * (INTERVAL_MINUTES[key[0]] * 60000):
                continue  # Every shared bucket is updated below, once.
            duration = INTERVAL_MINUTES[key[0]] * 60000
            opened = candle.open_time_ms // duration * duration
            buckets[key] = _SentimentBucket(opened, candle.open_time_ms == opened, price)
        for key, bucket in buckets.items():
            bucket.price = price
            bucket.flow_complete = bucket.flow_complete and flow is not None
            if flow is not None:
                bucket.flow = (
                    flow
                    if bucket.flow is None
                    else tuple(
                        round(_finite(old + new), key[1])
                        for old, new in zip(bucket.flow, flow, strict=True)
                    )
                )
            metric, _, _ = self._metric(frame, key[2])
            if metric is not None:
                bucket.metric = metric
        result = {}
        for (iid, line), kernel in kernels.items():
            bucket = buckets[self._bucket_key(kernel.config)]
            # An incomplete trade sum is unknown, never a smaller observed total.
            effective_flow = bucket.flow if bucket.flow_complete else None
            values = (
                kernel.preview(bucket.price, bucket.metric, effective_flow)
                if bucket.valid_prefix
                else None
            )
            for suffix, key in OUTPUTS.items():
                result[f"{iid}_MST_{line}_{suffix}"] = values[key] if values else None
            closed = (
                bucket.valid_prefix
                and candle.close_time_ms == bucket.open_time_ms + INTERVAL_MINUTES[iid] * 60000
            )
            result[f"CLOSED_{iid}"] = closed
            if closed:
                kernel.commit(values)
        quality = self._quality(frame, buckets)
        self._kernels, self._buckets = kernels, buckets
        self._last_frame, self._calculated_asof_ms = frame, frame.as_of_ms
        self._result, self._input_quality = result, quality
        return dict(result)

    def checkpoint(self):
        from omega.analysis.checkpoint import checkpoint

        return checkpoint(self)

    def restored(self, state, instrument):
        from omega.analysis.checkpoint import restore

        return restore(self, state, instrument)

    def validate_checkpoint(self):
        for kernel in self._kernels.values():
            kernel.validate_checkpoint()
        if not all(
            isinstance(value, dict) for value in (self._buckets, self._result, self._input_quality)
        ):
            raise ValidationError("invalid MST provider checkpoint mappings")
        if self._last_frame is None:
            if (
                self._calculated_asof_ms is not None
                or self._buckets
                or self._result
                or self._input_quality
                or any(kernel.history or kernel.previous for kernel in self._kernels.values())
            ):
                raise ValidationError("inconsistent initial MST checkpoint")
            return
        if (
            not isinstance(self._last_frame, MarketFeatureFrame)
            or type(self._calculated_asof_ms) is not int
            or not self._last_frame.candle.close_time_ms
            <= self._calculated_asof_ms
            <= self._last_frame.as_of_ms
        ):
            raise ValidationError("invalid MST checkpoint availability cursor")
        frame = replace(self._last_frame, as_of_ms=self._calculated_asof_ms)
        candle = frame.candle
        if (
            candle.instrument != self.instrument
            or type(candle.open_time_ms) is not int
            or type(candle.close_time_ms) is not int
            or candle.open_time_ms % 60000
            or candle.close_time_ms - candle.open_time_ms != 60000
        ):
            raise ValidationError("invalid MST checkpoint minute cursor")
        if set(self._buckets) != {
            self._bucket_key(kernel.config) for kernel in self._kernels.values()
        }:
            raise ValidationError("MST checkpoint bucket configuration mismatch")
        flow, _ = self._flow(frame)
        for key, bucket in self._buckets.items():
            duration = INTERVAL_MINUTES[key[0]] * 60000
            if (
                type(key) is not tuple
                or len(key) != 3
                or any(type(part) is not int for part in key)
                or not isinstance(bucket, _SentimentBucket)
                or type(bucket.open_time_ms) is not int
                or bucket.open_time_ms != candle.open_time_ms // duration * duration
                or type(bucket.valid_prefix) is not bool
                or type(bucket.flow_complete) is not bool
                or type(bucket.price) not in {int, float}
                or not math.isfinite(bucket.price)
                or bucket.price != float(candle.close)
                or bucket.price <= 0
            ):
                raise ValidationError("invalid MST bucket cursor checkpoint")
            for values, allow_missing in ((bucket.flow, False), (bucket.metric, True)):
                if values is not None and (
                    type(values) is not tuple
                    or len(values) != 2
                    or any(
                        (value is None and not allow_missing)
                        or (
                            value is not None
                            and (
                                type(value) not in {int, float}
                                or not math.isfinite(value)
                                or not allow_missing
                                and value < 0
                            )
                        )
                        for value in values
                    )
                ):
                    raise ValidationError("invalid MST bucket values")
            metric, _, _ = self._metric(frame, key[2])
            if (
                bucket.flow_complete
                and (flow is None or bucket.flow is None)
                or metric is not None
                and metric != bucket.metric
                or candle.open_time_ms == bucket.open_time_ms
                and (bucket.flow != flow or bucket.metric != metric)
            ):
                raise ValidationError("MST bucket contradicts its final observation")
        public = set()
        for (iid, line), kernel in self._kernels.items():
            bucket = self._buckets[self._bucket_key(kernel.config)]
            closed = (
                bucket.valid_prefix
                and candle.close_time_ms == bucket.open_time_ms + INTERVAL_MINUTES[iid] * 60000
            )
            if not bucket.valid_prefix:
                if kernel.history or kernel.previous:
                    raise ValidationError("partial initial MST bucket contains closed state")
                values = None
            elif closed:
                values = kernel.previous
                effective_flow = bucket.flow if bucket.flow_complete else None
                if (
                    not values
                    or values["PRICE"] != bucket.price
                    or (values["NB"], values["NS"]) != (effective_flow or (None, None))
                ):
                    raise ValidationError("MST closed state disagrees with its bucket")
                if bucket.metric is not None and any(
                    raw is not None and raw > 0 and values[name] != raw
                    for name, raw in zip(("OI", "LS"), bucket.metric, strict=True)
                ):
                    raise ValidationError("MST closed last-positive metrics disagree")
            else:
                values = kernel.preview(
                    bucket.price, bucket.metric, bucket.flow if bucket.flow_complete else None
                )
            for suffix, name in OUTPUTS.items():
                key = f"{iid}_MST_{line}_{suffix}"
                public.add(key)
                value = self._result.get(key)
                if (
                    value is not None
                    and (type(value) not in {int, float} or not math.isfinite(value))
                ) or value != (values[name] if values else None):
                    raise ValidationError("invalid MST cached public output")
            public.add(f"CLOSED_{iid}")
            if (
                type(self._result.get(f"CLOSED_{iid}")) is not bool
                or self._result[f"CLOSED_{iid}"] != closed
            ):
                raise ValidationError("invalid MST closure checkpoint")
        expected_quality = self._quality(self._last_frame, self._buckets)
        if (
            set(self._result) != public
            or self._input_quality != expected_quality
            or any(
                type(row.get("ready")) is not bool
                or type(row.get("partial_prefix")) is not bool
                or row.get("metric_age_ms") is not None
                and type(row["metric_age_ms"]) is not int
                for row in self._input_quality.values()
            )
        ):
            raise ValidationError("invalid MST quality checkpoint")
