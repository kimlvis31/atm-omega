"""Eta rolling SMA/WMA and selected-center Bollinger windows.

Float summation order, sliding state and rounded-center deviation preserve the
original calculation. Higher-bar previews never commit an unfinished bucket.
"""

# Imports
import math
from collections import deque
from copy import deepcopy
from dataclasses import dataclass

from omega.analysis.aggregation import (
    INTERVAL_MINUTES,
    AggregatedBar,
    CandleAggregation,
    bars_from_dependencies,
    validate_minute,
)
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Candle
from omega.ports.modules import ModuleDescriptor


def _configuration(interval, samples, line, precision):
    if (
        type(interval) is not int
        or interval not in INTERVAL_MINUTES
        or type(samples) is not int
        or not 2 <= samples <= 20000
        or type(line) is not int
        or not 0 <= line <= 9
        or type(precision) is not int
        or not 0 <= precision <= 16
    ):
        raise ValidationError("invalid bounded price-window configuration")


@dataclass(frozen=True)
class WindowAverageConfiguration:
    interval_id: int
    analysis_code: str
    nSamples: int = 10
    line_index: int = 0
    price_precision: int = 8

    def __post_init__(self):
        _configuration(self.interval_id, self.nSamples, self.line_index, self.price_precision)
        if type(self.analysis_code) is not str or self.analysis_code not in {"SMA", "WMA"}:
            raise ValidationError("window average must select SMA or WMA")


@dataclass(frozen=True)
class BollingerConfiguration:
    interval_id: int
    nSamples: int = 10
    MAType: str = "SMA"
    bandWidth: float = 2.0
    line_index: int = 0
    price_precision: int = 8

    def __post_init__(self):
        _configuration(self.interval_id, self.nSamples, self.line_index, self.price_precision)
        if (
            type(self.MAType) is not str
            or self.MAType not in {"SMA", "WMA", "EMA"}
            or type(self.bandWidth) not in {int, float}
        ):
            raise ValidationError("invalid Bollinger center or finite signed width")
        try:
            finite = math.isfinite(self.bandWidth)
        except OverflowError:
            finite = False
        if not finite:
            raise ValidationError("invalid Bollinger center or finite signed width")


def _code(configuration):
    return "BOL" if type(configuration) is BollingerConfiguration else configuration.analysis_code


def _finite(value):
    if not math.isfinite(value):
        raise ValidationError("price-window legacy arithmetic overflow")
    return value


def _close(value):
    close = float(value)
    if not math.isfinite(close) or close <= 0:
        raise ValidationError("price-window close must be a positive finite legacy float")
    return close


def _calculate(config, closes, previous, close):
    """One actual legacy step; prior closes are chronological, prices newest first."""
    samples, precision = config.nSamples, config.price_precision
    code = _code(config)
    prices = [close, *list(reversed(closes))[: samples - 1]]
    seeded = len(prices) == samples
    selected = config.MAType if code == "BOL" else code
    ready = previous.get("mode") == 1 if code == "BOL" else previous.get("fullCompute") is False
    simple = weighted = average = None
    if ready:
        if selected == "EMA":
            kValue = 2 / (samples + 1)
            average = round(_finite(close * kValue + previous["MA"] * (1 - kValue)), precision)
        else:
            if len(closes) < samples:
                raise ValidationError("price-window recurrence lacks an expired close")
            if code == "BOL":
                prior = previous["MACOMPUTATION"]
                prior_simple = prior[0] if selected == "WMA" else prior
                prior_weighted = prior[1] if selected == "WMA" else None
            else:
                prior_simple = previous["PRICESUM_SIMPLE" if code == "WMA" else "PRICESUM"]
                prior_weighted = previous.get("PRICESUM_WEIGHTED")
            simple = _finite(prior_simple - closes[-samples] + close)
            if selected == "WMA":
                weighted = _finite(prior_weighted - prior_simple + samples * close)
                average = round(_finite(weighted / (samples * (samples + 1) / 2)), precision)
            else:
                average = round(_finite(simple / samples), precision)
    elif seeded:
        simple = _finite(sum(prices))
        if selected == "WMA":
            weighted = _finite(sum(price * (samples - index) for index, price in enumerate(prices)))
            average = round(_finite(weighted / (samples * (samples + 1) / 2)), precision)
        else:
            average = round(_finite(simple / samples), precision)
    if code == "SMA":
        return {"PRICESUM": simple, "SMA": average, "fullCompute": not seeded}
    if code == "WMA":
        return {
            "PRICESUM_SIMPLE": simple,
            "PRICESUM_WEIGHTED": weighted,
            "WMA": average,
            "fullCompute": not seeded,
        }
    computation = (simple, weighted) if selected == "WMA" else simple if selected == "SMA" else None
    bands = None
    if seeded:
        # Eta uses deviation from its selected, already-rounded MA (not a
        # fresh unrounded simple mean). Signed width keeps named inversion.
        dsSum = _finite(sum(math.pow(price - average, 2) for price in prices))
        sd = math.sqrt(dsSum / samples)
        bands = (
            round(_finite(average - sd * config.bandWidth), precision),
            round(_finite(average + sd * config.bandWidth), precision),
        )
    return {"MACOMPUTATION": computation, "MA": average, "BOL": bands, "mode": int(seeded)}


def _raw_result(config, closes, previous, close):
    try:
        return _calculate(config, closes, previous, close)
    except (ArithmeticError, ValueError) as exc:
        raise ValidationError("price-window legacy arithmetic overflow") from exc


def _public(config, raw):
    code = _code(config)
    prefix = f"{config.interval_id}_{code}_{config.line_index}_"
    if code != "BOL":
        return {prefix + code: None if raw is None else raw[code]}
    bands = None if raw is None else raw["BOL"]
    return {
        prefix + "BOLLOW": None if bands is None else bands[0],
        prefix + "BOLHIGH": None if bands is None else bands[1],
        prefix + "MA": None if bands is None else raw["MA"],
    }


def _validate_raw(raw, config, count):
    code = _code(config)
    fields = {
        "SMA": {"PRICESUM", "SMA", "fullCompute"},
        "WMA": {"PRICESUM_SIMPLE", "PRICESUM_WEIGHTED", "WMA", "fullCompute"},
        "BOL": {"MACOMPUTATION", "MA", "BOL", "mode"},
    }[code]
    if type(raw) is not dict or set(raw) != fields:
        raise ValidationError("invalid price-window raw fields")
    seeded = count >= config.nSamples
    if code == "BOL":
        if type(raw["mode"]) is not int or raw["mode"] != int(seeded):
            raise ValidationError("inconsistent Bollinger warmup mode")
        computation = raw["MACOMPUTATION"]
        if config.MAType == "WMA":
            if type(computation) is not tuple or len(computation) != 2:
                raise ValidationError("invalid Bollinger weighted state")
            values = [*computation]
        elif config.MAType == "EMA":
            if computation is not None:
                raise ValidationError("Bollinger EMA has no sum state")
            values = []
        else:
            values = [computation]
        bands = raw["BOL"]
        if seeded:
            if type(bands) is not tuple or len(bands) != 2:
                raise ValidationError("invalid Bollinger band state")
            values.extend(bands)
        elif bands is not None:
            raise ValidationError("unseeded Bollinger bands must be absent")
        values.append(raw["MA"])
    else:
        if type(raw["fullCompute"]) is not bool or raw["fullCompute"] is seeded:
            raise ValidationError("inconsistent price-window warmup state")
        values = [value for key, value in raw.items() if key != "fullCompute"]
    if any(
        (type(value) is not float or not math.isfinite(value)) if seeded else value is not None
        for value in values
    ):
        raise ValidationError("invalid finite price-window raw values")


class _WindowKernel:
    def __init__(self, configuration):
        self.config = configuration
        self.closes = deque(maxlen=configuration.nSamples + 1)
        self.history = deque(maxlen=2)
        self.previous = {}
        self.count = 0
        self.first_open_ms = None

    def preview(self, frame):
        return _raw_result(self.config, self.closes, self.previous, _close(frame.close))

    def commit(self, frame, raw):
        if not self.count:
            self.first_open_ms = frame.open_time_ms
        self.closes.append(_close(frame.close))
        self.history.append(deepcopy(raw))
        self.previous = deepcopy(raw)
        self.count += 1

    def validate_checkpoint(self):
        if (
            type(self.count) is not int
            or not 0 <= self.count <= 2**63 - 1
            or not isinstance(self.closes, deque)
            or self.closes.maxlen != self.config.nSamples + 1
            or len(self.closes) != min(self.count, self.closes.maxlen)
            or any(
                type(value) is not float or not math.isfinite(value) or value <= 0
                for value in self.closes
            )
            or not isinstance(self.history, deque)
            or self.history.maxlen != 2
            or len(self.history) != min(self.count, 2)
            or type(self.previous) is not dict
        ):
            raise ValidationError("invalid price-window committed history")
        if not self.count:
            if self.previous or self.first_open_ms is not None:
                raise ValidationError("fresh price-window contains a result")
            return
        if (
            type(self.first_open_ms) is not int
            or self.first_open_ms < 0
            or self.first_open_ms % (INTERVAL_MINUTES[self.config.interval_id] * 60000)
        ):
            raise ValidationError("invalid price-window first committed cursor")
        if self.previous != self.history[-1]:
            raise ValidationError("price-window committed tail differs from result")
        for index, raw in enumerate(self.history, start=self.count - len(self.history) + 1):
            _validate_raw(raw, self.config, index)
        prior = self.history[-2] if len(self.history) == 2 else {}
        expected = _raw_result(self.config, list(self.closes)[:-1], prior, self.closes[-1])
        if expected != self.previous:
            raise ValidationError("price-window committed recurrence is inconsistent")


class LegacyPriceWindowFeatures:
    descriptor = ModuleDescriptor("legacy.price_windows", "1", frozenset({"OHLCV"}), True)
    uses_shared_aggregation = True

    @staticmethod
    def validate_configurations(configurations):
        configurations = tuple(configurations)
        if any(
            type(row) not in {WindowAverageConfiguration, BollingerConfiguration}
            for row in configurations
        ):
            raise ValidationError("typed price-window configurations required")
        identities = [(row.interval_id, _code(row), row.line_index) for row in configurations]
        if (
            not identities
            or len(identities) > 64
            or len(set(identities)) != len(identities)
            or sum(row.nSamples + 3 for row in configurations) > 100000
        ):
            raise ValidationError("unique bounded price-window configurations required")
        return configurations

    def __init__(self, instrument, configurations):
        configurations = self.validate_configurations(configurations)
        self.instrument = instrument
        self._kernels = {
            (row.interval_id, _code(row), row.line_index): _WindowKernel(row)
            for row in configurations
        }
        self._aggregation = CandleAggregation(
            instrument, tuple(sorted({row.interval_id for row in configurations}))
        )
        self._last, self._result, self._raw, self._frames = None, {}, {}, {}

    @property
    def warmup_alignment_minutes(self):
        return self._aggregation.warmup_alignment_minutes

    @property
    def state(self):
        return deepcopy(
            {
                "kernels": {
                    key: {
                        "closes": tuple(kernel.closes),
                        "history": tuple(kernel.history),
                        "previous": kernel.previous,
                        "count": kernel.count,
                        "first_open_ms": kernel.first_open_ms,
                    }
                    for key, kernel in self._kernels.items()
                },
                "raw": self._raw,
                "frames": self._frames,
            }
        )

    def update_frame(self, frame):
        if not isinstance(frame, MarketFeatureFrame):
            raise ValidationError("price windows require a market feature frame")
        return self.update(frame.candle)

    def update(self, candle):
        validate_minute(self.instrument, self._last, candle)
        if candle == self._last:
            return dict(self._result)
        aggregation = deepcopy(self._aggregation)
        result = self._update(candle, aggregation.update(candle), include_closure=True)
        self._aggregation = aggregation
        return result

    def update_with(self, candle, inputs):
        bars = bars_from_dependencies(inputs, candle, {key[0] for key in self._kernels})
        return self._update(
            candle, {f"BAR_{iid}": bar for iid, bar in bars.items()}, include_closure=False
        )

    def _update(self, candle, frames, *, include_closure):
        validate_minute(self.instrument, self._last, candle)
        if candle == self._last:
            return dict(self._result)
        kernels, result, raw, selected = deepcopy(self._kernels), {}, {}, {}
        for key, kernel in kernels.items():
            iid = key[0]
            frame = frames[f"BAR_{iid}"]
            self._validate_frame(frame, iid, candle)
            values = kernel.preview(frame) if frame.valid_prefix else None
            raw[key], selected[iid] = values, frame
            result.update(_public(kernel.config, values))
            if include_closure:
                result[f"CLOSED_{iid}"] = frame.complete
            if frame.complete:
                kernel.commit(frame, values)
        self._kernels, self._last = kernels, candle
        self._result, self._raw, self._frames = result, raw, selected
        return dict(result)

    def _validate_frame(self, frame, iid, candle):
        duration = INTERVAL_MINUTES[iid] * 60000
        if (
            not isinstance(frame, AggregatedBar)
            or frame.instrument != self.instrument
            or type(frame.interval_id) is not int
            or frame.interval_id != iid
            or type(frame.open_time_ms) is not int
            or type(frame.close_time_ms) is not int
            or frame.close_time_ms != candle.close_time_ms
            or frame.open_time_ms != candle.open_time_ms // duration * duration
            or frame.close != candle.close
            or type(frame.valid_prefix) is not bool
            or type(frame.complete) is not bool
            or frame.complete
            != (frame.valid_prefix and candle.close_time_ms == frame.open_time_ms + duration)
        ):
            raise ValidationError("invalid price-window aggregate cursor")

    def validate_checkpoint(self):
        if type(self._kernels) is not dict or not self._kernels:
            raise ValidationError("invalid price-window kernel mapping")
        for key, kernel in self._kernels.items():
            if key != (kernel.config.interval_id, _code(kernel.config), kernel.config.line_index):
                raise ValidationError("price-window configuration identity mismatch")
            kernel.validate_checkpoint()
        if self._last is None:
            if (
                self._result
                or self._raw
                or self._frames
                or any(kernel.count for kernel in self._kernels.values())
            ):
                raise ValidationError("fresh price windows contain historical state")
            return
        intervals = {key[0] for key in self._kernels}
        if (
            not isinstance(self._last, Candle)
            or self._last.instrument != self.instrument
            or type(self._raw) is not dict
            or set(self._raw) != set(self._kernels)
            or type(self._frames) is not dict
            or set(self._frames) != intervals
        ):
            raise ValidationError("invalid price-window cursor/raw mapping")
        expected, closures = {}, {}
        for key, kernel in self._kernels.items():
            frame, raw = self._frames[key[0]], self._raw[key]
            self._validate_frame(frame, key[0], self._last)
            if kernel.count and kernel.count != (
                (frame.open_time_ms - kernel.first_open_ms) // (INTERVAL_MINUTES[key[0]] * 60000)
                + int(frame.complete)
            ):
                raise ValidationError("price-window committed count differs from its cursor")
            if not frame.valid_prefix:
                if raw is not None or kernel.count:
                    raise ValidationError("initial partial prefix has a committed result")
            elif frame.complete:
                if (
                    not kernel.count
                    or raw != kernel.previous
                    or kernel.closes[-1] != _close(frame.close)
                ):
                    raise ValidationError(
                        "closed price-window preview differs from committed result"
                    )
            elif raw != kernel.preview(frame):
                raise ValidationError("price-window preview differs from its closed history")
            if raw is not None:
                _validate_raw(raw, kernel.config, kernel.count + int(not frame.complete))
            expected.update(_public(kernel.config, raw))
            closures[f"CLOSED_{key[0]}"] = frame.complete
        if (
            type(self._result) is not dict
            or set(self._result) - set(closures) != set(expected)
            or any(
                type(self._result[key]) is not type(value) or self._result[key] != value
                for key, value in expected.items()
            )
            or set(self._result) & set(closures) not in (set(), set(closures))
            or any(
                type(self._result[key]) is not bool or self._result[key] is not value
                for key, value in closures.items()
                if key in self._result
            )
        ):
            raise ValidationError("price-window public values differ from raw state")

    def checkpoint(self):
        from omega.analysis.checkpoint import checkpoint

        return checkpoint(self)

    def restored(self, value, instrument):
        from omega.analysis.checkpoint import restore

        return restore(self, value, instrument)
