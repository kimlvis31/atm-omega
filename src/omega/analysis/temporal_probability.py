"""Eta TPD signed-return bias/EMA over contiguous, explicitly closed minutes.

The historical name does not imply a probability bounded to [0,1]. Higher-bar
previews use the last committed kernel, and only complete bars advance state.
"""

# Imports
import math
from collections import deque
from copy import deepcopy
from dataclasses import dataclass

from omega.analysis.aggregation import (
    INTERVAL_MINUTES,
    CandleAggregation,
    bars_from_dependencies,
    validate_minute,
)
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Candle
from omega.ports.modules import ModuleDescriptor

RAW_FIELDS = {
    "LASTERM_PD",
    "TERMSUM_INCREMENTAL",
    "TERMSUM_DECREMENTAL",
    "BIAS",
    "TPD",
    "TPD_ABSMA",
    "TPD_ABSMAREL",
    "analysisCount",
}
PUBLIC_FIELDS = ("TPD", "TPDABSMA", "TPDABSMAREL")


def _finite(value):
    if not math.isfinite(value):
        raise ValidationError("TPD legacy arithmetic overflow")
    return value


@dataclass(frozen=True)
class TemporalProbabilityConfiguration:
    interval_id: int
    viewLength: int = 10
    nSamples: int = 100
    nSamplesMA: int = 20
    line_index: int = 0

    def __post_init__(self):
        if (
            type(self.interval_id) is not int
            or self.interval_id not in INTERVAL_MINUTES
            or type(self.line_index) is not int
            or not 0 <= self.line_index <= 9
            or any(
                type(value) is not int or not 2 <= value <= 20000
                for value in (self.viewLength, self.nSamples, self.nSamplesMA)
            )
        ):
            raise ValidationError("invalid bounded TPD configuration")


class _TemporalKernel:
    def __init__(self, configuration):
        self.config = configuration
        self.closes = deque(maxlen=configuration.viewLength + 1)
        self.history = deque(maxlen=max(configuration.nSamples, configuration.nSamplesMA) + 1)
        self.previous = {}

    def preview(self, frame):
        close = float(frame.close)
        if not math.isfinite(close) or close <= 0:
            raise ValidationError("TPD close must be a positive finite legacy float")
        config, previous = self.config, self.previous
        count = previous.get("analysisCount", -1) + 1
        change = (
            _finite(close / self.closes[-config.viewLength] - 1)
            if len(self.closes) >= config.viewLength
            else None
        )
        incremental = previous.get("TERMSUM_INCREMENTAL", 0)
        decremental = previous.get("TERMSUM_DECREMENTAL", 0)
        if change is not None:
            if change < 0:
                decremental += abs(change)
            elif change > 0:
                incremental += abs(change)
        expired = (
            self.history[-config.nSamples]["LASTERM_PD"]
            if len(self.history) >= config.nSamples
            else None
        )
        if expired is not None:
            if expired < 0:
                decremental -= abs(expired)
            elif expired > 0:
                incremental -= abs(expired)
        _finite(incremental)
        _finite(decremental)
        bias = (
            _finite((incremental - decremental) / config.nSamples)
            if count >= config.viewLength + config.nSamples - 1
            else None
        )
        first_tpd = config.viewLength + config.nSamples + config.nSamplesMA - 2
        value = None
        kValueMA = 2 / (config.nSamplesMA + 1)
        if count == first_tpd:
            # Eta's timestamp helper returns newest-to-oldest, then adds today.
            biasSum = (
                sum(row["BIAS"] for row in list(reversed(self.history))[: config.nSamplesMA - 1])
                + bias
            )
            value = round(_finite(biasSum / config.nSamplesMA), 5)
        elif count > first_tpd:
            value = round(_finite(bias * kValueMA + previous["TPD"] * (1 - kValueMA)), 5)
        absolute = None
        if value is not None and previous.get("TPD") is not None:
            absoluteMA_kValue = 2 / (config.nSamplesMA * 10 + 1)
            prior = previous["TPD_ABSMA"]
            absolute = _finite(
                abs(value) * absoluteMA_kValue
                + (abs(previous["TPD"]) if prior is None else prior) * (1 - absoluteMA_kValue)
            )
        relative = (
            None if absolute is None else round(_finite(value / absolute), 5) if absolute else 0.0
        )
        return {
            "LASTERM_PD": change,
            "TERMSUM_INCREMENTAL": incremental,
            "TERMSUM_DECREMENTAL": decremental,
            "BIAS": bias,
            "TPD": value,
            "TPD_ABSMA": absolute,
            "TPD_ABSMAREL": relative,
            "analysisCount": count,
        }

    def commit(self, frame, result):
        self.closes.append(float(frame.close))
        self.history.append(dict(result))
        self.previous = dict(result)

    def validate_checkpoint(self):
        config = self.config
        if (
            not isinstance(self.closes, deque)
            or self.closes.maxlen != config.viewLength + 1
            or any(
                type(value) is not float or not math.isfinite(value) or value <= 0
                for value in self.closes
            )
            or not isinstance(self.history, deque)
            or self.history.maxlen != max(config.nSamples, config.nSamplesMA) + 1
            or type(self.previous) is not dict
        ):
            raise ValidationError("invalid TPD checkpoint history")
        if not self.previous:
            if self.closes or self.history:
                raise ValidationError("TPD history lacks a committed result")
            return
        rows = list(self.history)
        if not rows or rows[-1] != self.previous:
            raise ValidationError("TPD checkpoint tail differs from its result")
        for index, row in enumerate(rows):
            _validate_raw(row, config)
            if index and row["analysisCount"] != rows[index - 1]["analysisCount"] + 1:
                raise ValidationError("TPD historical counters are not consecutive")
        count = self.previous["analysisCount"]
        if len(rows) != min(count + 1, self.history.maxlen) or len(self.closes) != min(
            count + 1, self.closes.maxlen
        ):
            raise ValidationError("TPD checkpoint history length differs from its counter")
        if len(self.closes) > config.viewLength:
            expected = self.closes[-1] / self.closes[-config.viewLength - 1] - 1
            if self.previous["LASTERM_PD"] != expected:
                raise ValidationError("TPD checkpoint return differs from its closes")
        if len(rows) >= 2:
            previous, current = rows[-2:]
            for field, negative in (("TERMSUM_INCREMENTAL", False), ("TERMSUM_DECREMENTAL", True)):
                expected = previous[field]
                change = current["LASTERM_PD"]
                if change is not None and (change < 0 if negative else change > 0):
                    expected += abs(change)
                expired = (
                    rows[-config.nSamples - 1]["LASTERM_PD"]
                    if len(rows) > config.nSamples
                    else None
                )
                if expired is not None and (expired < 0 if negative else expired > 0):
                    expected -= abs(expired)
                if current[field] != expected:
                    raise ValidationError("TPD checkpoint sliding sum is inconsistent")
            threshold = config.viewLength + config.nSamples + config.nSamplesMA - 2
            if count >= threshold:
                if count == threshold:
                    expected = round(
                        (
                            sum(row["BIAS"] for row in reversed(rows[-config.nSamplesMA : -1]))
                            + current["BIAS"]
                        )
                        / config.nSamplesMA,
                        5,
                    )
                else:
                    kValue = 2 / (config.nSamplesMA + 1)
                    expected = round(current["BIAS"] * kValue + previous["TPD"] * (1 - kValue), 5)
                if current["TPD"] != expected:
                    raise ValidationError("TPD checkpoint EMA is inconsistent")
                if previous["TPD"] is not None:
                    kValue = 2 / (config.nSamplesMA * 10 + 1)
                    prior = previous["TPD_ABSMA"]
                    expected = abs(current["TPD"]) * kValue + (
                        abs(previous["TPD"]) if prior is None else prior
                    ) * (1 - kValue)
                    if current["TPD_ABSMA"] != expected:
                        raise ValidationError("TPD checkpoint absolute EMA is inconsistent")


def _validate_raw(row, config):
    if (
        type(row) is not dict
        or set(row) != RAW_FIELDS
        or type(row["analysisCount"]) is not int
        or not 0 <= row["analysisCount"] <= 2**63 - 1
        or any(
            value is not None
            and (
                not (
                    type(value) is float
                    or type(value) is int
                    and key in {"TERMSUM_INCREMENTAL", "TERMSUM_DECREMENTAL"}
                    and value == 0
                )
                or not math.isfinite(value)
            )
            for key, value in row.items()
            if key != "analysisCount"
        )
        or any(row[key] is None for key in ("TERMSUM_INCREMENTAL", "TERMSUM_DECREMENTAL"))
    ):
        raise ValidationError("invalid TPD raw checkpoint")
    count = row["analysisCount"]
    first_bias = config.viewLength + config.nSamples - 1
    first_tpd = first_bias + config.nSamplesMA - 1
    if (
        (row["LASTERM_PD"] is None) != (count < config.viewLength)
        or (row["BIAS"] is None) != (count < first_bias)
        or (row["TPD"] is None) != (count < first_tpd)
        or (row["TPD_ABSMA"] is None) != (count <= first_tpd)
        or (row["TPD_ABSMAREL"] is None) != (count <= first_tpd)
    ):
        raise ValidationError("TPD checkpoint warmup phase is inconsistent")
    if count < config.viewLength and any(
        row[key] != 0 for key in ("TERMSUM_INCREMENTAL", "TERMSUM_DECREMENTAL")
    ):
        raise ValidationError("TPD unseeded sums must be zero")
    if (
        row["BIAS"] is not None
        and row["BIAS"]
        != (row["TERMSUM_INCREMENTAL"] - row["TERMSUM_DECREMENTAL"]) / config.nSamples
    ):
        raise ValidationError("TPD checkpoint bias is inconsistent")
    absolute = row["TPD_ABSMA"]
    if absolute is not None and (
        absolute < 0
        or row["TPD_ABSMAREL"] != (round(row["TPD"] / absolute, 5) if absolute else 0.0)
    ):
        raise ValidationError("TPD checkpoint relative value is inconsistent")


class LegacyTemporalProbabilityFeatures:
    descriptor = ModuleDescriptor("legacy.temporal_probability", "1", frozenset({"OHLCV"}), True)

    @staticmethod
    def validate_configurations(configurations):
        configurations = tuple(configurations)
        if any(not isinstance(row, TemporalProbabilityConfiguration) for row in configurations):
            raise ValidationError("typed TPD configurations required")
        identities = [(row.interval_id, row.line_index) for row in configurations]
        if (
            not identities
            or len(identities) > 64
            or len(set(identities)) != len(identities)
            or sum(row.viewLength + max(row.nSamples, row.nSamplesMA) + 2 for row in configurations)
            > 100000
        ):
            raise ValidationError("unique bounded TPD configurations required")
        return configurations

    def __init__(self, instrument, configurations):
        configurations = self.validate_configurations(configurations)
        self.instrument = instrument
        self._kernels = {
            (row.interval_id, row.line_index): _TemporalKernel(row) for row in configurations
        }
        self._aggregation = CandleAggregation(
            instrument, tuple(sorted({row.interval_id for row in configurations}))
        )
        self._last, self._result, self._raw = None, {}, {}

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
                    }
                    for key, kernel in self._kernels.items()
                },
                "raw": self._raw,
            }
        )

    def update_frame(self, frame):
        if not isinstance(frame, MarketFeatureFrame):
            raise ValidationError("TPD requires a market feature frame")
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
        kernels, result, raw = deepcopy(self._kernels), {}, {}
        for (iid, line), kernel in kernels.items():
            frame = frames[f"BAR_{iid}"]
            values = kernel.preview(frame) if frame.valid_prefix else None
            raw[(iid, line)] = values
            prefix = f"{iid}_TPD_{line}_"
            for public, original in zip(
                PUBLIC_FIELDS, ("TPD", "TPD_ABSMA", "TPD_ABSMAREL"), strict=True
            ):
                result[prefix + public] = None if values is None else values[original]
            if include_closure:
                result[f"CLOSED_{iid}"] = frame.complete
            if frame.complete:
                kernel.commit(frame, values)
        self._kernels, self._last, self._result, self._raw = kernels, candle, result, raw
        return dict(result)

    def validate_checkpoint(self):
        if type(self._kernels) is not dict or not self._kernels:
            raise ValidationError("invalid TPD provider mapping")
        for kernel in self._kernels.values():
            kernel.validate_checkpoint()
        if self._last is None:
            if (
                self._result
                or self._raw
                or any(kernel.previous for kernel in self._kernels.values())
            ):
                raise ValidationError("fresh TPD contains historical state")
            return
        if (
            not isinstance(self._last, Candle)
            or self._last.instrument != self.instrument
            or set(self._raw) != set(self._kernels)
        ):
            raise ValidationError("invalid TPD provider cursor/raw mapping")
        expected, closure_values = {}, {}
        for key, kernel in self._kernels.items():
            raw = self._raw[key]
            boundary = self._last.close_time_ms % (INTERVAL_MINUTES[key[0]] * 60000) == 0
            if raw is not None:
                _validate_raw(raw, kernel.config)
                count = kernel.previous.get("analysisCount", -1)
                if (
                    raw["analysisCount"] != (count if boundary else count + 1)
                    or boundary
                    and raw != kernel.previous
                ):
                    raise ValidationError("TPD preview differs from its committed cursor")
            elif kernel.previous:
                raise ValidationError("TPD preview cannot lose a seeded aggregate prefix")
            closure_values[f"CLOSED_{key[0]}"] = boundary and raw is not None
            prefix = f"{key[0]}_TPD_{key[1]}_"
            expected.update(
                {
                    prefix + public: None if raw is None else raw[original]
                    for public, original in zip(
                        PUBLIC_FIELDS, ("TPD", "TPD_ABSMA", "TPD_ABSMAREL"), strict=True
                    )
                }
            )
        closures = {f"CLOSED_{iid}" for iid, _line in self._kernels}
        if (
            type(self._result) is not dict
            or set(self._result) - closures != set(expected)
            or any(
                type(self._result[key]) is not type(value) or self._result[key] != value
                for key, value in expected.items()
            )
            or set(self._result) & closures not in (set(), closures)
            or any(
                type(value) is not bool or value is not closure_values[key]
                for key, value in self._result.items()
                if key in closures
            )
        ):
            raise ValidationError("TPD public checkpoint differs from its raw values")

    def checkpoint(self):
        from omega.analysis.checkpoint import checkpoint

        return checkpoint(self)

    def restored(self, value, instrument):
        from omega.analysis.checkpoint import restore

        return restore(self, value, instrument)
