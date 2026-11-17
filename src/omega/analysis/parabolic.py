"""Eta PSAR arithmetic with causal higher-bar previews and bounded closed state."""

# Imports
import math
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal

from omega.analysis.aggregation import (
    INTERVAL_MINUTES,
    CandleAggregation,
    bars_from_dependencies,
    validate_minute,
)
from omega.domain.errors import ValidationError
from omega.ports.modules import ModuleDescriptor

# Constants
_RESULT_KEYS = {"PD", "PDReversed", "AF", "EP", "PSAR", "DCC", "mode"}


def _finite(value):
    if not math.isfinite(value):
        raise ValidationError("PSAR legacy arithmetic overflow")
    return value


@dataclass(frozen=True)
class ParabolicConfiguration:
    interval_id: int
    line_index: int = 0
    start: float = 0.02
    acceleration: float = 0.005
    maximum: float = 0.2
    price_precision: int = 8

    def __post_init__(self):
        if (
            type(self.interval_id) is not int
            or not 0 <= self.interval_id <= 11
            or self.interval_id not in INTERVAL_MINUTES
            or type(self.line_index) is not int
            or not 0 <= self.line_index < 10
            or type(self.price_precision) is not int
            or not 0 <= self.price_precision <= 16
        ):
            raise ValidationError("invalid PSAR interval, line or precision")
        for name in ("start", "acceleration", "maximum"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
                raise ValidationError(f"PSAR {name} must be a finite positive number")
            try:
                value = float(value)
            except (ValueError, TypeError, OverflowError):
                raise ValidationError(f"invalid PSAR {name}") from None
            if not math.isfinite(value) or value <= 0:
                raise ValidationError(f"PSAR {name} must be finite and positive")
            object.__setattr__(self, name, value)
        if not self.start < self.maximum:
            raise ValidationError("PSAR start must be less than maximum")


class _ParabolicKernel:
    def __init__(self, configuration):
        self.config = configuration
        self.history = deque(maxlen=2)
        self.previous = {}

    def preview(self, frame):
        try:
            high, low = float(frame.high), float(frame.low)
        except (ValueError, TypeError, OverflowError):
            raise ValidationError("PSAR prices must be finite positive legacy floats") from None
        if not all(math.isfinite(value) and value > 0 for value in (high, low)) or low > high:
            raise ValidationError("PSAR prices must be finite positive legacy floats")
        previous = self.previous
        mode = previous.get("mode", 0)
        # [1]: Eta seeds direction from two bars; the first PSAR appears on bar three.
        if mode == 0:
            if not self.history:
                return {
                    "PD": None,
                    "PDReversed": False,
                    "AF": None,
                    "EP": None,
                    "PSAR": None,
                    "DCC": 0,
                    "mode": 0,
                }
            previous_high, previous_low = self.history[-1]
            high_delta = high - previous_high if previous_high <= high else 0
            low_delta = previous_low - low if low <= previous_low else 0
            return {
                "PD": low_delta <= high_delta,
                "PDReversed": False,
                "AF": None,
                "EP": None,
                "PSAR": previous.get("PSAR"),
                "DCC": 0,
                "mode": 1,
            }
        if len(self.history) != 2:
            raise ValidationError("PSAR checkpoint lacks two closed reference bars")
        high_prev2, low_prev2 = self.history[-2]
        high_prev1, low_prev1 = self.history[-1]
        if mode == 1:
            pd = previous["PD"]
            return {
                "PD": pd,
                "PDReversed": False,
                "AF": self.config.start,
                "EP": max(high_prev1, high_prev2) if pd else min(low_prev1, low_prev2),
                "PSAR": min(low_prev1, low_prev2) if pd else max(high_prev1, high_prev2),
                "DCC": 0,
                "mode": 2,
            }
        if mode != 2:
            raise ValidationError("invalid PSAR computation mode")
        # [2]: Preserve original binary-float arithmetic, rounding and strict reversal.
        psar = round(
            _finite(previous["PSAR"] + previous["AF"] * (previous["EP"] - previous["PSAR"])),
            self.config.price_precision,
        )
        if previous["PD"]:
            psar = min(psar, low_prev1, low_prev2)
            reversed_direction = low < psar
            new_extreme = previous["EP"] < high
            ep = high if new_extreme else previous["EP"]
        else:
            psar = max(psar, high_prev1, high_prev2)
            reversed_direction = psar < high
            new_extreme = low < previous["EP"]
            ep = low if new_extreme else previous["EP"]
        af = (
            min(self.config.maximum, _finite(previous["AF"] + self.config.acceleration))
            if new_extreme
            else previous["AF"]
        )
        if reversed_direction:
            pd = not previous["PD"]
            af = self.config.start
            ep = high if pd else low
            dcc = 0
            psar = previous["EP"]
        else:
            pd = previous["PD"]
            dcc = previous["DCC"] + 1
        return {
            "PD": pd,
            "PDReversed": reversed_direction,
            "AF": af,
            "EP": ep,
            "PSAR": _finite(psar),
            "DCC": dcc,
            "mode": 2,
        }

    def commit(self, frame, result):
        self.history.append((float(frame.high), float(frame.low)))
        self.previous = dict(result)

    def validate_checkpoint(self):
        if not isinstance(self.history, deque) or self.history.maxlen != 2:
            raise ValidationError("invalid PSAR history checkpoint")
        for row in self.history:
            if (
                not isinstance(row, tuple)
                or len(row) != 2
                or any(
                    type(value) not in {int, float} or not math.isfinite(value) or value <= 0
                    for value in row
                )
                or row[1] > row[0]
            ):
                raise ValidationError("invalid PSAR reference price checkpoint")
        previous = self.previous
        if previous == {} and not self.history:
            return
        if (
            not isinstance(previous, dict)
            or set(previous) != _RESULT_KEYS
            or type(previous["mode"]) is not int
            or previous["mode"] not in {0, 1, 2}
            or type(previous["DCC"]) is not int
            or previous["DCC"] < 0
            or type(previous["PDReversed"]) is not bool
        ):
            raise ValidationError("invalid PSAR result checkpoint")
        mode = previous["mode"]
        if len(self.history) != (1 if mode == 0 else 2):
            raise ValidationError("PSAR checkpoint mode/history mismatch")
        if mode < 2:
            if (
                any(previous[name] is not None for name in ("AF", "EP", "PSAR"))
                or previous["DCC"] != 0
                or previous["PDReversed"]
                or (previous["PD"] is not None if mode == 0 else type(previous["PD"]) is not bool)
            ):
                raise ValidationError("invalid PSAR warmup checkpoint")
        elif (
            type(previous["PD"]) is not bool
            or any(
                type(previous[name]) not in {int, float} or not math.isfinite(previous[name])
                for name in ("AF", "EP", "PSAR")
            )
            or not self.config.start <= previous["AF"] <= self.config.maximum
            or previous["EP"] <= 0
        ):
            raise ValidationError("invalid PSAR active checkpoint")


class LegacyParabolicFeatures:
    """Preview this minute's higher aggregate; commit kernel state only at closure."""

    descriptor = ModuleDescriptor("legacy.psar", "1", frozenset({"OHLCV"}), True)

    def __init__(self, instrument, configurations):
        identities = [(row.interval_id, row.line_index) for row in configurations]
        if not identities or len(identities) > 64 or len(set(identities)) != len(identities):
            raise ValidationError("unique bounded PSAR interval/line configurations required")
        self.instrument = instrument
        self._kernels = {
            key: _ParabolicKernel(config)
            for key, config in zip(identities, configurations, strict=True)
        }
        self._aggregation = CandleAggregation(
            instrument, tuple(sorted({key[0] for key in identities}))
        )
        self._last, self._result = None, {}

    @property
    def state(self):
        return deepcopy(
            {
                key: {"history": tuple(kernel.history), "previous": kernel.previous}
                for key, kernel in self._kernels.items()
            }
        )

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
        kernels, result = deepcopy(self._kernels), {}
        for (iid, line), kernel in kernels.items():
            frame = frames[f"BAR_{iid}"]
            values = kernel.preview(frame) if frame.valid_prefix else None
            prefix = f"{iid}_PSAR_{line}_"
            psar = values["PSAR"] if values else None
            result[prefix + "PSAR"] = psar
            result[prefix + "DCC"] = values["DCC"] if psar is not None else None
            if include_closure:
                result[f"CLOSED_{iid}"] = frame.complete
            if frame.complete:
                kernel.commit(frame, values)
        self._kernels, self._last, self._result = kernels, candle, result
        return dict(result)

    def checkpoint(self):
        from omega.analysis.checkpoint import checkpoint

        return checkpoint(self)

    def restored(self, state, instrument):
        from omega.analysis.checkpoint import restore

        return restore(self, state, instrument)
