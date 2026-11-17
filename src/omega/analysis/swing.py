"""Causal Eta SWING: extreme-first search and strictly confirmed reversal publication."""

# Imports
import math
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


def _finite(value):
    if not math.isfinite(value):
        raise ValidationError("SWING legacy arithmetic overflow")
    return value


@dataclass(frozen=True)
class SwingConfiguration:
    interval_id: int
    line_index: int = 0
    swing_range: float = 0.005

    def __post_init__(self):
        value = self.swing_range
        if (
            type(self.interval_id) is not int
            or self.interval_id not in INTERVAL_MINUTES
            or type(self.line_index) is not int
            or not 0 <= self.line_index < 10
            or isinstance(value, bool)
            or not isinstance(value, (str, int, float, Decimal))
        ):
            raise ValidationError("invalid SWING configuration")
        try:
            value = float(value)
        except (TypeError, ValueError, OverflowError):
            raise ValidationError("invalid SWING range") from None
        if not math.isfinite(value) or value < 0.0001:
            raise ValidationError("SWING range must be finite and at least 0.0001")
        object.__setattr__(self, "swing_range", value)


class _Kernel:
    def __init__(self, configuration):
        self.config = configuration
        self.search = self.confirmed = self.confirmed_at_ms = None

    def preview(self, frame):
        # Eta formulas use binary float. This explicit boundary follows the
        # compatibility arithmetic while domain OHLCV remains exact Decimal.
        high, low = float(frame.high), float(frame.low)
        if not all(math.isfinite(value) and value > 0 for value in (high, low)):
            raise ValidationError("SWING prices cannot be represented as finite legacy floats")
        stamp = frame.open_time_ms
        search, confirmed = deepcopy(self.search), self.confirmed
        if search is None:
            search = {
                "lastExtreme": True,
                "max": high,
                "min": low,
                "max_ts": stamp,
                "min_ts": stamp,
            }
        elif search["lastExtreme"]:
            if low < search["min"]:
                search.update(min=low, min_ts=stamp)
            elif _finite(search["min"] * (1 + self.config.swing_range)) < high:
                confirmed = search["min_ts"], search["min"], -1
                search.update(lastExtreme=False, max=high, max_ts=stamp)
        else:
            if search["max"] < high:
                search.update(max=high, max_ts=stamp)
            elif low < _finite(search["max"] * (1 - self.config.swing_range)):
                confirmed = search["max_ts"], search["max"], 1
                search.update(lastExtreme=True, min=low, min_ts=stamp)
        return search, confirmed


class LegacySwingFeatures:
    """Completed aggregate state plus a causal preview of each available higher bar.

    A missing minute is rejected before mutation. The caller must repair/replay
    contiguous history; no gap seeding or synthesized extreme is performed.
    """

    descriptor = ModuleDescriptor("legacy.swing", "1", frozenset({"OHLCV"}), True)

    def __init__(self, instrument, configurations):
        identities = [(row.interval_id, row.line_index) for row in configurations]
        if not identities or len(identities) > 64 or len(set(identities)) != len(identities):
            raise ValidationError("unique bounded SWING interval/line configurations required")
        self.instrument = instrument
        self._kernels = {
            key: _Kernel(config) for key, config in zip(identities, configurations, strict=True)
        }
        self._aggregation = CandleAggregation(
            instrument, tuple(sorted({key[0] for key in identities}))
        )
        self._last, self._result, self._published = None, {}, {}

    @property
    def state(self):
        return deepcopy(
            {
                key: {
                    "search": kernel.search,
                    "last_confirmed": kernel.confirmed,
                    "confirmed_at_ms": kernel.confirmed_at_ms,
                }
                for key, kernel in self._kernels.items()
            }
        )

    def update(self, candle):
        validate_minute(self.instrument, self._last, candle)
        if candle == self._last:
            return dict(self._result)
        aggregation = deepcopy(self._aggregation)
        frames = aggregation.update(candle)
        result = self._update(candle, frames, include_closure=True)
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
        kernels, published, result = deepcopy(self._kernels), deepcopy(self._published), {}
        for (iid, line), kernel in kernels.items():
            frame = frames[f"BAR_{iid}"]
            prefix = f"{iid}_SWING_{line}_"
            search, confirmed = kernel.preview(frame)
            if not frame.valid_prefix:
                confirmed = None
            previous = published.get((iid, line))
            available_at = None
            if confirmed is not None:
                available_at = (
                    previous[1]
                    if previous and previous[0] == confirmed
                    else kernel.confirmed_at_ms
                    if kernel.confirmed == confirmed
                    else candle.close_time_ms
                )
            result.update(
                {
                    prefix + "LSTIMESTAMP": confirmed[0] // 1000 if confirmed else None,
                    prefix + "LSPRICE": confirmed[1] if confirmed else None,
                    prefix + "LSTYPE": confirmed[2] if confirmed else None,
                    prefix + "CONFIRMED_AT_MS": available_at,
                }
            )
            if include_closure:
                result[f"CLOSED_{iid}"] = frame.complete
            published[(iid, line)] = confirmed, available_at
            if frame.complete:
                kernel.search, kernel.confirmed, kernel.confirmed_at_ms = (
                    search,
                    confirmed,
                    available_at,
                )
        self._kernels, self._published, self._last, self._result = (
            kernels,
            published,
            candle,
            result,
        )
        return dict(result)
