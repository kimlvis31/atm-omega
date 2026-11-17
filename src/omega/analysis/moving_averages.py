"""Shared 1m aggregation and legacy EMA/MMACD arithmetic, bounded by lookback."""

from collections import deque
from dataclasses import dataclass
from itertools import combinations

from omega.analysis.aggregation import (
    INTERVAL_MINUTES,
    CandleAggregation,
    bars_from_dependencies,
    validate_minute,
)
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument
from omega.ports.modules import ModuleDescriptor


@dataclass(frozen=True)
class MaConfiguration:
    interval_id: int
    ema_period: int | None = None
    mmacd_periods: tuple[int, ...] = ()
    signal_period: int = 9
    price_precision: int = 8

    def __post_init__(self):
        periods = self.mmacd_periods
        if (
            type(self.interval_id) is not int
            or self.interval_id not in INTERVAL_MINUTES
            or type(self.price_precision) is not int
            or not 0 <= self.price_precision <= 16
            or type(self.signal_period) is not int
            or not 2 <= self.signal_period <= 100_000
            or (
                self.ema_period is not None
                and (type(self.ema_period) is not int or not 2 <= self.ema_period <= 100_000)
            )
            or (
                periods
                and (
                    len(periods) < 2
                    or len(periods) > 10
                    or len(set(periods)) != len(periods)
                    or any(type(p) is not int or not 2 <= p <= 100_000 for p in periods)
                )
            )
            or (not periods and self.ema_period is None)
        ):
            raise ValidationError("invalid moving-average configuration")


class _Kernel:
    def __init__(self, configuration: MaConfiguration):
        self.config = configuration
        maximum = max((configuration.ema_period or 2, *configuration.mmacd_periods))
        self.closes = deque(maxlen=maximum)
        self.mas: dict[int, float | None] = {}
        self.count = 0
        self.signal = self.delta = self.absolute = None

    def preview(self, close: float) -> dict:
        """Calculate against last CLOSED bar state without advancing it."""
        c = self.config
        prices = [*self.closes, close]
        mas = {}
        for period in set(c.mmacd_periods) | ({c.ema_period} if c.ema_period else set()):
            previous = self.mas.get(period)
            if previous is None:
                value = (
                    round(sum(reversed(prices[-period:])) / period, c.price_precision)
                    if len(prices) >= period
                    else None
                )
            else:
                k = 2 / (period + 1)
                value = round(close * k + previous * (1 - k), c.price_precision)
            mas[period] = value
        signal, delta, absolute, relative = None, None, None, None
        if c.mmacd_periods:
            maximum = max(c.mmacd_periods)
            if self.count >= maximum + c.signal_period - 1:
                pairs = combinations(sorted(c.mmacd_periods), 2)
                mmacd = sum(mas[short] - mas[long] for short, long in pairs)
                k = 2 / (c.signal_period + 1)
                signal = mmacd if self.signal is None else mmacd * k + self.signal * (1 - k)
                delta = mmacd - signal
                if self.delta is not None:
                    ak = 2 / (maximum + 1)
                    previous = abs(self.delta) if self.absolute is None else self.absolute
                    absolute = abs(delta) * ak + previous * (1 - ak)
                    relative = round(delta / absolute, 5) if absolute else 0.0
        return {
            "mas": mas,
            "signal": signal,
            "delta": delta,
            "absolute": absolute,
            "relative": relative,
        }

    def commit(self, close: float, result: dict) -> None:
        self.closes.append(close)
        self.mas = result["mas"]
        self.signal, self.delta, self.absolute = (
            result["signal"],
            result["delta"],
            result["absolute"],
        )
        self.count += 1


class LegacyMaFeatures:
    """One instance per instrument/config group, shared by all subscribing strategies.

    Partial higher bars are visible as previews; CLOSED_i only marks complete bars
    built from every required minute. Completed-only TEFs hold their own last values.
    """

    descriptor = ModuleDescriptor("legacy.ema_mmacd", "1", frozenset({"OHLCV"}), True)

    def __init__(self, instrument: Instrument, configurations: tuple[MaConfiguration, ...]):
        if not configurations or len({c.interval_id for c in configurations}) != len(
            configurations
        ):
            raise ValidationError("one shared configuration per interval required")
        self.instrument = instrument
        self._kernels = {c.interval_id: _Kernel(c) for c in configurations}
        self._aggregation = CandleAggregation(instrument, tuple(self._kernels))
        self._last: Candle | None = None
        self._result: dict = {}

    def update(self, candle: Candle) -> dict:
        return self._update(candle, self._aggregation.update(candle), include_closure=True)

    def update_with(self, candle: Candle, inputs: dict) -> dict:
        bars = bars_from_dependencies(inputs, candle, self._kernels)
        return self._update(
            candle, {f"BAR_{iid}": bar for iid, bar in bars.items()}, include_closure=False
        )

    def _update(self, candle: Candle, frames: dict, *, include_closure: bool) -> dict:
        validate_minute(self.instrument, self._last, candle)
        if candle == self._last:
            return dict(self._result)
        result = {}
        for iid, kernel in self._kernels.items():
            frame = frames[f"BAR_{iid}"]
            complete = frame.complete
            values = kernel.preview(float(frame.close))
            # Starting inside a higher bar cannot seed a fabricated full aggregate.
            complete_prefix = frame.valid_prefix
            if include_closure:
                result[f"CLOSED_{iid}"] = complete
            if kernel.config.ema_period:
                result[f"{iid}_EMA_0_EMA"] = (
                    values["mas"][kernel.config.ema_period] if complete_prefix else None
                )
            if kernel.config.mmacd_periods:
                result[f"{iid}_MMACD_MSDELTAABSMAREL"] = (
                    values["relative"] if complete_prefix else None
                )
                result[f"{iid}_MMACD_MSDELTA"] = values["delta"] if complete_prefix else None
                result[f"{iid}_MMACD_MSDELTAABSMA"] = (
                    values["absolute"] if complete_prefix else None
                )
            if complete:
                kernel.commit(float(frame.close), values)
        self._last, self._result = candle, result
        return dict(result)
