"""Shared 1m aggregation and legacy EMA/MMACD arithmetic, bounded by lookback."""

from collections import deque
from dataclasses import dataclass
from itertools import combinations

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument
from omega.ports.modules import ModuleDescriptor

# Calendar/week/month alignment needs a calendar adapter, not a fixed-duration guess.
INTERVAL_MINUTES = {
    0: 1,
    1: 3,
    2: 5,
    3: 15,
    4: 30,
    5: 60,
    6: 120,
    7: 240,
    8: 360,
    9: 480,
    10: 720,
    11: 1440,
}


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
            self.interval_id not in INTERVAL_MINUTES
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
        self._buckets: dict[int, tuple[int, int]] = {}
        self._last: Candle | None = None
        self._result: dict = {}

    def update(self, candle: Candle) -> dict:
        if candle.instrument != self.instrument:
            raise ValidationError("analysis instrument mismatch")
        if self._last is not None:
            if candle == self._last:
                return dict(self._result)
            if candle.open_time_ms != self._last.close_time_ms:
                raise ValidationError("analysis needs contiguous immutable minute replay")
        result = {}
        for iid, kernel in self._kernels.items():
            duration = INTERVAL_MINUTES[iid] * 60_000
            bucket = candle.open_time_ms // duration * duration
            previous_bucket, count = self._buckets.get(iid, (-1, 0))
            count = count + 1 if previous_bucket == bucket else 1
            self._buckets[iid] = (bucket, count)
            complete = candle.close_time_ms == bucket + duration and count == INTERVAL_MINUTES[iid]
            values = kernel.preview(float(candle.close))
            # Starting inside a higher bar cannot seed a fabricated full aggregate.
            complete_prefix = count == (candle.close_time_ms - bucket) // 60_000
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
                kernel.commit(float(candle.close), values)
        self._last, self._result = candle, result
        return dict(result)
