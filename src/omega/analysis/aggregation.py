"""Shared causal OHLCV aggregation from immutable, contiguous one-minute candles."""

# Imports
from dataclasses import dataclass
from decimal import Decimal
from math import lcm

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument
from omega.ports.modules import ModuleDescriptor

# Constants
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
class AggregatedBar:
    instrument: Instrument
    interval_id: int
    open_time_ms: int
    close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    valid_prefix: bool
    complete: bool


def validate_minute(instrument: Instrument, previous: Candle | None, candle: Candle) -> None:
    if candle.instrument != instrument:
        raise ValidationError("analysis instrument mismatch")
    if (
        previous is not None
        and candle != previous
        and candle.open_time_ms != previous.close_time_ms
    ):
        raise ValidationError("analysis needs contiguous immutable minute replay")


def bars_from_dependencies(inputs: dict, candle: Candle, intervals) -> dict[int, AggregatedBar]:
    if len(inputs) != 1:
        raise ValidationError("exactly one shared aggregation dependency is required")
    source = next(iter(inputs.values()))
    bars = {iid: source.get(f"BAR_{iid}") for iid in intervals}
    for iid, bar in bars.items():
        if (
            not isinstance(bar, AggregatedBar)
            or bar.instrument != candle.instrument
            or bar.interval_id != iid
            or bar.close_time_ms != candle.close_time_ms
        ):
            raise ValidationError("missing or mismatched aggregate frame")
    return bars


class CandleAggregation:
    descriptor = ModuleDescriptor("candle_aggregation", "1", frozenset({"OHLCV"}), True)

    def __init__(self, instrument: Instrument, intervals: tuple[int, ...]):
        if (
            not intervals
            or len(set(intervals)) != len(intervals)
            or any(type(iid) is not int or iid not in INTERVAL_MINUTES for iid in intervals)
        ):
            raise ValidationError("unique supported interval IDs are required")
        self.instrument = instrument
        self.intervals = intervals
        self.public_fields = tuple(f"CLOSED_{iid}" for iid in intervals)
        self._buckets: dict[int, AggregatedBar] = {}
        self._last: Candle | None = None
        self._result: dict = {}

    @property
    def warmup_alignment_minutes(self):
        return lcm(*(INTERVAL_MINUTES[iid] for iid in self.intervals))

    def update(self, candle: Candle) -> dict:
        validate_minute(self.instrument, self._last, candle)
        if candle == self._last:
            return dict(self._result)
        result = {}
        for iid in self.intervals:
            duration = INTERVAL_MINUTES[iid] * 60000
            opened = candle.open_time_ms // duration * duration
            previous = self._buckets.get(iid)
            same_bucket = previous is not None and previous.open_time_ms == opened
            valid_prefix = previous.valid_prefix if same_bucket else candle.open_time_ms == opened
            frame = AggregatedBar(
                self.instrument,
                iid,
                opened,
                candle.close_time_ms,
                previous.open if same_bucket else candle.open,
                max(previous.high, candle.high) if same_bucket else candle.high,
                min(previous.low, candle.low) if same_bucket else candle.low,
                candle.close,
                previous.volume + candle.volume if same_bucket else candle.volume,
                valid_prefix,
                valid_prefix and candle.close_time_ms == opened + duration,
            )
            self._buckets[iid] = frame
            result[f"BAR_{iid}"] = frame
            result[f"CLOSED_{iid}"] = frame.complete
        self._last, self._result = candle, result
        return dict(result)
