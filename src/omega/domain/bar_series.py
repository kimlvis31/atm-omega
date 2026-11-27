"""Immutable metadata and observations for collector-owned base-bar series."""

import json
import re
from dataclasses import dataclass, field
from decimal import Decimal
from hashlib import sha256

from omega.domain.bar_schedule import BarSchedule
from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market, require_decimal

MAX_BAR_TIME_MS = 253_402_300_799_000
MAX_SESSIONS = 4_096
_BAR_SOURCES = frozenset({"rest", "websocket", "vision"})
_SERIES_FIELDS = {
    "version",
    "instrument",
    "feed",
    "native_intervals_ms",
    "interval_ms",
    "sessions",
    "receipt_grace_ms",
    "calendar_version",
    "effective_from_ms",
}


def validate_series_id(value: str) -> None:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValidationError("invalid base-bar series identity")


def bar_time(value: object) -> bool:
    return type(value) is int and 0 <= value <= MAX_BAR_TIME_MS


@dataclass(frozen=True, slots=True)
class BarSeries:
    instrument: Instrument
    feed: str
    native_intervals_ms: tuple[int, ...]
    schedule: BarSchedule
    effective_from_ms: int
    calendar_version: str | None = None
    series_id: str = field(init=False)

    def __post_init__(self):
        if (
            not isinstance(self.instrument, Instrument)
            or type(self.feed) is not str
            or re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,63}", self.feed) is None
            or not isinstance(self.schedule, BarSchedule)
            or not bar_time(self.effective_from_ms)
        ):
            raise ValidationError("invalid base-bar series metadata")
        intervals = self.native_intervals_ms
        if (
            type(intervals) is not tuple
            or not 1 <= len(intervals) <= 64
            or any(not bar_time(value) or not value or value % 60_000 for value in intervals)
            or intervals != tuple(sorted(set(intervals)))
            or self.schedule.interval_ms != intervals[0]
        ):
            raise ValidationError("base bars must use the shortest available native interval")
        sessions = self.schedule.sessions
        if (
            sessions is not None
            and (len(sessions) > MAX_SESSIONS or self.calendar_version is None)
            or self.calendar_version is not None
            and (
                type(self.calendar_version) is not str
                or re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", self.calendar_version) is None
            )
            or sessions is not None
            and any(not bar_time(close) for _, close in sessions)
            or not self.schedule.contains_open(self.effective_from_ms)
        ):
            raise ValidationError("invalid calendar version or effective bar boundary")
        canonical = json.dumps(
            self.document(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        object.__setattr__(self, "series_id", sha256(canonical).hexdigest())

    def document(self) -> dict:
        sessions = self.schedule.sessions
        return {
            "version": 1,
            "instrument": self.instrument.key,
            "feed": self.feed,
            "native_intervals_ms": list(self.native_intervals_ms),
            "interval_ms": self.schedule.interval_ms,
            "sessions": None if sessions is None else [list(window) for window in sessions],
            "receipt_grace_ms": self.schedule.receipt_grace_ms,
            "calendar_version": self.calendar_version,
            "effective_from_ms": self.effective_from_ms,
        }

    @classmethod
    def from_document(cls, document: dict) -> "BarSeries":
        if (
            type(document) is not dict
            or set(document) != _SERIES_FIELDS
            or type(document["version"]) is not int
            or document["version"] != 1
            or type(document["instrument"]) is not str
            or type(document["native_intervals_ms"]) is not list
            or not 1 <= len(document["native_intervals_ms"]) <= 64
        ):
            raise ValidationError("invalid base-bar series document")
        windows = document["sessions"]
        if windows is not None and (
            type(windows) is not list
            or len(windows) > MAX_SESSIONS
            or any(type(window) is not list or len(window) != 2 for window in windows)
        ):
            raise ValidationError("invalid base-bar sessions")
        try:
            provider, market, symbol = document["instrument"].split(":")
            instrument = Instrument(provider, Market(market), symbol)
        except (TypeError, ValueError, ValidationError):
            raise ValidationError("invalid series instrument") from None
        return cls(
            instrument,
            document["feed"],
            tuple(document["native_intervals_ms"]),
            BarSchedule(
                document["interval_ms"],
                None if windows is None else tuple(tuple(window) for window in windows),
                document["receipt_grace_ms"],
            ),
            document["effective_from_ms"],
            document["calendar_version"],
        )


@dataclass(frozen=True, slots=True)
class SeriesCandle:
    series: BarSeries
    open_time_ms: int
    close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    def __post_init__(self):
        if (
            not isinstance(self.series, BarSeries)
            or not bar_time(self.open_time_ms)
            or not bar_time(self.close_time_ms)
            or self.open_time_ms < self.series.effective_from_ms
            or self.close_time_ms - self.open_time_ms != self.series.schedule.interval_ms
            or not self.series.schedule.contains_open(self.open_time_ms)
        ):
            raise ValidationError("bar does not match its series interval and trading session")
        for number in (self.open, self.high, self.low, self.close, self.volume):
            require_decimal(number)
        if (
            self.low <= 0
            or self.low > min(self.open, self.close)
            or self.high < max(self.open, self.close)
            or self.volume < 0
        ):
            raise ValidationError("invalid base-bar OHLCV")


@dataclass(frozen=True, slots=True)
class SeriesObservation:
    candle: SeriesCandle
    source: str
    received_at_ms: int
    raw: str | None = None
    raw_source: str | None = None
    raw_received_at_ms: int | None = None

    def __post_init__(self):
        if (
            not isinstance(self.candle, SeriesCandle)
            or type(self.source) is not str
            or self.source not in _BAR_SOURCES
            or not bar_time(self.received_at_ms)
            or self.received_at_ms < self.candle.close_time_ms
        ):
            raise ValidationError("invalid closed-bar source or receipt")
        if self.raw is not None:
            if type(self.raw) is not str:
                raise ValidationError("raw bar must be bounded UTF-8 text")
            try:
                size = len(self.raw.encode("utf-8"))
            except UnicodeError:
                raise ValidationError("raw bar must be bounded UTF-8 text") from None
            if size > 65_536 or "\x00" in self.raw:
                raise ValidationError("raw bar must be bounded UTF-8 text")
        if self.raw_source is None and self.raw_received_at_ms is None:
            return  # Legacy payloads can have unknown reception provenance.
        if (
            self.raw is None
            or type(self.raw_source) is not str
            or self.raw_source not in _BAR_SOURCES
            or not bar_time(self.raw_received_at_ms)
        ):
            raise ValidationError("invalid raw bar provenance")
