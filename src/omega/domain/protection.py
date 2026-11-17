"""Received partial-minute evidence, never a canonical candle or fill price."""

import re
from dataclasses import dataclass
from decimal import Decimal

from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market, require_decimal


@dataclass(frozen=True, slots=True)
class ProtectionObservation:
    instrument: Instrument
    generation: str
    connection: int
    sequence: int
    event_time_ms: int
    received_at_ms: int
    open_time_ms: int
    close_time_ms: int
    last: Decimal
    high: Decimal
    low: Decimal
    source: str = "received_kline_1m_partial"
    price_kind: str = "kline_last"
    raw_received_at_ms: int | None = None

    def __post_init__(self):
        if (
            type(self.instrument) is not Instrument
            or type(self.generation) is not str
            or re.fullmatch(r"[0-9a-f]{32}", self.generation) is None
            or any(
                type(v) is not int or not 0 <= v <= 2**53 - 1
                for v in (
                    self.connection,
                    self.sequence,
                    self.event_time_ms,
                    self.received_at_ms,
                    self.open_time_ms,
                    self.close_time_ms,
                )
            )
            or self.connection < 1
            or self.sequence < 1
            or self.open_time_ms % 60000
            or self.close_time_ms != self.open_time_ms + 60000
            or not self.open_time_ms <= self.event_time_ms < self.close_time_ms
            or self.event_time_ms > self.received_at_ms
            or type(self.source) is not str
            or re.fullmatch(r"[A-Za-z][A-Za-z0-9_.:-]{0,127}", self.source) is None
            or type(self.price_kind) is not str
            or self.price_kind != "kline_last"
        ):
            raise ValidationError("invalid partial protection observation identity/time")
        if self.raw_received_at_ms is not None and (
            type(self.raw_received_at_ms) is not int
            or self.raw_received_at_ms < 0
            or not 0 < self.event_time_ms - self.raw_received_at_ms <= 2000
            or self.raw_received_at_ms > self.received_at_ms
        ):
            raise ValidationError("invalid delayed protection original receipt")
        for price in (self.last, self.high, self.low):
            require_decimal(price)
            if (
                price <= 0
                or len(price.as_tuple().digits) > 18
                or not -18 <= price.as_tuple().exponent <= 18
            ):
                raise ValidationError("invalid bounded protection observation price")
        if not self.low <= self.last <= self.high:
            raise ValidationError("partial protection prices disagree")

    @property
    def identity(self):
        return self.generation, self.connection, self.sequence, self.instrument.key

    @property
    def original_received_at_ms(self):
        return self.received_at_ms if self.raw_received_at_ms is None else self.raw_received_at_ms

    def current(self, as_of_ms, *, max_age_ms=5000):
        if (
            type(as_of_ms) is not int
            or type(max_age_ms) is not int
            or not 0 <= max_age_ms <= 30000
            or not 0 <= as_of_ms - self.received_at_ms <= max_age_ms
            or not 0 <= as_of_ms - self.original_received_at_ms <= max_age_ms
            or not 0 <= as_of_ms - self.event_time_ms <= max_age_ms
            or as_of_ms >= self.close_time_ms
        ):
            raise ValidationError("protection observation is stale or future")

    def document(self):
        result = {
            "instrument": self.instrument.key,
            "generation": self.generation,
            "connection": self.connection,
            "sequence": self.sequence,
            "event_time_ms": self.event_time_ms,
            "received_at_ms": self.received_at_ms,
            "open_time_ms": self.open_time_ms,
            "close_time_ms": self.close_time_ms,
            "last": str(self.last),
            "high": str(self.high),
            "low": str(self.low),
            "source": self.source,
            "price_kind": self.price_kind,
        }

        if self.raw_received_at_ms is not None:
            result["raw_received_at_ms"] = self.raw_received_at_ms
        return result

    @classmethod
    def from_document(cls, value):
        if type(value) is not dict or set(value) - {"raw_received_at_ms"} != {
            "instrument",
            "generation",
            "connection",
            "sequence",
            "event_time_ms",
            "received_at_ms",
            "open_time_ms",
            "close_time_ms",
            "last",
            "high",
            "low",
            "source",
            "price_kind",
        }:
            raise ValidationError("invalid protection observation document")
        try:
            provider, market, symbol = value["instrument"].split(":")
            if any(
                type(value[key]) is not str or len(value[key]) > 64
                for key in ("last", "high", "low")
            ):
                raise ValueError
            result = cls(
                Instrument(provider, Market(market), symbol),
                **{
                    key: value[key]
                    for key in (
                        "generation",
                        "connection",
                        "sequence",
                        "event_time_ms",
                        "received_at_ms",
                        "open_time_ms",
                        "close_time_ms",
                        "source",
                        "price_kind",
                    )
                },
                **{key: Decimal(value[key]) for key in ("last", "high", "low")},
                raw_received_at_ms=value.get("raw_received_at_ms"),
            )
            if result.document() != value:
                raise ValueError
            return result
        except (ValueError, TypeError, AttributeError, ArithmeticError):
            raise ValidationError("invalid protection observation document") from None
