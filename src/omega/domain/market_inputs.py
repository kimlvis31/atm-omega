"""Immutable minute-bound auxiliary input, separate from candle storage and trading."""

# Imports
import json
import re
from dataclasses import asdict, dataclass
from decimal import Decimal
from hashlib import sha256

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, require_decimal

# Constants
INPUT_QUALITIES = frozenset({"observed", "partial", "missing", "unsupported", "carried"})
MAX_INPUT_FIELDS = 64
MAX_FRAME_CHANNELS = 8


def _time(value):
    return type(value) is int and 0 <= value <= 253_402_300_799_000


@dataclass(frozen=True, slots=True)
class AuxiliaryInput:
    channel: str
    values: tuple[tuple[str, Decimal | int], ...]
    window_start_ms: int
    window_end_ms: int
    event_time_ms: int
    available_at_ms: int
    quality: str = "observed"
    coverage: Decimal = Decimal(1)
    source: str = ""

    def __post_init__(self):
        if (
            not isinstance(self.channel, str)
            or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", self.channel)
            or not isinstance(self.source, str)
            or not 1 <= len(self.source) <= 96
            or not self.source.isprintable()
            or not isinstance(self.quality, str)
            or self.quality not in INPUT_QUALITIES
        ):
            raise ValidationError("invalid auxiliary channel, quality or source")
        if (
            not all(
                _time(value)
                for value in (
                    self.window_start_ms,
                    self.window_end_ms,
                    self.event_time_ms,
                    self.available_at_ms,
                )
            )
            or self.window_start_ms % 60000
            or self.window_end_ms - self.window_start_ms != 60000
            or self.available_at_ms < self.event_time_ms
        ):
            raise ValidationError("auxiliary input requires an exact minute and valid source times")
        require_decimal(self.coverage)
        if not 0 <= self.coverage <= 1:
            raise ValidationError("auxiliary coverage must be a finite ratio")
        if type(self.values) is not tuple or len(self.values) > MAX_INPUT_FIELDS:
            raise ValidationError("auxiliary fields must be a bounded immutable tuple")
        seen = set()
        for row in self.values:
            if (
                type(row) is not tuple
                or len(row) != 2
                or not isinstance(row[0], str)
                or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", row[0])
                or row[0] in seen
            ):
                raise ValidationError("auxiliary field names must be valid and unique")
            value = row[1]
            if type(value) is not int:
                require_decimal(value)
            seen.add(row[0])
        if self.quality in {"missing", "unsupported"} and (self.values or self.coverage != 0):
            raise ValidationError(
                "missing/unsupported auxiliary input cannot contain observed values"
            )
        # Canonical order makes an identical observation independent of JSON key ordering.
        object.__setattr__(self, "values", tuple(sorted(self.values)))

    def mapping(self) -> dict[str, Decimal | int]:
        return dict(self.values)

    def usable_at(self, as_of_ms: int) -> bool:
        if not _time(as_of_ms):
            raise ValidationError("invalid auxiliary availability cutoff")
        return (
            self.quality == "observed"
            and self.coverage == 1
            and bool(self.values)
            and self.available_at_ms <= as_of_ms
        )


@dataclass(frozen=True, slots=True)
class MarketFeatureFrame:
    candle: Candle
    as_of_ms: int
    inputs: tuple[AuxiliaryInput, ...] = ()
    reference_time_ms: int | None = None

    def __post_init__(self):
        if (
            not isinstance(self.candle, Candle)
            or not _time(self.as_of_ms)
            or self.as_of_ms < self.candle.close_time_ms
            or type(self.inputs) is not tuple
            or len(self.inputs) > MAX_FRAME_CHANNELS
            or self.reference_time_ms is not None
            and (
                not _time(self.reference_time_ms)
                or self.reference_time_ms != self.candle.close_time_ms
            )
        ):
            raise ValidationError("invalid closed-minute feature frame")
        channels = set()
        for value in self.inputs:
            if (
                not isinstance(value, AuxiliaryInput)
                or value.channel in channels
                or value.window_start_ms != self.candle.open_time_ms
                or value.window_end_ms != self.candle.close_time_ms
                or value.available_at_ms > self.as_of_ms
            ):
                raise ValidationError("auxiliary input conflicts with frame range or availability")
            channels.add(value.channel)
        object.__setattr__(
            self, "inputs", tuple(sorted(self.inputs, key=lambda value: value.channel))
        )

    def input(self, channel: str) -> AuxiliaryInput | None:
        return next((value for value in self.inputs if value.channel == channel), None)

    @property
    def retrospective(self):
        """Explicit retrospective evaluation; actual availability is never rewritten."""
        return self.reference_time_ms is not None

    @property
    def feature_time_ms(self):
        return self.as_of_ms if self.reference_time_ms is None else self.reference_time_ms


def market_frame_checksum(frame: MarketFeatureFrame) -> str:
    """Versioned calculation-state identity, independent of any storage encoding."""
    if not isinstance(frame, MarketFeatureFrame):
        raise ValidationError("frame identity requires an immutable market frame")
    candle = frame.candle
    value = {
        "version": 1,
        "instrument": candle.instrument.key,
        "candle": {
            key: str(getattr(candle, key)) for key in ("open", "high", "low", "close", "volume")
        },
        "open_ms": candle.open_time_ms,
        "as_of_ms": frame.as_of_ms,
        "reference_time_ms": frame.reference_time_ms,
        "inputs": [asdict(item) for item in frame.inputs],
    }

    def encode(value):
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError("unsupported frame identity value")

    return sha256(
        json.dumps(
            value,
            default=encode,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class TradeFlowEvent:
    """Exact normalized trade; transport/raw provenance remains outside this contract.

    ``source`` identifies one dataset across REST repair and WebSocket delivery,
    not the transport. ``available_at_ms`` is actual local acceptance evidence.
    """

    instrument: Instrument
    aggregate_id: int
    first_trade_id: int
    last_trade_id: int
    price: Decimal
    quantity: Decimal
    buyer_is_maker: bool
    trade_time_ms: int
    available_at_ms: int
    source: str

    def __post_init__(self):
        if (
            not isinstance(self.instrument, Instrument)
            or any(
                type(value) is not int or not 0 <= value <= 2**63 - 1
                for value in (self.aggregate_id, self.first_trade_id, self.last_trade_id)
            )
            or self.first_trade_id > self.last_trade_id
            or type(self.buyer_is_maker) is not bool
            or not _time(self.trade_time_ms)
            or not _time(self.available_at_ms)
            or self.available_at_ms < self.trade_time_ms
            or not isinstance(self.source, str)
            or not 1 <= len(self.source) <= 96
            or not self.source.isprintable()
        ):
            raise ValidationError("invalid normalized trade flow identity or timestamp")
        require_decimal(self.price)
        require_decimal(self.quantity)
        if self.price <= 0 or self.quantity <= 0:
            raise ValidationError("trade flow price and quantity must be positive")

    @property
    def trade_count(self) -> int:
        return self.last_trade_id - self.first_trade_id + 1
