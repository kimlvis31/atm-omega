"""Pure public auxiliary decoding. No client, SQL, aggregation or trading side effects."""

import json
from dataclasses import dataclass
from decimal import Decimal

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import TradeFlowEvent
from omega.domain.models import Instrument, Market, decimal

MAX_PAYLOAD = 1_048_576
MAX_INTEGER = 2**63 - 1


@dataclass(frozen=True)
class AuxTradeEvent:
    instrument: Instrument
    aggregate_id: int
    first_trade_id: int
    last_trade_id: int
    price: Decimal
    quantity: Decimal
    buyer_is_maker: bool
    trade_time_ms: int
    event_time_ms: int | None
    available_at_ms: int
    source: str
    raw: bytes | None = None
    non_rpi_quantity: Decimal | None = None

    @property
    def trade_count(self) -> int:
        return self.last_trade_id - self.first_trade_id + 1

    def as_flow_event(self) -> TradeFlowEvent:
        """One dataset across REST/WS; keep this original transport record for export."""
        return TradeFlowEvent(
            instrument=self.instrument,
            aggregate_id=self.aggregate_id,
            first_trade_id=self.first_trade_id,
            last_trade_id=self.last_trade_id,
            price=self.price,
            quantity=self.quantity,
            buyer_is_maker=self.buyer_is_maker,
            trade_time_ms=self.trade_time_ms,
            available_at_ms=self.available_at_ms,
            source=f"{self.instrument.key}:aggTrade",
        )


@dataclass(frozen=True)
class AuxStatisticEvent:
    instrument: Instrument
    channel: str
    values: tuple[Decimal, ...]
    event_time_ms: int
    available_at_ms: int
    source_period_ms: int | None
    source: str = "rest"
    raw: bytes | None = None


def _instrument(instrument, *, futures=False):
    if (
        not isinstance(instrument, Instrument)
        or instrument.provider != "binance"
        or instrument.market not in {Market.SPOT, Market.USDM}
        or (futures and instrument.market != Market.USDM)
    ):
        raise ValidationError("auxiliary channel is unsupported for this instrument")


def _integer(value):
    if type(value) is not int or not 0 <= value <= MAX_INTEGER:
        raise ProtocolError("auxiliary identifier/time must be a nonnegative integer")
    return value


def _positive(value, *, allow_zero=False):
    result = decimal(value)
    if result < 0 or (not allow_zero and result == 0):
        raise ProtocolError("auxiliary value is outside its permitted range")
    return result


def _unique_object(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ProtocolError("duplicate JSON field in auxiliary payload")
        result[key] = value
    return result


def _payload(payload):
    raw = payload if isinstance(payload, bytes) else None
    if raw is not None:
        if not 0 < len(raw) <= MAX_PAYLOAD:
            raise ProtocolError("auxiliary payload size limit exceeded")
        try:
            payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
        except (ValueError, UnicodeError, RecursionError):
            raise ProtocolError("invalid auxiliary JSON bytes") from None
    if not isinstance(payload, dict):
        raise ProtocolError("auxiliary payload must be an object or original JSON bytes")
    return payload, raw


def agg_trade(
    instrument: Instrument, payload: dict | bytes, *, available_at_ms: int, source="rest"
):
    """Return one exchange event; ID dedup and minute sums belong to the application."""
    _instrument(instrument)
    available = _integer(available_at_ms)
    if not isinstance(source, str) or source not in {"rest", "websocket"}:
        raise ValidationError("unknown aggregate trade transport source")
    try:
        row, raw = _payload(payload)
        if "data" in row:
            if (
                source != "websocket"
                or row.get("stream") != f"{instrument.symbol.lower()}@aggTrade"
            ):
                raise ProtocolError("aggregate trade stream identity mismatch")
            row = row["data"]
        if not isinstance(row, dict):
            raise ProtocolError("aggregate trade payload is not an object")
        if source == "websocket" and not {"s", "e", "E"} <= row.keys():
            raise ProtocolError("aggregate trade stream metadata missing")
        if "s" in row and row["s"] != instrument.symbol:
            raise ProtocolError("aggregate trade symbol mismatch")
        if "e" in row and row["e"] != "aggTrade":
            raise ProtocolError("aggregate trade event type mismatch")
        if type(row["m"]) is not bool:
            raise ProtocolError("aggregate trade maker flag must be boolean")
        first, last = _integer(row["f"]), _integer(row["l"])
        if first > last:
            raise ProtocolError("aggregate trade ID range is reversed")
        quantity = _positive(row["q"])
        non_rpi = _positive(row["nq"], allow_zero=True) if "nq" in row else None
        if non_rpi is not None and non_rpi > quantity:
            raise ProtocolError("non-RPI quantity exceeds total aggregate quantity")
        return AuxTradeEvent(
            instrument,
            _integer(row["a"]),
            first,
            last,
            _positive(row["p"]),
            quantity,
            row["m"],
            _integer(row["T"]),
            _integer(row["E"]) if "E" in row else None,
            available,
            source,
            raw,
            non_rpi,
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        raise ProtocolError("unsupported aggregate trade schema") from None


def open_interest(instrument: Instrument, payload: dict | bytes, *, available_at_ms: int):
    """Current contract quantity point sample; no fabricated quote-value or 1m period."""
    _instrument(instrument, futures=True)
    try:
        row, raw = _payload(payload)
        if row["symbol"] != instrument.symbol:
            raise ProtocolError("open interest symbol mismatch")
        return AuxStatisticEvent(
            instrument,
            "open_interest",
            (_positive(row["openInterest"], allow_zero=True),),
            _integer(row["time"]),
            _integer(available_at_ms),
            None,
            raw=raw,
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        raise ProtocolError("unsupported current open interest schema") from None


def open_interest_history(
    instrument: Instrument, payload: dict | bytes, *, available_at_ms: int, period_ms: int
):
    """Historical quantity and provider quote-value share the actual sample timestamp."""
    _instrument(instrument, futures=True)
    try:
        row, raw = _payload(payload)
        if row["symbol"] != instrument.symbol or _integer(period_ms) == 0:
            raise ProtocolError("open interest identity/period mismatch")
        return AuxStatisticEvent(
            instrument,
            "open_interest_history",
            (
                _positive(row["sumOpenInterest"], allow_zero=True),
                _positive(row["sumOpenInterestValue"], allow_zero=True),
            ),
            _integer(row["timestamp"]),
            _integer(available_at_ms),
            period_ms,
            raw=raw,
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        raise ProtocolError("unsupported historical open interest schema") from None


def long_short_ratio(
    instrument: Instrument, payload: dict | bytes, *, available_at_ms: int, period_ms: int
):
    """Account ratio sample, not taker volume or position-notional ratio."""
    _instrument(instrument, futures=True)
    try:
        row, raw = _payload(payload)
        if "symbol" in row and row["symbol"] != instrument.symbol:
            raise ProtocolError("long/short sample symbol mismatch")
        if _integer(period_ms) == 0:
            raise ProtocolError("long/short sample period missing")
        ratio = _positive(row["longShortRatio"], allow_zero=True)
        long = _positive(row["longAccount"], allow_zero=True)
        short = _positive(row["shortAccount"], allow_zero=True)
        if long > 1 or short > 1:
            raise ProtocolError("account proportions must be in [0,1]")
        return AuxStatisticEvent(
            instrument,
            "long_short_ratio",
            (ratio, long, short),
            _integer(row["timestamp"]),
            _integer(available_at_ms),
            period_ms,
            raw=raw,
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        raise ProtocolError("unsupported long/short account ratio schema") from None


def funding_rate(instrument: Instrument, payload: dict | bytes, *, available_at_ms: int):
    """Settled funding rate, not a predicted mark-price stream funding estimate."""
    _instrument(instrument, futures=True)
    try:
        row, raw = _payload(payload)
        if row["symbol"] != instrument.symbol:
            raise ProtocolError("funding symbol mismatch")
        return AuxStatisticEvent(
            instrument,
            "funding_rate",
            (decimal(row["fundingRate"]),),
            _integer(row["fundingTime"]),
            _integer(available_at_ms),
            None,
            raw=raw,
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        raise ProtocolError("unsupported settled funding schema") from None
