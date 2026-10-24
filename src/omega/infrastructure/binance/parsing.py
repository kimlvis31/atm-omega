"""Strict Binance payload normalization; unsupported/missing data is not zero-filled."""

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import (
    Candle,
    Fill,
    Instrument,
    OrderObservation,
    OrderStatus,
    Side,
    decimal,
)


def order_observation(instrument: Instrument, payload: dict) -> OrderObservation:
    try:
        if payload["symbol"] != instrument.symbol:
            raise ProtocolError("order response symbol mismatch")
        quote = payload.get("cummulativeQuoteQty", payload.get("cumQuote"))
        if quote is None:
            raise ProtocolError("cumulative quote amount missing")
        return OrderObservation(
            instrument,
            str(payload["clientOrderId"]),
            str(payload["orderId"]),
            OrderStatus(payload["status"]),
            decimal(payload["executedQty"]),
            decimal(quote),
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        raise ProtocolError("unsupported order response schema") from None


def closed_candles(
    instrument: Instrument,
    payload: list,
    start_ms: int,
    end_ms: int,
    now_ms: int,
) -> tuple[Candle, ...]:
    result: dict[int, Candle] = {}
    try:
        for row in payload:
            opened, closed = int(row[0]), int(row[6]) + 1
            if not start_ms <= opened < end_ms or closed > now_ms or closed > end_ms:
                continue
            candle = Candle(instrument, opened, closed, *(decimal(row[i]) for i in range(1, 6)))
            if opened in result and result[opened] != candle:
                raise ProtocolError("conflicting duplicate candle")
            result[opened] = candle
    except (KeyError, IndexError, TypeError, ValueError, ValidationError):
        raise ProtocolError("unsupported kline schema") from None
    return tuple(result[k] for k in sorted(result))


def trade_fill(account: str, instrument: Instrument, row: dict) -> Fill:
    try:
        if row["symbol"] != instrument.symbol:
            raise ProtocolError("trade response symbol mismatch")
        side = row.get("side")
        if side is None:
            buyer = row["isBuyer"]
            if not isinstance(buyer, bool):
                raise ProtocolError("isBuyer must be boolean")
            side = "BUY" if buyer else "SELL"
        return Fill(
            account,
            instrument,
            str(row["id"]),
            str(row["orderId"]),
            Side(side),
            decimal(row["qty"]),
            decimal(row["price"]),
            decimal(row["commission"]),
            row["commissionAsset"],
            int(row["time"]),
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        raise ProtocolError("unsupported trade response schema") from None
