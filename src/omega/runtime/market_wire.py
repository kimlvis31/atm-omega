"""Exact market observations at the collector/data-manager boundary."""

from omega.domain.errors import ProtocolError
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.infrastructure.auxiliary_codec import decode_observation, encode_observation
from omega.ports.collection import MinuteObservation


def instrument_key(key):
    provider, market, symbol = key.split(":")
    result = Instrument(provider, Market(market), symbol)
    if result.key != key or provider != "binance":
        raise ProtocolError("invalid market instrument")
    return result


def candle_document(candle):
    return [
        candle.instrument.key,
        candle.open_time_ms,
        candle.close_time_ms,
        *[str(getattr(candle, name)) for name in ("open", "high", "low", "close", "volume")],
    ]


def candle_from_document(document):
    key, opened, closed, *values = document
    if type(opened) is not int or type(closed) is not int:
        raise ProtocolError("invalid market candle timestamps")
    return Candle(instrument_key(key), opened, closed, *map(decimal, values))


def minute_document(value):
    return [
        candle_document(value.candle),
        value.source,
        value.received_at_ms,
        value.raw,
        value.raw_source,
        value.raw_received_at_ms,
    ]


def minute_from_document(document):
    candle, source, received, raw, raw_source, raw_received = document
    return MinuteObservation(
        candle_from_document(candle), source, received, raw, raw_source, raw_received
    )


def auxiliary_document(instrument, observation):
    return encode_observation(instrument, observation).decode("utf-8")


def auxiliary_from_document(document):
    return decode_observation(document.encode("utf-8"))
