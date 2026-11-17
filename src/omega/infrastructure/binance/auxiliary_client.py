"""Bounded credential-free auxiliary GETs; no paging, scheduling or storage ownership."""

import json
import re
from dataclasses import dataclass, field, replace
from decimal import Decimal

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.auxiliary import (
    AuxStatisticEvent,
    AuxTradeEvent,
    _unique_object,
    agg_trade,
    long_short_ratio,
    open_interest,
    open_interest_history,
)
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.transport import HttpResponse

MAX_TIME = 253_402_300_799_000
MAX_BODY_BYTES = 4 * 1024 * 1024
DAY = 86_400_000


@dataclass(frozen=True)
class AuxiliaryBatch:
    """One public response body, not reserialized per-item wire payloads.

    Events contain parsed values and raw=None. Headers retain only public
    rate/date/content-type metadata, never a cookie or credential header.
    Saturated means the page may be truncated, never proof of full coverage.
    """

    instrument: Instrument
    channel: str
    events: tuple[AuxTradeEvent | AuxStatisticEvent, ...] = field(repr=False)
    body: bytes = field(repr=False)
    headers: tuple[tuple[str, str], ...]
    received_at_ms: int
    request_parameters: tuple[tuple[str, str], ...]
    saturated: bool


def _integer(value, *, maximum=MAX_TIME):
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValidationError("auxiliary request value must be a bounded nonnegative integer")
    return value


def _limit(value, maximum):
    if _integer(value, maximum=maximum) == 0:
        raise ValidationError("auxiliary request limit must be positive")


def _constant(_value):
    raise ProtocolError("non-finite JSON constant in public auxiliary response")


class BinanceAuxiliaryClient:
    def __init__(self, market: Market, client: BinanceClient):
        if not isinstance(market, Market) or market not in {Market.SPOT, Market.USDM}:
            raise ValidationError("unsupported public auxiliary market")
        self.market, self.client = market, client
        self._public_client()

    @property
    def capabilities(self):
        supported = {"agg_trades"}
        if self.market == Market.USDM:
            supported.update({"current_open_interest", "open_interest_history", "long_short_ratio"})
        return frozenset(supported)

    def _public_client(self):
        expected = (
            "https://api.binance.com" if self.market == Market.SPOT else "https://fapi.binance.com"
        )
        if (
            not isinstance(self.client, BinanceClient)
            or self.client.base_url != expected
            or self.client.credentials is not None
            or self.client.enable_trading is not False
            or self.client.budget is None
            or self.client.ip_scope != "binance-ip"
        ):
            raise ValidationError(
                "auxiliary requests require a public matching-origin rate-budget client"
            )

    def _instrument(self, instrument, *, futures=False):
        self._public_client()  # Recheck mutable client settings before every request.
        if (
            not isinstance(instrument, Instrument)
            or instrument.provider != "binance"
            or instrument.market != self.market
            or (futures and self.market != Market.USDM)
        ):
            raise ValidationError(
                "auxiliary instrument or channel is unsupported by this public client"
            )

    def _clock(self):
        return _integer(self.client.clock_ms())

    def _window(self, start_ms, end_ms, now_ms, *, history_days=None, max_span=None):
        _integer(start_ms)
        _integer(end_ms)
        if not start_ms < end_ms <= now_ms:
            raise ValidationError(
                "auxiliary request requires an ordered historical half-open range"
            )
        if history_days is not None and start_ms < max(0, now_ms - history_days * DAY):
            raise ValidationError("auxiliary query exceeds its supported historical lookback")
        if max_span is not None and end_ms - start_ms > max_span:
            raise ValidationError("auxiliary query time window exceeds the bounded endpoint range")

    def _request(self, instrument, channel, path, parameters, *, weight, parser, limit, array=True):
        self._public_client()
        reply = self.client.request_with_response(
            "GET", path, parameters, signed=False, order=False, weight=weight
        )
        received = self._clock()
        try:
            response = reply.response
            if (
                not isinstance(response, HttpResponse)
                or type(response.status) is not int
                or response.status != 200
                or type(response.body) is not bytes
                or not 0 < len(response.body) <= MAX_BODY_BYTES
                or type(response.headers) is not dict
                or len(response.headers) > 128
                or any(
                    type(k) is not str
                    or type(v) is not str
                    or not 0 < len(k) <= 256
                    or len(v) > 8192
                    or not k.isprintable()
                    or any(char in v for char in "\r\n\0")
                    for k, v in response.headers.items()
                )
            ):
                raise ProtocolError("invalid public auxiliary response envelope")
            data = json.loads(
                response.body.decode("utf-8"),
                parse_float=Decimal,
                object_pairs_hook=_unique_object,
                parse_constant=_constant,
            )
            if data != reply.data:
                raise ProtocolError("public auxiliary decoded data differs from its wire body")
            if array:
                if type(data) is not list or len(data) > limit:
                    raise ProtocolError(
                        "public auxiliary page has an invalid type or exceeds its limit"
                    )
                rows = data
            else:
                if type(data) is not dict:
                    raise ProtocolError("public auxiliary sample must be an object")
                rows = [data]
            events = tuple(parser(row, received) for row in rows)
            accepted = self._clock()
            if accepted < received:
                raise ProtocolError(
                    "local clock moved backward while accepting public auxiliary data"
                )
            events = tuple(replace(event, available_at_ms=accepted) for event in events)
            headers = tuple(
                sorted(
                    (key.lower(), value)
                    for key, value in response.headers.items()
                    if key.lower() in {"date", "content-type", "retry-after"}
                    or re.fullmatch(r"x-mbx-used-weight(?:-\d+[smhd])?", key.lower())
                )
            )
            return AuxiliaryBatch(
                instrument,
                channel,
                events,
                response.body,
                headers,
                received,
                tuple(sorted((key, str(value)) for key, value in parameters.items())),
                array and len(events) == limit,
            )
        except ProtocolError:
            raise
        except (AttributeError, KeyError, ValueError, TypeError, UnicodeError, RecursionError):
            raise ProtocolError("unsupported public auxiliary response schema") from None

    def agg_trades(self, instrument, *, from_id=None, start_ms=None, end_ms=None, limit=500):
        self._instrument(instrument)
        _limit(limit, 1000)
        now = self._clock()
        parameters = {"symbol": instrument.symbol, "limit": limit}
        if from_id is not None:
            _integer(from_id, maximum=2**63 - 1)
            if start_ms is not None or end_ms is not None:
                raise ValidationError("fromId cannot be mixed with aggregate trade time filters")
            parameters["fromId"] = from_id
        elif start_ms is not None or end_ms is not None:
            if start_ms is None or end_ms is None:
                raise ValidationError("aggregate trade time range requires both bounds")
            self._window(
                start_ms,
                end_ms,
                now,
                history_days=2 if self.market == Market.USDM else None,
                max_span=3_600_000 if self.market == Market.USDM else None,
            )
            parameters.update(startTime=start_ms, endTime=end_ms - 1)
        prefix = "/api/v3" if self.market == Market.SPOT else "/fapi/v1"
        batch = self._request(
            instrument,
            "agg_trades",
            prefix + "/aggTrades",
            parameters,
            weight=4 if self.market == Market.SPOT else 20,
            limit=limit,
            parser=lambda row, received: agg_trade(
                instrument, row, available_at_ms=received, source="rest"
            ),
        )
        previous = None
        for event in batch.events:
            if (
                previous is not None
                and (
                    event.aggregate_id <= previous.aggregate_id
                    or event.trade_time_ms < previous.trade_time_ms
                )
                or (from_id is not None and event.aggregate_id < from_id)
                or (start_ms is not None and not start_ms <= event.trade_time_ms < end_ms)
                or (self.market == Market.USDM and event.trade_time_ms < max(0, now - 2 * DAY))
            ):
                raise ProtocolError(
                    "aggregate trade response is unordered or outside its requested range"
                )
            previous = event
        return batch

    def current_open_interest(self, instrument):
        self._instrument(instrument, futures=True)
        return self._request(
            instrument,
            "open_interest",
            "/fapi/v1/openInterest",
            {"symbol": instrument.symbol},
            weight=1,
            limit=1,
            array=False,
            parser=lambda row, received: open_interest(instrument, row, available_at_ms=received),
        )

    def _history(self, instrument, *, start_ms, end_ms, limit, ratio):
        self._instrument(instrument, futures=True)
        _limit(limit, 500)
        # A deliberately smaller first-slice window than a variable calendar month.
        self._window(start_ms, end_ms, self._clock(), history_days=28)
        path = "/futures/data/" + ("globalLongShortAccountRatio" if ratio else "openInterestHist")
        parser = long_short_ratio if ratio else open_interest_history
        batch = self._request(
            instrument,
            "long_short_ratio" if ratio else "open_interest_history",
            path,
            {
                "symbol": instrument.symbol,
                "period": "5m",
                "limit": limit,
                "startTime": start_ms,
                "endTime": end_ms - 1,
            },
            weight=1,
            limit=limit,
            parser=lambda row, received: parser(
                instrument, row, available_at_ms=received, period_ms=300_000
            ),
        )
        previous = None
        for event in batch.events:
            if not start_ms <= event.event_time_ms < end_ms or (
                previous is not None and event.event_time_ms <= previous
            ):
                raise ProtocolError(
                    "auxiliary statistic page is unordered or outside its requested range"
                )
            previous = event.event_time_ms
        return batch

    def open_interest_history(self, instrument, *, start_ms, end_ms, limit=500):
        return self._history(instrument, start_ms=start_ms, end_ms=end_ms, limit=limit, ratio=False)

    def long_short_ratio(self, instrument, *, start_ms, end_ms, limit=500):
        return self._history(instrument, start_ms=start_ms, end_ms=end_ms, limit=limit, ratio=True)
