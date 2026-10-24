"""One-minute history and symbol reference data shared by Binance markets."""

from dataclasses import dataclass
from decimal import Decimal

from omega.domain.errors import ProtocolError, TradingBlocked, ValidationError
from omega.domain.models import Candle, Instrument, Market, OrderKind, OrderRequest, decimal
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.parsing import closed_candles


@dataclass(frozen=True)
class SymbolRules:
    instrument: Instrument
    base_asset: str
    quote_asset: str
    status: str
    filters: tuple[dict, ...]

    def validate(self, request: OrderRequest) -> None:
        if request.instrument != self.instrument or self.status != "TRADING":
            raise TradingBlocked("symbol is not available for trading")
        for rule in self.filters:
            kind = rule["filterType"]
            if kind == "LOT_SIZE" or (
                kind == "MARKET_LOT_SIZE" and request.kind == OrderKind.MARKET
            ):
                self._range_step(request.quantity, rule["minQty"], rule["maxQty"], rule["stepSize"])
            elif kind == "PRICE_FILTER" and request.price is not None:
                self._range_step(
                    request.price, rule["minPrice"], rule["maxPrice"], rule["tickSize"]
                )
            elif kind in {"MIN_NOTIONAL", "NOTIONAL"} and request.price is not None:
                notional = request.price * request.quantity
                minimum = decimal(rule.get("minNotional", rule.get("notional", "0")))
                maximum = decimal(rule.get("maxNotional", "0"))
                exempt_minimum = self.instrument.market == Market.USDM and request.reduce_only
                if (not exempt_minimum and notional < minimum) or (maximum and notional > maximum):
                    raise ValidationError("order violates notional filter")
        # Dynamic average/percent prices, market-order notional and account limits
        # still need the provider's validation; this check never claims otherwise.

    @staticmethod
    def _range_step(value: Decimal, minimum: str, maximum: str, step: str) -> None:
        low, high, unit = decimal(minimum), decimal(maximum), decimal(step)
        if (low and value < low) or (high and value > high) or (unit and value % unit):
            raise ValidationError("order violates exchange quantity/price filter")


class BinanceMarketData:
    def __init__(self, market: Market, client: BinanceClient):
        if market not in {Market.SPOT, Market.USDM}:
            raise ValidationError("unsupported Binance market")
        self.market = market
        self.client = client
        self.prefix = "/api/v3" if market == Market.SPOT else "/fapi/v1"
        self.page_limit = 1000 if market == Market.SPOT else 1500

    def _check(self, instrument: Instrument) -> None:
        if instrument.provider != "binance" or instrument.market != self.market:
            raise ValidationError("instrument belongs to another adapter")

    def symbols(self) -> tuple[SymbolRules, ...]:
        payload = self.client.request(
            "GET", self.prefix + "/exchangeInfo", weight=20 if self.market == Market.SPOT else 1
        )
        try:
            return tuple(
                SymbolRules(
                    Instrument("binance", self.market, row["symbol"]),
                    row["baseAsset"],
                    row["quoteAsset"],
                    row["status"],
                    tuple(row["filters"]),
                )
                for row in payload["symbols"]
                if self.market == Market.SPOT or row.get("contractType") == "PERPETUAL"
            )
        except (KeyError, TypeError, ValueError):
            raise ProtocolError("unsupported exchange information schema") from None

    def server_time(self) -> int:
        payload = self.client.request("GET", self.prefix + "/time")
        try:
            return int(payload["serverTime"])
        except (KeyError, TypeError, ValueError):
            raise ProtocolError("server clock unavailable") from None

    def closed_minutes(
        self,
        instrument: Instrument,
        start_ms: int,
        end_ms: int,
        *,
        now_ms: int,
    ) -> tuple[Candle, ...]:
        self._check(instrument)
        if start_ms < 0 or start_ms % 60_000 or end_ms % 60_000 or end_ms <= start_ms:
            raise ValidationError("aligned nonempty minute range required")
        if (end_ms - start_ms) // 60_000 > 20_000:
            raise ValidationError("history request exceeds bounded range; schedule smaller chunks")
        result = []
        cursor = start_ms
        until = min(end_ms, now_ms // 60_000 * 60_000)
        while cursor < until:
            # A page is a bounded time span. Missing rows are not interpolated here.
            page_end = min(until, cursor + self.page_limit * 60_000)
            params = {
                "symbol": instrument.symbol,
                "interval": "1m",
                "startTime": cursor,
                "endTime": page_end - 1,
                "limit": self.page_limit,
            }
            payload = self.client.request(
                "GET",
                self.prefix + "/klines",
                params,
                weight=2 if self.market == Market.SPOT else 10,
            )
            if not isinstance(payload, list):
                raise ProtocolError("expected candle list")
            result.extend(closed_candles(instrument, payload, cursor, page_end, now_ms))
            cursor = page_end
        return tuple(result)
