"""One-minute history and symbol reference data shared by Binance markets."""

import re
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal

from omega.domain.errors import BrokerRejected, ProtocolError, TradingBlocked, ValidationError
from omega.domain.models import Candle, Instrument, Market, OrderKind, OrderRequest, decimal
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.parsing import closed_candles
from omega.ports.collection import MinuteObservation
from omega.ports.raw_data import RawResponseArchive


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
    def __init__(
        self,
        market: Market,
        client: BinanceClient,
        *,
        raw_archive: RawResponseArchive | None = None,
    ):
        if market not in {Market.SPOT, Market.USDM}:
            raise ValidationError("unsupported Binance market")
        self.market = market
        self.client = client
        self.raw_archive = raw_archive
        self.prefix = "/api/v3" if market == Market.SPOT else "/fapi/v1"
        self.page_limit = 1000 if market == Market.SPOT else 1500
        self._symbol_diagnostics = {
            "state": "unobserved",
            "rows": 0,
            "accepted": 0,
            "invalid_rows": 0,
            "excluded_contracts": {},
            "examples": [],
        }

    @property
    def last_symbol_diagnostics(self):
        """Bounded counts/indexes, never raw invalid identity/control text."""
        return deepcopy(self._symbol_diagnostics)

    def _check(self, instrument: Instrument) -> None:
        if instrument.provider != "binance" or instrument.market != self.market:
            raise ValidationError("instrument belongs to another adapter")

    def symbols(self) -> tuple[SymbolRules, ...]:
        diagnostics = {
            "state": "fetching",
            "rows": 0,
            "accepted": 0,
            "invalid_rows": 0,
            "excluded_contracts": {},
            "examples": [],
        }
        self._symbol_diagnostics = diagnostics
        try:
            if self.market == Market.SPOT:
                # Rules never consume permissionSets; omit that optional bulk field.
                payload = self.client.request(
                    "GET",
                    self.prefix + "/exchangeInfo",
                    params={"showPermissionSets": "false"},
                    weight=20,
                )
            else:
                payload = self.client.request("GET", self.prefix + "/exchangeInfo", weight=1)
        except Exception:
            diagnostics["state"] = "fetch_failed"
            raise
        if (
            type(payload) is not dict
            or type(payload.get("symbols")) is not list
            or not 1 <= len(payload["symbols"]) <= 10000
        ):
            diagnostics["state"] = "invalid_catalog"
            raise ProtocolError("unsupported exchange information schema") from None
        diagnostics["rows"] = len(payload["symbols"])
        accepted, seen = {}, set()

        def invalid(index, reason):
            diagnostics["invalid_rows"] += 1
            if len(diagnostics["examples"]) < 8:
                diagnostics["examples"].append({"index": index, "reason": reason})

        for index, row in enumerate(payload["symbols"]):
            try:
                if type(row) is not dict:
                    raise ValueError
                if self.market == Market.USDM:
                    contract = row["contractType"]
                    if (
                        type(contract) is not str
                        or re.fullmatch("[A-Z][A-Z0-9_]{0,47}", contract) is None
                    ):
                        raise ValueError
                    if contract != "PERPETUAL":
                        excluded = diagnostics["excluded_contracts"]
                        name = (
                            contract
                            if contract in excluded or len(excluded) < 64
                            else "OTHER_UNSUPPORTED"
                        )
                        excluded[name] = excluded.get(name, 0) + 1
                        continue
                instrument = Instrument("binance", self.market, row["symbol"])
                if instrument in seen:
                    if instrument in accepted:
                        del accepted[instrument]
                        invalid(index, "duplicate_identity_first_row_quarantined")
                    invalid(index, "duplicate_identity")
                    continue
                seen.add(instrument)
                if (
                    any(
                        type(row[key]) is not str or not 1 <= len(row[key]) <= 40
                        for key in ("baseAsset", "quoteAsset")
                    )
                    or type(row["status"]) is not str
                    or re.fullmatch("[A-Z][A-Z0-9_]{0,47}", row["status"]) is None
                    or type(row["filters"]) is not list
                    or len(row["filters"]) > 64
                    or any(
                        type(rule) is not dict
                        or type(rule.get("filterType")) is not str
                        or re.fullmatch("[A-Z][A-Z0-9_]{0,63}", rule["filterType"]) is None
                        for rule in row["filters"]
                    )
                ):
                    raise ValueError
                accepted[instrument] = SymbolRules(
                    instrument,
                    row["baseAsset"],
                    row["quoteAsset"],
                    row["status"],
                    tuple(deepcopy(row["filters"])),
                )
            except (KeyError, TypeError, ValueError, ValidationError):
                invalid(index, "invalid_symbol_row")
        diagnostics["accepted"] = len(accepted)
        diagnostics["state"] = "ready" if accepted else "no_supported_symbols"
        if not accepted:
            raise ProtocolError("exchange information contains no supported valid symbols")
        return tuple(accepted.values())

    def server_time(self) -> int:
        payload = self.client.request("GET", self.prefix + "/time")
        try:
            return int(payload["serverTime"])
        except (KeyError, TypeError, ValueError):
            raise ProtocolError("server clock unavailable") from None

    def first_available_minute(self, instrument, *, trades=False):
        self._check(instrument)
        now = self.server_time()
        endpoint = "/aggTrades" if trades else "/klines"
        parameters = {"symbol": instrument.symbol, "startTime": 0, "limit": 1}
        if not trades:
            parameters["interval"] = "1m"

        def first(start):
            return self.client.request(
                "GET",
                self.prefix + endpoint,
                {**parameters, "startTime": start},
                weight=20 if trades else 2,
            )

        try:
            rows = first(0)
        except BrokerRejected as error:
            if not trades or error.status != 400 or error.code not in {-4161, -4166, -1127}:
                raise
            # The provider rejects expired start times. Find its accepted lower bound.
            lower, upper = 0, now // 60000 * 60000
            while upper - lower > 60000:
                middle = (lower + upper) // 120000 * 60000
                try:
                    first(middle)
                except BrokerRejected as rejected:
                    if rejected.status != 400 or rejected.code not in {-4161, -4166, -1127}:
                        raise
                    lower = middle + 60000
                else:
                    upper = middle
            rows = first(upper)

        if not isinstance(rows, list) or len(rows) != 1:
            raise ProtocolError("provider did not supply a first available observation")
        if trades:
            stamp = rows[0].get("T") if isinstance(rows[0], dict) else None
            if type(stamp) is not int or not 0 <= stamp < now:
                raise ProtocolError("invalid first trade observation")
            return stamp // 60000 * 60000
        candles = closed_candles(instrument, rows, 0, now // 60000 * 60000, now)
        if len(candles) != 1:
            raise ProtocolError("provider did not supply a first closed minute")
        return candles[0].open_time_ms

    def closed_minutes(
        self,
        instrument: Instrument,
        start_ms: int,
        end_ms: int,
        *,
        now_ms: int,
    ) -> tuple[Candle, ...]:
        result = []
        for cursor, page_end, params in self._history_pages(instrument, start_ms, end_ms, now_ms):
            payload = self.client.request(
                "GET",
                self.prefix + "/klines",
                params,
                weight=2 if self.market == Market.SPOT else 10,
            )
            if not isinstance(payload, list):
                raise ProtocolError("expected candle list")
            result.extend(closed_candles(instrument, payload, cursor, page_end, now_ms))
        return tuple(result)

    def _history_pages(self, instrument, start_ms, end_ms, now_ms):
        self._check(instrument)
        if start_ms < 0 or start_ms % 60_000 or end_ms % 60_000 or end_ms <= start_ms:
            raise ValidationError("aligned nonempty minute range required")
        if (end_ms - start_ms) // 60_000 > 20_000:
            raise ValidationError("history request exceeds bounded range; schedule smaller chunks")
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
            yield cursor, page_end, params
            cursor = page_end

    def _raw_public_client(self):
        expected = (
            "https://api.binance.com" if self.market == Market.SPOT else "https://fapi.binance.com"
        )
        prefix = "/api/v3" if self.market == Market.SPOT else "/fapi/v1"
        if (
            not isinstance(self.client, BinanceClient)
            or self.client.base_url != expected
            or self.client.credentials is not None
            or self.client.enable_trading is not False
            or self.client.budget is None
            or self.client.ip_scope != "binance-ip"
            or self.prefix != prefix
            or type(self.page_limit) is not int
            or not 1 <= self.page_limit <= (1000 if self.market == Market.SPOT else 1500)
        ):
            raise ValidationError("REST raw capture requires a matching public rate-budget client")
        if self.raw_archive is None or not callable(getattr(self.raw_archive, "put", None)):
            raise ValidationError("REST raw capture archive is not configured")

    def closed_observations(
        self, instrument: Instrument, start_ms: int, end_ms: int, *, now_ms: int
    ) -> tuple[MinuteObservation, ...]:
        """Explicit optional capture; legacy closed_minutes keeps its original request path."""
        from omega.infrastructure.binance.rest_minute_capture import capture_rest_page

        self._raw_public_client()
        if any(
            type(value) is not int or not 0 <= value <= 253_402_300_799_000
            for value in (start_ms, end_ms, now_ms)
        ):
            raise ValidationError("REST raw capture needs a bounded actual observation cutoff")
        result = []
        for cursor, page_end, params in self._history_pages(instrument, start_ms, end_ms, now_ms):
            self._raw_public_client()  # Recheck mutable capability before every page.
            reply = self.client.request_with_response(
                "GET",
                self.prefix + "/klines",
                params,
                signed=False,
                order=False,
                weight=2 if self.market == Market.SPOT else 10,
            )
            accepted = self.client.clock_ms()
            result.extend(
                capture_rest_page(
                    reply,
                    instrument,
                    params,
                    cursor,
                    page_end,
                    now_ms=now_ms,
                    accepted_at_ms=accepted,
                    archive=self.raw_archive,
                )
            )
        return tuple(result)
