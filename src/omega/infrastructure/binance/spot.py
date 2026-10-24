"""Spot execution: inventory sales, not leveraged futures short positions."""

from omega.domain.errors import OrderOutcomeUnknown, ProtocolError, TradingBlocked
from omega.domain.models import (
    AccountSnapshot,
    Balance,
    Instrument,
    Market,
    OrderObservation,
    OrderRequest,
    decimal,
)
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.market import BinanceMarketData, SymbolRules
from omega.infrastructure.binance.parsing import order_observation
from omega.infrastructure.binance.queries import OrderQueries


class SpotAdapter:
    def __init__(self, account_id: str, client: BinanceClient):
        if client.base_url != "https://api.binance.com":
            raise ValueError("spot client must use the spot origin")
        self.client = client
        self.market_data = BinanceMarketData(Market.SPOT, client)
        self._orders = OrderQueries(Market.SPOT, account_id, client)
        self._rules: dict[Instrument, SymbolRules] = {}
        self._reference_time = -1

    @property
    def account_id(self) -> str:
        return self._orders.account_id

    def refresh_symbols(self) -> tuple[SymbolRules, ...]:
        rules = self.market_data.symbols()
        self._rules = {r.instrument: r for r in rules}
        self._reference_time = self.client.clock_ms()
        return rules

    def account(self) -> AccountSnapshot:
        payload = self.client.request("GET", "/api/v3/account", signed=True, weight=20)
        if not isinstance(payload, dict) or "balances" not in payload:
            raise ProtocolError("unsupported spot account schema")
        try:
            if not isinstance(payload["canTrade"], bool):
                raise ProtocolError("invalid trading permission")
            balances = tuple(
                Balance(
                    row["asset"],
                    decimal(row["free"]) + decimal(row["locked"]),
                    decimal(row["free"]),
                    locked=decimal(row["locked"]),
                )
                for row in payload["balances"]
            )
            return AccountSnapshot(
                self._orders.account_id,
                Market.SPOT,
                payload["canTrade"],
                self.client.clock_ms(),
                balances,
            )
        except (KeyError, TypeError, ValueError):
            raise ProtocolError("unsupported spot balances schema") from None

    def submit(self, request: OrderRequest) -> OrderObservation:
        self._orders.check(request.instrument)
        if request.reduce_only:
            raise TradingBlocked(
                "spot has no futures reduce-only flag; validate inventory in owner"
            )
        rules = self._rules.get(request.instrument)
        if rules is None or not 0 <= self.client.clock_ms() - self._reference_time <= 60_000:
            raise TradingBlocked("fresh symbol reference is required")
        rules.validate(request)
        params = {
            "symbol": request.instrument.symbol,
            "side": request.side.value,
            "type": request.kind.value,
            "quantity": request.quantity,
            "newClientOrderId": request.client_order_id,
            "newOrderRespType": "RESULT",
        }
        if request.price is not None:
            params.update(price=request.price, timeInForce="GTC")
        payload = self.client.request("POST", "/api/v3/order", params, signed=True, order=True)
        try:
            observation = order_observation(request.instrument, payload)
            if observation.client_order_id != request.client_order_id:
                raise ProtocolError("submission returned another client order ID")
            return observation
        except ProtocolError:
            raise OrderOutcomeUnknown(
                "accepted spot order response unreadable; query original ID"
            ) from None

    def query(self, instrument: Instrument, client_order_id: str) -> OrderObservation:
        return self._orders.query(instrument, client_order_id)

    def cancel(self, instrument: Instrument, client_order_id: str) -> OrderObservation:
        return self._orders.cancel(instrument, client_order_id)

    def trades(self, instrument: Instrument, *, from_id: int | None = None):
        return self._orders.trades(instrument, from_id=from_id)
