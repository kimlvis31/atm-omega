"""USD-M perpetual futures; one-way only, ordinary orders, cross/isolated explicit."""

from omega.domain.errors import (
    OrderOutcomeUnknown,
    ProtocolError,
    TradingBlocked,
    ValidationError,
)
from omega.domain.models import (
    AccountSnapshot,
    Balance,
    Instrument,
    Market,
    OrderObservation,
    OrderRequest,
    PositionSnapshot,
    decimal,
)
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.market import BinanceMarketData, SymbolRules
from omega.infrastructure.binance.parsing import order_observation
from omega.infrastructure.binance.queries import OrderQueries


class UsdmAdapter:
    def __init__(self, account_id: str, client: BinanceClient):
        if client.base_url != "https://fapi.binance.com":
            raise ValueError("USD-M client must use the futures origin")
        self.client = client
        self.market_data = BinanceMarketData(Market.USDM, client)
        self._orders = OrderQueries(Market.USDM, account_id, client)
        self._rules: dict[Instrument, SymbolRules] = {}
        self._reference_time = -1
        self._one_way_verified_time = -1

    @property
    def account_id(self) -> str:
        return self._orders.account_id

    def refresh_symbols(self) -> tuple[SymbolRules, ...]:
        rules = self.market_data.symbols()
        self._rules = {r.instrument: r for r in rules}
        self._reference_time = self.client.clock_ms()
        return rules

    def verify_one_way(self) -> None:
        self._one_way_verified_time = -1
        mode = self.client.request("GET", "/fapi/v1/positionSide/dual", signed=True, weight=30)
        assets = self.client.request("GET", "/fapi/v1/multiAssetsMargin", signed=True, weight=30)
        if (
            mode.get("dualSidePosition") is not False
            or assets.get("multiAssetsMargin") is not False
        ):
            raise TradingBlocked(
                "hedge or multi-assets mode is unsupported; no automatic conversion"
            )
        self._one_way_verified_time = self.client.clock_ms()

    def account(self) -> AccountSnapshot:
        payload = self.client.request("GET", "/fapi/v3/account", signed=True, weight=5)
        if not isinstance(payload, dict) or "assets" not in payload or "positions" not in payload:
            raise ProtocolError("unsupported USD-M account schema")
        if any(p.get("positionSide", "BOTH") != "BOTH" for p in payload["positions"]):
            self._one_way_verified_time = -1
            raise TradingBlocked("hedge account observation; entry disabled")
        try:
            if not isinstance(payload["canTrade"], bool):
                raise ProtocolError("invalid trading permission")
            balances = tuple(
                Balance(
                    row["asset"],
                    decimal(row["walletBalance"]),
                    decimal(row["availableBalance"]),
                    margin_balance=decimal(row["marginBalance"])
                    if "marginBalance" in row
                    else None,
                )
                for row in payload["assets"]
            )
            positions = tuple(
                PositionSnapshot(
                    Instrument("binance", Market.USDM, row["symbol"]),
                    decimal(row["positionAmt"]),
                    decimal(row["entryPrice"]),
                    decimal(row["unrealizedProfit"]),
                )
                for row in payload["positions"]
            )
            return AccountSnapshot(
                self._orders.account_id,
                Market.USDM,
                payload["canTrade"],
                self.client.clock_ms(),
                balances,
                positions,
            )
        except (KeyError, TypeError, ValueError):
            raise ProtocolError("unsupported futures balances/positions schema") from None

    def _mode_ready(self) -> None:
        if self._one_way_verified_time < 0 or not (
            0 <= self.client.clock_ms() - self._one_way_verified_time <= 60_000
        ):
            raise TradingBlocked("fresh one-way/single-asset account verification required")

    def set_margin_mode(self, instrument: Instrument, mode: str) -> None:
        self._orders.check(instrument)
        self._mode_ready()
        if mode not in {"CROSSED", "ISOLATED"}:
            raise ValidationError("unsupported margin mode")
        # No implicit margin-mode change occurs when submitting an order.
        self.client.request(
            "POST",
            "/fapi/v1/marginType",
            {"symbol": instrument.symbol, "marginType": mode},
            signed=True,
        )

    def submit(self, request: OrderRequest) -> OrderObservation:
        self._orders.check(request.instrument)
        self._mode_ready()
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
            "positionSide": "BOTH",
            "reduceOnly": request.reduce_only,
            "newOrderRespType": "RESULT",
        }
        if request.price is not None:
            params.update(price=request.price, timeInForce="GTC")
        payload = self.client.request(
            "POST", "/fapi/v1/order", params, signed=True, weight=0, order=True
        )
        try:
            observation = order_observation(request.instrument, payload)
            if observation.client_order_id != request.client_order_id:
                raise ProtocolError("submission returned another client order ID")
            return observation
        except ProtocolError:
            raise OrderOutcomeUnknown(
                "accepted futures response unreadable; query original ID"
            ) from None

    def query(self, instrument: Instrument, client_order_id: str) -> OrderObservation:
        return self._orders.query(instrument, client_order_id)

    def open_orders(self):
        return self._orders.open_orders()

    def open_conditional_orders(self):
        return self._orders.open_conditional_orders()

    def cancel(self, instrument: Instrument, client_order_id: str) -> OrderObservation:
        return self._orders.cancel(instrument, client_order_id)

    def trades(self, instrument: Instrument, *, from_id: int | None = None):
        return self._orders.trades(instrument, from_id=from_id)
