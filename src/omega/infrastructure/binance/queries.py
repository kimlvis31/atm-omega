"""Order/trade reads share parsing, not cash/margin or strategy semantics."""

from omega.domain.errors import OrderOutcomeUnknown, ProtocolError, ValidationError
from omega.domain.models import Fill, Instrument, Market, OrderObservation
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.parsing import order_observation, trade_fill


class OrderQueries:
    def __init__(self, market: Market, account_id: str, client: BinanceClient):
        if market not in {Market.SPOT, Market.USDM}:
            raise ValidationError("unsupported Binance market")
        if not account_id:
            raise ValidationError("canonical account identity required")
        self.market, self.account_id, self.client = market, account_id, client
        self.prefix = "/api/v3" if market == Market.SPOT else "/fapi/v1"

    def check(self, instrument: Instrument):
        if instrument.provider != "binance" or instrument.market != self.market:
            raise ValidationError("instrument belongs to another adapter")

    def query(self, instrument: Instrument, client_order_id: str) -> OrderObservation:
        self.check(instrument)
        payload = self.client.request(
            "GET",
            self.prefix + "/order",
            {"symbol": instrument.symbol, "origClientOrderId": client_order_id},
            signed=True,
            weight=4 if self.market == Market.SPOT else 1,
        )
        observation = order_observation(instrument, payload)
        if observation.client_order_id != client_order_id:
            raise ProtocolError("query returned another client order ID")
        return observation

    def cancel(self, instrument: Instrument, client_order_id: str) -> OrderObservation:
        self.check(instrument)
        payload = self.client.request(
            "DELETE",
            self.prefix + "/order",
            {"symbol": instrument.symbol, "origClientOrderId": client_order_id},
            signed=True,
        )
        try:
            observation = order_observation(instrument, payload)
            if observation.client_order_id != client_order_id:
                raise ProtocolError("cancel returned another client order ID")
            return observation
        except ProtocolError:
            raise OrderOutcomeUnknown("cancel outcome must be reconciled") from None

    def trades(self, instrument: Instrument, *, from_id: int | None = None) -> tuple[Fill, ...]:
        """One page. Persist the last processed ID; caller owns bounded backfill scheduling."""
        self.check(instrument)
        params: dict[str, object] = {"symbol": instrument.symbol, "limit": 1000}
        if from_id is not None:
            if from_id < 0:
                raise ValidationError("negative trade cursor")
            params["fromId"] = from_id
        path = "/myTrades" if self.market == Market.SPOT else "/userTrades"
        payload = self.client.request(
            "GET",
            self.prefix + path,
            params,
            signed=True,
            weight=20 if self.market == Market.SPOT else 5,
        )
        if not isinstance(payload, list):
            raise ProtocolError("expected trade list")
        return tuple(trade_fill(self.account_id, instrument, row) for row in payload)
