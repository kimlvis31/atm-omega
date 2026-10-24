from typing import Protocol

from omega.domain.models import (
    AccountSnapshot,
    Candle,
    Fill,
    Instrument,
    OrderObservation,
    OrderRequest,
)


class MarketData(Protocol):
    def closed_minutes(
        self, instrument: Instrument, start_ms: int, end_ms: int, *, now_ms: int
    ) -> tuple[Candle, ...]: ...


class AccountReader(Protocol):
    def account(self) -> AccountSnapshot: ...


class Execution(Protocol):
    @property
    def account_id(self) -> str: ...

    def submit(self, request: OrderRequest) -> OrderObservation: ...

    def query(self, instrument: Instrument, client_order_id: str) -> OrderObservation: ...

    def cancel(self, instrument: Instrument, client_order_id: str) -> OrderObservation: ...

    def trades(self, instrument: Instrument, *, from_id: int | None = None) -> tuple[Fill, ...]: ...
