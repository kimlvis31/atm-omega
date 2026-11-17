"""Data-only shutdown participant boundary; no GUI or transport dependency."""

from typing import NotRequired, Protocol, TypedDict


class ShutdownPosition(TypedDict):
    account_id: str
    instance: str
    instrument: str
    quantity: str


class ShutdownPending(ShutdownPosition):
    order_id: str
    reducing: bool
    side: NotRequired[str]


class ShutdownSnapshot(TypedDict):
    owner: str
    ready: bool
    durable: bool
    supports_flatten: bool
    positions: list[ShutdownPosition]
    pending_orders: list[ShutdownPending]
    unknown: list[str]
    notes: list[str]


class ShutdownParticipant(Protocol):
    def shutdown_snapshot(self) -> ShutdownSnapshot: ...

    def shutdown_drain(self, operation_id: str, issued_at_ms: int) -> dict: ...

    def shutdown_cancel(self, operation_id: str) -> dict: ...
