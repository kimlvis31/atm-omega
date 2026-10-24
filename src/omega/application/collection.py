"""Contiguous minute delivery, bounded reordering and explicit quality gates."""

from collections.abc import Callable

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Candle, Instrument
from omega.ports.collection import CandleStore, MinuteObservation

MINUTE = 60_000


class MinuteCollector:
    """One owner per subscription group. Shared by accounts/strategy instances."""

    def __init__(
        self,
        starts: dict[Instrument, int],
        store: CandleStore,
        on_candle: Callable[[Candle], None],
        *,
        max_pending: int = 512,
    ):
        if not starts or not 1 <= max_pending <= 20_000:
            raise ValidationError("subscriptions and bounded capacity required")
        if any(type(t) is not int or t < 0 or t % MINUTE for t in starts.values()):
            raise ValidationError("aligned collection start required")
        self.cursor = dict(starts)
        self.store, self.on_candle = store, on_candle
        self.max_pending = max_pending
        self._pending: dict[Instrument, dict[int, Candle]] = {i: {} for i in starts}
        self.connected = False
        self.fault: str | None = None

    def accept(self, observation: MinuteObservation, *, now_ms: int) -> int:
        candle = observation.candle
        instrument = candle.instrument
        if instrument not in self.cursor:
            raise ProtocolError("unsubscribed instrument")
        if candle.close_time_ms > now_ms or observation.received_at_ms < candle.close_time_ms:
            raise ProtocolError("open or future minute cannot be published")
        if observation.source not in {"rest", "websocket"}:
            raise ValidationError("unknown market observation source")
        pending = self._pending[instrument]
        if candle.open_time_ms > self.cursor[instrument] and (
            candle.open_time_ms not in pending and len(pending) >= self.max_pending
        ):
            self.fault = "reorder_capacity"
            raise ProtocolError("minute reorder capacity exceeded; repair from durable history")
        try:
            self.store.put(observation)
            if candle.open_time_ms < self.cursor[instrument]:
                return 0  # Store checked the immutable prior value first.
            pending[candle.open_time_ms] = candle
            count = 0
            while self.cursor[instrument] in pending:
                current = pending[self.cursor[instrument]]
                self.on_candle(current)  # Failure leaves cursor and pending retryable.
                del pending[current.open_time_ms]
                self.cursor[instrument] = current.close_time_ms
                count += 1
            return count
        except Exception:
            self.fault = "storage_or_delivery"
            raise

    def missing_range(self, instrument: Instrument, now_ms: int, *, chunk: int = 500):
        if not 1 <= chunk <= 20_000:
            raise ValidationError("invalid backfill chunk")
        start = self.cursor[instrument]
        end = min(now_ms // MINUTE * MINUTE, start + chunk * MINUTE)
        return (start, end) if start < end else None

    def ready(self, instrument: Instrument, now_ms: int) -> bool:
        return (
            self.connected
            and self.fault is None
            and self.cursor[instrument] >= now_ms // MINUTE * MINUTE
        )

    def repaired(self) -> None:
        """Owner clears a technical incident only after a successful recovery pass."""
        self.fault = None
