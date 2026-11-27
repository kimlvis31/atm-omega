"""Contiguous minute delivery, bounded reordering and explicit quality gates."""

from collections.abc import Callable
from types import MappingProxyType

from omega.domain.bar_schedule import BarSchedule
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
        schedules: dict[Instrument, BarSchedule] | None = None,
    ):
        if not starts or not 1 <= max_pending <= 20_000:
            raise ValidationError("subscriptions and bounded capacity required")
        if any(type(t) is not int or t < 0 or t % MINUTE for t in starts.values()):
            raise ValidationError("aligned collection start required")
        configured_schedules = dict(schedules or {})
        if set(configured_schedules) - set(starts) or any(
            not isinstance(schedule, BarSchedule) for schedule in configured_schedules.values()
        ):
            raise ValidationError("bar schedules must match subscribed instruments")
        configured_schedules = {
            instrument: configured_schedules.get(instrument, BarSchedule()) for instrument in starts
        }
        if any(schedule.interval_ms != MINUTE for schedule in configured_schedules.values()):
            raise ValidationError("minute collector requires one-minute schedules")
        if any(
            timestamp % configured_schedules[instrument].interval_ms
            or not (
                configured_schedules[instrument].contains_open(timestamp)
                or configured_schedules[instrument].is_terminal_cursor(timestamp)
            )
            for instrument, timestamp in starts.items()
        ):
            raise ValidationError("collection start must align to its bar schedule")
        self._schedules = MappingProxyType(configured_schedules)
        self.cursor = dict(starts)
        self.store, self.on_candle = store, on_candle
        self.max_pending = max_pending
        self._pending: dict[Instrument, dict[int, Candle]] = {i: {} for i in starts}
        self.connected = False
        self.fault: str | None = None

    def _validate(self, observation: MinuteObservation, now_ms: int) -> Candle:
        candle = observation.candle
        instrument = candle.instrument
        if instrument not in self.cursor:
            raise ProtocolError("unsubscribed instrument")
        schedule = self._schedules[instrument]
        if candle.close_time_ms > now_ms or observation.received_at_ms < candle.close_time_ms:
            raise ProtocolError("open or future minute cannot be published")
        if (
            candle.close_time_ms - candle.open_time_ms != schedule.interval_ms
            or not schedule.contains_open(candle.open_time_ms)
        ):
            raise ProtocolError("bar does not match the instrument schedule")
        if observation.source not in {"rest", "websocket", "vision"}:
            raise ValidationError("unknown market observation source")
        return candle

    def _next_expected(self, candle):
        schedule = self._schedules[candle.instrument]
        return schedule.next_expected(candle.open_time_ms) or candle.close_time_ms

    def _check_capacity(self, opening, cursor, pending):
        if opening > cursor and opening not in pending and len(pending) >= self.max_pending:
            self.fault = "reorder_capacity"
            raise ProtocolError("minute reorder capacity exceeded; repair from durable history")

    def _publish_committed(self, candle):
        instrument = candle.instrument
        if candle.open_time_ms < self.cursor[instrument]:
            return 0
        pending = self._pending[instrument]
        pending[candle.open_time_ms] = candle
        count = 0
        while self.cursor[instrument] in pending:
            current = pending[self.cursor[instrument]]
            self.on_candle(current)  # Failure leaves cursor and pending retryable.
            del pending[current.open_time_ms]
            self.cursor[instrument] = self._next_expected(current)
            count += 1
        return count

    def accept(
        self, observation: MinuteObservation, *, now_ms: int, missing_only: bool = False
    ) -> int:
        candle = self._validate(observation, now_ms)
        instrument = candle.instrument
        self._check_capacity(
            candle.open_time_ms, self.cursor[instrument], self._pending[instrument]
        )
        try:
            if missing_only:
                # A websocket commit may have completed during the REST request.
                stored = self.store.get(instrument, candle.open_time_ms)
                if stored is None:
                    self.store.put(observation)
                else:
                    candle = stored
            else:
                self.store.put(observation)
            return self._publish_committed(candle)
        except Exception:
            self.fault = "storage_or_delivery"
            raise

    def accept_batch(self, observations, *, now_ms: int, missing_only: bool = False) -> int:
        # Simulate reorder capacity without publishing before the database confirms the write.
        cursors, queues = dict(self.cursor), {}
        for observation in observations:
            candle = self._validate(observation, now_ms)
            instrument, opening = candle.instrument, candle.open_time_ms
            if instrument not in queues:
                queues[instrument] = dict(self._pending[instrument])
            pending, cursor = queues[instrument], cursors[instrument]
            self._check_capacity(opening, cursor, pending)
            if opening >= cursor:
                pending[opening] = candle
                while current := pending.pop(cursor, None):
                    cursor = self._next_expected(current)
                cursors[instrument] = cursor
        try:
            committed = self.store.put_batch(observations, missing_only=missing_only)
            return sum(self._publish_committed(candle) for candle in committed)
        except Exception:
            self.fault = "storage_or_delivery"
            raise

    def missing_range(self, instrument: Instrument, now_ms: int, *, chunk: int = 500):
        start = self.cursor[instrument]
        return self._schedules[instrument].contiguous_missing_range(start, now_ms, chunk=chunk)

    def coverage_state(self, instrument: Instrument, now_ms: int) -> tuple[bool, bool]:
        """Return (calendar_known_now, has_due_missing_range) for health reporting."""
        schedule = self._schedules[instrument]
        return (
            schedule.coverage_known(now_ms),
            self.missing_range(instrument, now_ms) is not None,
        )

    def ready(self, instrument: Instrument, now_ms: int) -> bool:
        schedule = self._schedules[instrument]
        return (
            self.connected
            and self.fault is None
            and schedule.coverage_known(now_ms)
            and schedule.contiguous_missing_range(self.cursor[instrument], now_ms, chunk=1) is None
        )

    def repaired(self) -> None:
        """Owner clears a technical incident only after a successful recovery pass."""
        self.fault = None
