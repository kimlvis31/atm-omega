"""Read-only collection worker: await I/O, REST repair and bounded reconnect delay."""

import asyncio
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, replace

from omega.application.collection import MinuteCollector
from omega.domain.errors import ProtocolError, RateLimited, TransportError, ValidationError
from omega.infrastructure.binance.streams import StreamPlan, connect_minutes
from omega.ports.brokers import MarketData
from omega.ports.collection import MinuteObservation, accept_rest_observations


@dataclass(frozen=True, slots=True)
class _PendingProtection:
    raw: bytes
    instrument: object
    raw_received_at_ms: int
    event_time_ms: int
    sequence: int
    connection: int
    deadline: float


MAX_PROTECTION_PENDING_BYTES = 1_048_576
MAX_PROTECTION_PENDING_RECORDS = 256


class CollectionWorker:
    def __init__(
        self,
        plan: StreamPlan,
        history: MarketData,
        collector: MinuteCollector,
        *,
        connect=connect_minutes,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        idle_seconds: float = 15,
        session_seconds: float = 23 * 3600,
        on_health: Callable[[bool, str], None] = lambda ready, reason: None,
        repair_interval_seconds: float = 5,
        monotonic: Callable[[], float] = time.monotonic,
        capture_rest_raw: bool = False,
        on_protection=None,
        protection_generation=None,
        on_protection_state=None,
    ):
        if set(plan.instruments) != set(collector.cursor):
            raise ValidationError("stream and collector subscriptions must match")
        if not 0 < idle_seconds <= 60 or not 0 < session_seconds < 24 * 3600:
            raise ValidationError("invalid collection deadlines")
        if not 0 < repair_interval_seconds <= 60:
            raise ValidationError("invalid repair interval")
        if (
            type(capture_rest_raw) is not bool
            or capture_rest_raw
            and not callable(getattr(history, "closed_observations", None))
        ):
            raise ValidationError("REST capture requires original observation history")
        if on_protection is not None and (
            not callable(on_protection) or type(protection_generation) is not str
        ):
            raise ValidationError("protection callback needs an explicit owned generation")
        if on_protection_state is not None and (
            not callable(on_protection_state) or on_protection is None
        ):
            raise ValidationError("protection state callback needs its observation callback")
        self.on_protection_state = on_protection_state
        self.on_protection, self.protection_generation = on_protection, protection_generation
        self._protection_connection, self._protection_sequence = 0, 0
        self._protection_pending, self._protection_last = {}, {}
        self._protection_bytes = 0
        self._protection_counts = {"coalesced": 0, "capacity": 0, "expired": 0, "cleared": 0}

        self.capture_rest_raw = capture_rest_raw
        self.plan, self.history, self.collector = plan, history, collector
        self.connect, self.clock_ms = connect, clock_ms
        self.idle_seconds, self.session_seconds = idle_seconds, session_seconds
        self.on_health = on_health
        self.repair_interval_seconds, self.monotonic = repair_interval_seconds, monotonic
        self._next_repair = 0.0

    async def repair(self) -> None:
        """A page at a time off the event loop; a real missing row remains a gap."""
        self._next_repair = self.monotonic() + self.repair_interval_seconds
        now = self.clock_ms()
        for instrument in self.plan.instruments:
            while request := self.collector.missing_range(instrument, now):
                start, end = request
                fetch_history = (
                    self.history.closed_observations
                    if self.capture_rest_raw
                    else self.history.closed_minutes
                )
                fetch = asyncio.create_task(
                    asyncio.to_thread(fetch_history, instrument, start, end, now_ms=now)
                )
                try:
                    rows = await asyncio.shield(fetch)
                except asyncio.CancelledError:
                    # Cancelling a to_thread await doesn't stop its HTTP thread.
                    # Reap this owned read before the caller closes the store;
                    # its late result must not publish after the stop request.
                    await asyncio.gather(fetch, return_exceptions=True)
                    raise
                if self.capture_rest_raw:
                    accepted_at = self.clock_ms()
                    observations = accept_rest_observations(
                        rows, instrument, start, end, accepted_at
                    )
                    before = self.collector.cursor[instrument]
                    for observation in sorted(observations, key=lambda o: o.candle.open_time_ms):
                        self.collector.accept(observation, now_ms=accepted_at)
                else:
                    for candle in rows:
                        if (
                            candle.instrument != instrument
                            or not start <= candle.open_time_ms < end
                        ):
                            raise ProtocolError("history returned an out-of-range minute")
                    before = self.collector.cursor[instrument]
                    for candle in sorted(rows, key=lambda c: c.open_time_ms):
                        self.collector.accept(MinuteObservation(candle, "rest", now), now_ms=now)
                if self.collector.cursor[instrument] == before:
                    break  # No zero-filling or spinning on an absent minute.
        self.collector.repaired()

    def health(self, reason: str) -> None:
        ready = all(self.collector.ready(i, self.clock_ms()) for i in self.plan.instruments)
        self.on_health(ready, "ready" if ready else reason)

    def protection_pending_status(self):
        return {
            **self._protection_counts,
            "records": sum(map(len, self._protection_pending.values())),
            "bytes": self._protection_bytes,
        }

    def _clear_protection_pending(self):
        self._protection_counts["cleared"] += sum(map(len, self._protection_pending.values()))
        self._protection_pending.clear()
        self._protection_bytes = 0

    def _emit_protection(self, observation):
        previous = self._protection_last.get(observation.instrument)
        if previous is not None and (
            observation.sequence <= previous[0] or observation.event_time_ms < previous[1]
        ):
            raise ProtocolError("partial source order moved backwards")
        self.on_protection(observation)
        self._protection_last[observation.instrument] = (
            observation.sequence,
            observation.event_time_ms,
        )

    def _drain_protection(self):
        now, monotonic = self.clock_ms(), self.monotonic()
        for instrument in tuple(self._protection_pending):
            rows = self._protection_pending[instrument]
            while rows:
                row = rows[0]
                expired = (
                    row.connection != self._protection_connection
                    or monotonic > row.deadline
                    or not 0 <= now - row.raw_received_at_ms <= 5000
                )
                if not expired and now < row.event_time_ms:
                    break
                rows.pop(0)
                self._protection_bytes -= len(row.raw)
                if expired:
                    self._protection_counts["expired"] += 1
                    continue
                observation = self.plan.decode_protection(
                    row.raw,
                    received_at_ms=now,
                    generation=self.protection_generation,
                    connection=row.connection,
                    sequence=row.sequence,
                    raw_received_at_ms=row.raw_received_at_ms,
                )
                if observation is not None:
                    try:
                        observation.current(now)
                    except ValidationError:
                        self._protection_counts["expired"] += 1
                    else:
                        self._emit_protection(observation)
            if not rows:
                del self._protection_pending[instrument]

    def _queue_protection(self, raw, received_at_ms, instrument, event):
        encoded = raw.encode("utf-8") if isinstance(raw, str) else raw
        rows = self._protection_pending.get(instrument, [])
        if rows and event < rows[-1].event_time_ms:
            raise ProtocolError("partial source event moved backwards while pending")
        previous = self._protection_last.get(instrument)
        if previous is not None and event < previous[1]:
            raise ProtocolError("partial source event predates accepted evidence")
        replaced = rows[-1] if len(rows) == 2 else None
        size = self._protection_bytes - (len(replaced.raw) if replaced else 0) + len(encoded)
        count = sum(map(len, self._protection_pending.values())) + (0 if replaced else 1)
        if (
            len(encoded) > 65536
            or size > MAX_PROTECTION_PENDING_BYTES
            or count > MAX_PROTECTION_PENDING_RECORDS
        ):
            self._protection_counts["capacity"] += 1
            return
        row = _PendingProtection(
            encoded,
            instrument,
            received_at_ms,
            event,
            self._protection_sequence,
            self._protection_connection,
            self.monotonic() + 2,
        )
        if replaced is not None:
            rows[-1] = row
            self._protection_counts["coalesced"] += 1
        else:
            rows.append(row)
        self._protection_pending[instrument] = rows
        self._protection_bytes = size

    def _accept_protection(self, raw, received_at_ms):
        # Mature the retained oldest before any capacity/coalescing decision.
        self._drain_protection()
        self._protection_sequence += 1
        future = []
        partial = self.plan.decode_protection(
            raw,
            received_at_ms=received_at_ms,
            generation=self.protection_generation,
            connection=self._protection_connection,
            sequence=self._protection_sequence,
            on_future=lambda instrument, event: future.append((instrument, event)),
        )
        if future:
            self._queue_protection(raw, received_at_ms, *future[0])
            self._drain_protection()
        elif partial is not None:
            if self._protection_pending.get(partial.instrument):
                raise ProtocolError("current partial predates the pending source head")
            self._emit_protection(partial)

    def _protection_timeout(self, idle_deadline):
        now, monotonic = self.clock_ms(), self.monotonic()
        delays = [max(0, idle_deadline - monotonic)]
        for rows in self._protection_pending.values():
            head = rows[0]
            delays.append(
                max(0.001, min((head.event_time_ms - now) / 1000, head.deadline - monotonic))
            )
        return max(0.001, min(delays))

    async def _protection_stream(self, socket):
        """One owned recv task; timer wakes do not renew the socket idle deadline."""
        receiver = None
        idle_deadline = self.monotonic() + self.idle_seconds

        async def receive():
            raw = await socket.recv()
            return raw, self.clock_ms()

        try:
            while True:
                self._drain_protection()
                if receiver is None:
                    receiver = asyncio.create_task(receive())
                done, _ = await asyncio.wait(
                    {receiver}, timeout=self._protection_timeout(idle_deadline)
                )
                if not done:
                    self._drain_protection()
                    if self.monotonic() < idle_deadline:
                        continue
                    receiver.cancel()
                    outcome = (await asyncio.gather(receiver, return_exceptions=True))[0]
                    receiver = None
                    if isinstance(outcome, tuple):
                        raw, receipt = outcome
                    else:
                        if not isinstance(outcome, asyncio.CancelledError):
                            raise outcome
                        self.health("stale")
                        if self.monotonic() >= self._next_repair:
                            await self.repair()
                        self.health("gap")
                        idle_deadline = self.monotonic() + self.idle_seconds
                        continue
                else:
                    raw, receipt = receiver.result()
                    receiver = None
                idle_deadline = self.monotonic() + self.idle_seconds
                await self.accept_stream(raw, received_at_ms=receipt)
                if any(
                    self.collector.missing_range(i, self.clock_ms()) for i in self.plan.instruments
                ):
                    self.health("gap")
                    if self.monotonic() >= self._next_repair:
                        await self.repair()
                self.health("gap")
        finally:
            if receiver is not None:
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)

    async def accept_stream(self, raw, *, received_at_ms):
        """Wait for a slightly early closed frame; never publish a future minute.

        Binance's clock may lead the host by a few hundred milliseconds. The
        original transport receipt remains raw provenance; the canonical receipt
        is the actual local acceptance time after the minute has closed locally.
        A stopped/backward host clock cannot extend the monotonic two-second cap.
        """
        if self.on_protection is not None:
            self._accept_protection(raw, received_at_ms)
        observation = self.plan.decode(raw, received_at_ms=received_at_ms, future_tolerance_ms=2000)
        if observation is None:
            return None
        deadline = self.monotonic() + 2
        while True:
            now = self.clock_ms()
            if now >= observation.candle.close_time_ms:
                break
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                raise TransportError("local clock did not reach the closed minute in time")
            await asyncio.sleep(
                min(0.05, remaining, (observation.candle.close_time_ms - now) / 1000)
            )
        accepted = replace(
            observation,
            received_at_ms=now,
            raw_source="websocket",
            raw_received_at_ms=received_at_ms,
        )
        self.collector.accept(accepted, now_ms=now)
        return accepted

    async def session(self) -> None:
        """Open WS first so updates during history bootstrap are buffered boundedly."""
        self.collector.connected = False
        self.health("connecting")
        try:
            async with self.connect(self.plan) as socket:
                self.collector.connected = True
                if self.on_protection is not None:
                    self._protection_connection += 1
                    self._protection_sequence = 0
                    self._clear_protection_pending()
                    self._protection_last.clear()
                    if self.on_protection_state is not None:
                        self.on_protection_state(self._protection_connection, True)
                    self.on_protection(None)
                await self.repair()
                self.health("gap")
                async with asyncio.timeout(self.session_seconds):
                    if self.on_protection is not None:
                        await self._protection_stream(socket)
                    else:
                        while True:
                            try:
                                raw = await asyncio.wait_for(
                                    socket.recv(), timeout=self.idle_seconds
                                )
                            except TimeoutError:
                                # Healthy socket traffic is not evidence every symbol is current.
                                self.health("stale")
                                if self.monotonic() >= self._next_repair:
                                    await self.repair()
                                self.health("gap")
                                continue
                            await self.accept_stream(raw, received_at_ms=self.clock_ms())
                            if any(
                                self.collector.missing_range(i, self.clock_ms())
                                for i in self.plan.instruments
                            ):
                                self.health("gap")
                                if self.monotonic() >= self._next_repair:
                                    await self.repair()
                            self.health("gap")
        finally:
            self.collector.connected = False
            self._clear_protection_pending()
            if self.on_protection is not None:
                if self.on_protection_state is not None:
                    self.on_protection_state(self._protection_connection, False)
                self.on_protection(None)
            self.health("disconnected")

    async def run(self, stop: asyncio.Event) -> None:
        """Stop interrupts recv/backoff; storage/schema/delivery failures propagate."""
        from websockets.exceptions import ConnectionClosed

        failures = 0
        while not stop.is_set():
            task, stopped = asyncio.create_task(self.session()), asyncio.create_task(stop.wait())
            try:
                done, _ = await asyncio.wait({task, stopped}, return_when=asyncio.FIRST_COMPLETED)
                if stopped in done:
                    return
                try:
                    await task
                except RateLimited as exc:
                    self.health("rate_limited")
                    delay = max(1.0, exc.retry_after or 1.0)
                except (OSError, TimeoutError, TransportError, ConnectionClosed):
                    failures = min(failures + 1, 6)
                    delay = min(60.0, 2**failures) + random.uniform(0, 1)
                else:
                    delay = 1.0
            finally:
                for pending in (task, stopped):
                    pending.cancel()
                await asyncio.gather(task, stopped, return_exceptions=True)
            # Event-based wait; no busy queue polling. No retry of trading requests exists here.
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                pass
