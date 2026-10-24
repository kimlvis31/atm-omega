"""Read-only collection worker: await I/O, REST repair and bounded reconnect delay."""

import asyncio
import random
import time
from collections.abc import Callable

from omega.application.collection import MinuteCollector
from omega.domain.errors import ProtocolError, RateLimited, TransportError, ValidationError
from omega.infrastructure.binance.streams import StreamPlan, connect_minutes
from omega.ports.brokers import MarketData
from omega.ports.collection import MinuteObservation


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
    ):
        if set(plan.instruments) != set(collector.cursor):
            raise ValidationError("stream and collector subscriptions must match")
        if not 0 < idle_seconds <= 60 or not 0 < session_seconds < 24 * 3600:
            raise ValidationError("invalid collection deadlines")
        self.plan, self.history, self.collector = plan, history, collector
        self.connect, self.clock_ms = connect, clock_ms
        self.idle_seconds, self.session_seconds = idle_seconds, session_seconds
        self.on_health = on_health

    async def repair(self) -> None:
        """A page at a time off the event loop; a real missing row remains a gap."""
        now = self.clock_ms()
        for instrument in self.plan.instruments:
            while request := self.collector.missing_range(instrument, now):
                start, end = request
                rows = await asyncio.to_thread(
                    self.history.closed_minutes, instrument, start, end, now_ms=now
                )
                for candle in rows:
                    if candle.instrument != instrument or not start <= candle.open_time_ms < end:
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

    async def session(self) -> None:
        """Open WS first so updates during history bootstrap are buffered boundedly."""
        self.collector.connected = False
        self.health("connecting")
        try:
            async with self.connect(self.plan) as socket:
                self.collector.connected = True
                await self.repair()
                self.health("gap")
                async with asyncio.timeout(self.session_seconds):
                    while True:
                        try:
                            raw = await asyncio.wait_for(socket.recv(), timeout=self.idle_seconds)
                        except TimeoutError:
                            # Healthy socket traffic is not evidence every symbol is current.
                            self.health("stale")
                            await self.repair()
                            self.health("gap")
                            continue
                        observation = self.plan.decode(raw, received_at_ms=self.clock_ms())
                        if observation is not None:
                            self.collector.accept(observation, now_ms=self.clock_ms())
                        if any(
                            self.collector.missing_range(i, self.clock_ms())
                            for i in self.plan.instruments
                        ):
                            self.health("gap")
                            await self.repair()
                        self.health("gap")
        finally:
            self.collector.connected = False
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
