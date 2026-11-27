"""Live public auxiliary feeds for the standalone collector."""

import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from omega.application.trade_flow import trade_flow_values
from omega.domain.errors import (
    BrokerRejected,
    ProtocolError,
    RateLimited,
    StorageError,
    TransportError,
    ValidationError,
)
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Market
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.binance.auxiliary import agg_trade
from omega.infrastructure.binance.auxiliary_client import BinanceAuxiliaryClient
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.depth_book import DepthBook
from omega.infrastructure.binance.depth_sampling import SOURCE_SUFFIX, depth_bins

MINUTE = 60_000
METRIC_PERIOD = 5 * MINUTE
MAX_TRADE_EVENTS = 30_000
MAX_TRADE_REPAIR = 1000
FEED_ERRORS = (BrokerRejected, ProtocolError, TransportError, ValidationError, OSError)
METRICS = (
    ("open_interest_history", "open_interest", ("METRIC_OPENINTEREST", "METRIC_OPENINTERESTVALUE")),
    (
        "long_short_ratio",
        "long_short_ratio",
        ("METRIC_LONGSHORTRATIO", "METRIC_LONGACCOUNT", "METRIC_SHORTACCOUNT"),
    ),
)


def now_ms():
    return time.time_ns() // 1_000_000


def minute(timestamp):
    return timestamp // MINUTE * MINUTE


def report(channel, instrument, state, **details):
    print(
        json.dumps(
            {"aux_channel": channel, "instrument": instrument.key, "state": state, **details}
        ),
        flush=True,
    )


def stream_url(instrument, channel):
    spot = instrument.market == Market.SPOT
    if spot:
        base = "wss://stream.binance.com:443"
    else:
        base = "wss://fstream.binance.com" + ("/public" if channel == "depth" else "/market")
    stream = "depth@100ms" if channel == "depth" else "aggTrade"
    return f"{base}/ws/{instrument.symbol.lower()}@{stream}"


class TradeMinute:
    def __init__(self, instrument):
        self.instrument = instrument
        self.previous = None
        self.events = []
        self.complete = False

    def accept(self, event, received):
        previous = self.previous
        if previous is not None and event.aggregate_id <= previous.aggregate_id:
            return
        opening = minute(event.trade_time_ms)
        if previous is not None and previous.trade_time_ms > event.trade_time_ms:
            raise ProtocolError("aggregate trade timestamp reversed")
        continuous = previous is not None and event.aggregate_id == previous.aggregate_id + 1
        self.previous = event
        if previous is not None and (prior := minute(previous.trade_time_ms)) < opening:
            yield self.observation(prior, received, self.complete and continuous)
            self.events.clear()
            # An unbroken ID pair proves there were no trades in intervening minutes.
            if continuous:
                for empty in range(max(prior + MINUTE, opening - 10 * MINUTE), opening, MINUTE):
                    yield self.observation(empty, received, True)
            self.complete = continuous
        elif not continuous:
            self.complete = False
        if len(self.events) >= MAX_TRADE_EVENTS:
            self.complete = False
        else:
            self.events.append(event)

    def observation(self, opening, received, complete):
        return AuxiliaryInput(
            "trade_flow",
            trade_flow_values(self.events),
            opening,
            opening + MINUTE,
            opening + MINUTE,
            received,
            "observed" if complete else "partial",
            Decimal(1 if complete else 0),
            f"{self.instrument.key}:aggTrade",
        )


class LiveAuxiliary:
    def __init__(self, settings, clients):
        self.settings = settings
        self.clients = clients
        self.api = {
            market: BinanceAuxiliaryClient(market, client) for market, client in clients.items()
        }
        # REST uses the collector's existing shared on-disk IP-weight ledger.
        self.metric_requests = RequestBudget((Window("metrics", 100, 300),))
        self.connections = RequestBudget((Window("websocket", 120, 300),))
        self.store = None
        self.storage = ThreadPoolExecutor(max_workers=1, thread_name_prefix="auxiliary-db")

    def _close(self):
        if self.store is not None:
            self.store.close()
            self.store = None

    def _put(self, instrument, observation):
        for _ in range(2):
            try:
                if self.store is None:
                    self.store = PostgresAuxiliaryStore(self.settings)
                stored = self.store.get(
                    instrument,
                    observation.channel,
                    observation.source,
                    observation.window_start_ms,
                    as_of_ms=now_ms(),
                )
                # Repeated 5m polls and restarts preserve the first availability time.
                if stored is None:
                    self.store.put(instrument, observation)
                    return True
                if stored.observation.values != observation.values and observation.channel in {
                    "open_interest",
                    "long_short_ratio",
                }:
                    self.store.put(instrument, observation, expected_previous=stored.checksum)
                    return True
                return False
            except StorageError:
                self._close()
        raise StorageError("auxiliary database unavailable")

    async def write(self, instrument, observation):
        try:
            return await asyncio.get_running_loop().run_in_executor(
                self.storage, self._put, instrument, observation
            )
        except (StorageError, ProtocolError) as exc:
            report(observation.channel, instrument, "storage_error", error=type(exc).__name__)
            return False

    async def _trade_session(self, socket, instrument, rollup):
        report("trade_flow", instrument, "subscribed")
        async for raw in socket:
            received = now_ms()
            event = agg_trade(
                instrument,
                raw.encode() if isinstance(raw, str) else raw,
                available_at_ms=received,
                source="websocket",
            ).as_flow_event()
            previous = rollup.previous
            if previous is not None:
                missing = event.aggregate_id - previous.aggregate_id - 1
                if missing > 0:
                    recovered = 0
                    if missing <= MAX_TRADE_REPAIR:
                        try:
                            batch = await asyncio.to_thread(
                                self.api[instrument.market].agg_trades,
                                instrument,
                                from_id=previous.aggregate_id + 1,
                                limit=missing,
                            )
                            repaired = tuple(item.as_flow_event() for item in batch.events)
                            if (
                                len(repaired) == missing
                                and all(
                                    item.aggregate_id == previous.aggregate_id + index
                                    and previous.trade_time_ms
                                    <= item.trade_time_ms
                                    <= event.trade_time_ms
                                    for index, item in enumerate(repaired, 1)
                                )
                                and all(
                                    a.trade_time_ms <= b.trade_time_ms
                                    for a, b in zip(repaired, repaired[1:], strict=False)
                                )
                            ):
                                for item in repaired:
                                    for observation in rollup.accept(item, now_ms()):
                                        await self.write(instrument, observation)
                                recovered = len(repaired)
                        except RateLimited as exc:
                            report("trade_flow", instrument, "throttled", seconds=exc.retry_after)
                        except FEED_ERRORS:
                            pass
                    report("trade_flow", instrument, "gap", missing=missing, recovered=recovered)
                    received = now_ms()
            for observation in rollup.accept(event, received):
                await self.write(instrument, observation)

    async def _snapshot(self, instrument):
        spot = instrument.market == Market.SPOT
        return await asyncio.to_thread(
            self.clients[instrument.market].request,
            "GET",
            "/api/v3/depth" if spot else "/fapi/v1/depth",
            {"symbol": instrument.symbol, "limit": 1000},
            weight=50 if spot else 20,
        )

    async def _depth_session(self, socket, instrument):
        # Refresh the snapshot on socket replacement or an ID discontinuity.
        book = DepthBook(instrument, max_pending=1024, max_levels=20_000)
        source = f"{instrument.key}:{SOURCE_SUFFIX}"
        started = now_ms()
        opening = minute(started)
        latest = None
        snapshot = None
        pending = None
        synchronized_at = None
        report("depth", instrument, "subscribed")
        try:
            while True:
                now = now_ms()
                if now >= opening + MINUTE:
                    if latest is not None and book.state == "synced":
                        event, received = latest
                        view = book.view()
                        # One extra digit represents the half-tick mid price.
                        precision = min(
                            18,
                            max(
                                8,
                                1 - view.bids[0][0].as_tuple().exponent,
                                1 - view.asks[0][0].as_tuple().exponent,
                            ),
                        )
                        fields, coverage, center = depth_bins(
                            view, price_precision=precision, quote_precision=8
                        )
                        complete = (
                            synchronized_at <= opening
                            and opening + MINUTE - event <= 15_000
                            and coverage == 1
                        )
                        await self.write(
                            instrument,
                            AuxiliaryInput(
                                "depth",
                                (
                                    *fields,
                                    ("DEPTH_CENTER_PRICE", center),
                                    ("DEPTH_UPDATE_ID", view.update_id),
                                    ("DEPTH_RECEIVED_AT_MS", received),
                                    ("DEPTH_PROFILE_VERSION", 1),
                                    ("DEPTH_EVENT_CLOCK", int(instrument.market == Market.USDM)),
                                ),
                                opening,
                                opening + MINUTE,
                                event,
                                now,
                                "observed" if complete else "partial",
                                coverage,
                                source,
                            ),
                        )
                    opening = minute(now)
                    latest = None
                if snapshot is not None and snapshot.done():
                    if book.snapshot(snapshot.result(), generation=book.generation):
                        synchronized_at = now_ms()
                        report("depth", instrument, "synchronized")
                    snapshot = None
                    # No point is published until a timestamped diff is applied after the snapshot.
                wait = min(1, max(0.001, (opening + MINUTE - now) / 1000))
                if pending is None:
                    try:
                        raw = await asyncio.wait_for(socket.recv(), wait)
                    except TimeoutError:
                        continue
                    received = now_ms()
                    if received >= opening + MINUTE:
                        pending = raw, received
                        continue
                else:
                    raw, received = pending
                    pending = None
                payload = json.loads(raw)
                if not isinstance(payload, dict):
                    raise ProtocolError("invalid depth stream envelope")
                applied = book.receive(payload, generation=book.generation)
                if book.state == "waiting_snapshot" and snapshot is None:
                    snapshot = asyncio.create_task(self._snapshot(instrument))
                event = payload.get("T", payload["E"])
                if applied:
                    if synchronized_at is None:
                        synchronized_at = received
                        report("depth", instrument, "synchronized")
                    if opening <= event < opening + MINUTE and event <= received:
                        latest = event, received
        finally:
            if snapshot is not None:
                snapshot.cancel()
                await asyncio.gather(snapshot, return_exceptions=True)

    async def feed(self, instrument, channel):
        from websockets.asyncio.client import connect
        from websockets.exceptions import WebSocketException

        rollup = TradeMinute(instrument) if channel == "trade_flow" else None
        delay = 8
        while True:
            started = time.monotonic()
            try:
                self.connections.reserve({"websocket": 1})
                async with connect(
                    stream_url(instrument, channel),
                    open_timeout=12,
                    max_size=1_048_576,
                    max_queue=64,
                    proxy=None,
                ) as socket:
                    if rollup is None:
                        await self._depth_session(socket, instrument)
                    else:
                        await self._trade_session(socket, instrument, rollup)
            except RateLimited as exc:
                report(channel, instrument, "throttled", seconds=exc.retry_after)
                await asyncio.sleep(max(1, exc.retry_after or 0) + 1)
                continue
            except (*FEED_ERRORS, WebSocketException, TimeoutError, ValueError) as exc:
                report(channel, instrument, "reconnect", error=type(exc).__name__)
            delay = 8 if time.monotonic() - started > 120 else min(delay * 2, 120)
            await asyncio.sleep(delay)

    async def metrics(self, instrument):
        while True:
            now = now_ms()
            # Allow publication latency. Overlap the last three points and deduplicate in SQL.
            for dataset, channel, names in METRICS:
                try:
                    self.metric_requests.reserve({"metrics": 1})
                    batch = await asyncio.to_thread(
                        getattr(self.api[instrument.market], dataset),
                        instrument,
                        start_ms=now - 3 * METRIC_PERIOD,
                        end_ms=now,
                        limit=3,
                    )
                    for sample in batch.events:
                        opening = minute(sample.event_time_ms)
                        await self.write(
                            instrument,
                            AuxiliaryInput(
                                channel,
                                (
                                    *zip(names, sample.values, strict=True),
                                    ("SOURCE_PERIOD_MS", METRIC_PERIOD),
                                ),
                                opening,
                                opening + MINUTE,
                                sample.event_time_ms,
                                sample.available_at_ms,
                                source=f"{instrument.key}:{dataset}",
                            ),
                        )
                    if not batch.events:
                        report(channel, instrument, "no_sample")
                except RateLimited as exc:
                    report(channel, instrument, "throttled", seconds=exc.retry_after)
                    await asyncio.sleep(max(1, exc.retry_after or 0) + 1)
                except FEED_ERRORS as exc:
                    report(
                        channel,
                        instrument,
                        "request_error",
                        error=type(exc).__name__,
                        status=getattr(exc, "status", None),
                        code=getattr(exc, "code", None),
                    )
            now = now_ms()
            next_poll = ((now - 45_000) // METRIC_PERIOD + 1) * METRIC_PERIOD + 45_000
            await asyncio.sleep((next_poll - now) / 1000)

    async def run(self, instruments, stop):
        async def stagger(seconds, action, *args):
            await asyncio.sleep(seconds)
            await action(*args)

        try:
            async with asyncio.TaskGroup() as group:
                tasks = []
                for index, instrument in enumerate(instruments):
                    tasks.extend(
                        (
                            group.create_task(stagger(index * 4, self.feed, instrument, "depth")),
                            group.create_task(stagger(index, self.feed, instrument, "trade_flow")),
                        )
                    )
                    if instrument.market == Market.USDM:
                        tasks.append(
                            group.create_task(stagger(index * 2, self.metrics, instrument))
                        )
                await stop.wait()
                for task in tasks:
                    task.cancel()
        finally:
            await asyncio.get_running_loop().run_in_executor(self.storage, self._close)
            self.storage.shutdown()
