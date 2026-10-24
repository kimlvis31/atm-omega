"""Explicit public-data-only entry point. Does not load accounts or trading adapters."""

import argparse
import asyncio
import json
import signal
import time
from pathlib import Path

from omega.application.collection import MinuteCollector
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.market import BinanceMarketData
from omega.infrastructure.binance.streams import StreamPlan
from omega.infrastructure.binance.transport import UrllibTransport
from omega.infrastructure.candles import SqliteCandleStore
from omega.runtime.collection import CollectionWorker


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Collect public Binance closed 1m candles only")
    result.add_argument("--market", choices=["spot", "usdm"], required=True)
    result.add_argument("--symbols", nargs="+", required=True)
    result.add_argument("--database", type=Path, required=True)
    result.add_argument("--lookback-minutes", type=int, default=500)
    return result


async def collect(args) -> None:
    market = Market(args.market)
    instruments = tuple(
        sorted({Instrument("binance", market, s) for s in args.symbols}, key=lambda i: i.key)
    )
    plan = StreamPlan(market, instruments)
    if not 1 <= args.lookback_minutes <= 20_000:
        raise ValueError("lookback-minutes must be 1..20000")
    now = time.time_ns() // 1_000_000
    start = now // 60_000 * 60_000 - args.lookback_minutes * 60_000
    args.database.parent.mkdir(parents=True, exist_ok=True)
    store = SqliteCandleStore(args.database)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # Windows asyncio.run handles KeyboardInterrupt and cancels cleanly.
    try:
        # A conservative configurable composition limit, not a claim of provider quota discovery.
        budget = RequestBudget((Window("binance-ip", 1000, 60),))
        client = BinanceClient(
            "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com",
            UrllibTransport(),
            budget=budget,
        )
        history = BinanceMarketData(market, client)
        collector = MinuteCollector({i: start for i in instruments}, store, lambda candle: None)
        last_health = None

        def health(ready, reason):
            nonlocal last_health
            current = (ready, reason)
            if current != last_health:
                print(json.dumps({"ready": ready, "reason": reason}), flush=True)
                last_health = current

        worker = CollectionWorker(plan, history, collector, on_health=health)
        await worker.run(stop)
    finally:
        store.close()


def main() -> None:
    args = parser().parse_args()
    try:
        asyncio.run(collect(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
