"""Explicit public-data-only entry point. Does not load accounts or trading adapters."""

import argparse
import asyncio
import json
import signal
import time
from pathlib import Path

from omega.application.collection import MinuteCollector
from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.market import BinanceMarketData
from omega.infrastructure.binance.shared_budget import SharedRequestBudget
from omega.infrastructure.binance.streams import StreamPlan
from omega.infrastructure.binance.transport import UrllibTransport
from omega.infrastructure.candles import PostgresCandleStore
from omega.infrastructure.postgres.config import load_database_settings
from omega.runtime.collection import CollectionWorker


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Collect public Binance closed 1m candles only")
    result.add_argument("--market", choices=["spot", "usdm"], required=True)
    result.add_argument("--symbols", nargs="+", required=True)
    result.add_argument("--db-config", type=Path, default=Path("database.config.toml"))
    result.add_argument("--lookback-minutes", type=int, default=500)
    result.add_argument(
        "--capture-rest-raw",
        action="store_true",
        help="Archive original public REST responses and bounded row fragments",
    )
    result.add_argument(
        "--raw-directory", type=Path, help="Capture directory; requires --capture-rest-raw"
    )
    return result


async def collect(args) -> None:
    capture = getattr(args, "capture_rest_raw", False)
    directory = getattr(args, "raw_directory", None)
    if type(capture) is not bool or not capture and directory is not None:
        raise ValidationError("raw directory requires explicit REST capture")
    if directory is not None and (
        not isinstance(directory, (str, Path))
        or not str(directory)
        or any(ord(char) < 32 for char in str(directory))
    ):
        raise ValidationError("raw directory must be a local filesystem path")
    archive = None
    market = Market(args.market)
    instruments = tuple(
        sorted({Instrument("binance", market, s) for s in args.symbols}, key=lambda i: i.key)
    )
    plan = StreamPlan(market, instruments)
    if not 1 <= args.lookback_minutes <= 20_000:
        raise ValueError("lookback-minutes must be 1..20000")
    if capture:
        from omega.infrastructure.raw_archive import FileRawArchive

        archive = FileRawArchive(
            args.db_config.resolve().parent / "data/raw/candles" if directory is None else directory
        )
    now = time.time_ns() // 1_000_000
    start = now // 60_000 * 60_000 - args.lookback_minutes * 60_000
    store = PostgresCandleStore(load_database_settings(args.db_config))
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # Windows asyncio.run handles KeyboardInterrupt and cancels cleanly.
    try:
        # A conservative configurable composition limit, not a claim of provider quota discovery.
        budget = SharedRequestBudget(
            args.db_config.resolve().parent / "state" / "binance-public-budget.json"
        )
        client = BinanceClient(
            "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com",
            UrllibTransport(),
            budget=budget,
        )
        history = (
            BinanceMarketData(market, client, raw_archive=archive)
            if capture
            else BinanceMarketData(market, client)
        )
        collector = MinuteCollector({i: start for i in instruments}, store, lambda candle: None)
        last_health = None

        def health(ready, reason):
            nonlocal last_health
            current = (ready, reason)
            if current != last_health:
                print(json.dumps({"ready": ready, "reason": reason}), flush=True)
                last_health = current

        worker = CollectionWorker(
            plan,
            history,
            collector,
            on_health=health,
            **({"capture_rest_raw": True} if capture else {}),
        )
        await worker.run(stop)
    finally:
        store.close()


def main() -> None:
    arguments = parser()
    args = arguments.parse_args()
    if args.raw_directory is not None and not args.capture_rest_raw:
        arguments.error("--raw-directory requires --capture-rest-raw")
    try:
        asyncio.run(collect(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
