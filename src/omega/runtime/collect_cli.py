"""Explicit public-data-only entry point. Does not load accounts or trading adapters."""

import argparse
import asyncio
import json
import signal
import sys
import time
from contextlib import closing
from pathlib import Path

from omega.application.collection import MinuteCollector
from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.market import BinanceMarketData
from omega.infrastructure.binance.shared_budget import SharedRequestBudget
from omega.infrastructure.binance.streams import StreamPlan
from omega.infrastructure.binance.transport import UrllibTransport
from omega.infrastructure.candles import DatabaseUnavailable, PostgresCandleStore
from omega.infrastructure.postgres.config import load_database_settings
from omega.runtime.collection import CollectionWorker
from omega.runtime.collector_server import CollectorAPIServer, read_api_token


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Collect public Binance closed one-minute bars",
        epilog="For base-bar history, verification and reads, use omega-collect bars --help.",
    )
    market_selection = result.add_mutually_exclusive_group(required=True)
    market_selection.add_argument("--market", choices=["spot", "usdm"])
    market_selection.add_argument(
        "--subscription",
        action="append",
        metavar="MARKET:SYMBOL",
        help="Repeat to collect Spot and USD-M Futures in a single process",
    )
    result.add_argument("--symbols", nargs="+", help="One or more symbols with --market")
    result.add_argument("--fetch-workers", type=int, default=12)
    result.add_argument("--ipc-root", type=Path, help="Use the standalone data manager")
    result.add_argument("--db-config", type=Path, default=Path("database.config.toml"))
    result.add_argument("--request-budget-file", type=Path, help="Shared local REST budget file")
    start = result.add_mutually_exclusive_group()
    start.add_argument("--lookback-minutes", type=int, help="Legacy 1m lookback (default 500)")
    start.add_argument(
        "--series-effective-from-ms",
        type=int,
        help="Use committed base-bar storage with a fixed first UTC opening and durable resume",
    )
    result.add_argument(
        "--capture-rest-raw",
        action="store_true",
        help="Archive original public REST responses and bounded row fragments",
    )
    result.add_argument(
        "--raw-directory", type=Path, help="Capture directory; requires --capture-rest-raw"
    )
    result.add_argument(
        "--api-port",
        type=int,
        help="Enable authenticated loopback read API (0 selects an ephemeral port)",
    )
    result.add_argument(
        "--api-token-file", type=Path, help="Bearer token file required with --api-port"
    )
    result.add_argument(
        "--collect-auxiliary",
        action="store_true",
        help="Also collect live depth, aggregate trades and USD-M metrics",
    )
    return result


class _CollectionTransport:
    def __init__(self, transport, market):
        self.transport = transport
        self.market = market

    def send(self, request):
        response = self.transport.send(request)
        headers = {key.lower(): value for key, value in response.headers.items()}
        used_weight = headers.get("x-mbx-used-weight-1m")
        print(
            json.dumps(
                {
                    "rest_response": response.status,
                    "market": self.market.value,
                    "used_weight_1m": used_weight
                    if used_weight and used_weight.isdecimal()
                    else None,
                }
            ),
            flush=True,
        )
        return response


def _stream_plans(args) -> tuple[StreamPlan, ...]:
    subscriptions = getattr(args, "subscription", None)
    groups = {}
    if subscriptions is None:
        if not args.symbols:
            raise ValidationError("--market requires --symbols")
        market = Market(args.market)
        groups[market] = {Instrument("binance", market, symbol) for symbol in args.symbols}
    else:
        if args.symbols:
            raise ValidationError("use MARKET:SYMBOL with --subscription")
        for value in subscriptions:
            try:
                market_name, symbol = value.split(":")
                market = Market(market_name)
            except ValueError:
                raise ValidationError(
                    "--subscription requires spot:SYMBOL or usdm:SYMBOL"
                ) from None
            if market not in {Market.SPOT, Market.USDM}:
                raise ValidationError("--subscription requires spot:SYMBOL or usdm:SYMBOL")
            groups.setdefault(market, set()).add(Instrument("binance", market, symbol))
    return tuple(
        StreamPlan(market, tuple(sorted(instruments, key=lambda item: item.key)))
        for market, instruments in sorted(groups.items())
    )


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
    ipc_root = getattr(args, "ipc_root", None)
    plans = _stream_plans(args)
    instruments = tuple(instrument for plan in plans for instrument in plan.instruments)
    lookback = getattr(args, "lookback_minutes", None)
    lookback = 500 if lookback is None else lookback
    if not 1 <= lookback <= 20_000:
        raise ValueError("lookback-minutes must be 1..20000")
    if capture:
        from omega.infrastructure.raw_archive import FileRawArchive

        archive = FileRawArchive(
            args.db_config.resolve().parent / "data/raw/candles" if directory is None else directory
        )
    now = time.time_ns() // 1_000_000
    effective = getattr(args, "series_effective_from_ms", None)
    if ipc_root is not None and (effective is None or getattr(args, "api_port", None) is not None):
        raise ValidationError("split collection requires a fixed series and the data manager API")
    settings = load_database_settings(args.db_config) if ipc_root is None else None
    store = None
    native_series = None
    if effective is None:
        start = now // 60_000 * 60_000 - lookback * 60_000
        starts = dict.fromkeys(instruments, start)
        store = PostgresCandleStore(settings)
    else:
        from omega.infrastructure.binance_series_minutes import binance_minute_series

        native_series = binance_minute_series(instruments, effective)
        if effective > now:
            raise ValidationError("live series cannot begin at a future bar opening")
    series_ids = (
        None
        if native_series is None
        else {key.key: value.series_id for key, value in native_series.items()}
    )
    api = None
    auxiliary_task = None
    source = None
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass  # Windows asyncio.run handles KeyboardInterrupt and cancels cleanly.
    from contextlib import ExitStack

    socket_stack = ExitStack()
    try:
        if ipc_root is not None:
            from omega.runtime.market_history_fetch import MarketHistoryFetch
            from omega.runtime.market_ipc import serve_market_socket

            fetch = MarketHistoryFetch(ipc_root, instruments, stop)
            socket_stack.enter_context(
                serve_market_socket(
                    ipc_root / "ipc" / "market.sock", fetch.dispatch, capacity=args.fetch_workers
                )
            )
        if getattr(args, "api_port", None) is not None:
            token = read_api_token(args.api_token_file)
            ready = {"api_ready": True}
            if series_ids is None:
                from omega.infrastructure.candle_feed import CommittedCandleSource

                source = CommittedCandleSource(
                    args.db_config.resolve().parent, config_path=args.db_config
                )
                api = CollectorAPIServer(source, token, args.api_port)
            else:
                from omega.infrastructure.bar_series_store import PostgresBarSeriesStore

                api = CollectorAPIServer(
                    None,
                    token,
                    args.api_port,
                    series_store=lambda: PostgresBarSeriesStore(settings, read_only=True),
                )
                ready["series"] = series_ids
            api.start()
            print(json.dumps({**ready, "port": api.port}), flush=True)
        elif series_ids is not None:
            print(json.dumps({"series": series_ids}), flush=True)
        # A conservative configurable composition limit, not a claim of provider quota discovery.
        budget_path = getattr(args, "request_budget_file", None)
        if budget_path is None:
            budget_path = args.db_config.resolve().parent / "state" / "binance-public-budget.json"
        budget = SharedRequestBudget(budget_path)
        histories = {}
        clients = {}
        for plan in plans:
            market = plan.market
            transport = UrllibTransport()
            if native_series is not None:
                transport = _CollectionTransport(transport, market)
            client = BinanceClient(
                "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com",
                transport,
                budget=budget,
            )
            clients[market] = client
            histories[market] = (
                BinanceMarketData(market, client, raw_archive=archive)
                if capture
                else BinanceMarketData(market, client)
            )
        last_health = {}

        def health(ready, reason, *, market=None):
            current = (ready, reason)
            if current != last_health.get(market):
                message = {"ready": ready, "reason": reason}
                if len(plans) > 1 and market is not None:
                    message["market"] = market.value
                print(json.dumps(message), flush=True)
                last_health[market] = current

        async def run_worker(plan, active_store, active_starts):
            starts_for_plan = {
                instrument: active_starts[instrument] for instrument in plan.instruments
            }
            collector = MinuteCollector(starts_for_plan, active_store, lambda candle: None)

            def rest_throttle(seconds):
                print(
                    json.dumps({"rest_throttle_seconds": seconds, "market": plan.market.value}),
                    flush=True,
                )

            repair_options = (
                {
                    "repair_grace_ms": 5_000,
                    "on_rest_throttle": rest_throttle,
                    "stream_batch_seconds": 0.2,
                }
                if native_series is not None
                else {}
            )
            worker = CollectionWorker(
                plan,
                histories[plan.market],
                collector,
                on_health=lambda ready, reason: health(ready, reason, market=plan.market),
                **({"capture_rest_raw": True} if capture else {}),
                **repair_options,
            )
            await worker.run(stop)

        async def run_workers(active_store, active_starts):
            if len(plans) == 1:
                await run_worker(plans[0], active_store, active_starts)
                return
            workers = [
                asyncio.create_task(run_worker(plan, active_store, active_starts)) for plan in plans
            ]
            try:
                await asyncio.gather(*workers)
            finally:
                for worker in workers:
                    worker.cancel()
                await asyncio.gather(*workers, return_exceptions=True)

        if getattr(args, "collect_auxiliary", False):
            from omega.runtime.collect_aux_live import LiveAuxiliary

            if ipc_root is None:
                auxiliary = LiveAuxiliary(settings, clients)
            else:
                from omega.runtime.collector_data_manager import DatabaseAuxiliarySink

                auxiliary = LiveAuxiliary(
                    settings,
                    clients,
                    store_factory=lambda: DatabaseAuxiliarySink(ipc_root / "ipc" / "database.sock"),
                )
            auxiliary_task = asyncio.create_task(auxiliary.run(instruments, stop))
            auxiliary_task.add_done_callback(lambda task: stop.set())

        if native_series is None:
            await run_workers(store, starts)
        else:
            from omega.infrastructure.binance_series_minutes import BinanceMinuteSeriesStore

            async def run_native():
                if ipc_root is None:
                    active = BinanceMinuteSeriesStore(
                        settings, native_series, time.time_ns() // 1_000_000
                    )
                else:
                    from omega.runtime.collector_data_manager import DatabaseBarSink

                    active = DatabaseBarSink(ipc_root / "ipc" / "database.sock", instruments)
                with closing(active) as active_store:
                    await run_workers(active_store, active_store.starts)

            await _retry_native_storage(run_native, stop, health)
    finally:
        try:
            if auxiliary_task is not None:
                stop.set()
                if sys.exc_info()[0] is None:
                    await asyncio.shield(auxiliary_task)
                else:
                    await asyncio.gather(auxiliary_task, return_exceptions=True)
        finally:
            try:
                if api is not None:
                    api.close()
            finally:
                try:
                    if source is not None:
                        source.close()
                finally:
                    try:
                        if store is not None:
                            store.close()
                    finally:
                        stop.set()
                        socket_stack.close()


async def _retry_native_storage(run_native, stop, health):
    delay = 1
    while not stop.is_set():
        try:
            await run_native()
            return
        except DatabaseUnavailable:
            health(False, "database_unavailable")
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            delay = min(delay * 2, 30)


def main(argv=None) -> None:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["bars"]:
        from omega.runtime.collector_series_cli import main as bars_main

        bars_main(argv[1:])
        return
    arguments = parser()
    args = arguments.parse_args(argv)
    if not 1 <= args.fetch_workers <= 24:
        arguments.error("--fetch-workers must be 1..24")
    if args.raw_directory is not None and not args.capture_rest_raw:
        arguments.error("--raw-directory requires --capture-rest-raw")
    if (args.api_port is None) != (args.api_token_file is None):
        arguments.error("--api-port and --api-token-file must be supplied together")
    if args.api_port is not None and not 0 <= args.api_port <= 65535:
        arguments.error("--api-port must be 0..65535")
    try:
        asyncio.run(collect(args))
    except KeyboardInterrupt:
        pass
    except ValidationError as error:
        arguments.error(str(error))


if __name__ == "__main__":
    main()
