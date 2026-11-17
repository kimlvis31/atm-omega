"""Explicit bounded public trade-flow job; no account loading or order execution."""

import argparse
import json
import signal
import time
from hashlib import sha256
from pathlib import Path
from threading import Event

from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.binance.auxiliary_budget import AuxiliaryPublicBudget
from omega.infrastructure.binance.auxiliary_client import BinanceAuxiliaryClient
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.transport import UrllibTransport
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.raw_archive import FileRawArchive
from omega.runtime.auxiliary_collection import AuxiliaryCollection, validate_collection_plan
from omega.runtime.auxiliary_journal import AuxiliaryJobJournal
from omega.runtime.control import InstanceLock


def parser():
    result = argparse.ArgumentParser(
        description="Collect 1..10 fixed closed minutes of public Binance trade flow"
    )
    result.add_argument("--root", type=Path, default=Path.cwd())
    result.add_argument("--db-config", type=Path)
    result.add_argument("--market", choices=["spot", "usdm"])
    result.add_argument("--symbol")
    result.add_argument("--from-ms", type=int)
    result.add_argument("--to-ms", type=int)
    result.add_argument("--limit", type=int)
    result.add_argument("--max-pages", type=int)
    result.add_argument("--resume", metavar="JOB_ID")
    return result


def destination_id(settings):
    """Bind resumed jobs to public DB identity; password/file location is not an identity."""
    document = [settings.host, settings.port, settings.dbname, settings.user]
    return sha256(
        json.dumps(document, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def collection_plan(args, settings):
    root = args.root.resolve()
    if args.resume is not None:
        if any(
            getattr(args, key) is not None
            for key in ("market", "symbol", "from_ms", "to_ms", "limit", "max_pages")
        ):
            raise ValidationError(
                "resume uses its saved plan; collection overrides are not permitted"
            )
        plan = AuxiliaryJobJournal.load_plan(root / "state/auxiliary-jobs", args.resume)
        if plan["destination_id"] != destination_id(settings):
            raise ValidationError("resumed auxiliary job belongs to another database destination")
    else:
        if any(getattr(args, key) is None for key in ("market", "symbol", "from_ms", "to_ms")):
            raise ValidationError("a new collection requires market, symbol, from-ms and to-ms")
        instrument = Instrument("binance", Market(args.market), args.symbol)
        plan = {
            "instrument": instrument.key,
            "source": f"{instrument.key}:aggTrade",
            "start_ms": args.from_ms,
            "end_ms": args.to_ms,
            "limit": 500 if args.limit is None else args.limit,
            "max_pages": 20 if args.max_pages is None else args.max_pages,
            "destination_id": destination_id(settings),
        }
    validate_collection_plan(plan)
    return plan


def collect(args, *, stop=None, clock_ms=lambda: time.time_ns() // 1_000_000):
    root = args.root.resolve()
    config = args.db_config if args.db_config is not None else root / "database.config.toml"
    if not config.is_absolute():
        config = root / config
    settings = load_database_settings(config)
    if (settings.host, settings.port, settings.dbname, settings.user) not in {
        ("127.0.0.1", 55432, "omega", "omega_app"),
        ("127.0.0.1", 55433, "omega_test", "omega_app"),
    }:
        raise ValidationError("this initial collector only opens explicit local Omega databases")
    plan = collection_plan(args, settings)
    if plan["end_ms"] > clock_ms() // 60000 * 60000:
        raise ValidationError("auxiliary target must end at a closed UTC minute")
    lock_key = sha256((plan["instrument"] + "\n" + plan["source"]).encode()).hexdigest()
    owner = InstanceLock(root / "state/locks" / (lock_key + ".lock"))
    store = None
    try:
        journal = AuxiliaryJobJournal(root / "state/auxiliary-jobs", plan)
        provider, market, symbol = plan["instrument"].split(":")
        del provider, symbol
        market = Market(market)
        client = BinanceAuxiliaryClient(
            market,
            BinanceClient(
                "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com",
                UrllibTransport(),
                budget=AuxiliaryPublicBudget(root / "state"),
                clock_ms=clock_ms,
            ),
        )
        store = PostgresAuxiliaryStore(settings)
        archive = FileRawArchive(root / "state/auxiliary-raw")
        interrupted = stop or Event()
        return AuxiliaryCollection(
            plan,
            client,
            archive,
            store,
            journal,
            clock_ms=clock_ms,
            stop_requested=interrupted.is_set,
        ).run()
    finally:
        try:
            if store is not None:
                store.close()
        finally:
            owner.close()


def main(argv=None):
    args = parser().parse_args(argv)
    stop, previous = Event(), {}
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[name] = signal.signal(name, lambda *_args: stop.set())
        except (ValueError, OSError):
            pass
    try:
        result = collect(args, stop=stop)
    except Exception as exc:
        result = {"complete": False, "reason": "collection_failed", "error": type(exc).__name__}
    finally:
        for name, handler in previous.items():
            signal.signal(name, handler)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["complete"] else 130 if result["reason"] == "stopped" else 2


if __name__ == "__main__":
    raise SystemExit(main())
