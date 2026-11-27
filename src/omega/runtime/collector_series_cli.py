"""Stage, verify, and serve collector-owned base bars on a local database."""

import argparse
import json
import signal
from contextlib import closing
from pathlib import Path
from threading import Event

from omega.domain.bar_schedule import BarSchedule
from omega.domain.bar_series import BarSeries, SeriesCandle, SeriesObservation
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Instrument, Market, decimal
from omega.infrastructure.bar_series_store import PostgresBarSeriesStore
from omega.infrastructure.bar_series_transfer import (
    LEGACY_FEED,
    check_base_bar_transition,
    transfer_legacy_window,
)
from omega.infrastructure.postgres.config import load_database_settings
from omega.runtime.collector_series_api import BAR_VALUES
from omega.runtime.collector_server import CollectorAPIServer, read_api_token

MAX_BATCH_BYTES = 2 * 1024 * 1024


def _unique_fields(pairs):
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValidationError("base-bar batch repeats an object key")
    return result


def _load_batch(path):
    try:
        with path.open("rb") as stream:
            body = stream.read(MAX_BATCH_BYTES + 1)
        if len(body) > MAX_BATCH_BYTES:
            raise ValidationError("base-bar batch exceeds 2 MiB")
        value = json.loads(body.decode("utf-8"), object_pairs_hook=_unique_fields)
    except (OSError, ValueError, RecursionError):
        raise ValidationError("base-bar batch file cannot be read") from None
    if (
        type(value) is not dict
        or set(value) != {"series", "bars"}
        or type(value["bars"]) is not list
        or not 1 <= len(value["bars"]) <= 256
    ):
        raise ValidationError("base-bar batch must contain a series and 1..256 observations")
    series = BarSeries.from_document(value["series"])
    if series.feed == LEGACY_FEED:
        raise ValidationError("legacy-1m is reserved for verified legacy transfers")
    required = {"open_ms", "close_ms", "source", "received_at_ms", *BAR_VALUES}
    optional = {"raw", "raw_source", "raw_received_at_ms"}
    observations = []
    for bar in value["bars"]:
        if (
            type(bar) is not dict
            or not required <= set(bar) <= required | optional
            or any(type(bar[field]) is not str or len(bar[field]) > 256 for field in BAR_VALUES)
        ):
            raise ValidationError("invalid base-bar observation fields")
        candle = SeriesCandle(
            series,
            bar["open_ms"],
            bar["close_ms"],
            *(decimal(bar[field]) for field in BAR_VALUES),
        )
        observations.append(
            SeriesObservation(
                candle,
                bar["source"],
                bar["received_at_ms"],
                bar.get("raw"),
                bar.get("raw_source"),
                bar.get("raw_received_at_ms"),
            )
        )
    return series, tuple(observations)


def _serve(settings, port, token):
    with closing(PostgresBarSeriesStore(settings, read_only=True)):
        pass
    stop = Event()

    def on_stop(_signum, _frame):
        stop.set()

    previous = signal.signal(signal.SIGTERM, on_stop)
    try:
        with closing(
            CollectorAPIServer(
                None,
                token,
                port,
                series_store=lambda: PostgresBarSeriesStore(settings, read_only=True),
            )
        ) as server:
            server.start()
            print(json.dumps({"api_ready": True, "port": server.port}), flush=True)
            while not stop.wait(0.25):
                pass
    finally:
        signal.signal(signal.SIGTERM, previous)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="omega-collect bars", description="Stage and read verified collector base bars"
    )
    parser.add_argument("--db-config", type=Path, default=Path("database.config.toml"))
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("copy-legacy", "verify-legacy"):
        transfer = sub.add_parser(name)
        transfer.add_argument("--instrument", required=True, metavar="PROVIDER:MARKET:SYMBOL")
        transfer.add_argument("--effective-from-ms", type=int, required=True)
        transfer.add_argument("--start-ms", type=int, required=True)
        transfer.add_argument("--end-ms", type=int, required=True)
    transition = sub.add_parser("check-transition")
    transition.add_argument("--previous-series-id", required=True)
    transition.add_argument("--target-series-id", required=True)
    transition.add_argument("--as-of-ms", type=int, required=True)
    stage = sub.add_parser("import")
    stage.add_argument("file", type=Path)
    serve = sub.add_parser("serve")
    serve.add_argument("--port", type=int, default=0)
    serve.add_argument("--token-file", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        settings = load_database_settings(args.db_config)
        if args.command == "serve":
            _serve(settings, args.port, read_api_token(args.token_file))
            return
        if args.command == "check-transition":
            with closing(PostgresBarSeriesStore(settings, read_only=True)) as store:
                result = check_base_bar_transition(
                    store, args.previous_series_id, args.target_series_id, args.as_of_ms
                )
        elif args.command == "import":
            series, observations = _load_batch(args.file)
            with closing(PostgresBarSeriesStore(settings)) as store:
                inserted = store.import_batch(series, observations)
            result = {
                "series_id": series.series_id,
                "inserted": inserted,
                "rows": len(observations),
            }
        else:
            try:
                provider, market, symbol = args.instrument.split(":")
                instrument = Instrument(provider, Market(market), symbol)
            except ValueError:
                raise ValidationError("invalid legacy instrument") from None
            if args.instrument != instrument.key:
                raise ValidationError("legacy instrument must be canonical")
            series = BarSeries(
                instrument, LEGACY_FEED, (60_000,), BarSchedule(), args.effective_from_ms
            )
            verify = args.command == "verify-legacy"
            with closing(PostgresBarSeriesStore(settings, read_only=verify)) as store:
                result = transfer_legacy_window(
                    store, series, args.start_ms, args.end_ms, verify_only=verify
                )
        print(json.dumps(result))
        if args.command == "check-transition" and not result["verified"]:
            parser.exit(2)
    except KeyboardInterrupt:
        pass
    except (ValidationError, LookupError, StorageError, ProtocolError) as error:
        parser.exit(1, f"{error}\n")
    except OSError:
        parser.exit(1, "base-bar API could not open the local port\n")


if __name__ == "__main__":
    main()
