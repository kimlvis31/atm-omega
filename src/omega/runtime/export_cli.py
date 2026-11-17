"""Export committed PostgreSQL observations without contacting an exchange."""

import argparse
import csv
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

from omega.application.paper import candle_row, parse_instrument
from omega.domain.errors import StorageError, ValidationError
from omega.infrastructure.candles import PostgresCandleStore
from omega.infrastructure.postgres.config import load_database_settings


@contextmanager
def _output(target):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=target.parent,
            prefix=".omega-export-",
            suffix=".partial",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            yield stream
            stream.flush()
            os.fsync(stream.fileno())
        # An atomic exclusive hard link publishes a complete file and refuses to
        # replace an existing dataset, including one created concurrently.
        os.link(temporary, target)
        if os.name != "nt":
            descriptor = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            except OSError:
                target.unlink()
                raise
            finally:
                os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def export_observations(store, instrument, target, *, limit=10000, raw=False):
    observations = store.latest_observations(instrument, limit)
    if not observations:
        raise ValidationError("no committed market observations to export")
    target = Path(target).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise FileExistsError("export target already exists")
    with _output(target) as stream:
        if raw:
            for obs in observations:
                stream.write(
                    json.dumps(
                        {
                            "instrument": instrument.key,
                            "open_time_ms": obs.candle.open_time_ms,
                            "source": obs.source,
                            "received_at_ms": obs.received_at_ms,
                            "raw_payload": obs.raw,
                            "raw_available": obs.raw is not None,
                            "raw_source": obs.raw_source,
                            "raw_received_at_ms": obs.raw_received_at_ms,
                            "raw_origin_known": obs.raw_source is not None,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        else:
            writer = csv.DictWriter(
                stream,
                fieldnames=["instrument", "open_time_ms", "open", "high", "low", "close", "volume"],
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(
                {"instrument": instrument.key, **candle_row(obs.candle)} for obs in observations
            )
    return {
        "rows": len(observations),
        "source": "committed_postgresql",
        "representation": "raw_payload" if raw else "normalized_ohlcv",
        "raw_missing": sum(o.raw is None for o in observations) if raw else None,
        "file": str(target),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Export stored 1m OHLCV CSV or received raw JSONL")
    parser.add_argument(
        "--instrument", required=True, help="binance:spot:BTCUSDT or binance:usdm:BTCUSDT"
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--db-config", type=Path, default=Path("database.config.toml"))
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument(
        "--raw", action="store_true", help="raw received payloads and availability, JSONL"
    )
    parser.add_argument(
        "--candle-dataset",
        type=Path,
        help="existing OHLCV dataset to freeze beside selected stored observations",
    )
    parser.add_argument(
        "--auxiliary-source",
        action="append",
        default=[],
        metavar="CHANNEL=SOURCE",
        help="select normalized committed observations; repeat up to eight times",
    )
    parser.add_argument("--as-of-ms", type=int, help="non-future observation knowledge cutoff")
    args = parser.parse_args(argv)
    store = None
    try:
        instrument = parse_instrument(args.instrument)
        if not 1 <= args.limit <= 10000:
            raise ValidationError("limit must be 1..10000")
        if args.candle_dataset is not None:
            from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
            from omega.runtime.auxiliary_research import export_stored_auxiliary

            if args.raw:
                raise ValidationError("observation bundles contain normalized data, not raw JSONL")
            selections = []
            for value in args.auxiliary_source:
                channel, separator, source = value.partition("=")
                if not separator:
                    raise ValidationError("auxiliary source must be CHANNEL=SOURCE")
                selections.append(
                    {"instrument": instrument.key, "channel": channel, "source": source}
                )
            result = export_stored_auxiliary(
                args.candle_dataset,
                args.output,
                selections,
                store_factory=lambda: PostgresAuxiliaryStore(
                    load_database_settings(args.db_config)
                ),
                as_of_ms=args.as_of_ms,
            )
        else:
            if args.auxiliary_source or args.as_of_ms is not None:
                raise ValidationError("observation source/cutoff requires --candle-dataset")
            store = PostgresCandleStore(load_database_settings(args.db_config))
            result = export_observations(
                store, instrument, args.output, limit=args.limit, raw=args.raw
            )
        print(json.dumps(result))
    except (StorageError, ValidationError, OSError) as exc:
        print(f"Export failed: {exc}")
        return 1
    finally:
        if store is not None:
            store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
