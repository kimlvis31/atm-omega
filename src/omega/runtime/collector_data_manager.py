"""Database-owned planning, live ingestion and committed market reads."""

import argparse
import signal
import time
from contextlib import closing
from itertools import batched
from pathlib import Path
from threading import Event, RLock

from omega.domain.errors import ProtocolError, ValidationError
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.bar_series_store import PostgresBarSeriesStore
from omega.infrastructure.binance_series_minutes import (
    BinanceMinuteSeriesStore,
    binance_minute_series,
)
from omega.infrastructure.candles import DatabaseUnavailable, connection_lost
from omega.infrastructure.postgres.batching import COMMIT_BATCH_ROWS
from omega.infrastructure.postgres.config import load_database_settings
from omega.runtime.collector_history import HistoryWorker, read_inventory
from omega.runtime.collector_progress import BackfillProgress, TransferMeter
from omega.runtime.collector_server import CollectorAPIServer, read_api_token
from omega.runtime.collector_write_queue import CollectorWriteQueue, run_write
from omega.runtime.control import InstanceLock
from omega.runtime.market_history_fetch import RemoteHistoryFetch
from omega.runtime.market_ipc import MarketSocket, serve_market_socket
from omega.runtime.market_wire import (
    auxiliary_document,
    auxiliary_from_document,
    candle_document,
    candle_from_document,
    minute_document,
    minute_from_document,
)


class DatabaseIngestion:
    def __init__(self, settings, series, stop, *, write=run_write, on_commit=None):
        self.settings, self.series, self.stop = settings, series, stop
        self.write = write
        self.on_commit = on_commit
        self.lock = RLock()
        self.bars = self.auxiliary = None

    def close(self):
        with self.lock:
            for field in ("bars", "auxiliary"):
                store = getattr(self, field)
                if store is not None:
                    store.close()
                    setattr(self, field, None)

    def _bars(self):
        if self.bars is None:
            self.bars = BinanceMinuteSeriesStore(
                self.settings, self.series, time.time_ns() // 1_000_000, on_commit=self.on_commit
            )
        return self.bars

    def _allowed(self, instruments):
        if any(instrument not in self.series for instrument in instruments):
            raise ProtocolError("market batch contains an unsubscribed instrument")

    def dispatch(self, operation, args):
        kind = {
            "starts": "cursor",
            "bars": "repair" if args.get("missing_only") else "stream_bars",
            "auxiliary": "stream_auxiliary",
        }.get(operation)
        if kind is None:
            raise ProtocolError("unknown database manager operation")
        return self.write(kind, self._handle, operation, args)

    def _handle(self, operation, args):
        with self.lock:
            try:
                if operation == "starts":
                    bars = self._bars()
                    db = bars.store.connection
                    try:
                        cursors = db.execute(
                            "SELECT series_id,next_ms FROM market.series_live_cursor "
                            "WHERE series_id=ANY(%s)",
                            ([item.series_id for item in self.series.values()],),
                        ).fetchall()
                    except bars.store._database_error as error:
                        raise bars.store._storage_error(
                            error, "market cursor read failed"
                        ) from None
                    offsets = dict(cursors)
                    return {
                        item.instrument.key: offsets[item.series_id]
                        for item in self.series.values()
                    }
                if operation == "bars":
                    if (
                        type(args["missing_only"]) is not bool
                        or not 1 <= len(args["rows"]) <= COMMIT_BATCH_ROWS
                    ):
                        raise ValidationError("invalid live bar batch")
                    rows = [minute_from_document(row) for row in args["rows"]]
                    self._allowed(row.candle.instrument for row in rows)
                    return [
                        candle_document(row)
                        for row in self._bars().put_batch(rows, missing_only=args["missing_only"])
                    ]
                if operation == "auxiliary":
                    if not 1 <= len(args["rows"]) <= COMMIT_BATCH_ROWS:
                        raise ValidationError("invalid live auxiliary batch")
                    rows = [auxiliary_from_document(row) for row in args["rows"]]
                    self._allowed(instrument for instrument, _ in rows)
                    if self.auxiliary is None:
                        self.auxiliary = PostgresAuxiliaryStore(
                            self.settings, on_commit=self.on_commit
                        )
                    return self.auxiliary.put_live_batch(rows)
            except DatabaseUnavailable:
                for field, session in (
                    ("bars", self.bars.store if self.bars is not None else None),
                    ("auxiliary", self.auxiliary),
                ):
                    if session is not None and connection_lost(session.connection):
                        session.close()
                        setattr(self, field, None)
                raise
        raise ProtocolError("unknown database manager operation")


class DatabaseBarSink:
    def __init__(self, path, instruments):
        self.connection = MarketSocket(path)
        starts = self.connection.call("starts")
        self.starts = {instrument: starts[instrument.key] for instrument in instruments}

    def put_batch(self, observations, *, missing_only=False):
        committed = []
        for batch in batched(observations, COMMIT_BATCH_ROWS, strict=False):
            rows = self.connection.call(
                "bars", rows=[minute_document(row) for row in batch], missing_only=missing_only
            )
            for original, document in zip(batch, rows, strict=True):
                candle = candle_from_document(document)
                if (
                    original.candle.instrument != candle.instrument
                    or original.candle.open_time_ms != candle.open_time_ms
                ):
                    raise ProtocolError("database reply does not match the committed batch")
                committed.append(candle)
        return committed

    def close(self):
        pass


class DatabaseAuxiliarySink:
    def __init__(self, path):
        self.connection = MarketSocket(path)

    def put_live_batch(self, batch):
        return sum(
            self.connection.call(
                "auxiliary",
                rows=[auxiliary_document(instrument, row) for instrument, row in part],
            )
            for part in batched(batch, COMMIT_BATCH_ROWS, strict=False)
        )

    def close(self):
        pass


def main(argv=None):
    parser = argparse.ArgumentParser(description="Manage collector data and committed reads")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--effective-from-ms", type=int, required=True)
    parser.add_argument("--fetch-workers", type=int, default=12)
    parser.add_argument("--api-port", type=int, required=True)
    parser.add_argument("--api-token-file", type=Path, required=True)
    args = parser.parse_args(argv)
    if not 1 <= args.fetch_workers <= 24:
        parser.error("--fetch-workers must be 1..24")
    root = args.root.resolve()
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    settings = load_database_settings(root / "database.config.toml")
    inventory = read_inventory(root / "handoff.json")
    series = binance_minute_series(inventory, args.effective_from_ms)
    if {instrument: item.series_id for instrument, item in series.items()} != inventory:
        raise ValidationError("data manager series differ from the saved inventory")
    ipc = root / "ipc"
    meter = TransferMeter()
    progress = BackfillProgress(root, settings, inventory, meter)
    # The old backfill owner shares this lock across upgrades.
    with (
        closing(InstanceLock(root / "history-worker.lock")),
        closing(CollectorWriteQueue()) as writes,
        closing(
            DatabaseIngestion(settings, series, stop, write=writes.run, on_commit=meter.record)
        ) as ingest,
    ):
        ingest._bars()
        with serve_market_socket(ipc / "database.sock", ingest.dispatch, capacity=8):
            api = CollectorAPIServer(
                None,
                read_api_token(args.api_token_file),
                args.api_port,
                series_store=lambda: PostgresBarSeriesStore(settings, read_only=True),
                progress=progress,
            )
            api.start()
            try:
                print('{"data_manager":"ready"}', flush=True)
                fetch = RemoteHistoryFetch(ipc / "market.sock")
                HistoryWorker(
                    root,
                    settings,
                    inventory,
                    stop,
                    fetch,
                    workers=args.fetch_workers,
                    write=writes.run,
                    on_commit=meter.record,
                ).run()
            except InterruptedError:
                if not stop.is_set():
                    raise
            finally:
                api.close()


if __name__ == "__main__":
    main()
