"""Resume market history alongside the existing realtime collector."""

import argparse
import json
import shutil
import signal
import time
from contextlib import closing
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from threading import Event

from omega.application.trade_flow import trade_flow_values
from omega.domain.bar_series import SeriesCandle, SeriesObservation
from omega.domain.errors import (
    OmegaError,
    ProtocolError,
    RateLimited,
    StorageError,
    ValidationError,
)
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.bar_series_store import PostgresBarSeriesStore
from omega.infrastructure.binance.auxiliary_client import BinanceAuxiliaryClient
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.market import BinanceMarketData
from omega.infrastructure.binance.shared_budget import SharedRequestBudget
from omega.infrastructure.binance.transport import UrllibTransport
from omega.infrastructure.binance.vision import DAY, VisionArchive
from omega.infrastructure.binance.vision_market import VisionMarketData
from omega.infrastructure.candles import connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import verify_schema
from omega.infrastructure.raw_archive import FileRawArchive
from omega.ports.raw_data import PublicRawResponse
from omega.runtime.control import InstanceLock
from omega.runtime.settings import atomic_json

MINUTE = 60_000
CHANNELS = ("kline", "agg_trade", "depth", "metrics")
DISK_RESERVE = 32 * 1024**3


def clock_ms():
    return time.time_ns() // 1_000_000


def log(event, **details):
    print(json.dumps({"history": event, **details}, sort_keys=True), flush=True)


def read_inventory(path):
    if path.stat().st_size > 65536:
        raise ValidationError("history inventory exceeds its bound")
    entries = json.loads(path.read_text(encoding="utf-8"))["expected_series"]
    if type(entries) is not dict or not 1 <= len(entries) <= 25:
        raise ValidationError("invalid history inventory")
    result = {}
    for key, series_id in entries.items():
        provider, market, symbol = key.split(":")
        instrument = Instrument(provider, Market(market), symbol)
        if instrument.provider != "binance" or instrument.key != key:
            raise ValidationError("history inventory requires Binance canonical IDs")
        result[instrument] = series_id
    return result


def usable_archive_row(row):
    if row.channel == "metrics":
        return all(row.values[i].strip().lower() not in {"", "nan", "null"} for i in (2, 3, 6))
    if row.channel == "depth":
        try:
            percentage, depth, notional = map(Decimal, row.values[1:])
            return (
                all(x.is_finite() for x in (percentage, depth, notional))
                and 0 < abs(percentage) <= 5
                and depth >= 0
                and notional >= 0
            )
        except ArithmeticError:
            return False
    return True


def discover(instrument, channel, rest, vision):
    if channel == "metrics" and instrument.market == Market.SPOT:
        return "unavailable", None, None, None, {"reason": "spot_has_no_futures_metrics"}
    rest_first, rest_error = None, None
    if channel == "kline":
        try:
            rest_first = rest.first_available_minute(instrument)
        except OmegaError as exc:
            rest_error = type(exc).__name__
    row = vision.first_row(instrument, channel, usable=usable_archive_row)
    vision_first = None if row is None else row.timestamp_ms
    if row is None and channel == "agg_trade" and instrument.market == Market.SPOT:
        try:
            rest_first = rest.first_available_minute(instrument, trades=True)
        except OmegaError as exc:
            rest_error = type(exc).__name__
    dates = [stamp for stamp in (rest_first, vision_first) if stamp is not None]
    evidence = {}
    if row is not None:
        evidence.update(
            vision_key=row.receipt.object_key,
            vision_zip_sha256=row.receipt.zip_sha256,
            vision_row=row.row_number,
            vision_received_ms=row.receipt.received_at_ms,
        )
    if rest_error:
        evidence["rest_error"] = rest_error
    if dates and not (channel == "kline" and rest_error):
        return "verified", min(dates), rest_first, vision_first, evidence
    if rest_error:
        return "retry", None, rest_first, vision_first, evidence
    evidence["reason"] = "no_public_daily_archive"
    return "unavailable", None, rest_first, vision_first, evidence


class HistoryWorker:
    def __init__(self, root, settings, inventory, stop):
        self.root, self.settings, self.inventory, self.stop = root, settings, inventory, stop
        budget = SharedRequestBudget(root / "binance-public-budget.json")
        self.rest = {}
        self.aux = {}
        for market in {instrument.market for instrument in inventory}:
            client = BinanceClient(
                "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com",
                UrllibTransport(),
                budget=budget,
            )
            self.rest[market] = BinanceMarketData(market, client)
            self.aux[market] = BinanceAuxiliaryClient(market, client)
        self.vision = VisionArchive(root / "vision-history", paced=True, stop_requested=stop.is_set)
        self.vision_market = {
            market: VisionMarketData(rest, self.vision) for market, rest in self.rest.items()
        }
        self.raw = FileRawArchive(root / "history-repair-raw")
        self.repair_budget = RequestBudget((Window("repair", 120, 60),))

    def check_origins(self, db):
        cached = {
            (key, channel): (state, next_check)
            for key, channel, state, next_check in db.execute(
                "SELECT instrument,channel,discovery_state,next_check_ms "
                "FROM market.binance_history_origins"
            )
        }
        for channel in CHANNELS:
            for instrument in self.inventory:
                if self.stop.is_set():
                    return
                now = clock_ms()
                existing = cached.get((instrument.key, channel))
                if existing is not None and (existing[0] == "verified" or existing[1] > now):
                    continue
                try:
                    state, first, rest_first, vision_first, evidence = discover(
                        instrument, channel, self.rest[instrument.market], self.vision
                    )
                except (OSError, OmegaError) as exc:
                    state, first, rest_first, vision_first = "retry", None, None, None
                    evidence = {"error": type(exc).__name__}
                    if isinstance(exc, RateLimited):
                        evidence["retry_after"] = exc.retry_after or 0
                if state == "unavailable" and channel in {"depth", "agg_trade"}:
                    live_channel = "depth" if channel == "depth" else "trade_flow"
                    first = db.execute(
                        "SELECT (extract(epoch FROM min(open_time)) * 1000)::bigint "
                        "FROM market.auxiliary_minutes WHERE instrument=%s AND channel=%s",
                        (instrument.key, live_channel),
                    ).fetchone()[0]
                    evidence["live_first_ms"] = first
                checked = clock_ms()
                delay = max(300_000, int(evidence.get("retry_after", 0) * 1000))
                if state == "unavailable":
                    delay = 86_400_000 if instrument.market == Market.SPOT else 3_600_000
                next_check = None if state == "verified" else checked + delay
                if evidence.get("reason") == "spot_has_no_futures_metrics":
                    next_check = 253_402_300_799_000
                db.execute(
                    """INSERT INTO market.binance_history_origins
                      (instrument,channel,discovery_state,first_ms,rest_first_ms,vision_first_ms,
                       evidence,checked_ms,next_check_ms) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)
                      ON CONFLICT (instrument,channel) DO UPDATE SET
                       discovery_state=EXCLUDED.discovery_state,first_ms=EXCLUDED.first_ms,
                       rest_first_ms=EXCLUDED.rest_first_ms,vision_first_ms=EXCLUDED.vision_first_ms,
                       evidence=EXCLUDED.evidence,checked_ms=EXCLUDED.checked_ms,
                       next_check_ms=EXCLUDED.next_check_ms
                      WHERE market.binance_history_origins.discovery_state <> 'verified'""",
                    (
                        instrument.key,
                        channel,
                        state,
                        first,
                        rest_first,
                        vision_first,
                        json.dumps(evidence),
                        checked,
                        next_check,
                    ),
                )
                log(
                    "origin",
                    instrument=instrument.key,
                    channel=channel,
                    state=state,
                    first_ms=first,
                )

    def write_kline_day(self, db, store, instrument, origin):
        live = store.load(self.inventory[instrument])
        if live.instrument != instrument or live.feed != "binance-public":
            raise ProtocolError("history inventory does not match its live series")
        first = origin // MINUTE * MINUTE
        complete = replace(live, effective_from_ms=min(first, live.effective_from_ms))
        store.register(complete)
        if complete == live:
            return False
        db.execute(
            """INSERT INTO market.binance_kline_history
               (instrument, live_series_id, historical_series_id, next_ms, end_ms, mirror_ms)
               VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (instrument) DO NOTHING""",
            (
                instrument.key,
                live.series_id,
                complete.series_id,
                first,
                live.effective_from_ms,
                live.effective_from_ms,
            ),
        )
        saved_series, cursor, end, mirror = db.execute(
            "SELECT historical_series_id,next_ms,end_ms,mirror_ms "
            "FROM market.binance_kline_history WHERE instrument=%s",
            (instrument.key,),
        ).fetchone()
        if saved_series != complete.series_id or end != live.effective_from_ms:
            raise StorageError("saved historical series conflicts with discovery")
        # Keep a current suffix in the history series too. The live identity remains available.
        mirrored = store.read_observations(
            live.series_id,
            mirror,
            mirror + 256 * MINUTE,
            as_of_ms=clock_ms(),
            limit=256,
            include_raw=True,
        )
        inserted = []
        for item in mirrored:
            if item.candle.open_time_ms != mirror + len(inserted) * MINUTE:
                break
            inserted.append(replace(item, candle=replace(item.candle, series=complete)))
        if inserted:
            store.import_batch(complete, tuple(inserted))
            mirror = inserted[-1].candle.close_time_ms
            db.execute(
                "UPDATE market.binance_kline_history SET mirror_ms=%s WHERE instrument=%s",
                (mirror, instrument.key),
            )
        if cursor >= end or self.stop.is_set():
            return False
        until = min(end, (cursor // DAY + 1) * DAY)
        rows = self.vision_market[instrument.market].closed_observations(
            instrument, cursor, until, now_ms=clock_ms()
        )
        batch = []
        for item in rows:
            bar = item.candle
            batch.append(
                SeriesObservation(
                    SeriesCandle(
                        complete,
                        bar.open_time_ms,
                        bar.close_time_ms,
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.volume,
                    ),
                    item.source,
                    item.received_at_ms,
                    item.raw,
                    item.raw_source,
                    item.raw_received_at_ms,
                )
            )
        for offset in range(0, len(batch), 256):
            if self.stop.is_set():
                return False
            store.import_batch(complete, tuple(batch[offset : offset + 256]))
        missing = (until - cursor) // MINUTE - len(batch)
        # Commit the cursor only after the imported data is readable. Preserve exact
        # unresolved ranges for recheck instead of generating bars in missing slots.
        with db.transaction():
            if missing:
                actual = {item.candle.open_time_ms for item in batch}
                gap_start = None
                for opening in range(cursor, until + MINUTE, MINUTE):
                    if opening == until or opening in actual:
                        if gap_start is not None:
                            db.execute(
                                """INSERT INTO market.binance_kline_history_gaps
                                   (instrument,start_ms,end_ms) VALUES (%s,%s,%s)
                                   ON CONFLICT DO NOTHING""",
                                (instrument.key, gap_start, opening),
                            )
                            gap_start = None
                    elif gap_start is None:
                        gap_start = opening
            db.execute(
                """UPDATE market.binance_kline_history SET next_ms=%s, observed=observed+%s,
                   missing=missing+%s, updated_ms=%s WHERE instrument=%s AND next_ms=%s""",
                (until, len(batch), missing, clock_ms(), instrument.key, cursor),
            )
        log(
            "kline_batch",
            instrument=instrument.key,
            start_ms=cursor,
            end_ms=until,
            saved=len(batch),
            missing=missing,
        )
        return True

    def repair_trade(self, instrument, opening, volume, store):
        source = instrument.key + ":aggTrade"
        prior = store.get(instrument, "trade_flow", source, opening, as_of_ms=clock_ms())
        if prior is not None and prior.observation.quality == "observed":
            return True
        end, pending = opening + MINUTE, []
        client = self.aux[instrument.market]
        archive_ids = []
        for _ in range(16):
            parameters = (
                {"from_id": pending[-1].aggregate_id + 1}
                if pending
                else {
                    "start_ms": opening,
                    "end_ms": end,
                }
            )
            self.repair_budget.reserve({"repair": 4 if instrument.market == Market.SPOT else 20})
            batch = client.agg_trades(instrument, **parameters, limit=1000)
            receipt = self.raw.put(
                PublicRawResponse(
                    instrument,
                    "agg_trades",
                    batch.request_parameters,
                    batch.received_at_ms,
                    batch.body,
                    batch.headers,
                )
            )
            archive_ids.append(receipt.archive_id)
            pending.extend(
                item.as_flow_event() for item in batch.events if item.trade_time_ms < end
            )
            if len(batch.events) < 1000 or batch.events[-1].trade_time_ms >= end:
                break
        else:
            return False
        if not pending or any(
            left.aggregate_id + 1 != right.aggregate_id
            for left, right in zip(pending, pending[1:], strict=False)
        ):
            return False
        values = dict(trade_flow_values(pending))
        if values["AGGTRADE_QUANTITYBUY"] + values["AGGTRADE_QUANTITYSELL"] != volume:
            return False
        digest = sha256("".join(archive_ids).encode()).hexdigest()
        atomic_json(
            self.root / "history-repair-receipts" / (digest + ".json"),
            {
                "instrument": instrument.key,
                "open_ms": opening,
                "raw_responses": archive_ids,
            },
        )
        values["AGGTRADE_REST_REPAIR"] = 1
        values["AGGTRADE_REST_ARCHIVE_DIGEST"] = int(digest, 16)
        try:
            store.put(
                instrument,
                AuxiliaryInput(
                    "trade_flow",
                    tuple(values.items()),
                    opening,
                    end,
                    end,
                    clock_ms(),
                    "observed",
                    Decimal(1),
                    source,
                ),
                expected_previous=None if prior is None else prior.checksum,
            )
        except ProtocolError:
            latest = store.get(instrument, "trade_flow", source, opening, as_of_ms=clock_ms())
            if latest is None or latest.observation.quality != "observed":
                raise
        log("trade_repaired", instrument=instrument.key, open_ms=opening, events=len(pending))
        return True

    def recent_trade_gaps(self, db, store):
        now = clock_ms()
        by_id = {series: instrument for instrument, series in self.inventory.items()}
        candidates = db.execute(
            """SELECT series_id, (extract(epoch FROM open_time)*1000)::bigint, v
               FROM market.series_candles AS bar JOIN market.bar_series AS series USING (series_id)
               WHERE series_id=ANY(%s) AND v>0 AND open_time>=now()-interval '1 day'
                 AND open_time<now()-interval '3 minutes'
                 AND open_time >= (SELECT min(a.open_time) FROM market.auxiliary_minutes a
                       WHERE a.instrument=series.definition->>'instrument'
                         AND a.channel='trade_flow'
                         AND a.dataset=a.instrument||':aggTrade')
                 AND NOT EXISTS (SELECT 1 FROM market.auxiliary_minutes a
                       WHERE a.instrument=series.definition->>'instrument'
                         AND a.channel='trade_flow' AND a.open_time=bar.open_time
                         AND convert_from(a.payload,'UTF8')::jsonb
                             ->'observation'->>'quality'='observed')
               ORDER BY open_time LIMIT 60""",
            (list(by_id),),
        ).fetchall()
        for series_id, opening, volume in candidates:
            if self.stop.is_set():
                break
            instrument = by_id[series_id]
            key = instrument.key, opening
            if self.retry.get(key, 0) > now:
                continue
            try:
                self.retry[key] = now + 15 * MINUTE
                if not self.repair_trade(instrument, opening, volume, store):
                    log("trade_repair_unconfirmed", instrument=instrument.key, open_ms=opening)
            except RateLimited as exc:
                self.retry[key] = now + max(1_000, int((exc.retry_after or 0) * 1000))
                log("repair_throttled", seconds=exc.retry_after)
                break
            except (OSError, OmegaError) as exc:
                log(
                    "trade_repair_error",
                    instrument=instrument.key,
                    open_ms=opening,
                    error=type(exc).__name__,
                )
        self.retry = {key: until for key, until in self.retry.items() if until > now}

    def run(self):
        self.retry = {}
        next_repair, next_discovery = 0, 0
        with (
            connect_database(self.settings) as db,
            closing(PostgresBarSeriesStore(self.settings)) as bars,
            closing(PostgresAuxiliaryStore(self.settings)) as auxiliary,
        ):
            verify_schema(db)
            while not self.stop.is_set():
                if shutil.disk_usage(self.root).free < DISK_RESERVE:
                    log("disk_reserve", minimum_bytes=DISK_RESERVE)
                    self.stop.wait(60)
                    continue
                now = clock_ms()
                if now >= next_repair:
                    self.recent_trade_gaps(db, auxiliary)
                    next_repair = clock_ms() + MINUTE
                if now >= next_discovery:
                    self.check_origins(db)
                    next_discovery = clock_ms() + MINUTE
                known = dict(
                    db.execute(
                        """SELECT instrument,first_ms FROM market.binance_history_origins
                       WHERE channel='kline' AND discovery_state='verified'"""
                    ).fetchall()
                )
                progressed = False
                for instrument in self.inventory:
                    if self.stop.is_set():
                        return
                    if instrument.key not in known:
                        continue
                    try:
                        progressed |= self.write_kline_day(
                            db, bars, instrument, known[instrument.key]
                        )
                    except RateLimited as exc:
                        log("backfill_throttled", seconds=exc.retry_after)
                        self.stop.wait(max(1, min(60, exc.retry_after or 0)))
                        break
                    except (OSError, OmegaError) as exc:
                        log(
                            "kline_history_error",
                            instrument=instrument.key,
                            error=type(exc).__name__,
                        )
                    if clock_ms() >= next_repair:
                        self.recent_trade_gaps(db, auxiliary)
                        next_repair = clock_ms() + MINUTE
                if not progressed:
                    self.stop.wait(15)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Discover and backfill the Binance collector inventory"
    )
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    with closing(InstanceLock(root / "history-worker.lock")):
        try:
            worker = HistoryWorker(
                root,
                load_database_settings(root / "database.config.toml"),
                read_inventory(root / "handoff.json"),
                stop,
            )
            worker.run()
        except InterruptedError:
            if not stop.is_set():
                raise


if __name__ == "__main__":
    main()
