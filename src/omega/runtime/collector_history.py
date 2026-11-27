"""Resume market history alongside the existing realtime collector."""

import json
import shutil
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import closing
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from itertools import zip_longest

from psycopg.errors import QueryCanceled

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
from omega.infrastructure.bar_series_store import BAR_WRITE_BATCH_SIZE, PostgresBarSeriesStore
from omega.infrastructure.binance.vision import DAY
from omega.infrastructure.candles import connect_database
from omega.infrastructure.collector_coverage import (
    first_covered,
    note_source_check,
    recent_unconfirmed_trades,
)
from omega.infrastructure.postgres.schema import verify_schema
from omega.runtime.collector_aux_history import DailyAuxiliaryHistory
from omega.runtime.collector_write_queue import run_write
from omega.runtime.market_ipc import BackfillPaused
from omega.runtime.settings import atomic_json

MINUTE = 60_000
CHANNELS = ("kline", "agg_trade", "depth", "metrics")
DISK_RESERVE = 32 * 1024**3
BACKFILL_MAX_PAGES = 12
BACKFILL_PAUSE_PAGES = 10
BACKFILL_RESUME_PAGES = 6
TRADE_REPAIR_INTERVAL_MS = 15_000


def clock_ms():
    return time.time_ns() // 1_000_000


def log(event, **details):
    print(json.dumps({"history": event, **details}, sort_keys=True), flush=True)


def read_inventory(path):
    if path.stat().st_size > 16 * 1024 * 1024:
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
    def __init__(
        self, root, settings, inventory, stop, fetch, *, workers=12, write=run_write, on_commit=None
    ):
        self.root, self.settings, self.inventory, self.stop = root, settings, inventory, stop
        self.fetch, self.workers, self.write = fetch, workers, write
        self.on_commit = on_commit
        self.origins = None
        self.auxiliary_history = DailyAuxiliaryHistory(
            root, None, None, clock_ms=clock_ms, stop=stop, fetch=fetch, write=write
        )

    def check_origins(self, db):
        if getattr(self, "origins", None) is None:
            self.origins = {
                (key, channel): (state, next_check, first)
                for key, channel, state, next_check, first in db.execute(
                    "SELECT instrument,channel,discovery_state,next_check_ms,first_ms "
                    "FROM market.binance_history_origins"
                )
            }
        for channel in CHANNELS:
            for instrument in self.inventory:
                if self.stop.is_set():
                    return
                now = clock_ms()
                existing = self.origins.get((instrument.key, channel))
                if existing is not None and (existing[0] == "verified" or existing[1] > now):
                    continue
                try:
                    state, first, rest_first, vision_first, evidence = self.fetch.origin(
                        instrument, channel
                    )
                except (OSError, OmegaError) as exc:
                    state, first, rest_first, vision_first = "retry", None, None, None
                    evidence = {"error": type(exc).__name__}
                    if isinstance(exc, RateLimited):
                        evidence["retry_after"] = exc.retry_after or 0
                if state == "unavailable" and channel in {"depth", "agg_trade"}:
                    live_channel = "depth" if channel == "depth" else "trade_flow"
                    suffix = ":binance_depth_ws_v1" if channel == "depth" else ":aggTrade"
                    first = first_covered(
                        db, instrument.key, live_channel, (instrument.key + suffix,)
                    )
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
                self.origins[(instrument.key, channel)] = (state, next_check, first)
                log(
                    "origin",
                    instrument=instrument.key,
                    channel=channel,
                    state=state,
                    first_ms=first,
                )

    def _day_plan(self, db, series_id, start, end):
        day = start // DAY * DAY
        stop = min(end, day + DAY)
        coverage = db.execute(
            "SELECT observed FROM market.collector_coverage "
            "WHERE scope_key=%s AND channel='kline' AND dataset='' AND day_ms=%s",
            (series_id, day),
        ).fetchone()
        existing = set()
        if coverage is not None:
            for interval in coverage[0]:
                existing.update(
                    range(max(start, interval.lower), min(stop, interval.upper), MINUTE)
                )
        gaps, opened = [], None
        for stamp in range(start, stop + MINUTE, MINUTE):
            if stamp == stop or stamp in existing:
                if opened is not None:
                    gaps.append((opened, stamp))
                    opened = None
            elif opened is None:
                opened = stamp
        return stop, existing, gaps

    def _prepare_kline(self, db, store, pool, instrument, origin):
        live = store.load(self.inventory[instrument])
        if live.instrument != instrument or live.feed != "binance-public":
            raise ProtocolError("history inventory does not match its live series")
        first = origin // MINUTE * MINUTE
        complete = replace(live, effective_from_ms=min(first, live.effective_from_ms))
        store.register(complete)
        if complete == live:
            return None
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
            mirror + BAR_WRITE_BATCH_SIZE * MINUTE,
            as_of_ms=clock_ms(),
            limit=BAR_WRITE_BATCH_SIZE,
            include_raw=True,
        )
        inserted = []
        for item in mirrored:
            if item.candle.open_time_ms != mirror + len(inserted) * MINUTE:
                break
            inserted.append(replace(item, candle=replace(item.candle, series=complete)))
        if inserted:
            self.write("history", store.import_batch, complete, tuple(inserted), mirror=True)
            mirror = inserted[-1].candle.close_time_ms
            db.execute(
                "UPDATE market.binance_kline_history SET mirror_ms=%s WHERE instrument=%s",
                (mirror, instrument.key),
            )
        if cursor >= end or self.stop.is_set():
            return None
        until, existing, gaps = self._day_plan(db, complete.series_id, cursor, end)
        return (
            complete,
            cursor,
            until,
            existing,
            pool.submit(self.fetch.klines, instrument, cursor, until, gaps),
        )

    def write_kline_day(self, db, store, instrument, plan):
        complete, cursor, until, existing, future = plan
        rows, archived = future.result()
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
        for offset in range(0, len(batch), BAR_WRITE_BATCH_SIZE):
            if self.stop.is_set():
                return False
            self.write(
                "history",
                store.import_batch,
                complete,
                tuple(batch[offset : offset + BAR_WRITE_BATCH_SIZE]),
            )
        actual = existing | {item.candle.open_time_ms for item in batch}
        verified = len(actual)
        missing = (until - cursor) // MINUTE - verified
        # Commit the cursor only after the imported data is readable. Preserve exact
        # unresolved ranges for recheck instead of generating bars in missing slots.
        with db.transaction():
            if missing:
                checked = clock_ms()
                note_source_check(
                    db,
                    instrument,
                    "kline",
                    "vision",
                    cursor,
                    until,
                    ("incomplete" if batch else "no_rows") if archived else "unlisted",
                    checked,
                )
                if not archived:
                    note_source_check(
                        db,
                        instrument,
                        "kline",
                        "rest",
                        cursor,
                        until,
                        "incomplete" if batch else "no_rows",
                        checked,
                    )
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
                (until, verified, missing, clock_ms(), instrument.key, cursor),
            )
        log(
            "kline_batch",
            instrument=instrument.key,
            start_ms=cursor,
            end_ms=until,
            saved=len(batch),
            covered=verified,
            missing=missing,
        )
        return True

    def repair_trade(self, instrument, opening, volume, store):
        source = instrument.key + ":aggTrade"
        prior = store.get(instrument, "trade_flow", source, opening, as_of_ms=clock_ms())
        if prior is not None and prior.observation.quality == "observed":
            return True
        if volume == 0:
            if prior is not None and prior.observation.mapping().get("AGGTRADE_EVENTCOUNT", 0):
                raise ProtocolError("closed zero-volume candle conflicts with saved trades")
            observation = AuxiliaryInput(
                "trade_flow",
                (*trade_flow_values(()), ("AGGTRADE_KLINE_ZERO", 1)),
                opening,
                opening + MINUTE,
                opening + MINUTE,
                clock_ms(),
                "observed",
                Decimal(1),
                source,
            )
            try:
                self.write(
                    "repair",
                    store.put,
                    instrument,
                    observation,
                    expected_previous=None if prior is None else prior.checksum,
                )
            except ProtocolError:
                latest = store.get(instrument, "trade_flow", source, opening, as_of_ms=clock_ms())
                if latest is None or latest.observation.mapping().get("AGGTRADE_EVENTCOUNT") != 0:
                    raise
            log("trade_confirmed_empty", instrument=instrument.key, open_ms=opening)
            return True
        end = opening + MINUTE
        fetched = self.fetch.repair_trade(instrument, opening)
        if fetched is None:
            return False
        values, events, archive_ids = fetched
        quantity = values["AGGTRADE_QUANTITYBUY"] + values["AGGTRADE_QUANTITYSELL"]
        complete = quantity == volume
        if not complete:
            values["AGGTRADE_KLINE_QUANTITY"] = volume
            values["AGGTRADE_KLINE_DELTA"] = quantity - volume
        values["AGGTRADE_REST_REPAIR"] = 1
        if prior is not None and not complete:
            previous = prior.observation.mapping()
            if all(previous.get(name) == value for name, value in values.items()):
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
        values["AGGTRADE_REST_ARCHIVE_DIGEST"] = int(digest, 16)
        try:
            self.write(
                "repair",
                store.put,
                instrument,
                AuxiliaryInput(
                    "trade_flow",
                    tuple(values.items()),
                    opening,
                    end,
                    end,
                    clock_ms(),
                    "observed" if complete else "partial",
                    Decimal(int(complete)),
                    source,
                ),
                expected_previous=None if prior is None else prior.checksum,
            )
        except ProtocolError:
            latest = store.get(instrument, "trade_flow", source, opening, as_of_ms=clock_ms())
            if latest is None or latest.observation.quality != "observed":
                raise
            return True
        if complete:
            log("trade_repaired", instrument=instrument.key, open_ms=opening, events=events)
        else:
            log(
                "trade_provider_totals_differ",
                instrument=instrument.key,
                open_ms=opening,
                aggregate_quantity=str(quantity),
                candle_volume=str(volume),
            )
        return complete

    def recent_trade_gaps(self, db, store):
        now = clock_ms()
        by_id = {series: instrument for instrument, series in self.inventory.items()}
        try:
            candidates = recent_unconfirmed_trades(db, by_id, now, self.retry, limit=15)
        except QueryCanceled:
            log("trade_repair_scan_timeout")
            return
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

    def _tasks(self):
        lanes = {channel: [] for channel in CHANNELS}
        for instrument in self.inventory:
            for channel in CHANNELS:
                found = self.origins.get((instrument.key, channel))
                if found is not None and found[0] == "verified":
                    lanes[channel].append((instrument, channel, found[2]))
        for group in zip_longest(*lanes.values()):
            yield from (job for job in group if job is not None)

    def _pause_backfill(self, paused):
        try:
            self.fetch.set_backfill_paused(paused)
        except (OSError, OmegaError) as error:
            log("backfill_control_retry", paused=paused, error=type(error).__name__)
            return False
        return True

    def _prepare(self, db, bars, pool, task):
        instrument, channel, first = task
        if channel == "kline":
            return self._prepare_kline(db, bars, pool, instrument, first)
        return self.auxiliary_history.prepare(db, pool, instrument, channel, first)

    def _persist(self, db, bars, auxiliary, task, prepared):
        instrument, channel, first = task
        if channel == "kline":
            return self.write_kline_day(db, bars, instrument, prepared)
        result = self.auxiliary_history.step(
            db, auxiliary, instrument, channel, first, prepared=prepared
        )
        if result:
            log("auxiliary_batch", instrument=instrument.key, channel=channel, **result)
        return bool(result)

    def _task_error(self, task, error):
        instrument, channel, _ = task
        if isinstance(error, BackfillPaused):
            return
        if isinstance(error, RateLimited):
            self.throttled[instrument, channel] = time.monotonic() + max(1, error.retry_after or 0)
            log(
                "backfill_throttled",
                instrument=instrument.key,
                channel=channel,
                seconds=error.retry_after,
            )
            return
        log(
            "kline_history_error" if channel == "kline" else "auxiliary_history_error",
            instrument=instrument.key,
            channel=channel,
            error=type(error).__name__,
        )

    def _repair_if_due(self, db, auxiliary):
        if clock_ms() >= self.next_repair and not self.stop.is_set():
            self.recent_trade_gaps(db, auxiliary)
            self.next_repair = clock_ms() + TRADE_REPAIR_INTERVAL_MS

    def _fetch_round(self, db, bars, auxiliary, pool):
        todo, pending = iter(self._tasks()), deque()
        capacity = min(self.workers, BACKFILL_MAX_PAGES)
        exhausted = paused = progressed = False
        resume_at = 0.0
        try:
            while not self.stop.is_set() and (pending or not exhausted):
                self._repair_if_due(db, auxiliary)
                # Reserve a full transaction's rows for every open fetch, including
                # those still on the wire. Count completed requests before admitting more.
                buffered = sum(
                    future is not None and future.done() and future.exception() is None
                    for _, _, future in pending
                )
                if not paused and buffered >= BACKFILL_PAUSE_PAGES:
                    paused = True
                    self._pause_backfill(True)
                    log("backfill_paused", reserved_rows=buffered * BAR_WRITE_BATCH_SIZE)
                if paused and buffered <= BACKFILL_RESUME_PAGES and time.monotonic() >= resume_at:
                    if self._pause_backfill(False):
                        paused = False
                        log("backfill_resumed", reserved_rows=buffered * BAR_WRITE_BATCH_SIZE)
                    else:
                        resume_at = time.monotonic() + 1
                while not paused and not exhausted and len(pending) < capacity:
                    task = next(todo, None)
                    if task is None:
                        exhausted = True
                        break
                    instrument, channel, _ = task
                    if self.throttled.get((instrument, channel), 0) > time.monotonic():
                        continue
                    try:
                        prepared = self._prepare(db, bars, pool, task)
                    except (OSError, OmegaError, ArithmeticError) as error:
                        self._task_error(task, error)
                        continue
                    if prepared is not None:
                        pending.append((task, prepared, prepared[-1]))
                for item in pending:
                    task, prepared, future = item
                    if future is None or future.done():
                        pending.remove(item)
                        try:
                            progressed |= self._persist(db, bars, auxiliary, task, prepared)
                        except (OSError, OmegaError, ArithmeticError) as error:
                            self._task_error(task, error)
                        del item, prepared, future
                        break
                else:
                    if pending:
                        wait(
                            [future for _, _, future in pending],
                            timeout=0.25,
                            return_when=FIRST_COMPLETED,
                        )
                    elif paused:
                        self.stop.wait(0.25)
        finally:
            for _, _, future in pending:
                if future is not None:
                    future.cancel()
            if paused and not self.stop.is_set():
                self.resume_pending = not self._pause_backfill(False)
        return progressed

    def run(self):
        self.retry, self.throttled = {}, {}
        self.next_repair, next_discovery = 0, 0
        self.resume_pending = True
        with (
            connect_database(self.settings) as db,
            closing(PostgresBarSeriesStore(self.settings, on_commit=self.on_commit)) as bars,
            closing(PostgresAuxiliaryStore(self.settings, on_commit=self.on_commit)) as auxiliary,
            ThreadPoolExecutor(
                max_workers=min(self.workers, BACKFILL_MAX_PAGES),
                thread_name_prefix="market-prefetch",
            ) as pool,
        ):
            verify_schema(db)
            # Clear any pause left by an interrupted previous data manager.
            while not self.stop.is_set():
                if self.resume_pending:
                    self.resume_pending = not self._pause_backfill(False)
                    if self.resume_pending:
                        self.stop.wait(1)
                        continue
                if shutil.disk_usage(self.root).free < DISK_RESERVE:
                    log("disk_reserve", minimum_bytes=DISK_RESERVE)
                    self.stop.wait(60)
                    continue
                now = clock_ms()
                self._repair_if_due(db, auxiliary)
                if self.origins is None or now >= next_discovery:
                    self.check_origins(db)
                    next_discovery = clock_ms() + MINUTE
                if not self._fetch_round(db, bars, auxiliary, pool):
                    self.stop.wait(15)
