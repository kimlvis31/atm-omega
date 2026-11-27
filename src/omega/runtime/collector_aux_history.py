"""Resumable imports of verified Binance auxiliary archives."""

import json
from decimal import Context, Decimal, localcontext
from itertools import groupby

from omega.application.trade_flow import trade_flow_values
from omega.domain.errors import ProtocolError, StorageError
from omega.domain.market_inputs import AuxiliaryInput, TradeFlowEvent
from omega.domain.models import decimal
from omega.infrastructure.auxiliary_store import AUXILIARY_BATCH_SIZE
from omega.infrastructure.binance.vision import DAY
from omega.infrastructure.collector_coverage import (
    clear_unlisted_check,
    first_covered,
    note_source_check,
)
from omega.runtime.collector_write_queue import run_write
from omega.runtime.settings import atomic_json

MINUTE = 60_000
PERIOD = 5 * MINUTE
DEPTH_BOUNDS = tuple(map(Decimal, (".2", "1", "2", "3", "4", "5")))
DEPTH_SOURCE = "binance_depth_vision_v1"
ARCHIVE_FIELDS = {
    "open_interest": (
        ("METRIC_OPENINTEREST", "sum_open_interest"),
        ("METRIC_OPENINTERESTVALUE", "sum_open_interest_value"),
    ),
    "long_short_ratio": (("METRIC_LONGSHORTRATIO", "count_long_short_ratio"),),
}


def trade_provenance(row):
    values = row.values
    return TradeFlowEvent(
        instrument=row.instrument,
        aggregate_id=int(values[0]),
        first_trade_id=int(values[3]),
        last_trade_id=int(values[4]),
        price=decimal(values[1]),
        quantity=decimal(values[2]),
        buyer_is_maker=values[6].lower() in {"true", "1"},
        trade_time_ms=row.timestamp_ms,
        available_at_ms=row.receipt.received_at_ms,
        source=row.instrument.key + ":aggTrade",
    )


def depth_fields(rows):
    """Eta's piecewise redistribution of the archive's cumulative notional."""
    sides = {"BIDS": {}, "ASKS": {}}
    for row in rows:
        percent, _amount, notional = map(Decimal, row.values[1:])
        if not (
            percent.is_finite() and notional.is_finite() and 0 < abs(percent) <= 5 and notional >= 0
        ):
            raise ProtocolError("invalid archived depth level")
        levels = sides["BIDS" if percent < 0 else "ASKS"]
        level = abs(percent)
        if level in levels and levels[level] != notional:
            raise ProtocolError("conflicting archived depth level")
        levels[level] = notional

    fields = {}
    with localcontext(Context(prec=96)):
        for side, levels in sides.items():
            curve = [(Decimal(0), Decimal(0)), *sorted(levels.items())]
            if any(right[1] < left[1] for left, right in zip(curve, curve[1:], strict=False)):
                raise ProtocolError("archived cumulative depth is not monotonic")
            prior = Decimal(0)
            for index, bound in enumerate(DEPTH_BOUNDS):
                for (lo, lo_value), (hi, hi_value) in zip(curve, curve[1:], strict=False):
                    if lo < bound <= hi:
                        value = lo_value + (hi_value - lo_value) * (bound - lo) / (hi - lo)
                        fields[f"DEPTH_{side}{index}"] = (value - prior).quantize(
                            Decimal(".00000001")
                        )
                        prior = value
                        break
        return fields, Decimal(len(fields)) / 12


class HistoricalAuxiliaryFetch:
    def __init__(self, vision, clients, *, clock_ms, stop):
        self.vision, self.clients = vision, clients
        self.clock_ms, self.stop = clock_ms, stop

    def trade_day(self, instrument, start, end, volumes):
        observations, complete, seen = [], 0, set()
        rows = self.vision.iter_rows(instrument, "agg_trade", start, end)
        for opening, group in groupby(rows, key=lambda row: row.timestamp_ms // MINUTE * MINUTE):
            if self.stop.is_set():
                raise InterruptedError
            events = []
            for row in group:
                if len(events) >= 150_000:
                    raise ProtocolError("archived trade minute exceeds its evidence bound")
                events.append(trade_provenance(row))
            seen.add(opening)
            values = dict(trade_flow_values(events))
            qty = values["AGGTRADE_QUANTITYBUY"] + values["AGGTRADE_QUANTITYSELL"]
            proven = qty == volumes.get(opening) and all(
                right.aggregate_id == left.aggregate_id + 1
                for left, right in zip(events, events[1:], strict=False)
            )
            values["AGGTRADE_VISION_ARCHIVE"] = 1
            complete += int(proven)
            observations.append(
                AuxiliaryInput(
                    "trade_flow",
                    tuple(values.items()),
                    opening,
                    opening + MINUTE,
                    opening + MINUTE,
                    self.clock_ms(),
                    "observed" if proven else "partial",
                    Decimal(int(proven)),
                    instrument.key + ":aggTrade",
                ),
            )
        for opening, volume in volumes.items():
            if self.stop.is_set():
                raise InterruptedError
            if opening in seen or volume != 0:
                continue
            values = (*trade_flow_values(()), ("AGGTRADE_KLINE_ZERO", 1))
            observations.append(
                AuxiliaryInput(
                    "trade_flow",
                    values,
                    opening,
                    opening + MINUTE,
                    opening + MINUTE,
                    self.clock_ms(),
                    "observed",
                    Decimal(1),
                    instrument.key + ":aggTrade",
                ),
            )
            complete += 1
        return observations, complete

    def depth_day(self, instrument, start, end):
        observations, complete = [], 0
        rows = self.vision.iter_rows(instrument, "depth", start, end)
        for opening, group in groupby(rows, key=lambda row: row.timestamp_ms // MINUTE * MINUTE):
            if self.stop.is_set():
                raise InterruptedError
            last = None
            for stamp, levels in groupby(group, key=lambda row: row.timestamp_ms):
                fields, coverage = depth_fields(levels)
                if fields:
                    last = stamp, fields, coverage
            if last is None:
                continue
            stamp, fields, coverage = last
            fields["DEPTH_VISION_REBINNED"] = 1
            full = coverage == 1
            complete += int(full)
            observations.append(
                AuxiliaryInput(
                    "depth",
                    tuple(fields.items()),
                    opening,
                    opening + MINUTE,
                    stamp,
                    self.clock_ms(),
                    "observed" if full else "partial",
                    coverage,
                    instrument.key + ":" + DEPTH_SOURCE,
                ),
            )
        return observations, complete

    @staticmethod
    def _metric_input(instrument, channel, stamp, values, available):
        suffix = "open_interest_history" if channel == "open_interest" else "long_short_ratio"
        return AuxiliaryInput(
            channel,
            (*values, ("SOURCE_PERIOD_MS", PERIOD)),
            stamp,
            stamp + MINUTE,
            stamp,
            available,
            source=instrument.key + ":" + suffix,
        )

    def metric_day(self, instrument, start, end, from_archive):
        observations = []
        matched = {"open_interest": set(), "long_short_ratio": set()}
        if from_archive:
            for row in self.vision.iter_rows(instrument, "metrics", start, end):
                if self.stop.is_set():
                    raise InterruptedError
                columns = dict(zip(row.columns, row.values, strict=True))
                for channel, fields in ARCHIVE_FIELDS.items():
                    if not all(
                        columns.get(name, "").lower() not in {"", "nan", "null"}
                        for _, name in fields
                    ):
                        continue
                    values = tuple((field, Decimal(columns[name])) for field, name in fields)
                    if any(not value.is_finite() or value < 0 for _, value in values):
                        continue
                    observations.append(
                        self._metric_input(
                            instrument,
                            channel,
                            row.timestamp_ms,
                            values,
                            self.clock_ms(),
                        ),
                    )
                    matched[channel].add(row.timestamp_ms)
        else:
            client = self.clients[instrument.market]
            calls = (
                (
                    client.open_interest_history,
                    "open_interest",
                    ("METRIC_OPENINTEREST", "METRIC_OPENINTERESTVALUE"),
                ),
                (
                    client.long_short_ratio,
                    "long_short_ratio",
                    ("METRIC_LONGSHORTRATIO", "METRIC_LONGACCOUNT", "METRIC_SHORTACCOUNT"),
                ),
            )
            for call, channel, fields in calls:
                batch = call(instrument, start_ms=start, end_ms=end, limit=300)
                for row in batch.events:
                    if self.stop.is_set():
                        raise InterruptedError
                    if row.event_time_ms % PERIOD:
                        raise ProtocolError("unaligned Binance statistic")
                    observations.append(
                        self._metric_input(
                            instrument,
                            channel,
                            row.event_time_ms,
                            tuple(zip(fields, row.values, strict=True)),
                            row.available_at_ms,
                        ),
                    )
                    matched[channel].add(row.event_time_ms)
        return observations, len(matched["open_interest"] & matched["long_short_ratio"])


class DailyAuxiliaryHistory:
    def __init__(self, root, vision, clients, *, clock_ms, stop, fetch=None, write=run_write):
        self.root, self.remote = root, fetch is not None
        self.write = write
        self.fetch = fetch or HistoricalAuxiliaryFetch(
            vision, clients, clock_ms=clock_ms, stop=stop
        )
        self.vision = vision if fetch is None else fetch
        self.clock_ms, self.stop = clock_ms, stop
        self.path = root / "auxiliary-history-progress.json"
        self.progress = {}
        if self.path.exists():
            if self.path.stat().st_size > 1_048_576:
                raise StorageError("auxiliary history progress exceeds its bound")
            state = json.loads(self.path.read_text(encoding="utf-8"))
            if state.get("version") != 1 or not isinstance(state.get("jobs"), dict):
                raise StorageError("invalid auxiliary history checkpoint")
            self.progress = state["jobs"]

    def _save(self):
        atomic_json(self.path, {"version": 1, "jobs": self.progress})

    @staticmethod
    def _end(db, instrument, channel):
        # Historical depth has its own provenance. Streaming snapshots are the boundary.
        sources = {
            "agg_trade": (instrument.key + ":aggTrade",),
            "metrics": tuple(
                instrument.key + ":" + key for key in ("open_interest_history", "long_short_ratio")
            ),
            "depth": (instrument.key + ":binance_depth_ws_v1",),
        }[channel]
        native_channel = "trade_flow" if channel == "agg_trade" else channel
        if channel == "metrics":
            available = [
                first_covered(db, instrument.key, name, (source,))
                for name, source in zip(("open_interest", "long_short_ratio"), sources, strict=True)
            ]
            return None if None in available else max(available)
        return first_covered(db, instrument.key, native_channel, sources)

    def _trade_reference(self, db, instrument, start, end):
        checkpoint = db.execute(
            """SELECT historical_series_id,next_ms,end_ms,mirror_ms
               FROM market.binance_kline_history WHERE instrument=%s""",
            (instrument.key,),
        ).fetchone()
        if checkpoint is None:
            return None
        series, next_ms, live_start, mirror = checkpoint
        if next_ms < min(end, live_start) or (end > live_start and mirror < end):
            return None
        volumes = dict(
            db.execute(
                """SELECT (extract(epoch FROM open_time)*1000)::bigint,v
               FROM market.series_candles WHERE series_id=%s
                 AND open_time>=to_timestamp(%s/1000) AND open_time<to_timestamp(%s/1000)""",
                (series, start, end),
            ).fetchall()
        )
        return volumes

    def _trade_day(self, db, instrument, start, end):
        volumes = self._trade_reference(db, instrument, start, end)
        return None if volumes is None else self.fetch.trade_day(instrument, start, end, volumes)

    def _depth_day(self, instrument, start, end):
        return self.fetch.depth_day(instrument, start, end)

    def _metric_day(self, instrument, start, end, from_archive):
        return self.fetch.metric_day(instrument, start, end, from_archive)

    def _plan(self, db, instrument, channel, first):
        now = self.clock_ms()
        job_key = instrument.key + "|" + channel
        job = self.progress.get(job_key)
        interval = PERIOD if channel == "metrics" else MINUTE
        if job is None:
            cutoff = self._end(db, instrument, channel)
            if cutoff is None:
                return None
            job = {
                "next_ms": first // interval * interval,
                "end_ms": cutoff,
                "written": 0,
                "verified": 0,
                "unresolved_slots": 0,
            }
            self.progress[job_key] = job
            self._save()
        if job["next_ms"] >= job["end_ms"] or job.get("check_again_ms", 0) > now:
            return None
        start = job["next_ms"]
        end = min(job["end_ms"], (start // DAY + 1) * DAY)
        return job, start, end, interval

    def _known_minutes(self, db, instrument, channel, start, end, step):
        requested = {
            "agg_trade": (("trade_flow", "aggTrade"),),
            "depth": (("depth", DEPTH_SOURCE),),
            "metrics": (
                ("open_interest", "open_interest_history"),
                ("long_short_ratio", "long_short_ratio"),
            ),
        }[channel]
        present = set(range(start, end, step))
        for native, suffix in requested:
            row = db.execute(
                "SELECT observed FROM market.collector_coverage "
                "WHERE scope_key=%s AND channel=%s AND dataset=%s AND day_ms=%s",
                (instrument.key, native, instrument.key + ":" + suffix, start // DAY * DAY),
            ).fetchone()
            if row is None:
                return set()
            covered = set()
            for window in row[0]:
                first = (max(start, window.lower) + step - 1) // step * step
                covered.update(range(first, min(end, window.upper), step))
            present.intersection_update(covered)
        return present

    def prepare(self, db, pool, instrument, channel, first):
        plan = self._plan(db, instrument, channel, first)
        if plan is None:
            return None
        _job, start, end, interval = plan
        known = self._known_minutes(db, instrument, channel, start, end, interval)
        if len(known) == len(range(start, end, interval)):
            return known, None
        volumes = (
            self._trade_reference(db, instrument, start, end) if channel == "agg_trade" else None
        )
        if channel == "agg_trade" and volumes is None:
            return None
        return known, pool.submit(
            self.fetch.day,
            instrument,
            channel,
            start,
            end,
            channel == "metrics" and start >= self.clock_ms() - 27 * DAY,
            volumes,
            known,
        )

    def step(self, db, store, instrument, channel, first, *, prepared=None):
        plan = self._plan(db, instrument, channel, first)
        if plan is None:
            return False
        job, start, end, interval = plan
        now = self.clock_ms()
        if self.remote:
            if prepared is None:
                return False
            known, pending = prepared
            if pending is None:
                archive, from_rest, result = True, False, ([], len(known))
            else:
                archive, from_rest, observations, verified = pending.result()
                result = observations, verified + len(known)
        else:
            archive = self.vision.files(instrument, channel, start, end)
            from_rest = not archive and channel == "metrics" and start >= now - 27 * DAY
            if channel == "metrics" and (archive or from_rest):
                result = self._metric_day(instrument, start, end, bool(archive))
            elif archive and channel == "depth":
                result = self._depth_day(instrument, start, end)
            elif archive:
                result = self._trade_day(db, instrument, start, end)
            else:
                result = ([], 0)
        if not archive:
            note_source_check(
                db, instrument, channel, "vision", start, end, "unlisted", self.clock_ms()
            )
        if not archive and not from_rest and end >= now - 2 * DAY:
            job.update(check_again_ms=now + 60 * MINUTE, state="waiting_archive")
            self._save()
            return False
        if result is None:
            return False
        observations, verified = result
        written = 0
        # Keep the day checkpoint behind all committed chunks. A retry retains committed rows.
        for offset in range(0, len(observations), AUXILIARY_BATCH_SIZE):
            if self.stop.is_set():
                raise InterruptedError
            batch = observations[offset : offset + AUXILIARY_BATCH_SIZE]
            written += self.write("history", store.put_history_batch, instrument, batch)
        unresolved = max(0, (end - start) // interval - verified)
        if archive and job.get("state") == "waiting_archive":
            clear_unlisted_check(db, instrument, channel, start, end)
        if unresolved:
            if archive or from_rest:
                note_source_check(
                    db,
                    instrument,
                    channel,
                    "vision" if archive else "rest",
                    start,
                    end,
                    "incomplete" if observations else "no_rows",
                    self.clock_ms(),
                )
            path = (
                self.root
                / "auxiliary-history-unresolved"
                / instrument.market.value
                / instrument.symbol
                / channel
                / f"{start}.json"
            )
            atomic_json(
                path,
                {
                    "start_ms": start,
                    "end_ms": end,
                    "unresolved_slots": unresolved,
                    "status": "partial_archive"
                    if archive
                    else "rest_partial"
                    if from_rest
                    else "no_archive",
                },
            )
        job.update(
            next_ms=end,
            written=job["written"] + written,
            verified=job["verified"] + verified,
            unresolved_slots=job["unresolved_slots"] + unresolved,
            updated_ms=self.clock_ms(),
            state="scanned" if end >= job["end_ms"] else "active",
            check_again_ms=0,
        )
        self._save()
        return {
            "start_ms": start,
            "end_ms": end,
            "written": written,
            "verified": verified,
            "unresolved": unresolved,
        }
