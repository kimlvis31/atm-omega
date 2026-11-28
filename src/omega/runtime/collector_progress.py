"""Committed market byte rates and the checkpoint-based backfill estimate."""

import json
import time
from collections import OrderedDict, defaultdict
from threading import Lock

from omega.domain.errors import StorageError, ValidationError
from omega.infrastructure.candles import connect_database

BACKFILL_PROGRESS_PATH = "/api/collector/v2/backfill-progress"
WINDOWS = (60, 300)
EMA_INTERVAL_MS = 100
EMA_SPAN = 100
EMA_ALPHA = 2 / (EMA_SPAN + 1)
_TICKS_PER_SECOND = 1000 // EMA_INTERVAL_MS
_TICK_NS = EMA_INTERVAL_MS * 1_000_000
MINUTE = 60_000
GROUPS = ("kline", "agg_trade", "depth", "metrics")
STORAGE_GROUP = {
    "kline": "kline",
    "trade_flow": "agg_trade",
    "depth": "depth",
    "open_interest": "metrics",
    "long_short_ratio": "metrics",
}
SCOPES = ("backfill", "live", "repair")


class TransferMeter:
    """Track bytes committed in shared, monotonic 100 ms periods."""

    def __init__(self, clock=None):
        self.clock_ns = time.monotonic_ns if clock is None else lambda: round(clock() * 1e9)
        self.started = self.clock_ns() // _TICK_NS
        self.lock = Lock()
        self.buckets = OrderedDict()
        self.backfill_sizes = {}
        self.ema_tick = self.started
        self.ema_rates = {}

    def _prune(self, tick):
        cutoff = tick - max(WINDOWS) * _TICKS_PER_SECOND
        while self.buckets and next(iter(self.buckets)) < cutoff:
            self.buckets.popitem(last=False)

    def _advance_ema(self, stop):
        if stop <= self.ema_tick:
            return
        bucket = self.buckets.get(self.ema_tick, {})
        keep = 1 - EMA_ALPHA
        # During idle time, apply all skipped zero samples in one step.
        idle_decay = keep ** (stop - self.ema_tick - 1)
        self.ema_rates = (
            {
                key: (
                    keep * self.ema_rates.get(key, 0)
                    + EMA_ALPHA * bucket.get(key, 0) * _TICKS_PER_SECOND
                )
                * idle_decay
                for key in self.ema_rates.keys() | bucket.keys()
            }
            if idle_decay
            else {}
        )
        self.ema_tick = stop

    def record(self, scope, storage_channel, rows, size):
        channel = STORAGE_GROUP.get(storage_channel)
        if channel is None or not rows or not size:
            return
        with self.lock:
            tick = self.clock_ns() // _TICK_NS
            self._advance_ema(tick)
            self._prune(tick)
            bucket = self.buckets.setdefault(tick, {})
            key = (scope, channel)
            bucket[key] = bucket.get(key, 0) + size
            if scope == "backfill":
                total_rows, total_bytes = self.backfill_sizes.get(channel, (0, 0))
                self.backfill_sizes[channel] = (total_rows + rows, total_bytes + size)

    def snapshot(self):
        with self.lock:
            stop, partial_ns = divmod(self.clock_ns(), _TICK_NS)
            self._advance_ema(stop)
            self._prune(stop)
            # Copy closed buckets while locked, then calculate without blocking commits.
            closed = tuple(bucket for tick, bucket in self.buckets.items() if tick < stop)
            recent = tuple(
                bucket
                for tick, bucket in self.buckets.items()
                if stop - WINDOWS[0] * _TICKS_PER_SECOND <= tick < stop
            )
            samples = self.backfill_sizes.copy()
            ema_rates = self.ema_rates.copy()
        windows = {}
        for duration, buckets in zip(WINDOWS, (recent, closed), strict=True):
            elapsed = min(stop - self.started, duration * _TICKS_PER_SECOND) / _TICKS_PER_SECOND
            counted = defaultdict(int)
            for bucket in buckets:
                for key, size in bucket.items():
                    counted[key] += size
            windows[str(duration)] = {
                "elapsed_seconds": elapsed,
                **self._format_scopes(
                    counted, lambda size, elapsed=elapsed: self._rate(size, elapsed)
                ),
            }
        ema = {
            "interval_ms": EMA_INTERVAL_MS,
            "span_samples": EMA_SPAN,
            "elapsed_samples": stop - self.started,
            **self._format_scopes(ema_rates, lambda rate: {"bytes_per_second": round(rate, 2)}),
        }
        return windows, samples, partial_ns // 1_000_000, ema

    @staticmethod
    def _format_scopes(values, render):
        channels = {
            scope: {channel: values.get((scope, channel), 0) for channel in GROUPS}
            for scope in SCOPES
        }
        channels["combined"] = {
            channel: sum(channels[scope][channel] for scope in SCOPES) for channel in GROUPS
        }
        return {
            scope: {
                "channels": {channel: render(value) for channel, value in by_channel.items()},
                "total": render(sum(by_channel.values())),
            }
            for scope, by_channel in channels.items()
        }

    @staticmethod
    def _rate(size, elapsed):
        return {"bytes": size, "bytes_per_second": round(size / elapsed, 2) if elapsed else None}


class BackfillProgress:
    def __init__(self, root, settings, inventory, meter):
        self.path = root / "auxiliary-history-progress.json"
        self.settings, self.inventory, self.meter = settings, inventory, meter
        self.ranges = None
        self.next_refresh = 0
        self.lock = Lock()

    def snapshot(self):
        with self.lock:
            if self.ranges is None or time.monotonic() >= self.next_refresh:
                self.ranges = self._read_ranges()
                self.next_refresh = time.monotonic() + 5
            ranges, ranges_at = self.ranges
        windows, samples, last_partial_ms, ema = self.meter.snapshot()
        channels = {}
        for channel, (target, verified, unresolved, pending) in ranges.items():
            slots = max(0, target - verified)
            rows, size = samples.get(channel, (0, 0))
            # The native five-minute metric slot has two persisted series.
            bytes_per_slot = size / rows * (2 if channel == "metrics" else 1) if rows else None
            channels[channel] = {
                "target_slots": target,
                "verified_slots": verified,
                "unresolved_slots": unresolved,
                "pending_target_series": pending,
                "verified_percent": self._percent(verified, target, pending),
                "sampled_rows": rows,
                "sampled_bytes": size,
                "bytes_per_slot_estimate": round(bytes_per_slot, 2) if rows else None,
                "remaining_bytes_estimate": (
                    None
                    if pending
                    else 0
                    if not slots
                    else round(slots * bytes_per_slot)
                    if rows
                    else None
                ),
            }
        total = {
            key: sum(entry[key] for entry in channels.values())
            for key in (
                "target_slots",
                "verified_slots",
                "unresolved_slots",
                "pending_target_series",
            )
        }
        remaining = [entry["remaining_bytes_estimate"] for entry in channels.values()]
        total["remaining_bytes_estimate"] = None if None in remaining else sum(remaining)
        total["verified_percent"] = self._percent(
            total["verified_slots"], total["target_slots"], total["pending_target_series"]
        )
        return {
            "schema_version": 1,
            "as_of_ms": time.time_ns() // 1_000_000 - last_partial_ms,
            "progress_as_of_ms": ranges_at,
            "byte_basis": "committed_postgresql_row",
            "windows": windows,
            "ema": ema,
            "channels": channels,
            "total": total,
        }

    @staticmethod
    def _percent(verified, target, pending):
        return round(verified * 100 / target, 4) if target and not pending else None

    def _read_ranges(self):
        from psycopg import Error

        try:
            jobs = json.loads(self.path.read_text(encoding="utf-8"))["jobs"]
        except FileNotFoundError:
            jobs = {}
        except (OSError, ValueError, KeyError):
            raise StorageError("collector backfill checkpoint is unavailable") from None
        keys = [instrument.key for instrument in self.inventory]
        try:
            with connect_database(self.settings) as connection:
                with connection.transaction():
                    connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    origins = connection.execute(
                        """SELECT instrument,channel,discovery_state,first_ms
                           FROM market.binance_history_origins WHERE instrument=ANY(%s)""",
                        (keys,),
                    ).fetchall()
                    klines = dict(
                        (instrument, (verified, unresolved, remaining))
                        for instrument, verified, unresolved, remaining in connection.execute(
                            """SELECT instrument,observed,missing,(end_ms-next_ms)/60000
                               FROM market.binance_kline_history WHERE instrument=ANY(%s)""",
                            (keys,),
                        )
                    )
        except Error:
            raise StorageError("collector progress database is unavailable") from None
        targets = {channel: [0, 0, 0, 0] for channel in GROUPS}
        by_origin = {
            (instrument, channel): (state, first) for instrument, channel, state, first in origins
        }
        for instrument in self.inventory:
            for channel in GROUPS:
                if instrument.market.value == "spot" and channel == "metrics":
                    continue
                key = instrument.key
                state, first = by_origin.get((key, channel), ("pending", None))
                if state == "unavailable":
                    continue
                progress = targets[channel]
                if state != "verified":
                    progress[3] += 1
                    continue
                if channel == "kline":
                    record = klines.get(key)
                    if record is None:
                        progress[3] += 1
                        continue
                    verified, unresolved, remaining = record
                    target = verified + unresolved + remaining
                else:
                    record = jobs.get(key + "|" + channel)
                    if record is None:
                        progress[3] += 1
                        continue
                    interval = 5 * MINUTE if channel == "metrics" else MINUTE
                    target = max(0, (record["end_ms"] - first // interval * interval) // interval)
                    verified, unresolved = record["verified"], record["unresolved_slots"]
                progress[0] += target
                progress[1] += verified
                progress[2] += unresolved
        return targets, time.time_ns() // 1_000_000


def read_backfill_progress(source, query):
    if query:
        raise ValidationError("backfill progress does not use query parameters")
    return source.snapshot()
