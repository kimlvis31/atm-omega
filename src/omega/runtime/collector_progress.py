"""Committed market byte rates and the checkpoint-based backfill estimate."""

import json
import time
from collections import OrderedDict
from threading import Lock

from omega.domain.errors import StorageError, ValidationError
from omega.infrastructure.candles import connect_database

BACKFILL_PROGRESS_PATH = "/api/collector/v2/backfill-progress"
WINDOWS = (60, 300)
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
    """Use one monotonic time window for all lanes, including gaps between commits."""

    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.started = int(clock())
        self.lock = Lock()
        self.buckets = OrderedDict()
        self.backfill_sizes = {}

    def _prune(self, second):
        while self.buckets and next(iter(self.buckets)) < second - max(WINDOWS):
            self.buckets.popitem(last=False)

    def record(self, scope, storage_channel, rows, size):
        channel = STORAGE_GROUP.get(storage_channel)
        if channel is None or not rows or not size:
            return
        with self.lock:
            second = int(self.clock())
            self._prune(second)
            bucket = self.buckets.setdefault(second, {})
            key = (scope, channel)
            bucket[key] = bucket.get(key, 0) + size
            if scope == "backfill":
                total_rows, total_bytes = self.backfill_sizes.get(channel, (0, 0))
                self.backfill_sizes[channel] = (total_rows + rows, total_bytes + size)

    def snapshot(self):
        with self.lock:
            now = self.clock()
            stop = int(now)
            self._prune(stop)
            # Exclude the current partial second and divide every lane by the same time.
            windows = {}
            for duration in WINDOWS:
                start = max(stop - duration, self.started)
                elapsed = stop - start
                inflight = [rows for second, rows in self.buckets.items() if start <= second < stop]
                scopes = {}
                for scope in SCOPES:
                    by_channel = {
                        channel: sum(bucket.get((scope, channel), 0) for bucket in inflight)
                        for channel in GROUPS
                    }
                    scopes[scope] = {
                        "channels": {
                            name: self._rate(size, elapsed) for name, size in by_channel.items()
                        },
                        "total": self._rate(sum(by_channel.values()), elapsed),
                    }
                combined = {
                    channel: sum(scopes[scope]["channels"][channel]["bytes"] for scope in SCOPES)
                    for channel in GROUPS
                }
                scopes["combined"] = {
                    "channels": {
                        name: self._rate(size, elapsed) for name, size in combined.items()
                    },
                    "total": self._rate(sum(combined.values()), elapsed),
                }
                windows[str(duration)] = {"elapsed_seconds": elapsed, **scopes}
            return windows, self.backfill_sizes.copy(), round((now - stop) * 1000)

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
        windows, samples, last_partial_ms = self.meter.snapshot()
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
