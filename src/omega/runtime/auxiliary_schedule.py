"""One bounded public REST scheduler; no strategy, order or market-store ownership.

This first slice schedules existing fixed-minute collectors. It does not supply
WS/depth, reservation persistence, automatic repair or a strategy subscription.
Only the worker creates, uses and closes injected HTTP/archive/store resources.
"""

import json
import math
import re
import threading
import time
from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from omega.domain.errors import (
    ProtocolError,
    RateLimited,
    StorageError,
    TransportError,
    ValidationError,
)
from omega.domain.models import Instrument, Market
from omega.runtime.auxiliary_collection import AuxiliaryCollection
from omega.runtime.auxiliary_journal import AuxiliaryJobJournal
from omega.runtime.auxiliary_statistics import AuxiliaryStatisticsCollector
from omega.runtime.control import InstanceLock
from omega.runtime.settings import atomic_json

MINUTE, PERIOD, MAX_BYTES = 60_000, 300_000, 4_000_000
MAX_TIME = 253_402_300_799_000
CHANNELS = {"trade_flow", "open_interest_history", "long_short_ratio"}
STATE_FIELDS = {
    "schema_version",
    "destination_id",
    "revision",
    "generation",
    "enabled",
    "subscriptions",
    "tasks",
    "journal_slots",
    "recent",
}


def _hash(value):
    return type(value) is str and re.fullmatch("[a-f0-9]{64}", value) is not None


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate auxiliary schedule field")
        result[key] = value
    return result


def _instrument(key):
    try:
        provider, market, symbol = key.split(":")
        value = Instrument(provider, Market(market), symbol)
        if provider != "binance" or value.market not in {Market.SPOT, Market.USDM}:
            raise ValueError
        return value
    except (TypeError, ValueError, AttributeError):
        raise ValidationError("schedule requires a public Binance instrument") from None


def _time(value):
    return type(value) is int and 0 <= value <= MAX_TIME


def _subscriptions(values):
    if type(values) not in {list, tuple} or len(values) > 512:
        raise ValidationError("auxiliary subscriptions must be a bounded list")
    merged = {}
    for value in values:
        if type(value) is not dict or set(value) != {"instrument", "channels"}:
            raise ValidationError("subscription requires instrument and channels")
        instrument = _instrument(value["instrument"])
        channels = value["channels"]
        if (
            type(channels) not in {list, tuple}
            or not 1 <= len(channels) <= 2
            or any(
                type(item) is not str or item not in {"trade_flow", "metric"} for item in channels
            )
        ):
            raise ValidationError("subscriptions support trade_flow and metric")
        if "metric" in channels and instrument.market == Market.SPOT:
            raise ValidationError("Spot native OI/LS is unsupported")
        merged.setdefault(instrument.key, set()).update(channels)
    if len(merged) > 128:
        raise ValidationError("auxiliary schedule supports at most 128 instruments")
    return [
        {"instrument": key, "channels": sorted(channels)}
        for key, channels in sorted(merged.items())
    ]


class _UnchangedSamples:
    """Reject an automatic revision of an already observed native point."""

    def __init__(self, store):
        self.store = store
        self.destination_id = store.destination_id

    def get(self, *args, **kwargs):
        return self.store.get(*args, **kwargs)

    def put(self, instrument, observation, *, expected_previous=None):
        prior = self.store.get(
            instrument,
            observation.channel,
            observation.source,
            observation.window_start_ms,
            as_of_ms=observation.available_at_ms,
        )
        if prior is not None:
            old = prior.observation
            same = replace(old, available_at_ms=observation.available_at_ms) == observation and all(
                type(left[1]) is type(right[1])
                for left, right in zip(old.values, observation.values, strict=True)
            )
            if not same:
                raise ProtocolError("native point changed; automatic revision is forbidden")
        return self.store.put(instrument, observation, expected_previous=expected_previous)


class _ArchiveReceipt:
    def __init__(self, archive, callback):
        self.archive, self.callback = archive, callback

    def put(self, record):
        receipt = self.archive.put(record)
        self.callback(receipt.archive_id)
        return receipt

    def get(self, identity):
        return self.archive.get(identity)


class _TradeRequests:
    """Keep the provider cooldown that the bounded runner's summary omits."""

    def __init__(self, client):
        self.client, self.retry_after_ms = client, 0

    def agg_trades(self, *args, **kwargs):
        try:
            return self.client.agg_trades(*args, **kwargs)
        except RateLimited as exc:
            delay = exc.retry_after
            if type(delay) in {int, float} and math.isfinite(delay) and delay >= 0:
                self.retry_after_ms = min(MAX_TIME, math.ceil(delay * 1000))
            raise


class AuxiliarySchedule:
    """Explicit start/configure; restart never automatically issues requests.

    resource_factory({kind, parameters}) is injected by the runtime composition
    and must supply the existing shared public budgets and bound local DB.
    Journal capacity counts this scheduler's reserved child/attempt slots only;
    it is not a cross-service filesystem quota or an automatic retention policy.
    """

    def __init__(
        self,
        root,
        *,
        destination_id,
        resource_factory,
        clock_ms=None,
        journal_capacity=10_000,
        max_retries=6,
        backoff_ms=5_000,
        max_backoff_ms=300_000,
        native_delay_ms=15_000,
        native_grace_ms=180_000,
        limit=1000,
        max_pages=20,
    ):
        if (
            not _hash(destination_id)
            or not callable(resource_factory)
            or (clock_ms is not None and not callable(clock_ms))
        ):
            raise ValidationError("schedule requires destination, worker factory and clock")
        for value, low, high in (
            (journal_capacity, 2, 1_000_000),
            (max_retries, 1, 20),
            (backoff_ms, 1, 300_000),
            (max_backoff_ms, 1, 3_600_000),
            (native_delay_ms, 1, 180_000),
            (native_grace_ms, 1, 600_000),
            (limit, 1, 1000),
            (max_pages, 1, 20),
        ):
            if type(value) is not int or not low <= value <= high:
                raise ValidationError("invalid bounded auxiliary schedule policy")
        if backoff_ms > max_backoff_ms or native_delay_ms >= native_grace_ms:
            raise ValidationError("invalid retry or native publication grace")
        self.root, self.destination_id = Path(root).absolute(), destination_id
        self.directory = self.root / "state/auxiliary-schedule"
        self.path = self.directory / "schedule.json"
        self.resource_factory, self.clock_ms = (
            resource_factory,
            clock_ms or (lambda: time.time_ns() // 1_000_000),
        )
        self.capacity, self.max_retries = journal_capacity, max_retries
        self.backoff_ms, self.max_backoff_ms = backoff_ms, max_backoff_ms
        self.native_delay_ms, self.native_grace_ms = native_delay_ms, native_grace_ms
        self.limit, self.max_pages = limit, max_pages
        self.condition = threading.Condition()
        self.thread, self.closed, self.working, self.storage_failed = None, False, False, False
        self.stop_event = threading.Event()
        self._last_clock, self._failure = None, None
        self.data = {
            "schema_version": 1,
            "destination_id": destination_id,
            "revision": 0,
            "generation": 0,
            "enabled": False,
            "subscriptions": [],
            "tasks": {},
            "journal_slots": 0,
            "recent": [],
        }
        self._guard()
        if self.path.exists():
            try:
                self.data = self._read(self.path)
                self._validate(self.data)
            except (KeyError, TypeError, ValueError, ValidationError, RecursionError):
                raise StorageError(
                    "invalid auxiliary schedule descriptor; original preserved"
                ) from None
            self.data["enabled"] = False
            for row in self.data["tasks"].values():
                row["state"] = "unknown" if row["attempt"] is not None else "stopped"
                row["reason"] = "manual_apply_required"

    def _guard(self):
        for path in (
            self.root / "state",
            self.directory,
            self.path,
            self.directory / "attempts",
            self.directory / "owner.lock",
        ):
            if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
                raise StorageError("auxiliary schedule cannot use linked paths")

    def _read(self, path):
        self._guard()
        if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
            raise StorageError("linked auxiliary attempt is forbidden")
        try:
            with path.open("rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
            envelope = json.loads(raw, object_pairs_hook=_unique)
            if (
                len(raw) > MAX_BYTES
                or type(envelope) is not dict
                or set(envelope) != {"data", "sha256"}
                or (envelope["sha256"] != sha256(_encoded(envelope["data"])).hexdigest())
            ):
                raise ValueError
            return envelope["data"]
        except (OSError, ValueError, TypeError, KeyError, RecursionError):
            raise StorageError(
                "auxiliary schedule metadata is unavailable; original preserved"
            ) from None

    def _validate(self, data):
        if (
            type(data) is not dict
            or set(data) != STATE_FIELDS
            or (
                type(data["schema_version"]) is not int
                or data["schema_version"] != 1
                or data["destination_id"] != self.destination_id
                or any(
                    type(data[key]) is not int or not 0 <= data[key] <= 2**63 - 1
                    for key in ("revision", "generation", "journal_slots")
                )
                or type(data["enabled"]) is not bool
                or type(data["tasks"]) is not dict
                or len(data["tasks"]) > 384
                or type(data["recent"]) is not list
                or len(data["recent"]) > 64
                or _subscriptions(data["subscriptions"]) != data["subscriptions"]
            )
        ):
            raise StorageError("invalid auxiliary schedule descriptor")
        allowed = {
            item["instrument"] + "|" + channel
            for item in data["subscriptions"]
            for channel in (
                (["trade_flow"] if "trade_flow" in item["channels"] else [])
                + (
                    ["open_interest_history", "long_short_ratio"]
                    if "metric" in item["channels"]
                    else []
                )
            )
        }
        if set(data["tasks"]) != allowed or any(
            type(row) is not dict
            or set(row) != {"instrument", "channel", "start_ms", "outcome", "reason"}
            or not _time(row["start_ms"])
            or row["channel"] not in CHANNELS
            or type(row["reason"]) is not str
            or len(row["reason"]) > 160
            or row["outcome"] not in {"committed", "partial", "empty", "retry", "unknown"}
            for row in data["recent"]
        ):
            raise StorageError("invalid auxiliary schedule demand or recent history")
        fields = {
            "instrument",
            "channel",
            "next_ms",
            "state",
            "attempt",
            "failures",
            "retry_at_ms",
            "reason",
            "last_run_ms",
        }
        for key, row in data["tasks"].items():
            if (
                type(row) is not dict
                or set(row) != fields
                or key != row["instrument"] + "|" + row["channel"]
            ):
                raise StorageError("invalid auxiliary schedule task")
            instrument = _instrument(row["instrument"])
            if (
                row["channel"] not in CHANNELS
                or instrument.market == Market.SPOT
                and row["channel"] != "trade_flow"
                or (
                    any(
                        not _time(row[field]) for field in ("next_ms", "retry_at_ms", "last_run_ms")
                    )
                    or row["next_ms"] % (MINUTE if row["channel"] == "trade_flow" else PERIOD)
                    or row["state"]
                    not in {"queued", "running", "backoff", "unknown", "blocked", "stopped"}
                    or type(row["failures"]) is not int
                    or not 0 <= row["failures"] <= 1000
                    or type(row["reason"]) is not str
                    or len(row["reason"]) > 160
                    or row["attempt"] is not None
                    and (
                        type(row["attempt"]) is not str
                        or re.fullmatch("[a-f0-9]{32}", row["attempt"]) is None
                    )
                )
            ):
                raise StorageError("invalid auxiliary schedule task values")

    def _write(self, path, data):
        self._guard()
        envelope = {"data": data, "sha256": sha256(_encoded(data)).hexdigest()}
        if len(_encoded(envelope)) > MAX_BYTES:
            raise StorageError("auxiliary schedule capacity reached")
        try:
            atomic_json(path, envelope)
        except OSError:
            raise StorageError("auxiliary schedule commit outcome unknown") from None

    def _save(self, candidate):
        self._validate(candidate)
        try:
            self._write(self.path, candidate)
        except StorageError:
            self.storage_failed = True
            self.stop_event.set()
            raise
        self.data = candidate

    def _now(self):
        value = self.clock_ms()
        if not _time(value):
            raise ValidationError("invalid actual auxiliary scheduling clock")
        if self._last_clock is not None and value < self._last_clock:
            raise ProtocolError("auxiliary schedule clock moved backward")
        self._last_clock = value
        return value

    def snapshot(self):
        with self.condition:
            result = deepcopy(self.data)
            result.update(
                closed=self.closed,
                worker_alive=bool(self.thread and self.thread.is_alive()),
                storage_available=not self.storage_failed,
                manual_apply_required=not self.data["enabled"],
                error=self._failure,
            )
            return result

    def configure(self, subscriptions, *, expected_revision, live_start_ms=None):
        normalized, now = _subscriptions(subscriptions), self._now()
        closed = now // MINUTE * MINUTE
        start = max(0, closed - MINUTE) if live_start_ms is None else live_start_ms
        if (
            not _time(start)
            or start % MINUTE
            or not max(0, closed - 10 * MINUTE) <= start <= closed
        ):
            raise ValidationError("live start must be aligned within the last ten closed minutes")
        with self.condition:
            if self.closed or self.storage_failed or self.working:
                raise StorageError("stop and await an available auxiliary owner before configure")
            if type(expected_revision) is not int or expected_revision != self.data["revision"]:
                raise ProtocolError("auxiliary schedule revision conflict")
            candidate, tasks = deepcopy(self.data), {}
            for subscription in normalized:
                channels = set(subscription["channels"]) - {"metric"}
                if "metric" in subscription["channels"]:
                    channels.update({"open_interest_history", "long_short_ratio"})
                for channel in sorted(channels):
                    instrument = subscription["instrument"]
                    key = instrument + "|" + channel
                    old = candidate["tasks"].get(key)
                    row = (
                        deepcopy(old)
                        if old
                        else {
                            "instrument": instrument,
                            "channel": channel,
                            "next_ms": start
                            if channel == "trade_flow"
                            else (now if live_start_ms is None else start) // PERIOD * PERIOD,
                            "state": "queued",
                            "attempt": None,
                            "failures": 0,
                            "retry_at_ms": 0,
                            "reason": "configured",
                            "last_run_ms": 0,
                        }
                    )
                    if row["attempt"] is None:
                        row.update(state="queued", reason="configured")
                    tasks[key] = row
            if any(
                row["attempt"] is not None and key not in tasks
                for key, row in candidate["tasks"].items()
            ):
                raise ProtocolError(
                    "resolve pending auxiliary attempts before removing subscriptions"
                )
            candidate.update(
                tasks=tasks,
                subscriptions=normalized,
                enabled=True,
                revision=candidate["revision"] + 1,
                generation=candidate["generation"] + 1,
            )
            self._save(candidate)
            self.stop_event.clear()
            self.condition.notify_all()
            return self.snapshot()

    def start(self):
        with self.condition:
            if self.closed or self.storage_failed:
                raise StorageError("auxiliary schedule is closed or unavailable")
            if self.thread is None:
                self.thread = threading.Thread(
                    target=self._loop, name="omega-auxiliary-schedule", daemon=True
                )
                self.thread.start()
            return self.snapshot()

    def notify(self):
        with self.condition:
            self.condition.notify_all()

    def stop(self, *, expected_generation=None):
        with self.condition:
            if expected_generation is not None and (
                type(expected_generation) is not int
                or expected_generation != self.data["generation"]
            ):
                raise ProtocolError("auxiliary schedule generation conflict")
            candidate = deepcopy(self.data)
            candidate.update(enabled=False, generation=candidate["generation"] + 1)
            for row in candidate["tasks"].values():
                row.update(state="unknown" if row["attempt"] else "stopped", reason="stopped")
            self._save(candidate)
            self.stop_event.set()
            self.condition.notify_all()
            return self.snapshot()

    def resume(self, instrument, channel, *, expected_generation):
        if type(channel) is not str or channel not in CHANNELS:
            raise ValidationError("unknown auxiliary channel")
        key = _instrument(instrument).key + "|" + channel
        with self.condition:
            if self.closed or self.storage_failed or self.working:
                raise StorageError("auxiliary owner is not available for resume")
            if (
                type(expected_generation) is not int
                or expected_generation != self.data["generation"]
            ):
                raise ProtocolError("auxiliary schedule generation conflict")
            if key not in self.data["tasks"]:
                raise ValidationError("unknown auxiliary subscription")
            row = self.data["tasks"][key]
            if row["retry_at_ms"] > self._now():
                raise ProtocolError("auxiliary retry backoff has not elapsed")
            candidate = deepcopy(self.data)
            candidate["tasks"][key].update(state="queued", failures=0, reason="manual_resume")
            candidate["enabled"] = True
            self._save(candidate)
            self.stop_event.clear()
            self.condition.notify_all()
            return self.snapshot()

    def _attempt_path(self, identity):
        if type(identity) is not str or re.fullmatch("[a-f0-9]{32}", identity) is None:
            raise StorageError("invalid fixed auxiliary attempt identity")
        return self.directory / "attempts" / (identity + ".json")

    def _prepare(self, key, now):
        candidate = deepcopy(self.data)
        row = candidate["tasks"][key]
        if row["attempt"] is None:
            if candidate["journal_slots"] + 2 > self.capacity:
                row.update(state="blocked", reason="journal_capacity_reached")
                self._save(candidate)
                return None
            identity = uuid4().hex
            attempt = {
                "schema_version": 1,
                "id": identity,
                "destination_id": self.destination_id,
                "instrument": row["instrument"],
                "channel": row["channel"],
                "start_ms": row["next_ms"],
                "end_ms": row["next_ms"] + (MINUTE if row["channel"] == "trade_flow" else 1),
                "archive_id": None,
                "limit": self.limit if row["channel"] == "trade_flow" else 1,
                "max_pages": self.max_pages,
            }
            self._write(self._attempt_path(identity), attempt)
            row["attempt"] = identity
            candidate["journal_slots"] += 2
        row.update(state="running", last_run_ms=now, reason="collecting")
        self._save(candidate)
        return deepcopy(row)

    def _load_attempt(self, row):
        value = self._read(self._attempt_path(row["attempt"]))
        expected = {
            "schema_version",
            "id",
            "destination_id",
            "instrument",
            "channel",
            "start_ms",
            "end_ms",
            "archive_id",
            "limit",
            "max_pages",
        }
        if (
            type(value) is not dict
            or set(value) != expected
            or value["schema_version"] != 1
            or type(value["schema_version"]) is not int
            or (
                value["id"] != row["attempt"]
                or value["destination_id"] != self.destination_id
                or value["instrument"] != row["instrument"]
                or value["channel"] != row["channel"]
                or not _time(value["start_ms"])
                or value["start_ms"] != row["next_ms"]
                or not _time(value["end_ms"])
                or value["end_ms"]
                != value["start_ms"] + (MINUTE if row["channel"] == "trade_flow" else 1)
                or value["archive_id"] is not None
                and not _hash(value["archive_id"])
                or type(value["limit"]) is not int
                or not 1 <= value["limit"] <= 1000
                or row["channel"] != "trade_flow"
                and value["limit"] != 1
                or type(value["max_pages"]) is not int
                or not 1 <= value["max_pages"] <= 20
            )
        ):
            raise StorageError("fixed auxiliary attempt is invalid")
        return value

    def _execute(self, row):
        attempt = self._load_attempt(row)
        instrument, channel = _instrument(row["instrument"]), row["channel"]
        source = instrument.key + (
            ":aggTrade" if channel == "trade_flow" else ":aux-schedule:" + channel
        )
        owner = InstanceLock(
            self.root
            / "state/locks"
            / (sha256((instrument.key + "\n" + source).encode()).hexdigest() + ".lock")
        )
        resources = ()
        journal = None
        try:
            if self.stop_event.is_set():
                raise InterruptedError
            kind = (
                "trade_flow"
                if channel == "trade_flow"
                else "history_oi"
                if channel == "open_interest_history"
                else "long_short_ratio"
            )
            parameters = {
                "instrument": instrument.key,
                "start_ms": attempt["start_ms"],
                "end_ms": attempt["end_ms"],
                "limit": attempt["limit"],
            }
            if channel == "trade_flow":
                parameters["max_pages"] = attempt["max_pages"]
            supplied = self.resource_factory({"kind": kind, "parameters": parameters})
            resources = tuple(supplied) if type(supplied) in {tuple, list} else ()
            if len(resources) != 3:
                raise ValidationError(
                    "auxiliary resource factory requires client, archive and store"
                )
            client, archive, store = resources
            if getattr(store, "destination_id", None) != self.destination_id:
                raise ProtocolError("auxiliary worker destination changed")

            def recorded(identity):
                if not _hash(identity):
                    raise StorageError("invalid archived public receipt")
                attempt["archive_id"] = identity
                # Fixed attempt metadata is independent of the mutable generation.
                # A retired worker cannot overwrite a later descriptor/cursor.
                self._write(self._attempt_path(attempt["id"]), attempt)
                if self.stop_event.is_set():
                    raise InterruptedError("public response archived after stop")

            archive = _ArchiveReceipt(archive, recorded)
            if self.stop_event.is_set():
                raise InterruptedError
            if channel == "trade_flow":
                plan = {
                    **parameters,
                    "source": instrument.key + ":aggTrade",
                    "destination_id": self.destination_id,
                }
                journal = AuxiliaryJobJournal(self.root / "state/auxiliary-jobs", plan)
                identity = attempt["archive_id"]
                saved = journal.snapshot()
                if identity is not None and identity not in saved["batches"]:
                    if saved["pending"] is not None or saved["deliveries"]:
                        raise ProtocolError("untracked raw response conflicts with frozen input")
                    entry = archive.get(identity)
                    record = entry.record
                    parameters_raw = dict(record.request_parameters)
                    if (
                        record.instrument != instrument
                        or record.channel != "agg_trades"
                        or (
                            len(parameters_raw) != len(record.request_parameters)
                            or parameters_raw.get("symbol") != instrument.symbol
                            or parameters_raw.get("limit") != str(attempt["limit"])
                        )
                    ):
                        raise ProtocolError("orphan trade response has another request identity")
                    if set(parameters_raw) == {"symbol", "limit", "fromId"}:
                        identifiers = []
                        for previous_id in saved["batches"]:
                            data = json.loads(
                                archive.get(previous_id).record.body, object_pairs_hook=_unique
                            )
                            if type(data) is not list or any(
                                type(item.get("a")) is not int for item in data
                            ):
                                raise ProtocolError("prior trade archive has invalid IDs")
                            identifiers.extend(item["a"] for item in data)
                        if not identifiers or parameters_raw["fromId"] != str(max(identifiers) + 1):
                            raise ProtocolError("orphan trade page is not the next archived ID")
                    elif set(parameters_raw) == {"symbol", "limit", "startTime", "endTime"}:
                        if (
                            saved["batches"]
                            or parameters_raw["startTime"]
                            != str(max(0, attempt["start_ms"] - MINUTE))
                            or parameters_raw["endTime"]
                            not in {str(attempt["end_ms"] - 1), str(attempt["end_ms"] + MINUTE - 1)}
                        ):
                            raise ProtocolError("orphan trade response has another fixed window")
                    else:
                        raise ProtocolError("invalid orphan trade response parameters")
                    journal.record_batch(entry.receipt)
                requests = _TradeRequests(client)
                result = AuxiliaryCollection(
                    plan,
                    requests,
                    archive,
                    store,
                    journal,
                    clock_ms=self.clock_ms,
                    stop_requested=self.stop_event.is_set,
                ).run()
                actual = journal.snapshot()
                if (
                    result.get("target_committed") is True
                    and actual["next_ms"] == attempt["end_ms"]
                    and actual["pending"] is None
                    and len(actual["deliveries"]) == 1
                ):
                    return "committed" if result.get("complete") is True else "partial", result
                if actual["pending"] is not None or actual["deliveries"]:
                    return "unknown", {"reason": "frozen_commit_requires_resume"}
                if result.get("error") in {"TransportError", "RateLimited", "TimeoutError"}:
                    return "retry", {
                        "reason": result["error"],
                        "retry_after_ms": requests.retry_after_ms,
                    }
                return "unknown", {"reason": result.get("reason", "collection_unconfirmed")}
            statistics = AuxiliaryStatisticsCollector(
                self.root, client, archive, _UnchangedSamples(store), self.clock_ms
            )
            if attempt["archive_id"] is not None:
                expected = tuple(
                    sorted(
                        {
                            "symbol": instrument.symbol,
                            "period": "5m",
                            "startTime": str(attempt["start_ms"]),
                            "endTime": str(attempt["end_ms"] - 1),
                            "limit": str(attempt["limit"]),
                        }.items()
                    )
                )
                result = statistics.recover_archived(
                    attempt["archive_id"],
                    instrument=instrument,
                    channel=channel,
                    request_parameters=expected,
                )
            else:
                operation = (
                    statistics.collect_history
                    if channel == "open_interest_history"
                    else statistics.collect_ratios
                )
                result = operation(
                    instrument, attempt["start_ms"], attempt["end_ms"], attempt["limit"]
                )
            if result.get("status") == "committed":
                return "committed" if result.get("received_rows") else "empty", result
            return "unknown", {"reason": result.get("reason", "statistics_unconfirmed")}
        except (TransportError, RateLimited, TimeoutError) as exc:
            return ("retry" if attempt["archive_id"] is None else "unknown"), {
                "reason": type(exc).__name__,
                "retry_after_ms": int(max(0, getattr(exc, "retry_after", 0) or 0) * 1000),
            }
        except Exception as exc:
            return "unknown", {"reason": type(exc).__name__}
        finally:
            try:
                for resource in reversed(resources):
                    close = getattr(resource, "close", None)
                    if callable(close):
                        close()
            finally:
                owner.close()

    def _finish(self, key, generation, row, outcome, result, now):
        with self.condition:
            if (
                self.closed
                or self.data["generation"] != generation
                or self.data["tasks"].get(key, {}).get("attempt") != row["attempt"]
            ):
                return
            candidate = deepcopy(self.data)
            current = candidate["tasks"][key]
            current["failures"] += 1
            delay = min(
                self.max_backoff_ms, self.backoff_ms * 2 ** min(current["failures"] - 1, 20)
            )
            current["retry_at_ms"] = min(
                MAX_TIME, now + max(delay, result.get("retry_after_ms", 0))
            )
            if outcome in {"committed", "partial"}:
                current.update(
                    next_ms=current["next_ms"]
                    + (MINUTE if current["channel"] == "trade_flow" else PERIOD),
                    attempt=None,
                    state="queued",
                    failures=0,
                    retry_at_ms=0,
                    reason="committed"
                    if outcome == "committed"
                    else "partial_committed_manual_repair_only",
                )
            elif outcome == "empty":
                current["attempt"] = None
                if now >= current["next_ms"] + self.native_grace_ms:
                    current.update(
                        next_ms=current["next_ms"] + PERIOD,
                        state="queued",
                        failures=0,
                        retry_at_ms=0,
                        reason="native_sample_missing_after_grace",
                    )
                else:
                    current.update(state="backoff", reason="waiting_native_publication")
                    deadline = current["next_ms"] + self.native_grace_ms
                    current["retry_at_ms"] = (
                        deadline
                        if current["failures"] >= self.max_retries
                        else min(current["retry_at_ms"], deadline)
                    )
            elif outcome == "retry" and current["failures"] <= self.max_retries:
                current.update(state="backoff", reason="public_request_backoff")
            else:
                current.update(
                    state="unknown" if outcome == "unknown" else "blocked",
                    reason=str(result.get("reason", "manual_resume_required"))[:160],
                )
            candidate["recent"] = (
                candidate["recent"]
                + [
                    {
                        "instrument": row["instrument"],
                        "channel": row["channel"],
                        "start_ms": row["next_ms"],
                        "outcome": outcome,
                        "reason": current["reason"],
                    }
                ]
            )[-64:]
            self._save(candidate)

    def _loop(self):
        owner = None
        try:
            owner = InstanceLock(self.directory / "owner.lock")
            while True:
                with self.condition:
                    if self.closed or self.storage_failed:
                        return
                    if not self.data["enabled"]:
                        self.condition.wait()
                        continue
                    now = self._now()
                    rows = [
                        (
                            max(
                                row["next_ms"]
                                + (
                                    MINUTE
                                    if row["channel"] == "trade_flow"
                                    else self.native_delay_ms
                                ),
                                row["retry_at_ms"],
                            ),
                            row["last_run_ms"],
                            key,
                        )
                        for key, row in self.data["tasks"].items()
                        if row["state"] in {"queued", "backoff"}
                    ]
                    due = sorted(
                        (row for row in rows if row[0] <= now),
                        key=lambda row: (row[1], row[0], row[2]),
                    )
                    if not due:
                        self.condition.wait(
                            None
                            if not rows
                            else max(0.001, (min(row[0] for row in rows) - now) / 1000)
                        )
                        continue
                    key, generation = due[0][2], self.data["generation"]
                    row = self._prepare(key, now)
                    if row is None:
                        continue
                    self.working = True
                try:
                    outcome, result = self._execute(row)
                    self._finish(key, generation, row, outcome, result, self._now())
                finally:
                    with self.condition:
                        self.working = False
                        self.condition.notify_all()
        except Exception as exc:
            with self.condition:
                self.storage_failed = True
                self.stop_event.set()
                self._failure = type(exc).__name__
                self.condition.notify_all()
        finally:
            if owner is not None:
                owner.close()

    def close(self):
        failure = None
        with self.condition:
            if not self.closed:
                try:
                    if not self.storage_failed:
                        self.stop()
                except StorageError as exc:
                    failure = exc
                finally:
                    self.stop_event.set()
                    self.closed = True
                    self.condition.notify_all()
            thread = self.thread
        if thread is not None:
            thread.join(1)
            if thread.is_alive():
                raise StorageError("auxiliary worker shutdown is unconfirmed; owner lock retained")
        if failure is not None:
            raise failure
        if self.storage_failed:
            # Joining the worker does not acknowledge an earlier unknown stop
            # or descriptor write. Preserve that manual-recovery boundary.
            raise StorageError("auxiliary shutdown metadata remains unconfirmed")
        return self.snapshot()
