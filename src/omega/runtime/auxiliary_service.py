"""Durable, bounded public auxiliary jobs on a single background owner thread."""

import json
import re
import threading
import time
from collections import deque
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.runtime.auxiliary_collection import AuxiliaryCollection
from omega.runtime.auxiliary_journal import AuxiliaryJobJournal, validate_plan
from omega.runtime.auxiliary_repair import AuxiliaryRepair
from omega.runtime.auxiliary_statistics import AuxiliaryStatisticsCollector
from omega.runtime.control import InstanceLock
from omega.runtime.settings import atomic_json

KINDS = frozenset(
    {"trade_flow", "trade_flow_repair", "current_oi", "history_oi", "long_short_ratio"}
)
_ACTIVE = {"queued", "running", "cancelling"}
_STATES = _ACTIVE | {"completed", "partial", "cancelled", "unknown", "error"}
MAX_BYTES = 4_000_000
MAX_TIME = 253_402_300_799_000
_FIELDS = {
    "id",
    "kind",
    "parameters",
    "destination_id",
    "state",
    "execution_id",
    "generation",
    "cancel_requested",
    "worker_started",
    "archive_id",
    "result",
    "error",
}


def _encoded(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _hash(value):
    return type(value) is str and re.fullmatch("[a-f0-9]{64}", value) is not None


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate auxiliary service field")
        result[key] = value
    return result


def _identity(value):
    if type(value) is not str or re.fullmatch("[A-Za-z0-9_-]{1,96}", value) is None:
        raise ValidationError("invalid auxiliary service job identity")
    return value


def _instrument(value):
    try:
        provider, market, symbol = value.split(":")
        instrument = Instrument(provider, Market(market), symbol)
        if provider != "binance" or instrument.market not in {Market.SPOT, Market.USDM}:
            raise ValueError
        return instrument
    except (TypeError, AttributeError, ValueError):
        raise ValidationError("auxiliary jobs require a public Binance instrument") from None


def _parameters(kind, parameters):
    if type(kind) is not str or kind not in KINDS or type(parameters) is not dict:
        raise ValidationError("invalid auxiliary job kind or parameters")
    if kind == "trade_flow_repair":
        required = {"instrument", "parent_job_id", "parent_execution_id", "open_time_ms"}
        if not required <= parameters.keys() or parameters.keys() - required - {
            "limit",
            "max_pages",
        }:
            raise ValidationError("unexpected or missing repair parameters")
        _instrument(parameters["instrument"])
        _identity(parameters["parent_job_id"])
        result = {"limit": 500, "max_pages": 20, **deepcopy(parameters)}
        if (
            type(result["parent_execution_id"]) is not str
            or re.fullmatch("[a-f0-9]{32}", result["parent_execution_id"]) is None
            or type(result["open_time_ms"]) is not int
            or not 0 <= result["open_time_ms"] <= MAX_TIME - 60000
            or result["open_time_ms"] % 60000
            or type(result["limit"]) is not int
            or not 1 <= result["limit"] <= 1000
            or type(result["max_pages"]) is not int
            or not 1 <= result["max_pages"] <= 20
        ):
            raise ValidationError("repair requires one closed minute and its parent execution")
        return result
    allowed = {"instrument"}
    if kind != "current_oi":
        allowed |= {"start_ms", "end_ms", "limit"}
    if kind == "trade_flow":
        allowed.add("max_pages")
    required = {"instrument"} if kind == "current_oi" else {"instrument", "start_ms", "end_ms"}
    if not required <= parameters.keys() or parameters.keys() - allowed:
        raise ValidationError("unexpected or missing auxiliary job parameters")
    instrument = _instrument(parameters["instrument"])
    if instrument.market == Market.SPOT and kind != "trade_flow":
        raise ValidationError("Spot only supports trade-flow jobs")
    result = deepcopy(parameters)
    if kind != "current_oi":
        result.setdefault("limit", 500)
        if (
            any(type(result[key]) is not int for key in ("start_ms", "end_ms", "limit"))
            or not 0 <= result["start_ms"] < result["end_ms"] <= MAX_TIME
            or not 1 <= result["limit"] <= (1000 if kind == "trade_flow" else 500)
        ):
            raise ValidationError("invalid bounded auxiliary time range or limit")
        if kind != "trade_flow" and result["end_ms"] - result["start_ms"] > 28 * 86400000:
            raise ValidationError("statistics range cannot exceed 28 days")
    if kind == "trade_flow":
        result.setdefault("max_pages", 20)
        # Reuse the runner's one authoritative fixed-minute plan validation.
        validate_plan(_trade_plan(result, "0" * 64))
    return result


def _trade_plan(parameters, destination_id):
    return {
        **parameters,
        "source": parameters["instrument"] + ":aggTrade",
        "destination_id": destination_id,
    }


class _RecordingArchive:
    def __init__(self, archive, recorded):
        self.archive, self.recorded = archive, recorded

    def put(self, record):
        receipt = self.archive.put(record)
        self.recorded(receipt.archive_id)
        return receipt

    def get(self, archive_id):
        return self.archive.get(archive_id)


class AuxiliaryService:
    """No automatic replay. Running cancellation is cooperative, never a DB rollback promise.

    resource_factory(plan) returns (public_client, raw_archive, auxiliary_store).
    It runs on the worker; stores and their close operation stay on that owner.
    """

    def __init__(self, root, *, destination_id, resource_factory=None, clock_ms=None):
        if (
            not _hash(destination_id)
            or (resource_factory is not None and not callable(resource_factory))
            or (clock_ms is not None and not callable(clock_ms))
        ):
            raise ValidationError("auxiliary service requires its database destination")
        self.root = Path(root).resolve()
        self.path = self.root / "state/auxiliary-service.json"
        self.destination_id = destination_id
        self.resource_factory = resource_factory or self._resources
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.condition = threading.Condition(threading.RLock())
        self.queue, self.jobs, self.stops = deque(), {}, {}
        self.thread = None
        self.closed, self.storage_failed = False, False
        self._load()

    def _now(self):
        value = self.clock_ms()
        if type(value) is not int or not 0 <= value <= MAX_TIME:
            raise ValidationError("invalid actual auxiliary clock")
        return value

    def _save(self, jobs):
        data = {"schema_version": 1, "destination_id": self.destination_id, "jobs": jobs}
        envelope = {"data": data, "sha256": sha256(_encoded(data)).hexdigest()}
        if len(_encoded(envelope)) > MAX_BYTES:
            raise StorageError("auxiliary service journal exceeds capacity")
        try:
            if self.path.is_symlink():
                raise OSError
            atomic_json(self.path, envelope)
        except OSError:
            self.storage_failed = True
            for event in self.stops.values():
                event.set()
            raise StorageError("auxiliary service recording unavailable") from None

    def _load(self):
        if not self.path.exists():
            return
        try:
            if self.path.is_symlink():
                raise ValueError
            with self.path.open("rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise ValueError
            envelope = json.loads(raw, object_pairs_hook=_unique)
            if type(envelope) is not dict or set(envelope) != {"data", "sha256"}:
                raise ValueError
            data = envelope["data"]
            if (
                sha256(_encoded(data)).hexdigest() != envelope["sha256"]
                or type(data) is not dict
                or set(data) != {"schema_version", "destination_id", "jobs"}
                or type(data["schema_version"]) is not int
                or data["schema_version"] != 1
                or data["destination_id"] != self.destination_id
                or type(data["jobs"]) is not dict
                or len(data["jobs"]) > 128
            ):
                raise ValueError
            jobs = data["jobs"]
            for identity, row in jobs.items():
                _identity(identity)
                if (
                    type(row) is not dict
                    or set(row) != _FIELDS
                    or row["id"] != identity
                    or row["destination_id"] != self.destination_id
                    or _parameters(row["kind"], row["parameters"]) != row["parameters"]
                    or row["state"] not in _STATES
                    or type(row["execution_id"]) is not str
                    or re.fullmatch("[a-f0-9]{32}", row["execution_id"]) is None
                    or type(row["generation"]) is not int
                    or not 0 <= row["generation"] < 2**63
                    or type(row["cancel_requested"]) is not bool
                    or type(row["worker_started"]) is not bool
                    or row["archive_id"] is not None
                    and not _hash(row["archive_id"])
                    or row["result"] is not None
                    and type(row["result"]) is not dict
                    or row["error"] is not None
                    and (type(row["error"]) is not str or len(row["error"]) > 160)
                ):
                    raise ValueError
                if row["state"] in _ACTIVE:
                    row.update(state="unknown", error="interrupted_before_confirmation")
            self.jobs = jobs
            self._save(jobs)
        except (OSError, ValueError, TypeError, KeyError, ValidationError, RecursionError):
            raise StorageError("auxiliary service journal invalid; original preserved") from None

    def _require_owner(self):
        if self.closed or self.storage_failed:
            raise StorageError("auxiliary service is closed or recording is unavailable")

    def snapshot(self):
        with self.condition:
            return {
                "schema_version": 1,
                "jobs": deepcopy(list(self.jobs.values())),
                "closed": self.closed,
                "storage_failed": self.storage_failed,
                "worker_alive": bool(self.thread and self.thread.is_alive()),
            }

    def status(self, identity):
        _identity(identity)
        with self.condition:
            if identity not in self.jobs:
                raise ValidationError("auxiliary job not found")
            return deepcopy(self.jobs[identity])

    def _enqueue(self, row):
        if len(self.queue) >= 8:
            raise ValidationError("auxiliary job queue capacity reached")
        candidate = deepcopy(self.jobs)
        candidate[row["id"]] = row
        self._save(candidate)
        self.jobs = candidate
        self.stops[row["id"]] = threading.Event()
        self.queue.append((row["id"], row["execution_id"]))
        if self.thread is None or not self.thread.is_alive():
            self.thread = threading.Thread(
                target=self._loop, name="Omega-AuxiliaryJobs", daemon=True
            )
            self.thread.start()
        self.condition.notify_all()
        return deepcopy(row)

    def submit(self, identity, kind, parameters):
        _identity(identity)
        checked = _parameters(kind, parameters)
        with self.condition:
            self._require_owner()
            previous = self.jobs.get(identity)
            if previous is not None:
                if previous["kind"] != kind or previous["parameters"] != checked:
                    raise ValidationError("auxiliary job ID conflicts with its saved plan")
                return deepcopy(previous)
            if kind == "trade_flow_repair":
                parent = self.jobs.get(checked["parent_job_id"])
                if (
                    parent is None
                    or parent["kind"] != "trade_flow"
                    or parent["state"] != "partial"
                    or parent["execution_id"] != checked["parent_execution_id"]
                    or parent["parameters"]["instrument"] != checked["instrument"]
                    or not parent["parameters"]["start_ms"]
                    <= checked["open_time_ms"]
                    < parent["parameters"]["end_ms"]
                ):
                    raise ValidationError("repair parent changed or is not a partial trade job")
            target_end = (
                checked["open_time_ms"] + 60000
                if kind == "trade_flow_repair"
                else checked.get("end_ms")
            )
            if target_end is not None and target_end > self._now() // 60000 * 60000:
                raise ValidationError("auxiliary targets must end at a closed UTC minute")
            if len(self.jobs) >= 128:
                raise ValidationError("auxiliary job history capacity reached")
            return self._enqueue(
                {
                    "id": identity,
                    "kind": kind,
                    "parameters": checked,
                    "destination_id": self.destination_id,
                    "state": "queued",
                    "execution_id": uuid4().hex,
                    "generation": 0,
                    "cancel_requested": False,
                    "worker_started": False,
                    "archive_id": None,
                    "result": None,
                    "error": None,
                }
            )

    @staticmethod
    def _expected(row, execution_id):
        if execution_id is not None and execution_id != row["execution_id"]:
            raise ValidationError("stale auxiliary execution request")

    def cancel(self, identity, expected_execution_id=None):
        with self.condition:
            self._require_owner()
            row = self.status(identity)
            self._expected(row, expected_execution_id)
            if row["state"] not in _ACTIVE:
                return row
            row.update(
                cancel_requested=True,
                state="cancelled" if row["state"] == "queued" else "cancelling",
            )
            candidate = deepcopy(self.jobs)
            candidate[identity] = row
            self._save(candidate)
            self.jobs = candidate
            self.stops[identity].set()
            return deepcopy(row)

    def resume(self, identity, expected_execution_id=None):
        with self.condition:
            self._require_owner()
            row = self.status(identity)
            self._expected(row, expected_execution_id)
            if row["state"] in _ACTIVE:
                return row
            if row["state"] == "completed":
                return row
            if row["generation"] >= 2**63 - 1:
                raise ValidationError("auxiliary execution generation capacity reached")
            if (
                row["kind"] not in {"trade_flow", "trade_flow_repair"}
                and row["archive_id"] is None
                and row["worker_started"]
            ):
                raise ValidationError(
                    "statistics outcome unknown without archive; "
                    "manual repair or a new request is required"
                )
            row.update(
                state="queued",
                execution_id=uuid4().hex,
                generation=row["generation"] + 1,
                cancel_requested=False,
                worker_started=False,
                error=None,
            )
            return self._enqueue(row)

    def _resources(self, plan):
        from omega.runtime.auxiliary_resources import PublicAuxiliaryResources

        return PublicAuxiliaryResources(self.root, self.destination_id, self.clock_ms)(plan)

    def _record_archive(self, identity, execution, archive_id):
        if not _hash(archive_id):
            raise StorageError("archive returned an invalid receipt")
        with self.condition:
            self._require_owner()
            row = self.jobs[identity]
            if row["execution_id"] != execution or row["state"] not in {"running", "cancelling"}:
                raise StorageError("auxiliary archive callback belongs to a retired execution")
            candidate = deepcopy(self.jobs)
            candidate[identity]["archive_id"] = archive_id
            self._save(candidate)
            self.jobs = candidate

    def _execute(self, row, stop):
        parameters, kind = row["parameters"], row["kind"]
        instrument = _instrument(parameters["instrument"])
        source = instrument.key + (
            ":aggTrade" if kind in {"trade_flow", "trade_flow_repair"} else ":aux-service:" + kind
        )
        owner = InstanceLock(
            self.root
            / "state/locks"
            / (sha256((instrument.key + "\n" + source).encode()).hexdigest() + ".lock")
        )
        store = None
        try:
            if stop.is_set():
                raise InterruptedError
            client, archive, store = self.resource_factory(deepcopy(row))
            if getattr(store, "destination_id", None) != self.destination_id:
                raise ValidationError("auxiliary worker store destination does not match")
            archive = _RecordingArchive(
                archive,
                lambda identity: self._record_archive(row["id"], row["execution_id"], identity),
            )
            if stop.is_set():
                raise InterruptedError
            if kind == "trade_flow_repair":
                parent = self.status(parameters["parent_job_id"])
                if (
                    parent["kind"] != "trade_flow"
                    or parent["parameters"]["instrument"] != instrument.key
                ):
                    raise ValidationError("repair parent identity changed before worker start")
                plan = _trade_plan(parent["parameters"], self.destination_id)
                journal_root = self.root / "state/auxiliary-jobs"
                plan_id = sha256(_encoded(plan)).hexdigest()
                if AuxiliaryJobJournal.load_plan(journal_root, plan_id) != plan:
                    raise ValidationError("repair parent journal does not match its saved request")
                parent_journal = AuxiliaryJobJournal(journal_root, plan)
                repair = AuxiliaryRepair(
                    self.root,
                    row["id"],
                    parent_journal,
                    parameters["open_time_ms"],
                    self.destination_id,
                    limit=parameters["limit"],
                    max_pages=parameters["max_pages"],
                )
                # Once an attempt has started, its durable pin/pending is the
                # recovery authority. A later resume of the immutable parent must
                # not prevent reconciling an already submitted repair commit.
                if not repair.snapshot()["fetch_started"] and (
                    parent["state"] != "partial"
                    or parent["execution_id"] != parameters["parent_execution_id"]
                ):
                    raise ValidationError("repair parent execution changed before first fetch")
                return repair.run(
                    client,
                    archive,
                    store,
                    clock_ms=self.clock_ms,
                    stop_requested=stop.is_set,
                )
            if kind == "trade_flow":
                plan = _trade_plan(parameters, self.destination_id)
                journal = AuxiliaryJobJournal(self.root / "state/auxiliary-jobs", plan)
                return AuxiliaryCollection(
                    plan,
                    client,
                    archive,
                    store,
                    journal,
                    clock_ms=self.clock_ms,
                    stop_requested=stop.is_set,
                ).run()
            statistics = AuxiliaryStatisticsCollector(
                self.root, client, archive, store, self.clock_ms
            )
            if row["archive_id"] is not None:
                channel = {
                    "current_oi": "open_interest",
                    "history_oi": "open_interest_history",
                    "long_short_ratio": "long_short_ratio",
                }[kind]
                expected = {"symbol": instrument.symbol}
                if kind != "current_oi":
                    expected.update(
                        period="5m",
                        startTime=str(parameters["start_ms"]),
                        endTime=str(parameters["end_ms"] - 1),
                        limit=str(parameters["limit"]),
                    )
                return statistics.recover_archived(
                    row["archive_id"],
                    instrument=instrument,
                    channel=channel,
                    request_parameters=tuple(sorted(expected.items())),
                )
            if kind == "current_oi":
                return statistics.collect_current(instrument)
            operation = (
                statistics.collect_history if kind == "history_oi" else statistics.collect_ratios
            )
            return operation(
                instrument, parameters["start_ms"], parameters["end_ms"], parameters["limit"]
            )
        finally:
            try:
                if store is not None:
                    store.close()
            finally:
                owner.close()

    @staticmethod
    def _outcome(kind, result):
        if type(result) is not dict:
            raise ValidationError("auxiliary collector returned no public summary")
        _encoded(result)
        if kind in {"trade_flow", "trade_flow_repair"}:
            if result.get("complete") is True:
                return "completed"
            if result.get("commit_outcome") in {"unconfirmed", "confirmed_pending_delivery"}:
                return "unknown"
            if result.get("reason") == "stopped":
                return "cancelled"
            return "partial"
        return "completed" if result.get("status") == "committed" else "partial"

    def _loop(self):
        while True:
            with self.condition:
                while not self.queue and not self.closed:
                    self.condition.wait()
                if self.closed or self.storage_failed:
                    return
                identity, execution = self.queue.popleft()
                row = self.jobs[identity]
                if row["execution_id"] != execution or row["state"] != "queued":
                    continue
                candidate = deepcopy(self.jobs)
                candidate[identity]["state"] = "running"
                candidate[identity]["worker_started"] = True
                try:
                    self._save(candidate)
                except StorageError:
                    self.jobs[identity].update(state="unknown", error="recording_unavailable")
                    return
                self.jobs = candidate
                row, stop = deepcopy(candidate[identity]), self.stops[identity]
            try:
                result = self._execute(row, stop)
                state, error = self._outcome(row["kind"], result), None
            except InterruptedError:
                result, state, error = None, "cancelled", None
            except Exception as exc:
                result, state, error = None, "unknown", type(exc).__name__[:120]
            with self.condition:
                current = self.jobs[identity]
                if self.closed or current["execution_id"] != execution:
                    continue
                candidate = deepcopy(self.jobs)
                candidate[identity].update(state=state, result=result, error=error)
                try:
                    self._save(candidate)
                except StorageError:
                    self.jobs[identity].update(state="unknown", error="recording_unavailable")
                    return
                self.jobs = candidate

    def close(self):
        with self.condition:
            if self.closed:
                return {
                    "closed": True,
                    "durable": not self.storage_failed,
                    "worker_alive": bool(self.thread and self.thread.is_alive()),
                }
            candidate = deepcopy(self.jobs)
            for row in candidate.values():
                if row["state"] in _ACTIVE:
                    row.update(
                        state="unknown",
                        cancel_requested=True,
                        error="owner_closed_before_confirmation",
                    )
            durable = True
            try:
                self._save(candidate)
            except StorageError:
                durable = False
            self.jobs = candidate
            self.closed = True
            for event in self.stops.values():
                event.set()
            self.condition.notify_all()
            return {
                "closed": True,
                "durable": durable,
                "worker_alive": bool(self.thread and self.thread.is_alive()),
            }
