"""Durable owner shutdown orchestration; financial actions stay in participants."""

# Imports
import json
import re
import threading
import time
from collections import OrderedDict, deque
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import decimal
from omega.research.artifacts import read_status_json, validate_artifact_path
from omega.runtime.settings import atomic_json
from omega.runtime.shutdown_release import ExitCompletionIntent, RootReleaseCorrection

KINDS = frozenset({"review", "save_exit", "flatten_exit", "cancel"})
TERMINAL = frozenset(
    {"completed", "failed", "canceled", "interrupted", "blocked", "review_required"}
)
MAX_BYTES = 1_048_576


def identity(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value) is None:
        raise ValidationError("invalid shutdown operation identity")
    return value


def encoded(value):
    raw = json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()
    if len(raw) > MAX_BYTES:
        raise ValidationError("shutdown record exceeds capacity")
    return raw


def parameters(kind, value):
    if kind not in KINDS or type(value) is not dict:
        raise ValidationError("invalid shutdown request")
    keys = {"acknowledge_unmanaged_positions"} if kind == "save_exit" else set()
    if set(value) != keys or keys and type(value["acknowledge_unmanaged_positions"]) is not bool:
        raise ValidationError("unexpected shutdown parameters")
    return deepcopy(value)


def participant_view(value):
    """Validate small owner facts without evaluating a strategy or calculating fills."""
    value = json.loads(encoded(value))
    required = {
        "owner",
        "ready",
        "durable",
        "supports_flatten",
        "positions",
        "pending_orders",
        "unknown",
        "notes",
    }
    if type(value) is not dict or set(value) != required:
        raise ValidationError("shutdown participant schema mismatch")
    if not all(type(value[k]) is bool for k in ("ready", "durable", "supports_flatten")):
        raise ValidationError("shutdown participant flags must be native booleans")
    identity(value["owner"])
    for field in ("positions", "pending_orders", "unknown", "notes"):
        if type(value[field]) is not list or len(value[field]) > 1000:
            raise ValidationError("shutdown participant list exceeds capacity")
    for field in ("unknown", "notes"):
        if any(type(v) is not str or not 1 <= len(v) <= 256 for v in value[field]):
            raise ValidationError("invalid shutdown participant diagnostic")
    for field in ("positions", "pending_orders"):
        seen = set()
        for row in value[field]:
            if (
                type(row) is not dict
                or not {"account_id", "instance", "instrument", "quantity"} <= row.keys()
            ):
                raise ValidationError("invalid shutdown exposure")
            for key in ("account_id", "instance", "instrument"):
                if type(row[key]) is not str or not 1 <= len(row[key]) <= 256:
                    raise ValidationError("invalid shutdown exposure identity")
            if (
                type(row["quantity"]) is not str
                or len(row["quantity"]) > 128
                or not decimal(row["quantity"])
            ):
                raise ValidationError("shutdown exposures need finite nonzero decimal quantities")
            key = row["account_id"], row["instance"], row["instrument"]
            if field == "pending_orders":
                if type(row.get("reducing")) is not bool or decimal(row["quantity"]) <= 0:
                    raise ValidationError("invalid shutdown pending quantity/type")
                # Existing financial order IDs are wider than file-backed shutdown IDs.
                order_id = row.get("order_id")
                if (
                    type(order_id) is not str
                    or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", order_id) is None
                ):
                    raise ValidationError("invalid shutdown pending order identity")
                key += (row["order_id"],)
            if key in seen:
                raise ValidationError("duplicate shutdown exposure identity")
            seen.add(key)
    return value


def summary(views):
    return {
        "positions_count": sum(len(v["positions"]) for v in views),
        "pending_count": sum(len(v["pending_orders"]) for v in views),
        "unknown_count": sum(len(v["unknown"]) + int(not v["ready"]) for v in views),
        "durable": all(v["durable"] for v in views),
        "supports_flatten": all(v["supports_flatten"] for v in views),
        "notes": list(dict.fromkeys(note for v in views for note in v["notes"])),
    }


class ShutdownService:
    def __init__(
        self,
        root,
        participants,
        *,
        core_epoch,
        pause_entries,
        on_exit=None,
        on_incident=None,
        clock_ms=None,
    ):
        self.root = Path(root).absolute() / "state/shutdown"
        self.core_epoch = core_epoch
        self.participants = tuple(participants)
        self.pause_entries, self.on_exit, self.on_incident = pause_entries, on_exit, on_incident
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.condition = threading.Condition(threading.RLock())
        self.queue, self.cache = deque(), OrderedDict()
        self.thread = None
        self.closed = self.ready = self.storage_failed = False
        self.error = None
        self.exit_intent = False
        self._exit_completion = None
        self._root_release_correction = RootReleaseCorrection(core_epoch, encoded)
        self.views = []
        self.index = {
            "schema_version": 1,
            "core_epoch": core_epoch,
            "revision": 0,
            "active": None,
            "recent": [],
            "state": "idle",
            "last_exit": None,
        }

    def _now(self):
        value = self.clock_ms()
        if type(value) is not int or not 0 <= value <= 253_402_300_799_000:
            raise ValidationError("invalid shutdown clock")
        return value

    def _path(self, op_id):
        return self.root / "operations" / (identity(op_id) + ".json")

    def _read(self, path):
        path = validate_artifact_path(path)
        value = read_status_json(path)
        if (
            type(value) is not dict
            or set(value) != {"data", "checksum"}
            or value["checksum"] != sha256(encoded(value["data"])).hexdigest()
        ):
            raise StorageError("shutdown record checksum mismatch")
        return value["data"]

    def _write(self, path, value):
        try:
            validate_artifact_path(path, allow_missing=True)
            atomic_json(path, {"data": value, "checksum": sha256(encoded(value)).hexdigest()})
        except Exception:
            self.storage_failed, self.ready, self.error = (
                True,
                False,
                "shutdown_recording_unavailable",
            )
            raise StorageError("shutdown record commit is not confirmed") from None

    def _remember(self, row):
        self.cache[row["id"]] = deepcopy(row)
        self.cache.move_to_end(row["id"])
        while len(self.cache) > 32:
            key = next(
                (
                    k
                    for k in self.cache
                    if k != self.index["active"] and self.cache[k]["state"] in TERMINAL
                ),
                None,
            )
            if key is None:
                break
            del self.cache[key]  # Immutable receipt remains on disk for exact-ID lookup.

    def _save_index(self):
        self._write(self.root / "index.json", self.index)

    def _record(self, op_id, updates, *, state=None, active=...):
        with self.condition:
            if self.closed or self.storage_failed:
                raise StorageError("shutdown owner is retired")
            row = deepcopy(self.cache[op_id])
            row.update(deepcopy(updates))
            self._write(self._path(op_id), row)
            self._remember(row)
            self.index["revision"] += 1
            if state is not None:
                self.index["state"] = state
            if active is not ...:
                self.index["active"] = active
            self._save_index()
            self.condition.notify_all()
            return row

    def _bootstrap(self):
        path = self.root / "index.json"
        if path.exists():
            self.index = self._read(path)
            if (
                set(self.index)
                != {
                    "schema_version",
                    "core_epoch",
                    "revision",
                    "active",
                    "recent",
                    "state",
                    "last_exit",
                }
                or self.index["schema_version"] != 1
                or type(self.index["revision"]) is not int
                or self.index["revision"] < 0
                or type(self.index["recent"]) is not list
                or len(self.index["recent"]) > 32
            ):
                raise StorageError("invalid shutdown index")
            for op_id in list(
                dict.fromkeys(
                    [
                        *self.index["recent"],
                        *([self.index["active"]] if self.index["active"] else []),
                    ]
                )
            ):
                row = self._read(self._path(op_id))
                if row.get("id") != op_id or row.get("kind") not in KINDS:
                    raise StorageError("invalid shutdown operation")
                parameters(row["kind"], row["parameters"])
                if row["state"] not in TERMINAL:
                    row.update(state="interrupted", error="core_restarted")
                    self._write(self._path(op_id), row)
                self._remember(row)
            self.index.update(core_epoch=self.core_epoch, active=None, state="idle")
            self.index["revision"] += 1
        elif (self.root / "operations").exists() and any((self.root / "operations").iterdir()):
            raise StorageError("shutdown index is missing beside existing receipts")
        self._save_index()
        self.ready = True

    def start(self):
        with self.condition:
            if self.closed:
                raise ValidationError("shutdown service is closed")
            if self.thread is None:
                self.thread = threading.Thread(
                    target=self._loop, name="Omega-Shutdown", daemon=True
                )
                self.thread.start()
        return self.snapshot()

    def submit(self, op_id, core_epoch, expected_revision, kind, values):
        identity(op_id)
        values = parameters(kind, values)
        with self.condition:
            if self.closed or not self.ready or self.storage_failed:
                raise ValidationError("shutdown service is unavailable")
            if core_epoch != self.core_epoch:
                raise ValidationError("shutdown core epoch is stale")
            path = self._path(op_id)
            previous = self.cache.get(op_id) or (self._read(path) if path.exists() else None)
            if previous is not None:
                if (
                    previous["kind"] != kind
                    or previous["parameters"] != values
                    or previous["expected_revision"] != expected_revision
                ):
                    raise ValidationError("shutdown ID conflicts with its original request")
                return deepcopy(previous)
            if type(expected_revision) is not int or expected_revision != self.index["revision"]:
                raise ValidationError("shutdown revision is stale")
            if len(self.queue) >= 8:
                raise ValidationError("shutdown queue is full")
            if self.index["active"] and kind not in {"review", "cancel", "save_exit"}:
                raise ValidationError("a shutdown drain is already active")
            if (
                self.index["active"]
                and self.cache[self.index["active"]]["state"] == "exit_requested"
                and kind != "review"
            ):
                raise ValidationError("core exit has already been requested")
            if kind in {"save_exit", "flatten_exit"}:
                self.exit_intent = True
            row = {
                "id": op_id,
                "kind": kind,
                "parameters": values,
                "core_epoch": core_epoch,
                "expected_revision": expected_revision,
                "state": "queued",
                "summary": None,
                "result": None,
                "error": None,
                "issued_at_ms": self._now(),
            }
            self._write(path, row)
            self._remember(row)
            self.index["recent"] = [k for k in self.index["recent"] if k != op_id][-31:] + [op_id]
            self.index["revision"] += 1
            self._save_index()
            self.queue.append(op_id)
            self.condition.notify_all()
            return deepcopy(row)

    def status(self, op_id):
        with self.condition:
            row = self.cache.get(identity(op_id))
            if row is None:
                row = self._read(self._path(op_id))
                if row.get("id") != op_id:
                    raise StorageError("shutdown receipt identity mismatch")
            row = deepcopy(row)
            if self.storage_failed and row["state"] not in TERMINAL:
                row.update(state="unknown", error="shutdown_recording_unavailable")
            return row

    def snapshot(self):
        with self.condition:
            view = (
                summary(self.views)
                if self.views
                else {
                    "positions_count": 0,
                    "pending_count": 0,
                    "unknown_count": 1,
                    "durable": False,
                    "supports_flatten": False,
                    "notes": [],
                }
            )
            return {
                "schema_version": 1,
                "core_epoch": self.core_epoch,
                "revision": self.index["revision"],
                "state": "closed"
                if self.closed
                else self.index["state"]
                if self.ready
                else "starting"
                if not self.error
                else "failed",
                "ready": self.ready,
                "closed": self.closed,
                "error": self.error,
                "operation_id": self.index["active"],
                "summary": deepcopy(view),
                "participants": deepcopy(self.views),
                "operations": [deepcopy(v) for v in self.cache.values()],
            }

    @property
    def blocks_new_risk(self):
        with self.condition:
            return (
                self.storage_failed
                and self.exit_intent
                or self.index["active"] is not None
                or any(
                    row["core_epoch"] == self.core_epoch
                    and row["kind"] in {"save_exit", "flatten_exit"}
                    and row["state"] in {"queued", "running", "waiting_fill", "exit_requested"}
                    for row in self.cache.values()
                )
            )

    def _observe(self):
        views = [participant_view(p.shutdown_snapshot()) for p in self.participants]
        if len({v["owner"] for v in views}) != len(views):
            raise ValidationError("duplicate shutdown participant")
        with self.condition:
            self.views = views
        return summary(views)

    def _cancel_active(self):
        active = self.index["active"]
        if active:
            for participant in self.participants:
                participant.shutdown_cancel(active)
            self._record(
                active,
                {"state": "canceled", "result": {"pending_preserved": True}},
                state="canceled",
                active=None,
            )

    def _exit(self, op_id, facts):
        if self.on_exit is None:
            self._record(
                op_id,
                {"state": "blocked", "summary": facts, "error": "core_exit_callback_missing"},
                state="blocked",
                active=None,
            )
            return
        self._record(
            op_id,
            {
                "state": "exit_requested",
                "summary": facts,
                "result": {"exit_requested": True, "core_exit_confirmed": False},
            },
            state="exit_requested",
            active=op_id,
        )
        if self.on_exit is not None:
            self.on_exit()

    def _execute(self, op_id):
        row = self.status(op_id)
        self._record(op_id, {"state": "running"})
        if row["kind"] == "cancel":
            self._cancel_active()
            self._record(
                op_id,
                {"state": "completed", "result": {"canceled": True, "pending_preserved": True}},
                state="canceled",
                active=None,
            )
            return
        if row["kind"] != "review":
            self._cancel_active()
            self.pause_entries(op_id)
        facts = self._observe()
        if row["kind"] == "review":
            self._record(
                op_id,
                {"state": "completed", "summary": facts, "result": facts},
                state=None if self.index["active"] else "idle",
            )
            return
        if not facts["durable"] or facts["unknown_count"]:
            self._record(
                op_id,
                {"state": "blocked", "summary": facts, "error": "financial_state_unconfirmed"},
                state="blocked",
            )
            return
        if row["kind"] == "save_exit":
            if (facts["positions_count"] or facts["pending_count"]) and not row["parameters"][
                "acknowledge_unmanaged_positions"
            ]:
                self._record(
                    op_id,
                    {
                        "state": "review_required",
                        "summary": facts,
                        "error": "unmanaged_positions_confirmation_required",
                    },
                    state="review_required",
                )
                return
            self._exit(op_id, facts)
            return
        if not facts["supports_flatten"]:
            self._record(
                op_id,
                {"state": "blocked", "summary": facts, "error": "participant_flatten_unsupported"},
                state="blocked",
            )
            return
        self._record(op_id, {"state": "running", "summary": facts}, state="draining", active=op_id)
        for participant in self.participants:
            participant.shutdown_drain(op_id, row["issued_at_ms"])
        self._record(op_id, {"state": "waiting_fill"}, state="draining")
        self._poll()

    def _poll(self):
        active = self.index["active"]
        if not active or self.cache[active]["state"] != "waiting_fill":
            return
        facts = self._observe()
        if not facts["durable"] or facts["unknown_count"]:
            self._record(
                active,
                {"state": "blocked", "summary": facts, "error": "financial_state_unconfirmed"},
                state="blocked",
            )
        elif not facts["positions_count"] and not facts["pending_count"]:
            self._exit(active, facts)
        elif facts != self.cache[active]["summary"]:
            self._record(active, {"summary": facts})

    def _loop(self):
        try:
            with self.condition:
                self._bootstrap()
            while True:
                with self.condition:
                    if self.closed or self.storage_failed:
                        return
                    if not self.queue:
                        active = self.index["active"]
                        self.condition.wait(
                            0.5
                            if active and self.cache[active]["state"] == "waiting_fill"
                            else None
                        )
                    if self.closed or self.storage_failed:
                        return
                    op_id = self.queue.popleft() if self.queue else None
                try:
                    self._execute(op_id) if op_id else self._poll()
                except Exception as error:
                    if self.closed or self.storage_failed:
                        raise
                    active = op_id or self.index["active"]
                    if active:
                        self._record(
                            active,
                            {"state": "blocked", "error": type(error).__name__},
                            state="blocked",
                        )
        except Exception as error:
            with self.condition:
                self.error, self.ready = type(error).__name__, False
            if self.on_incident is not None:
                self.on_incident("SHUTDOWN_RECORDING_UNAVAILABLE")

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()
        if self.thread is not None and self.thread is not threading.current_thread():
            self.thread.join(6)
            if self.thread.is_alive():
                raise StorageError("shutdown coordinator cleanup is not confirmed")

    def finish_exit(self, *, successful):
        """Called by the core before releasing its instance lock, after worker join."""
        with self.condition:
            if self._exit_completion is None:
                active = self.index["active"]
                if not active or self.cache[active]["state"] != "exit_requested":
                    return
                # Fix both original/target documents before any write can lose
                # its acknowledgement. Active/cache cannot identify a retry.
                self._exit_completion = ExitCompletionIntent(
                    self.cache[active],
                    self.index,
                    core_epoch=self.core_epoch,
                    successful=successful,
                )
            intent = self._exit_completion
            operation = intent.original[0]["id"]

            def path(kind):
                return self._path(operation) if kind == "record" else self.root / "index.json"

            row, index = intent.complete(
                core_epoch=self.core_epoch,
                successful=successful,
                read=lambda kind: self._read(path(kind)),
                write=lambda kind, value: self._write(path(kind), value),
            )
            self._remember(row)
            self.index = index
            if intent.successful is True:
                return self._root_release_correction.arm(row, self.index)

    def fail_root_release(self, guard, *, root_lease_held):
        """Trusted Core only: correct a terminal ACK while its root lease is held.

        API admission must be closed and workers joined. The same process-local
        guard can finish an ACK-unknown partial correction. Never call after OS
        release; neither a terminal OP nor this correction proves process death.
        """
        with self.condition:
            if not self.closed or self.thread is not None and self.thread.is_alive():
                raise StorageError("root release correction requires a joined closed manager")
            operation = guard.get("operation_id") if type(guard) is dict else None

            def path(kind):
                return self._path(operation) if kind == "record" else self.root / "index.json"

            row, index = self._root_release_correction.correct(
                guard,
                core_epoch=self.core_epoch,
                root_lease_held=root_lease_held,
                read=lambda kind: self._read(path(kind)),
                write=lambda kind, value: self._write(path(kind), value),
            )
            self._remember(row)
            self.index = index
            self.condition.notify_all()
            return deepcopy(row)
