"""Explicit one-minute repair attempts; parent history and normal resume stay immutable."""

import json
import re
from contextlib import closing
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.infrastructure.auxiliary_codec import decode_observation
from omega.ports.auxiliary import StoredAuxiliary
from omega.runtime.auxiliary_collection import AuxiliaryCollection
from omega.runtime.auxiliary_journal import AuxiliaryJobJournal, validate_plan
from omega.runtime.control import InstanceLock
from omega.runtime.settings import atomic_json

MAX_BYTES = 65_536


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate repair manifest field")
        result[key] = value
    return result


class _ReplayOnlyClient:
    def agg_trades(self, *args, **kwargs):
        raise ProtocolError("repair attempt requires a new operation for additional public GET")


class AuxiliaryRepair:
    """A service must hold its existing source owner lock around construction/run.

    The operation ID adds a namespace, not a new market dataset. Re-entering an
    attempted operation only replays saved raw batches/pending; it never GETs again.
    Source/archive/store ownership and close remain with the caller.
    """

    def __init__(
        self,
        root,
        operation_id,
        parent_journal,
        opening_ms,
        destination_id,
        limit=500,
        max_pages=20,
    ):
        if (
            type(operation_id) is not str
            or re.fullmatch("[A-Za-z0-9_-]{1,96}", operation_id) is None
            or not isinstance(parent_journal, AuxiliaryJobJournal)
            or type(opening_ms) is not int
            or opening_ms % 60_000
        ):
            raise ValidationError("invalid explicit auxiliary repair identity")
        parent = parent_journal.snapshot()
        original_plan = validate_plan(parent["plan"])
        if original_plan["destination_id"] != destination_id:
            raise ValidationError("repair destination differs from its parent")
        entry = next(
            (
                value
                for value in parent["deliveries"]
                if decode_observation(value["payload"].encode())[1].window_start_ms == opening_ms
            ),
            None,
        )
        if entry is None:
            raise ValidationError("repair requires a committed parent minute")
        instrument, observation = decode_observation(entry["payload"].encode())
        if (
            observation.quality not in {"partial", "missing"}
            or sha256(entry["payload"].encode()).hexdigest() != entry["receipt"]["checksum"]
        ):
            raise ValidationError("repair requires an intact partial or missing parent pin")
        plan = validate_plan(
            {
                **original_plan,
                "start_ms": opening_ms,
                "end_ms": opening_ms + 60_000,
                "limit": limit,
                "max_pages": max_pages,
            }
        )
        self.instrument = instrument
        self.directory = Path(root) / "state/auxiliary-repairs" / operation_id
        self.path = self.directory / "operation.json"
        self._check_paths()
        self.identity = {
            "schema_version": 1,
            "operation_id": operation_id,
            "parent_job_id": parent["job_id"],
            "plan": plan,
            "anchor": {
                "revision": entry["receipt"]["revision"],
                "checksum": entry["receipt"]["checksum"],
                "available_at_ms": observation.available_at_ms,
                "event_time_ms": observation.event_time_ms,
                "quality": observation.quality,
            },
        }
        if self.path.exists():
            self.data = self._read()
        else:
            self.data = {"identity": self.identity, "fetch_started": False, "result": None}
            self._save(self.data)

    def _check_paths(self):
        for path in (
            self.directory.parent.parent,
            self.directory.parent,
            self.directory,
            self.path,
            self.directory / "journal",
            self.directory / "operation.lock",
        ):
            if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
                raise StorageError("auxiliary repair cannot use linked paths")

    def _read(self):
        try:
            self._check_paths()
            with self.path.open("rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise ValueError
            envelope = json.loads(raw, object_pairs_hook=_unique)
            if type(envelope) is not dict or set(envelope) != {"data", "sha256"}:
                raise ValueError
            data = envelope["data"]
            if (
                type(data) is not dict
                or set(data) != {"identity", "fetch_started", "result"}
                or _encoded(data["identity"]) != _encoded(self.identity)
                or type(data["fetch_started"]) is not bool
                or data["result"] is not None
                and type(data["result"]) is not dict
                or data["result"] is not None
                and not data["fetch_started"]
                or envelope["sha256"] != sha256(_encoded(data)).hexdigest()
            ):
                raise ValueError
            return data
        except (OSError, ValueError, TypeError, KeyError, RecursionError, UnicodeError):
            raise StorageError("auxiliary repair manifest is missing or invalid") from None

    def _save(self, candidate):
        self._check_paths()
        envelope = {"data": candidate, "sha256": sha256(_encoded(candidate)).hexdigest()}
        if len(_encoded(envelope)) > MAX_BYTES:
            raise StorageError("auxiliary repair manifest exceeds its bound")
        try:
            atomic_json(self.path, envelope)
        except OSError:
            raise StorageError("auxiliary repair manifest commit failed") from None
        self.data = deepcopy(candidate)

    def snapshot(self):
        return deepcopy(self._read())

    def run(self, client, archive, store, *, clock_ms, stop_requested=lambda: False):
        if not callable(clock_ms) or not callable(stop_requested):
            raise ValidationError("repair requires explicit clock and stop callbacks")
        if getattr(store, "destination_id", None) != self.identity["plan"]["destination_id"]:
            raise ValidationError("repair store destination changed")
        self._check_paths()
        with closing(InstanceLock(self.directory / "operation.lock")):
            self.data = self._read()
            plan, anchor = self.identity["plan"], self.identity["anchor"]
            journal_path = (
                self.directory / "journal" / (sha256(_encoded(plan)).hexdigest() + ".json")
            )
            if self.data["fetch_started"] and not journal_path.exists():
                raise StorageError("attempted repair journal is missing")
            journal = AuxiliaryJobJournal(self.directory / "journal", plan)
            state = journal.snapshot()
            if self.data["result"] is not None:
                result = self.data["result"]
                if (
                    result.get("operation_id") != self.identity["operation_id"]
                    or result.get("parent_job_id") != self.identity["parent_job_id"]
                    or result.get("job_id") != journal.job_id
                    or result.get("repair") is not True
                    or type(result.get("complete")) is not bool
                    or type(result.get("committed")) is not bool
                    or result["complete"] != state["complete"]
                    or result["committed"] != (state["next_ms"] == plan["end_ms"])
                    or state["pending"] is not None
                    or result["complete"]
                    and (
                        result.get("reason") != "complete"
                        or len(state["deliveries"]) != 1
                        or decode_observation(state["deliveries"][0]["payload"].encode())[1].quality
                        != "observed"
                    )
                    or not result["complete"]
                    and (
                        result.get("reason") != "repair_incomplete"
                        or state["reason"] != "repair_incomplete"
                        or state["deliveries"]
                    )
                ):
                    raise StorageError("repair result does not match its durable journal")
                return deepcopy(result)
            if state["pending"] is None and not state["deliveries"]:
                now = clock_ms()
                if type(now) is not int or not 0 <= now <= 253_402_300_799_000:
                    raise ValidationError("invalid actual repair clock")
                current = store.get(
                    self.instrument, "trade_flow", plan["source"], plan["start_ms"], as_of_ms=now
                )
                if (
                    not isinstance(current, StoredAuxiliary)
                    or current.instrument != self.instrument
                    or current.observation.source != plan["source"]
                    or current.observation.channel != "trade_flow"
                    or current.observation.window_start_ms != plan["start_ms"]
                    or current.revision != anchor["revision"]
                    or current.checksum != anchor["checksum"]
                    or current.observation.available_at_ms != anchor["available_at_ms"]
                    or current.observation.event_time_ms != anchor["event_time_ms"]
                    or current.observation.quality != anchor["quality"]
                ):
                    raise ProtocolError("repair parent revision changed; no automatic rebase")
            first_fetch = not self.data["fetch_started"]
            if first_fetch:
                self._save({**self.data, "fetch_started": True})
            result = AuxiliaryCollection(
                plan,
                client if first_fetch else _ReplayOnlyClient(),
                archive,
                store,
                journal,
                clock_ms=clock_ms,
                stop_requested=stop_requested,
                expected_previous=anchor["checksum"],
                observed_only=True,
            ).run()
            result = {
                **result,
                "operation_id": self.identity["operation_id"],
                "parent_job_id": self.identity["parent_job_id"],
                "repair": True,
                "committed": result["target_committed"],
            }
            if result["complete"] or result["reason"] == "repair_incomplete":
                self._save({**self.data, "result": result})
            return result
