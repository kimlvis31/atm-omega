"""Bounded, resumable historical observation scans over existing market ports.

Only completed pages advance the durable cursor. Repeating a page after an
uncertain database commit is safe because CandleStore owns immutable key checks.
"""

# Imports
import json
import re
import threading
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from hashlib import sha256
from itertools import islice

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.ports.brokers import MarketData
from omega.ports.collection import CandleStore, MinuteObservation

# Constants
MINUTE = 60_000
MAX_HORIZON_MINUTES = 525_600
MAX_GAP_RANGES = 128
MAX_STATE_BYTES = 65_536


def _digest(value):
    return sha256(
        json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":")).encode()
    ).hexdigest()


def _stamp(value):
    return type(value) is int and 0 <= value < 2**63 and value % MINUTE == 0


@dataclass(frozen=True)
class BackfillPlan:
    instrument: Instrument
    start_ms: int
    end_ms: int
    destination_id: str
    chunk_minutes: int = 512

    def __post_init__(self):
        if (
            not isinstance(self.instrument, Instrument)
            or self.instrument.provider != "binance"
            or self.instrument.market not in {Market.SPOT, Market.USDM}
            or not _stamp(self.start_ms)
            or not _stamp(self.end_ms)
            or not self.start_ms < self.end_ms
            or (self.end_ms - self.start_ms) // MINUTE > MAX_HORIZON_MINUTES
            or type(self.chunk_minutes) is not int
            or not 1 <= self.chunk_minutes <= 1000
            or not isinstance(self.destination_id, str)
            or not re.fullmatch(r"[a-f0-9]{64}", self.destination_id)
        ):
            raise ValidationError("invalid bounded historical backfill plan")

    @property
    def specification(self):
        return {
            "instrument": self.instrument.key,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "destination_id": self.destination_id,
            "chunk_minutes": self.chunk_minutes,
        }

    @property
    def plan_id(self):
        return _digest(self.specification)

    @classmethod
    def from_specification(cls, value):
        try:
            if not isinstance(value, dict) or set(value) != {
                "instrument",
                "start_ms",
                "end_ms",
                "destination_id",
                "chunk_minutes",
            }:
                raise ValidationError("invalid historical plan fields")
            provider, market, symbol = value["instrument"].split(":")
            return cls(
                Instrument(provider, Market(market), symbol),
                value["start_ms"],
                value["end_ms"],
                value["destination_id"],
                value["chunk_minutes"],
            )
        except (TypeError, ValueError, AttributeError, KeyError) as exc:
            raise ValidationError("invalid historical plan specification") from exc


def _encode(candle):
    return {
        "open_time_ms": candle.open_time_ms,
        **{key: str(getattr(candle, key)) for key in ("open", "high", "low", "close", "volume")},
    }


def _decode(plan, row):
    if (
        not isinstance(row, dict)
        or set(row) != {"open_time_ms", "open", "high", "low", "close", "volume"}
        or not _stamp(row["open_time_ms"])
    ):
        raise ValidationError("invalid historical checkpoint anchor")
    if any(
        not isinstance(row[key], str) or len(row[key]) > 128
        for key in ("open", "high", "low", "close", "volume")
    ):
        raise ValidationError("invalid historical checkpoint amount")
    return Candle(
        plan.instrument,
        row["open_time_ms"],
        row["open_time_ms"] + MINUTE,
        *(decimal(row[key]) for key in ("open", "high", "low", "close", "volume")),
    )


class HistoryBackfill:
    def __init__(
        self,
        plan: BackfillPlan,
        history: MarketData,
        store: CandleStore,
        *,
        persist: Callable[[dict], None],
        clock_ms: Callable[[], int],
        state=None,
    ):
        self.plan, self.history, self.store = plan, history, store
        self.persist, self.clock_ms = persist, clock_ms
        self._lock, self._step_lock = threading.RLock(), threading.Lock()
        self._closed = threading.Event()
        self._generation = 0
        self._check_now()
        if state is None:
            self.data = {
                "schema_version": 1,
                "plan": plan.specification,
                "plan_id": plan.plan_id,
                "next_open_ms": plan.start_ms,
                "processed_minutes": 0,
                "observed_minutes": 0,
                "missing_minutes": 0,
                "missing_ranges": [],
                "omitted_missing_minutes": 0,
                "last_candle": None,
                "state": "scanning",
                "raw_payload": "unavailable_from_history_port",
            }
            self.data["sha256"] = _digest(self.data)
            self.persist(deepcopy(self.data))
        else:
            self.validate_state(plan, state)
            self.data = deepcopy(state)

    @staticmethod
    def validate_state(plan, state):
        try:
            keys = {
                "schema_version",
                "plan",
                "plan_id",
                "next_open_ms",
                "processed_minutes",
                "observed_minutes",
                "missing_minutes",
                "missing_ranges",
                "omitted_missing_minutes",
                "last_candle",
                "state",
                "raw_payload",
                "sha256",
            }
            if (
                not isinstance(state, dict)
                or set(state) != keys
                or type(state["schema_version"]) is not int
                or state["schema_version"] != 1
                or state["plan"] != plan.specification
                or state["plan_id"] != plan.plan_id
                or state["raw_payload"] != "unavailable_from_history_port"
                or len(json.dumps(state, allow_nan=False).encode()) > MAX_STATE_BYTES
                or state["sha256"]
                != _digest({key: value for key, value in state.items() if key != "sha256"})
                or not _stamp(state["next_open_ms"])
                or not plan.start_ms <= state["next_open_ms"] <= plan.end_ms
            ):
                raise ValidationError("historical checkpoint plan/checksum mismatch")
            for key in (
                "processed_minutes",
                "observed_minutes",
                "missing_minutes",
                "omitted_missing_minutes",
            ):
                if type(state[key]) is not int or state[key] < 0:
                    raise ValidationError("invalid historical checkpoint counters")
            if (
                state["processed_minutes"] != (state["next_open_ms"] - plan.start_ms) // MINUTE
                or state["observed_minutes"] + state["missing_minutes"]
                != state["processed_minutes"]
                or state["omitted_missing_minutes"] > state["missing_minutes"]
                or not isinstance(state["missing_ranges"], list)
                or len(state["missing_ranges"]) > MAX_GAP_RANGES
            ):
                raise ValidationError("historical checkpoint progress is inconsistent")
            gap_minutes, previous_end = 0, plan.start_ms
            for gap in state["missing_ranges"]:
                if (
                    not isinstance(gap, list)
                    or len(gap) != 2
                    or not all(_stamp(v) for v in gap)
                    or not previous_end <= gap[0] < gap[1] <= state["next_open_ms"]
                ):
                    raise ValidationError("invalid historical checkpoint missing range")
                gap_minutes += (gap[1] - gap[0]) // MINUTE
                previous_end = gap[1]
            if gap_minutes + state["omitted_missing_minutes"] != state["missing_minutes"]:
                raise ValidationError("historical checkpoint gap counts disagree")
            complete = state["next_open_ms"] == plan.end_ms
            expected = "complete_with_gaps" if state["missing_minutes"] else "complete"
            if state["state"] != (expected if complete else "scanning"):
                raise ValidationError("historical checkpoint completion is inconsistent")
            anchor = state["last_candle"]
            if anchor is not None:
                candle = _decode(plan, anchor)
                if not plan.start_ms <= candle.open_time_ms < state["next_open_ms"]:
                    raise ValidationError("historical anchor is outside committed progress")
            if bool(state["observed_minutes"]) != (anchor is not None):
                raise ValidationError("historical checkpoint lacks its observed anchor")
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as exc:
            raise ValidationError("malformed historical checkpoint") from exc

    def _check_now(self):
        now = self.clock_ms()
        if type(now) is not int or now < 0 or self.plan.end_ms > now // MINUTE * MINUTE:
            raise ValidationError("historical target must be closed under the current clock")
        return now

    def snapshot(self):
        with self._lock:
            return deepcopy(self.data)

    def close(self):
        self._closed.set()
        with self._lock:
            self._generation += 1

    def _current(self, generation, plan, history, store):
        if self._closed.is_set():
            raise InterruptedError("historical owner is closed")
        if (
            self._generation != generation
            or self.plan is not plan
            or self.history is not history
            or self.store is not store
        ):
            raise ValidationError("historical source/store binding changed")

    @staticmethod
    def _rows(plan, values, start, end, now):
        rows = tuple(islice(values, 1001))
        if len(rows) > 1000:
            raise ProtocolError("historical source exceeded the bounded page")
        unique = {}
        for candle in rows:
            if (
                not isinstance(candle, Candle)
                or candle.instrument != plan.instrument
                or not _stamp(candle.open_time_ms)
                or not _stamp(candle.close_time_ms)
                or not start <= candle.open_time_ms < end
                or candle.close_time_ms > now
            ):
                raise ProtocolError("historical source returned an invalid minute")
            if candle.open_time_ms in unique and unique[candle.open_time_ms] != candle:
                raise ProtocolError("historical source returned conflicting duplicate minutes")
            unique[candle.open_time_ms] = candle
        return tuple(unique[key] for key in sorted(unique))

    @staticmethod
    def _gap(state, start, end):
        if start == end:
            return
        minutes = (end - start) // MINUTE
        state["missing_minutes"] += minutes
        gaps = state["missing_ranges"]
        if gaps and gaps[-1][1] == start and not state["omitted_missing_minutes"]:
            gaps[-1][1] = end
        elif len(gaps) < MAX_GAP_RANGES:
            gaps.append([start, end])
        else:
            state["omitted_missing_minutes"] += minutes

    def step(self):
        """Scan at most one page. An admitted DB write may finish during close;
        a retired owner never publishes its result/cursor or starts another write.
        """
        if not self._step_lock.acquire(blocking=False):
            raise ValidationError("historical page is already being processed")
        try:
            with self._lock:
                generation, plan, history, store = (
                    self._generation,
                    self.plan,
                    self.history,
                    self.store,
                )
                self._current(generation, plan, history, store)
                previous = deepcopy(self.data)
            now = self._check_now()
            # [1]: Recheck the last committed source AND destination observation.
            if previous["last_candle"] is not None:
                anchor = _decode(plan, previous["last_candle"])
                found = self._rows(
                    plan,
                    history.closed_minutes(
                        plan.instrument, anchor.open_time_ms, anchor.close_time_ms, now_ms=now
                    ),
                    anchor.open_time_ms,
                    anchor.close_time_ms,
                    now,
                )
                self._current(generation, plan, history, store)
                if found != (anchor,) or store.get(plan.instrument, anchor.open_time_ms) != anchor:
                    raise ProtocolError("historical resume anchor changed or disappeared")
            self._current(generation, plan, history, store)
            if previous["next_open_ms"] == plan.end_ms:
                return self.snapshot()
            start = previous["next_open_ms"]
            end = min(plan.end_ms, start + plan.chunk_minutes * MINUTE)
            values = history.closed_minutes(plan.instrument, start, end, now_ms=now)
            self._current(generation, plan, history, store)
            rows = self._rows(plan, values, start, end, self._check_now())
            candidate, cursor = deepcopy(previous), start
            # [2]: Immutable per-candle commits. Do not invent missing/raw data.
            for candle in rows:
                self._current(generation, plan, history, store)
                accepted_at = self._check_now()
                if accepted_at < candle.close_time_ms:
                    raise ProtocolError("local clock moved before a historical close")
                store.put(MinuteObservation(candle, "rest", accepted_at))
                self._gap(candidate, cursor, candle.open_time_ms)
                cursor = candle.close_time_ms
                candidate["observed_minutes"] += 1
                candidate["last_candle"] = _encode(candle)
            self._gap(candidate, cursor, end)
            candidate["next_open_ms"] = end
            candidate["processed_minutes"] += (end - start) // MINUTE
            if end == plan.end_ms:
                candidate["state"] = (
                    "complete_with_gaps" if candidate["missing_minutes"] else "complete"
                )
            candidate["sha256"] = _digest(
                {key: value for key, value in candidate.items() if key != "sha256"}
            )
            self.validate_state(plan, candidate)
            # [3]: Cursor becomes visible only after the same checkpoint commit.
            with self._lock:
                self._current(generation, plan, history, store)
                self.persist(deepcopy(candidate))
                self._current(generation, plan, history, store)
                self.data = candidate
            return self.snapshot()
        finally:
            self._step_lock.release()
