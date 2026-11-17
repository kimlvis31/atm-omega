"""Opt-in committed-minute supervisor; paper owns atomic cursors and user controls."""

import math
import threading
import time
from copy import deepcopy

from omega.analysis.warmup import CHUNK_MINUTES
from omega.application.paper import candle_row
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Candle

MINUTE = 60_000


class PaperFeedSupervisor:
    def __init__(
        self,
        paper,
        source,
        *,
        clock_ms=None,
        poll_seconds=1,
        batch_frames=16,
        stale_ms=120_000,
        close_timeout_s=12,
        on_incident=None,
        on_execution_incident=None,
    ):
        if (
            not 0.05 <= poll_seconds <= 60
            or not 1 <= batch_frames <= 32
            or not MINUTE <= stale_ms <= 600_000
            or type(close_timeout_s) not in {int, float}
            or not math.isfinite(close_timeout_s)
            or not 0.001 <= close_timeout_s <= 60
        ):
            raise ValidationError("invalid bounded paper feed scheduling settings")
        self.paper, self.source = paper, source
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.poll_seconds, self.batch_frames, self.stale_ms = poll_seconds, batch_frames, stale_ms
        self.close_timeout_s = close_timeout_s
        self.on_incident = on_incident
        self.on_execution_incident = on_execution_incident
        self._execution_alerts = set()
        self._seen_recording_failure, self._storage_alerted = 0, False
        self._storage_alert_pending = False
        self.stop = threading.Event()
        self.lock, self.tick_lock = threading.Lock(), threading.Lock()
        self.thread = None
        self._source_closed = False
        self._source_close_error = None
        self._status = {
            "state": "disabled",
            "reason": "disabled",
            "frames": 0,
            "last_open_ms": None,
        }

    def status(self):
        with self.lock:
            return {
                **deepcopy(self._status),
                "supervisor_alive": bool(self.thread and self.thread.is_alive()),
            }

    def _state(self, state, reason, **fields):
        with self.lock:
            self._status.update(state=state, reason=reason, **fields)

    def _blocked(self, reason):
        self.paper.data_gate(False, reason)
        self._state("blocked", reason)

    def run_once(self):
        with self.tick_lock:
            if self.stop.is_set():
                return
            try:
                self._tick()
            except Exception as exc:
                if self.stop.is_set():
                    self._state("stopped", "owner_retired")
                    return
                # Exception text can contain SQL/configuration details; report only its class.
                try:
                    self._blocked(type(exc).__name__)
                except Exception:
                    self._state("blocked", "recording_unavailable")
            finally:
                self._record_storage_health()
                self._record_execution_health()

    def _record_execution_health(self):
        # The paper snapshot releases its lock before invoking core callbacks.
        # This preserves owner -> paper lock ordering for HTTP control commands.
        provider = getattr(self.paper, "execution_health", None)
        if self.stop.is_set() or not callable(provider):
            return
        try:
            health = provider()
            if health.get("owner_retired"):
                return
            active, failed = set(), False
            for incident in health.get("incidents", [])[:100]:
                key = tuple(
                    incident.get(field)
                    for field in ("account_id", "instance", "instrument", "code", "severity")
                )
                active.add(key)
                if key in self._execution_alerts:
                    continue
                try:
                    if self.on_execution_incident is not None:
                        self.on_execution_incident(deepcopy(incident))
                    elif self.on_incident is not None:
                        self.on_incident(incident["code"], incident["severity"])
                    else:
                        raise RuntimeError("execution incident delivery is unavailable")
                except Exception:
                    active.discard(key)  # Retry delivery, without marking it acknowledged.
                    failed = True
            self._execution_alerts = active
            with self.lock:
                self._status["execution_alert_unavailable"] = failed
        except Exception:
            with self.lock:
                self._status["execution_alert_unavailable"] = True

    def _record_storage_health(self):
        # Called after every paper operation has released paper.lock. The callback
        # may take the control/owner lock; never invert the normal lock order.
        health = self.paper.recording_health()
        if self.stop.is_set() or health["owner_retired"]:
            return
        if health["failure_revision"] > self._seen_recording_failure:
            self._seen_recording_failure = health["failure_revision"]
            if not self._storage_alerted or not health["incident_announced"]:
                self._storage_alert_pending = True
        if self._storage_alert_pending:
            try:
                if self.on_incident is None:
                    raise RuntimeError("incident callback unavailable")
                self.on_incident("PAPER_RECORDING_UNAVAILABLE", "critical")
                self.paper.mark_recording_incident_announced()
            except Exception:
                with self.lock:
                    self._status["critical_alert_unavailable"] = True
            else:
                self._storage_alert_pending, self._storage_alerted = False, True
                with self.lock:
                    self._status["critical_alert_unavailable"] = False
        elif health["available"] and self.status()["state"] == "ready":
            self._storage_alerted = False

    def _tick(self):
        plan = self.paper.feed_snapshot()
        if not plan["enabled"]:
            self._state("disabled", "disabled")
            return
        if not plan["instruments"] and not plan["warmup"]:
            self._blocked("no_subscriptions")
            return
        if plan["recovery_required"]:
            self._blocked("manual_recovery_required")
            return
        now = self.clock_ms()
        if plan["warmup"]:
            try:
                self._warmup(plan, now)
            except Exception as exc:
                self._blocked(type(exc).__name__)
            if self.stop.is_set():
                return
            plan = self.paper.feed_snapshot()
            if (
                not plan["warmup"]
                or plan["long_warmup_preparing"]
                or not plan.get("protection_required")
            ):
                return
        if not plan["instruments"]:
            return
        starts = {item["next_open_ms"] for item in plan["instruments"].values()}
        if len(starts) != 1:
            self._blocked("portfolio_cursor_mismatch")
            return
        start = starts.pop()
        if plan["frontier_ms"] is not None and start < plan["frontier_ms"]:
            self._blocked("portfolio_frontier_conflict")
            return
        closed_end = now // MINUTE * MINUTE
        if start >= closed_end:
            # No new closed minute is normal, but a clock reversal must never reopen entry.
            if start > closed_end:
                self._blocked("clock_before_durable_cursor")
                return
            self._state("waiting", "waiting_for_closed_minute")
            return
        end = min(closed_end, start + self.batch_frames * MINUTE)
        keys = sorted(plan["instruments"])
        overlap = (
            start - MINUTE
            if start >= MINUTE and any(item["last_candle"] for item in plan["instruments"].values())
            else start
        )
        rows = self._observed(self.source.read(keys, overlap, end), now)
        if self.stop.is_set():
            return
        if any(
            row.instrument.key not in keys or not overlap <= row.open_time_ms < end for row in rows
        ):
            raise ProtocolError("feed source returned an out-of-plan observation")
        indexed = {(row.open_time_ms, row.instrument.key): row for row in rows}
        for key, item in plan["instruments"].items():
            last = item["last_candle"]
            if last is not None:
                observed = indexed.get((last["open_time_ms"], key))
                if observed is None or candle_row(observed) != last:
                    self._blocked("committed_cursor_conflict_or_missing")
                    return
        for stamp in range(start, end, MINUTE):
            if self.stop.is_set():
                return
            frame = [indexed.get((stamp, key)) for key in keys]
            if any(row is None for row in frame):
                self._blocked("missing_minute")
                return
            delivered_at = self.clock_ms()
            if delivered_at < now:
                self._blocked("clock_reversed_during_read")
                return
            selection = self._selected(
                plan, frame, delivered_at, names=set(plan.get("live_strategies", ()))
            )
            if self.stop.is_set():
                return
            decision_at = self.clock_ms()
            if decision_at < delivered_at:
                self._blocked("clock_reversed_during_input_read")
                return
            fresh = (
                stamp + MINUTE == decision_at // MINUTE * MINUTE
                and decision_at - (stamp + MINUTE) <= self.stale_ms
            )
            allow_entries = fresh and not plan["quarantined"] and not plan["warmup"]
            reason = (
                "warmup"
                if plan["warmup"]
                else "quarantined_strategy"
                if plan["quarantined"]
                else "ready"
                if fresh
                else "catching_up"
            )
            applied = self.paper.feed_apply(
                tuple(frame),
                expected=plan,
                entries_allowed=allow_entries,
                reason=reason,
                now_ms=decision_at,
                **selection,
            )
            allow_entries, reason = (
                applied.get("entries_ready", False),
                applied.get("reason", reason),
            )
            plan = self.paper.feed_snapshot()
            current = self.status()
            inputs_ready = applied.get("analysis_inputs_ready", True)
            self._state(
                "ready"
                if allow_entries and inputs_ready
                else "degraded"
                if allow_entries
                else "blocked"
                if fresh
                else "catching_up",
                reason if inputs_ready else "analysis_inputs_not_ready",
                frames=current["frames"] + 1,
                last_open_ms=stamp,
                analysis_inputs_ready=inputs_ready,
            )

    def _warmup(self, plan, now):
        self.paper.data_gate(False, "warmup")
        plan = self.paper.feed_snapshot()
        empty = {
            name: ()
            for name, item in plan["warmup"].items()
            if item["minutes"] == 0 and not item.get("staged")
        }
        if empty:
            self.paper.feed_warmup(empty, expected=plan, now_ms=now)
            self._state("warming", "warmup")
            return
        # One strategy per tick keeps warmup memory/DB work bounded to 2000 rows.
        name, item = min(
            plan["warmup"].items(),
            key=lambda pair: (
                pair[1].get("processed_minutes", 0)
                / max(1, pair[1].get("total_minutes", pair[1]["minutes"])),
                pair[0],
            ),
        )
        if item.get("staged"):
            if item["state"] == "not_ready":
                self._blocked("warmup_features_unavailable")
                return
            if item["target_end_ms"] > now // MINUTE * MINUTE:
                self._blocked("warmup_waiting_for_closed_minute")
                return
            low = item["next_open_ms"]
            high = min(item["target_end_ms"], low + CHUNK_MINUTES * MINUTE)
            overlap = low - MINUTE if item["last_candle"] is not None else low
            received = self.source.read([item["instrument"]], overlap, high)
            if self.stop.is_set():
                return
            delivered_at = self.clock_ms()
            if delivered_at < now:
                self._blocked("clock_reversed_during_warmup_read")
                return
            rows = self._observed(received, delivered_at)
            if any(
                row.instrument.key != item["instrument"] or not overlap <= row.open_time_ms < high
                for row in rows
            ):
                raise ProtocolError("warmup source returned an out-of-plan observation")
            if item["last_candle"] is not None:
                if not rows or candle_row(rows[0]) != item["last_candle"]:
                    self._blocked("warmup_cursor_conflict_or_missing")
                    return
                rows = rows[1:]
            selection = self._selected(plan, rows, delivered_at, names={name}, retrospective=True)
            if self.stop.is_set():
                return
            result = self.paper.feed_warmup_chunk(
                name, rows, expected=plan, now_ms=delivered_at, **selection
            )
            self._state(
                "warming",
                "warmup_features_unavailable"
                if result["plan"]["state"] == "not_ready"
                else "warmup",
            )
            return
        if item["end_ms"] > now // MINUTE * MINUTE:
            self._blocked("warmup_waiting_for_closed_minute")
            return
        rows = (
            ()
            if not item["minutes"]
            else self.source.read(
                [item["instrument"]],
                max(0, item["end_ms"] - item["minutes"] * MINUTE),
                item["end_ms"],
            )
        )
        if self.stop.is_set():
            return
        rows = self._observed(rows, now)
        delivered_at = self.clock_ms()
        if delivered_at < now:
            self._blocked("clock_reversed_during_warmup_read")
            return
        selection = self._selected(plan, rows, delivered_at, names={name}, retrospective=True)
        if self.stop.is_set():
            return
        self.paper.feed_warmup({name: rows}, expected=plan, now_ms=delivered_at, **selection)
        self._state("warming", "warmup")
        return

    def _selected(self, plan, rows, as_of_ms, **kwargs):
        prepare = getattr(self.source, "prepare_inputs", None)
        if not callable(prepare) or not rows:
            return {}
        return {"analysis_inputs": prepare(plan, tuple(rows), as_of_ms=as_of_ms, **kwargs)}

    @staticmethod
    def _observed(rows, now):
        rows = tuple(rows)
        if len(rows) > 4096:
            raise ProtocolError("feed source returned an oversized batch")
        seen = {}
        for candle in rows:
            if not isinstance(candle, Candle) or candle.close_time_ms > now:
                raise ProtocolError("feed source returned an unclosed minute")
            key = candle.instrument.key, candle.open_time_ms
            if key in seen:
                if seen[key] != candle:
                    raise ProtocolError("feed source returned conflicting duplicate minutes")
                continue
            seen[key] = candle
        return tuple(sorted(seen.values(), key=lambda c: (c.open_time_ms, c.instrument.key)))

    def start(self):
        if self.thread is not None or self.stop.is_set():
            raise ValidationError("paper feed supervisor already started or closed")
        self.thread = threading.Thread(target=self._loop, name="omega-paper-feed", daemon=True)
        self.thread.start()

    def _loop(self):
        try:
            while not self.stop.is_set():
                self.run_once()
                delay = (
                    self.poll_seconds
                    if self.paper.feed_snapshot()["enabled"]
                    else max(5, self.poll_seconds)
                )
                self.stop.wait(delay)
        finally:
            # Manifest/DB handles are created and released by this worker.
            self._close_source()

    def _close_source(self):
        if not self._source_closed:
            try:
                self.source.close()
            except Exception:
                self._source_close_error = "paper feed source cleanup failed"
                self._state("error", "source_close_failed")
            finally:
                self._source_closed = True

    def close(self):
        # A timeout may leave a read-only source thread alive. The paper lock
        # fence, not a best-effort stop check, revokes all late write authority.
        self.paper.retire_feed_owner()
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=self.close_timeout_s)
            if self.thread.is_alive():
                raise RuntimeError("paper feed query is still stopping")
        self._close_source()
        if self._source_close_error:
            raise RuntimeError(self._source_close_error)
