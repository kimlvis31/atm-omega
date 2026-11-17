"""One bounded trade-flow collection job with archive-before-use and durable pins."""

import json
import re
from dataclasses import replace

from omega.application.auxiliary_ingestion import AuxiliaryIngestion, FinalizationDeferred
from omega.domain.errors import OmegaError, ProtocolError, RateLimited, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.auxiliary import _unique_object, agg_trade
from omega.ports.raw_data import PublicRawResponse
from omega.runtime.auxiliary_journal import validate_plan

MINUTE = 60_000


def validate_collection_plan(plan):
    checked = validate_plan(plan)
    provider, market, symbol = checked["instrument"].split(":")
    return Instrument(provider, Market(market), symbol)


class AuxiliaryCollection:
    def __init__(
        self,
        plan,
        client,
        archive,
        store,
        journal,
        *,
        clock_ms,
        stop_requested=lambda: False,
        expected_previous=None,
        observed_only=False,
    ):
        self.instrument = validate_collection_plan(plan)
        if (
            journal.snapshot()["plan"] != plan
            or not callable(clock_ms)
            or not callable(stop_requested)
        ):
            raise ValidationError("auxiliary job plan or callbacks do not match")
        self.plan = dict(plan)
        if (
            type(observed_only) is not bool
            or expected_previous is not None
            and (
                type(expected_previous) is not str
                or re.fullmatch("[a-f0-9]{64}", expected_previous) is None
            )
            or observed_only
            and (expected_previous is None or plan["end_ms"] - plan["start_ms"] != MINUTE)
        ):
            raise ValidationError("observed-only repair requires one minute and a fixed checksum")
        self.expected_previous, self.observed_only = expected_previous, observed_only
        self.client, self.archive, self.store, self.journal = client, archive, store, journal
        self.clock_ms, self.stop_requested = clock_ms, stop_requested
        self._known, self._last_id = {}, None
        self._ingestion = None
        self._commit_attempted = False
        self._fetch_reason = "no_after_anchor"
        self._last_page_empty = False

    def _now(self):
        now = self.clock_ms()
        if type(now) is not int or not 0 <= now <= 253_402_300_799_000:
            raise ValidationError("invalid auxiliary collection clock")
        return now

    def _check_stop(self):
        if self.stop_requested():
            raise InterruptedError("auxiliary collection stopped")

    def _accept_raw(self, record):
        if (
            record.instrument != self.instrument
            or record.channel != "agg_trades"
            or dict(record.request_parameters).get("symbol") != self.instrument.symbol
        ):
            raise ProtocolError("archived response belongs to another public collection")
        try:
            rows = json.loads(record.body.decode("utf-8"), object_pairs_hook=_unique_object)
        except (UnicodeError, ValueError, RecursionError):
            raise ProtocolError("archived trade response is not valid JSON") from None
        if type(rows) is not list or len(rows) > self.plan["limit"]:
            raise ProtocolError("archived trade page exceeds its declared collection limit")
        self._last_page_empty = not rows
        for row in rows:
            self._check_stop()
            raw = agg_trade(
                self.instrument, row, available_at_ms=record.received_at_ms, source="rest"
            )
            self._last_id = (
                raw.aggregate_id if self._last_id is None else max(self._last_id, raw.aggregate_id)
            )
            now = self._now()
            if raw.trade_time_ms > now:
                raise FinalizationDeferred(raw.trade_time_ms)
            item = replace(raw, available_at_ms=now).as_flow_event()
            opening = self._ingestion.status()["delivered_next_ms"]
            if item.trade_time_ms < opening:
                self._ingestion.remember_before(item, generation=self._ingestion.generation)
            else:
                self._ingestion.accept(item, generation=self._ingestion.generation)
            previous = self._known.get(item.aggregate_id)
            if previous is None:
                self._known[item.aggregate_id] = item
            # The first actual trade beyond the target is sufficient right evidence.
            if item.trade_time_ms >= self.plan["end_ms"]:
                return

    def _has_after(self):
        return any(item.trade_time_ms >= self.plan["end_ms"] for item in self._known.values())

    def _fetch(self):
        if self._last_page_empty:
            self._fetch_reason = "source_page_empty"
            return
        while (
            not self._has_after()
            and len(self.journal.snapshot()["batches"]) < self.plan["max_pages"]
        ):
            self._check_stop()
            options = {"limit": self.plan["limit"]}
            if self._last_id is None:
                closed = self._now() // MINUTE * MINUTE
                options.update(
                    start_ms=max(0, self.plan["start_ms"] - MINUTE),
                    end_ms=min(closed, self.plan["end_ms"] + MINUTE),
                )
            else:
                options["from_id"] = self._last_id + 1
            batch = self.client.agg_trades(self.instrument, **options)
            # An already returned response is archived even if stop was requested during GET.
            receipt = self.archive.put(
                PublicRawResponse(
                    self.instrument,
                    "agg_trades",
                    batch.request_parameters,
                    batch.received_at_ms,
                    batch.body,
                    batch.headers,
                )
            )
            self.journal.record_batch(receipt)
            self._check_stop()
            if not batch.events:
                self._last_page_empty = True
                self._fetch_reason = "source_page_empty"
                break
            previous = self._last_id
            self._accept_raw(self.archive.get(receipt.archive_id).record)
            if self._last_id == previous:
                raise ProtocolError("public trade pagination made no progress")
        if self._has_after():
            self._fetch_reason = "after_anchor_observed"
        elif len(self.journal.snapshot()["batches"]) >= self.plan["max_pages"]:
            self._fetch_reason = "page_cap_reached"

    def _coverage(self, opening):
        before = [item for item in self._known.values() if item.trade_time_ms < opening]
        after = [item for item in self._known.values() if item.trade_time_ms >= opening + MINUTE]
        if not before or not after:
            return None
        return self._ingestion.coverage(
            max(before, key=lambda item: item.aggregate_id).aggregate_id,
            min(after, key=lambda item: item.aggregate_id).aggregate_id,
            generation=self._ingestion.generation,
        )

    def _summary(self, reason, *, error=None):
        state = self.journal.snapshot()
        qualities = {key: 0 for key in ("observed", "partial", "missing")}
        repairable = []
        for entry in state["deliveries"]:
            # Durable journal payload is derived typed JSON, not a provider response.
            quality = json.loads(entry["payload"])["observation"]["quality"]
            qualities[quality] += 1
            if quality in {"partial", "missing"}:
                observation = json.loads(entry["payload"])["observation"]
                repairable.append(
                    {
                        "open_time_ms": observation["window_start_ms"],
                        "quality": quality,
                        "revision": entry["receipt"]["revision"],
                        "checksum": entry["receipt"]["checksum"],
                    }
                )
        pending_receipt = None if state["pending"] is None else state["pending"]["receipt"]
        outcome = (
            "confirmed_pending_delivery"
            if pending_receipt is not None
            else "unconfirmed"
            if self._commit_attempted and state["pending"] is not None
            else "not_attempted"
            if state["pending"] is not None
            else "confirmed"
            if state["deliveries"]
            else "none"
        )
        return {
            "job_id": self.journal.job_id,
            "complete": state["complete"],
            "reason": reason,
            "target_committed": state["next_ms"] == self.plan["end_ms"],
            "next_ms": state["next_ms"],
            "pages": len(state["batches"]),
            "quality_minutes": qualities,
            "repairable_minutes": repairable,
            "pending": state["pending"] is not None,
            "commit_outcome": outcome,
            "normalization_channels": ["trade_flow"],
            "oi_lsr_normalization_enabled": False,
            **({"error": error} if error else {}),
        }

    def run(self):
        state = self.journal.snapshot()
        if state["next_ms"] == self.plan["end_ms"]:
            return self._summary(state["reason"])
        if self.plan["end_ms"] > self._now() // MINUTE * MINUTE:
            raise ValidationError("auxiliary collection target must contain only closed minutes")
        pending = self.journal.pending()
        if (
            self.observed_only
            and pending is not None
            and (
                pending.expected_previous != self.expected_previous
                or pending.observation.quality != "observed"
                or pending.observation.coverage != 1
            )
        ):
            raise ProtocolError("saved repair candidate does not match its immutable policy")
        generation = pending.original_generation + 1 if pending is not None else 0
        self._ingestion = AuxiliaryIngestion(
            self.instrument,
            state["next_ms"],
            self.store,
            self.journal.delivered,
            source=self.plan["source"],
            clock_ms=self.clock_ms,
            generation=generation,
            max_pending_events=20_000,
            max_minutes=16,
            max_anchors=20_002,
        )
        try:
            self._check_stop()
            if pending is not None:
                self._ingestion.restore_pending(pending, generation=generation)
                self._check_stop()
                self._commit_attempted = True
                self._ingestion.flush(generation=generation)
            for archive_id in self.journal.snapshot()["batches"]:
                self._check_stop()
                self._accept_raw(self.archive.get(archive_id).record)
            self._fetch()
            while self.journal.snapshot()["next_ms"] < self.plan["end_ms"]:
                self._check_stop()
                opening = self.journal.snapshot()["next_ms"]
                snapshot = self._ingestion.stage_next(
                    batch_id=f"{self.journal.job_id}:{opening}",
                    generation=generation,
                    coverage=self._coverage(opening),
                    expected_previous=self.expected_previous,
                )
                if self.observed_only and (
                    snapshot.observation.quality != "observed" or snapshot.observation.coverage != 1
                ):
                    # No durable mutation intent and no DB call for another incomplete candidate.
                    self.journal.finish("repair_incomplete", complete=False)
                    return self._summary("repair_incomplete")
                self.journal.stage(snapshot)  # Durable snapshot before any possible DB mutation.
                self._check_stop()
                self._commit_attempted = True
                self._ingestion.flush(generation=generation)
            result = self._summary("checking_quality")
            complete = (
                result["quality_minutes"]["partial"] == result["quality_minutes"]["missing"] == 0
            )
            reason = (
                "complete"
                if complete
                else "target_committed_incomplete_coverage:" + self._fetch_reason
            )
            self.journal.finish(reason, complete=complete)
            return self._summary(reason)
        except (OmegaError, InterruptedError, OSError) as exc:
            reason = (
                "stopped"
                if isinstance(exc, InterruptedError)
                else "normalization_deferred"
                if isinstance(exc, FinalizationDeferred)
                else "rate_limited"
                if isinstance(exc, RateLimited)
                else "collection_failed"
            )
            snapshot = self._ingestion.pending_snapshot()
            if (
                snapshot is not None
                and snapshot.observation.window_start_ms == self.journal.snapshot()["next_ms"]
                and (
                    not self.observed_only
                    or snapshot.observation.quality == "observed"
                    and snapshot.observation.coverage == 1
                )
            ):
                self.journal.stage(snapshot)
            self.journal.finish(reason, complete=False)
            return self._summary(reason, error=type(exc).__name__)
        finally:
            self._ingestion.close()
