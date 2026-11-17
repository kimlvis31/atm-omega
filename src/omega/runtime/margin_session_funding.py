"""Optional frozen scenario funding, independent of strategy and source IO."""

import re
from copy import deepcopy

from omega.application.margin_funding import (
    MODEL,
    POLICY_SHA256,
    FundingPoint,
    FundingSchedule,
    frozen_funding_document,
    funding_fact_from_document,
    prepare_funding_batch,
    restore_funding_batch,
)
from omega.domain.errors import ProtocolError, TradingBlocked
from omega.domain.margin import MAX_EVENTS

LIMITATION = (
    "OHLCV proxy marks; full fills; zero spread/slippage; assumed scenario funding only; "
    "no venue account payments/liquidation"
)
_PLAN_KEYS = {
    "schema_version",
    "model",
    "policy_sha256",
    "batch_id",
    "checksum",
    "settlement_time_ms",
    "as_of_ms",
    "baseline_sequence",
    "baseline_sha256",
    "points",
    "facts",
    "prefix_checksums",
}


def prepare_session_funding(funding, instruments, start_ms):
    if funding is None:
        return None
    schedule = funding if isinstance(funding, FundingSchedule) else FundingSchedule.parse(funding)
    schedule.validate_for(instruments, start_ms)
    return schedule


class MarginSessionFunding:
    def __init__(self, funding, initial, start_ms):
        self.schedule = prepare_session_funding(
            funding, tuple(position.instrument for position in initial.positions), start_ms
        )
        self.enabled = self.schedule is not None
        self.schedule_sha256 = None if self.schedule is None else self.schedule.sha256
        self.configuration = (
            funding.document() if isinstance(funding, FundingSchedule) else deepcopy(funding)
        )
        self.owners = {position.instrument: position.owner_id for position in initial.positions}

    def freeze(self, candles, *, as_of_ms, financial):
        if not self.enabled:
            raise ProtocolError("ordinary session has no funding phase")
        if financial.state != "ready" or financial.account is None:
            raise TradingBlocked("funding input needs a confirmed financial baseline")
        opening = candles[0].open_time_ms
        points = self.schedule.at(opening)
        plan = (
            None
            if not points
            else prepare_funding_batch(financial.account, points, as_of_ms=as_of_ms)
        )
        return {
            "schema_version": 1,
            "model": MODEL,
            "schedule_sha256": self.schedule_sha256,
            "settlement_time_ms": opening,
            "plan": None if plan is None else frozen_funding_document(plan),
        }

    def validate(self, document, candles, *, as_of_ms):
        """Pure syntax/source/fact validation, without claiming baseline authority."""
        if (
            not self.enabled
            or type(document) is not dict
            or set(document)
            != {"schema_version", "model", "schedule_sha256", "settlement_time_ms", "plan"}
            or type(document["schema_version"]) is not int
            or document["schema_version"] != 1
            or document["model"] != MODEL
            or document["schedule_sha256"] != self.schedule_sha256
            or type(document["settlement_time_ms"]) is not int
            or document["settlement_time_ms"] != candles[0].open_time_ms
        ):
            raise ProtocolError("frozen session funding source/configuration differs")
        points = self.schedule.at(candles[0].open_time_ms)
        plan = document["plan"]
        if not points:
            if plan is not None:
                raise ProtocolError("unscheduled minute cannot contain a funding plan")
            return
        if (
            type(plan) is not dict
            or set(plan) != _PLAN_KEYS
            or type(plan["schema_version"]) is not int
            or plan["schema_version"] != 1
            or plan["model"] != MODEL
            or plan["policy_sha256"] != POLICY_SHA256
            or type(plan["settlement_time_ms"]) is not int
            or plan["settlement_time_ms"] != candles[0].open_time_ms
            or type(plan["as_of_ms"]) is not int
            or plan["as_of_ms"] != as_of_ms
            or type(plan["baseline_sequence"]) is not int
            or not 0 <= plan["baseline_sequence"] <= MAX_EVENTS
            or type(plan["points"]) is not list
            or len(plan["points"]) != len(points)
            or tuple(FundingPoint.parse(value) for value in plan["points"]) != points
            or type(plan["facts"]) is not list
            or len(plan["facts"]) != len(points)
            or plan["baseline_sequence"] + len(points) > MAX_EVENTS
            or type(plan["prefix_checksums"]) is not list
            or len(plan["prefix_checksums"]) != len(points) + 1
        ):
            raise ProtocolError("frozen funding plan is incomplete or differs from its source")
        for value in (
            plan["batch_id"],
            plan["checksum"],
            plan["baseline_sha256"],
            *plan["prefix_checksums"],
        ):
            if type(value) is not str or re.fullmatch("[a-f0-9]{64}", value) is None:
                raise ProtocolError("invalid frozen funding plan checksum")
        if plan["baseline_sha256"] != plan["prefix_checksums"][0]:
            raise ProtocolError("frozen funding baseline prefix differs")
        for point, raw in zip(points, plan["facts"], strict=True):
            fact = funding_fact_from_document(raw)
            if (
                fact.instrument != point.instrument
                or fact.owner_id != self.owners[point.instrument]
                or fact.rate != point.rate
                or fact.reference_price != point.reference_price
                or fact.event_time_ms != point.event_time_ms
                or fact.source_available_at_ms != point.source_available_at_ms
                or fact.available_at_ms != as_of_ms
                or fact.source_sha256 != point.source_sha256
                or fact.policy_sha256 != POLICY_SHA256
            ):
                raise ProtocolError("frozen funding fact differs from its original scenario")

    def _committed(self, document, execution):
        records = execution.committed_funding_batches(document["settlement_time_ms"])
        if len(records) > 1:
            raise ProtocolError("multiple funding batches claim one session settlement")
        if records:
            if document["plan"] is None or frozen_funding_document(records[0]) != document["plan"]:
                raise ProtocolError("committed funding differs from its frozen session authority")
            return records[0]
        return None

    def validate_history(self, metadata, execution):
        """Check actual source membership only after financial owner reconstruction."""
        if not self.enabled:
            return
        for frame in (metadata["last_frame"], metadata["pending"]):
            if frame is None:
                continue
            document = frame["funding"]
            if self._committed(document, execution) is not None:
                continue
            if document["plan"] is None:
                continue
            if frame["phase"] != "funding":
                raise ProtocolError("completed funding phase has no actual source journal")
            financial = execution.snapshot()
            if financial.state != "ready" or financial.account is None:
                raise TradingBlocked("pending funding requires a verified original baseline")
            restore_funding_batch(document["plan"], financial.account)

    def settle(self, frame, execution):
        """Look up original source before restoring exact frozen facts, never reprice."""
        execution.reconcile()
        document = frame["funding"]
        if self._committed(document, execution) is not None:
            return
        if document["plan"] is not None:
            plan = execution.restore_prepared_funding(document["plan"])
            execution.commit_funding(plan)

    def status(self):
        return {
            "model": MODEL,
            "schedule_sha256": self.schedule_sha256,
            "points": len(self.schedule.points),
            "source_kind": "scenario_rate",
            "venue_account_payment": False,
        }
