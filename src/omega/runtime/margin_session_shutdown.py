"""Financial-only session drain; strategy/analysis checkpoints remain untouched.

The normal session cursor is deliberately not advanced. Resuming its strategies
requires explicit model preparation after any drain, including a canceled one.
"""

import json
import os
import re
import stat
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256

from omega.application.margin_execution import FrozenPendingOrder
from omega.application.margin_risk import approve_order
from omega.application.trading import validate_decision
from omega.application.virtual_margin import VirtualMarginSnapshot
from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.domain.margin import MarginBracketSnapshot, MarkSnapshot
from omega.domain.models import OrderKind, Side
from omega.domain.trading import (
    BudgetView,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
)
from omega.research.artifacts import _status_descriptor
from omega.runtime.settings import atomic_json
from omega.runtime.virtual_margin_journal import _decode, _encode

MAX_BYTES = 4_194_304
INSPECTION_ID = "shutdown-inspection"


def _json(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > MAX_BYTES:
        raise StorageError("margin shutdown document exceeds capacity")
    return raw


def _digest(value):
    return sha256(_json(value)).hexdigest()


def _identity(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value) is None:
        raise ValidationError("invalid margin shutdown operation identity")


class MarginSessionShutdown:
    def __init__(self, session):
        self.session = session
        self.path = (
            session.root
            / "state/margin-shutdown"
            / (sha256(session.account_id.encode()).hexdigest() + ".json")
        )
        self.uncertain, self.error = None, None
        self.data = self._read()
        self._funding_history()
        if self.data is None:
            financial = session.execution.snapshot()
            order_ids = {item.order_id for item in financial.pending}
            if financial.account is not None:
                order_ids.update(
                    entry.fact.order_id
                    for entry in financial.account.ledger
                    if hasattr(entry.fact, "order_id")
                )
            for order_id in order_ids:
                intent = session.execution.original_intent(order_id)
                if intent is None or not intent.original.proposal.reason.startswith("shutdown:"):
                    continue
                operation_id = intent.original.proposal.reason.removeprefix("shutdown:")
                expected_id = _digest(
                    (
                        session.account_id,
                        operation_id,
                        intent.original.as_of_ms
                        if session._costs.enabled
                        else intent.eligible_at_ms,
                        intent.original.instrument.key,
                        "reduce",
                    )
                )
                if order_id == expected_id:
                    raise StorageError(
                        "margin shutdown metadata is missing beside its financial intent"
                    )

    def _guard(self):
        self.session._owner()
        for path in (self.path, self.path.parent, *self.path.parent.parents):
            if path.is_symlink() or path.is_junction():
                raise StorageError("margin shutdown cannot use linked paths")

    def _read(self):
        self._guard()
        descriptor = None
        try:
            descriptor = _status_descriptor(self.path)
            details = os.fstat(descriptor)
            if not stat.S_ISREG(details.st_mode) or details.st_size > MAX_BYTES:
                raise StorageError("margin shutdown must be a bounded regular file")
            stream = os.fdopen(descriptor, "rb")
            descriptor = None
            with stream:
                raw = stream.read(MAX_BYTES + 1)
        except FileNotFoundError:
            return None
        finally:
            if descriptor is not None:
                os.close(descriptor)
        if len(raw) > MAX_BYTES:
            raise StorageError("margin shutdown document exceeds capacity")
        from omega.runtime.margin_session import _unique

        envelope = json.loads(raw, object_pairs_hook=_unique)
        if (
            type(envelope) is not dict
            or set(envelope) != {"data", "sha256"}
            or _digest(envelope["data"]) != envelope["sha256"]
        ):
            raise StorageError("margin shutdown checksum mismatch")
        self._validate(envelope["data"])
        return envelope["data"]

    def _validate(self, data):
        from omega.runtime.margin_session import _time

        if (
            type(data) is not dict
            or set(data)
            != {
                "schema_version",
                "config_hash",
                "operation_id",
                "issued_at_ms",
                "state",
                "cursor_ms",
                "orders",
                "cancellations",
                "marks",
                "brackets",
                "pending_frame",
                "last_frame",
            }
            or type(data["schema_version"]) is not int
            or data["schema_version"] != (2 if self.session._costs.enabled else 1)
            or data["config_hash"] != self.session.config_hash
        ):
            raise ProtocolError("margin shutdown schema/configuration mismatch")
        _identity(data["operation_id"])
        _time(data["issued_at_ms"])
        if data["state"] not in {"preparing", "draining", "canceled", "complete", "blocked"}:
            raise ProtocolError("invalid margin shutdown state")
        if data["cursor_ms"] is not None:
            _time(data["cursor_ms"])
            if data["cursor_ms"] % 60000:
                raise ProtocolError("invalid margin shutdown cursor")
        orders, tables = _decode(data["orders"]), _decode(data["brackets"])
        if (
            type(orders) is not tuple
            or len(orders) > 100
            or any(not isinstance(value, FrozenPendingOrder) for value in orders)
        ):
            raise ProtocolError("invalid frozen shutdown reductions")
        if len({value.order_id for value in orders}) != len(orders):
            raise ProtocolError("duplicate shutdown reduction identity")
        for value in orders:
            if self.session._costs.enabled:
                self.session._costs.validate_order(value, issued_at_ms=data["issued_at_ms"])
            if (
                value.original.account_id != self.session.account_id
                or not value.original.proposal.reducing
                or not self.session._costs.enabled
                and value.eligible_at_ms > data["issued_at_ms"]
            ):
                raise ProtocolError("shutdown reduction identity/eligibility changed")
        if (
            type(tables) is not tuple
            or len(tables) > 100
            or any(not isinstance(table, MarginBracketSnapshot) for table in tables)
        ):
            raise ProtocolError("invalid shutdown brackets")
        if (
            type(data["cancellations"]) is not list
            or len(data["cancellations"]) > 100
            or any(
                type(item) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", item) is None
                for item in data["cancellations"]
            )
        ):
            raise ProtocolError("invalid shutdown cancellations")
        if len(set(data["cancellations"])) != len(data["cancellations"]):
            raise ProtocolError("duplicate shutdown cancellation")
        self._marks(data["marks"])
        for key in ("pending_frame", "last_frame"):
            frame = data[key]
            if frame is not None:
                bars, _ = self._frame(frame)
                expected_opening = (
                    data["cursor_ms"]
                    if key == "last_frame"
                    else (
                        self.session.start_ms
                        if data["cursor_ms"] is None
                        else data["cursor_ms"] + 60000
                    )
                )
                if expected_opening is None:
                    raise ProtocolError("shutdown frame lacks a cursor authority")
                self.session._bars(bars, frame["as_of_ms"], expected_opening)
        if data["last_frame"] is None and data["cursor_ms"] != self.session._meta["cursor_ms"]:
            raise ProtocolError("shutdown cursor moved without its frozen frame")

    def _save(self, candidate):
        self._guard()
        self._validate(candidate)
        if candidate == self.data:
            return
        self.uncertain = deepcopy(self.data), deepcopy(candidate)
        try:
            atomic_json(self.path, {"data": candidate, "sha256": _digest(candidate)})
        except Exception:
            self.error = "shutdown_commit_unknown"
            raise StorageError("margin shutdown commit outcome is unknown") from None
        self.data, self.uncertain, self.error = deepcopy(candidate), None, None

    def reconcile(self):
        self._guard()
        loaded = self._read()
        if self.uncertain is None:
            if loaded != self.data:
                raise ProtocolError("margin shutdown changed outside its owner")
        else:
            previous, candidate = self.uncertain
            if loaded == candidate:
                self.data, self.uncertain, self.error = candidate, None, None
            elif loaded == previous:
                self._save(candidate)
            else:
                raise ProtocolError("margin shutdown commit conflicts with external facts")
        financial = self.session.execution.snapshot()
        if financial.state != "ready" or financial.pending_record_id is not None:
            raise TradingBlocked("financial reconciliation is not confirmed")
        self._funding_history()
        self.error = None

    def _funding_history(self):
        if not self.session._funding.enabled or self.data is None:
            return
        # Shutdown has its own cursor. Reuse the exact frozen funding authority,
        # without reading strategy/graph/NN state or repricing a settled minute.
        self.session._funding.validate_history(
            {
                "last_frame": (
                    None
                    if self.data["last_frame"] is None
                    else {**self.data["last_frame"], "phase": "open"}
                ),
                "pending": (
                    None
                    if self.data["pending_frame"] is None
                    else {**self.data["pending_frame"], "phase": "funding"}
                ),
            },
            self.session.execution,
        )

    def _financial(self):
        self._guard()
        if self.uncertain is not None or self.error:
            raise TradingBlocked("margin shutdown requires explicit reconciliation")
        financial = self.session.execution.snapshot()
        if (
            not isinstance(financial, VirtualMarginSnapshot)
            or financial.account is None
            or financial.state != "ready"
            or financial.pending_record_id is not None
        ):
            raise TradingBlocked("margin shutdown requires confirmed financial facts")
        if financial.account.account_id != self.session.account_id:
            raise ProtocolError("shutdown financial owner changed")
        self._funding_history()
        return financial

    def status(self):
        self._guard()
        data = self.data
        cursor = data["cursor_ms"] if data else self.session._meta["cursor_ms"]
        result = {
            "operation_id": None if data is None else data["operation_id"],
            "state": "idle" if data is None else data["state"],
            "active": bool(data and data["state"] in {"preparing", "draining"}),
            "needs_settlement": False,
            "next_open_ms": self.session.start_ms if cursor is None else cursor + 60000,
            "recovery_required": data is not None or self.uncertain is not None,
        }
        if data is None and self.uncertain is None and self.error is None:
            return result  # An ordinary financial fault is not a shutdown transaction.
        try:
            financial = self._financial()
            result["needs_settlement"] = bool(
                data and data["state"] in {"draining", "canceled"} and financial.pending
            )
        except (StorageError, ProtocolError, TradingBlocked):
            result.update(state="unknown", active=False, recovery_required=True)
        return result

    def _marks(self, values):
        from omega.runtime.margin_session import _instrument, _money

        if (
            type(values) is not list
            or len(values) > 100
            or any(type(row) is not list or len(row) != 5 for row in values)
        ):
            raise ProtocolError("invalid bounded shutdown marks")
        return tuple(
            MarkSnapshot(_instrument(row[0]), _money(row[1]), row[2], row[3], row[4])
            for row in values
        )

    def _plan(self, operation_id, issued_at_ms, brackets):
        from omega.runtime.margin_session import _candle, _time

        _identity(operation_id)
        _time(issued_at_ms)
        financial = self._financial()
        if self.session._uncertain is not None or self.session._meta["pending"] is not None:
            raise TradingBlocked("finish or reconcile the frozen strategy frame before shutdown")
        if any(
            entry.fact.available_at_ms > issued_at_ms for entry in financial.account.ledger
        ) or any(order.original.as_of_ms > issued_at_ms for order in financial.pending):
            raise ProtocolError("shutdown issue time precedes its financial facts")
        # A new explicit exit after cancel adopts already submitted reductions.
        # Their original eligibility and frozen financial request stay unchanged.
        if self.data is not None and self.data["state"] in {"draining", "canceled", "complete"}:
            active = {value.original.instrument: value for value in financial.pending}
            known = {value.order_id for value in _decode(self.data["orders"])}
            positions_by_instrument = {
                value.instrument: value for value in financial.account.positions if value.quantity
            }
            if (
                issued_at_ms >= self.data["issued_at_ms"]
                and set(active) == set(positions_by_instrument)
                and all(
                    order.order_id in known
                    and order.original.proposal.reducing
                    and order.original.proposal.quantity
                    == abs(positions_by_instrument[instrument].quantity)
                    for instrument, order in active.items()
                )
            ):
                candidate = deepcopy(self.data)
                candidate.update(
                    operation_id=operation_id,
                    issued_at_ms=issued_at_ms,
                    state="draining" if active else "complete",
                    cancellations=[],
                )
                return candidate
        last = self.session._meta["last_frame"]
        positions = tuple(position for position in financial.account.positions if position.quantity)
        marks = (
            ()
            if last is None
            else tuple(
                MarkSnapshot(
                    bar.instrument,
                    bar.close,
                    bar.close_time_ms,
                    last["as_of_ms"],
                    "session_closed_ohlcv_proxy",
                )
                for bar in (_candle(item) for item in last["candles"])
            )
        )
        if positions and (
            last is None
            or issued_at_ms < last["as_of_ms"]
            or any(issued_at_ms - mark.event_time_ms > 120000 for mark in marks)
        ):
            raise TradingBlocked("fresh closed-minute proxy marks are required for shutdown")
        by_instrument = {value.instrument: value for value in marks}
        reservations, orders = [], []
        for position in sorted(positions, key=lambda item: item.instrument.key):
            mark = by_instrument[position.instrument]
            rules = self.session._rules[position.owner_id]
            proposal = OrderProposal(
                Side.SELL if position.quantity > 0 else Side.BUY,
                abs(position.quantity),
                OrderKind.MARKET,
                reducing=True,
                reason="shutdown:" + operation_id,
            )
            context = TradeContext(
                position.instrument,
                mark.price,
                issued_at_ms,
                PositionView(
                    position.quantity, position.entry_price, len(financial.account.ledger)
                ),
                BudgetView(position.budget_margin, Decimal(0)),
                rules,
                entries_allowed=False,
            )
            validate_decision(context, TradeDecision((proposal,), "shutdown"))
            order_id = _digest(
                (
                    self.session.account_id,
                    operation_id,
                    issued_at_ms,
                    position.instrument.key,
                    "reduce",
                )
            )
            references = dict(
                instrument=position.instrument,
                owner_id=position.owner_id,
                as_of_ms=issued_at_ms,
                marks=marks,
                brackets=brackets,
                worst_execution_price=mark.price,
                fee_rate=self.session.fee_rate,
                reservations=tuple(reservations),
            )
            if self.session._costs.enabled:
                order = self.session._costs.prepare_order(
                    financial.account, order_id, proposal, **references
                )
            else:
                reservation = approve_order(financial.account, order_id, proposal, **references)
                order = FrozenPendingOrder(reservation, issued_at_ms)
            reservations.append(order.original)
            orders.append(order)
        return {
            "schema_version": 2 if self.session._costs.enabled else 1,
            "config_hash": self.session.config_hash,
            "operation_id": operation_id,
            "issued_at_ms": issued_at_ms,
            "state": "preparing",
            "cursor_ms": self.session._meta["cursor_ms"],
            "orders": _encode(tuple(orders)),
            "cancellations": [value.order_id for value in financial.pending],
            "marks": [
                [
                    mark.instrument.key,
                    str(mark.price),
                    mark.event_time_ms,
                    mark.available_at_ms,
                    mark.source,
                ]
                for mark in marks
            ],
            "brackets": _encode(brackets),
            "pending_frame": None,
            "last_frame": None,
        }

    def preflight(self, operation_id, issued_at_ms, *, brackets=()):
        from omega.runtime.margin_session import _time

        _identity(operation_id)
        _time(issued_at_ms)
        try:
            financial = self._financial()
            duplicate = self.data is not None and self.data["operation_id"] == operation_id
            if self.data is not None and operation_id == INSPECTION_ID:
                if self.data["state"] in {"blocked", "preparing"}:
                    raise TradingBlocked("shutdown preparation/recovery is incomplete")
            elif duplicate:
                if issued_at_ms != self.data["issued_at_ms"]:
                    raise ProtocolError("shutdown operation time conflicts with original request")
            else:
                self._plan(operation_id, issued_at_ms, brackets)
            return {
                "ready": True,
                "reason": None,
                "positions": sum(bool(p.quantity) for p in financial.account.positions),
                "pending": len(financial.pending),
                "duplicate": duplicate,
            }
        except (StorageError, ProtocolError, TradingBlocked, ValidationError) as error:
            return {
                "ready": False,
                "reason": str(error),
                "positions": 0,
                "pending": 0,
                "duplicate": False,
            }

    def drain(self, operation_id, issued_at_ms, *, brackets=()):
        check = self.preflight(operation_id, issued_at_ms, brackets=brackets)
        if not check["ready"]:
            raise TradingBlocked(check["reason"])
        if not check["duplicate"]:
            self._save(self._plan(operation_id, issued_at_ms, brackets))
        if self.data["state"] != "preparing":
            return self.status()
        data = self.data
        try:
            self.session.execution.pause_entries(_digest((operation_id, "pause")))
            for order_id in data["cancellations"]:
                self.session.execution.cancel_order(
                    order_id, _digest((operation_id, "cancel", order_id))
                )
            marks, tables = self._marks(data["marks"]), _decode(data["brackets"])
            for order in _decode(data["orders"]):
                self.session.execution.submit_intent(order, marks=marks, brackets=tables)
            final = deepcopy(data)
            final["state"] = "draining" if self.session.execution.snapshot().pending else "complete"
            self._save(final)
        except Exception:
            self.error = "shutdown_submission_unconfirmed"
            raise
        return self.status()

    def cancel(self, operation_id):
        _identity(operation_id)
        self._financial()
        if (
            self.data is not None
            and self.data["operation_id"] == operation_id
            and self.data["state"] != "complete"
        ):
            candidate = deepcopy(self.data)
            candidate["state"] = "canceled"
            self._save(candidate)
        return self.status()

    def _frame(self, frame):
        from omega.runtime.margin_session import _bracket, _candle, _time

        expected = {"candles", "as_of_ms", "brackets"}
        if self.session._funding.enabled:
            expected.add("funding")
        if self.session._costs.enabled:
            expected.add("cost_policy")
        if (
            type(frame) is not dict
            or set(frame) != expected
            or type(frame["candles"]) is not list
            or not 1 <= len(frame["candles"]) <= 100
            or type(frame["brackets"]) is not list
            or len(frame["brackets"]) > 100
        ):
            raise ProtocolError("invalid frozen shutdown frame")
        _time(frame["as_of_ms"])
        bars = tuple(_candle(value) for value in frame["candles"])
        if self.session._funding.enabled:
            self.session._funding.validate(frame["funding"], bars, as_of_ms=frame["as_of_ms"])
        if self.session._costs.enabled:
            self.session._costs.validate_frame(frame["cost_policy"])
        return bars, tuple(_bracket(value) for value in frame["brackets"])

    def run_once(self, candles, *, as_of_ms, brackets=()):
        from omega.runtime.margin_session import _bracket_document, _candle_document, _time

        _time(as_of_ms)
        financial = self._financial()
        if self.data is None or self.data["state"] not in {"draining", "canceled", "complete"}:
            raise TradingBlocked("no confirmed shutdown drain to settle")
        if self.data["state"] == "complete":
            return self.status()
        known_orders = {order.order_id for order in _decode(self.data["orders"])}
        if any(order.order_id not in known_orders for order in financial.pending):
            raise ProtocolError("shutdown pending book changed outside its owner")
        pending = self.data["pending_frame"]
        if pending is None:
            if (
                self.data["last_frame"] is not None
                and candles
                and all(bar.open_time_ms == self.data["cursor_ms"] for bar in candles)
            ):
                if [_candle_document(bar) for bar in candles] != self.data["last_frame"]["candles"]:
                    raise ProtocolError("consumed shutdown candle changed")
                return self.status()
            ordered = self.session._bars(candles, as_of_ms, self.status()["next_open_ms"])
            pending = {
                "candles": [_candle_document(bar) for bar in ordered],
                "as_of_ms": as_of_ms,
                "brackets": [_bracket_document(value) for value in brackets],
            }
            if self.session._funding.enabled:
                pending["funding"] = self.session._funding.freeze(
                    ordered, as_of_ms=as_of_ms, financial=financial
                )
            if self.session._costs.enabled:
                pending["cost_policy"] = self.session._costs.frame_identity()
            candidate = deepcopy(self.data)
            candidate["pending_frame"] = pending
            self._save(candidate)
        elif (
            as_of_ms < pending["as_of_ms"]
            or candles
            and [
                _candle_document(bar)
                for bar in sorted(candles, key=lambda item: item.instrument.key)
            ]
            != pending["candles"]
        ):
            raise ProtocolError("pending shutdown frame cannot be replaced")
        bars, tables = self._frame(pending)
        try:
            if self.session._funding.enabled:
                self.session._funding.settle(pending, self.session.execution)
            existing = self.session.execution.committed_batches(bars[0].open_time_ms, "open")
            if existing:
                if (
                    len(existing) != 1
                    or existing[0].candles != bars
                    or existing[0].as_of_ms != pending["as_of_ms"]
                    or existing[0].brackets != tables
                ):
                    raise ProtocolError("financial shutdown batch conflicts with frozen inputs")
            else:
                plan = self.session.execution.prepare_batch(
                    bars, as_of_ms=pending["as_of_ms"], phase="open", brackets=tables
                )
                if plan.facts or plan.rejections:
                    self.session.execution.commit_batch(plan)
                    if plan.rejections:
                        raise TradingBlocked(
                            "shutdown reduction was rejected; manual recovery required"
                        )
            candidate = deepcopy(self.data)
            candidate.update(cursor_ms=bars[0].open_time_ms, last_frame=pending, pending_frame=None)
            if not self.session.execution.snapshot().pending:
                candidate["state"] = "complete"
            self._save(candidate)
        except Exception:
            self.error = "shutdown_settlement_unconfirmed"
            raise
        return self.status()
