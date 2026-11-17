"""Durable virtual intent/frozen-batch outbox over a single factual margin owner.

No broker, source clock, filesystem or new-frame evaluation runs in reconciliation.
The injected journal merges primary/fallback typed records; codec/storage and core
generation fencing remain its caller's responsibility. Restart always pauses entries.
"""

import json
import re
from dataclasses import dataclass, fields, is_dataclass
from decimal import Context, Decimal, localcontext
from enum import Enum
from hashlib import sha256
from threading import RLock
from typing import Protocol

from omega.application.margin_account import MarginAccountOwner
from omega.application.margin_cost import (
    CostMarginReservation,
    CostPendingFillAdmission,
    FrozenCostPendingOrder,
    PreparedCostMarginBatch,
)
from omega.application.margin_execution import (
    FEE_QUANTUM,
    MODEL,
    FrozenPendingOrder,
    PreparedMarginBatch,
    account_prefix_checksum,
    advance_prefix_checksum,
    prepare_pending_batch,
)
from omega.application.margin_funding import (
    PreparedFundingBatch,
    prepare_funding_batch,
    restore_funding_batch,
    validate_funding_batch,
)
from omega.application.margin_risk import MarginReservation, _account_fingerprint, approve_order
from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.domain.margin import MAX_TIME, FillEvent, FundingCashflow, MarginAccount, apply_fill
from omega.domain.models import Side
from omega.ports.journal import Durability

MAX_RECORDS = 20_000
_DECIMAL = Context(prec=256)
_RANK = {Durability.VOLATILE: 0, Durability.FALLBACK: 1, Durability.PRIMARY: 2}


def _canonical(value):
    if isinstance(value, Decimal):
        with localcontext(_DECIMAL):
            return "0" if value == 0 else str(value.normalize())
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {field.name: _canonical(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, (tuple, list)):
        return [_canonical(item) for item in value]
    return value


def _digest(value):
    return sha256(
        json.dumps(
            _canonical(value), sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def _id(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValidationError("invalid virtual margin operation identity")


@dataclass(frozen=True, slots=True)
class AccountAnchor:
    account: MarginAccount


@dataclass(frozen=True, slots=True)
class CancelIntent:
    order_id: str
    original_request_fingerprint: str


@dataclass(frozen=True, slots=True)
class EntryControl:
    paused: bool
    request_id: str


@dataclass(frozen=True, slots=True)
class VirtualMarginRecord:
    account_id: str
    sequence: int
    previous_checksum: str | None
    record_id: str
    payload: (
        AccountAnchor
        | FrozenPendingOrder
        | FrozenCostPendingOrder
        | PreparedMarginBatch
        | PreparedCostMarginBatch
        | PreparedFundingBatch
        | CancelIntent
        | EntryControl
    )

    def __post_init__(self):
        _id(self.account_id)
        if (
            type(self.sequence) is not int
            or not 1 <= self.sequence <= MAX_RECORDS
            or type(self.record_id) is not str
            or re.fullmatch("[a-f0-9]{64}", self.record_id) is None
            or (self.sequence == 1) != (self.previous_checksum is None)
            or self.previous_checksum is not None
            and (
                type(self.previous_checksum) is not str
                or re.fullmatch("[a-f0-9]{64}", self.previous_checksum) is None
            )
            or type(self.payload)
            not in {
                AccountAnchor,
                FrozenPendingOrder,
                FrozenCostPendingOrder,
                PreparedMarginBatch,
                PreparedCostMarginBatch,
                PreparedFundingBatch,
                CancelIntent,
                EntryControl,
            }
        ):
            raise ValidationError("invalid typed virtual margin journal record")

    @property
    def checksum(self):
        # Avoid duplicating every account prefix when hashing a frozen batch.
        payload = (
            ("batch", self.payload.checksum)
            if isinstance(self.payload, PreparedMarginBatch)
            else ("funding", self.payload.checksum)
            if isinstance(self.payload, PreparedFundingBatch)
            else self.payload
        )
        return _digest(
            (1, self.account_id, self.sequence, self.previous_checksum, self.record_id, payload)
        )


@dataclass(frozen=True, slots=True)
class JournalReceipt:
    record: VirtualMarginRecord
    durability: Durability


class VirtualMarginJournal(Protocol):
    def read(self, account_id: str) -> tuple[JournalReceipt, ...] | None:
        """Return verified source records, or None only for an absent namespace.

        Include primary and fallback records; never convert corruption into None.
        The application verifies exact dedup/sequence/hash edges before mutation.
        """
        ...

    def append(self, record: VirtualMarginRecord) -> Durability:
        """Return durable status after commit. Same ID/exact payload is idempotent."""
        ...


@dataclass(frozen=True, slots=True)
class VirtualMarginSnapshot:
    account: MarginAccount | None
    pending: tuple[FrozenPendingOrder, ...]
    paused: bool
    state: str
    error: str | None
    pending_record_id: str | None


def _allowed(receipt):
    if receipt.durability == Durability.PRIMARY:
        return True
    if receipt.durability != Durability.FALLBACK:
        return False
    payload = receipt.record.payload
    if isinstance(payload, FrozenPendingOrder):
        return payload.original.proposal.reducing
    if isinstance(payload, PreparedMarginBatch):
        by_id = {value.order_id: value for value in payload.original_pending}
        return all(by_id[fact.order_id].original.proposal.reducing for fact in payload.facts)
    if isinstance(payload, PreparedFundingBatch):
        # Both signs are confirmed source facts. Recovery validates exact
        # prefixes before applying; the fallback durability still blocks entry.
        return True
    return isinstance(payload, (CancelIntent, EntryControl)) and (
        not isinstance(payload, EntryControl) or payload.paused
    )


def _validate_batch(plan, expected, active):
    if type(plan) is PreparedCostMarginBatch:
        if (
            not plan.prefix_accounts
            or plan.prefix_accounts[0] != expected
            or plan.original_pending
            != tuple(
                sorted(
                    active.values(),
                    key=lambda value: (value.original.instrument.key, value.order_id),
                )
            )
        ):
            raise ProtocolError("frozen cost batch conflicts with its original account/book")
        # Verify deterministic frozen inputs, never a current clock/source/policy.
        verified = prepare_pending_batch(
            expected,
            plan.original_pending,
            plan.candles,
            as_of_ms=plan.as_of_ms,
            phase=plan.phase,
            brackets=plan.brackets,
        )
        if type(verified) is not PreparedCostMarginBatch or verified != plan:
            raise ProtocolError("frozen cost quote/risk/fact/prefix changed")
        return plan.candidate_account
    if (
        type(plan) is not PreparedMarginBatch
        or any(type(value) is not FrozenPendingOrder for value in plan.original_pending)
        or any(type(value.original) is not MarginReservation for value in plan.original_pending)
        or plan.model != MODEL
        or type(plan.phase) is not str
        or plan.phase not in {"open", "close"}
        or plan.fee_quantum != FEE_QUANTUM
        or not plan.candles
        or len(plan.facts) > 100
        or len(plan.prefix_accounts) != len(plan.facts) + 1
        or len(plan.prefix_checksums) != len(plan.prefix_accounts)
        or plan.prefix_accounts[0] != expected
        or plan.original_pending
        != tuple(
            sorted(
                active.values(), key=lambda value: (value.original.instrument.key, value.order_id)
            )
        )
        or plan.prefix_checksums[0] != account_prefix_checksum(expected)
    ):
        raise ProtocolError("frozen virtual batch conflicts with its original account/book")
    expected_id = _digest(
        (
            MODEL,
            expected.account_id,
            plan.candles[0].open_time_ms,
            plan.phase,
            tuple(value.order_id for value in plan.original_pending),
        )
    )
    if plan.batch_id != expected_id:
        raise ProtocolError("frozen virtual batch phase/identity changed")
    checksum = _digest(
        (
            MODEL,
            FEE_QUANTUM,
            "ROUND_CEILING",
            plan.batch_id,
            plan.as_of_ms,
            plan.candles,
            plan.brackets,
            plan.original_pending,
            plan.admissions,
            plan.facts,
            plan.prefix_checksums,
            plan.remaining,
            plan.rejections,
        )
    )
    if checksum != plan.checksum:
        raise ProtocolError("frozen virtual batch checksum changed")
    for index, fact in enumerate(plan.facts):
        if not isinstance(fact, FillEvent) or fact.order_id not in active:
            raise ProtocolError("frozen fill has no original intent")
        original = active[fact.order_id].original
        signed = (
            original.proposal.quantity
            if original.proposal.side == Side.BUY
            else original.proposal.quantity.copy_negate()
        )
        if (
            fact.instrument != original.instrument
            or fact.owner_id != original.owner_id
            or fact.delta_qty != signed
            or fact.available_at_ms != plan.as_of_ms
            or fact.fill_id
            != _digest((MODEL, expected.account_id, fact.order_id, original.request_fingerprint))
        ):
            raise ProtocolError("frozen full-fill fact conflicts with its original intent")
        candidate = apply_fill(plan.prefix_accounts[index], fact).account
        if (
            candidate != plan.prefix_accounts[index + 1]
            or len(candidate.ledger) != len(plan.prefix_accounts[index].ledger) + 1
            or advance_prefix_checksum(plan.prefix_checksums[index], candidate)
            != plan.prefix_checksums[index + 1]
        ):
            raise ProtocolError("frozen virtual candidate prefix changed")
    removed = {fact.order_id for fact in plan.facts} | {value[0] for value in plan.rejections}
    if not removed <= active.keys() or plan.remaining != tuple(
        value for value in plan.original_pending if value.order_id not in removed
    ):
        raise ProtocolError("frozen virtual pending settlement changed")
    return plan.candidate_account


class VirtualMarginEngine:
    def __init__(
        self, owner: MarginAccountOwner, journal: VirtualMarginJournal, *, create_if_missing=False
    ):
        if (
            not isinstance(owner, MarginAccountOwner)
            or type(create_if_missing) is not bool
            or not callable(getattr(journal, "read", None))
            or not callable(getattr(journal, "append", None))
        ):
            raise ValidationError("virtual margin engine needs an owner and typed journal")
        self._owner, self._journal, self._lock = owner, journal, RLock()
        self._history, self._intents, self._active = (), {}, {}
        self._pending_record, self._prepared = None, None
        self._paused, self._state, self._error = True, "unavailable", None
        with self._lock:
            try:
                account = owner.snapshot()
                if account is None:
                    raise StorageError("virtual account is not confirmed")
                loaded = self._read(account.account_id)
                if loaded is None:
                    if not create_if_missing or account.ledger:
                        raise StorageError("virtual intent history is missing")
                    self._append(AccountAnchor(account), "anchor")
                else:
                    self._history = loaded
                    self._recover()
            except Exception as error:
                self._fail(error)

    def _fail(self, error):
        self._state = (
            "unknown"
            if self._pending_record is not None or self._owner.pending_snapshot() is not None
            else "unavailable"
        )
        if isinstance(error, ProtocolError):
            self._state = "conflict"
        self._error = type(error).__name__

    def _read(self, account_id):
        values = self._journal.read(account_id)
        if values is None:
            return None
        if type(values) is not tuple or not 1 <= len(values) <= MAX_RECORDS * 2:
            raise StorageError("virtual journal is empty or oversized")
        selected = {}
        for receipt in values:
            if (
                not isinstance(receipt, JournalReceipt)
                or not isinstance(receipt.record, VirtualMarginRecord)
                or not isinstance(receipt.durability, Durability)
                or receipt.record.account_id != account_id
            ):
                raise StorageError("virtual journal returned invalid typed records")
            record = receipt.record
            existing = selected.get(record.record_id)
            if existing is not None:
                if existing.record != record or existing.record.checksum != record.checksum:
                    raise ProtocolError("virtual record ID conflicts with its original payload")
                if _RANK[receipt.durability] > _RANK[existing.durability]:
                    selected[record.record_id] = receipt
            else:
                selected[record.record_id] = receipt
        ordered = tuple(sorted(selected.values(), key=lambda value: value.record.sequence))
        previous = None
        for sequence, receipt in enumerate(ordered, 1):
            record = receipt.record
            if record.sequence != sequence or record.previous_checksum != previous:
                raise ProtocolError("virtual journal has a sequence/hash gap or conflict")
            previous = record.checksum
        return ordered

    def _record(self, payload, identity):
        account = self._owner.snapshot()
        return VirtualMarginRecord(
            account.account_id,
            len(self._history) + 1,
            self._history[-1].record.checksum if self._history else None,
            _digest((account.account_id, identity)),
            payload,
        )

    def _append(self, payload, identity):
        self._append_record(self._record(payload, identity))

    def _append_record(self, record):
        self._pending_record = record
        self._state = "unknown"
        durability = self._journal.append(record)
        if not isinstance(durability, Durability):
            raise StorageError("virtual journal returned no declared durability")
        receipt = JournalReceipt(record, durability)
        if not _allowed(receipt):
            raise StorageError("virtual record lacks required durability")
        self._history += (receipt,)
        self._pending_record = None
        self._recover()

    def _recover(self):
        if not self._history or not isinstance(self._history[0].record.payload, AccountAnchor):
            raise ProtocolError("virtual journal has no account anchor")
        expected = self._history[0].record.payload.account
        if not isinstance(expected, MarginAccount) or expected.ledger:
            raise ProtocolError("virtual account anchor is not an explicit initial state")
        authorized, active, intents = [expected], {}, {}
        last_consumed_open, last_funding = -1, -1
        consumed_phases = {}
        for receipt in self._history:
            if not _allowed(receipt):
                self._pending_record = receipt.record
                raise StorageError("virtual record requires primary reconciliation")
            payload = receipt.record.payload
            if receipt.record.sequence == 1:
                continue
            if isinstance(payload, FrozenPendingOrder):
                if type(payload.original) is not (
                    CostMarginReservation
                    if type(payload) is FrozenCostPendingOrder
                    else MarginReservation
                ):
                    raise ProtocolError("virtual intent hides another reservation model")
                if (
                    type(payload) is FrozenCostPendingOrder
                    and payload.original.envelope.anchor_account_fingerprint
                    != _account_fingerprint(expected)
                ):
                    raise ProtocolError("cost intent anchor account prefix changed")
                owner = next(
                    (
                        value
                        for value in expected.positions
                        if value.instrument == payload.original.instrument
                    ),
                    None,
                )
                if (
                    payload.order_id in intents
                    or payload.original.account_id != expected.account_id
                    or payload.original.account_sequence != len(expected.ledger)
                    or owner is None
                    or owner.owner_id != payload.original.owner_id
                    or owner.margin_mode != payload.original.margin_mode
                    or owner.leverage != payload.original.leverage
                ):
                    raise ProtocolError("virtual intent identity was reused")
                active[payload.order_id] = payload
                intents[payload.order_id] = payload
            elif isinstance(payload, PreparedMarginBatch):
                candidate = _validate_batch(payload, expected, active)
                phase_key = (payload.candles[0].open_time_ms, payload.phase)
                previous_cost = consumed_phases.get(phase_key)
                costed = type(payload) is PreparedCostMarginBatch
                if phase_key in consumed_phases and (costed or previous_cost):
                    raise ProtocolError("cost settlement phase was repeated across models")
                expected = candidate
                consumed_phases[phase_key] = costed
                opening = payload.candles[0].open_time_ms
                if opening < last_funding:
                    raise ProtocolError("virtual fill phase predates an applied funding settlement")
                last_consumed_open = max(last_consumed_open, opening)
                authorized.extend(payload.prefix_accounts[1:])
                active = {value.order_id: value for value in payload.remaining}
            elif isinstance(payload, PreparedFundingBatch):
                if (
                    payload.settlement_time_ms <= last_consumed_open
                    or payload.settlement_time_ms <= last_funding
                ):
                    raise ProtocolError("funding phase was reordered or its settlement repeated")
                expected = validate_funding_batch(payload, expected)
                authorized.extend(payload.prefix_accounts[1:])
                last_funding = payload.settlement_time_ms
            elif isinstance(payload, CancelIntent):
                original = active.get(payload.order_id)
                if (
                    original is None
                    or original.original.request_fingerprint != payload.original_request_fingerprint
                ):
                    raise ProtocolError("virtual cancellation conflicts with its original intent")
                active.pop(payload.order_id)
            elif isinstance(payload, EntryControl):
                if type(payload.paused) is not bool:
                    raise ProtocolError("invalid virtual entry control")
            else:
                raise ProtocolError("unexpected account anchor in virtual history")
        owner_pending = self._owner.pending_snapshot()
        if owner_pending is not None and owner_pending.candidate not in authorized:
            raise ProtocolError("unconfirmed account candidate has no durable frozen source")
        if self._owner.status()["state"] != "ready":
            self._owner.reconcile()
        observed = self._owner.snapshot()
        index = next((index for index, value in enumerate(authorized) if value == observed), None)
        if index is None:
            raise ProtocolError("account changed outside its durable virtual prefixes")
        for candidate in authorized[index + 1 :]:
            fact = candidate.ledger[-1].fact
            if isinstance(fact, FundingCashflow):
                self._owner.apply_funding(fact)
            elif isinstance(fact, FillEvent):
                self._owner.apply_fill(fact)
            else:
                raise ProtocolError("unexpected non-fill virtual candidate")
            if self._owner.snapshot() != candidate:
                raise ProtocolError("committed virtual account differs from its frozen prefix")
        self._intents, self._active = intents, active
        self._state, self._error = "ready", None

    def snapshot(self):
        with self._lock:
            account = self._owner.snapshot()
            # During partial commit, ledger facts consume their orders even when
            # the batch acknowledgement/book cache has not yet been published.
            active = {}
            frozen = {
                fact.fill_id: fact
                for receipt in self._history
                if isinstance(receipt.record.payload, PreparedMarginBatch)
                for fact in receipt.record.payload.facts
            }
            filled = (
                {
                    entry.fact.order_id
                    for entry in account.ledger
                    if isinstance(entry.fact, FillEvent)
                    and frozen.get(entry.fact.fill_id) == entry.fact
                }
                if account
                else set()
            )
            for receipt in self._history:
                payload = receipt.record.payload
                if isinstance(payload, FrozenPendingOrder):
                    active[payload.order_id] = payload
                elif isinstance(payload, CancelIntent):
                    active.pop(payload.order_id, None)
                elif isinstance(payload, PreparedMarginBatch):
                    for identity, _ in payload.rejections:
                        active.pop(identity, None)
            return VirtualMarginSnapshot(
                account,
                tuple(
                    sorted(
                        (value for identity, value in active.items() if identity not in filled),
                        key=lambda value: value.order_id,
                    )
                ),
                self._paused
                or self._state != "ready"
                or any(value.durability != Durability.PRIMARY for value in self._history),
                self._state,
                self._error,
                self._pending_record.record_id if self._pending_record else None,
            )

    def _ready(self, *, new_risk=False):
        if self._state != "ready" or self._owner.status()["state"] != "ready":
            raise TradingBlocked("virtual margin execution requires explicit reconciliation")
        if new_risk and (
            self._paused or any(value.durability != Durability.PRIMARY for value in self._history)
        ):
            raise TradingBlocked("new virtual risk is paused or primary history is unavailable")

    def submit_intent(
        self, pending, *, marks, brackets, max_mark_age_ms=120000, max_bracket_age_ms=86400000
    ):
        with self._lock:
            if type(pending) not in {FrozenPendingOrder, FrozenCostPendingOrder} or type(
                pending.original
            ) is not (
                CostMarginReservation
                if type(pending) is FrozenCostPendingOrder
                else MarginReservation
            ):
                raise ValidationError("typed frozen pending order required")
            self._ready()
            existing = self._intents.get(pending.order_id)
            if existing is not None:
                if existing != pending:
                    raise ProtocolError("virtual order ID conflicts with its original intent")
                return existing
            self._ready(new_risk=not pending.original.proposal.reducing)
            original = pending.original
            verified = approve_order(
                self._owner.snapshot(),
                pending.order_id,
                original.proposal,
                instrument=original.instrument,
                owner_id=original.owner_id,
                as_of_ms=original.as_of_ms,
                marks=marks,
                brackets=brackets,
                worst_execution_price=original.worst_execution_price,
                fee_rate=original.fee_rate,
                reservations=tuple(value.original for value in self._active.values()),
                max_mark_age_ms=max_mark_age_ms,
                max_bracket_age_ms=max_bracket_age_ms,
                **(
                    {"cost_envelope": original.envelope}
                    if type(pending) is FrozenCostPendingOrder
                    else {}
                ),
            )
            if verified != original:
                raise ProtocolError(
                    "virtual intent reservation was not approved against its current book"
                )
            try:
                self._append(pending, ("intent", pending.order_id))
            except Exception as error:
                self._fail(error)
                raise
            return pending

    def prepare_batch(self, candles, *, as_of_ms, phase="open", brackets=()):
        with self._lock:
            self._ready()
            self._prepared = prepare_pending_batch(
                self._owner.snapshot(),
                tuple(self._active.values()),
                candles,
                as_of_ms=as_of_ms,
                phase=phase,
                brackets=brackets,
            )
            return self._prepared

    def committed_batches(self, open_time_ms, phase):
        """Read immutable durable phase records; never reconcile or create facts.

        A caller must separately reconcile before treating an ACK-unconfirmed
        record as applied to the account. Ready owners use their verified cache;
        uncertain append outcomes inspect the original journal without mutation.
        """
        if (
            type(open_time_ms) is not int
            or not 0 <= open_time_ms <= 253402300799000
            or open_time_ms % 60000
            or type(phase) is not str
            or phase not in {"open", "close"}
        ):
            raise ValidationError("invalid frozen margin batch lookup")
        with self._lock:
            account = self._owner.snapshot()
            if account is None:
                raise TradingBlocked("virtual account is not confirmed")
            history = self._history if self._state == "ready" else self._read(account.account_id)
            if history is None:
                raise StorageError("virtual journal is missing")
            return tuple(
                receipt.record.payload
                for receipt in history
                if _allowed(receipt)
                and isinstance(receipt.record.payload, PreparedMarginBatch)
                and receipt.record.payload.phase == phase
                and receipt.record.payload.candles
                and receipt.record.payload.candles[0].open_time_ms == open_time_ms
            )

    def original_intent(self, order_id):
        """Return a verified accepted original, never hide uncertain history as absence."""
        _id(order_id)
        with self._lock:
            self._ready()
            return self._intents.get(order_id)

    def ledger_observation(self):
        """Return only confirmed immutable memory; never reconcile or read sources."""
        with self._lock:
            self._ready()
            account = self._owner.snapshot()
            if (
                account is None
                or not self._history
                or type(self._history[0].record.payload) is not AccountAnchor
                or self._history[0].record.account_id != account.account_id
            ):
                raise TradingBlocked("financial ledger source anchor is unavailable")
            return self._history[0].record.checksum, account

    def confirmed_source_observation(self, *, require_paused=False):
        """Immutable full source identity, including controls absent from ledger.

        This port performs no journal/store IO or reconciliation and grants no
        closure authority. Its caller still checks paused/flat/pending gates.
        """
        if type(require_paused) is not bool:
            raise ValidationError("source observation pause requirement must be a native boolean")
        with self._lock:
            self._ready()
            if require_paused and self._paused is not True:
                raise TradingBlocked("source observation requires an explicit financial pause")
            if self._pending_record is not None:
                raise TradingBlocked("source acknowledgement remains unconfirmed")
            anchor, account = self.ledger_observation()
            records = tuple(
                (item.record.sequence, item.record.record_id, item.record.checksum)
                for item in self._history
            )
            if not records:
                raise TradingBlocked("confirmed source history is unavailable")
            return anchor, records[-1][2], len(records), _digest(records), account

    def cost_quote_summaries(self, *, limit=10000):
        """Explain only confirmed applied facts; never reprice or debit a wallet."""
        from omega.application.margin_cost import COST_MODEL

        if type(limit) is not int or not 1 <= limit <= 10000:
            raise ValidationError("cost quote display limit must be in [1,10000]")
        unavailable = {
            "schema_version": 1,
            "model": COST_MODEL,
            "state": "unavailable",
            "total": 0,
            "truncated": False,
            "rows": [],
        }
        with self._lock:
            try:
                self._ready()
            except TradingBlocked:
                return unavailable
            account = self._owner.snapshot()
            if account is None:
                return unavailable
            applied = {
                entry.fact.fill_id: entry.fact
                for entry in account.ledger
                if type(entry.fact) is FillEvent
            }
            rows = {}
            for receipt in self._history:
                plan = receipt.record.payload
                if type(plan) is not PreparedCostMarginBatch or not _allowed(receipt):
                    continue
                pending = {order.order_id: order for order in plan.original_pending}
                facts = {fact.order_id: fact for fact in plan.facts}
                for admission in plan.admissions:
                    if (
                        type(admission) is not CostPendingFillAdmission
                        or admission.outcome != "fill"
                    ):
                        continue
                    fact = facts.get(admission.order_id)
                    if fact is None or applied.get(fact.fill_id) != fact:
                        return unavailable
                    order = pending[admission.order_id]
                    quote = admission.quote
                    if quote is None:
                        return unavailable
                    quantity = fact.delta_qty.copy_abs()
                    with localcontext(_DECIMAL):
                        difference = (
                            fact.price - quote.raw_price
                            if order.original.proposal.side == Side.BUY
                            else quote.raw_price - fact.price
                        ) * quantity
                    rows[fact.fill_id] = {
                        "fill_id": fact.fill_id,
                        "order_id": fact.order_id,
                        "instrument": fact.instrument.key,
                        "side": order.original.proposal.side.value,
                        "quantity": str(quantity),
                        "raw_price": str(quote.raw_price),
                        "final_price": str(fact.price),
                        "fee": str(fact.fee),
                        "explanatory_slippage_amount": str(difference),
                        "policy_sha256": order.original.envelope.policy.sha256,
                        "issued_at_ms": order.original.as_of_ms,
                        "eligible_at_ms": order.eligible_at_ms,
                        "event_time_ms": fact.event_time_ms,
                        "available_at_ms": fact.available_at_ms,
                    }
            values = list(rows.values())
            return {
                "schema_version": 1,
                "model": COST_MODEL,
                "state": "ready",
                "total": len(values),
                "truncated": len(values) > limit,
                "rows": values[-limit:],
            }

    def commit_batch(self, plan):
        with self._lock:
            self._ready()
            if plan is not self._prepared:
                raise ProtocolError(
                    "commit the exact prepared object without recomputing its input"
                )
            if not plan.ready and not plan.facts and not plan.rejections:
                raise TradingBlocked("unavailable batch has no frozen progress to commit")
            _validate_batch(plan, self._owner.snapshot(), self._active)
            identity = _digest((self._owner.snapshot().account_id, ("batch", plan.batch_id)))
            previous = next(
                (value.record for value in self._history if value.record.record_id == identity),
                None,
            )
            if previous is not None:
                if previous.payload != plan:
                    raise ProtocolError("virtual batch ID conflicts with its frozen checksum")
                return self.snapshot()
            for receipt in self._history:
                payload = receipt.record.payload
                if (
                    isinstance(payload, PreparedMarginBatch)
                    and payload.phase == plan.phase
                    and payload.candles[0].open_time_ms == plan.candles[0].open_time_ms
                    and (
                        type(plan) is PreparedCostMarginBatch
                        or type(payload) is PreparedCostMarginBatch
                    )
                ):
                    raise ProtocolError("cost settlement phase was already committed")
            try:
                self._append(plan, ("batch", plan.batch_id))
                self._prepared = None
            except Exception as error:
                self._fail(error)
                raise
            return self.snapshot()

    def prepare_funding(self, points, *, as_of_ms):
        with self._lock:
            self._ready()
            plan = prepare_funding_batch(self._owner.snapshot(), points, as_of_ms=as_of_ms)
            self._funding_order(plan.settlement_time_ms)
            self._prepared = plan
            return self._prepared

    def restore_prepared_funding(self, document):
        """Restore exact saved facts against their original baseline, never a new quote."""
        with self._lock:
            self._ready()
            plan = restore_funding_batch(document, self._owner.snapshot())
            self._funding_order(plan.settlement_time_ms)
            self._prepared = plan
            return self._prepared

    def _funding_order(self, settlement_time_ms):
        for receipt in self._history:
            payload = receipt.record.payload
            if (
                isinstance(payload, PreparedMarginBatch)
                and payload.candles
                and payload.candles[0].open_time_ms >= settlement_time_ms
                or isinstance(payload, PreparedFundingBatch)
                and payload.settlement_time_ms >= settlement_time_ms
            ):
                raise ProtocolError("funding must precede the current minute open phase")

    def committed_funding_batches(self, settlement_time_ms):
        if (
            type(settlement_time_ms) is not int
            or not 0 <= settlement_time_ms <= MAX_TIME
            or settlement_time_ms % 60000
        ):
            raise ValidationError("invalid frozen funding batch lookup")
        with self._lock:
            account = self._owner.snapshot()
            if account is None:
                raise TradingBlocked("virtual account is not confirmed")
            history = self._history if self._state == "ready" else self._read(account.account_id)
            if history is None:
                raise StorageError("virtual journal is missing")
            result = []
            for receipt in history:
                payload = receipt.record.payload
                if (
                    isinstance(payload, PreparedFundingBatch)
                    and payload.settlement_time_ms == settlement_time_ms
                ):
                    if not _allowed(receipt):
                        raise StorageError("funding source has unconfirmed durability")
                    result.append(payload)
            return tuple(result)

    def commit_funding(self, plan):
        with self._lock:
            self._ready()
            if not isinstance(plan, PreparedFundingBatch) or plan is not self._prepared:
                raise ProtocolError("commit the exact restored/prepared funding object")
            identity = _digest((self._owner.snapshot().account_id, ("funding", plan.batch_id)))
            existing = next(
                (item.record for item in self._history if item.record.record_id == identity), None
            )
            if existing is not None:
                if existing.payload != plan:
                    raise ProtocolError("funding batch ID conflicts with its original facts")
                self._prepared = None
                return self.snapshot()
            self._funding_order(plan.settlement_time_ms)
            validate_funding_batch(plan, self._owner.snapshot())
            try:
                self._append(plan, ("funding", plan.batch_id))
                self._prepared = None
            except Exception as error:
                self._fail(error)
                raise
            return self.snapshot()

    def _control(self, paused, request_id):
        _id(request_id)
        self._ready()
        payload = EntryControl(paused, request_id)
        identity = _digest((self._owner.snapshot().account_id, ("control", request_id)))
        existing = next(
            (value.record for value in self._history if value.record.record_id == identity), None
        )
        if existing is not None:
            if existing.payload != payload:
                raise ProtocolError("entry control ID conflicts with its original request")
            # Retrying an old acknowledged command must not undo a later pause
            # or clear a new process's restart gate.
            return False
        try:
            self._append(payload, ("control", request_id))
            return True
        except Exception as error:
            self._fail(error)
            raise

    def manual_resume(self, request_id):
        with self._lock:
            # The explicit command itself is allowed while restart-paused.
            self._ready()
            if any(value.durability != Durability.PRIMARY for value in self._history):
                raise TradingBlocked("primary history must recover before new entries")
            if self._control(False, request_id):
                self._paused = False

    def pause_entries(self, request_id):
        with self._lock:
            self._paused = True
            self._control(True, request_id)

    def cancel_order(self, order_id, request_id):
        with self._lock:
            self._ready()
            _id(request_id)
            identity = _digest((self._owner.snapshot().account_id, ("cancel", request_id)))
            existing = next(
                (value.record for value in self._history if value.record.record_id == identity),
                None,
            )
            if existing is not None:
                if (
                    not isinstance(existing.payload, CancelIntent)
                    or existing.payload.order_id != order_id
                ):
                    raise ProtocolError("cancellation ID conflicts with its original request")
                return
            original = self._active.get(order_id)
            if original is None:
                raise ValidationError("virtual pending order not found")
            try:
                self._append(
                    CancelIntent(order_id, original.original.request_fingerprint),
                    ("cancel", request_id),
                )
            except Exception as error:
                self._fail(error)
                raise

    def reconcile(self):
        with self._lock:
            try:
                account = self._owner.snapshot()
                if account is None:
                    raise StorageError("virtual account requires separate initial reconciliation")
                loaded = self._read(account.account_id)
                pending = self._pending_record
                if loaded is None:
                    loaded = ()
                if pending is not None:
                    found = next(
                        (value for value in loaded if value.record.record_id == pending.record_id),
                        None,
                    )
                    if found is not None and found.record != pending:
                        raise ProtocolError("unconfirmed virtual record changed")
                    if found is None or not _allowed(found):
                        prefix = tuple(
                            value for value in loaded if value.record.sequence < pending.sequence
                        )
                        if (
                            len(prefix) != pending.sequence - 1
                            or (prefix[-1].record.checksum if prefix else None)
                            != pending.previous_checksum
                            or any(
                                value.record.sequence >= pending.sequence
                                and value.record.record_id != pending.record_id
                                for value in loaded
                            )
                        ):
                            raise ProtocolError(
                                "virtual append conflict; automatic rebase is forbidden"
                            )
                        self._history = prefix
                        self._append_record(pending)
                        return self.snapshot()
                    self._pending_record = None
                if not loaded:
                    raise StorageError("previously registered virtual history is missing")
                self._history = loaded
                self._recover()
                return self.snapshot()
            except Exception as error:
                self._fail(error)
                raise
