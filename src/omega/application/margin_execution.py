"""Pure next-bar full-fill preparation; persist the frozen result before applying it.

Prices are phase-specific OHLCV proxies, never exchange mark prices. This recent
closed-bar reference model has no funding, liquidation or partial-fill generator.
It does not call a broker/store, publish candidates or alter original intents.
"""

import json
import re
from dataclasses import dataclass, fields, is_dataclass
from decimal import ROUND_CEILING, ROUND_HALF_EVEN, Context, Decimal, DecimalException, localcontext
from enum import Enum
from hashlib import sha256

from omega.application.margin_risk import MarginReservation, approve_order
from omega.application.paper_fill import BarFillQuote, resolve_bar_fill
from omega.domain.errors import ProtocolError, TradingBlocked, ValidationError
from omega.domain.margin import (
    FillEvent,
    MarginAccount,
    MarginBracketSnapshot,
    MarginMode,
    MarkSnapshot,
    apply_fill,
    evaluate_margin,
)
from omega.domain.models import Candle, Instrument, OrderKind, Side

MODEL = "usdm_full_fill_ohlcv_proxy_v1"
FEE_QUANTUM = Decimal(".00000001")
MAX_TIME = 253_402_300_799_000
_MATH = Context(prec=80, rounding=ROUND_HALF_EVEN)
_EXACT = Context(prec=256, rounding=ROUND_HALF_EVEN)
ZERO = Decimal(0)


class _FixedBoundViolation(TradingBlocked):
    """An original pending entry needs a new explicit request, not more reserve."""


def _value(value):
    if isinstance(value, Decimal):
        with localcontext(_EXACT):
            return "0" if value == 0 else str(value.normalize())
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {field.name: _value(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, (tuple, list)):
        return [_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _value(item) for key, item in value.items()}
    return value


def _digest(value):
    return sha256(
        json.dumps(_value(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _total(*values):
    with localcontext(_EXACT):
        return sum(values, ZERO)


def account_prefix_checksum(account):
    """Initial full-account anchor; subsequent hashes are a causal prefix chain."""
    if not isinstance(account, MarginAccount):
        raise ValidationError("typed margin account required for a prefix anchor")
    return _digest(account)


def advance_prefix_checksum(previous, candidate):
    """Reproduce a prepared prefix checksum after applying its exact frozen fact."""
    if (
        type(previous) is not str
        or re.fullmatch("[a-f0-9]{64}", previous) is None
        or not isinstance(candidate, MarginAccount)
        or not candidate.ledger
    ):
        raise ValidationError("prefix advancement requires its previous hash and a recorded fact")
    head = {
        field.name: getattr(candidate, field.name)
        for field in fields(candidate)
        if field.name != "ledger"
    }
    return _digest((previous, len(candidate.ledger), candidate.ledger[-1], head))


@dataclass(frozen=True, slots=True)
class FrozenPendingOrder:
    original: MarginReservation
    eligible_at_ms: int

    def __post_init__(self):
        if (
            not isinstance(self.original, MarginReservation)
            or type(self.eligible_at_ms) is not int
            or not self.original.as_of_ms <= self.eligible_at_ms <= MAX_TIME
        ):
            raise ValidationError("pending order requires its original reservation and eligibility")

    @property
    def order_id(self):
        return self.original.order_id


@dataclass(frozen=True, slots=True)
class PendingFillAdmission:
    order_id: str
    original_request_fingerprint: str
    outcome: str
    reason: str | None
    quote: BarFillQuote | None
    reservation: MarginReservation | None
    fee_adjustment: Decimal = ZERO


@dataclass(frozen=True, slots=True)
class PreparedMarginBatch:
    batch_id: str
    checksum: str
    phase: str
    as_of_ms: int
    candles: tuple[Candle, ...]
    brackets: tuple[MarginBracketSnapshot, ...]
    original_pending: tuple[FrozenPendingOrder, ...]
    admissions: tuple[PendingFillAdmission, ...]
    facts: tuple[FillEvent, ...]
    prefix_accounts: tuple[MarginAccount, ...]
    prefix_checksums: tuple[str, ...]
    remaining: tuple[FrozenPendingOrder, ...]
    rejections: tuple[tuple[str, str], ...]
    model: str = MODEL
    fee_quantum: Decimal = FEE_QUANTUM

    @property
    def candidate_account(self):
        return self.prefix_accounts[-1]

    @property
    def ready(self):
        return all(
            value.outcome not in {"deferred", "open_phase_required"} for value in self.admissions
        )


def _quote(pending, candle, phase):
    from omega.application.margin_cost import FrozenCostPendingOrder, cost_quote

    proposal = pending.original.proposal
    if candle.open_time_ms < pending.eligible_at_ms:
        return None
    # Do not even inspect the later wick when preparing the open phase.
    if (
        phase == "open"
        and proposal.kind == OrderKind.LIMIT
        and (
            proposal.side == Side.BUY
            and candle.open > proposal.price
            or proposal.side == Side.SELL
            and candle.open < proposal.price
        )
    ):
        return None
    raw = resolve_bar_fill(proposal, candle, pending.eligible_at_ms)
    return cost_quote(pending, raw) if type(pending) is FrozenCostPendingOrder and raw else raw


def _fee(quantity, price, rate):
    try:
        with localcontext(_MATH):
            return (quantity * price * rate).quantize(FEE_QUANTUM, rounding=ROUND_CEILING)
    except DecimalException:
        raise ValidationError("proxy fee cannot be represented by the bounded model") from None


def _fee_room(account, reservation, others, fee_adjustment, marks, brackets, as_of_ms):
    from omega.application.margin_cost import reservation_fee, reservation_valuation

    if reservation.proposal.reducing:
        return
    evaluation = evaluate_margin(account, marks, brackets, as_of_ms=as_of_ms)
    outputs = {value.instrument: value for value in evaluation.positions}
    position = next(
        value for value in account.positions if value.instrument == reservation.instrument
    )
    needed = _total(reservation.total_reserved, fee_adjustment)
    if _total(outputs[position.instrument].initial_margin, needed) > position.budget_margin:
        raise TradingBlocked("fee rounding exceeds allocated margin budget")
    if position.margin_mode == MarginMode.ISOLATED:
        free = outputs[position.instrument].free_collateral
    else:
        crossed = [value for value in account.positions if value.margin_mode == MarginMode.CROSS]
        if any(not outputs[value.instrument].ready for value in crossed):
            raise TradingBlocked("fee rounding collateral inputs are unavailable")
        free = _total(
            account.cross_wallet,
            *(outputs[value.instrument].unrealized_pnl for value in crossed),
            *(outputs[value.instrument].initial_margin.copy_negate() for value in crossed),
            *(
                value.total_reserved.copy_negate()
                for value in others
                if value.margin_mode == MarginMode.CROSS
            ),
            *(
                _total(
                    _fee(value.proposal.quantity, reservation_valuation(value), value.fee_rate),
                    reservation_fee(value).copy_negate(),
                ).copy_negate()
                for value in others
                if value.margin_mode == MarginMode.CROSS
            ),
        )
    if needed > free:
        raise TradingBlocked("fee rounding exceeds available collateral")


def _admit(account, pending, quote, others, marks, brackets, as_of_ms):
    from omega.application.margin_cost import (
        FrozenCostPendingOrder,
        validate_entry_envelope,
    )

    original = pending.original
    costed = type(pending) is FrozenCostPendingOrder
    if costed and not original.proposal.reducing:
        mark = next((value for value in marks if value.instrument == original.instrument), None)
        if mark is not None:
            try:
                validate_entry_envelope(
                    original, mark_price=mark.price, quote_price=quote.price if quote else None
                )
            except TradingBlocked as error:
                raise _FixedBoundViolation(str(error)) from None
    elif not original.proposal.reducing:
        if quote is not None and (
            original.proposal.side == Side.BUY
            and quote.price > original.adverse_fill_price
            or original.proposal.side == Side.SELL
            and quote.price < original.adverse_fill_price
        ):
            raise _FixedBoundViolation("original_execution_bound_exceeded")
        mark = next((value for value in marks if value.instrument == original.instrument), None)
        if mark is not None:
            with localcontext(_MATH):
                movement = (
                    original.adverse_fill_price - mark.price
                    if original.proposal.side == Side.BUY
                    else mark.price - original.adverse_fill_price
                )
                needed_loss = max(ZERO, original.proposal.quantity * movement)
            if needed_loss > original.entry_loss:
                raise _FixedBoundViolation("original_entry_loss_bound_exceeded")
    return approve_order(
        account,
        pending.order_id,
        original.proposal,
        instrument=original.instrument,
        owner_id=original.owner_id,
        as_of_ms=as_of_ms,
        marks=marks,
        brackets=brackets,
        worst_execution_price=(
            quote.price if quote and original.proposal.reducing else original.worst_execution_price
        ),
        fee_rate=original.fee_rate,
        reservations=others,
        **({"cost_envelope": original.envelope} if costed else {}),
    )


def _entry_inputs_available(account, instrument, others, marks, brackets, as_of_ms):
    mark_map = {value.instrument: value for value in marks}
    table = next((value for value in brackets if value.instrument == instrument), None)

    def fresh_mark(key):
        value = mark_map.get(key)
        return (
            value is not None
            and value.available_at_ms <= as_of_ms
            and 0 <= as_of_ms - value.event_time_ms <= 120000
        )

    if (
        not fresh_mark(instrument)
        or table is None
        or table.available_at_ms > as_of_ms
        or not 0 <= as_of_ms - table.observed_at_ms <= 86400000
        or any(not fresh_mark(value.instrument) for value in others)
    ):
        return False
    current = evaluate_margin(account, marks, brackets, as_of_ms=as_of_ms)
    outputs = {value.instrument: value for value in current.positions}
    owner = next(value for value in account.positions if value.instrument == instrument)
    return outputs[instrument].ready and (
        owner.margin_mode != MarginMode.CROSS
        or all(
            outputs[value.instrument].ready
            for value in account.positions
            if value.margin_mode == MarginMode.CROSS
        )
    )


def prepare_pending_batch(account, pending, candles, *, as_of_ms, phase="open", brackets=()):
    """Prepare immutable full-fill facts and exact candidate prefixes, without I/O.

    Each admission is a new *pre-fill check*, not a reissued order or an update
    of its original reservation. Other pending claims are explicitly admitted
    against the current prefix before an entry, so stale books are not reused.
    Unavailable other claims defer new entries; known reductions remain possible.
    """
    from omega.application.margin_cost import (
        COST_MODEL,
        CostMarginReservation,
        CostPendingFillAdmission,
        FrozenCostPendingOrder,
        PreparedCostMarginBatch,
        reservation_fee,
        reservation_valuation,
    )

    if (
        not isinstance(account, MarginAccount)
        or type(pending) is not tuple
        or len(pending) > 100
        or any(type(value) not in {FrozenPendingOrder, FrozenCostPendingOrder} for value in pending)
        or any(
            type(value.original)
            is not (
                CostMarginReservation
                if type(value) is FrozenCostPendingOrder
                else MarginReservation
            )
            for value in pending
        )
        or len({value.order_id for value in pending}) != len(pending)
        or len({value.original.instrument for value in pending}) != len(pending)
        or type(candles) is not tuple
        or not 1 <= len(candles) <= 100
        or any(not isinstance(value, Candle) for value in candles)
        or any(
            not isinstance(value.instrument, Instrument)
            or type(value.open_time_ms) is not int
            or type(value.close_time_ms) is not int
            or value.open_time_ms < 0
            or value.close_time_ms > MAX_TIME
            or value.open_time_ms % 60000
            or value.close_time_ms - value.open_time_ms != 60000
            for value in candles
        )
        or len({value.instrument for value in candles}) != len(candles)
        or len({(value.open_time_ms, value.close_time_ms) for value in candles}) != 1
        or type(as_of_ms) is not int
        or not 0 <= as_of_ms <= MAX_TIME
        or type(phase) is not str
        or phase not in {"open", "close"}
    ):
        raise ValidationError("invalid bounded pending-order candle batch")
    candles = tuple(sorted(candles, key=lambda value: value.instrument.key))
    opening, ending = candles[0].open_time_ms, candles[0].close_time_ms
    if as_of_ms < ending:
        raise ValidationError("pending preparation requires actual knowledge of the closed candle")
    if any(entry.fact.available_at_ms > as_of_ms for entry in account.ledger):
        raise ProtocolError("account snapshot contains facts unavailable at the batch cutoff")
    # Validate snapshot shape/freshness policies even for a reduction-only batch.
    evaluate_margin(account, (), brackets, as_of_ms=as_of_ms)
    brackets = tuple(sorted(brackets, key=lambda value: value.instrument.key))
    pending = tuple(
        sorted(pending, key=lambda value: (value.original.instrument.key, value.order_id))
    )
    owners = {value.instrument: value for value in account.positions}
    for value in pending:
        original, owner = value.original, owners.get(value.original.instrument)
        if (
            original.account_id != account.account_id
            or owner is None
            or original.owner_id != owner.owner_id
            or original.margin_mode != owner.margin_mode
            or original.leverage != owner.leverage
            or original.account_sequence > len(account.ledger)
            or original.as_of_ms > as_of_ms
        ):
            raise ProtocolError("pending intent conflicts with its account/configuration prefix")
    marks = tuple(
        MarkSnapshot(
            value.instrument,
            value.open if phase == "open" else value.close,
            opening if phase == "open" else ending,
            as_of_ms,
            f"ohlcv_proxy:{phase}:v1",
        )
        for value in candles
    )
    lookup = {value.instrument: value for value in candles}
    active = {value.order_id: value for value in pending}
    current, prefixes = account, [account]
    prefix_checksums = [account_prefix_checksum(account)]
    admissions, facts, rejected = [], [], []
    for order in pending:
        original = order.original
        costed = type(order) is FrozenCostPendingOrder
        admission_type = CostPendingFillAdmission if costed else PendingFillAdmission
        stable_fill_id = _digest(
            (
                COST_MODEL if costed else MODEL,
                account.account_id,
                order.order_id,
                original.request_fingerprint,
            )
        )
        recorded = next(
            (
                entry.fact
                for entry in current.ledger
                if isinstance(entry.fact, FillEvent)
                and (entry.fact.fill_id == stable_fill_id or entry.fact.order_id == order.order_id)
            ),
            None,
        )
        if recorded is not None:
            signed = (
                original.proposal.quantity
                if original.proposal.side == Side.BUY
                else original.proposal.quantity.copy_negate()
            )
            if (
                recorded.fill_id != stable_fill_id
                or recorded.instrument != original.instrument
                or recorded.owner_id != original.owner_id
                or recorded.delta_qty != signed
                or recorded.event_time_ms < order.eligible_at_ms
                or recorded.event_time_ms % 60000
                or recorded.fee
                != _fee(recorded.delta_qty.copy_abs(), recorded.price, original.fee_rate)
            ):
                raise ProtocolError("recorded fill conflicts with its original full-fill intent")
            active.pop(order.order_id)
            admissions.append(
                admission_type(
                    order.order_id,
                    original.request_fingerprint,
                    "already_settled",
                    None,
                    None,
                    None,
                )
            )
            continue
        candle = lookup.get(original.instrument)
        reason = "candle_missing" if candle is None else None
        if candle is not None and opening < order.eligible_at_ms:
            admissions.append(
                admission_type(
                    order.order_id,
                    original.request_fingerprint,
                    "waiting_eligible",
                    None,
                    None,
                    None,
                )
            )
            continue
        quote = None if candle is None else _quote(order, candle, phase)
        if reason is None and quote is None:
            admissions.append(
                admission_type(
                    order.order_id,
                    original.request_fingerprint,
                    "await_close" if phase == "open" else "not_touched",
                    None,
                    None,
                    None,
                )
            )
            continue
        if reason is None and quote.phase != phase:
            reason = "open_phase_required"
        if reason is not None:
            admissions.append(
                admission_type(
                    order.order_id, original.request_fingerprint, "deferred", reason, quote, None
                )
            )
            continue
        # Quantization/FillEvent-bound failure rejects the *whole* pure prepare;
        # no partial result or external fact has been published.
        fee = _fee(original.proposal.quantity, quote.price, original.fee_rate)
        signed = (
            original.proposal.quantity
            if original.proposal.side == Side.BUY
            else original.proposal.quantity.copy_negate()
        )
        fact = FillEvent(
            stable_fill_id,
            order.order_id,
            original.instrument,
            original.owner_id,
            signed,
            quote.price,
            fee,
            "USDT",
            quote.event_time_ms,
            as_of_ms,
        )
        others = []
        context_missing = False
        if not original.proposal.reducing:
            for other in sorted(active.values(), key=lambda value: value.order_id):
                if other.order_id == order.order_id:
                    continue
                other_candle = lookup.get(other.original.instrument)
                other_quote = None if other_candle is None else _quote(other, other_candle, phase)
                if other_quote is not None and other_quote.phase != phase:
                    other_quote = None
                try:
                    reservation = _admit(
                        current, other, other_quote, tuple(others), marks, brackets, as_of_ms
                    )
                    adjustment = _total(
                        _fee(
                            reservation.proposal.quantity,
                            reservation_valuation(reservation),
                            reservation.fee_rate,
                        ),
                        reservation_fee(reservation).copy_negate(),
                    )
                    _fee_room(
                        current, reservation, tuple(others), adjustment, marks, brackets, as_of_ms
                    )
                    others.append(reservation)
                except TradingBlocked:
                    context_missing = True
                    break
        if context_missing:
            admissions.append(
                admission_type(
                    order.order_id,
                    original.request_fingerprint,
                    "deferred",
                    "reservation_context_unavailable",
                    quote,
                    None,
                )
            )
            continue
        if not original.proposal.reducing and not _entry_inputs_available(
            current, original.instrument, others, marks, brackets, as_of_ms
        ):
            admissions.append(
                admission_type(
                    order.order_id,
                    original.request_fingerprint,
                    "deferred",
                    "input_unavailable",
                    quote,
                    None,
                )
            )
            continue
        reservation = None
        adjustment = ZERO
        try:
            reservation = _admit(current, order, quote, tuple(others), marks, brackets, as_of_ms)
            adjustment = max(ZERO, _total(fee, reservation_fee(reservation).copy_negate()))
            _fee_room(current, reservation, tuple(others), adjustment, marks, brackets, as_of_ms)
        except TradingBlocked as error:
            reason = (
                error.args[0]
                if isinstance(error, _FixedBoundViolation)
                else "prefill_risk_rejected"
            )
            admissions.append(
                admission_type(
                    order.order_id,
                    original.request_fingerprint,
                    "rejected",
                    reason,
                    quote,
                    reservation,
                    adjustment,
                )
            )
            rejected.append((order.order_id, reason))
            active.pop(order.order_id)
            continue
        transition = apply_fill(current, fact)
        current = transition.account
        facts.append(fact)
        prefixes.append(current)
        prefix_checksums.append(advance_prefix_checksum(prefix_checksums[-1], current))
        active.pop(order.order_id)
        admissions.append(
            admission_type(
                order.order_id,
                original.request_fingerprint,
                "fill",
                None,
                quote,
                reservation,
                adjustment,
            )
        )
    remaining = tuple(value for value in pending if value.order_id in active)
    costed_batch = any(type(value) is FrozenCostPendingOrder for value in pending)
    model = COST_MODEL if costed_batch else MODEL
    batch_id = _digest(
        (model, account.account_id, opening, phase, tuple(value.order_id for value in pending))
    )
    checksum = _digest(
        (
            model,
            FEE_QUANTUM,
            "ROUND_CEILING",
            batch_id,
            as_of_ms,
            candles,
            brackets,
            pending,
            tuple(admissions),
            tuple(facts),
            tuple(prefix_checksums),
            remaining,
            tuple(rejected),
        )
    )
    batch_type = PreparedCostMarginBatch if costed_batch else PreparedMarginBatch
    return batch_type(
        batch_id,
        checksum,
        phase,
        as_of_ms,
        candles,
        brackets,
        pending,
        tuple(admissions),
        tuple(facts),
        tuple(prefixes),
        tuple(prefix_checksums),
        remaining,
        tuple(rejected),
        model=model,
    )
