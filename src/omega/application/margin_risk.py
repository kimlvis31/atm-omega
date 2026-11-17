"""Bounded margin reservation values; not a venue executor or factual ledger.

The caller must first verify factual-owner certainty, venue grids and its actual
open-order ledger. No reservation persistence, partial consumption or automatic
revaluation/release is implemented here. A reservation never changes quantities.
"""

import json
import re
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Context, Decimal, DecimalException, Inexact, localcontext
from hashlib import sha256

from omega.domain.errors import ProtocolError, TradingBlocked, ValidationError
from omega.domain.margin import (
    MarginAccount,
    MarginBracketSnapshot,
    MarginMode,
    MarkSnapshot,
    evaluate_margin,
)
from omega.domain.models import Instrument, Side, require_decimal
from omega.domain.trading import OrderProposal

MAX_RESERVATIONS = 100
MAX_TIME = 253_402_300_799_000
ZERO = Decimal(0)
_MATH = Context(prec=80, rounding=ROUND_HALF_EVEN)
_TOTAL = Context(prec=256, rounding=ROUND_HALF_EVEN)


def _number(value, *, external=True):
    require_decimal(value)
    bound = 18 if external else 256
    if len(value.as_tuple().digits) > bound or not -bound <= value.as_tuple().exponent <= bound:
        raise ValidationError("reservation Decimal exceeds its calculation bound")


def _total(*values):
    try:
        with localcontext(_TOTAL) as context:
            context.traps[Inexact] = True
            return sum(values, ZERO)
    except DecimalException:
        raise ValidationError("reservation total exceeds its exact arithmetic bound") from None


def _id(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValidationError("invalid margin reservation identity")


def _hash(value):
    if type(value) is not str or re.fullmatch("[a-f0-9]{64}", value) is None:
        raise ValidationError("invalid margin reservation fingerprint")


def _decimal(value):
    # normalize() without an explicit context would round under a caller's context.
    with localcontext(_TOTAL):
        return "0" if value == 0 else str(value.normalize())


def _digest(value):
    return sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _proposal(value):
    return (
        value.side.value,
        _decimal(value.quantity),
        value.kind.value,
        None if value.price is None else _decimal(value.price),
        value.reducing,
        value.reason,
    )


def _adverse_price(proposal, worst):
    if proposal.price is None:
        return worst
    return (max if proposal.side == Side.BUY else min)(worst, proposal.price)


def _opening_loss(proposal, adverse, mark):
    if proposal.reducing:
        return ZERO
    with localcontext(_MATH):
        movement = adverse - mark if proposal.side == Side.BUY else mark - adverse
        return max(ZERO, proposal.quantity * movement)


def _account_fingerprint(account):
    return _digest(
        (
            account.account_id,
            account.settlement_asset,
            len(account.ledger),
            account.event_capacity,
            _decimal(account.opening_balance),
            _decimal(account.cross_wallet),
            sorted(
                (
                    position.instrument.key,
                    position.owner_id,
                    position.margin_mode.value,
                    position.leverage,
                    *(
                        _decimal(value)
                        for value in (
                            position.budget_margin,
                            position.quantity,
                            position.entry_price,
                            position.isolated_wallet,
                            position.realized_pnl,
                        )
                    ),
                )
                for position in account.positions
            ),
        )
    )


@dataclass(frozen=True, slots=True)
class MarginReservation:
    order_id: str
    account_id: str
    instrument: Instrument
    owner_id: str
    margin_mode: MarginMode
    account_sequence: int
    proposal: OrderProposal
    as_of_ms: int
    worst_execution_price: Decimal
    valuation_price: Decimal
    fee_rate: Decimal
    leverage: int
    initial_margin: Decimal
    fee_reserve: Decimal
    mark_price: Decimal | None
    adverse_fill_price: Decimal
    entry_loss: Decimal
    account_fingerprint: str
    request_fingerprint: str

    def __post_init__(self):
        for value in (self.order_id, self.account_id, self.owner_id):
            _id(value)
        for value in (self.account_fingerprint, self.request_fingerprint):
            _hash(value)
        if (
            not isinstance(self.instrument, Instrument)
            or not isinstance(self.margin_mode, MarginMode)
            or not isinstance(self.proposal, OrderProposal)
            or type(self.account_sequence) is not int
            or not 0 <= self.account_sequence <= 10000
            or type(self.as_of_ms) is not int
            or not 0 <= self.as_of_ms <= MAX_TIME
            or type(self.leverage) is not int
            or not 1 <= self.leverage <= 1000
        ):
            raise ValidationError("invalid margin reservation metadata")
        for value in (
            self.worst_execution_price,
            self.valuation_price,
            self.adverse_fill_price,
            self.fee_rate,
            self.proposal.quantity,
        ):
            _number(value)
        if self.proposal.price is not None:
            _number(self.proposal.price)
        if self.mark_price is not None:
            _number(self.mark_price)
        for value in (self.initial_margin, self.fee_reserve, self.entry_loss):
            _number(value, external=False)
        if (
            self.worst_execution_price <= 0
            or self.valuation_price < self.worst_execution_price
            or not ZERO <= self.fee_rate <= 1
            or self.initial_margin < 0
            or self.fee_reserve < 0
            or self.adverse_fill_price <= 0
            or self.entry_loss < 0
            or self.mark_price is None
            and not self.proposal.reducing
            or self.mark_price is not None
            and (self.mark_price <= 0 or self.valuation_price < self.mark_price)
            or self.proposal.price is not None
            and self.valuation_price < self.proposal.price
        ):
            raise ValidationError("invalid margin reservation prices or amounts")
        with localcontext(_MATH):
            notional = self.proposal.quantity * self.valuation_price
            expected = ZERO if self.proposal.reducing else notional / self.leverage
            if (
                self.initial_margin != expected
                or self.fee_reserve != notional * self.fee_rate
                or self.adverse_fill_price
                != _adverse_price(self.proposal, self.worst_execution_price)
                or self.entry_loss
                != _opening_loss(self.proposal, self.adverse_fill_price, self.mark_price)
            ):
                raise ValidationError("margin reservation amounts do not match their proposal")

    @property
    def total_reserved(self):
        return _total(self.initial_margin, self.fee_reserve, self.entry_loss)


def _reference_values(marks, brackets):
    return (
        sorted(
            (
                value.instrument.key,
                _decimal(value.price),
                value.event_time_ms,
                value.available_at_ms,
                value.source,
            )
            for value in marks
        ),
        sorted(
            (
                value.instrument.key,
                value.observed_at_ms,
                value.available_at_ms,
                value.source,
                value.checksum,
                [
                    (
                        _decimal(row.notional_floor),
                        _decimal(row.notional_cap),
                        _decimal(row.maintenance_rate),
                        _decimal(row.maintenance_amount),
                        row.max_leverage,
                    )
                    for row in value.rows
                ],
            )
            for value in brackets
        ),
    )


def _request_fingerprint(
    account_hash,
    order_id,
    proposal,
    instrument,
    owner_id,
    as_of_ms,
    marks,
    brackets,
    worst,
    fee,
    reservations,
    mark_age,
    bracket_age,
    mark_price,
    adverse_fill_price,
    entry_loss,
):
    return _digest(
        (
            "margin_reservation_v2_opening_loss",
            account_hash,
            order_id,
            _proposal(proposal),
            instrument.key,
            owner_id,
            as_of_ms,
            _reference_values(marks, brackets),
            _decimal(worst),
            _decimal(fee),
            mark_age,
            bracket_age,
            None if mark_price is None else _decimal(mark_price),
            _decimal(adverse_fill_price),
            _decimal(entry_loss),
            sorted(
                (
                    value.order_id,
                    value.request_fingerprint,
                    value.account_fingerprint,
                    value.account_sequence,
                    value.instrument.key,
                    value.owner_id,
                    value.margin_mode.value,
                    _proposal(value.proposal),
                    value.as_of_ms,
                    _decimal(value.worst_execution_price),
                    _decimal(value.valuation_price),
                    _decimal(value.fee_rate),
                    value.leverage,
                    _decimal(value.initial_margin),
                    _decimal(value.fee_reserve),
                    None if value.mark_price is None else _decimal(value.mark_price),
                    _decimal(value.adverse_fill_price),
                    _decimal(value.entry_loss),
                )
                for value in reservations
                if value.order_id != order_id
            ),
        )
    )


def approve_order(
    account: MarginAccount,
    order_id: str,
    proposal: OrderProposal,
    *,
    instrument: Instrument,
    owner_id: str,
    as_of_ms: int,
    marks: tuple[MarkSnapshot, ...],
    brackets: tuple[MarginBracketSnapshot, ...],
    worst_execution_price: Decimal,
    fee_rate: Decimal,
    reservations: tuple[MarginReservation, ...] = (),
    max_mark_age_ms: int = 120000,
    max_bracket_age_ms: int = 86400000,
    cost_envelope=None,
) -> MarginReservation:
    """Return one immutable estimate or reject; never mutate ledger/book or send orders.

    Confirmed reductions reserve fee but are not denied for insufficient margin,
    over-budget existing exposure or missing/stale brackets. Serial ownership and
    known current quantity are still mandatory. Increasing risk requires fresh
    supplied references and every existing reservation to remain conservative.
    Opening loss is an explicit conservative model: BUY uses max(worst, limit)
    and SELL min(worst, limit), while notional uses max(mark, worst, limit).
    This can over-reserve an impossible limit-fill range; it is not venue pricing.
    """
    from omega.application.margin_cost import (
        CostEnvelope,
        CostMarginReservation,
        cost_reservation,
        reservation_valuation,
        validate_entry_envelope,
    )

    if cost_envelope is not None and type(cost_envelope) is not CostEnvelope:
        raise ValidationError("cost admission requires its exact frozen envelope")
    if not isinstance(account, MarginAccount) or not isinstance(proposal, OrderProposal):
        raise ValidationError("typed margin account and order proposal required")
    _id(order_id)
    _id(owner_id)
    if not isinstance(instrument, Instrument):
        raise ValidationError("typed margin instrument required")
    if type(as_of_ms) is not int or not 0 <= as_of_ms <= MAX_TIME:
        raise ValidationError("invalid reservation observation cutoff")
    if any(entry.fact.available_at_ms > as_of_ms for entry in account.ledger):
        raise ProtocolError("account facts were unavailable at the reservation cutoff")
    for value in (worst_execution_price, fee_rate, proposal.quantity):
        _number(value)
    if proposal.price is not None:
        _number(proposal.price)
    if worst_execution_price <= 0 or not ZERO <= fee_rate <= 1:
        raise ValidationError("reservation needs positive worst price and a fee rate in [0,1]")
    if (
        type(reservations) is not tuple
        or len(reservations) > MAX_RESERVATIONS
        or any(
            type(value) not in {MarginReservation, CostMarginReservation} for value in reservations
        )
        or len({value.order_id for value in reservations}) != len(reservations)
        or len({value.instrument for value in reservations}) != len(reservations)
    ):
        raise ValidationError("invalid bounded margin reservation book")
    # This also validates tuple/type/duplicate/time policy before fingerprinting,
    # but its account-level ready flag never substitutes for new-notional checks.
    current = evaluate_margin(
        account,
        marks,
        brackets,
        as_of_ms=as_of_ms,
        max_mark_age_ms=max_mark_age_ms,
        max_bracket_age_ms=max_bracket_age_ms,
    )
    position = next((value for value in account.positions if value.instrument == instrument), None)
    if position is None or position.owner_id != owner_id:
        raise TradingBlocked("reservation instrument has another or absent owner")
    positions = {value.instrument: value for value in account.positions}
    for reservation in reservations:
        owner = positions.get(reservation.instrument)
        if (
            reservation.account_id != account.account_id
            or owner is None
            or reservation.owner_id != owner.owner_id
            or reservation.margin_mode != owner.margin_mode
            or reservation.leverage != owner.leverage
        ):
            raise ProtocolError("reservation book belongs to another account or configuration")
    mark_map = {value.instrument: value for value in marks}
    mark = mark_map.get(instrument)
    # A protective reduction can estimate fees from its explicit worst price
    # without pretending that a missing/stale quote is a current observation.
    eligible_mark = (
        mark is not None
        and mark.available_at_ms <= as_of_ms
        and mark.event_time_ms <= as_of_ms
        and as_of_ms - mark.event_time_ms <= max_mark_age_ms
    )
    valuation = max(
        worst_execution_price,
        proposal.price or worst_execution_price,
        mark.price if eligible_mark else worst_execution_price,
    )
    mark_price = mark.price if eligible_mark else None
    adverse_fill_price = _adverse_price(proposal, worst_execution_price)
    entry_loss = (
        ZERO
        if proposal.reducing or mark_price is None
        else _opening_loss(proposal, adverse_fill_price, mark_price)
    )
    with localcontext(_MATH):
        notional = proposal.quantity * valuation
        initial = ZERO if proposal.reducing else notional / position.leverage
        fee = notional * fee_rate
    account_hash = _account_fingerprint(account)
    fingerprint = _request_fingerprint(
        account_hash,
        order_id,
        proposal,
        instrument,
        owner_id,
        as_of_ms,
        marks,
        brackets,
        worst_execution_price,
        fee_rate,
        reservations,
        max_mark_age_ms,
        max_bracket_age_ms,
        mark_price,
        adverse_fill_price,
        entry_loss,
    )
    cost_candidate = None
    if cost_envelope is not None:
        base = MarginReservation(
            order_id,
            account.account_id,
            instrument,
            owner_id,
            position.margin_mode,
            len(account.ledger),
            proposal,
            as_of_ms,
            worst_execution_price,
            valuation,
            fee_rate,
            position.leverage,
            initial,
            fee,
            mark_price,
            adverse_fill_price,
            entry_loss,
            account_hash,
            fingerprint,
        )
        cost_candidate = cost_reservation(base, cost_envelope)
        if not proposal.reducing and mark_price is not None:
            validate_entry_envelope(cost_candidate, mark_price=mark_price)
        fingerprint = cost_candidate.request_fingerprint
    duplicate = next((value for value in reservations if value.order_id == order_id), None)
    if duplicate is not None:
        if (
            duplicate.request_fingerprint != fingerprint
            or duplicate.proposal != proposal
            or duplicate.instrument != instrument
            or duplicate.owner_id != owner_id
            or duplicate.as_of_ms != as_of_ms
            or duplicate.worst_execution_price != worst_execution_price
            or duplicate.fee_rate != fee_rate
            or duplicate.account_sequence != len(account.ledger)
            or duplicate.account_fingerprint != account_hash
            or duplicate.valuation_price != valuation
            or duplicate.initial_margin != initial
            or duplicate.fee_reserve != fee
            or duplicate.mark_price != mark_price
            or duplicate.adverse_fill_price != adverse_fill_price
            or duplicate.entry_loss != entry_loss
            or (cost_candidate is None) != (type(duplicate) is MarginReservation)
            or cost_candidate is not None
            and duplicate != cost_candidate
        ):
            raise ProtocolError("order ID conflicts with its frozen reservation request")
        return duplicate
    if len(reservations) >= MAX_RESERVATIONS:
        raise TradingBlocked("margin reservation capacity reached")
    if any(value.instrument == instrument for value in reservations):
        raise TradingBlocked("settle the instrument's existing reservation before another order")
    opposite = (
        position.quantity > 0
        and proposal.side == Side.SELL
        or position.quantity < 0
        and proposal.side == Side.BUY
    )
    if proposal.reducing:
        if not opposite or proposal.quantity > position.quantity.copy_abs():
            raise TradingBlocked("reduction exceeds or reverses the observed position")
    elif opposite:
        raise TradingBlocked("close the current direction before opening the opposite side")
    with localcontext(_MATH):
        if not proposal.reducing:
            if not eligible_mark:
                raise TradingBlocked("new margin risk requires a fresh available mark")
            snapshot = next((value for value in brackets if value.instrument == instrument), None)
            if (
                snapshot is None
                or snapshot.available_at_ms > as_of_ms
                or snapshot.observed_at_ms > as_of_ms
                or as_of_ms - snapshot.observed_at_ms > max_bracket_age_ms
            ):
                raise TradingBlocked("new margin risk requires fresh available brackets")
            planned_notional = (position.quantity.copy_abs() + proposal.quantity) * (
                reservation_valuation(cost_candidate) if cost_candidate is not None else valuation
            )
            row = next(
                (
                    value
                    for value in snapshot.rows
                    if value.notional_floor <= planned_notional < value.notional_cap
                ),
                None,
            )
            if row is None or position.leverage > row.max_leverage:
                raise TradingBlocked("planned notional or leverage exceeds supported brackets")
            for reservation in reservations:
                pending_mark = mark_map.get(reservation.instrument)
                if (
                    reservation.account_sequence != len(account.ledger)
                    or reservation.account_fingerprint != account_hash
                    or reservation.as_of_ms > as_of_ms
                    or pending_mark is None
                    or pending_mark.available_at_ms > as_of_ms
                    or pending_mark.event_time_ms > as_of_ms
                    or as_of_ms - pending_mark.event_time_ms > max_mark_age_ms
                ):
                    raise TradingBlocked("existing reservations require explicit revalidation")
                if type(reservation) is CostMarginReservation:
                    validate_entry_envelope(reservation, mark_price=pending_mark.price)
                elif (
                    pending_mark.price > reservation.valuation_price
                    or _opening_loss(
                        reservation.proposal, reservation.adverse_fill_price, pending_mark.price
                    )
                    > reservation.entry_loss
                ):
                    raise TradingBlocked("existing reservations require explicit revalidation")
                if not reservation.proposal.reducing:
                    pending_snapshot = next(
                        (value for value in brackets if value.instrument == reservation.instrument),
                        None,
                    )
                    if (
                        pending_snapshot is None
                        or pending_snapshot.available_at_ms > as_of_ms
                        or pending_snapshot.observed_at_ms > as_of_ms
                        or as_of_ms - pending_snapshot.observed_at_ms > max_bracket_age_ms
                    ):
                        raise TradingBlocked("existing reservations require explicit revalidation")
                    pending_notional = (
                        positions[reservation.instrument].quantity.copy_abs()
                        + reservation.proposal.quantity
                    ) * max(reservation_valuation(reservation), pending_mark.price)
                    pending_row = next(
                        (
                            value
                            for value in pending_snapshot.rows
                            if value.notional_floor <= pending_notional < value.notional_cap
                        ),
                        None,
                    )
                    if pending_row is None or reservation.leverage > pending_row.max_leverage:
                        raise TradingBlocked("existing reservations require explicit revalidation")
            outputs = {value.instrument: value for value in current.positions}
            target = outputs[instrument]
            if not target.ready:
                raise TradingBlocked("current position inputs are unavailable")
            needed = (
                cost_candidate.total_reserved
                if cost_candidate is not None
                else _total(initial, fee, entry_loss)
            )
            allocated = _total(
                target.initial_margin,
                needed,
                *(value.total_reserved for value in reservations if value.instrument == instrument),
            )
            if allocated > position.budget_margin:
                raise TradingBlocked("proposal exceeds its allocated margin budget")
            if position.margin_mode == MarginMode.CROSS:
                crossed = [
                    value for value in account.positions if value.margin_mode == MarginMode.CROSS
                ]
                if any(not outputs[value.instrument].ready for value in crossed):
                    raise TradingBlocked("cross collateral has unavailable position inputs")
                free = _total(
                    account.cross_wallet,
                    *(outputs[value.instrument].unrealized_pnl for value in crossed),
                    *(outputs[value.instrument].initial_margin.copy_negate() for value in crossed),
                    *(
                        value.total_reserved.copy_negate()
                        for value in reservations
                        if value.margin_mode == MarginMode.CROSS
                    ),
                )
            else:
                free = target.free_collateral
            if needed > free:
                raise TradingBlocked("proposal exceeds its wallet's available collateral")
    if cost_candidate is not None:
        return cost_candidate
    return MarginReservation(
        order_id,
        account.account_id,
        instrument,
        owner_id,
        position.margin_mode,
        len(account.ledger),
        proposal,
        as_of_ms,
        worst_execution_price,
        valuation,
        fee_rate,
        position.leverage,
        initial,
        fee,
        mark_price,
        adverse_fill_price,
        entry_loss,
        account_hash,
        fingerprint,
    )
