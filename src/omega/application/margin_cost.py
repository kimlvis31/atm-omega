"""Explicit virtual full-fill costs; observations and legacy contracts stay intact.

Risk/execution import these variants inside functions, after their base modules
have loaded. Module initialization performs no approval, preparation or I/O.
"""

# Imports
import re
from dataclasses import dataclass, fields
from decimal import (
    ROUND_CEILING,
    ROUND_HALF_EVEN,
    Context,
    Decimal,
    DecimalException,
    Inexact,
    localcontext,
)

from omega.application.margin_execution import (
    FEE_QUANTUM,
    FrozenPendingOrder,
    PendingFillAdmission,
    PreparedMarginBatch,
)
from omega.application.margin_risk import (
    MarginReservation,
    _account_fingerprint,
    _digest,
    _hash,
    _id,
    _number,
)
from omega.application.paper_fill import MODEL as RAW_QUOTE_MODEL
from omega.application.paper_fill import BarFillQuote
from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.margin import (
    MAX_TIME,
    FillEvent,
    MarginAccount,
    MarginBracketSnapshot,
    MarkSnapshot,
    _instrument,
)
from omega.domain.models import Candle, Instrument, Side
from omega.domain.trading import OrderProposal

MODEL = "linear_usdt_ohlcv_cost_v1"
COST_MODEL = MODEL
QUOTE_MODEL = "linear_usdt_ohlcv_cost_quote_v1"
LIMIT_POLICY = "adverse_then_cap_v1"
ZERO, ONE = Decimal(0), Decimal(1)
_EXACT = Context(prec=256, rounding=ROUND_HALF_EVEN)
_MATH = Context(prec=80, rounding=ROUND_HALF_EVEN)


def _time(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("cost timestamp exceeds its native bound")


def _canonical(value):
    """Remove representational zeros only; never context-round an exact value."""
    if type(value) is not Decimal or not value.is_finite():
        raise ValidationError("cost quantities/prices require finite native Decimals")
    sign, digits, exponent = value.as_tuple()
    if len(digits) > 256 or not -256 <= exponent <= 256:
        raise ValidationError("cost Decimal exceeds its exact capacity")
    if value == 0:
        return ZERO
    digits = list(digits)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    return Decimal((sign, tuple(digits), exponent))


def _external(value):
    value = _canonical(value)
    _number(value)
    return value


def _exact(operation):
    try:
        with localcontext(_EXACT) as context:
            context.traps[Inexact] = True
            return operation()
    except DecimalException:
        raise ValidationError("cost calculation exceeds its exact capacity") from None


def _sum(*values):
    return _exact(lambda: sum(values, ZERO))


def _text(value):
    return str(_canonical(value))


@dataclass(frozen=True, slots=True, kw_only=True)
class MarginCostPolicy:
    slippage_rate: Decimal
    execution_price_bound_rate: Decimal
    mark_drift_bound_rate: Decimal
    entry_delay_ms: int
    reduction_delay_ms: int
    schema_version: int = 1
    model: str = MODEL
    limit_price_policy: str = LIMIT_POLICY

    def __post_init__(self):
        if (
            type(self.schema_version) is not int
            or self.schema_version != 1
            or type(self.model) is not str
            or self.model != MODEL
            or type(self.limit_price_policy) is not str
            or self.limit_price_policy != LIMIT_POLICY
        ):
            raise ValidationError("invalid explicit margin cost model")
        for name, maximum in (
            ("slippage_rate", Decimal(".25")),
            ("execution_price_bound_rate", Decimal(".5")),
            ("mark_drift_bound_rate", Decimal(".5")),
        ):
            value = _external(getattr(self, name))
            if not ZERO <= value <= maximum:
                raise ValidationError("cost scenario rate exceeds its supported range")
            object.__setattr__(self, name, value)
        for value in (self.entry_delay_ms, self.reduction_delay_ms):
            if type(value) is not int or not 0 <= value <= 86400000:
                raise ValidationError("cost delay must be native milliseconds within one day")

    def document(self):
        return {
            "schema_version": self.schema_version,
            "model": self.model,
            "slippage_rate": _text(self.slippage_rate),
            "execution_price_bound_rate": _text(self.execution_price_bound_rate),
            "mark_drift_bound_rate": _text(self.mark_drift_bound_rate),
            "entry_delay_ms": self.entry_delay_ms,
            "reduction_delay_ms": self.reduction_delay_ms,
            "limit_price_policy": self.limit_price_policy,
        }

    @property
    def sha256(self):
        return _digest(self.document())

    @classmethod
    def parse(cls, document):
        if type(document) is not dict or set(document) != {field.name for field in fields(cls)}:
            raise ValidationError("cost policy needs every exact versioned field")
        data = dict(document)
        for name in ("slippage_rate", "execution_price_bound_rate", "mark_drift_bound_rate"):
            value = data[name]
            if (
                type(value) is not str
                or not 1 <= len(value) <= 128
                or re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:E[+-]?[0-9]+)?", value) is None
            ):
                raise ValidationError("cost rates must be bounded decimal strings")
            try:
                data[name] = Decimal(value)
            except DecimalException:
                raise ValidationError("invalid cost decimal string") from None
        return cls(**data)


def _bands(policy, proposal, reference, anchor):
    if proposal.reducing:
        return None, None, None, None
    lower, upper, mark_lower, mark_upper = _exact(
        lambda: (
            reference * (ONE - policy.execution_price_bound_rate) * (ONE - policy.slippage_rate),
            reference * (ONE + policy.execution_price_bound_rate) * (ONE + policy.slippage_rate),
            anchor.price * (ONE - policy.mark_drift_bound_rate),
            anchor.price * (ONE + policy.mark_drift_bound_rate),
        )
    )
    if proposal.price is not None:
        if proposal.side == Side.BUY:
            upper = min(upper, proposal.price)
        else:
            lower = max(lower, proposal.price)
    values = tuple(_external(value) for value in (lower, upper, mark_lower, mark_upper))
    if values[0] <= 0 or values[0] > values[1] or values[2] <= 0 or values[2] > values[3]:
        raise TradingBlocked("cost_execution_envelope_empty")
    return values


@dataclass(frozen=True, slots=True)
class CostEnvelope:
    policy: MarginCostPolicy
    order_id: str
    account_id: str
    instrument: Instrument
    owner_id: str
    proposal: OrderProposal
    issued_at_ms: int
    anchor_account_fingerprint: str
    reference_price: Decimal
    anchor: MarkSnapshot | None
    execution_lower: Decimal | None
    execution_upper: Decimal | None
    mark_lower: Decimal | None
    mark_upper: Decimal | None

    def __post_init__(self):
        for value in (self.order_id, self.account_id, self.owner_id):
            _id(value)
        _time(self.issued_at_ms)
        _hash(self.anchor_account_fingerprint)
        if (
            type(self.policy) is not MarginCostPolicy
            or type(self.instrument) is not Instrument
            or type(self.proposal) is not OrderProposal
            or self.anchor is not None
            and type(self.anchor) is not MarkSnapshot
        ):
            raise ValidationError("cost envelope needs explicit immutable host types")
        _instrument(self.instrument)
        reference = _external(self.reference_price)
        if reference <= 0:
            raise ValidationError("cost reference assumption must be positive")
        if self.anchor is not None and (
            self.anchor.instrument != self.instrument
            or self.anchor.available_at_ms > self.issued_at_ms
            or self.anchor.event_time_ms > self.issued_at_ms
        ):
            raise ValidationError("cost anchor was unavailable at the original issue")
        if not self.proposal.reducing and self.anchor is None:
            raise TradingBlocked("cost_entry_requires_observed_anchor")
        actual = (self.execution_lower, self.execution_upper, self.mark_lower, self.mark_upper)
        if any(value is not None and type(value) is not Decimal for value in actual):
            raise ValidationError("cost bounds require exact Decimals")
        if actual != _bands(self.policy, self.proposal, reference, self.anchor):
            raise ValidationError("cost envelope differs from its pinned policy calculation")

    def document(self):
        proposal = self.proposal
        return {
            "policy": self.policy.document(),
            "order_id": self.order_id,
            "account_id": self.account_id,
            "instrument": self.instrument.key,
            "owner_id": self.owner_id,
            "issued_at_ms": self.issued_at_ms,
            "anchor_account_fingerprint": self.anchor_account_fingerprint,
            "reference_price": _text(self.reference_price),
            "proposal": {
                "side": proposal.side.value,
                "quantity": _text(proposal.quantity),
                "kind": proposal.kind.value,
                "price": None if proposal.price is None else _text(proposal.price),
                "reducing": proposal.reducing,
                "reason": proposal.reason,
            },
            "anchor": None
            if self.anchor is None
            else {
                "instrument": self.anchor.instrument.key,
                "price": _text(self.anchor.price),
                "event_time_ms": self.anchor.event_time_ms,
                "available_at_ms": self.anchor.available_at_ms,
                "source": self.anchor.source,
            },
            **{
                name: None if getattr(self, name) is None else _text(getattr(self, name))
                for name in ("execution_lower", "execution_upper", "mark_lower", "mark_upper")
            },
        }

    @property
    def sha256(self):
        return _digest(self.document())


def make_cost_envelope(
    account,
    order_id,
    proposal,
    *,
    policy,
    instrument,
    owner_id,
    as_of_ms,
    marks,
    worst_execution_price,
):
    if type(account) is not MarginAccount or type(policy) is not MarginCostPolicy:
        raise ValidationError("cost envelope needs the current typed account/policy")
    if type(proposal) is not OrderProposal or type(instrument) is not Instrument:
        raise ValidationError("cost envelope needs a typed original proposal/instrument")
    _time(as_of_ms)
    if (
        type(marks) is not tuple
        or len(marks) > 100
        or any(type(mark) is not MarkSnapshot for mark in marks)
        or len({mark.instrument for mark in marks}) != len(marks)
    ):
        raise ValidationError("cost envelope needs bounded observed references")
    anchor = next(
        (
            mark
            for mark in marks
            if mark.instrument == instrument
            and mark.available_at_ms <= as_of_ms
            and mark.event_time_ms <= as_of_ms
        ),
        None,
    )
    if anchor is None and not proposal.reducing:
        raise TradingBlocked("cost_entry_requires_observed_anchor")
    reference = _external(worst_execution_price)
    return CostEnvelope(
        policy,
        order_id,
        account.account_id,
        instrument,
        owner_id,
        proposal,
        as_of_ms,
        _account_fingerprint(account),
        reference,
        anchor,
        *_bands(policy, proposal, reference, anchor),
    )


def _valuation(base, envelope):
    return (
        base.valuation_price
        if base.proposal.reducing
        else max(base.valuation_price, envelope.execution_upper, envelope.mark_upper)
    )


def _deltas(base, envelope):
    valuation = _valuation(base, envelope)
    try:
        with localcontext(_MATH):
            notional = base.proposal.quantity * valuation
            initial = ZERO if base.proposal.reducing else notional / base.leverage
            fee = (notional * base.fee_rate).quantize(FEE_QUANTUM, rounding=ROUND_CEILING)
        loss = (
            ZERO
            if base.proposal.reducing
            else _exact(
                lambda: (
                    base.proposal.quantity
                    * max(
                        ZERO,
                        envelope.execution_upper - envelope.mark_lower
                        if base.proposal.side == Side.BUY
                        else envelope.mark_upper - envelope.execution_lower,
                    )
                )
            )
        )
    except DecimalException:
        raise ValidationError("cost reserve exceeds its supported capacity") from None
    return tuple(
        max(ZERO, _sum(actual, original.copy_negate()))
        for actual, original in (
            (initial, base.initial_margin),
            (fee, base.fee_reserve),
            (loss, base.entry_loss),
        )
    )


def _cost_fingerprint(base_fingerprint, envelope, deltas):
    return _digest(
        (
            MODEL,
            "reservation",
            base_fingerprint,
            envelope.sha256,
            tuple(_text(value) for value in deltas),
        )
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class CostMarginReservation(MarginReservation):
    envelope: CostEnvelope
    base_request_fingerprint: str
    extra_initial_margin: Decimal
    extra_fee: Decimal
    extra_entry_loss: Decimal

    def __post_init__(self):
        MarginReservation.__post_init__(self)
        _hash(self.base_request_fingerprint)
        if type(self.envelope) is not CostEnvelope:
            raise ValidationError("cost reservation needs its immutable envelope")
        envelope = self.envelope
        if (
            (self.order_id, self.account_id, self.instrument, self.owner_id, self.proposal)
            != (
                envelope.order_id,
                envelope.account_id,
                envelope.instrument,
                envelope.owner_id,
                envelope.proposal,
            )
            or self.as_of_ms < envelope.issued_at_ms
            or not self.proposal.reducing
            and self.worst_execution_price
            != (
                envelope.execution_upper
                if self.proposal.side == Side.BUY
                else envelope.execution_lower
            )
        ):
            raise ValidationError("cost reservation differs from its original order envelope")
        deltas = (self.extra_initial_margin, self.extra_fee, self.extra_entry_loss)
        for value in deltas:
            _number(value, external=False)
            if type(value) is not Decimal or value < 0:
                raise ValidationError("cost reserve deltas require finite nonnegative Decimals")
        if deltas != _deltas(self, envelope) or self.request_fingerprint != _cost_fingerprint(
            self.base_request_fingerprint, envelope, deltas
        ):
            raise ValidationError(
                "cost reservation delta/fingerprint differs from exact calculation"
            )

    @property
    def total_reserved(self):
        return _sum(
            self.initial_margin,
            self.fee_reserve,
            self.entry_loss,
            self.extra_initial_margin,
            self.extra_fee,
            self.extra_entry_loss,
        )


def cost_reservation(base, envelope):
    if type(base) is not MarginReservation or type(envelope) is not CostEnvelope:
        raise ValidationError("cost conversion needs an exact legacy base and frozen envelope")
    deltas = _deltas(base, envelope)
    data = {field.name: getattr(base, field.name) for field in fields(MarginReservation)}
    data["request_fingerprint"] = _cost_fingerprint(base.request_fingerprint, envelope, deltas)
    return CostMarginReservation(
        **data,
        envelope=envelope,
        base_request_fingerprint=base.request_fingerprint,
        extra_initial_margin=deltas[0],
        extra_fee=deltas[1],
        extra_entry_loss=deltas[2],
    )


def reservation_valuation(value):
    if type(value) is MarginReservation:
        return value.valuation_price
    if type(value) is CostMarginReservation:
        return _valuation(value, value.envelope)
    raise ValidationError("unregistered margin reservation type")


def reservation_fee(value):
    if type(value) is MarginReservation:
        return value.fee_reserve
    if type(value) is CostMarginReservation:
        return _sum(value.fee_reserve, value.extra_fee)
    raise ValidationError("unregistered margin reservation type")


def validate_entry_envelope(reservation, *, mark_price, quote_price=None):
    if type(reservation) is not CostMarginReservation:
        raise ValidationError("entry envelope needs an exact cost reservation")
    if reservation.proposal.reducing:
        return
    envelope = reservation.envelope
    if mark_price is None:
        raise TradingBlocked("cost_mark_unavailable")
    mark_price = _external(mark_price)
    if not envelope.mark_lower <= mark_price <= envelope.mark_upper:
        raise TradingBlocked("cost_mark_bound_exceeded")
    if (
        quote_price is not None
        and not envelope.execution_lower <= _external(quote_price) <= envelope.execution_upper
    ):
        raise TradingBlocked("cost_execution_bound_exceeded")


def _eligibility(reservation):
    envelope = reservation.envelope
    delay = (
        envelope.policy.reduction_delay_ms
        if reservation.proposal.reducing
        else envelope.policy.entry_delay_ms
    )
    eligible = ((envelope.issued_at_ms + delay + 59999) // 60000) * 60000
    _time(eligible)
    return eligible


@dataclass(frozen=True, slots=True)
class FrozenCostPendingOrder(FrozenPendingOrder):
    original: CostMarginReservation

    def __post_init__(self):
        if type(self.original) is not CostMarginReservation:
            raise ValidationError("cost pending cannot hide a legacy/unregistered reservation")
        FrozenPendingOrder.__post_init__(self)
        if (
            self.original.as_of_ms != self.original.envelope.issued_at_ms
            or self.original.account_fingerprint
            != self.original.envelope.anchor_account_fingerprint
            or self.eligible_at_ms != _eligibility(self.original)
        ):
            raise ValidationError("cost pending original prefix/issue/eligibility differs")


def freeze_cost_pending(reservation):
    if type(reservation) is not CostMarginReservation:
        raise ValidationError("cost pending needs its actual original cost reservation")
    return FrozenCostPendingOrder(reservation, _eligibility(reservation))


@dataclass(frozen=True, slots=True, kw_only=True)
class CostBarFillQuote(BarFillQuote):
    raw_price: Decimal
    slippage_rate: Decimal
    limit_capped: bool
    policy_sha256: str
    model: str = QUOTE_MODEL

    def __post_init__(self):
        if type(self.phase) is not str or type(self.model) is not str:
            raise ValidationError("cost quote phase/model must be native strings")
        _external(self.price)
        _external(self.raw_price)
        # Validate the shared raw phase/time shape without passing the distinct
        # cost-model tag through the legacy-only base validator.
        BarFillQuote(self.price, self.event_time_ms, self.phase, RAW_QUOTE_MODEL)
        slippage = _external(self.slippage_rate)
        _hash(self.policy_sha256)
        if (
            self.model != QUOTE_MODEL
            or type(self.model) is not str
            or self.raw_price <= 0
            or not ZERO <= slippage <= Decimal(".25")
            or type(self.limit_capped) is not bool
        ):
            raise ValidationError("invalid explicit cost quote")


def cost_quote(cost_pending, raw_quote):
    if type(cost_pending) is not FrozenCostPendingOrder or type(raw_quote) is not BarFillQuote:
        raise ValidationError("cost quote requires exact original pending and resolved raw quote")
    if raw_quote.event_time_ms < cost_pending.eligible_at_ms:
        raise ValidationError("cost quote precedes the original delayed eligibility")
    original = cost_pending.original
    policy, proposal = original.envelope.policy, original.proposal
    price = _exact(
        lambda: (
            raw_quote.price
            * (
                ONE + policy.slippage_rate
                if proposal.side == Side.BUY
                else ONE - policy.slippage_rate
            )
        )
    )
    final = (
        price
        if proposal.price is None
        else (
            min(price, proposal.price) if proposal.side == Side.BUY else max(price, proposal.price)
        )
    )
    return CostBarFillQuote(
        price=_external(final),
        event_time_ms=raw_quote.event_time_ms,
        phase=raw_quote.phase,
        raw_price=raw_quote.price,
        slippage_rate=policy.slippage_rate,
        limit_capped=final != price,
        policy_sha256=policy.sha256,
    )


@dataclass(frozen=True, slots=True)
class CostPendingFillAdmission(PendingFillAdmission):
    quote: CostBarFillQuote | None
    reservation: CostMarginReservation | None

    def __post_init__(self):
        _id(self.order_id)
        _hash(self.original_request_fingerprint)
        _number(self.fee_adjustment, external=False)
        if (
            type(self.outcome) is not str
            or self.outcome
            not in {
                "already_settled",
                "waiting_eligible",
                "await_close",
                "not_touched",
                "deferred",
                "open_phase_required",
                "rejected",
                "fill",
            }
            or self.fee_adjustment < 0
            or self.reason is not None
            and (
                type(self.reason) is not str or re.fullmatch(r"[a-z_]{1,128}", self.reason) is None
            )
            or self.quote is not None
            and type(self.quote) is not CostBarFillQuote
            or self.reservation is not None
            and type(self.reservation) is not CostMarginReservation
        ):
            raise ValidationError("invalid exact cost admission")


@dataclass(frozen=True, slots=True)
class PreparedCostMarginBatch(PreparedMarginBatch):
    original_pending: tuple[FrozenPendingOrder | FrozenCostPendingOrder, ...]
    admissions: tuple[PendingFillAdmission | CostPendingFillAdmission, ...]
    remaining: tuple[FrozenPendingOrder | FrozenCostPendingOrder, ...]
    model: str = MODEL

    def __post_init__(self):
        if (
            self.model != MODEL
            or type(self.model) is not str
            or type(self.original_pending) is not tuple
            or not 1 <= len(self.original_pending) <= 100
            or any(
                type(row) not in {FrozenPendingOrder, FrozenCostPendingOrder}
                for row in self.original_pending
            )
            or not any(type(row) is FrozenCostPendingOrder for row in self.original_pending)
            or type(self.remaining) is not tuple
            or len(self.remaining) > 100
            or any(
                type(row) not in {FrozenPendingOrder, FrozenCostPendingOrder}
                for row in self.remaining
            )
            or type(self.admissions) is not tuple
            or len(self.admissions) > 100
            or any(
                type(row) not in {PendingFillAdmission, CostPendingFillAdmission}
                for row in self.admissions
            )
        ):
            raise ValidationError("cost batch must contain known immutable cost/legacy orders")
        _time(self.as_of_ms)
        if type(self.prefix_checksums) is not tuple or not 1 <= len(self.prefix_checksums) <= 101:
            raise ValidationError("cost batch needs bounded exact prefix checksums")
        for value in (self.batch_id, self.checksum, *self.prefix_checksums):
            _hash(value)
        if (
            type(self.phase) is not str
            or self.phase not in {"open", "close"}
            or type(self.fee_quantum) is not Decimal
            or self.fee_quantum != FEE_QUANTUM
            or type(self.candles) is not tuple
            or not 1 <= len(self.candles) <= 100
            or any(type(value) is not Candle for value in self.candles)
            or type(self.brackets) is not tuple
            or len(self.brackets) > 100
            or any(type(value) is not MarginBracketSnapshot for value in self.brackets)
            or type(self.facts) is not tuple
            or len(self.facts) > 100
            or any(type(value) is not FillEvent for value in self.facts)
            or type(self.prefix_accounts) is not tuple
            or len(self.prefix_accounts) != len(self.facts) + 1
            or any(type(value) is not MarginAccount for value in self.prefix_accounts)
            or type(self.prefix_checksums) is not tuple
            or len(self.prefix_checksums) != len(self.prefix_accounts)
            or type(self.rejections) is not tuple
            or len(self.rejections) > 100
        ):
            raise ValidationError("invalid bounded immutable cost batch")
        original = {row.order_id: row for row in self.original_pending}
        if len(original) != len(self.original_pending):
            raise ValidationError("duplicate original cost batch order")
        for row in self.remaining:
            if original.get(row.order_id) != row:
                raise ValidationError("cost batch changed a remaining original order")
        for row in self.admissions:
            order = original.get(row.order_id)
            expected = (
                CostPendingFillAdmission
                if type(order) is FrozenCostPendingOrder
                else PendingFillAdmission
            )
            if order is None or type(row) is not expected:
                raise ValidationError("cost admission type does not match its original order")


def approve_cost_order(
    account,
    order_id,
    proposal,
    *,
    policy,
    instrument,
    owner_id,
    as_of_ms,
    marks,
    brackets,
    worst_execution_price,
    fee_rate,
    reservations=(),
    max_mark_age_ms=120000,
    max_bracket_age_ms=86400000,
):
    # Called only after the base modules/types have loaded; no import-time work.
    from omega.application.margin_risk import approve_order

    envelope = make_cost_envelope(
        account,
        order_id,
        proposal,
        policy=policy,
        instrument=instrument,
        owner_id=owner_id,
        as_of_ms=as_of_ms,
        marks=marks,
        worst_execution_price=worst_execution_price,
    )
    bound = (
        envelope.reference_price
        if proposal.reducing
        else (envelope.execution_upper if proposal.side == Side.BUY else envelope.execution_lower)
    )
    return approve_order(
        account,
        order_id,
        proposal,
        instrument=instrument,
        owner_id=owner_id,
        as_of_ms=as_of_ms,
        marks=marks,
        brackets=brackets,
        worst_execution_price=bound,
        fee_rate=fee_rate,
        reservations=reservations,
        max_mark_age_ms=max_mark_age_ms,
        max_bracket_age_ms=max_bracket_age_ms,
        cost_envelope=envelope,
    )
