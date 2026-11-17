"""Opt-in linear USDT accounting primitives, independent of paper v1 and risk approval.

Rate-derived scenario funding is explicit; no venue payment authority,
liquidation execution, order reservation or liquidity model is supplied.
Computed prices use fixed 80-digit HALF_EVEN arithmetic; cash additions use 256
digits to retain their finite amounts. Settlement quantization is not performed.
"""

# Imports
import re
from dataclasses import dataclass, replace
from decimal import ROUND_HALF_EVEN, Context, Decimal, DecimalException, Inexact, localcontext
from enum import StrEnum

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Instrument, Market, require_decimal

# Constants
ZERO = Decimal(0)
MAX_TIME = 253_402_300_799_000
MAX_POSITIONS = 100
MAX_EVENTS = 10_000
_MATH = Context(prec=80, rounding=ROUND_HALF_EVEN)
_CASH = Context(prec=256, rounding=ROUND_HALF_EVEN)
FUNDING_QUANTUM = Decimal(".00000001")


def _number(value, *, external=False):
    require_decimal(value)
    digits, exponent = len(value.as_tuple().digits), value.as_tuple().exponent
    bound = 18 if external else 256
    if digits > bound or not -bound <= exponent <= bound:
        raise ValidationError("margin Decimal exceeds its calculation bound")


def _sum(*values):
    try:
        with localcontext(_CASH) as context:
            context.traps[Inexact] = True
            result = sum(values, ZERO)
    except DecimalException:
        raise ValidationError("margin cash/quantity addition exceeds its exact bound") from None
    _number(result)
    return result


def _id(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValidationError("invalid margin ledger identity")


def _time(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("invalid margin observation time")


def _instrument(value):
    if (
        not isinstance(value, Instrument)
        or value.provider != "binance"
        or value.market != Market.USDM
        or not value.symbol.endswith("USDT")
    ):
        raise ValidationError("margin v2 only supports Binance linear USDT USDM")


class MarginMode(StrEnum):
    CROSS = "cross"
    ISOLATED = "isolated"


@dataclass(frozen=True, slots=True)
class MarginPosition:
    instrument: Instrument
    owner_id: str
    margin_mode: MarginMode
    leverage: int
    budget_margin: Decimal
    quantity: Decimal = ZERO
    entry_price: Decimal = ZERO
    isolated_wallet: Decimal = ZERO
    realized_pnl: Decimal = ZERO

    def __post_init__(self):
        _instrument(self.instrument)
        _id(self.owner_id)
        for value in (
            self.budget_margin,
            self.quantity,
            self.entry_price,
            self.isolated_wallet,
            self.realized_pnl,
        ):
            _number(value)
        quantity_parts, entry_parts = self.quantity.as_tuple(), self.entry_price.as_tuple()
        if (
            not isinstance(self.margin_mode, MarginMode)
            or type(self.leverage) is not int
            or not 1 <= self.leverage <= 1000
            or self.budget_margin < 0
            or self.entry_price < 0
            or len(quantity_parts.digits) > 60
            or not -18 <= quantity_parts.exponent <= 18
            or len(entry_parts.digits) > 80
            or not -100 <= entry_parts.exponent <= 18
            or (self.quantity == 0) != (self.entry_price == 0)
            or self.margin_mode == MarginMode.CROSS
            and self.isolated_wallet != 0
        ):
            raise ValidationError("invalid margin position")


@dataclass(frozen=True, slots=True)
class FillEvent:
    fill_id: str
    order_id: str
    instrument: Instrument
    owner_id: str
    delta_qty: Decimal
    price: Decimal
    fee: Decimal
    fee_asset: str
    event_time_ms: int
    available_at_ms: int

    def __post_init__(self):
        for value in (self.fill_id, self.order_id, self.owner_id):
            _id(value)
        _instrument(self.instrument)
        for value in (self.delta_qty, self.price, self.fee):
            _number(value, external=True)
        _time(self.event_time_ms)
        _time(self.available_at_ms)
        if (
            self.delta_qty == 0
            or self.price <= 0
            or self.fee < 0
            or self.fee_asset != "USDT"
            or self.event_time_ms > self.available_at_ms
        ):
            raise ValidationError("invalid signed margin fill")

    @property
    def event_id(self):
        return self.fill_id


@dataclass(frozen=True, slots=True)
class CollateralTransfer:
    transfer_id: str
    instrument: Instrument
    owner_id: str
    amount: Decimal  # Positive is cross -> isolated. Never an automatic top-up.
    event_time_ms: int
    available_at_ms: int

    def __post_init__(self):
        _id(self.transfer_id)
        _id(self.owner_id)
        _instrument(self.instrument)
        _number(self.amount, external=True)
        _time(self.event_time_ms)
        _time(self.available_at_ms)
        if self.amount == 0 or self.event_time_ms > self.available_at_ms:
            raise ValidationError("invalid explicit collateral transfer")

    @property
    def event_id(self):
        return self.transfer_id


def funding_amount(quantity: Decimal, reference_price: Decimal, rate: Decimal) -> Decimal:
    """Pinned linear USDT scenario policy, independent of ambient Decimal context."""
    _number(quantity)
    _number(reference_price, external=True)
    _number(rate, external=True)
    parts = quantity.as_tuple()
    if (
        len(parts.digits) > 60
        or not -18 <= parts.exponent <= 18
        or reference_price <= 0
        or rate.copy_abs() > 1
    ):
        raise ValidationError("invalid bounded scenario funding inputs")
    try:
        with localcontext(_CASH) as context:
            context.traps[Inexact] = True
            exact = quantity.copy_negate() * reference_price * rate
            context.traps[Inexact] = False
            amount = exact.quantize(FUNDING_QUANTUM, rounding=ROUND_HALF_EVEN)
    except DecimalException:
        raise ValidationError("scenario funding amount exceeds its exact bound") from None
    _number(amount)
    return amount


@dataclass(frozen=True, slots=True, kw_only=True)
class FundingCashflow:
    """Calculated virtual cashflow; never a declared actual venue account payment."""

    funding_id: str
    instrument: Instrument
    owner_id: str
    amount: Decimal
    rate: Decimal
    reference_price: Decimal
    signed_quantity: Decimal
    event_time_ms: int
    source_available_at_ms: int
    available_at_ms: int  # Original account-effective batch cutoff, not a source timestamp.
    source_kind: str
    source_sha256: str
    policy_sha256: str
    settlement_asset: str = "USDT"
    quantum: Decimal = FUNDING_QUANTUM

    def __post_init__(self):
        _id(self.funding_id)
        _id(self.owner_id)
        _instrument(self.instrument)
        _number(self.amount)
        _number(self.quantum)
        for value in (self.event_time_ms, self.source_available_at_ms, self.available_at_ms):
            _time(value)
        if (
            type(self.source_kind) is not str
            or self.source_kind != "scenario_rate"
            or self.settlement_asset != "USDT"
            or self.quantum != FUNDING_QUANTUM
            or self.event_time_ms % 60000
            or not self.event_time_ms <= self.source_available_at_ms <= self.available_at_ms
            or any(
                type(value) is not str or re.fullmatch("[a-f0-9]{64}", value) is None
                for value in (self.source_sha256, self.policy_sha256)
            )
            or self.amount != funding_amount(self.signed_quantity, self.reference_price, self.rate)
        ):
            raise ValidationError("invalid calculated scenario funding fact")

    @property
    def event_id(self):
        return self.funding_id


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    sequence: int
    fact: FillEvent | CollateralTransfer | FundingCashflow
    realized_pnl: Decimal
    fee: Decimal
    cross_delta: Decimal
    isolated_delta: Decimal

    def __post_init__(self):
        if type(self.sequence) is not int or not 1 <= self.sequence <= MAX_EVENTS:
            raise ValidationError("invalid margin ledger sequence")
        if not isinstance(self.fact, (FillEvent, CollateralTransfer, FundingCashflow)):
            raise ValidationError("typed margin ledger fact required")
        for value in (self.realized_pnl, self.fee, self.cross_delta, self.isolated_delta):
            _number(value)
        if (
            self.fee < 0
            or _sum(self.cross_delta, self.isolated_delta)
            != _sum(
                self.realized_pnl,
                self.fee.copy_negate(),
                self.fact.amount if isinstance(self.fact, FundingCashflow) else ZERO,
            )
            or isinstance(self.fact, FillEvent)
            and self.fee != self.fact.fee
            or isinstance(self.fact, FundingCashflow)
            and (self.realized_pnl != 0 or self.fee != 0)
            or isinstance(self.fact, CollateralTransfer)
            and (
                self.realized_pnl != 0
                or self.fee != 0
                or self.cross_delta != self.fact.amount.copy_negate()
                or self.isolated_delta != self.fact.amount
            )
        ):
            raise ValidationError("margin ledger cash conservation failed")


@dataclass(frozen=True, slots=True)
class MarginAccount:
    account_id: str
    settlement_asset: str
    cross_wallet: Decimal
    positions: tuple[MarginPosition, ...] = ()
    ledger: tuple[LedgerEntry, ...] = ()
    event_capacity: int = MAX_EVENTS
    opening_balance: Decimal | None = None

    def __post_init__(self):
        _id(self.account_id)
        _number(self.cross_wallet)
        if (
            self.settlement_asset != "USDT"
            or type(self.positions) is not tuple
            or len(self.positions) > MAX_POSITIONS
            or any(not isinstance(value, MarginPosition) for value in self.positions)
            or len({value.instrument for value in self.positions}) != len(self.positions)
            or type(self.ledger) is not tuple
            or type(self.event_capacity) is not int
            or not 1 <= self.event_capacity <= MAX_EVENTS
            or len(self.ledger) > self.event_capacity
            or any(
                not isinstance(value, LedgerEntry) or value.sequence != index
                for index, value in enumerate(self.ledger, 1)
            )
            or len({entry.fact.event_id for entry in self.ledger}) != len(self.ledger)
        ):
            raise ValidationError("invalid or oversized one-way margin account")
        owners = {value.instrument: value for value in self.positions}
        previous_times = {}
        for entry in self.ledger:
            owner = owners.get(entry.fact.instrument)
            if owner is None or owner.owner_id != entry.fact.owner_id:
                raise ValidationError("margin ledger owner is absent from account")
            previous = previous_times.get(entry.fact.instrument)
            current = (entry.fact.event_time_ms, entry.fact.available_at_ms)
            if previous is not None and (current[0] < previous[0] or current[1] < previous[1]):
                raise ValidationError("margin ledger fact ordering is inconsistent")
            previous_times[entry.fact.instrument] = current
            if (
                isinstance(entry.fact, (FillEvent, FundingCashflow))
                and (
                    owner.margin_mode == MarginMode.CROSS
                    and entry.isolated_delta != 0
                    or owner.margin_mode == MarginMode.ISOLATED
                    and entry.cross_delta != 0
                )
                or isinstance(entry.fact, CollateralTransfer)
                and owner.margin_mode != MarginMode.ISOLATED
            ):
                raise ValidationError("margin ledger cash was assigned to the wrong wallet")
        for position in self.positions:
            if position.realized_pnl != _sum(
                *(
                    entry.realized_pnl
                    for entry in self.ledger
                    if entry.fact.instrument == position.instrument
                )
            ):
                raise ValidationError("margin position realized total differs from ledger")
        total = _sum(self.cross_wallet, *(value.isolated_wallet for value in self.positions))
        if self.opening_balance is None:
            if self.ledger:
                raise ValidationError("restored margin ledger requires its opening balance")
            object.__setattr__(self, "opening_balance", total)
        _number(self.opening_balance)
        expected = _sum(
            self.opening_balance,
            *(entry.realized_pnl for entry in self.ledger),
            *(entry.fee.copy_negate() for entry in self.ledger),
            *(
                entry.fact.amount
                for entry in self.ledger
                if isinstance(entry.fact, FundingCashflow)
            ),
        )
        if total != expected:
            raise ValidationError("margin account cash conservation failed")


@dataclass(frozen=True, slots=True)
class MarginTransition:
    account: MarginAccount
    entry: LedgerEntry | None  # None is an already-applied identical typed fact.


def _prepare(account, fact):
    if not isinstance(account, MarginAccount):
        raise ValidationError("typed margin account required")
    for entry in account.ledger:
        if entry.fact.event_id == fact.event_id:
            if entry.fact != fact:
                raise ProtocolError("margin event ID conflicts with its original fact")
            return None
    if len(account.ledger) >= account.event_capacity:
        raise ProtocolError("margin ledger capacity reached; preserve new facts externally")
    position = next(
        (value for value in account.positions if value.instrument == fact.instrument), None
    )
    if position is None or position.owner_id != fact.owner_id:
        raise ProtocolError("margin instrument belongs to another or absent strategy owner")
    prior = [entry.fact for entry in account.ledger if entry.fact.instrument == fact.instrument]
    if prior and (
        fact.event_time_ms < prior[-1].event_time_ms
        or fact.available_at_ms < prior[-1].available_at_ms
    ):
        raise ProtocolError("late margin facts require explicit ordered replay")
    return position


def _commit(account, position, updated, fact, realized, fee, cross_delta, isolated_delta):
    entry = LedgerEntry(len(account.ledger) + 1, fact, realized, fee, cross_delta, isolated_delta)
    candidate = replace(
        account,
        cross_wallet=_sum(account.cross_wallet, cross_delta),
        positions=tuple(updated if value is position else value for value in account.positions),
        ledger=account.ledger + (entry,),
    )
    return MarginTransition(candidate, entry)


def apply_fill(account: MarginAccount, fact: FillEvent) -> MarginTransition:
    """Record actual fill facts, including debt. This is not a risk-approval function."""
    if not isinstance(fact, FillEvent):
        raise ValidationError("typed margin fill required")
    position = _prepare(account, fact)
    if position is None:
        return MarginTransition(account, None)
    with localcontext(_MATH):
        old, delta = position.quantity, fact.delta_qty
        quantity = _sum(old, delta)
        opposite = old != 0 and (old < 0) != (delta < 0)
        closed = min(abs(old), abs(delta)) if opposite else ZERO
        realized = closed * (fact.price - position.entry_price) * (-1 if old < 0 else 1)
        if quantity == 0:
            entry_price = ZERO
        elif old == 0 or opposite and abs(delta) > abs(old):
            entry_price = fact.price
        elif opposite:
            entry_price = position.entry_price
        else:
            entry_price = (abs(old) * position.entry_price + abs(delta) * fact.price) / abs(
                quantity
            )
    cash_delta = _sum(realized, fact.fee.copy_negate())
    isolated = position.margin_mode == MarginMode.ISOLATED
    updated = replace(
        position,
        quantity=quantity,
        entry_price=entry_price,
        isolated_wallet=_sum(position.isolated_wallet, cash_delta) if isolated else ZERO,
        realized_pnl=_sum(position.realized_pnl, realized),
    )
    return _commit(
        account,
        position,
        updated,
        fact,
        realized,
        fact.fee,
        ZERO if isolated else cash_delta,
        cash_delta if isolated else ZERO,
    )


def apply_transfer(account: MarginAccount, fact: CollateralTransfer) -> MarginTransition:
    """Move explicitly requested cash; caller must separately approve margin risk."""
    if not isinstance(fact, CollateralTransfer):
        raise ValidationError("typed collateral transfer required")
    position = _prepare(account, fact)
    if position is None:
        return MarginTransition(account, None)
    if position.margin_mode != MarginMode.ISOLATED:
        raise ValidationError("collateral transfers require an isolated position")
    if (
        fact.amount > 0
        and account.cross_wallet < fact.amount
        or fact.amount < 0
        and position.isolated_wallet < fact.amount.copy_negate()
    ):
        raise ProtocolError("collateral source wallet has insufficient cash")
    updated = replace(position, isolated_wallet=_sum(position.isolated_wallet, fact.amount))
    return _commit(
        account, position, updated, fact, ZERO, ZERO, fact.amount.copy_negate(), fact.amount
    )


def apply_funding(account: MarginAccount, fact: FundingCashflow) -> MarginTransition:
    """Apply an explicitly calculated cashflow, preserving debt and trade PnL."""
    if not isinstance(fact, FundingCashflow):
        raise ValidationError("typed calculated funding fact required")
    position = _prepare(account, fact)  # Identical retries precede current-quantity validation.
    if position is None:
        return MarginTransition(account, None)
    if position.quantity != fact.signed_quantity:
        raise ProtocolError("funding evidence differs from the original settlement quantity")
    isolated = position.margin_mode == MarginMode.ISOLATED
    updated = (
        replace(position, isolated_wallet=_sum(position.isolated_wallet, fact.amount))
        if isolated
        else position
    )
    return _commit(
        account,
        position,
        updated,
        fact,
        ZERO,
        ZERO,
        ZERO if isolated else fact.amount,
        fact.amount if isolated else ZERO,
    )


@dataclass(frozen=True, slots=True)
class MarginBracket:
    notional_floor: Decimal
    notional_cap: Decimal
    maintenance_rate: Decimal
    maintenance_amount: Decimal
    max_leverage: int

    def __post_init__(self):
        for value in (
            self.notional_floor,
            self.notional_cap,
            self.maintenance_rate,
            self.maintenance_amount,
        ):
            _number(value, external=True)
        if (
            not ZERO <= self.notional_floor < self.notional_cap
            or not ZERO <= self.maintenance_rate < 1
            or self.maintenance_amount < 0
            or type(self.max_leverage) is not int
            or not 1 <= self.max_leverage <= 1000
        ):
            raise ValidationError("invalid margin bracket")
        with localcontext(_MATH):
            if self.notional_floor * self.maintenance_rate < self.maintenance_amount:
                raise ValidationError("margin bracket has negative maintenance")


@dataclass(frozen=True, slots=True)
class MarginBracketSnapshot:
    instrument: Instrument
    rows: tuple[MarginBracket, ...]
    observed_at_ms: int
    available_at_ms: int
    source: str
    checksum: str

    def __post_init__(self):
        _instrument(self.instrument)
        _time(self.observed_at_ms)
        _time(self.available_at_ms)
        if (
            type(self.rows) is not tuple
            or not 1 <= len(self.rows) <= 256
            or any(not isinstance(row, MarginBracket) for row in self.rows)
            or self.observed_at_ms > self.available_at_ms
            or type(self.source) is not str
            or not 1 <= len(self.source) <= 96
            or not self.source.isprintable()
            or type(self.checksum) is not str
            or re.fullmatch("[a-f0-9]{64}", self.checksum) is None
            or self.rows[0].notional_floor != 0
        ):
            raise ValidationError("invalid margin bracket snapshot")
        with localcontext(_MATH):
            for previous, current in zip(self.rows, self.rows[1:], strict=False):
                if (
                    previous.notional_cap != current.notional_floor
                    or previous.maintenance_rate > current.maintenance_rate
                    or previous.notional_cap * previous.maintenance_rate
                    - previous.maintenance_amount
                    != current.notional_floor * current.maintenance_rate
                    - current.maintenance_amount
                ):
                    raise ValidationError("margin brackets must be contiguous and continuous")


@dataclass(frozen=True, slots=True)
class MarkSnapshot:
    instrument: Instrument
    price: Decimal
    event_time_ms: int
    available_at_ms: int
    source: str

    def __post_init__(self):
        _instrument(self.instrument)
        _number(self.price, external=True)
        _time(self.event_time_ms)
        _time(self.available_at_ms)
        if (
            self.price <= 0
            or self.event_time_ms > self.available_at_ms
            or type(self.source) is not str
            or not 1 <= len(self.source) <= 96
            or not self.source.isprintable()
        ):
            raise ValidationError("invalid margin mark snapshot")


@dataclass(frozen=True, slots=True)
class PositionMargin:
    instrument: Instrument
    ready: bool
    reason: str | None
    initial_margin: Decimal | None
    maintenance_margin: Decimal | None
    unrealized_pnl: Decimal | None
    equity: Decimal | None
    free_collateral: Decimal | None
    budget_remaining: Decimal | None
    bracket_checksum: str | None = None


@dataclass(frozen=True, slots=True)
class AccountMargin:
    ready: bool
    reason: str | None
    wallet_balance: Decimal
    equity: Decimal | None
    initial_margin: Decimal | None
    maintenance_margin: Decimal | None
    unrealized_pnl: Decimal | None
    cross_free_collateral: Decimal | None
    positions: tuple[PositionMargin, ...]


def evaluate_margin(
    account, marks, brackets, *, as_of_ms, max_mark_age_ms=120_000, max_bracket_age_ms=86_400_000
):
    """Evaluate existing exposures only; ready does not approve future orders.

    Flat positions need no mark/bracket for their zero IM/MM. Entry risk must
    separately validate proposed notional, reserves and bracket leverage.
    Isolated free funds never become cross collateral.
    """
    if not isinstance(account, MarginAccount):
        raise ValidationError("typed margin account required")
    _time(as_of_ms)
    for age in (max_mark_age_ms, max_bracket_age_ms):
        if type(age) is not int or not 0 <= age <= 30 * 86_400_000:
            raise ValidationError("invalid bounded margin freshness policy")
    for values, kind in ((marks, MarkSnapshot), (brackets, MarginBracketSnapshot)):
        if (
            type(values) is not tuple
            or len(values) > MAX_POSITIONS
            or any(not isinstance(value, kind) for value in values)
            or len({value.instrument for value in values}) != len(values)
        ):
            raise ValidationError("invalid or duplicate margin snapshot inputs")
    mark_map, bracket_map = (
        {value.instrument: value for value in values} for values in (marks, brackets)
    )
    outputs = []
    with localcontext(_MATH):
        for position in account.positions:
            mark, snapshot = mark_map.get(position.instrument), bracket_map.get(position.instrument)
            reason, row = None, None
            if position.quantity != 0:
                if mark is None:
                    reason = "mark_missing"
                elif mark.available_at_ms > as_of_ms or mark.event_time_ms > as_of_ms:
                    reason = "mark_unavailable_at_cutoff"
                elif as_of_ms - mark.event_time_ms > max_mark_age_ms:
                    reason = "mark_stale"
                elif snapshot is None:
                    reason = "bracket_missing"
                elif snapshot.available_at_ms > as_of_ms or snapshot.observed_at_ms > as_of_ms:
                    reason = "bracket_unavailable_at_cutoff"
                elif as_of_ms - snapshot.observed_at_ms > max_bracket_age_ms:
                    reason = "bracket_stale"
                else:
                    notional = abs(position.quantity) * mark.price
                    row = next(
                        (
                            value
                            for value in snapshot.rows
                            if value.notional_floor <= notional < value.notional_cap
                        ),
                        None,
                    )
                    if row is None:
                        reason = "bracket_range_unsupported"
                    elif position.leverage > row.max_leverage:
                        reason = "leverage_exceeds_bracket"
            if reason:
                outputs.append(
                    PositionMargin(
                        position.instrument, False, reason, None, None, None, None, None, None
                    )
                )
                continue
            initial = (
                abs(position.quantity) * mark.price / position.leverage
                if position.quantity
                else ZERO
            )
            maintenance = notional * row.maintenance_rate - row.maintenance_amount if row else ZERO
            unrealized = (
                position.quantity * (mark.price - position.entry_price)
                if position.quantity
                else ZERO
            )
            isolated = position.margin_mode == MarginMode.ISOLATED
            equity = _sum(position.isolated_wallet, unrealized) if isolated else None
            outputs.append(
                PositionMargin(
                    position.instrument,
                    True,
                    None,
                    initial,
                    maintenance,
                    unrealized,
                    equity,
                    _sum(equity, -initial) if isolated else None,
                    _sum(position.budget_margin, -initial),
                    snapshot.checksum if row else None,
                )
            )
        wallet = _sum(account.cross_wallet, *(value.isolated_wallet for value in account.positions))
        if any(not output.ready for output in outputs):
            return AccountMargin(
                False,
                "position_inputs_unavailable",
                wallet,
                None,
                None,
                None,
                None,
                None,
                tuple(outputs),
            )
        unrealized = _sum(*(value.unrealized_pnl for value in outputs))
        initial = _sum(*(value.initial_margin for value in outputs))
        maintenance = _sum(*(value.maintenance_margin for value in outputs))
        crossed = [
            (position, output)
            for position, output in zip(account.positions, outputs, strict=True)
            if position.margin_mode == MarginMode.CROSS
        ]
        cross_free = _sum(
            account.cross_wallet,
            *(output.unrealized_pnl for _, output in crossed),
            *(-output.initial_margin for _, output in crossed),
        )
        return AccountMargin(
            True,
            None,
            wallet,
            _sum(wallet, unrealized),
            initial,
            maintenance,
            unrealized,
            cross_free,
            tuple(outputs),
        )
