"""Single-owner cash accounting and shared reservations, without I/O."""

from dataclasses import dataclass, field
from decimal import ROUND_HALF_EVEN, Context, Decimal, DecimalException, localcontext
from fractions import Fraction

from omega.domain.errors import ValidationError
from omega.domain.models import Fill, Instrument, Market, Side, decimal

ZERO = Decimal(0)


@dataclass
class CashLedger:
    account_id: str
    instrument: Instrument
    quote_asset: str
    cash: Decimal
    base_asset: str | None = None
    quantity: Decimal = ZERO
    book_cost: Decimal = ZERO
    realized_gross_pnl: Decimal = ZERO
    fees: dict[str, Decimal] = field(default_factory=dict)
    base_fee_cost_in_quote: Decimal = ZERO
    _fills: dict[tuple[str, str, str], Fill] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        if self.instrument.market != Market.SPOT or decimal(self.cash) < 0:
            raise ValidationError("cash ledger requires spot and nonnegative initial cash")

    def apply(self, fill: Fill) -> bool:
        if fill.account_id != self.account_id or fill.instrument != self.instrument:
            raise ValidationError("fill belongs to another account/instrument")
        previous = self._fills.get(fill.key)
        if previous is not None:
            if previous != fill:
                raise ValidationError("same trade ID has conflicting financial contents")
            return False
        if fill.realized_pnl is not None:
            raise ValidationError("spot cash ledger cannot apply reported futures PnL")
        if fill.quote_quantity is not None:
            return self._apply_reported(fill)
        cost = fill.quantity * fill.price
        quote_fee = fill.fee if fill.fee_asset == self.quote_asset else ZERO
        if fill.fee_asset != self.quote_asset and self.base_asset is None:
            raise ValidationError("base asset required to classify non-quote commission")
        base_fee = fill.fee if fill.fee_asset == self.base_asset else ZERO
        # Third-asset fees remain in their own units and need a separate wallet/valuation.
        if fill.side == Side.BUY:
            acquired = fill.quantity - base_fee
            if acquired < 0:
                raise ValidationError("base commission exceeds acquisition")
            self.cash -= cost + quote_fee
            self.quantity += acquired
            self.book_cost += acquired * fill.price
            self.base_fee_cost_in_quote += base_fee * fill.price
        else:
            removed = fill.quantity + base_fee
            if removed > self.quantity or removed < 0 or not self.quantity:
                raise ValidationError("spot sell exceeds reconciled inventory")
            unit_cost = self.book_cost / self.quantity
            removed_cost = unit_cost * removed
            self.realized_gross_pnl += cost - unit_cost * fill.quantity
            self.base_fee_cost_in_quote += unit_cost * base_fee
            self.book_cost -= removed_cost
            self.quantity -= removed
            self.cash += cost - quote_fee
        self.fees[fill.fee_asset] = self.fees.get(fill.fee_asset, ZERO) + fill.fee
        self._fills[fill.key] = fill
        return True

    def _apply_reported(self, fill):
        """Atomic reported-gross-quote path; no provider settlement/PnL inference.

        Proportional inventory valuation divides with 256 significant digits,
        HALF_EVEN. Subsequent wallet/inventory/fee and linear valuation sums use
        exact finite decimal construction, bounded to 512 digits and exponent
        +/-512; an excess or nonterminating result rejects before any commit.
        Base-commission valuation receives the residual between gross and retained
        cost (buy), or removed and sold cost (sell). No global context is changed.
        """
        from dataclasses import replace

        fill = replace(fill)  # Revalidate new optional controls even after unsafe construction.
        if fill.fee_asset != self.quote_asset and self.base_asset is None:
            raise ValidationError("base asset required to classify non-quote commission")

        def rational(value):
            if type(value) is not Decimal or not value.is_finite():
                raise ValidationError("reported cash accounting requires finite native Decimals")
            parts = value.as_tuple()
            if len(parts.digits) > 512 or not -512 <= parts.exponent <= 512:
                raise ValidationError("reported cash accounting exceeds bounded numeric capacity")
            return Fraction(value)

        def value(number, *, exact=True):
            if exact:
                if not number:
                    return Decimal(0)
                denominator = number.denominator
                powers = []
                for factor in (2, 5):
                    count = 0
                    while denominator % factor == 0:
                        denominator //= factor
                        count += 1
                        if count > 512:
                            raise ValidationError(
                                "reported cash update exceeds exact numeric capacity"
                            )
                    powers.append(count)
                if denominator != 1:
                    raise ValidationError("reported cash update is not a finite decimal")
                scale = max(powers)
                coefficient = (
                    abs(number.numerator) * 2 ** (scale - powers[0]) * 5 ** (scale - powers[1])
                )
                digits = str(coefficient)
                trimmed = digits.rstrip("0")
                exponent = -scale + len(digits) - len(trimmed)
                if len(trimmed) > 512 or not -512 <= exponent <= 512:
                    raise ValidationError("reported cash update exceeds exact numeric capacity")
                return Decimal((int(number < 0), tuple(map(int, trimmed)), exponent))
            try:
                result = Decimal(number.numerator) / Decimal(number.denominator)
            except DecimalException:
                raise ValidationError("reported cash candidate cannot be represented") from None
            if not result.is_finite():
                raise ValidationError("reported cash update exceeds exact numeric capacity")
            return result

        with localcontext(Context(prec=256, rounding=ROUND_HALF_EVEN)):
            cash, quantity, basis, realized, base_cost = (
                rational(getattr(self, name))
                for name in (
                    "cash",
                    "quantity",
                    "book_cost",
                    "realized_gross_pnl",
                    "base_fee_cost_in_quote",
                )
            )
            gross, units, fee = (
                rational(item) for item in (fill.quote_quantity, fill.quantity, fill.fee)
            )
            quote_fee = fee if fill.fee_asset == self.quote_asset else Fraction()
            base_fee = fee if fill.fee_asset == self.base_asset else Fraction()
            if fill.side == Side.BUY:
                acquired = units - base_fee
                if acquired < 0:
                    raise ValidationError("base commission exceeds acquisition")
                acquired_basis = Fraction(value(gross * acquired / units, exact=False))
                candidate_cash, candidate_quantity = cash - gross - quote_fee, quantity + acquired
                candidate_basis, candidate_realized = basis + acquired_basis, realized
                candidate_base_cost = base_cost + gross - acquired_basis
            else:
                removed = units + base_fee
                if removed < 0 or removed > quantity or not quantity:
                    raise ValidationError("spot sell exceeds reconciled inventory")
                removed_basis = (
                    basis
                    if removed == quantity
                    else Fraction(value(basis * removed / quantity, exact=False))
                )
                sold_basis = (
                    basis
                    if units == quantity
                    else Fraction(value(basis * units / quantity, exact=False))
                )
                candidate_cash, candidate_quantity = cash + gross - quote_fee, quantity - removed
                candidate_basis, candidate_realized = (
                    basis - removed_basis,
                    realized + gross - sold_basis,
                )
                candidate_base_cost = base_cost + removed_basis - sold_basis
            fees = dict(self.fees)
            fees[fill.fee_asset] = value(rational(fees.get(fill.fee_asset, ZERO)) + fee)
            candidate = {
                "cash": value(candidate_cash),
                "quantity": value(candidate_quantity),
                "book_cost": value(candidate_basis),
                "realized_gross_pnl": value(candidate_realized),
                "base_fee_cost_in_quote": value(candidate_base_cost),
            }
        # Publish only after every exact accounting update and valuation has passed.
        self.cash, self.quantity, self.book_cost = (
            candidate[name] for name in ("cash", "quantity", "book_cost")
        )
        self.realized_gross_pnl = candidate["realized_gross_pnl"]
        self.base_fee_cost_in_quote, self.fees = candidate["base_fee_cost_in_quote"], fees
        self._fills[fill.key] = fill
        return True


class Reservations:
    def __init__(self, cash: Decimal):
        self.cash = decimal(cash)
        self._amounts: dict[str, Decimal] = {}

    @property
    def reserved(self) -> Decimal:
        return sum(self._amounts.values(), ZERO)

    @property
    def available(self) -> Decimal:
        return self.cash - self.reserved

    def reserve(self, operation_id: str, amount: Decimal) -> None:
        amount = decimal(amount)
        if not operation_id or amount <= 0:
            raise ValidationError("positive reservation and identity required")
        if operation_id in self._amounts:
            if self._amounts[operation_id] != amount:
                raise ValidationError("conflicting reservation identity")
            return
        if amount > self.available:
            raise ValidationError("insufficient unreserved capital")
        self._amounts[operation_id] = amount

    def consume(self, operation_id: str, spent: Decimal) -> None:
        spent = decimal(spent)
        if spent < 0 or spent > self._amounts[operation_id]:
            raise ValidationError("fill exceeds reserved amount; reconciliation required")
        self._amounts[operation_id] -= spent
        self.cash -= spent

    def terminal(self, operation_id: str) -> None:
        self._amounts.pop(operation_id, None)
