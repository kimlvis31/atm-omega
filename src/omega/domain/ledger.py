"""Single-owner cash accounting and shared reservations, without I/O."""

from dataclasses import dataclass, field
from decimal import Decimal

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
