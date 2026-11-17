"""TS-neutral market/account views and proposed orders. Exposure belongs to TEF."""

# Imports
from collections.abc import Mapping
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, Decimal
from math import gcd
from types import MappingProxyType
from typing import Any

from omega.domain.errors import ValidationError
from omega.domain.execution_rules import bounded_decimal
from omega.domain.models import Candle, Instrument, OrderKind, Side, require_decimal
from omega.domain.protection import ProtectionObservation


@dataclass(frozen=True)
class PositionView:
    quantity: Decimal = Decimal(0)
    entry_price: Decimal = Decimal(0)
    revision: int = 0
    last_changed_ms: int = 0

    def __post_init__(self):
        require_decimal(self.quantity)
        require_decimal(self.entry_price)
        if (
            self.entry_price < 0
            or (self.quantity != 0 and self.entry_price <= 0)
            or type(self.revision) is not int
            or self.revision < 0
            or type(self.last_changed_ms) is not int
            or self.last_changed_ms < 0
        ):
            raise ValidationError("invalid position view")


@dataclass(frozen=True)
class BudgetView:
    allocated: Decimal
    available: Decimal

    def __post_init__(self):
        for value in (self.allocated, self.available):
            require_decimal(value)
            if value < 0:
                raise ValidationError("negative trading budget")


@dataclass(frozen=True)
class ExecutionConstraints:
    quantity_step: Decimal = Decimal("0.001")
    price_tick: Decimal = Decimal("0.01")
    min_quantity: Decimal = Decimal("0.001")
    min_notional: Decimal = Decimal(0)
    leverage: Decimal = Decimal(1)
    can_short: bool = False
    quantity_origin: Decimal = Decimal(0)
    price_origin: Decimal = Decimal(0)
    market_quantity_step: Decimal | None = None
    market_quantity_origin: Decimal = Decimal(0)
    market_min_quantity: Decimal | None = None

    def __post_init__(self):
        for value in (
            self.quantity_step,
            self.price_tick,
            self.min_quantity,
            self.min_notional,
            self.leverage,
            self.quantity_origin,
            self.price_origin,
            self.market_quantity_origin,
        ):
            bounded_decimal(value)
        for value in (self.market_quantity_step, self.market_min_quantity):
            if value is not None:
                bounded_decimal(value)
        if (
            self.quantity_step <= 0
            or self.price_tick <= 0
            or self.min_quantity < 0
            or self.min_notional < 0
            or self.leverage < 1
            or type(self.can_short) is not bool
            or self.quantity_origin < 0
            or self.price_origin < 0
            or self.market_quantity_origin < 0
            or self.market_quantity_step is not None
            and self.market_quantity_step < 0
            or self.market_min_quantity is not None
            and self.market_min_quantity < 0
            or self.market_quantity_step is None
            and self.market_quantity_origin != 0
        ):
            raise ValidationError("invalid execution constraints")
        self.quantity_grid(OrderKind.MARKET)

    def quantity_grid(self, kind=OrderKind.MARKET):
        if not isinstance(kind, OrderKind):
            raise ValidationError("typed order kind required for quantity grid")
        minimum = self.min_quantity
        if kind != OrderKind.MARKET:
            return self.quantity_step, self.quantity_origin, minimum
        minimum = max(minimum, self.market_min_quantity or Decimal(0))
        if not self.market_quantity_step:
            return self.quantity_step, self.quantity_origin, minimum
        scale, values = _integers(
            self.quantity_step,
            self.quantity_origin,
            self.market_quantity_step,
            self.market_quantity_origin,
        )
        first, a, second, b = values
        divisor = gcd(first, second)
        if (b - a) % divisor:
            raise ValidationError("regular and market quantity grids have no intersection")
        modulus = second // divisor
        multiplier = (
            0
            if modulus == 1
            else ((b - a) // divisor * pow(first // divisor, -1, modulus)) % modulus
        )
        step = first // divisor * second
        origin = (a + first * multiplier) % step
        result = _decimal_integer(step, scale), _decimal_integer(origin, scale), minimum
        bounded_decimal(result[0])
        bounded_decimal(result[1])
        return result

    def floor_quantity(self, value, kind=OrderKind.MARKET):
        step, origin, minimum = self.quantity_grid(kind)
        result = floor_to_step(value, step, origin)
        return result if result > 0 and result >= minimum else Decimal(0)

    def limit_price(self, price, side):
        return limit_price(price, self.price_tick, side, self.price_origin)

    def round_price(self, price, rounding=ROUND_HALF_EVEN):
        return _round_to_grid(price, self.price_tick, self.price_origin, rounding)


@dataclass(frozen=True)
class TradeContext:
    instrument: Instrument
    price: Decimal
    observed_at_ms: int
    position: PositionView
    budget: BudgetView
    constraints: ExecutionConstraints
    closed_candle: Candle | None = None
    features: Mapping[str, Any] = field(default_factory=dict)
    entries_allowed: bool = True
    state_certain: bool = True
    control_owned: bool = True
    has_pending_order: bool = False
    protection_observation: ProtectionObservation | None = None

    def __post_init__(self):
        require_decimal(self.price)
        if self.price <= 0 or type(self.observed_at_ms) is not int or self.observed_at_ms < 0:
            raise ValidationError("invalid market observation")
        if self.position.last_changed_ms > self.observed_at_ms:
            raise ValidationError("position change is newer than the observation")
        if self.closed_candle is not None and (
            self.closed_candle.instrument != self.instrument
            or self.closed_candle.close_time_ms > self.observed_at_ms
        ):
            raise ValidationError("mismatched or future closed candle")
        if self.protection_observation is not None:
            observation = self.protection_observation
            if (
                type(observation) is not ProtectionObservation
                or observation.instrument != self.instrument
                or self.closed_candle is not None
                or self.entries_allowed
                or self.price != observation.last
                or observation.event_time_ms < self.position.last_changed_ms
                or observation.original_received_at_ms < self.position.last_changed_ms
                or observation.received_at_ms > self.observed_at_ms
            ):
                raise ValidationError("unqualified protection-only trading context")
            observation.current(self.observed_at_ms)
        for value in (
            self.entries_allowed,
            self.state_certain,
            self.control_owned,
            self.has_pending_order,
        ):
            if type(value) is not bool:
                raise ValidationError("invalid trading control flag")
        object.__setattr__(self, "features", MappingProxyType(dict(self.features)))


@dataclass(frozen=True)
class OrderProposal:
    side: Side
    quantity: Decimal
    kind: OrderKind = OrderKind.MARKET
    price: Decimal | None = None
    reducing: bool = False
    reason: str = "strategy"

    def __post_init__(self):
        require_decimal(self.quantity)
        if self.price is not None:
            require_decimal(self.price)
        if (
            not isinstance(self.side, Side)
            or not isinstance(self.kind, OrderKind)
            or self.quantity <= 0
            or type(self.reducing) is not bool
            or not isinstance(self.reason, str)
            or not 1 <= len(self.reason) <= 128
        ):
            raise ValidationError("invalid order proposal")
        if (self.kind == OrderKind.LIMIT and (self.price is None or self.price <= 0)) or (
            self.kind == OrderKind.MARKET and self.price is not None
        ):
            raise ValidationError("order proposal price does not match order kind")


@dataclass(frozen=True)
class TradeDecision:
    orders: tuple[OrderProposal, ...] = ()
    reason: str = "hold"
    cancel_pending: bool = False

    def __post_init__(self):
        if not isinstance(self.orders, tuple) or not all(
            isinstance(o, OrderProposal) for o in self.orders
        ):
            raise ValidationError("trade decision requires immutable order proposals")
        if not isinstance(self.reason, str) or not 1 <= len(self.reason) <= 128:
            raise ValidationError("invalid trade decision reason")
        if type(self.cancel_pending) is not bool or (self.cancel_pending and self.orders):
            raise ValidationError("confirm cancellation before proposing replacement orders")


def _integers(*values):
    for value in values:
        bounded_decimal(value)
    scale = min(value.as_tuple().exponent for value in values)
    result = []
    for value in values:
        sign, digits, exponent = value.as_tuple()
        coefficient = int("".join(map(str, digits)))
        result.append((-1 if sign else 1) * coefficient * 10 ** (exponent - scale))
    return scale, result


def _decimal_integer(value, scale):
    return Decimal((int(value < 0), tuple(map(int, str(abs(value)))), scale))


def _round_to_grid(value, step, origin, rounding):
    scale, (number, unit, start) = _integers(value, step, origin)
    if unit <= 0 or start < 0 or rounding not in {ROUND_FLOOR, ROUND_CEILING, ROUND_HALF_EVEN}:
        raise ValidationError("unsupported or disabled execution grid")
    quotient, remainder = divmod(number - start, unit)
    if rounding == ROUND_CEILING and remainder:
        quotient += 1
    elif rounding == ROUND_HALF_EVEN and (
        2 * remainder > unit or 2 * remainder == unit and quotient % 2
    ):
        quotient += 1
    grid_scale = min(step.as_tuple().exponent, origin.as_tuple().exponent)
    if origin == 0:
        # Preserve divide's preferred exponent for an exact integral quotient,
        # using integer trailing zeros rather than context-dependent division.
        grid_scale = step.as_tuple().exponent
        if remainder == 0:
            preferred = max(0, value.as_tuple().exponent - step.as_tuple().exponent)
            trailing = len(str(abs(quotient))) - len(str(abs(quotient)).rstrip("0"))
            grid_scale += preferred if quotient == 0 else min(preferred, trailing)
    result = _decimal_integer((start + quotient * unit) // 10 ** (grid_scale - scale), grid_scale)
    bounded_decimal(result)
    return result


def floor_to_step(value: Decimal, step: Decimal, origin: Decimal = Decimal(0)) -> Decimal:
    return _round_to_grid(value, step, origin, ROUND_FLOOR)


def limit_price(price: Decimal, tick: Decimal, side: Side, origin: Decimal = Decimal(0)) -> Decimal:
    if not isinstance(side, Side):
        raise ValidationError("typed side required for limit price rounding")
    return _round_to_grid(price, tick, origin, ROUND_FLOOR if side == Side.BUY else ROUND_CEILING)


def close_position(context: TradeContext, reason: str) -> TradeDecision:
    quantity = context.constraints.floor_quantity(context.position.quantity.copy_abs())
    if quantity <= 0:
        return TradeDecision(reason="flat_or_untradeable_dust")
    side = Side.SELL if context.position.quantity > 0 else Side.BUY
    return TradeDecision((OrderProposal(side, quantity, reducing=True, reason=reason),), reason)
