"""Provider-neutral identities and exact values. Floats are not money inputs."""

import re
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum

from omega.domain.errors import ValidationError


def decimal(value: str | int | Decimal) -> Decimal:
    if isinstance(value, (float, bool)):
        raise ValidationError("money/quantity must be a finite decimal string")
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValidationError("invalid decimal") from exc
    if not result.is_finite():
        raise ValidationError("non-finite decimal")
    return result


def require_decimal(value: Decimal) -> None:
    if not isinstance(value, Decimal):
        raise ValidationError("domain money/quantity fields require Decimal values")
    decimal(value)


class Market(StrEnum):
    SPOT = "spot"
    USDM = "usdm"
    CASH_EQUITY = "cash_equity"  # Contract for future adapters; not an implemented provider.


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderKind(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderStatus(StrEnum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    EXPIRED_IN_MATCH = "EXPIRED_IN_MATCH"
    PENDING_CANCEL = "PENDING_CANCEL"


def is_valid_symbol(value) -> bool:
    """Stable case-sensitive market ID, never normalized into a different identity."""
    return (
        type(value) is str
        and 2 <= len(value) <= 40
        and value.upper() == value
        and unicodedata.normalize("NFKC", value) == value
        and all(
            character in "_-"
            or (character.isalnum() and unicodedata.bidirectional(character) in {"L", "EN"})
            for character in value
        )
    )


@dataclass(frozen=True)
class Instrument:
    provider: str
    market: Market
    symbol: str

    def __post_init__(self):
        if (
            not re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", self.provider)
            or not isinstance(self.market, Market)
            or not is_valid_symbol(self.symbol)
        ):
            raise ValidationError("invalid instrument identity")

    @property
    def key(self) -> str:
        return f"{self.provider}:{self.market}:{self.symbol}"


@dataclass(frozen=True)
class OrderRequest:
    instrument: Instrument
    client_order_id: str
    side: Side
    kind: OrderKind
    quantity: Decimal
    price: Decimal | None = None
    reduce_only: bool = False

    def __post_init__(self):
        if type(self.reduce_only) is not bool:
            raise ValidationError("reduce-only control must be a native boolean")
        require_decimal(self.quantity)
        if self.price is not None:
            require_decimal(self.price)
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,36}", self.client_order_id):
            raise ValidationError("invalid client order ID")
        if decimal(self.quantity) <= 0:
            raise ValidationError("quantity must be positive")
        if self.kind == OrderKind.LIMIT:
            if self.price is None or decimal(self.price) <= 0:
                raise ValidationError("limit order needs a positive price")
        elif self.price is not None:
            raise ValidationError("market order must not include a limit price")
        if not isinstance(self.side, Side) or not isinstance(self.kind, OrderKind):
            raise ValidationError("typed side/order kind required")


@dataclass(frozen=True)
class OrderObservation:
    instrument: Instrument
    client_order_id: str
    provider_order_id: str
    status: OrderStatus
    executed_quantity: Decimal
    cumulative_quote: Decimal

    def __post_init__(self):
        if not isinstance(self.status, OrderStatus):
            raise ValidationError("typed order status required")
        require_decimal(self.executed_quantity)
        require_decimal(self.cumulative_quote)
        if self.executed_quantity < 0 or self.cumulative_quote < 0:
            raise ValidationError("negative cumulative order observation")
        if not self.client_order_id or not self.provider_order_id:
            raise ValidationError("order observation identity required")


@dataclass(frozen=True)
class Fill:
    account_id: str
    instrument: Instrument
    trade_id: str
    provider_order_id: str
    side: Side
    quantity: Decimal
    price: Decimal
    fee: Decimal
    fee_asset: str
    timestamp_ms: int
    quote_quantity: Decimal | None = None
    realized_pnl: Decimal | None = None
    realized_pnl_asset: str | None = None

    def __post_init__(self):
        for value in (self.quantity, self.price, self.fee):
            require_decimal(value)
        if not isinstance(self.side, Side):
            raise ValidationError("typed fill side required")
        if not self.account_id or not self.trade_id or not self.provider_order_id:
            raise ValidationError("fill identity required")
        if decimal(self.quantity) <= 0 or decimal(self.price) <= 0:
            raise ValidationError("invalid fill amount")
        decimal(self.fee)  # Negative commission rebates are possible.
        if self.timestamp_ms < 0 or (self.fee and not self.fee_asset):
            raise ValidationError("invalid fill metadata")
        for value in (self.quote_quantity, self.realized_pnl):
            if value is not None:
                if type(value) is not Decimal:
                    raise ValidationError("reported fill amounts require native Decimals")
                require_decimal(value)
                parts = value.as_tuple()
                if len(parts.digits) > 80 or not -60 <= parts.exponent <= 60:
                    raise ValidationError(
                        "reported fill amount exceeds its exact calculation bound"
                    )
        if self.quote_quantity is not None and self.quote_quantity <= 0:
            raise ValidationError("reported fill quote quantity must be positive")
        if (self.realized_pnl is None) != (self.realized_pnl_asset is None):
            raise ValidationError("reported fill PnL requires its paired settlement asset")
        if self.realized_pnl_asset is not None and (
            type(self.realized_pnl_asset) is not str
            or not 1 <= len(self.realized_pnl_asset) <= 40
            or not self.realized_pnl_asset.isprintable()
            or any(char.isspace() for char in self.realized_pnl_asset)
        ):
            raise ValidationError("reported fill PnL asset must be bounded native text")

    @property
    def key(self) -> tuple[str, str, str]:
        return self.account_id, self.instrument.key, self.trade_id


@dataclass(frozen=True)
class Candle:
    instrument: Instrument
    open_time_ms: int
    close_time_ms: int  # Exclusive boundary.
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    def __post_init__(self):
        for value in (self.open, self.high, self.low, self.close, self.volume):
            require_decimal(value)
        prices = [decimal(v) for v in (self.open, self.high, self.low, self.close)]
        if min(prices) <= 0 or self.low > min(self.open, self.close):
            raise ValidationError("invalid OHLC")
        if self.high < max(self.open, self.close) or self.high < self.low:
            raise ValidationError("invalid OHLC extrema")
        if (
            self.open_time_ms < 0
            or self.close_time_ms - self.open_time_ms != 60_000
            or self.open_time_ms % 60_000
        ):
            raise ValidationError("expected aligned one-minute candle")
        if decimal(self.volume) < 0:
            raise ValidationError("negative volume")


@dataclass(frozen=True)
class Balance:
    asset: str
    wallet: Decimal
    available: Decimal
    locked: Decimal | None = None
    margin_balance: Decimal | None = None

    def __post_init__(self):
        if not self.asset:
            raise ValidationError("balance asset required")
        for value in (self.wallet, self.available, self.locked, self.margin_balance):
            if value is not None:
                require_decimal(value)


@dataclass(frozen=True)
class PositionSnapshot:
    instrument: Instrument
    signed_quantity: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal

    def __post_init__(self):
        for value in (self.signed_quantity, self.entry_price, self.unrealized_pnl):
            require_decimal(value)
        if self.entry_price < 0:
            raise ValidationError("negative entry price")


@dataclass(frozen=True)
class AccountSnapshot:
    account_id: str
    market: Market
    can_trade: bool
    observed_at_ms: int
    balances: tuple[Balance, ...]
    positions: tuple[PositionSnapshot, ...] = ()

    def __post_init__(self):
        if not self.account_id or not isinstance(self.can_trade, bool) or self.observed_at_ms < 0:
            raise ValidationError("invalid account snapshot metadata")
