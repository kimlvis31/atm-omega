"""Bounded factual venue accounting; no margin approval, clock or broker access."""

from dataclasses import dataclass, replace
from decimal import Context, Decimal, Inexact, Rounded, localcontext

from omega.domain.errors import ValidationError
from omega.domain.execution_rules import MAX_TIME
from omega.domain.ledger import CashLedger
from omega.domain.models import (
    AccountSnapshot,
    Balance,
    Fill,
    Instrument,
    Market,
    PositionSnapshot,
    Side,
)

MAX_ACCOUNT_FILLS = 128
MAX_ACCOUNT_ASSETS = 64
ZERO = Decimal(0)


def _asset(value):
    if (
        type(value) is not str
        or not 1 <= len(value) <= 40
        or not value.isprintable()
        or any(x.isspace() for x in value)
    ):
        raise ValidationError("account asset requires bounded native text")
    return value


def _amount(value):
    if type(value) is not Decimal or not value.is_finite():
        raise ValidationError("account amount requires a finite native Decimal")
    parts = value.as_tuple()
    if len(parts.digits) > 512 or not -512 <= parts.exponent <= 512:
        raise ValidationError("account amount exceeds exact capacity")
    return value


def _add(first, second):
    context = Context(prec=2048)
    context.traps[Inexact] = context.traps[Rounded] = True
    with localcontext(context):
        return _amount(_amount(first) + _amount(second))


def _time(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("account observation requires a native timestamp")
    return value


def _account(value, account_id, market):
    if (
        type(value) is not AccountSnapshot
        or value.account_id != account_id
        or value.market is not market
        or type(value.can_trade) is not bool
    ):
        raise ValidationError("account snapshot identity or market differs")
    _time(value.observed_at_ms)
    if (
        type(value.balances) is not tuple
        or len(value.balances) > MAX_ACCOUNT_ASSETS
        or type(value.positions) is not tuple
        or len(value.positions) > 32
    ):
        raise ValidationError("account snapshot exceeds capacity")
    balances, positions = {}, {}
    for row in value.balances:
        if type(row) is not Balance or _asset(row.asset) in balances:
            raise ValidationError("account balances are malformed or duplicated")
        for amount in (row.wallet, row.available, row.locked, row.margin_balance):
            if amount is not None:
                _amount(amount)
        if row.wallet is None or row.available is None:
            raise ValidationError("account wallet/available cannot be missing")
        balances[row.asset] = replace(row)
    for row in value.positions:
        if (
            type(row) is not PositionSnapshot
            or type(row.instrument) is not Instrument
            or row.instrument.market is not market
            or row.instrument.key in positions
        ):
            raise ValidationError("account positions are malformed or duplicated")
        replace(row.instrument)
        for amount in (row.signed_quantity, row.entry_price, row.unrealized_pnl):
            _amount(amount)
        if row.entry_price < 0:
            raise ValidationError("account entry price is invalid")
        positions[row.instrument.key] = replace(row, instrument=replace(row.instrument))
    return replace(
        value,
        balances=tuple(balances[key] for key in sorted(balances)),
        positions=tuple(positions[key] for key in sorted(positions)),
    )


@dataclass(frozen=True, slots=True)
class ExecutionAccountOpening:
    account: AccountSnapshot
    instrument: Instrument
    base_asset: str
    quote_asset: str
    settlement_asset: str | None
    initial_from_id: int

    def __post_init__(self):
        if type(self.instrument) is not Instrument or self.instrument.market not in (
            Market.SPOT,
            Market.USDM,
        ):
            raise ValidationError("account opening requires a supported instrument")
        object.__setattr__(self, "instrument", replace(self.instrument))
        if (
            type(self.account) is not AccountSnapshot
            or type(self.account.account_id) is not str
            or not 1 <= len(self.account.account_id) <= 128
            or not self.account.account_id.isprintable()
        ):
            raise ValidationError("account opening identity is invalid")
        account = _account(self.account, self.account.account_id, self.instrument.market)
        object.__setattr__(self, "account", account)
        if _asset(self.base_asset) == _asset(self.quote_asset):
            raise ValidationError("base and quote assets must differ")
        wallets = {row.asset: row.wallet for row in account.balances}
        if any(row.instrument.provider != self.instrument.provider for row in account.positions):
            raise ValidationError("opening positions belong to another provider")
        if any(row.signed_quantity != 0 or row.unrealized_pnl != 0 for row in account.positions):
            raise ValidationError("first account recovery requires an explicit flat opening")
        if self.instrument.market is Market.SPOT:
            if (
                self.settlement_asset is not None
                or self.base_asset not in wallets
                or self.quote_asset not in wallets
                or wallets[self.base_asset] != 0
                or any(value < 0 for value in wallets.values())
            ):
                raise ValidationError("spot opening requires flat base and explicit wallets")
        elif _asset(self.settlement_asset) not in wallets:
            raise ValidationError("futures settlement wallet must be explicit")
        if type(self.initial_from_id) is not int or not 0 <= self.initial_from_id < 10**20:
            raise ValidationError("account opening cursor is invalid")


@dataclass(frozen=True, slots=True)
class ExecutionAccountBook:
    account_id: str
    instrument: Instrument
    revision: int
    wallets: tuple[tuple[str, Decimal], ...]
    quantity: Decimal
    book_cost: Decimal | None
    realized_gross_pnl: Decimal | None
    base_fee_cost_in_quote: Decimal | None
    fees: tuple[tuple[str, Decimal], ...]
    realized_pnl: tuple[tuple[str, Decimal], ...]
    applied_fills: tuple[Fill, ...]

    def wallet(self, asset):
        return dict(self.wallets).get(asset)


def _fill(value, opening):
    if type(value) is not Fill or type(value.instrument) is not Instrument:
        raise ValidationError("account application requires native fills")
    value = replace(value, instrument=replace(value.instrument))
    if value.account_id != opening.account.account_id or value.instrument != opening.instrument:
        raise ValidationError("account fill identity differs")
    if (
        type(value.trade_id) is not str
        or len(value.trade_id) > 20
        or not value.trade_id.isascii()
        or not value.trade_id.isdigit()
        or str(int(value.trade_id)) != value.trade_id
    ):
        raise ValidationError("account fill ID is noncanonical")
    if type(value.provider_order_id) is not str or not 1 <= len(value.provider_order_id) <= 128:
        raise ValidationError("account fill provider identity is invalid")
    if value.fee_asset:
        _asset(value.fee_asset)
    elif value.fee != 0:
        raise ValidationError("nonzero commission requires its asset")
    _time(value.timestamp_ms)
    if value.timestamp_ms < opening.account.observed_at_ms:
        raise ValidationError("account fill timestamp precedes its opening anchor")
    if int(value.trade_id) < opening.initial_from_id:
        raise ValidationError("account fill precedes its explicit scan boundary")
    for amount in (
        value.quantity,
        value.price,
        value.fee,
        value.quote_quantity,
        value.realized_pnl,
    ):
        if amount is not None:
            _amount(amount)
    return value


def _build(opening, fills):
    opening = replace(opening)
    if type(fills) is not tuple or len(fills) > MAX_ACCOUNT_FILLS:
        raise ValidationError("account fill capacity reached")
    fills = tuple(_fill(fill, opening) for fill in fills)
    if (
        len({fill.trade_id for fill in fills}) != len(fills)
        or tuple(sorted(fills, key=lambda fill: int(fill.trade_id))) != fills
    ):
        raise ValidationError("account fills require unique ordered identities")
    wallets = {row.asset: row.wallet for row in opening.account.balances}
    fees, pnl, quantity = {}, {}, ZERO
    spot = opening.instrument.market is Market.SPOT
    ledger = (
        CashLedger(
            opening.account.account_id,
            opening.instrument,
            opening.quote_asset,
            wallets[opening.quote_asset],
            base_asset=opening.base_asset,
        )
        if spot
        else None
    )
    for fill in fills:
        if fill.fee_asset and fill.fee_asset not in wallets:
            raise ValidationError("commission wallet is missing from the opening")
        if fill.fee_asset:
            fees[fill.fee_asset] = _add(fees.get(fill.fee_asset, ZERO), fill.fee)
        if spot:
            if fill.quote_quantity is None or fill.realized_pnl is not None:
                raise ValidationError("spot application needs reported quote and no futures PnL")
            ledger.apply(fill)
            if fill.fee_asset not in (opening.base_asset, opening.quote_asset) and fill.fee_asset:
                wallets[fill.fee_asset] = _add(wallets[fill.fee_asset], fill.fee.copy_negate())
        else:
            if fill.realized_pnl is None or fill.realized_pnl_asset != opening.settlement_asset:
                raise ValidationError(
                    "reported futures PnL and explicit settlement asset are required"
                )
            wallets[fill.realized_pnl_asset] = _add(
                wallets[fill.realized_pnl_asset], fill.realized_pnl
            )
            pnl[fill.realized_pnl_asset] = _add(
                pnl.get(fill.realized_pnl_asset, ZERO), fill.realized_pnl
            )
            if fill.fee_asset:
                wallets[fill.fee_asset] = _add(wallets[fill.fee_asset], fill.fee.copy_negate())
            quantity = _add(
                quantity, fill.quantity if fill.side is Side.BUY else fill.quantity.copy_negate()
            )
    if spot:
        wallets[opening.base_asset], wallets[opening.quote_asset] = ledger.quantity, ledger.cash
        quantity = ledger.quantity
    return ExecutionAccountBook(
        opening.account.account_id,
        opening.instrument,
        len(fills),
        tuple(sorted(wallets.items())),
        quantity,
        ledger.book_cost if spot else None,
        ledger.realized_gross_pnl if spot else None,
        ledger.base_fee_cost_in_quote if spot else None,
        tuple(sorted(fees.items())),
        tuple(sorted(pnl.items())),
        fills,
    )


def initial_execution_book(opening):
    if type(opening) is not ExecutionAccountOpening:
        raise ValidationError("typed execution account opening required")
    return _build(opening, ())


def apply_execution_fills(book, opening, fills):
    if (
        type(book) is not ExecutionAccountBook
        or type(opening) is not ExecutionAccountOpening
        or type(fills) is not tuple
    ):
        raise ValidationError("typed account book/opening/fills required")
    if book != _build(opening, book.applied_fills):
        raise ValidationError("account book differs from its original financial facts")
    selected = {fill.trade_id: fill for fill in book.applied_fills}
    through = max(map(int, selected), default=-1)
    for value in fills:
        fill = _fill(value, opening)
        previous = selected.get(fill.trade_id)
        if previous is not None:
            if previous != fill:
                raise ValidationError("same fill ID has conflicting financial contents")
        elif int(fill.trade_id) <= through:
            raise ValidationError("late fill precedes already applied financial history")
        else:
            selected[fill.trade_id] = fill
    return _build(opening, tuple(selected[key] for key in sorted(selected, key=int)))


def check_execution_account_snapshot(opening, account, *, observed_at_ms, minimum_ms):
    _time(observed_at_ms)
    _time(minimum_ms)
    value = _account(account, opening.account.account_id, opening.instrument.market)
    if (
        not max(opening.account.observed_at_ms, minimum_ms)
        <= value.observed_at_ms
        <= observed_at_ms
    ):
        raise ValidationError("account observation is stale or future")
    return value
