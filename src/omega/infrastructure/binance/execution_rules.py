"""Translate Binance filter dictionaries once; core consumers only see typed rules."""

from decimal import Decimal

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.execution_rules import DecimalRange, ExecutionRulesSnapshot, NotionalRule
from omega.domain.models import Instrument, Market, decimal
from omega.infrastructure.binance.market import SymbolRules


def _amount(row, key):
    value = row[key]
    if type(value) is not str:
        raise ValidationError("exchange filter values require decimal strings")
    return decimal(value)


def _flag(row, key):
    value = row[key]
    if type(value) is not bool:
        raise ValidationError("exchange filter applicability must be boolean")
    return value


def _window(row):
    value = row["avgPriceMins"]
    if type(value) is not int or not 0 <= value <= 1440:
        raise ValidationError("invalid exchange notional reference window")
    return value * 60000


def rules_snapshot(rules, *, observed_at_ms, source=None, reduction_statuses=()):
    if (
        not isinstance(rules, SymbolRules)
        or not isinstance(rules.instrument, Instrument)
        or rules.instrument.provider != "binance"
    ):
        raise ValidationError("Binance normalized rules require their symbol reference")
    if type(reduction_statuses) is not tuple or any(
        type(status) is not str for status in reduction_statuses
    ):
        raise ValidationError("explicit reduction status policy must be an immutable tuple")
    try:
        by_name = {}
        if type(rules.filters) is not tuple or not 1 <= len(rules.filters) <= 64:
            raise ValueError
        for row in rules.filters:
            if (
                type(row) is not dict
                or type(row.get("filterType")) is not str
                or row["filterType"] in by_name
            ):
                raise ValueError
            by_name[row["filterType"]] = row
        futures = rules.instrument.market == Market.USDM
        if rules.instrument.market not in {Market.SPOT, Market.USDM}:
            raise ValueError

        def range_rule(name, *, price=False):
            row = by_name[name]
            low = _amount(row, "minPrice" if price else "minQty")
            return DecimalRange(
                low,
                _amount(row, "maxPrice" if price else "maxQty"),
                _amount(row, "tickSize" if price else "stepSize"),
                low if futures else Decimal(0),
            )

        quantity, price = range_rule("LOT_SIZE"), range_rule("PRICE_FILTER", price=True)
        market_quantity = range_rule("MARKET_LOT_SIZE") if "MARKET_LOT_SIZE" in by_name else None
        notionals = []
        for name in ("MIN_NOTIONAL", "NOTIONAL"):
            if name not in by_name:
                continue
            row = by_name[name]
            if futures and name == "MIN_NOTIONAL":
                notionals.append(
                    NotionalRule(
                        _amount(row, "notional"),
                        apply_min_to_market=True,
                        reference_kind="mark_price",
                        minimum_reduction_exempt=True,
                    )
                )
            elif futures:
                # NOTIONAL is not currently a verified USD-M contract. Do not invent its meaning.
                continue
            else:
                window = _window(row)
                notionals.append(
                    NotionalRule(
                        _amount(row, "minNotional"),
                        _amount(row, "maxNotional") if name == "NOTIONAL" else Decimal(0),
                        _flag(
                            row, "applyToMarket" if name == "MIN_NOTIONAL" else "applyMinToMarket"
                        ),
                        _flag(row, "applyMaxToMarket") if name == "NOTIONAL" else False,
                        "provider_average" if window else "last_trade",
                        window,
                    )
                )
        supported = {"LOT_SIZE", "PRICE_FILTER", "MARKET_LOT_SIZE", "MIN_NOTIONAL"}
        if not futures:
            supported.add("NOTIONAL")
        return ExecutionRulesSnapshot(
            rules.instrument,
            observed_at_ms,
            source or f"binance:{rules.instrument.market}:exchangeInfo",
            rules.status,
            quantity,
            market_quantity,
            price,
            tuple(notionals),
            tuple(sorted(set(by_name) - supported)),
            rules.status == "TRADING" or rules.status in reduction_statuses,
        )
    except (KeyError, TypeError, ValueError, ValidationError):
        raise ProtocolError(
            "exchange static execution filters are incomplete or malformed"
        ) from None
