"""Shared static-rule validation with actual-clock freshness and explicit blind spots."""

from dataclasses import dataclass
from decimal import Decimal, localcontext

from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.execution_rules import (
    MAX_TIME,
    ExecutionRulesSnapshot,
    NotionalReference,
    bounded_decimal,
)
from omega.domain.models import Market, OrderKind
from omega.domain.trading import ExecutionConstraints, OrderProposal


@dataclass(frozen=True, slots=True)
class RuleValidation:
    fresh: bool
    unchecked: tuple[str, ...]
    snapshot_hash: str
    retrospective: bool


def sizing_constraints(snapshot, leverage=Decimal(1)):
    """Sizing only. Call actual-clock rule validation again before accepting an order."""
    if not isinstance(snapshot, ExecutionRulesSnapshot):
        raise ValidationError("sizing requires a normalized execution rules snapshot")
    if snapshot.quantity.step <= 0 or snapshot.price.step <= 0:
        raise ValidationError("disabled regular quantity/price grid is unsupported for TS sizing")
    bounded_decimal(leverage)
    if snapshot.instrument.market == Market.SPOT and leverage != 1:
        raise ValidationError("spot cash sizing requires leverage one")
    market = snapshot.market_quantity
    return ExecutionConstraints(
        quantity_step=snapshot.quantity.step,
        price_tick=snapshot.price.step,
        min_quantity=snapshot.quantity.minimum,
        min_notional=Decimal(0),
        leverage=leverage,
        can_short=snapshot.instrument.market == Market.USDM,
        quantity_origin=snapshot.quantity.grid_origin,
        price_origin=snapshot.price.grid_origin,
        market_quantity_step=market.step if market else None,
        market_quantity_origin=market.grid_origin if market else Decimal(0),
        market_min_quantity=market.minimum if market else None,
    )


def _range(value, rule):
    bounded_decimal(value)
    with localcontext() as context:
        context.prec = 256
        if (
            rule.minimum
            and value < rule.minimum
            or rule.maximum
            and value > rule.maximum
            or rule.step
            and (value - rule.grid_origin) % rule.step
        ):
            raise ValidationError("order violates normalized quantity or price range")


def validate_execution_order(
    instrument,
    proposal,
    snapshot,
    *,
    now_ms,
    max_age_ms=60000,
    strict=True,
    references=(),
    retrospective=False,
):
    if (
        not isinstance(proposal, OrderProposal)
        or not isinstance(snapshot, ExecutionRulesSnapshot)
        or snapshot.instrument != instrument
        or type(now_ms) is not int
        or not 0 <= now_ms <= MAX_TIME
        or type(max_age_ms) is not int
        or not 1 <= max_age_ms <= 86400000
        or type(strict) is not bool
        or type(retrospective) is not bool
        or type(references) is not tuple
        or len(references) > 8
        or any(not isinstance(reference, NotionalReference) for reference in references)
        or any(reference.instrument != instrument for reference in references)
        or len({(reference.kind, reference.window_ms) for reference in references})
        != len(references)
    ):
        raise ValidationError("invalid normalized execution validation context")
    age = now_ms - snapshot.observed_at_ms
    if age < 0:
        raise TradingBlocked("execution rules observation is ahead of the actual decision clock")
    fresh = age <= max_age_ms
    unchecked = ["unsupported_filter:" + name for name in snapshot.unsupported_filters]
    if proposal.reducing:
        if not snapshot.reductions_allowed:
            raise TradingBlocked("symbol status does not explicitly permit reductions")
        if not fresh:
            unchecked.append("stale_rules_reduction")
    elif snapshot.status != "TRADING" or not fresh:
        raise TradingBlocked("fresh trading-status execution rules are required for new risk")
    if strict and unchecked and not proposal.reducing:
        raise TradingBlocked("strict new risk requires all exchange filters to be supported")
    _range(proposal.quantity, snapshot.quantity)
    if proposal.kind == OrderKind.MARKET and snapshot.market_quantity is not None:
        # A disabled extra grid never disables the independent regular lot rule.
        _range(proposal.quantity, snapshot.market_quantity)
    if proposal.price is not None:
        _range(proposal.price, snapshot.price)
    for rule in snapshot.notionals:
        minimum = rule.minimum and not (proposal.reducing and rule.minimum_reduction_exempt)
        maximum = bool(rule.maximum)
        if proposal.kind == OrderKind.MARKET:
            minimum = minimum and rule.apply_min_to_market
            maximum = maximum and rule.apply_max_to_market
        if not minimum and not maximum:
            continue
        price = proposal.price
        if price is None:
            reference = next(
                (
                    item
                    for item in references
                    if item.kind == rule.reference_kind
                    and item.window_ms == rule.reference_window_ms
                ),
                None,
            )
            if reference is None or not 0 <= now_ms - reference.observed_at_ms <= max_age_ms:
                label = f"missing_market_reference:{rule.reference_kind}:{rule.reference_window_ms}"
                if strict and not proposal.reducing:
                    raise TradingBlocked(
                        "market notional requires a fresh matching provider reference"
                    )
                unchecked.append(label)
                continue
            price = reference.price
        with localcontext() as context:
            context.prec = 256
            notional = proposal.quantity * price
            if minimum and notional < rule.minimum or maximum and notional > rule.maximum:
                raise ValidationError("order violates normalized notional range")
    return RuleValidation(
        fresh, tuple(sorted(set(unchecked))), snapshot.content_hash, retrospective
    )
