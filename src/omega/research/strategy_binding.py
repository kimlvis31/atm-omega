"""Bridge the common TS contract to closed-minute CPU reference replay.

Factory/feature instances are supplied from outside. This adapter knows no TEF
model, exposure or broker. Immediate intrabar local-policy parity is not claimed.
"""

# Imports
from collections.abc import Callable
from dataclasses import replace
from decimal import Decimal
from typing import Any

from omega.analysis.catalog import create_features, validate_configuration
from omega.application.trading import TradingEngine
from omega.domain.models import Candle, Instrument, Market, Side
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext
from omega.research.simulation import CancelPendingRequest, OrderRequest, SimulationView


def build_features(instrument: Instrument, configuration: dict):
    """Compose supported analysis nodes without coupling them to any TS family."""
    if not configuration:
        return None
    if (
        isinstance(configuration, dict)
        and not configuration.keys() - {"moving_averages", "momentum", "swing"}
        and all(type(rows) is list and not rows for rows in configuration.values())
    ):
        return None
    canonical = validate_configuration(configuration)
    required = {f"CLOSED_{row['interval_id']}" for rows in canonical.values() for row in rows}
    return create_features(instrument, required, canonical)


class TradeStrategyBinding:
    def __init__(
        self,
        factory: Callable,
        configuration: dict | None = None,
        *,
        feature_factory: Callable | None = None,
        constraints: dict | None = None,
        allocated_per_instrument: Decimal | None = None,
    ):
        self.factory = factory
        self.configuration = configuration or {}
        self.feature_factory = feature_factory
        self.constraints = dict(constraints or {})
        self.allocated = allocated_per_instrument
        if self.allocated is not None and (not self.allocated.is_finite() or self.allocated <= 0):
            raise ValueError("allocated_per_instrument must be positive")
        self.engines: dict[str, TradingEngine] = {}
        self.features: dict[str, Any] = {}
        self._cancel_contexts = {}

    def __call__(self, view: SimulationView) -> list[OrderRequest | CancelPendingRequest]:
        orders = []
        self._cancel_contexts = {}
        for row in view.candles:
            parts = row.instrument.split(":")
            if len(parts) != 3:
                raise ValueError("common TS binding requires provider:market:symbol identity")
            instrument = Instrument(parts[0], Market(parts[1]), parts[2])
            if row.instrument not in self.engines:
                self.engines[row.instrument] = TradingEngine(
                    self.factory(instrument, self.configuration)
                )
                if self.feature_factory:
                    self.features[row.instrument] = self.feature_factory(instrument)
            candle = Candle(
                instrument,
                row.open_time_ms,
                row.open_time_ms + 60000,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
            )
            rules = ExecutionConstraints(
                can_short=instrument.market == Market.USDM, **self.constraints
            )
            if rules.leverage != 1:
                raise ValueError(
                    "CPU reference binding only supports leverage=1; margin emulator pending"
                )
            allocated = self.allocated or max(Decimal(0), view.wallet) / len(view.positions)
            committed = abs(view.positions[row.instrument]) * view.entry_prices[row.instrument]
            features = (
                self.features[row.instrument].update(candle)
                if self.features.get(row.instrument)
                else {}
            )
            required = self.engines[row.instrument].module.descriptor.required_fields
            if required - features.keys():
                raise ValueError(
                    "research feature configuration does not provide "
                    f"{sorted(required - features.keys())}"
                )
            context = TradeContext(
                instrument=instrument,
                price=row.close,
                observed_at_ms=view.timestamp_ms,
                position=PositionView(
                    view.positions[row.instrument],
                    view.entry_prices[row.instrument],
                    view.position_revisions[row.instrument],
                    view.position_changed_ms[row.instrument],
                ),
                budget=BudgetView(allocated, max(Decimal(0), allocated - committed)),
                constraints=rules,
                closed_candle=candle,
                features=features,
                has_pending_order=row.instrument in view.pending_instruments,
            )
            decision = self.engines[row.instrument].evaluate(context)
            if decision.cancel_pending:
                self._cancel_contexts[row.instrument] = context
                orders.append(CancelPendingRequest(row.instrument))
            else:
                orders.extend(self._orders(row.instrument, decision, allocated))
        return orders

    def on_cancellation_confirmed(self, instrument):
        context = self._cancel_contexts.pop(instrument)
        decision = self.engines[instrument].evaluate(replace(context, has_pending_order=False))
        if decision.cancel_pending:
            raise ValueError("TS requested another cancellation after confirmation")
        return self._orders(instrument, decision, context.budget.allocated)

    @staticmethod
    def _orders(instrument, decision, allocated):
        return [
            OrderRequest(
                instrument,
                proposal.quantity * (1 if proposal.side == Side.BUY else -1),
                proposal.kind.value.lower(),
                proposal.price,
                proposal.reducing,
                proposal.reason,
                entry_budget=allocated,
            )
            for proposal in decision.orders
        ]
