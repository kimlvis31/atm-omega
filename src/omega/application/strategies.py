"""Compute shared features once, then isolate independent TS family instances."""

from collections.abc import Callable

from omega.application.trading import TradingEngine
from omega.domain.models import Candle
from omega.domain.trading import TradeContext, TradeDecision
from omega.ports.modules import Feature
from omega.ports.trade_strategy import TradeStrategy


class StrategyRunner:
    def __init__(
        self,
        analysis: Feature,
        strategies: dict[str, TradeStrategy],
        context_factory: Callable[[str, Candle, dict], TradeContext],
    ):
        self.analysis = analysis
        self.engines = {name: TradingEngine(module) for name, module in strategies.items()}
        self.context_factory = context_factory
        self.decisions: dict[str, TradeDecision] = {}
        self.errors: dict[str, str] = {}

    def update(self, candle: Candle) -> None:
        features = self.analysis.update(candle)
        # No broker or normalized TEF exposure appears in this common runner.
        for instance, engine in self.engines.items():
            if instance in self.errors:
                continue
            try:
                context = self.context_factory(instance, candle, dict(features))
                self.decisions[instance] = engine.evaluate(context)
            except Exception as exc:
                # A malformed plugin quarantines only that instance until explicit recovery.
                self.errors[instance] = type(exc).__name__
                self.decisions[instance] = TradeDecision(reason="module_quarantined")
