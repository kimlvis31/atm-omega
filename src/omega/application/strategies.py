"""Shared analysis followed by independent strategy instances; no execution port."""

from omega.domain.models import Candle
from omega.ports.modules import Feature, Strategy, StrategySignal


class StrategyRunner:
    def __init__(self, analysis: Feature, strategies: dict[str, Strategy]):
        self.analysis, self.strategies = analysis, dict(strategies)
        self.signals: dict[str, StrategySignal | None] = {}

    def update(self, candle: Candle) -> None:
        features = self.analysis.update(candle)
        # Deliberately stores decisions only. No broker or coordinator is reachable here.
        for instance, strategy in self.strategies.items():
            self.signals[instance] = strategy.evaluate(candle, dict(features))
