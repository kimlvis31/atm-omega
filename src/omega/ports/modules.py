"""Future analysis, strategy and external NN integrations cannot access a broker port."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from omega.domain.models import Candle, Instrument


@dataclass(frozen=True)
class ModuleDescriptor:
    module_id: str
    api_version: str
    required_fields: frozenset[str]
    stateful: bool = False


class Feature(Protocol):
    descriptor: ModuleDescriptor

    def update(self, candle: Candle) -> dict[str, Any]: ...


@dataclass(frozen=True)
class StrategySignal:
    instrument: Instrument
    target_exposure: Decimal
    available_at_ms: int


class Strategy(Protocol):
    descriptor: ModuleDescriptor

    def evaluate(
        self, closed_candle: Candle, features: dict[str, Any]
    ) -> StrategySignal | None: ...


class ExternalModel(Protocol):
    descriptor: ModuleDescriptor

    def preprocess(self, source: Any) -> Any: ...

    def predict(self, inputs: Any, state: Any) -> tuple[Any, Any]: ...

    def reset_state(self) -> Any: ...
