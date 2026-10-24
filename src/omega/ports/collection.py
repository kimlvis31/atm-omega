"""Single-owner collection storage; ingestion does not know SQL or WebSockets."""

from dataclasses import dataclass
from typing import Protocol

from omega.domain.models import Candle, Instrument


@dataclass(frozen=True)
class MinuteObservation:
    candle: Candle
    source: str
    received_at_ms: int
    raw: str | None = None


class CandleStore(Protocol):
    def get(self, instrument: Instrument, open_time_ms: int) -> Candle | None: ...

    def put(self, observation: MinuteObservation) -> bool:
        """Commit before publication; equal duplicate is false, conflict raises."""
        ...
