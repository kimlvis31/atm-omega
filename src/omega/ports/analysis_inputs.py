"""Immutable selected analysis inputs, separate from their database implementation."""

from dataclasses import dataclass
from typing import Protocol

from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Instrument
from omega.ports.auxiliary import StoredAuxiliary


@dataclass(frozen=True, slots=True)
class PreparedMarketFrame:
    frame: MarketFeatureFrame
    pins: tuple[StoredAuxiliary, ...]

    def __post_init__(self):
        if (
            not isinstance(self.frame, MarketFeatureFrame)
            or type(self.pins) is not tuple
            or any(not isinstance(value, StoredAuxiliary) for value in self.pins)
        ):
            raise ValidationError("prepared frame needs immutable typed source pins")

    @property
    def retrospective(self):
        return self.frame.retrospective


class AnalysisFrameSource(Protocol):
    def check_destination(self, expected_hash: str) -> None:
        """Reject a changed storage destination before opening or querying it."""
        ...

    def get_auxiliary(
        self,
        instrument: Instrument,
        channel: str,
        source: str,
        open_time_ms: int,
        *,
        as_of_ms: int,
    ) -> StoredAuxiliary | None: ...

    def read_frames(
        self,
        instrument: Instrument,
        start_ms: int,
        end_ms: int,
        *,
        as_of_ms: int,
        channels: tuple[str, ...],
        maximum_metric_age_ms: int,
        retrospective_until_ms: int | None = None,
        limit: int = 512,
    ) -> tuple[PreparedMarketFrame, ...]: ...

    def close(self) -> None: ...
