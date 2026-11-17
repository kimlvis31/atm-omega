"""TEF-specific intermediate result; never the common TS decision contract."""

from dataclasses import dataclass
from decimal import Decimal

from omega.domain.models import Instrument


@dataclass(frozen=True)
class TefSignal:
    instrument: Instrument
    target_exposure: Decimal
    available_at_ms: int
