"""Pure full-fill bar price/time reference shared by virtual execution callers."""

from dataclasses import dataclass
from decimal import Decimal

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, OrderKind, Side, require_decimal
from omega.domain.trading import OrderProposal

MODEL = "next_bar_full_fill_v1"
MAX_TIME = 253_402_300_799_000


@dataclass(frozen=True, slots=True)
class BarFillQuote:
    price: Decimal
    event_time_ms: int
    phase: str
    model: str = MODEL

    def __post_init__(self):
        require_decimal(self.price)
        if (
            self.price <= 0
            or type(self.event_time_ms) is not int
            or not 0 <= self.event_time_ms <= MAX_TIME
            or self.phase not in {"open", "close"}
            or self.model != MODEL
        ):
            raise ValidationError("invalid paper bar-fill quote")


def resolve_bar_fill(
    proposal: OrderProposal, candle: Candle, eligible_at_ms: int
) -> BarFillQuote | None:
    """Preserve Paper._fill price/time semantics without claiming intrabar order.

    Callers compute the next eligible minute at intent creation. A later-touched
    limit uses the close boundary because its actual intrabar time is unknown.
    This function does not assess margin, apply fees, persist or send orders.
    """
    if (
        not isinstance(proposal, OrderProposal)
        or not isinstance(candle, Candle)
        or not isinstance(candle.instrument, Instrument)
        or type(eligible_at_ms) is not int
        or not 0 <= eligible_at_ms <= MAX_TIME
        or type(candle.open_time_ms) is not int
        or type(candle.close_time_ms) is not int
        or not 0 <= candle.open_time_ms < candle.close_time_ms <= MAX_TIME
    ):
        raise ValidationError("bar fills require a typed candle and bounded eligibility time")
    if candle.open_time_ms < eligible_at_ms:
        return None
    price, timing, phase = candle.open, candle.open_time_ms, "open"
    if proposal.kind == OrderKind.LIMIT:
        limit = proposal.price
        if proposal.side == Side.BUY:
            if candle.low > limit:
                return None
            if price > limit:
                timing, phase = candle.close_time_ms, "close"
            price = min(price, limit)
        else:
            if candle.high < limit:
                return None
            if price < limit:
                timing, phase = candle.close_time_ms, "close"
            price = max(price, limit)
    return BarFillQuote(price, timing, phase)
