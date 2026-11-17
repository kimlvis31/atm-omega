"""Bounded trade-ID reordering and explicit proof for immutable one-minute flows."""

from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal, DecimalException, localcontext

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput, TradeFlowEvent
from omega.domain.models import Instrument

MINUTE = 60_000
MAX_TIME = 253_402_300_799_000


@dataclass(frozen=True)
class TradeFlowCoverage:
    """Accepted source events bracketing a minute, never a user 'covered=True' flag.

    Their intervening aggregate IDs must all be present and time-monotonic.
    A runtime/adapter must derive these anchors from actual observations.
    """

    before: TradeFlowEvent
    after: TradeFlowEvent
    generation: int

    @property
    def through_id(self):
        return self.after.aggregate_id


def _start(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME - MINUTE or value % MINUTE:
        raise ValidationError("trade flow requires a valid UTC minute boundary")


def _signature(event):
    # A second transport receipt is not a conflicting exchange trade.
    return (
        event.instrument,
        event.aggregate_id,
        event.first_trade_id,
        event.last_trade_id,
        event.price,
        event.quantity,
        event.buyer_is_maker,
        event.trade_time_ms,
        event.source,
    )


class TradeFlowAggregator:
    def __init__(
        self, instrument, start_ms, *, source, generation=0, max_pending_events=4096, max_minutes=16
    ):
        _start(start_ms)
        if (
            not isinstance(instrument, Instrument)
            or not isinstance(source, str)
            or not 1 <= len(source) <= 96
            or not source.isprintable()
            or type(generation) is not int
            or not 0 <= generation <= 2**63 - 1
            or type(max_pending_events) is not int
            or not 1 <= max_pending_events <= 20_000
            or type(max_minutes) is not int
            or not 1 <= max_minutes <= 1440
        ):
            raise ValidationError("invalid bounded trade flow configuration")
        self.instrument, self.source, self.generation = instrument, source, generation
        self.next_open_ms = start_ms
        self.max_pending_events, self.max_minutes = max_pending_events, max_minutes
        self._events, self._recent = {}, OrderedDict()

    def _owner(self, generation):
        if type(generation) is not int or generation != self.generation:
            raise ProtocolError("stale trade flow generation")

    def _event(self, event):
        if (
            not isinstance(event, TradeFlowEvent)
            or event.instrument != self.instrument
            or event.source != self.source
        ):
            raise ValidationError("trade flow event belongs to another instrument or dataset")

    def accept(self, event, *, generation):
        """False is an equal duplicate; every rejection leaves the prior buffer intact."""
        self._owner(generation)
        self._event(event)
        previous = self._events.get(event.aggregate_id) or self._recent.get(event.aggregate_id)
        if previous is not None:
            if _signature(previous) != _signature(event):
                raise ProtocolError("conflicting aggregate trade ID")
            return False
        if event.trade_time_ms < self.next_open_ms:
            raise ProtocolError("late trade for an immutable finalized minute")
        if event.trade_time_ms >= self.next_open_ms + self.max_minutes * MINUTE:
            raise ProtocolError("trade flow reordering time window exceeded")
        if len(self._events) >= self.max_pending_events:
            raise ProtocolError("trade flow pending capacity exceeded")
        self._events[event.aggregate_id] = event
        return True

    def _proof(self, coverage, opening, ending, as_of_ms):
        if coverage is None:
            return False
        if not isinstance(coverage, TradeFlowCoverage):
            raise ValidationError("trade flow coverage requires observed boundary anchors")
        self._owner(coverage.generation)
        known = dict(self._recent) | self._events
        anchors = []
        for event in (coverage.before, coverage.after):
            self._event(event)
            original = known.get(event.aggregate_id)
            if original is not None and _signature(original) != _signature(event):
                raise ProtocolError("coverage anchor conflicts with observed aggregate trade")
            event = original or event
            if event.available_at_ms > as_of_ms:
                raise ValidationError("coverage anchor was not yet available")
            known[event.aggregate_id] = event
            anchors.append(event)
        before, after = anchors
        if not (
            before.trade_time_ms < opening
            and after.trade_time_ms >= ending
            and before.aggregate_id < after.aggregate_id
        ):
            raise ValidationError("trade flow coverage anchors do not bracket the minute")
        if any(
            opening <= event.trade_time_ms < ending
            and not before.aggregate_id < event.aggregate_id < after.aggregate_id
            for event in self._events.values()
        ):
            raise ProtocolError("coverage IDs exclude a known trade inside this minute")
        interval = sorted(
            (
                event
                for key, event in known.items()
                if before.aggregate_id <= key <= after.aggregate_id
            ),
            key=lambda event: event.aggregate_id,
        )
        if any(max(event.available_at_ms, event.trade_time_ms) > as_of_ms for event in interval):
            raise ValidationError("coverage contains an unavailable trade")
        if any(
            left.trade_time_ms > right.trade_time_ms
            for left, right in zip(interval, interval[1:], strict=False)
        ):
            raise ProtocolError("aggregate trade IDs reverse exchange trade time")
        # Count the bounded known IDs; do not allocate a potentially enormous ID range.
        return len(interval) == after.aggregate_id - before.aggregate_id + 1

    def finalize(self, opening, *, as_of_ms, generation, coverage=None):
        self._owner(generation)
        _start(opening)
        ending = opening + MINUTE
        if (
            opening != self.next_open_ms
            or type(as_of_ms) is not int
            or not ending <= as_of_ms <= MAX_TIME
        ):
            raise ValidationError("finalize the next closed trade flow minute in order")
        events = [
            event for event in self._events.values() if opening <= event.trade_time_ms < ending
        ]
        if any(event.available_at_ms > as_of_ms for event in events):
            raise ValidationError("trade flow observation was not yet available")
        complete = self._proof(coverage, opening, ending, as_of_ms)
        totals = {
            key: Decimal(0)
            for key in (
                "AGGTRADE_QUANTITYBUY",
                "AGGTRADE_QUANTITYSELL",
                "AGGTRADE_NOTIONALBUY",
                "AGGTRADE_NOTIONALSELL",
            )
        }
        totals.update(
            AGGTRADE_NTRADESBUY=0, AGGTRADE_NTRADESSELL=0, AGGTRADE_EVENTCOUNT=len(events)
        )
        # Decimal's default 28 significant digits can silently round exact money.
        # Bound the required working precision before allocating its arithmetic state.
        low, high = 0, 0
        for event in events:
            low = min(
                low,
                event.quantity.as_tuple().exponent,
                event.quantity.as_tuple().exponent + event.price.as_tuple().exponent,
            )
            high = max(
                high,
                event.quantity.adjusted(),
                event.quantity.adjusted() + event.price.adjusted() + 1,
            )
        precision = max(28, high - low + len(str(len(events) + 1)) + 4)
        if precision > 4096:
            raise ValidationError("trade flow exact arithmetic capacity exceeded")
        try:
            with localcontext() as context:
                context.prec = precision
                for event in events:
                    side = "SELL" if event.buyer_is_maker else "BUY"
                    totals["AGGTRADE_QUANTITY" + side] += event.quantity
                    totals["AGGTRADE_NOTIONAL" + side] += event.quantity * event.price
                    totals["AGGTRADE_NTRADES" + side] += event.trade_count
        except DecimalException:
            raise ValidationError("trade flow notional outside exact arithmetic limits") from None
        quality = "observed" if complete else "partial" if events else "missing"
        result = AuxiliaryInput(
            "trade_flow",
            tuple(totals.items()) if events or complete else (),
            opening,
            ending,
            ending,
            as_of_ms,
            quality,
            Decimal(1) if complete else Decimal(0),
            self.source,
        )
        # All validation/calculation happened before changing the delivery cursor.
        for event in events:
            del self._events[event.aggregate_id]
            self._recent[event.aggregate_id] = event
        while len(self._recent) > self.max_pending_events:
            self._recent.popitem(last=False)
        self.next_open_ms = ending
        return result

    def reset(self, start_ms, *, generation):
        """A new connection cannot inherit unproven coverage from the retired epoch."""
        _start(start_ms)
        if type(generation) is not int or not self.generation < generation <= 2**63 - 1:
            raise ValidationError("trade flow reset requires a strictly newer generation")
        discarded = {event.trade_time_ms // MINUTE for event in self._events.values()}
        result = {
            "previous_generation": self.generation,
            "discarded_events": len(self._events),
            "discarded_minutes": len(discarded),
        }
        self.next_open_ms, self.generation = start_ms, generation
        self._events, self._recent = {}, OrderedDict()
        return result
