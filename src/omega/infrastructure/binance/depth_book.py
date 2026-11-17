"""Bounded Spot/USD-M diff-book synchronization, independent of sockets and storage."""

from dataclasses import dataclass
from decimal import Decimal

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Market
from omega.infrastructure.binance.auxiliary import _instrument, _integer, _payload, _positive


@dataclass(frozen=True)
class BookView:
    update_id: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    bid_floor: Decimal
    ask_ceiling: Decimal

    def covers(self, radius=Decimal(".05")):
        if not isinstance(radius, Decimal) or not radius.is_finite() or not 0 < radius < 1:
            raise ValidationError("depth radius must be a finite Decimal ratio")
        center = (self.bids[0][0] + self.asks[0][0]) / 2
        return self.bid_floor <= center * (1 - radius) and self.ask_ceiling >= center * (1 + radius)


@dataclass(frozen=True)
class DepthDiff:
    first_id: int
    final_id: int
    previous_id: int | None
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]


def _levels(rows, maximum, *, snapshot=False):
    if not isinstance(rows, list) or len(rows) > maximum:
        raise ProtocolError("depth level capacity exceeded")
    result, seen = [], set()
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            raise ProtocolError("invalid depth level")
        price = _positive(row[0])
        quantity = _positive(row[1], allow_zero=not snapshot)
        if price in seen:
            raise ProtocolError("duplicate price in a depth event")
        seen.add(price)
        result.append((price, quantity))
    return tuple(result)


class DepthBook:
    def __init__(self, instrument, *, max_levels=10_000, max_pending=256):
        _instrument(instrument)
        if (
            type(max_levels) is not int
            or not 1 <= max_levels <= 20_000
            or type(max_pending) is not int
            or not 1 <= max_pending <= 2048
        ):
            raise ValidationError("invalid depth book capacity")
        self.instrument, self.max_levels, self.max_pending = instrument, max_levels, max_pending
        self.generation = 0
        self.reset()

    def reset(self):
        self.generation += 1
        self.state = "waiting_snapshot"
        self.bids, self.asks, self.pending = {}, {}, []
        self.update_id = self.bid_floor = self.ask_ceiling = None
        self._last_diff = None
        return self.generation

    def close(self):
        self.reset()
        self.state = "closed"

    def _owner(self, generation):
        if type(generation) is not int or generation != self.generation or self.state == "closed":
            raise ProtocolError("stale or closed depth generation")
        if self.state == "resync_required":
            raise ProtocolError("reset the depth generation before resynchronization")

    def _fail(self):
        self.bids, self.asks, self.pending = {}, {}, []
        self.update_id = self.bid_floor = self.ask_ceiling = None
        self.state = "resync_required"

    def _decode(self, payload):
        row, _raw = _payload(payload)
        if "data" in row:
            names = {
                f"{self.instrument.symbol.lower()}@depth",
                f"{self.instrument.symbol.lower()}@depth@100ms",
            }
            if row.get("stream") not in names:
                raise ProtocolError("depth stream identity mismatch")
            row = row["data"]
        if row["e"] != "depthUpdate" or row["s"] != self.instrument.symbol:
            raise ProtocolError("depth event identity mismatch")
        _integer(row["E"])
        if "T" in row:
            _integer(row["T"])
        first, final = _integer(row["U"]), _integer(row["u"])
        if first > final:
            raise ProtocolError("depth update range is reversed")
        previous = _integer(row["pu"]) if self.instrument.market == Market.USDM else None
        return DepthDiff(
            first,
            final,
            previous,
            _levels(row["b"], self.max_levels),
            _levels(row["a"], self.max_levels),
        )

    def receive(self, payload, *, generation):
        self._owner(generation)
        try:
            event = self._decode(payload)
            if self.state == "waiting_snapshot":
                levels = sum(len(item.bids) + len(item.asks) for item in self.pending)
                if (
                    len(self.pending) >= self.max_pending
                    or levels + len(event.bids) + len(event.asks) > 2 * self.max_levels
                ):
                    raise ProtocolError("depth pending buffer capacity exceeded")
                self.pending.append(event)
                return False
            return self._apply(event)
        except (KeyError, TypeError, ValueError, ValidationError, ProtocolError) as exc:
            self._fail()
            raise ProtocolError("invalid or discontinuous depth event; resync required") from exc

    def snapshot(self, payload, *, generation):
        self._owner(generation)
        if self.state != "waiting_snapshot":
            raise ProtocolError("depth snapshot belongs to an already initialized generation")
        try:
            row, _raw = _payload(payload)
            identifier = _integer(row["lastUpdateId"])
            bids = _levels(row["bids"], self.max_levels, snapshot=True)
            asks = _levels(row["asks"], self.max_levels, snapshot=True)
            if (
                self.instrument.market == Market.SPOT
                and self.pending
                and identifier < self.pending[0].first_id
            ):
                raise ProtocolError("Spot snapshot is older than first buffered update")
            self.bids, self.asks = dict(bids), dict(asks)
            self._valid_book()
            self.bid_floor, self.ask_ceiling = min(self.bids), max(self.asks)
            self.update_id, self.state = identifier, "waiting_bridge"
            pending, self.pending = self.pending, []
            for event in pending:
                self._apply(event)
            return self.state == "synced"
        except (KeyError, TypeError, ValueError, ValidationError, ProtocolError) as exc:
            self._fail()
            raise ProtocolError("invalid or stale depth snapshot; resync required") from exc

    def _valid_book(self):
        if (
            not self.bids
            or not self.asks
            or max(self.bids) >= min(self.asks)
            or len(self.bids) > self.max_levels
            or len(self.asks) > self.max_levels
        ):
            raise ProtocolError("empty, crossed or oversized depth book")

    def _apply(self, event):
        first = self.state == "waiting_bridge"
        identifier = self.update_id
        if event.final_id < identifier or (
            first and self.instrument.market == Market.SPOT and event.final_id == identifier
        ):
            return False
        if not first and event.final_id == identifier:
            if self._last_diff == event:
                return False
            raise ProtocolError("conflicting duplicate depth update")
        if self.instrument.market == Market.USDM:
            continuous = (
                event.first_id <= identifier <= event.final_id
                if first
                else event.previous_id == identifier
            )
        else:
            continuous = event.first_id <= identifier + 1 <= event.final_id
        if not continuous:
            raise ProtocolError("missing depth bridge or update gap")
        for levels, target in ((event.bids, self.bids), (event.asks, self.asks)):
            for price, quantity in levels:
                if quantity == 0:
                    target.pop(price, None)  # Unknown zero-removal is a normal event.
                else:
                    target[price] = quantity
        self._valid_book()
        self.update_id, self._last_diff, self.state = event.final_id, event, "synced"
        return True

    def view(self):
        if self.state != "synced":
            raise ProtocolError("depth snapshot is not synchronized")
        return BookView(
            self.update_id,
            tuple(sorted(self.bids.items(), reverse=True)),
            tuple(sorted(self.asks.items())),
            self.bid_floor,
            self.ask_ceiling,
        )
