"""Eta-style latest-minute quote-notional snapshots over a synchronized book.

This adapter owns spatial bins and observation quality, never sockets or SQL.
Decimal half-even is explicit; float rounding at every legacy boundary is not
claimed bit-identical. Unknown outer prices are omitted rather than invented0.
"""

import re
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.depth_book import BookView

MINUTE = 60_000
MAX_TIME = 253_402_300_799_000
SOURCE_SUFFIX = "binance_depth_ws_v1"
DEPTH_FIELDS = tuple(f"DEPTH_BIDS{i}" for i in range(5, -1, -1)) + tuple(
    f"DEPTH_ASKS{i}" for i in range(6)
)
_RADII = tuple(Decimal(item) for item in (".002", ".01", ".02", ".03", ".04", ".05"))
_MATH = Context(prec=256, rounding=ROUND_HALF_EVEN)


def _time(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("invalid depth observation timestamp")
    return value


def _precision(value):
    if type(value) is not int or not 0 <= value <= 18:
        raise ValidationError("depth precision must be a native integer in 0..18")
    return value


@dataclass(frozen=True)
class DepthMinute:
    observation: AuxiliaryInput
    raw_refs: tuple[str, ...]
    diagnostics: tuple[tuple[str, str | int], ...]


def depth_bins(view: BookView, *, price_precision: int, quote_precision: int):
    """Return known fields, spatial coverage and the actual rounded center.

    Snapshot bounds prove only their price range, irrespective of level count.
    The caller must separately prove socket continuity and minute freshness.
    """
    _precision(price_precision)
    _precision(quote_precision)
    if type(view) is not BookView or not view.bids or not view.asks:
        raise ValidationError("synchronized nonempty depth view required")
    if (
        type(view.update_id) is not int
        or not 0 <= view.update_id <= 2**63 - 1
        or type(view.bids) is not tuple
        or type(view.asks) is not tuple
        or max(len(view.bids), len(view.asks)) > 20000
    ):
        raise ValidationError("invalid bounded depth view identity")
    for levels in (view.bids, view.asks):
        for row in levels:
            if (
                type(row) is not tuple
                or len(row) != 2
                or any(
                    type(value) is not Decimal
                    or not value.is_finite()
                    or value <= 0
                    or len(value.as_tuple().digits) > 38
                    or abs(value.as_tuple().exponent) > 38
                    for value in row
                )
            ):
                raise ValidationError("depth levels require bounded positive Decimal values")
    if (
        any(
            type(value) is not Decimal or not value.is_finite() or value <= 0
            for value in (view.bid_floor, view.ask_ceiling)
        )
        or view.bids != tuple(sorted(view.bids, reverse=True))
        or view.asks != tuple(sorted(view.asks))
        or len({price for price, _ in view.bids}) != len(view.bids)
        or len({price for price, _ in view.asks}) != len(view.asks)
        or view.bids[0][0] >= view.asks[0][0]
    ):
        raise ValidationError("depth prices must form an ordered uncrossed book")
    with localcontext(_MATH):
        price_quantum = Decimal((0, (1,), -price_precision))
        quote_quantum = Decimal((0, (1,), -quote_precision))
        center = ((view.bids[0][0] + view.asks[0][0]) / 2).quantize(price_quantum)
        if center <= 0 or not view.bids[0][0] <= center <= view.asks[0][0]:
            raise ValidationError("configured depth price precision cannot represent the spread")
        sums = [[Decimal(0) for _ in range(6)] for _ in range(2)]
        known = [
            [view.bid_floor <= center * (1 - radius) for radius in _RADII],
            [view.ask_ceiling >= center * (1 + radius) for radius in _RADII],
        ]
        for side, levels in enumerate((view.bids, view.asks)):
            for price, quantity in levels:
                distance = ((center - price) if side == 0 else (price - center)) / center
                if distance < 0:
                    continue
                for index, radius in enumerate(_RADII):
                    if distance <= radius:
                        notional = (price * quantity).quantize(quote_quantum)
                        sums[side][index] = (sums[side][index] + notional).quantize(quote_quantum)
                        break
        fields = []
        for side, prefix in enumerate(("BIDS", "ASKS")):
            for index in range(6):
                if known[side][index]:
                    fields.append((f"DEPTH_{prefix}{index}", sums[side][index]))
        count = sum(sum(side) for side in known)
        return tuple(sorted(fields)), Decimal(count) / 12, center


class DepthMinuteSampler:
    """Bounded, immutable minute frontier. No old-minute revisions or carry rows."""

    def __init__(
        self,
        instrument,
        *,
        price_precision,
        quote_precision,
        max_raw_refs=8192,
        maximum_sample_age_ms=5000,
    ):
        if (
            not isinstance(instrument, Instrument)
            or instrument.provider != "binance"
            or instrument.market not in {Market.SPOT, Market.USDM}
        ):
            raise ValidationError("depth sampler requires a public Binance instrument")
        _precision(price_precision)
        _precision(quote_precision)
        if (
            type(max_raw_refs) is not int
            or not 1 <= max_raw_refs <= 8192
            or type(maximum_sample_age_ms) is not int
            or not 0 < maximum_sample_age_ms <= 60_000
        ):
            raise ValidationError("invalid depth sampler bounds")
        self.instrument = instrument
        self.price_precision, self.quote_precision = price_precision, quote_precision
        self.max_raw_refs, self.maximum_sample_age_ms = max_raw_refs, maximum_sample_age_ms
        self.generation = self.started_at_ms = None
        self.closed_until_ms = None
        self._points, self._gaps = {}, {}
        self._active = False
        self._last_event_ms = None

    def begin(self, generation, *, started_at_ms):
        _time(started_at_ms)
        if (
            type(generation) is not int
            or not 1 <= generation <= 2**63 - 1
            or self.generation is not None
            and generation <= self.generation
        ):
            raise ValidationError("depth sampler requires a new monotonic generation")
        self.generation, self.started_at_ms, self._active = generation, started_at_ms, True
        self._last_event_ms = None

    def break_stream(self, reason="stream_gap"):
        if type(reason) is not str or re.fullmatch(r"[a-z_]{1,64}", reason) is None:
            raise ValidationError("invalid depth gap reason")
        self._active = False
        for opened in self._points:
            self._gaps[opened] = reason

    def observe(self, view, *, event_time_ms, received_at_ms, accepted_at_ms, generation, raw_refs):
        for value in (event_time_ms, received_at_ms, accepted_at_ms):
            _time(value)
        if not self._active or type(generation) is not int or generation != self.generation:
            raise ProtocolError("stale or disconnected depth sample generation")
        if event_time_ms > accepted_at_ms or received_at_ms > accepted_at_ms:
            raise ProtocolError("future depth observation cannot be published")
        if (
            type(raw_refs) is not tuple
            or not 1 <= len(raw_refs) <= self.max_raw_refs
            or len(set(raw_refs)) != len(raw_refs)
            or any(
                type(value) is not str or re.fullmatch("[a-f0-9]{64}", value) is None
                for value in raw_refs
            )
        ):
            raise ValidationError("depth sample requires bounded original archive references")
        opened = event_time_ms // MINUTE * MINUTE
        if self.closed_until_ms is not None and opened < self.closed_until_ms:
            return (
                False  # Still apply the wire diff to the book, never rewrite a closed observation.
            )
        if self._last_event_ms is not None and event_time_ms < self._last_event_ms:
            raise ProtocolError("depth source event time regressed")
        if opened not in self._points and len(self._points) >= 3:
            raise ProtocolError("depth minute sample buffer capacity reached")
        if self._points and opened > min(self._points) + 2 * MINUTE:
            raise ProtocolError("finalize depth minutes before advancing beyond the bounded buffer")
        previous = self._points.get(opened)
        if previous is not None and (event_time_ms < previous[0] or accepted_at_ms < previous[2]):
            raise ProtocolError("depth sample time regressed within its minute")
        values, coverage, center = depth_bins(
            view, price_precision=self.price_precision, quote_precision=self.quote_precision
        )
        self._points[opened] = (
            event_time_ms,
            received_at_ms,
            accepted_at_ms,
            values,
            coverage,
            center,
            generation,
            self.started_at_ms,
            view.update_id,
            raw_refs,
        )
        self._last_event_ms = event_time_ms
        return True

    def finalize(self, open_time_ms, *, as_of_ms):
        _time(open_time_ms)
        _time(as_of_ms)
        if (
            open_time_ms % MINUTE
            or open_time_ms + MINUTE > as_of_ms
            or self.closed_until_ms is not None
            and open_time_ms < self.closed_until_ms
        ):
            raise ValidationError("depth finalization requires a new actually closed minute")
        point = self._points.get(open_time_ms)
        reason = self._gaps.get(open_time_ms)
        values, refs, coverage, event = (), (), Decimal(0), open_time_ms + MINUTE
        quality = "missing"
        if point is not None:
            (
                event,
                received,
                accepted,
                values,
                coverage,
                center,
                generation,
                started,
                update_id,
                refs,
            ) = point
            if accepted > as_of_ms:
                raise ProtocolError("depth normalization clock precedes its accepted source")
            if not self._active or generation != self.generation:
                reason = reason or "stream_gap"
            elif started > open_time_ms:
                reason = reason or "partial_connection_prefix"
            elif open_time_ms + MINUTE - event > self.maximum_sample_age_ms:
                reason = reason or "stale_snapshot"
            elif coverage < 1:
                reason = reason or "insufficient_price_coverage"
            quality = "observed" if reason is None else "partial"
            values = (
                *values,
                ("DEPTH_CENTER_PRICE", center),
                ("DEPTH_UPDATE_ID", update_id),
                ("DEPTH_GENERATION", generation),
                ("DEPTH_RECEIVED_AT_MS", received),
                ("DEPTH_PROFILE_VERSION", 1),
                ("DEPTH_EVENT_CLOCK", 1 if self.instrument.market is Market.USDM else 0),
            )
        observation = AuxiliaryInput(
            "depth",
            tuple(values),
            open_time_ms,
            open_time_ms + MINUTE,
            event,
            as_of_ms,
            quality,
            coverage,
            self.instrument.key + ":" + SOURCE_SUFFIX,
        )
        self.closed_until_ms = open_time_ms + MINUTE
        self._points = {key: value for key, value in self._points.items() if key > open_time_ms}
        self._gaps = {key: value for key, value in self._gaps.items() if key > open_time_ms}
        return DepthMinute(
            observation,
            refs,
            (
                ("reason", reason or ("sample_missing" if point is None else "observed")),
                ("profile", SOURCE_SUFFIX),
            ),
        )
