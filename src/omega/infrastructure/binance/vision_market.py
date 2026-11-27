"""Verified Vision history, local observations and the still-unpublished REST tail."""

import json
from dataclasses import asdict

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Candle, decimal
from omega.ports.collection import MinuteObservation

MINUTE = 60_000


class VisionMarketData:
    native_observations = True

    def __init__(self, rest, vision, *, cached=None, clock_ms=None):
        self.rest, self.vision, self.cached = rest, vision, cached
        self.market, self.client, self.prefix = rest.market, rest.client, rest.prefix
        self.raw_archive = rest.raw_archive
        self.clock_ms = clock_ms or self.client.clock_ms

    @property
    def last_symbol_diagnostics(self):
        return self.rest.last_symbol_diagnostics

    def symbols(self):
        return self.rest.symbols()

    def server_time(self):
        return self.rest.server_time()

    def first_available_minute(self, instrument, *, trades=False):
        return self.discovery(instrument, trades=trades)["first_observed_minute_ms"]

    def discovery(self, instrument, *, trades=False):
        """Actual observation floor; REST retention is never listing inception proof."""
        stamp = self.vision.first_timestamp(instrument, "agg_trade" if trades else "kline")
        archived = stamp is not None
        minute = (
            stamp // MINUTE * MINUTE
            if archived
            else self.rest.first_available_minute(instrument, trades=trades)
        )
        if type(minute) is not int or minute < 0 or minute % MINUTE:
            raise ProtocolError("invalid first observed minute")
        return {
            "first_observed_minute_ms": minute,
            "source": "vision" if archived else "rest",
            "discovery_state": "observed" if archived else "bounded_fallback",
            "unknown_before": not archived,
            "fallback_reason": None if archived else "no_archive_observation",
            "coverage_state": "partial",
            "full_requested_range_verified": False,
        }

    @staticmethod
    def _missing(rows, start, end):
        gaps = []
        opening = None
        for stamp in range(start, end, MINUTE):
            if stamp not in rows and opening is None:
                opening = stamp
            if stamp in rows and opening is not None:
                gaps.append((opening, stamp))
                opening = None
        if opening is not None:
            gaps.append((opening, end))
        return gaps

    def closed_observations(self, instrument, start_ms, end_ms, *, now_ms):
        if (
            type(start_ms) is not int
            or type(end_ms) is not int
            or not 0 <= start_ms < end_ms
            or start_ms % MINUTE
            or end_ms % MINUTE
            or end_ms - start_ms > 20000 * MINUTE
        ):
            raise ValidationError("bounded aligned Vision history window required")
        end_ms = min(end_ms, now_ms // MINUTE * MINUTE)
        if end_ms <= start_ms:
            return ()
        rows = {}
        if self.cached is not None:
            rows = {
                observation.candle.open_time_ms: observation
                for observation in self.cached(instrument, start_ms, end_ms)
                if not self.raw_archive or observation.raw is not None
            }
        for low, high in self._missing(rows, start_ms, end_ms):
            for item in self.vision.iter_rows(instrument, "kline", low, high):
                values = item.values
                opened = item.timestamp_ms
                if len(values) != 12:
                    raise ProtocolError("unsupported Vision kline columns")
                scale = 1000 if int(values[0]) >= 100_000_000_000_000 else 1
                closed = int(values[6]) // scale + 1
                candle = Candle(
                    instrument, opened, closed, *(decimal(value) for value in values[1:6])
                )
                if not low <= opened < high or closed > now_ms:
                    raise ProtocolError("Vision candle is outside its requested closed range")
                prior = rows.get(opened)
                if prior is not None and prior.candle != candle:
                    raise ProtocolError("conflicting verified Vision candle")
                raw = json.dumps(
                    {
                        "format": "binance_vision_csv",
                        "row": item.row_number,
                        "values": values,
                        "receipt": asdict(item.receipt),
                    },
                    default=str,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                accepted_at = self.clock_ms()
                if accepted_at < max(item.receipt.received_at_ms, candle.close_time_ms):
                    raise ProtocolError(
                        "actual normalization clock predates the closed archive observation"
                    )
                rows[opened] = MinuteObservation(
                    candle,
                    "vision",
                    accepted_at,
                    raw,
                    "vision",
                    item.receipt.received_at_ms,
                )
        # A published daily file is authoritative for that date. Missing archive
        # observations remain gaps. Only the unpublished recent tail uses REST.
        files = self.vision.files(instrument, "kline", start_ms, end_ms)
        covered_days = {file.day_start_ms for file in files}
        for low, high in self._missing(rows, start_ms, end_ms):
            cursor = low
            while cursor < high:
                day = cursor // 86400000 * 86400000
                until = min(high, day + 86400000)
                if day not in covered_days:
                    observations = (
                        self.rest.closed_observations(instrument, cursor, until, now_ms=now_ms)
                        if self.raw_archive is not None
                        else tuple(
                            MinuteObservation(candle, "rest", self.clock_ms())
                            for candle in self.rest.closed_minutes(
                                instrument, cursor, until, now_ms=now_ms
                            )
                        )
                    )
                    rows.update((row.candle.open_time_ms, row) for row in observations)
                cursor = until
        return tuple(rows[key] for key in sorted(rows))

    def closed_minutes(self, instrument, start_ms, end_ms, *, now_ms):
        return tuple(
            row.candle
            for row in self.closed_observations(instrument, start_ms, end_ms, now_ms=now_ms)
        )
