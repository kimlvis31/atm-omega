"""Small cached display query owner; no collection, write or trading responsibility."""

import time
from copy import deepcopy
from pathlib import Path
from threading import BoundedSemaphore, RLock

from omega.domain.calendar import CALENDAR_QUERY_LIMITS, MINUTE_MS, calendar_query_window
from omega.domain.errors import StorageError, ValidationError
from omega.infrastructure.calendar_candles import CalendarCandleReader
from omega.infrastructure.market_data import instrument_key
from omega.infrastructure.postgres.config import load_database_settings


class CalendarView:
    def __init__(
        self, root: Path, *, enabled=True, clock_ms=None, reader_factory=CalendarCandleReader
    ):
        self.root, self.enabled, self.reader_factory = Path(root), enabled, reader_factory
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.lock, self.cache = RLock(), {}
        self.in_flight, self.query_slots = set(), BoundedSemaphore(2)

    def __call__(self, key, period, limit=None, end_ms=None):
        instrument = instrument_key(key)
        if not isinstance(period, str) or period not in CALENDAR_QUERY_LIMITS:
            raise ValidationError("unsupported calendar period")
        limit = CALENDAR_QUERY_LIMITS[period] if limit is None else limit
        closed = self.clock_ms() // MINUTE_MS * MINUTE_MS
        end = closed if end_ms is None else end_ms
        try:
            begin, end = calendar_query_window(end, period, limit)
        except ValueError as exc:
            raise ValidationError(str(exc)) from None
        if end > closed:
            raise ValidationError("calendar range ends in the future")
        cache_key = key, period, limit, end

        def unavailable(reason):
            return {
                "schema_version": 1,
                "instrument": key,
                "period": period,
                "source": "unavailable",
                "begin_ms": begin,
                "end_ms": end,
                "closed_until_ms": closed,
                "candles": [],
                "quality": {
                    "status": "unavailable",
                    "source_minutes": 0,
                    "missing_minutes": 0,
                    "partial_periods": 0,
                    "bars": [],
                    "reason": reason,
                },
            }

        with self.lock:
            now = time.monotonic()
            self.cache = {k: v for k, v in self.cache.items() if now - v[0] < 2}
            if cache_key in self.cache:
                return deepcopy(self.cache[cache_key][1])
            # No slow database IO under the cache lock and no waiting request
            # threads: leave the core's other HTTP slots free for control/state.
            if cache_key in self.in_flight or not self.query_slots.acquire(blocking=False):
                return unavailable("calendar_database_busy")
            self.in_flight.add(cache_key)
        try:
            try:
                if not self.enabled:
                    raise StorageError("calendar database view disabled")
                reader = self.reader_factory(
                    load_database_settings(self.root / "database.config.toml")
                )
                result = reader.read(instrument, period, limit, end_ms=end, closed_until_ms=closed)
            except (StorageError, OSError, ValidationError):
                result = unavailable("calendar_database_unavailable")
            with self.lock:
                if len(self.cache) >= 12:
                    del self.cache[min(self.cache, key=lambda k: self.cache[k][0])]
                self.cache[cache_key] = time.monotonic(), result
            return deepcopy(result)
        finally:
            with self.lock:
                self.in_flight.discard(cache_key)
                self.query_slots.release()
