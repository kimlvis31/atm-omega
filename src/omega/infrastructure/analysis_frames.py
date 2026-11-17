"""Worker-owned, cutoff-bound committed candles and real auxiliary frame inputs.

This source neither collects nor creates observations. Missing coverage remains
missing for the engine's precommit gate; no manifest is written by this adapter.
"""

import re
import threading
from dataclasses import replace
from pathlib import Path

from omega.application.market_frame_builder import MarketFrameBuilder
from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candle_feed import CommittedCandleSource
from omega.infrastructure.postgres.config import (
    database_destination_id,
    load_database_settings,
)
from omega.ports.auxiliary import AuxiliaryReadRange

MAX_TIME = 253_402_300_799_000


class _BoundCandleSource(CommittedCandleSource):
    def __init__(self, owner, connect):
        super().__init__(owner.root, connect=connect)
        self.owner = owner

    def _connect(self):
        return self._connect_settings(self.owner._checked_settings())


class CommittedAnalysisFrameSource:
    def __init__(self, root, *, connect=None, store_factory=None):
        if (connect is not None and not callable(connect)) or (
            store_factory is not None and not callable(store_factory)
        ):
            raise ValidationError("analysis frame source factories must be callable")
        self.root = Path(root)
        self._store_factory = store_factory or PostgresAuxiliaryStore
        self._candles = _BoundCandleSource(self, connect)
        self._binding_lock = threading.Lock()
        self._owner_thread = self._destination_id = self._auxiliary_store = None
        self._auxiliary_connect_timeout_s = None
        self._closed = False

    @property
    def destination_id(self):
        return self._destination_id

    def _check_owner(self):
        with self._binding_lock:
            if self._closed:
                raise StorageError("analysis frame source is closed")
            identity = threading.get_ident()
            if self._owner_thread is None:
                self._owner_thread = identity
            elif self._owner_thread != identity:
                raise ValidationError("analysis frame source belongs to another worker thread")

    def _checked_settings(self):
        self._check_owner()
        settings = load_database_settings(self.root / "database.config.toml")
        if settings.host not in {"127.0.0.1", "::1", "localhost"}:
            raise ValidationError("analysis frames require an explicit local Omega database")
        identity = database_destination_id(settings)
        if self._destination_id is None:
            self._destination_id = identity
        elif identity != self._destination_id:
            raise ValidationError("analysis frame destination changed; create a new source owner")
        return settings

    def _store(self, *, maximum_connect_timeout_s=None):
        settings = self._checked_settings()
        if maximum_connect_timeout_s is not None:
            settings = replace(
                settings,
                connect_timeout_s=min(settings.connect_timeout_s, maximum_connect_timeout_s),
            )
            if (
                self._auxiliary_store is not None
                and self._auxiliary_connect_timeout_s > maximum_connect_timeout_s
            ):
                raise ValidationError("bounded chart reads require a fresh source owner")
        if self._auxiliary_store is None:
            store = self._store_factory(settings)
            if getattr(store, "destination_id", None) != self._destination_id:
                self._auxiliary_store = store
                self._closed = True
                try:
                    store.close()
                except Exception:
                    raise StorageError("mismatched analysis store cleanup failed") from None
                self._auxiliary_store = None
                raise ValidationError("analysis auxiliary store destination does not match")
            self._auxiliary_store = store
            self._auxiliary_connect_timeout_s = settings.connect_timeout_s
        elif getattr(self._auxiliary_store, "destination_id", None) != self._destination_id:
            raise ValidationError("analysis auxiliary store destination changed")
        return self._auxiliary_store

    def read_revisions(self, ranges, *, as_of_ms, limit=10000, check_stop=None):
        """Chart-owned bounded native snapshot, retaining original revision evidence."""
        if check_stop is not None and not callable(check_stop):
            raise ValidationError("analysis revision stop check must be callable")
        if (
            type(ranges) is not tuple
            or not 1 <= len(ranges) <= 64
            or any(not isinstance(row, AuxiliaryReadRange) for row in ranges)
            or type(as_of_ms) is not int
            or not 0 <= as_of_ms <= MAX_TIME
            or type(limit) is not int
            or not 1 <= limit <= 10000
            or sum((row.end_ms - row.start_ms) // 60000 for row in ranges) > limit
        ):
            raise ValidationError("invalid bounded analysis revision request")
        spans = {}
        for row in ranges:
            existing = spans.setdefault((row.instrument, row.channel, row.source), [])
            if any(row.start_ms < end and start < row.end_ms for start, end in existing):
                raise ValidationError("analysis revision request ranges overlap")
            existing.append((row.start_ms, row.end_ms))
        if check_stop is not None:
            check_stop()
        store = self._store(maximum_connect_timeout_s=5)
        if check_stop is not None:
            check_stop()
        result = store.read_revisions(ranges, as_of_ms=as_of_ms, limit=limit, check_stop=check_stop)
        if check_stop is not None:
            check_stop()
        self._checked_settings()
        return result

    def read(self, keys, start_ms, end_ms, *, limit=4096, as_of_ms=None):
        rows = self._candles.read(keys, start_ms, end_ms, limit=limit, as_of_ms=as_of_ms)
        self._checked_settings()  # A late config change cannot publish a mixed-destination read.
        return rows

    def get_auxiliary(self, instrument, channel, source, open_time_ms, *, as_of_ms):
        """Replay one saved native pin at its original actual availability cutoff."""
        value = self._store().get(instrument, channel, source, open_time_ms, as_of_ms=as_of_ms)
        self._checked_settings()
        return value

    def check_destination(self, expected_hash):
        """Check worker/config identity before any replay DB connection is opened."""
        if type(expected_hash) is not str or re.fullmatch(r"[a-f0-9]{64}", expected_hash) is None:
            raise ValidationError("invalid expected analysis destination")
        self._checked_settings()
        if self._destination_id != expected_hash:
            raise ValidationError("analysis replay destination does not match its pinned policy")

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
    ):
        if (
            not isinstance(instrument, Instrument)
            or any(type(value) is not int for value in (start_ms, end_ms, as_of_ms, limit))
            or not 0 <= start_ms < end_ms <= as_of_ms <= MAX_TIME
            or start_ms % 60000
            or end_ms % 60000
            or not 1 <= limit <= 512
            or (end_ms - start_ms) // 60000 > limit
            or type(channels) is not tuple
            or len(channels) > 3
            or any(
                type(channel) is not str or channel not in {"trade_flow", "metric", "depth"}
                for channel in channels
            )
            or len(set(channels)) != len(channels)
            or type(maximum_metric_age_ms) is not int
            or not 0 <= maximum_metric_age_ms <= 86400000
            or retrospective_until_ms is not None
            and (
                type(retrospective_until_ms) is not int
                or not 0 <= retrospective_until_ms <= as_of_ms
                or retrospective_until_ms % 60000
            )
        ):
            raise ValidationError(
                "invalid bounded analysis frame request or unsupported input channel"
            )
        candles = self.read((instrument.key,), start_ms, end_ms, limit=limit, as_of_ms=as_of_ms)
        # A zero-row read has no selected frames and needs no auxiliary DB handle.
        needs_auxiliary = (
            "trade_flow" in channels
            or "depth" in channels
            or (
                "metric" in channels
                and instrument.market != Market.SPOT
                and maximum_metric_age_ms > 0
            )
        )
        store = self._store() if needs_auxiliary and candles else None
        builder = MarketFrameBuilder(store, maximum_metric_age_ms=maximum_metric_age_ms)
        result = tuple(
            builder.prepare(
                candle,
                as_of_ms=as_of_ms,
                channels=channels,
                retrospective=retrospective_until_ms is not None
                and candle.close_time_ms <= retrospective_until_ms,
            )
            for candle in candles
        )
        self._checked_settings()
        return result

    def close(self):
        with self._binding_lock:
            if self._owner_thread is not None and self._owner_thread != threading.get_ident():
                raise ValidationError("analysis frame source must close on its worker thread")
            self._closed = True
        try:
            if self._auxiliary_store is not None:
                self._auxiliary_store.close()
                self._auxiliary_store = None
        finally:
            self._candles.close()
