"""Actual committed OHLCV for causal NN inputs, never historical availability proof.

The database stores local reception time but not a durable-commit timestamp.
Therefore every returned row becomes available no earlier than the actual wall
clock after this read/connection cleanup completes. Historical received_ms alone
can never promote a newly read row into an earlier decision. The scheduler must
sample its decision clock after read_window returns.
"""

# Imports
import os
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from omega.domain.errors import ProtocolError, StorageError
from omega.domain.inference import digest, timestamp
from omega.infrastructure.candles import candle_from_row
from omega.infrastructure.causal_market import (
    read_committed_window,
    select_committed_rows,
    validate_window,
)
from omega.infrastructure.causal_market_read import (
    InferenceReadScope,
    ReadCleanupError,
    ReadScopeReceipt,
)
from omega.infrastructure.postgres.config import database_destination_id, load_database_settings
from omega.research.datasets import MarketRow
from omega.runtime.causal_nn_proof import CommittedInferenceObservation, wall_ms


def _decimal_text(value):
    text = format(value, "f")
    if value == 0:
        return "0"
    return text.rstrip("0").rstrip(".") if "." in text else text


@dataclass(frozen=True)
class SourceReadRetirementSnapshot:
    generation: str
    scope_token: str
    source_closed: bool
    complete_default_factory: bool
    legacy_read_attempted: bool
    identity_intact: bool
    read_scope: ReadScopeReceipt
    physical_confirmed: bool
    actor_recovery_authorized: bool = False


class CommittedInferenceMarketSource:
    """Constructor has no IO; each bounded read owns and closes its own DB handle."""

    def __init__(self, root, *, store_factory=None, clock_ms=wall_ms):
        self.root = Path(root)
        self.store_factory, self.clock_ms = store_factory, clock_ms
        self._pid, self._thread = os.getpid(), threading.current_thread()
        self._original_store_factory = store_factory
        self._default_store_factory = store_factory is None
        self._read_scope = InferenceReadScope(uuid4().hex)
        self._original_read_scope = self._read_scope
        self._closed = self._legacy_read_attempted = self._identity_unknown = False
        self._read_started = False

    def _owner_guard(self):
        if os.getpid() != self._pid or threading.current_thread() is not self._thread:
            raise StorageError(
                "inference market source requires its original PID and Thread object"
            )

    def _identity_intact(self):
        if self._read_scope is not self._original_read_scope:
            self._identity_unknown = True
        if self.store_factory is not self._original_store_factory:
            if (
                not self._identity_unknown
                and self._default_store_factory
                and not self._read_started
                and not self._closed
                and callable(self.store_factory)
                and self._original_read_scope.snapshot().reads == 0
            ):
                # Existing callers select an observation factory before their
                # first read. This one-way compatibility downgrade grants no
                # original connection or restoration authority.
                self._original_store_factory = self.store_factory
                self._default_store_factory = False
            else:
                self._identity_unknown = True
        return not self._identity_unknown

    def read_window(self, instrument, start_ms, end_ms):
        self._owner_guard()
        if not self._identity_intact() or self._closed:
            raise StorageError("original inference market source admission is closed")
        self._read_started = True
        count = validate_window(instrument, start_ms, end_ms)
        started_ms = timestamp(self.clock_ms())
        settings = load_database_settings(self.root / "database.config.toml")
        namespace = database_destination_id(settings)
        if self._default_store_factory:
            try:
                rows = self._original_read_scope.read(
                    settings,
                    lambda connection: select_committed_rows(
                        connection, instrument, start_ms, end_ms, count
                    ),
                    max_rows=count + 1,
                )
            except ReadCleanupError:
                raise StorageError(
                    "committed inference market connection cleanup unconfirmed"
                ) from None
            except Exception:
                raise StorageError("committed inference market query unavailable") from None
        else:
            # Compatibility reads do not gain an original connection registry.
            self._legacy_read_attempted = True
            rows = read_committed_window(
                settings, instrument, start_ms, end_ms, store_factory=self.store_factory
            )
        observed_ms = timestamp(self.clock_ms())
        if observed_ms < started_ms:
            raise ProtocolError("clock regressed while observing committed inference input")
        if len(rows) > count:
            raise ProtocolError("committed inference query exceeded bounded minute count")
        result, previous_ms = [], start_ms - 60000
        for values in rows:
            if type(values) not in {tuple, list} or len(values) != 10:
                raise ProtocolError("invalid committed inference row schema")
            key, *values = values
            if key != instrument.key:
                raise ProtocolError("committed inference query returned another instrument")
            if any(
                not isinstance(value, datetime)
                or value.utcoffset() is None
                or value.microsecond % 1000
                for value in values[:2]
            ):
                raise ProtocolError(
                    "committed inference timestamps must retain exact UTC milliseconds"
                )
            candle = candle_from_row(instrument, values[:7])
            source, received_ms = values[7:]
            timestamp(received_ms)
            if (
                not previous_ms < candle.open_time_ms < end_ms
                or candle.open_time_ms < start_ms
                or source not in {"rest", "websocket"}
                or not candle.close_time_ms <= received_ms <= observed_ms
            ):
                raise ProtocolError("causal input requires ordered original closed observations")
            row = MarketRow(
                instrument.key,
                candle.open_time_ms,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
            )
            # Normalize Decimal spellings so scale-only differences do not imply
            # different logical stored observations; do not round exact values.
            normalized = row.as_dict()
            for name in ("open", "high", "low", "close", "volume"):
                normalized[name] = _decimal_text(getattr(row, name))
            commit = digest(
                {
                    "scope": "observed_committed_ohlcv_identity",
                    "database_namespace": namespace,
                    "row": normalized,
                    "source": source,
                    "received_ms": received_ms,
                }
            )
            result.append(CommittedInferenceObservation(row, max(received_ms, observed_ms), commit))
            previous_ms = candle.open_time_ms
        if not self._identity_intact() or self._closed:
            raise StorageError("original inference market source closed before publication")
        return tuple(result)

    def close(self):
        self._owner_guard()
        self._identity_intact()
        self._closed = True
        try:
            self._original_read_scope.close()
        except Exception:
            raise StorageError(
                "committed inference market connection cleanup unconfirmed"
            ) from None
        result = self.retirement_status()
        if not result.identity_intact:
            raise StorageError("original inference market source identity is unconfirmed")
        return result

    def retirement_status(self):
        self._owner_guard()
        identity_intact = self._identity_intact()
        receipt = self._original_read_scope.snapshot()
        return SourceReadRetirementSnapshot(
            self._original_read_scope.generation,
            self._original_read_scope.token,
            self._closed,
            self._default_store_factory,
            self._legacy_read_attempted,
            identity_intact,
            receipt,
            self._closed
            and self._default_store_factory
            and not self._legacy_read_attempted
            and identity_intact
            and type(receipt) is ReadScopeReceipt
            and receipt.physical_confirmed,
        )
