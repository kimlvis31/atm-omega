"""Bounded two-phase public trade normalization and commit-before-delivery.

The runtime must durably archive stage_next()'s snapshot before flush(). Unknown
commits reuse that exact observation; neither a retry nor a reconnect changes its
availability. Consumer effects are at-least-once and must deduplicate receipts.
"""

# Imports
import re
from collections import OrderedDict
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, replace
from decimal import Decimal
from threading import Lock, RLock

from omega.application.trade_flow import MAX_TIME, MINUTE, TradeFlowAggregator, TradeFlowCoverage
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput, TradeFlowEvent
from omega.domain.models import Instrument
from omega.ports.auxiliary import AuxiliaryStore, AuxiliaryWriteReceipt, StoredAuxiliary


class FinalizationDeferred(ValidationError):
    """Wait for a real clock condition; never turn the threshold into a fake receipt."""

    def __init__(self, threshold_ms: int, *, strict=False):
        self.threshold_ms, self.strict = threshold_ms, strict
        super().__init__("actual normalization clock has not reached the required boundary")


@dataclass(frozen=True, slots=True)
class PendingAuxiliary:
    instrument: Instrument
    batch_id: str
    observation: AuxiliaryInput
    original_generation: int
    expected_previous: str | None = None
    receipt: AuxiliaryWriteReceipt | None = None
    committed_previous: str | None = None

    def __post_init__(self):
        if (
            not isinstance(self.instrument, Instrument)
            or not isinstance(self.observation, AuxiliaryInput)
            or not isinstance(self.batch_id, str)
            or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", self.batch_id)
            or type(self.original_generation) is not int
            or not 0 <= self.original_generation <= 2**63 - 1
            or self.expected_previous is not None
            and (
                not isinstance(self.expected_previous, str)
                or not re.fullmatch(r"[a-f0-9]{64}", self.expected_previous)
            )
            or self.receipt is not None
            and not isinstance(self.receipt, AuxiliaryWriteReceipt)
            or self.committed_previous is not None
            and (
                self.receipt is None
                or self.receipt.revision == 1
                or not isinstance(self.committed_previous, str)
                or not re.fullmatch(r"[a-f0-9]{64}", self.committed_previous)
            )
        ):
            raise ValidationError("invalid pending auxiliary identity or receipt")
        observation = self.observation
        if (
            observation.channel != "trade_flow"
            or observation.window_start_ms % MINUTE
            or observation.event_time_ms != observation.window_end_ms
            or observation.quality not in {"observed", "partial", "missing"}
            or observation.coverage
            != (Decimal(1) if observation.quality == "observed" else Decimal(0))
        ):
            raise ValidationError("pending auxiliary is not a normalized trade minute")
        values = observation.mapping()
        decimal_fields = {
            "AGGTRADE_QUANTITYBUY",
            "AGGTRADE_QUANTITYSELL",
            "AGGTRADE_NOTIONALBUY",
            "AGGTRADE_NOTIONALSELL",
        }
        count_fields = {"AGGTRADE_NTRADESBUY", "AGGTRADE_NTRADESSELL", "AGGTRADE_EVENTCOUNT"}
        if observation.quality == "missing":
            if values:
                raise ValidationError("missing trade minute cannot contain manufactured totals")
        elif (
            set(values) != decimal_fields | count_fields
            or any(
                type(values[key]) is not Decimal or not values[key].is_finite() or values[key] < 0
                for key in decimal_fields
            )
            or any(type(values[key]) is not int or values[key] < 0 for key in count_fields)
            or values["AGGTRADE_NTRADESBUY"] + values["AGGTRADE_NTRADESSELL"]
            < values["AGGTRADE_EVENTCOUNT"]
            or observation.quality == "partial"
            and not values["AGGTRADE_EVENTCOUNT"]
        ):
            raise ValidationError("invalid pending trade totals or dtypes")
        if values:
            for side in ("BUY", "SELL"):
                count = values["AGGTRADE_NTRADES" + side]
                quantity = values["AGGTRADE_QUANTITY" + side]
                notional = values["AGGTRADE_NOTIONAL" + side]
                if (
                    (count == 0) != (quantity == 0)
                    or (quantity == 0) != (notional == 0)
                    or values["AGGTRADE_EVENTCOUNT"] == 0
                    and count != 0
                ):
                    raise ValidationError("pending trade counts and positive amounts disagree")


@dataclass(frozen=True, slots=True)
class AuxiliaryDelivery:
    instrument: Instrument
    batch_id: str
    observation: AuxiliaryInput
    receipt: AuxiliaryWriteReceipt
    generation: int
    previous_checksum: str | None = None

    @property
    def idempotency_key(self):
        return (
            self.instrument.key,
            self.observation.channel,
            self.observation.source,
            self.observation.window_start_ms,
            self.receipt.revision,
            self.receipt.checksum,
        )


@dataclass
class _Pending:
    snapshot: PendingAuxiliary
    candidate: TradeFlowAggregator
    verify_receipt: bool = False
    store: object = None
    callback: object = None


def _same_observation(left, right):
    return (
        left == right
        and len(left.values) == len(right.values)
        and all(type(a[1]) is type(b[1]) for a, b in zip(left.values, right.values, strict=True))
    )


class AuxiliaryIngestion:
    """One owner, one staged minute. Incoming events are backpressured until flush.

    Stage snapshots contain data only. Restoring one retries that minute, but all
    later buffered source events/anchors must be replayed from the raw archive.
    This coordinator does not own/close its injected store or implement an outbox.
    """

    def __init__(
        self,
        instrument: Instrument,
        start_ms: int,
        store: AuxiliaryStore,
        on_committed: Callable[[AuxiliaryDelivery], None],
        *,
        source: str,
        clock_ms: Callable[[], int],
        generation=0,
        max_pending_events=4096,
        max_minutes=16,
        max_anchors=4098,
    ):
        self._aggregator = TradeFlowAggregator(
            instrument,
            start_ms,
            source=source,
            generation=generation,
            max_pending_events=max_pending_events,
            max_minutes=max_minutes,
        )
        if (
            not callable(clock_ms)
            or not callable(on_committed)
            or type(max_anchors) is not int
            or not 2 <= max_anchors <= 40002
        ):
            raise ValidationError("invalid auxiliary clock, callback or anchor capacity")
        self.instrument, self.source = instrument, source
        self.store, self.on_committed, self.clock_ms = store, on_committed, clock_ms
        self._limits = (max_pending_events, max_minutes)
        self._max_anchors = max_anchors
        self._generation, self._epoch = generation, object()
        self._lock, self._operation = RLock(), Lock()
        self._anchors, self._buffered = OrderedDict(), {}
        self._pending = None
        self._committed_next_ms = self._delivered_next_ms = start_ms
        self._last_clock_ms, self._fault = None, None
        self._closed = False

    @property
    def generation(self):
        return self._generation

    def _active(self, generation):
        if self._closed:
            raise InterruptedError("auxiliary owner is closed")
        if type(generation) is not int or generation != self._generation:
            raise ProtocolError("stale auxiliary generation")

    def _current(self, generation, epoch, store, callback, pending=None):
        self._active(generation)
        if self._epoch is not epoch or self.store is not store or self.on_committed is not callback:
            raise ProtocolError("auxiliary binding changed during an operation")
        if pending is not None and self._pending is not pending:
            raise ProtocolError("auxiliary pending generation changed")

    def _now(self):
        value = self.clock_ms()
        if type(value) is not int or not 0 <= value <= MAX_TIME:
            raise ValidationError("invalid actual auxiliary acceptance clock")
        if self._last_clock_ms is not None and value < self._last_clock_ms:
            raise FinalizationDeferred(self._last_clock_ms)
        self._last_clock_ms = value
        return value

    def _remember(self, event, now):
        previous = self._anchors.get(event.aggregate_id)
        if previous is not None:
            if replace(event, available_at_ms=previous[0].available_at_ms) != previous[0]:
                raise ProtocolError("accepted auxiliary anchor conflicts with prior observation")
            return
        self._anchors[event.aggregate_id] = (event, now)
        while len(self._anchors) > self._max_anchors:
            self._anchors.popitem(last=False)

    def _check_known(self, event):
        previous = self._anchors.get(event.aggregate_id)
        if (
            previous is not None
            and replace(event, available_at_ms=previous[0].available_at_ms) != previous[0]
        ):
            raise ProtocolError("accepted auxiliary anchor conflicts with prior observation")

    def _event(self, event, now):
        if (
            not isinstance(event, TradeFlowEvent)
            or event.instrument != self.instrument
            or event.source != self.source
        ):
            raise ValidationError("auxiliary event belongs to another dataset")
        if event.available_at_ms > now:
            raise FinalizationDeferred(event.available_at_ms)

    def accept(self, event: TradeFlowEvent, *, generation):
        with self._lock:
            self._active(generation)
            if self._pending is not None or self._operation.locked():
                raise ProtocolError(
                    "flush the staged auxiliary minute before accepting more events"
                )
            now = self._now()
            self._event(event, now)
            self._check_known(event)
            inserted = self._aggregator.accept(event, generation=generation)
            self._remember(event, now)
            if inserted:
                self._buffered[event.aggregate_id] = event
            return inserted

    def remember_before(self, event: TradeFlowEvent, *, generation):
        """Register an actual pre-start/previous-minute event as boundary evidence."""
        with self._lock:
            self._active(generation)
            if self._pending is not None or self._operation.locked():
                raise ProtocolError("cannot replace boundary evidence during auxiliary flush")
            now = self._now()
            self._event(event, now)
            if event.trade_time_ms >= self._aggregator.next_open_ms:
                raise ValidationError("before-anchor must precede the next auxiliary minute")
            self._remember(event, now)

    def coverage(self, before_id, after_id, *, generation):
        with self._lock:
            self._active(generation)
            if any(
                type(value) is not int or not 0 <= value <= 2**63 - 1
                for value in (before_id, after_id)
            ):
                raise ValidationError("coverage anchor IDs must be exact nonnegative integers")
            try:
                before, after = self._anchors[before_id][0], self._anchors[after_id][0]
            except (KeyError, TypeError):
                raise ValidationError(
                    "coverage requires previously accepted source anchors"
                ) from None
            return TradeFlowCoverage(before, after, generation)

    def _coverage(self, coverage, generation):
        if coverage is None:
            return None
        if not isinstance(coverage, TradeFlowCoverage) or coverage.generation != generation:
            raise ValidationError("invalid auxiliary coverage generation")
        known = self.coverage(
            coverage.before.aggregate_id, coverage.after.aggregate_id, generation=generation
        )
        if known != coverage:
            raise ValidationError("coverage must reference the accepted source observations")
        return known

    def stage_next(self, *, batch_id, generation, coverage=None, expected_previous=None):
        if not self._operation.acquire(blocking=False):
            raise ProtocolError("an auxiliary operation is already running")
        try:
            with self._lock:
                self._active(generation)
                if (
                    not isinstance(batch_id, str)
                    or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", batch_id)
                    or expected_previous is not None
                    and (
                        not isinstance(expected_previous, str)
                        or not re.fullmatch(r"[a-f0-9]{64}", expected_previous)
                    )
                ):
                    raise ValidationError("invalid auxiliary batch identity or revision anchor")
                if self._pending is not None:
                    snapshot = self._pending.snapshot
                    if (
                        snapshot.batch_id != batch_id
                        or snapshot.expected_previous != expected_previous
                    ):
                        raise ProtocolError("a different auxiliary batch is already pending")
                    return snapshot
                epoch, store, callback = self._epoch, self.store, self.on_committed
                now = self._now()
                opening = self._aggregator.next_open_ms
                if now < opening + MINUTE:
                    raise FinalizationDeferred(opening + MINUTE)
                proof = self._coverage(coverage, generation)
                candidate = deepcopy(self._aggregator)
            previous = None
            if expected_previous is not None:
                previous = store.get(
                    self.instrument, "trade_flow", self.source, opening, as_of_ms=now
                )
                if (
                    not isinstance(previous, StoredAuxiliary)
                    or previous.instrument != self.instrument
                    or previous.observation.channel != "trade_flow"
                    or previous.observation.source != self.source
                    or previous.observation.window_start_ms != opening
                    or previous.checksum != expected_previous
                    or previous.observation.available_at_ms > now
                ):
                    raise ProtocolError("auxiliary repair anchor is absent or changed")
            observation = candidate.finalize(
                opening, as_of_ms=now, generation=generation, coverage=proof
            )
            with self._lock:
                self._current(generation, epoch, store, callback)
                # This one completion receipt is frozen. Retrying never re-samples it.
                completed = self._now()
                if previous is not None and completed <= previous.observation.available_at_ms:
                    raise FinalizationDeferred(previous.observation.available_at_ms, strict=True)
                snapshot = PendingAuxiliary(
                    self.instrument,
                    batch_id,
                    replace(observation, available_at_ms=completed),
                    generation,
                    expected_previous,
                )
                self._pending = _Pending(snapshot, candidate, store=store, callback=callback)
                self._fault = None
                return snapshot
        finally:
            self._operation.release()

    def pending_snapshot(self):
        with self._lock:
            return self._pending.snapshot if self._pending is not None else None

    def restore_pending(self, snapshot: PendingAuxiliary, *, generation):
        with self._lock:
            self._active(generation)
            if (
                self._pending is not None
                or self._operation.locked()
                or self._buffered
                or self._anchors
                or self._committed_next_ms != self._delivered_next_ms
            ):
                raise ProtocolError("restore pending auxiliary data into a fresh empty owner")
            if (
                not isinstance(snapshot, PendingAuxiliary)
                or snapshot.instrument != self.instrument
                or snapshot.observation.source != self.source
                or snapshot.observation.window_start_ms != self._aggregator.next_open_ms
                or snapshot.original_generation > generation
            ):
                raise ValidationError("pending auxiliary snapshot does not match this owner")
            candidate = TradeFlowAggregator(
                self.instrument,
                snapshot.observation.window_end_ms,
                source=self.source,
                generation=generation,
                max_pending_events=self._limits[0],
                max_minutes=self._limits[1],
            )
            self._pending = _Pending(
                snapshot,
                candidate,
                verify_receipt=snapshot.receipt is not None,
                store=self.store,
                callback=self.on_committed,
            )
            return {
                "replay_required_from_ms": snapshot.observation.window_end_ms,
                "buffer_restored": False,
            }

    def flush(self, *, generation):
        if not self._operation.acquire(blocking=False):
            raise ProtocolError("an auxiliary operation is already running")
        try:
            with self._lock:
                self._active(generation)
                pending = self._pending
                if pending is None:
                    return None
                epoch, store, callback = self._epoch, self.store, self.on_committed
                snapshot = pending.snapshot
                if (
                    pending.store is not store
                    or pending.callback is not callback
                    or snapshot.instrument != self.instrument
                    or snapshot.observation.source != self.source
                ):
                    raise ProtocolError("pending auxiliary data belongs to a retired binding")
                if self._now() < snapshot.observation.available_at_ms:
                    raise FinalizationDeferred(snapshot.observation.available_at_ms)
            if snapshot.receipt is None:
                receipt = store.put(
                    snapshot.instrument,
                    snapshot.observation,
                    expected_previous=snapshot.expected_previous,
                )
                if not isinstance(receipt, AuxiliaryWriteReceipt):
                    raise ProtocolError("auxiliary storage did not return a commit receipt")
                with self._lock:
                    self._current(generation, epoch, store, callback, pending)
                    pending.snapshot = replace(snapshot, receipt=receipt)
                    pending.verify_receipt = True
                    snapshot = pending.snapshot
            if pending.verify_receipt:
                stored = store.get(
                    snapshot.instrument,
                    "trade_flow",
                    self.source,
                    snapshot.observation.window_start_ms,
                    as_of_ms=snapshot.observation.available_at_ms,
                )
                if (
                    not isinstance(stored, StoredAuxiliary)
                    or stored.instrument != snapshot.instrument
                    or not _same_observation(stored.observation, snapshot.observation)
                    or stored.revision != snapshot.receipt.revision
                    or stored.checksum != snapshot.receipt.checksum
                    or snapshot.receipt.inserted
                    and stored.previous_checksum != snapshot.expected_previous
                    or snapshot.committed_previous is not None
                    and stored.previous_checksum != snapshot.committed_previous
                ):
                    raise ProtocolError(
                        "restored auxiliary receipt has no matching committed record"
                    )
                # The port permits typed-semantic duplicates (Decimal scale may
                # differ). Pin the actual stored representation with its receipt,
                # keeping the original write CAS condition separately immutable.
                snapshot = replace(
                    snapshot,
                    observation=stored.observation,
                    committed_previous=stored.previous_checksum,
                )
            receipt = snapshot.receipt
            with self._lock:
                self._current(generation, epoch, store, callback, pending)
                pending.snapshot = snapshot
                pending.verify_receipt = False
                self._aggregator = pending.candidate
                ending = snapshot.observation.window_end_ms
                self._committed_next_ms = ending
                self._buffered = {
                    key: event
                    for key, event in self._buffered.items()
                    if event.trade_time_ms >= ending
                }
                delivery = AuxiliaryDelivery(
                    snapshot.instrument,
                    snapshot.batch_id,
                    snapshot.observation,
                    receipt,
                    generation,
                    snapshot.committed_previous,
                )
            callback(delivery)  # inserted=False still requires delivery/idempotent consumption.
            with self._lock:
                self._current(generation, epoch, store, callback, pending)
                self._delivered_next_ms = ending
                self._pending, self._fault = None, None
            return delivery
        except Exception as exc:
            with self._lock:
                if not self._closed:
                    self._fault = type(exc).__name__
            raise
        finally:
            self._operation.release()

    def status(self):
        with self._lock:
            snapshot = self.pending_snapshot()
            return {
                "generation": self._generation,
                "closed": self._closed,
                "committed_next_ms": self._committed_next_ms,
                "delivered_next_ms": self._delivered_next_ms,
                "pending": "commit"
                if snapshot is not None and snapshot.receipt is None
                else "notification"
                if snapshot is not None
                else None,
                "pending_batch_id": snapshot.batch_id if snapshot else None,
                "buffered_events": len(self._buffered),
                "accepted_anchors": len(self._anchors),
                "fault": self._fault,
            }

    def reset(self, start_ms, *, generation):
        with self._lock:
            self._active(self._generation)
            if self._pending is not None:
                raise ProtocolError("preserve/flush the pending auxiliary record before reset")
            report = self._aggregator.reset(start_ms, generation=generation)
            self._generation, self._epoch = generation, object()
            self._committed_next_ms = self._delivered_next_ms = start_ms
            self._anchors, self._buffered, self._fault = OrderedDict(), {}, None
            return report

    def close(self):
        with self._lock:
            snapshot = self.pending_snapshot()
            self._closed, self._epoch = True, object()
            report = {
                "generation": self._generation,
                "pending": snapshot,
                "commit_outcome": "unconfirmed"
                if snapshot is not None and snapshot.receipt is None
                else "confirmed"
                if snapshot is not None
                else "none",
                "discarded_events": len(self._buffered),
                "discarded_minutes": len(
                    {event.trade_time_ms // MINUTE for event in self._buffered.values()}
                ),
                "requires_archive_replay": True,
            }
            self._anchors, self._buffered = OrderedDict(), {}
            return report
