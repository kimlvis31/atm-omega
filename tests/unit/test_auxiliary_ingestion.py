"""Fake-port commit uncertainty, explicit source proof and bounded owner recovery."""

import threading
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256

import pytest

from omega.application.auxiliary_ingestion import (
    AuxiliaryIngestion,
    FinalizationDeferred,
    PendingAuxiliary,
)
from omega.application.trade_flow import TradeFlowCoverage
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import TradeFlowEvent
from omega.domain.models import Instrument, Market
from omega.ports.auxiliary import AuxiliaryWriteReceipt, StoredAuxiliary

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
SOURCE = "binance_usdm_agg_trade"


class Clock:
    def __init__(self, now=120001):
        self.now = now

    def __call__(self):
        return self.now


class FakeAuxStore:
    def __init__(self):
        self.rows, self.put_calls, self.get_calls = [], [], []
        self.fail_before = self.fail_after = False
        self.on_put = self.on_get = None
        self.closed = False

    def put(self, instrument, observation, *, expected_previous=None):
        self.put_calls.append((instrument, observation, expected_previous))
        if self.fail_before:
            self.fail_before = False
            raise StorageError("failed before commit")
        candidates = [
            row
            for row in self.rows
            if row.instrument == instrument
            and row.observation.source == observation.source
            and row.observation.window_start_ms == observation.window_start_ms
        ]
        previous = candidates[-1] if candidates else None
        if previous is not None and previous.observation == observation:
            result = AuxiliaryWriteReceipt(previous.revision, previous.checksum, False)
        else:
            if previous is None and expected_previous is not None:
                raise ProtocolError("missing revision anchor")
            if previous is not None and (
                previous.checksum != expected_previous
                or observation.available_at_ms <= previous.observation.available_at_ms
            ):
                raise ProtocolError("revision conflict")
            checksum = sha256(repr((instrument, observation)).encode()).hexdigest()
            stored = StoredAuxiliary(
                instrument,
                observation,
                previous.revision + 1 if previous else 1,
                checksum,
                previous.checksum if previous else None,
            )
            self.rows.append(stored)
            result = AuxiliaryWriteReceipt(stored.revision, checksum, True)
        if self.on_put:
            self.on_put()
        if self.fail_after:
            self.fail_after = False
            raise StorageError("commit outcome unknown")
        return result

    def get(self, instrument, channel, source, opening, *, as_of_ms):
        self.get_calls.append((instrument, channel, source, opening, as_of_ms))
        if self.on_get:
            self.on_get()
        rows = [
            row
            for row in self.rows
            if row.instrument == instrument
            and row.observation.channel == channel
            and row.observation.source == source
            and row.observation.window_start_ms == opening
            and row.observation.available_at_ms <= as_of_ms
        ]
        return rows[-1] if rows else None

    def close(self):
        self.closed = True


def event(identifier, when, **changes):
    return TradeFlowEvent(
        **{
            "instrument": INSTRUMENT,
            "aggregate_id": identifier,
            "first_trade_id": identifier * 3,
            "last_trade_id": identifier * 3 + 2,
            "price": Decimal("100.01"),
            "quantity": Decimal(".03"),
            "buyer_is_maker": False,
            "trade_time_ms": when,
            "available_at_ms": when + 1,
            "source": SOURCE,
            **changes,
        }
    )


def owner(*, store=None, clock=None, callback=None, generation=1, **limits):
    store, clock = store or FakeAuxStore(), clock or Clock()
    deliveries = []
    worker = AuxiliaryIngestion(
        INSTRUMENT,
        60000,
        store,
        callback or deliveries.append,
        source=SOURCE,
        clock_ms=clock,
        generation=generation,
        **limits,
    )
    return worker, store, clock, deliveries


def prepare(worker):
    generation = worker.generation
    worker.remember_before(event(1, 59999), generation=generation)
    worker.accept(event(2, 60000), generation=generation)
    worker.accept(event(3, 119999, buyer_is_maker=True), generation=generation)
    worker.accept(event(4, 120000), generation=generation)
    return worker.coverage(1, 4, generation=generation)


def stage(worker, *, coverage=None, batch_id="batch-1", expected_previous=None):
    return worker.stage_next(
        batch_id=batch_id,
        generation=worker.generation,
        coverage=coverage,
        expected_previous=expected_previous,
    )


def test_stage_is_data_only_and_flush_notifies_only_after_a_commit_receipt():
    worker, store, clock, deliveries = owner()
    proof = prepare(worker)
    snapshot = stage(worker, coverage=proof)
    assert not store.put_calls and not deliveries
    assert snapshot.observation.quality == "observed"
    assert snapshot.observation.mapping()["AGGTRADE_NOTIONALBUY"] == Decimal("3.0003")
    assert snapshot.observation.mapping()["AGGTRADE_NOTIONALSELL"] == Decimal("3.0003")
    assert snapshot.observation.available_at_ms == clock.now
    assert worker.status()["committed_next_ms"] == worker.status()["delivered_next_ms"] == 60000
    delivered = worker.flush(generation=1)
    assert deliveries == [delivered] and delivered.receipt.inserted
    assert delivered.observation == snapshot.observation
    assert worker.status()["committed_next_ms"] == worker.status()["delivered_next_ms"] == 120000
    assert worker.status()["buffered_events"] == 1
    assert worker.pending_snapshot() is None and worker.flush(generation=1) is None


def test_completion_clock_is_sampled_after_derivation_and_frozen_on_same_batch_retry():
    worker, store, clock, _ = owner()
    proof = prepare(worker)
    samples = iter((120010, 120075))
    worker.clock_ms = lambda: next(samples)
    snapshot = stage(worker, coverage=proof)
    assert snapshot.observation.available_at_ms == 120075
    assert all(
        item.available_at_ms == item.trade_time_ms + 1
        for item in (
            proof.before,
            proof.after,
        )
    )
    worker.clock_ms = lambda: pytest.fail("same staged batch re-sampled normalization availability")
    assert stage(worker, coverage=proof) is snapshot
    assert not store.put_calls


@pytest.mark.parametrize("unknown_commit", [False, True])
def test_store_failure_keeps_identical_pending_bytes_and_expected_cas_for_retry(unknown_commit):
    worker, store, clock, deliveries = owner()
    snapshot = stage(worker, coverage=prepare(worker))
    store.fail_after = unknown_commit
    store.fail_before = not unknown_commit
    with pytest.raises(StorageError):
        worker.flush(generation=1)
    assert not deliveries
    assert worker.pending_snapshot() is snapshot
    assert worker.status()["committed_next_ms"] == worker.status()["delivered_next_ms"] == 60000
    clock.now = 240000
    result = worker.flush(generation=1)
    assert len(store.rows) == 1 and len(deliveries) == 1
    assert store.put_calls[0] == store.put_calls[1]
    assert result.observation.available_at_ms == 120001
    assert result.receipt.inserted is (not unknown_commit)


def test_inserted_false_still_delivers_and_callback_failure_does_not_rewrite_storage():
    attempts, effects = [], set()

    def callback(delivery):
        attempts.append(delivery)
        effects.add(delivery.idempotency_key)
        if len(attempts) == 1:
            raise RuntimeError("consumer acknowledgment lost after effect")

    worker, store, _, _ = owner(callback=callback)
    snapshot = stage(worker, coverage=prepare(worker))
    store.put(INSTRUMENT, snapshot.observation)  # Previously committed but not consumed.
    with pytest.raises(RuntimeError):
        worker.flush(generation=1)
    assert worker.status()["committed_next_ms"] == 120000
    assert worker.status()["delivered_next_ms"] == 60000
    pending = worker.pending_snapshot()
    assert pending.receipt is not None and not pending.receipt.inserted
    worker.flush(generation=1)
    assert len(store.put_calls) == 2 and len(attempts) == 2 and len(effects) == 1
    assert worker.pending_snapshot() is None


def test_only_accepted_anchors_prove_zero_and_missing_does_not_mean_zero():
    worker, _, _, _ = owner()
    forged = TradeFlowCoverage(event(1, 50000), event(2, 120000), 1)
    with pytest.raises(ValidationError):
        stage(worker, coverage=forged)
    worker.remember_before(forged.before, generation=1)
    worker.accept(forged.after, generation=1)
    snapshot = stage(worker, coverage=worker.coverage(1, 2, generation=1))
    assert snapshot.observation.quality == "observed"
    assert snapshot.observation.mapping()["AGGTRADE_EVENTCOUNT"] == 0
    other, _, _, _ = owner()
    missing = stage(other).observation
    assert missing.quality == "missing" and missing.values == ()


def test_id_gap_and_wrong_anchor_do_not_silently_claim_complete_observation():
    worker, _, _, _ = owner()
    worker.remember_before(event(1, 59999), generation=1)
    worker.accept(event(3, 60000), generation=1)  # Missing ID2.
    worker.accept(event(4, 120000), generation=1)
    proof = worker.coverage(1, 4, generation=1)
    snapshot = stage(worker, coverage=proof)
    assert snapshot.observation.quality == "partial" and snapshot.observation.coverage == 0
    second, _, _, _ = owner()
    proof = prepare(second)
    with pytest.raises(ValidationError):
        stage(second, coverage=replace(proof, after=replace(proof.after, quantity=Decimal("1"))))
    assert second.pending_snapshot() is None


def test_pending_record_applies_backpressure_and_never_changes_to_a_new_batch():
    worker, _, _, _ = owner()
    snapshot = stage(worker, coverage=prepare(worker))
    for operation in (
        lambda: worker.accept(event(5, 125000), generation=1),
        lambda: worker.remember_before(event(1, 59999), generation=1),
        lambda: stage(worker, batch_id="different"),
        lambda: worker.reset(60000, generation=2),
    ):
        with pytest.raises(ProtocolError):
            operation()
    assert worker.pending_snapshot() is snapshot


def test_repair_requires_real_later_clock_not_a_manufactured_plus_one():
    first, store, clock, _ = owner()
    first.accept(event(2, 60000), generation=1)
    prior = first.flush(generation=1)  # No staged record is a no-op.
    assert prior is None
    stage(first)
    receipt = first.flush(generation=1).receipt
    repair, _, _, deliveries = owner(store=store, clock=clock, generation=2)
    proof = prepare(repair)
    with pytest.raises(FinalizationDeferred) as deferred:
        stage(repair, coverage=proof, expected_previous=receipt.checksum)
    assert deferred.value.strict and deferred.value.threshold_ms == clock.now
    assert repair.pending_snapshot() is None and not deliveries
    clock.now = 250000
    snapshot = stage(repair, coverage=proof, expected_previous=receipt.checksum)
    assert snapshot.observation.available_at_ms == 250000
    store.fail_after = True
    with pytest.raises(StorageError):
        repair.flush(generation=2)
    clock.now = 500000
    result = repair.flush(generation=2)
    assert result.receipt.revision == 2 and not result.receipt.inserted
    assert len(store.rows) == 2
    assert store.put_calls[-1] == store.put_calls[-2]


def test_clock_regression_future_receipt_and_open_minute_defer_without_mutating_data():
    clock = Clock(119000)
    worker, store, _, _ = owner(clock=clock)
    with pytest.raises(FinalizationDeferred):
        stage(worker)
    with pytest.raises(FinalizationDeferred):
        worker.accept(event(2, 120000), generation=1)
    assert worker.status()["buffered_events"] == 0 and worker.pending_snapshot() is None
    clock.now = 120001
    snapshot = stage(worker)
    clock.now = 100000
    with pytest.raises(FinalizationDeferred):
        worker.flush(generation=1)
    assert not store.put_calls and worker.pending_snapshot() is snapshot
    clock.now = 130000
    assert worker.flush(generation=1).observation.available_at_ms == 120001


@pytest.mark.parametrize("committed", [False, True])
def test_restart_restores_fixed_pending_and_requires_future_archive_replay(committed):
    def fail_callback(_):
        raise RuntimeError("delivery failure")

    worker, store, clock, _ = owner(callback=fail_callback)
    snapshot = stage(worker, coverage=prepare(worker))
    if committed:
        with pytest.raises(RuntimeError):
            worker.flush(generation=1)
        snapshot = worker.pending_snapshot()
    report = worker.close()
    assert report["pending"] == snapshot
    restarted, _, _, delivered = owner(store=store, clock=clock, generation=2)
    resume = restarted.restore_pending(snapshot, generation=2)
    assert resume == {"replay_required_from_ms": 120000, "buffer_restored": False}
    result = restarted.flush(generation=2)
    assert result.observation == snapshot.observation and len(delivered) == 1
    assert len(store.rows) == 1 and restarted.status()["buffered_events"] == 0
    if committed:
        assert (
            len(store.put_calls) == 1
            and store.get_calls[-1][-1] == snapshot.observation.available_at_ms
        )


def test_restored_forged_receipt_or_different_binding_cannot_publish():
    worker, store, clock, _ = owner()
    snapshot = stage(worker, coverage=prepare(worker))
    forged = replace(snapshot, receipt=AuxiliaryWriteReceipt(1, "f" * 64, True))
    restarted, _, _, delivered = owner(store=store, clock=clock, generation=2)
    restarted.restore_pending(forged, generation=2)
    with pytest.raises(ProtocolError, match="matching committed"):
        restarted.flush(generation=2)
    assert not delivered and restarted.status()["committed_next_ms"] == 60000
    worker.store = FakeAuxStore()
    with pytest.raises(ProtocolError, match="retired binding"):
        worker.flush(generation=1)


def test_duplicate_receipt_restore_uses_committed_identity_not_a_previously_unused_cas():
    worker, store, clock, _ = owner()
    snapshot = stage(worker, coverage=prepare(worker))
    # Equal decoded put is a no-op before CAS evaluation, including existing
    # revisions above one. The notification must survive that legitimate receipt.
    checksum = sha256(b"existing-revision-two").hexdigest()
    prior = "a" * 64
    store.rows.append(StoredAuxiliary(INSTRUMENT, snapshot.observation, 2, checksum, prior))

    def fail(_):
        raise RuntimeError("callback failed")

    other, _, _, _ = owner(store=store, clock=clock, callback=fail, generation=2)
    other.restore_pending(snapshot, generation=2)
    with pytest.raises(RuntimeError):
        other.flush(generation=2)
    saved = other.pending_snapshot()
    assert (
        saved.receipt.revision == 2
        and not saved.receipt.inserted
        and saved.expected_previous is None
    )
    assert saved.committed_previous == prior
    restored, _, _, delivered = owner(store=store, clock=clock, generation=3)
    restored.restore_pending(saved, generation=3)
    assert restored.flush(generation=3).receipt.checksum == checksum and len(delivered) == 1


def test_typed_semantic_duplicate_delivers_the_actual_stored_decimal_representation():
    attempts = []

    def callback(delivery):
        attempts.append(delivery)
        if len(attempts) == 1:
            raise RuntimeError("consumer retry")

    worker, store, _, _ = owner(callback=callback)
    snapshot = stage(worker, coverage=prepare(worker))
    canonical = replace(
        snapshot.observation,
        values=tuple(
            (key, value.quantize(Decimal(".00000000")) if type(value) is Decimal else value)
            for key, value in snapshot.observation.values
        ),
    )
    committed = store.put(INSTRUMENT, canonical)
    assert repr(canonical) != repr(snapshot.observation) and canonical == snapshot.observation
    with pytest.raises(RuntimeError):
        worker.flush(generation=1)
    pending = worker.pending_snapshot()
    assert pending.receipt.checksum == committed.checksum and not pending.receipt.inserted
    assert repr(pending.observation) == repr(canonical)
    assert repr(attempts[0].observation) == repr(canonical)
    assert (
        sha256(repr((INSTRUMENT, attempts[0].observation)).encode()).hexdigest()
        == committed.checksum
    )
    worker.flush(generation=1)
    assert len(store.put_calls) == 2 and len(attempts) == 2


def test_post_commit_lookup_failure_preserves_receipt_and_retries_only_lookup():
    worker, store, _, deliveries = owner()
    snapshot = stage(worker, coverage=prepare(worker))

    def fail_lookup():
        raise StorageError("lookup failed after successful commit receipt")

    store.on_get = fail_lookup
    with pytest.raises(StorageError):
        worker.flush(generation=1)
    pending = worker.pending_snapshot()
    assert pending.receipt is not None and pending.observation == snapshot.observation
    assert len(store.rows) == len(store.put_calls) == 1 and not deliveries
    assert worker.status()["committed_next_ms"] == 60000
    store.on_get = None
    result = worker.flush(generation=1)
    assert len(store.put_calls) == 1 and len(deliveries) == 1
    assert result.receipt == pending.receipt


def test_a_claimed_new_insert_cannot_pin_a_different_revision_predecessor():
    worker, store, _, deliveries = owner()
    stage(worker, coverage=prepare(worker))

    def corrupt_previous():
        store.rows[-1] = replace(store.rows[-1], revision=2, previous_checksum="b" * 64)

    store.on_put = corrupt_previous
    with pytest.raises(ProtocolError, match="matching committed"):
        worker.flush(generation=1)
    assert worker.pending_snapshot().receipt is not None and not deliveries
    assert worker.status()["committed_next_ms"] == 60000


def test_reset_during_repair_lookup_retires_late_stage_without_writing_or_advancing():
    first, store, clock, _ = owner()
    stage(first)
    receipt = first.flush(generation=1).receipt
    clock.now = 250000
    repair, _, _, delivered = owner(store=store, clock=clock, generation=2)
    proof = prepare(repair)
    store.on_get = lambda: repair.reset(120000, generation=3)
    with pytest.raises(ProtocolError, match="generation"):
        stage(repair, coverage=proof, expected_previous=receipt.checksum)
    assert repair.pending_snapshot() is None and not delivered
    assert len(store.put_calls) == 1 and repair.status()["committed_next_ms"] == 120000


def test_same_payload_with_changed_value_dtype_cannot_validate_a_restored_receipt():
    worker, store, clock, _ = owner()
    snapshot = stage(worker, coverage=prepare(worker))
    result = store.put(INSTRUMENT, snapshot.observation)
    saved = replace(snapshot, receipt=result)
    wrong_values = tuple(
        (name, Decimal(value) if name == "AGGTRADE_EVENTCOUNT" else value)
        for name, value in snapshot.observation.values
    )
    store.rows[0] = replace(
        store.rows[0], observation=replace(snapshot.observation, values=wrong_values)
    )
    restored, _, _, deliveries = owner(store=store, clock=clock, generation=2)
    restored.restore_pending(saved, generation=2)
    with pytest.raises(ProtocolError, match="matching committed"):
        restored.flush(generation=2)
    assert not deliveries


def test_generation_reset_reports_discarded_raw_events_and_invalidates_old_anchors():
    worker, store, _, _ = owner()
    prepare(worker)
    report = worker.reset(120000, generation=2)
    assert report == {"previous_generation": 1, "discarded_events": 3, "discarded_minutes": 2}
    assert worker.status()["accepted_anchors"] == worker.status()["buffered_events"] == 0
    assert worker.status()["committed_next_ms"] == worker.status()["delivered_next_ms"] == 120000
    with pytest.raises(ProtocolError):
        worker.accept(event(5, 130000), generation=1)
    with pytest.raises(ValidationError):
        worker.coverage(1, 4, generation=2)
    assert not store.put_calls


def test_late_old_owner_commit_after_close_preserves_unknown_snapshot_without_dispatch():
    worker, store, clock, deliveries = owner()
    snapshot = stage(worker, coverage=prepare(worker))
    entered, release = threading.Event(), threading.Event()

    def block():
        entered.set()
        assert release.wait(3)

    store.on_put = block
    failures = []

    def flush():
        try:
            worker.flush(generation=1)
        except Exception as error:
            failures.append(error)

    thread = threading.Thread(target=flush)
    thread.start()
    try:
        assert entered.wait(3)
        report = worker.close()
        assert report["pending"] == snapshot and report["commit_outcome"] == "unconfirmed"
    finally:
        release.set()
        thread.join(3)
    assert (
        not thread.is_alive() and len(failures) == 1 and isinstance(failures[0], InterruptedError)
    )
    assert not deliveries and len(store.rows) == 1
    store.on_put = None
    new_owner, _, _, received = owner(store=store, clock=clock, generation=2)
    new_owner.restore_pending(report["pending"], generation=2)
    assert not new_owner.flush(generation=2).receipt.inserted and len(received) == 1


def test_callback_retirement_keeps_receipt_for_idempotent_new_owner_delivery():
    container, attempts = {}, []

    def retire(delivery):
        attempts.append(delivery)
        container["closed"] = container["worker"].close()

    worker, store, clock, _ = owner(callback=retire)
    container["worker"] = worker
    stage(worker, coverage=prepare(worker))
    with pytest.raises(InterruptedError):
        worker.flush(generation=1)
    snapshot = container["closed"]["pending"]
    assert snapshot.receipt is not None and len(attempts) == 1
    assert worker.status()["delivered_next_ms"] == 60000
    restarted, _, _, received = owner(store=store, clock=clock, generation=2)
    restarted.restore_pending(snapshot, generation=2)
    assert restarted.flush(generation=2).idempotency_key == attempts[0].idempotency_key
    assert len(received) == 1


def test_accept_conflicting_prior_anchor_is_atomic_and_buffer_capacity_is_bounded():
    worker, _, _, _ = owner(max_pending_events=1, max_anchors=2)
    worker.remember_before(event(1, 59999), generation=1)
    with pytest.raises((ProtocolError, ValidationError)):
        worker.accept(event(1, 60000, quantity=Decimal(".07")), generation=1)
    assert worker.status()["buffered_events"] == 0
    worker.accept(event(2, 60000), generation=1)
    with pytest.raises(ProtocolError, match="capacity"):
        worker.accept(event(3, 61000), generation=1)
    assert worker.status()["buffered_events"] == 1 and worker.status()["accepted_anchors"] <= 2


@pytest.mark.parametrize(
    "changes",
    [
        {"batch_id": "../outside"},
        {"original_generation": True},
        {"expected_previous": "not-a-hash"},
        {"receipt": False},
    ],
)
def test_pending_snapshot_rejects_malformed_control_metadata(changes):
    worker, _, _, _ = owner()
    snapshot = stage(worker)
    with pytest.raises(ValidationError):
        replace(snapshot, **changes)
    assert isinstance(snapshot, PendingAuxiliary)


def test_first_revision_pending_cannot_claim_a_committed_predecessor():
    worker, _, _, _ = owner()
    snapshot = stage(worker)
    receipt = AuxiliaryWriteReceipt(1, "a" * 64, True)
    with pytest.raises(ValidationError):
        replace(snapshot, receipt=receipt, committed_previous="b" * 64)
    # A later revision can still await its postcommit canonical predecessor lookup.
    assert (
        replace(snapshot, receipt=AuxiliaryWriteReceipt(2, "c" * 64, False)).receipt.revision == 2
    )
