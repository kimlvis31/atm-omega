"""Typed outbox + actual factual owner over fake atomic stores/journals; no I/O."""

from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.application.margin_account import MarginAccountOwner, MarginCommitUnknown
from omega.application.margin_cost import (
    MarginCostPolicy,
    approve_cost_order,
    freeze_cost_pending,
)
from omega.application.margin_execution import FrozenPendingOrder
from omega.application.margin_funding import (
    FundingPoint,
    frozen_funding_document,
    prepare_funding_batch,
)
from omega.application.margin_risk import approve_order
from omega.application.virtual_margin import (
    AccountAnchor,
    JournalReceipt,
    VirtualMarginEngine,
    VirtualMarginRecord,
)
from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.domain.margin import (
    MarginAccount,
    MarginBracket,
    MarginBracketSnapshot,
    MarginMode,
    MarginPosition,
    MarkSnapshot,
)
from omega.domain.models import Candle, Instrument, Market, Side
from omega.domain.trading import OrderProposal
from omega.ports.journal import Durability

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")


def initial(quantity="0", cash="1000"):
    return MarginAccount(
        "a",
        "USDT",
        D(cash),
        tuple(
            MarginPosition(
                value,
                value.symbol,
                MarginMode.CROSS,
                10,
                D(100),
                D(quantity) if value == BTC else D(0),
                D(100) if value == BTC and D(quantity) else D(0),
            )
            for value in (BTC, ETH)
        ),
    )


def marks():
    return tuple(MarkSnapshot(value, D(100), 0, 0, "test_mark") for value in (BTC, ETH))


def brackets():
    return tuple(
        MarginBracketSnapshot(
            value,
            (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
            0,
            0,
            "test_bracket",
            "a" * 64,
        )
        for value in (BTC, ETH)
    )


def pending(account, identity="order", instrument=BTC, *, reducing=False, reservations=()):
    proposal = OrderProposal(Side.SELL if reducing else Side.BUY, D(1), reducing=reducing)
    original = approve_order(
        account,
        identity,
        proposal,
        instrument=instrument,
        owner_id=instrument.symbol,
        as_of_ms=0,
        marks=marks(),
        brackets=brackets(),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
        reservations=reservations,
    )
    return FrozenPendingOrder(original, 60000)


def candles():
    return tuple(
        Candle(value, 60000, 120000, D(100), D(120), D(80), D(110), D(10)) for value in (BTC, ETH)
    )


class Store:
    def __init__(self):
        self.value, self.calls = None, []
        self.lose_at = None

    def load(self, account_id):
        return self.value

    def save(self, account, *, expected_sequence):
        self.calls.append((account, expected_sequence))
        if expected_sequence is None:
            assert self.value is None
        elif len(self.value.ledger) != expected_sequence:
            raise ProtocolError("CAS conflict")
        self.value = account
        if len(account.ledger) == self.lose_at:
            self.lose_at = None
            raise StorageError("commit ACK loss")


class Journal:
    def __init__(self):
        self.entries, self.calls = None, []
        self.durability = Durability.PRIMARY
        self.before_failure = self.ack_loss = False
        self.read_failure = False

    def read(self, account_id):
        if self.read_failure:
            raise StorageError("sensitive source details")
        return None if self.entries is None else tuple(self.entries)

    def append(self, record):
        self.calls.append(record)
        if self.before_failure:
            self.before_failure = False
            raise StorageError("before append")
        if self.durability != Durability.VOLATILE:
            if self.entries is None:
                self.entries = []
            existing = next(
                (
                    value
                    for value in self.entries
                    if value.record.record_id == record.record_id
                    and value.durability == self.durability
                ),
                None,
            )
            if existing:
                if existing.record != record:
                    raise ProtocolError("same record ID changed")
            else:
                self.entries.append(JournalReceipt(record, self.durability))
        if self.ack_loss:
            self.ack_loss = False
            raise StorageError("append ACK loss")
        return self.durability


def setup(*, quantity="0"):
    source, store, journal = initial(quantity), Store(), Journal()
    owner = MarginAccountOwner(source, store, create_if_missing=True)
    engine = VirtualMarginEngine(owner, journal, create_if_missing=True)
    return source, store, journal, owner, engine


def submit(engine, order):
    return engine.submit_intent(order, marks=marks(), brackets=brackets())


def prepare(engine):
    return engine.prepare_batch(candles(), as_of_ms=120000, brackets=brackets())


@pytest.mark.parametrize("boundary", ["source_before", "source_ack", "account_ack"])
def test_funding_unknown_recovers_original_fact_and_never_resettles(boundary):
    _, store, journal, _, engine = setup(quantity="1")
    point = FundingPoint(BTC, 0, 1, 0, D(".001"), D(100))
    plan = engine.prepare_funding((point,), as_of_ms=60000)
    original = frozen_funding_document(plan)
    journal.before_failure = boundary == "source_before"
    journal.ack_loss = boundary == "source_ack"
    store.lose_at = 1 if boundary == "account_ack" else None
    with pytest.raises((StorageError, MarginCommitUnknown)):
        engine.commit_funding(plan)
    assert engine.snapshot().state == "unknown"
    with pytest.raises(TradingBlocked):
        engine.prepare_funding((point,), as_of_ms=60001)
    recovered = engine.reconcile()
    assert recovered.account.cross_wallet == D("999.90000000")
    assert len(recovered.account.ledger) == 1
    settled = engine.committed_funding_batches(0)
    assert len(settled) == 1 and frozen_funding_document(settled[0]) == original
    calls = len(store.calls), len(journal.calls)
    assert engine.reconcile().account == recovered.account
    assert (len(store.calls), len(journal.calls)) == calls


@pytest.mark.parametrize("rate", ["-.001", ".001"])
def test_confirmed_fallback_funding_blocks_entry_but_keeps_original_protection(rate):
    source, store, journal, _, engine = setup(quantity="1")
    reduce = pending(source, reducing=True)
    submit(engine, reduce)
    journal.durability = Durability.FALLBACK
    plan = engine.prepare_funding((FundingPoint(BTC, 0, 1, 0, D(rate), D(100)),), as_of_ms=60000)
    engine.commit_funding(plan)
    assert engine.snapshot().paused and engine.original_intent(reduce.order_id) == reduce
    with pytest.raises(TradingBlocked):
        engine.manual_resume("no-new-risk")
    engine.commit_batch(prepare(engine))
    assert store.value.positions[0].quantity == 0
    assert len(store.value.ledger) == 2
    assert store.value.ledger[1].fact.order_id == reduce.order_id
    assert engine.original_intent(reduce.order_id).original.request_fingerprint == (
        reduce.original.request_fingerprint
    )


def test_unrecorded_funding_preserves_unknown_without_claiming_following_protection_is_safe():
    source, store, journal, _, engine = setup(quantity="1")
    submit(engine, pending(source, reducing=True))
    journal.durability = Durability.VOLATILE
    plan = engine.prepare_funding((FundingPoint(BTC, 0, 1, 0, D(".001"), D(100)),), as_of_ms=60000)
    with pytest.raises(StorageError):
        engine.commit_funding(plan)
    assert engine.snapshot().pending_record_id is not None
    assert store.value == source
    with pytest.raises(TradingBlocked):
        prepare(engine)
    journal.durability = Durability.PRIMARY
    assert engine.reconcile().account == plan.candidate_account


@pytest.mark.parametrize("lost_sequence", [1, 2])
def test_funding_multi_position_prefix_ack_loss_finishes_remaining_original_zero_fact(
    lost_sequence,
):
    source, store, journal, _, engine = setup(quantity="1")
    points = tuple(FundingPoint(i, 0, 1, 0, D(".001"), D(100)) for i in (BTC, ETH))
    plan = engine.prepare_funding(points, as_of_ms=60000)
    store.lose_at = lost_sequence
    with pytest.raises(MarginCommitUnknown):
        engine.commit_funding(plan)
    result = engine.reconcile()
    assert result.account == plan.candidate_account
    assert len(result.account.ledger) == 2 and result.account.ledger[-1].fact.amount == 0
    assert len(store.calls) == 3  # Initial registration and each exact prefix once.
    restored = VirtualMarginEngine(MarginAccountOwner(source, store), journal)
    assert restored.snapshot().paused and restored.snapshot().account == result.account


def test_current_minute_open_cannot_create_exposure_for_that_minutes_funding():
    source, store, journal, _, engine = setup()
    engine.manual_resume("resume")
    submit(engine, pending(source))
    engine.commit_batch(prepare(engine))
    original = store.value
    funding_point = FundingPoint(BTC, 60000, 60001, 60000, D(".001"), D(100))
    with pytest.raises(ProtocolError, match="precede"):
        engine.prepare_funding((funding_point,), as_of_ms=120000)
    # Intrinsically valid source data still needs the correct journal phase.
    late = prepare_funding_batch(original, (funding_point,), as_of_ms=120000)
    journal.append(engine._record(late, ("funding", late.batch_id)))
    restored = VirtualMarginEngine(MarginAccountOwner(source, store), journal)
    assert restored.snapshot().state == "conflict" and store.value == original


def test_one_settlement_has_one_account_batch_even_when_second_batch_is_other_instrument():
    _, store, _, _, engine = setup(quantity="1")
    engine.commit_funding(
        engine.prepare_funding((FundingPoint(BTC, 0, 1, 0, D(".001"), D(100)),), as_of_ms=60000)
    )
    before = store.value
    with pytest.raises(ProtocolError, match="precede"):
        engine.prepare_funding((FundingPoint(ETH, 0, 1, 0, D(".001"), D(100)),), as_of_ms=60000)
    assert store.value is before


def test_new_anchor_restart_manual_pause_durable_intent_and_full_fill_ledger_settlement():
    source, store, journal, owner, engine = setup()
    assert engine.snapshot().paused and isinstance(journal.entries[0].record.payload, AccountAnchor)
    with pytest.raises(TradingBlocked):
        submit(engine, pending(source))
    engine.manual_resume("resume")
    order = pending(source)
    submit(engine, order)
    assert engine.snapshot().pending == (order,)
    writes = len(journal.calls)
    assert submit(engine, order) is order and len(journal.calls) == writes
    engine.pause_entries("pause")
    result = engine.commit_batch(prepare(engine))
    assert result.paused and result.pending == () and len(result.account.ledger) == 1
    assert result.account.cross_wallet == D("999.9")
    restored = VirtualMarginEngine(MarginAccountOwner(source, store), journal)
    assert restored.snapshot().state == "ready" and restored.snapshot().paused
    assert restored.snapshot().pending == () and len(store.value.ledger) == 1


@pytest.mark.parametrize("ack_loss", [False, True], ids=["before-write", "ack-loss"])
def test_intent_append_unknown_reuses_exact_record_and_accepts_no_other_mutation(ack_loss):
    source, _, journal, _, engine = setup()
    engine.manual_resume("resume")
    journal.before_failure, journal.ack_loss = not ack_loss, ack_loss
    order = pending(source)
    with pytest.raises(StorageError):
        submit(engine, order)
    fixed = journal.calls[-1]
    assert engine.snapshot().state == "unknown" and engine.snapshot().pending == ()
    with pytest.raises(TradingBlocked):
        submit(engine, pending(source, "another"))
    result = engine.reconcile()
    assert result.pending == (order,) and result.state == "ready"
    if not ack_loss:
        assert journal.calls[-1] is fixed
    assert all(value == fixed for value in journal.calls if value.record_id == fixed.record_id)


def test_frozen_batch_append_ack_loss_recovers_original_facts_without_new_prepare_clock_or_price():
    source, store, journal, _, engine = setup()
    engine.manual_resume("resume")
    submit(engine, pending(source))
    plan = prepare(engine)
    journal.ack_loss = True
    before = len(store.calls)
    with pytest.raises(StorageError):
        engine.commit_batch(plan)
    assert len(store.calls) == before
    result = engine.reconcile()
    assert result.pending == () and result.account.ledger[-1].fact is plan.facts[0]
    assert result.account.ledger[-1].fact.available_at_ms == 120000


def test_mid_batch_account_ack_loss_restarts_from_authorized_prefix_and_remains_entry_paused():
    source, store, journal, _, engine = setup()
    engine.manual_resume("resume")
    first = pending(source, "btc")
    submit(engine, first)
    second = pending(source, "eth", ETH, reservations=(first.original,))
    submit(engine, second)
    plan = prepare(engine)
    store.lose_at = 1
    with pytest.raises(MarginCommitUnknown):
        engine.commit_batch(plan)
    assert len(store.value.ledger) == 1 and engine.snapshot().state == "unknown"
    writes = len(store.calls)
    restored = VirtualMarginEngine(MarginAccountOwner(source, store), journal)
    snapshot = restored.snapshot()
    assert snapshot.state == "ready" and snapshot.paused and snapshot.pending == ()
    assert len(snapshot.account.ledger) == 2 and len(store.calls) == writes + 1
    assert tuple(entry.fact for entry in snapshot.account.ledger) == plan.facts


def test_live_reconcile_completes_durable_batch_while_paused_without_resaving_acknowledged_prefix():
    source, store, _, _, engine = setup()
    engine.manual_resume("resume")
    submit(engine, pending(source))
    engine.pause_entries("pause")
    store.lose_at = 1
    with pytest.raises(MarginCommitUnknown):
        engine.commit_batch(prepare(engine))
    writes = len(store.calls)
    snapshot = engine.reconcile()
    assert snapshot.paused and snapshot.pending == () and len(store.calls) == writes


def test_fallback_protection_is_durable_but_new_risk_and_volatile_virtual_facts_are_blocked():
    source, store, journal, _, engine = setup(quantity="1")
    engine.manual_resume("resume-before-fallback")
    journal.durability = Durability.FALLBACK
    reduce = pending(source, reducing=True)
    submit(engine, reduce)  # Restart pause does not prevent known protection.
    engine.commit_batch(prepare(engine))
    assert store.value.positions[0].quantity == 0 and engine.snapshot().paused
    with pytest.raises(TradingBlocked):
        engine.manual_resume("resume")
    source, store, journal, _, engine = setup(quantity="1")
    submit(engine, pending(source, reducing=True))
    journal.durability = Durability.VOLATILE
    with pytest.raises(StorageError):
        engine.commit_batch(prepare(engine))
    assert store.value.positions[0].quantity == 1 and engine.snapshot().state == "unknown"
    journal.durability = Durability.PRIMARY
    assert engine.reconcile().account.positions[0].quantity == 0


def test_fallback_entry_intent_waits_for_primary_confirmation_and_exact_duplicate_is_merged():
    source, _, journal, _, engine = setup()
    engine.manual_resume("resume")
    order = pending(source)
    journal.durability = Durability.FALLBACK
    with pytest.raises(StorageError):
        submit(engine, order)
    journal.durability = Durability.PRIMARY
    assert engine.reconcile().pending == (order,)
    assert (
        len(
            [
                value
                for value in journal.entries
                if isinstance(value.record.payload, FrozenPendingOrder)
            ]
        )
        == 2
    )
    assert engine.reconcile().pending == (order,)


def test_missing_history_and_corruption_never_recreate_account_or_generate_a_frame_fill():
    source, store, journal, _, engine = setup()
    writes = len(store.calls)
    journal.entries = None
    with pytest.raises(StorageError):
        engine.reconcile()
    assert len(store.calls) == writes
    restored = VirtualMarginEngine(MarginAccountOwner(source, store), journal)
    assert restored.snapshot().state == "unavailable" and journal.entries is None
    journal.read_failure = True
    with pytest.raises(StorageError):
        restored.reconcile()
    assert "sensitive" not in str(restored.snapshot())


def test_chain_gap_and_conflicting_exact_id_block_before_any_account_write():
    source, store, journal, _, engine = setup()
    engine.manual_resume("resume")
    submit(engine, pending(source))
    plan = prepare(engine)
    journal.ack_loss = True
    with pytest.raises(StorageError):
        engine.commit_batch(plan)
    writes = len(store.calls)
    original = tuple(journal.entries)
    journal.entries.pop(1)
    with pytest.raises(ProtocolError):
        engine.reconcile()
    assert len(store.calls) == writes
    journal.entries = list(original)
    duplicate = journal.entries[-1]
    journal.entries.append(
        replace(duplicate, record=replace(duplicate.record, previous_checksum="b" * 64))
    )
    with pytest.raises(ProtocolError):
        engine.reconcile()
    assert len(store.calls) == writes


def test_cancel_ack_loss_retries_exact_envelope_without_losing_original_request_identity():
    source, _, journal, _, engine = setup()
    engine.manual_resume("resume")
    submit(engine, pending(source))
    journal.before_failure = True
    with pytest.raises(StorageError):
        engine.cancel_order("order", "cancel")
    fixed = journal.calls[-1]
    assert engine.reconcile().pending == () and journal.calls[-1] is fixed
    count = len(journal.calls)
    engine.cancel_order("order", "cancel")
    assert len(journal.calls) == count


def test_new_source_cannot_overwrite_unconfirmed_record_at_the_same_sequence():
    source, store, journal, _, engine = setup()
    engine.manual_resume("resume")
    journal.before_failure = True
    with pytest.raises(StorageError):
        submit(engine, pending(source))
    fixed = journal.calls[-1]
    competing = VirtualMarginRecord(
        fixed.account_id,
        fixed.sequence,
        fixed.previous_checksum,
        "f" * 64,
        pending(source, "competing"),
    )
    journal.entries.append(JournalReceipt(competing, Durability.PRIMARY))
    writes, appends = len(store.calls), len(journal.calls)
    with pytest.raises(ProtocolError):
        engine.reconcile()
    assert len(store.calls) == writes and len(journal.calls) == appends


def test_old_resume_request_is_idempotent_without_clearing_later_pause_or_restart_gate():
    source, store, journal, _, engine = setup()
    engine.manual_resume("resume")
    engine.pause_entries("later-pause")
    count = len(journal.calls)
    engine.manual_resume("resume")
    assert engine.snapshot().paused and len(journal.calls) == count
    restored = VirtualMarginEngine(MarginAccountOwner(source, store), journal)
    restored.manual_resume("resume")
    assert restored.snapshot().paused and len(journal.calls) == count
    restored.manual_resume("fresh-resume")
    assert not restored.snapshot().paused


@pytest.mark.parametrize(
    "change", [{"phase": "close"}, {"phase": "invalid"}, {"batch_id": "f" * 64}]
)
def test_phase_or_batch_identity_tamper_is_rejected_on_restart_before_account_commit(change):
    source, store, journal, _, engine = setup()
    engine.manual_resume("resume")
    submit(engine, pending(source))
    plan = prepare(engine)
    journal.ack_loss = True
    with pytest.raises(StorageError):
        engine.commit_batch(plan)
    receipt = journal.entries[-1]
    journal.entries[-1] = replace(
        receipt, record=replace(receipt.record, payload=replace(plan, **change))
    )
    writes = len(store.calls)
    restored = VirtualMarginEngine(MarginAccountOwner(source, store), journal)
    assert restored.snapshot().state == "conflict" and len(store.calls) == writes


def cost_pending(account, *, reducing=False, identity="cost", instrument=BTC, reservations=()):
    policy = MarginCostPolicy(
        slippage_rate=D(".01"),
        execution_price_bound_rate=D(".1"),
        mark_drift_bound_rate=D(".1"),
        entry_delay_ms=0,
        reduction_delay_ms=0,
    )
    reservation = approve_cost_order(
        account,
        identity,
        OrderProposal(Side.SELL if reducing else Side.BUY, D(1), reducing=reducing),
        policy=policy,
        instrument=instrument,
        owner_id=instrument.symbol,
        as_of_ms=0,
        marks=marks(),
        brackets=brackets(),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
        reservations=reservations,
    )
    return freeze_cost_pending(reservation)


@pytest.mark.parametrize("boundary", ["source_before", "source_ack", "account_ack"])
def test_cost_frozen_source_and_account_ack_recovery_keep_original_quote_fee_and_id(boundary):
    source, store, journal, _, engine = setup()
    engine.manual_resume("start")
    original = cost_pending(source)
    submit(engine, original)
    plan = prepare(engine)
    journal.before_failure = boundary == "source_before"
    journal.ack_loss = boundary == "source_ack"
    store.lose_at = 1 if boundary == "account_ack" else None
    with pytest.raises((StorageError, MarginCommitUnknown)):
        engine.commit_batch(plan)
    assert engine.snapshot().state == "unknown"
    recovered = engine.reconcile()
    assert recovered.account == plan.candidate_account
    assert recovered.account.ledger[-1].fact.price == 101
    assert recovered.account.ledger[-1].fact.fee == D(".101")
    assert engine.original_intent(original.order_id) == original
    assert engine.committed_batches(60000, "open") == (plan,)
    writes = len(store.calls), len(journal.calls)
    engine.reconcile()
    assert (len(store.calls), len(journal.calls)) == writes
    restarted = VirtualMarginEngine(MarginAccountOwner(source, store), journal)
    assert restarted.snapshot().state == "ready" and restarted.snapshot().paused
    assert restarted.snapshot().account == plan.candidate_account


def test_confirmed_fallback_cost_reduction_keeps_slippage_and_blocks_new_risk():
    source, store, journal, _, engine = setup(quantity="1")
    original = cost_pending(source, reducing=True)
    submit(engine, original)
    journal.durability = Durability.FALLBACK
    plan = prepare(engine)
    engine.commit_batch(plan)
    assert store.value.positions[0].quantity == 0
    assert store.value.ledger[-1].fact.price == 99
    assert store.value.ledger[-1].fact.fee == D(".099")
    assert engine.snapshot().paused
    with pytest.raises(TradingBlocked):
        engine.manual_resume("do-not-enable-entry")
    assert engine.original_intent(original.order_id) == original


def test_cost_phase_cannot_be_committed_again_under_legacy_model_after_financial_settlement():
    source, store, journal, _, engine = setup()
    engine.manual_resume("start")
    submit(engine, cost_pending(source))
    plan = prepare(engine)
    engine.commit_batch(plan)
    # Empty pending would ordinarily select the legacy batch class/model.
    duplicate = prepare(engine)
    writes = len(store.calls), len(journal.calls)
    with pytest.raises(ProtocolError, match="already committed"):
        engine.commit_batch(duplicate)
    assert (len(store.calls), len(journal.calls)) == writes
    assert store.value == plan.candidate_account


def test_cost_source_quote_tamper_is_rejected_before_any_factual_owner_cas():
    source, store, journal, _, engine = setup()
    engine.manual_resume("start")
    submit(engine, cost_pending(source))
    plan = prepare(engine)
    admission = plan.admissions[0]
    changed = replace(admission, quote=replace(admission.quote, raw_price=D(99)))
    forged = replace(plan, admissions=(changed,))
    journal.append(engine._record(forged, ("batch", forged.batch_id)))
    writes = len(store.calls)
    restored = VirtualMarginEngine(MarginAccountOwner(source, store), journal)
    assert restored.snapshot().state == "conflict"
    assert len(store.calls) == writes and store.value == source


def test_cost_quote_summaries_are_confirmed_bounded_copies_and_do_not_mutate_finance():
    source, store, journal, _, engine = setup()
    engine.manual_resume("start")
    first = cost_pending(source)
    submit(engine, first)
    submit(
        engine,
        cost_pending(source, identity="second", instrument=ETH, reservations=(first.original,)),
    )
    plan = prepare(engine)
    engine.commit_batch(plan)
    calls = len(store.calls), len(journal.calls)
    result = engine.cost_quote_summaries(limit=1)
    assert result["state"] == "ready" and result["total"] == 2 and result["truncated"]
    row = result["rows"][0]
    assert row["instrument"] == ETH.key
    assert D(row["raw_price"]) == 100 and D(row["final_price"]) == 101
    assert D(row["fee"]) == D(".101")
    assert D(row["explanatory_slippage_amount"]) == 1
    assert row["issued_at_ms"] == row["eligible_at_ms"] == 0
    assert row["event_time_ms"] == 60000 and row["available_at_ms"] == 120000
    row["final_price"] = "0"
    assert D(engine.cost_quote_summaries()["rows"][-1]["final_price"]) == 101
    assert (len(store.calls), len(journal.calls)) == calls
    assert store.value == plan.candidate_account


def test_cost_quote_summaries_do_not_present_an_unconfirmed_source_as_applied():
    source, store, journal, _, engine = setup()
    engine.manual_resume("start")
    submit(engine, cost_pending(source))
    plan = prepare(engine)
    journal.ack_loss = True
    with pytest.raises(StorageError):
        engine.commit_batch(plan)
    assert engine.cost_quote_summaries()["state"] == "unavailable"
    assert engine.cost_quote_summaries()["rows"] == [] and store.value == source
    engine.reconcile()
    assert engine.cost_quote_summaries()["total"] == 1


@pytest.mark.parametrize("limit", [True, 0, 10001, 1.0, None])
def test_cost_quote_summary_rejects_invalid_native_limit(limit):
    *_, engine = setup()
    with pytest.raises(ValidationError, match="limit"):
        engine.cost_quote_summaries(limit=limit)
