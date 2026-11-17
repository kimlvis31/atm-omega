"""Exact cost tags/frozen inputs and actual file ACK recovery; no venue or clock."""

import json
from decimal import Decimal as D
from decimal import localcontext
from hashlib import sha256

import pytest
from test_margin_cost_execution import BTC, ETH, account, bar, marks, order, policy, tables

from omega.application.margin_account import MarginAccountOwner, MarginCommitUnknown
from omega.application.margin_cost import (
    CostPendingFillAdmission,
    FrozenCostPendingOrder,
    PreparedCostMarginBatch,
    approve_cost_order,
    freeze_cost_pending,
)
from omega.application.margin_execution import FrozenPendingOrder, prepare_pending_batch
from omega.application.margin_risk import approve_order
from omega.application.virtual_margin import VirtualMarginEngine, VirtualMarginRecord
from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.margin import MarginMode
from omega.domain.models import Side
from omega.domain.trading import OrderProposal
from omega.ports.journal import Durability
from omega.runtime import virtual_margin_journal as module
from omega.runtime.margin_store import FileMarginStateStore


def record(payload):
    return VirtualMarginRecord("cost", 2, "a" * 64, "b" * 64, payload)


def prepared(*, mixed=False, mode=MarginMode.CROSS, side=Side.BUY):
    instruments = (BTC, ETH) if mixed else (BTC,)
    state = account(mode=mode, instruments=instruments)
    pending = []
    if mixed:
        original = approve_order(
            state,
            "legacy",
            OrderProposal(Side.BUY, D(1)),
            instrument=BTC,
            owner_id=BTC.symbol,
            as_of_ms=60000,
            marks=marks(instruments),
            brackets=tables(instruments),
            worst_execution_price=D(100),
            fee_rate=D(".001"),
        )
        pending.append(FrozenPendingOrder(original, 60000))
    pending.append(
        order(
            state,
            side=side,
            identity="costed",
            instrument=ETH if mixed else BTC,
            reservations=tuple(row.original for row in pending),
        )
    )
    result = prepare_pending_batch(
        state,
        tuple(pending),
        tuple(bar(instrument=item) for item in instruments),
        as_of_ms=120000,
        brackets=tables(instruments),
    )
    return state, tuple(pending), result


def refresh_hash(document):
    document["sha256"] = sha256(module._json(document["data"])).hexdigest()
    return json.dumps(document).encode()


@pytest.mark.parametrize("mode", [MarginMode.CROSS, MarginMode.ISOLATED])
@pytest.mark.parametrize("side", [Side.BUY, Side.SELL])
def test_cost_pending_batch_actual_roundtrip_compressed_prefix_and_decimal_context(mode, side):
    state, pending, plan = prepared(mode=mode, side=side)
    encoded = module.encode_record(record(plan))
    assert b'"type": "PreparedCostMarginBatch"' in encoded
    assert b'"prefix_accounts"' not in encoded
    assert encoded.count(b'"opening_balance"') == 1
    with localcontext() as context:
        context.prec = 2
        assert module.decode_record(encoded) == record(plan)
        assert module.decode_record(module.encode_record(record(pending[0]))) == record(pending[0])
    assert module.encode_record(module.decode_record(encoded)) == encoded
    assert state.ledger == ()


def test_mixed_cost_batch_retains_exact_legacy_tag_and_fill_identity():
    state, pending, plan = prepared(mixed=True)
    assert type(plan) is PreparedCostMarginBatch
    assert type(plan.admissions[0]) is module.PendingFillAdmission
    assert type(plan.admissions[1]) is CostPendingFillAdmission
    restored = module.decode_record(module.encode_record(record(plan))).payload
    assert restored == plan
    assert type(restored.original_pending[0]) is FrozenPendingOrder
    assert type(restored.original_pending[1]) is FrozenCostPendingOrder
    legacy_only = prepare_pending_batch(
        state, pending[:1], plan.candles, as_of_ms=120000, brackets=tables((BTC, ETH))
    )
    assert restored.facts[0].fill_id == legacy_only.facts[0].fill_id


@pytest.mark.parametrize(
    "fault",
    [
        "legacy_pending",
        "legacy_reservation",
        "unknown",
        "delay_bool",
        "eligible",
        "policy",
        "fingerprint",
        "anchor",
        "extra_fee",
    ],
)
def test_exact_pending_tags_reject_hidden_costs_types_policy_and_original_identity(fault):
    _, pending, _ = prepared()
    document = json.loads(module.encode_record(record(pending[0])))
    payload = document["data"]["fields"]["payload"]
    original = payload["fields"]["original"]
    envelope = original["fields"]["envelope"]["fields"]
    if fault == "legacy_pending":
        payload["type"] = "FrozenPendingOrder"
    elif fault == "legacy_reservation":
        original["type"] = "MarginReservation"
    elif fault == "unknown":
        payload["type"] = "UserCostPendingSubclass"
    elif fault == "delay_bool":
        envelope["policy"]["fields"]["entry_delay_ms"] = True
    elif fault == "eligible":
        payload["fields"]["eligible_at_ms"] += 60000
    elif fault == "policy":
        envelope["policy"]["fields"]["slippage_rate"]["value"] = "0.02"
    elif fault == "fingerprint":
        original["fields"]["base_request_fingerprint"] = "f" * 64
    elif fault == "anchor":
        envelope["anchor_account_fingerprint"] = "f" * 64
    else:
        original["fields"]["extra_fee"]["value"] = "10"
    with pytest.raises(ValidationError):
        module.decode_record(refresh_hash(document))


@pytest.mark.parametrize(
    "fault",
    [
        "legacy_batch",
        "model",
        "raw_price",
        "final_price",
        "cap",
        "quote_model",
        "legacy_admission",
        "legacy_quote",
        "native_bool",
        "prefix",
    ],
)
def test_frozen_batch_checks_actual_quote_policy_fee_and_type_not_display_data(fault):
    _, _, plan = prepared()
    document = json.loads(module.encode_record(record(plan)))
    payload = document["data"]["fields"]["payload"]
    fields = payload["fields"]
    admission = fields["admissions"]["items"][0]
    quote = admission["fields"]["quote"]
    if fault == "legacy_batch":
        payload["type"] = "PreparedMarginBatch"
    elif fault == "model":
        fields["model"] = module.MODEL
    elif fault in {"raw_price", "final_price"}:
        quote["fields"]["raw_price" if fault == "raw_price" else "price"]["value"] = "90"
    elif fault == "cap":
        quote["fields"]["limit_capped"] = True
    elif fault == "quote_model":
        quote["fields"]["model"] = "next_bar_full_fill_v1"
    elif fault == "legacy_admission":
        admission["type"] = "PendingFillAdmission"
    elif fault == "legacy_quote":
        quote["type"] = "BarFillQuote"
    elif fault == "native_bool":
        quote["fields"]["limit_capped"] = 1
    else:
        fields["prefix_checksums"]["items"][1] = "f" * 64
    with pytest.raises(ValidationError):
        module.decode_record(refresh_hash(document))


def setup(root, initial):
    store = FileMarginStateStore(root, initial.account_id)
    journal = module.FileVirtualMarginJournal(root, initial.account_id)
    engine = VirtualMarginEngine(
        MarginAccountOwner(initial, store, create_if_missing=True), journal, create_if_missing=True
    )
    assert engine.snapshot().state == "ready"
    return store, journal, engine


@pytest.mark.parametrize("failure", ["source_ack", "first_account_ack", "second_account_ack"])
def test_actual_mixed_source_account_prefix_ack_loss_restart_never_reissues_policy(
    tmp_path, monkeypatch, failure
):
    initial, pending, expected = prepared(mixed=True)
    store, journal, engine = setup(tmp_path, initial)
    engine.manual_resume("resume")
    for item in pending:
        engine.submit_intent(item, marks=marks((BTC, ETH)), brackets=tables((BTC, ETH)))
    plan = engine.prepare_batch(expected.candles, as_of_ms=120000, brackets=tables((BTC, ETH)))
    original_publish, original_save = journal._publish, store.save

    def lost_source(directory, value, payload):
        original_publish(directory, value, payload)
        if type(value.payload) is PreparedCostMarginBatch:
            raise OSError("synthetic ACK lost after actual immutable publication")

    def lost_account(value, *, expected_sequence):
        original_save(value, expected_sequence=expected_sequence)
        target = 1 if failure == "first_account_ack" else 2
        if len(value.ledger) == target:
            raise StorageError("synthetic account ACK lost after actual CAS")

    try:
        if failure == "source_ack":
            monkeypatch.setattr(journal, "_publish", lost_source)
        else:
            monkeypatch.setattr(store, "save", lost_account)
        with pytest.raises(StorageError if failure == "source_ack" else MarginCommitUnknown):
            engine.commit_batch(plan)
        assert engine.snapshot().state == "unknown"
        known = store.load(initial.account_id)
        expected_prefix = (
            0 if failure == "source_ack" else 1 if failure == "first_account_ack" else 2
        )
        assert known == plan.prefix_accounts[expected_prefix]
        assert engine.committed_batches(60000, "open") == (plan,)
    finally:
        store.close()
        journal.close()
    restored_store = FileMarginStateStore(tmp_path, initial.account_id)
    restored_journal = module.FileVirtualMarginJournal(tmp_path, initial.account_id)
    try:
        restored = VirtualMarginEngine(
            MarginAccountOwner(initial, restored_store), restored_journal
        )
        result = restored.snapshot()
        assert result.state == "ready" and result.paused and result.pending == ()
        assert result.account == plan.candidate_account
        assert tuple(entry.fact for entry in result.account.ledger) == expected.facts
        assert result.account.cross_wallet == D("999.799")
        assert restored.reconcile().account == result.account
        assert len(restored_store.load(initial.account_id).ledger) == 2
        copies = [
            receipt
            for receipt in restored_journal.read(initial.account_id)
            if type(receipt.record.payload) is PreparedCostMarginBatch
        ]
        assert len(copies) == (2 if failure == "source_ack" else 1)
    finally:
        restored_store.close()
        restored_journal.close()


def test_actual_cost_reducing_fallback_preserves_delay_fills_and_negative_debt(
    tmp_path, monkeypatch
):
    initial = account(cash="1", quantity="1")
    store, journal, engine = setup(tmp_path, initial)
    pending = freeze_cost_pending(
        approve_cost_order(
            initial,
            "reduce",
            OrderProposal(Side.SELL, D(1), reducing=True),
            policy=policy(reduction_delay_ms=1),
            instrument=BTC,
            owner_id=BTC.symbol,
            as_of_ms=60000,
            marks=(),
            brackets=(),
            worst_execution_price=D(100),
            fee_rate=D(".001"),
        )
    )
    original = journal._publish

    def unavailable(directory, value, payload):
        if directory == journal.primary and type(value.payload) in {
            FrozenCostPendingOrder,
            PreparedCostMarginBatch,
        }:
            raise PermissionError("synthetic primary unavailable for cost reduction")
        return original(directory, value, payload)

    try:
        monkeypatch.setattr(journal, "_publish", unavailable)
        engine.submit_intent(pending, marks=(), brackets=())
        assert engine.snapshot().pending == (pending,) and pending.eligible_at_ms == 120000
        early = engine.prepare_batch((bar(),), as_of_ms=120000, brackets=tables())
        assert not early.facts and early.remaining == (pending,)
        delayed = engine.prepare_batch((bar(opening=120000),), as_of_ms=180000, brackets=tables())
        assert delayed.facts[0].event_time_ms == 120000
        result = engine.commit_batch(delayed)
        assert result.account.positions[0].quantity == 0 and result.account.cross_wallet == D(
            "-.099"
        )
        assert result.pending == () and result.paused
        with pytest.raises(TradingBlocked, match="primary"):
            engine.manual_resume("unsafe")
        values = journal.read(initial.account_id)
        assert any(
            type(receipt.record.payload) is PreparedCostMarginBatch
            and receipt.durability is Durability.FALLBACK
            for receipt in values
        )
    finally:
        store.close()
        journal.close()
    restored_store = FileMarginStateStore(tmp_path, initial.account_id)
    restored_journal = module.FileVirtualMarginJournal(tmp_path, initial.account_id)
    try:
        restored = VirtualMarginEngine(
            MarginAccountOwner(initial, restored_store), restored_journal
        )
        assert restored.snapshot().account == result.account
        assert restored.snapshot().state == "ready" and restored.snapshot().paused
    finally:
        restored_store.close()
        restored_journal.close()


def test_actual_cost_entry_fallback_is_unconfirmed_until_primary_reconciles(tmp_path, monkeypatch):
    initial = account()
    store, journal, engine = setup(tmp_path, initial)
    pending = order(initial)
    engine.manual_resume("resume")
    original = journal._publish

    def unavailable(directory, value, payload):
        if directory == journal.primary and type(value.payload) is FrozenCostPendingOrder:
            raise PermissionError("synthetic primary unavailable for entry")
        return original(directory, value, payload)

    try:
        monkeypatch.setattr(journal, "_publish", unavailable)
        with pytest.raises(StorageError):
            engine.submit_intent(pending, marks=marks(), brackets=tables())
        assert engine.snapshot().state == "unknown" and store.load(initial.account_id) == initial
        with pytest.raises(TradingBlocked):
            engine.prepare_batch((bar(),), as_of_ms=120000, brackets=tables())
        monkeypatch.setattr(journal, "_publish", original)
        assert engine.reconcile().pending == (pending,)
        assert store.load(initial.account_id) == initial
    finally:
        store.close()
        journal.close()
