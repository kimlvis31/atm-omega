"""Actual frozen funding journal/CAS recovery over explicit scenario inputs."""

import json
from dataclasses import replace
from decimal import Decimal as D
from decimal import localcontext
from hashlib import sha256

import pytest

from omega.application.margin_account import MarginAccountOwner, MarginCommitUnknown
from omega.application.margin_execution import FrozenPendingOrder
from omega.application.margin_funding import (
    FundingPoint,
    PreparedFundingBatch,
    prepare_funding_batch,
)
from omega.application.margin_risk import approve_order
from omega.application.virtual_margin import VirtualMarginEngine, VirtualMarginRecord
from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.margin import (
    FundingCashflow,
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
from omega.runtime import virtual_margin_journal as module
from omega.runtime.margin_store import FileMarginStateStore

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")


def initial():
    return MarginAccount(
        "funding",
        "USDT",
        D("1000.00"),
        (
            MarginPosition(BTC, "long", MarginMode.CROSS, 10, D(100), D(1), D(100)),
            MarginPosition(ETH, "short", MarginMode.ISOLATED, 5, D(100), D(-2), D(100), D(100)),
        ),
    )


def points(rate=".01", reference="100", opened=60000):
    return tuple(
        FundingPoint(instrument, opened, opened + 10, opened, D(rate), D(reference))
        for instrument in (BTC, ETH)
    )


def setup(root):
    store = FileMarginStateStore(root, "funding")
    journal = module.FileVirtualMarginJournal(root, "funding")
    engine = VirtualMarginEngine(
        MarginAccountOwner(initial(), store, create_if_missing=True),
        journal,
        create_if_missing=True,
    )
    assert engine.snapshot().state == "ready"
    return store, journal, engine


def funding_record(plan):
    return VirtualMarginRecord("funding", 2, "a" * 64, "b" * 64, plan)


def refresh_hash(document):
    document["sha256"] = sha256(module._json(document["data"])).hexdigest()
    return json.dumps(document).encode()


def test_frozen_funding_codec_reconstructs_mixed_prefixes_without_source_prepare(monkeypatch):
    first = prepare_funding_batch(initial(), points(), as_of_ms=120000)
    second = prepare_funding_batch(
        first.candidate_account, points("-.01", opened=120000), as_of_ms=180000
    )
    original = funding_record(second)
    encoded = module.encode_record(original)
    assert '"prefix_accounts"' not in encoded.decode()
    assert encoded.decode().count('"opening_balance"') == 1
    monkeypatch.setattr(
        "omega.application.margin_funding.prepare_funding_batch",
        lambda *args, **kwargs: pytest.fail("no fresh funding evaluation during decode"),
    )
    with localcontext() as context:
        context.prec = 3
        decoded = module.decode_record(encoded)
    assert decoded == original
    assert module.encode_record(decoded) == encoded
    assert decoded.payload.prefix_accounts[0] == first.candidate_account
    assert len(decoded.payload.prefix_accounts[-1].ledger) == 4


@pytest.mark.parametrize(
    "mutation",
    ["baseline", "amount", "quantity", "source", "asof", "prefix", "extra", "bool_time", "kind"],
)
def test_funding_journal_strict_typed_source_baseline_prefix_and_time(mutation):
    plan = prepare_funding_batch(initial(), points(), as_of_ms=120000)
    document = json.loads(module.encode_record(funding_record(plan)))
    payload = document["data"]["fields"]["payload"]
    frozen = payload["document"]
    if mutation == "baseline":
        payload["baseline"]["value"]["account"]["cross_wallet"] = "100000"
    elif mutation == "amount":
        frozen["facts"][0]["amount"] = "1000"
    elif mutation == "quantity":
        frozen["facts"][0]["signed_quantity"] = "1000"
    elif mutation == "source":
        frozen["facts"][0]["source_kind"] = "actual_venue_payment"
    elif mutation == "asof":
        frozen["as_of_ms"] += 1
    elif mutation == "prefix":
        frozen["prefix_checksums"][1] = "f" * 64
    elif mutation == "extra":
        frozen["unexpected"] = True
    elif mutation == "bool_time":
        frozen["facts"][0]["available_at_ms"] = True
    else:
        payload["type"] = "PreparedVenuePaymentBatch"
    with pytest.raises(ValidationError):
        module.decode_record(refresh_hash(document))
    with pytest.raises(ValidationError, match="another account"):
        module.encode_record(replace(funding_record(plan), account_id="other"))


@pytest.mark.parametrize("prefix", [1, 2])
def test_actual_partial_account_ack_loss_restarts_from_exact_authorized_funding_prefix(
    tmp_path, monkeypatch, prefix
):
    store, journal, engine = setup(tmp_path)
    original_save = store.save
    prepared = engine.prepare_funding(points(), as_of_ms=120000)

    def lost(account, *, expected_sequence):
        original_save(account, expected_sequence=expected_sequence)
        if len(account.ledger) == prefix:
            raise StorageError("synthetic ACK lost after actual funding account commit")

    try:
        monkeypatch.setattr(store, "save", lost)
        with pytest.raises(MarginCommitUnknown):
            engine.commit_funding(prepared)
        assert engine.snapshot().state == "unknown"
        assert store.load("funding") == prepared.prefix_accounts[prefix]
        assert len(engine.committed_funding_batches(60000)) == 1
    finally:
        store.close()
        journal.close()
    monkeypatch.setattr(
        "omega.application.margin_funding.prepare_funding_batch",
        lambda *args, **kwargs: pytest.fail("no new source evaluation during restart"),
    )
    reopened = FileMarginStateStore(tmp_path, "funding")
    reopened_journal = module.FileVirtualMarginJournal(tmp_path, "funding")
    try:
        restored = VirtualMarginEngine(MarginAccountOwner(initial(), reopened), reopened_journal)
        assert restored.snapshot().state == "ready" and restored.snapshot().paused
        assert restored.snapshot().account == prepared.candidate_account
        assert restored.committed_funding_batches(60000) == (prepared,)
        assert restored.reconcile().account == prepared.candidate_account
        for fact in prepared.facts:
            assert isinstance(fact, FundingCashflow)
        assert len(reopened.load("funding").ledger) == 2
    finally:
        reopened.close()
        reopened_journal.close()


def test_actual_source_ack_loss_merges_identical_primary_fallback_and_restarts_once(
    tmp_path, monkeypatch
):
    store, journal, engine = setup(tmp_path)
    original = journal._publish
    prepared = engine.prepare_funding(points(), as_of_ms=120000)

    def lost(directory, record, payload):
        original(directory, record, payload)
        if isinstance(record.payload, PreparedFundingBatch) and directory == journal.primary:
            raise OSError("synthetic source ACK loss after immutable publication")

    try:
        monkeypatch.setattr(journal, "_publish", lost)
        result = engine.commit_funding(prepared)
        assert result.account == prepared.candidate_account
        assert result.paused
        copies = [
            receipt
            for receipt in journal.read("funding")
            if isinstance(receipt.record.payload, PreparedFundingBatch)
        ]
        assert len(copies) == 2 and copies[0].record == copies[1].record
        assert {receipt.durability for receipt in copies} == {
            Durability.PRIMARY,
            Durability.FALLBACK,
        }
        assert engine.reconcile().account == prepared.candidate_account
    finally:
        store.close()
        journal.close()
    reopened = FileMarginStateStore(tmp_path, "funding")
    reopened_journal = module.FileVirtualMarginJournal(tmp_path, "funding")
    try:
        restored = VirtualMarginEngine(MarginAccountOwner(initial(), reopened), reopened_journal)
        assert restored.snapshot().account == prepared.candidate_account
        assert len(restored.snapshot().account.ledger) == 2
    finally:
        reopened.close()
        reopened_journal.close()


def test_both_sources_unavailable_preserve_unknown_and_do_not_skip_funding(tmp_path, monkeypatch):
    store, journal, engine = setup(tmp_path)
    original = journal._publish
    plan = engine.prepare_funding(points(), as_of_ms=120000)
    try:
        monkeypatch.setattr(
            journal,
            "_publish",
            lambda *args: (_ for _ in ()).throw(OSError("both stores unavailable")),
        )
        with pytest.raises(StorageError):
            engine.commit_funding(plan)
        result = engine.snapshot()
        assert result.state == "unknown" and result.pending_record_id is not None
        assert store.load("funding") == initial()
        with pytest.raises(TradingBlocked):
            engine.prepare_funding(points(), as_of_ms=120000)
        with pytest.raises(TradingBlocked):
            engine.prepare_batch(
                (Candle(BTC, 60000, 120000, D(100), D(101), D(99), D(100), D(1)),), as_of_ms=120000
            )
        monkeypatch.setattr(journal, "_publish", original)
        assert engine.reconcile().account == plan.candidate_account
        assert len(store.load("funding").ledger) == 2
    finally:
        store.close()
        journal.close()


@pytest.mark.parametrize("rate", ["1", "-1", "0"])
def test_verified_funding_fallback_keeps_original_reduce_and_blocks_new_risk(
    tmp_path, monkeypatch, rate
):
    store, journal, engine = setup(tmp_path)
    brackets = (
        MarginBracketSnapshot(
            BTC, (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),), 0, 0, "fixture", "a" * 64
        ),
    )
    marks = (MarkSnapshot(BTC, D(100), 0, 0, "fixture"),)
    pending = FrozenPendingOrder(
        approve_order(
            initial(),
            "reduce",
            OrderProposal(Side.SELL, D(1), reducing=True),
            instrument=BTC,
            owner_id="long",
            as_of_ms=0,
            marks=marks,
            brackets=brackets,
            worst_execution_price=D(100),
            fee_rate=D(0),
        ),
        60000,
    )
    try:
        engine.submit_intent(pending, marks=marks, brackets=brackets)
        original = journal._publish

        def unavailable(directory, record, payload):
            if directory == journal.primary and isinstance(record.payload, PreparedFundingBatch):
                raise PermissionError("synthetic funding primary unavailable")
            return original(directory, record, payload)

        monkeypatch.setattr(journal, "_publish", unavailable)
        plan = engine.prepare_funding(points(rate, "10000"), as_of_ms=120000)
        funded = engine.commit_funding(plan)
        assert funded.state == "ready" and funded.paused
        assert funded.pending == (pending,)
        assert engine.original_intent("reduce") == pending
        with pytest.raises(TradingBlocked, match="primary"):
            engine.manual_resume("unsafe")
        prepared_fill = engine.prepare_batch(
            (Candle(BTC, 60000, 120000, D(100), D(101), D(99), D(100), D(1)),),
            as_of_ms=120000,
            brackets=brackets,
        )
        assert len(prepared_fill.facts) == 1
        result = engine.commit_batch(prepared_fill)
        assert result.account.positions[0].quantity == 0
        assert result.pending == () and result.paused
        assert tuple(entry.fact for entry in result.account.ledger[:2]) == plan.facts
        assert module.decode_record(module.encode_record(funding_record(plan))).payload == plan
    finally:
        store.close()
        journal.close()
