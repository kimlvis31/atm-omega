"""Real local owner/CAS/codec; separate v2 namespace, no broker or market DB."""

import json
import os
from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.application.margin_account import MarginAccountOwner, MarginCommitUnknown
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.margin import (
    CollateralTransfer,
    FillEvent,
    MarginAccount,
    MarginMode,
    MarginPosition,
    apply_fill,
    apply_transfer,
)
from omega.domain.models import Instrument, Market
from omega.runtime import margin_store as module

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")


def initial():
    return MarginAccount(
        "account:1",
        "USDT",
        D("1000.00"),
        (
            MarginPosition(BTC, "s1", MarginMode.CROSS, 10, D(500)),
            MarginPosition(ETH, "s2", MarginMode.ISOLATED, 5, D(100)),
        ),
    )


def states():
    first = initial()
    second = apply_transfer(first, CollateralTransfer("t", ETH, "s2", D("100.0"), 1, 2)).account
    third = apply_fill(
        second, FillEvent("f1", "o1", BTC, "s1", D("1.00"), D("100.0"), D(".1"), "USDT", 3, 4)
    ).account
    fourth = apply_fill(
        third, FillEvent("f2", "o2", ETH, "s2", D("2"), D("50.00"), D(".10"), "USDT", 5, 6)
    ).account
    return first, second, third, fourth


def test_display_summary_cannot_be_loaded_as_authoritative_financial_state():
    state = states()[-1]
    original = module.account_document(state)
    summary = module.account_document(state, ledger_limit=1)
    assert summary["summary"] == {"ledger_total": 3, "ledger_limit": 1, "ledger_truncated": True}
    assert len(summary["account"]["ledger"]) == 1
    assert module.account_document(state) == original
    with pytest.raises(ValidationError):
        module.account_from_document(summary)


def test_real_snapshot_roundtrip_and_exclusive_owner_release(tmp_path):
    store = module.FileMarginStateStore(tmp_path, "account:1")
    try:
        assert store.load("account:1") is None
        for sequence, state in enumerate(states()):
            store.save(state, expected_sequence=None if sequence == 0 else sequence - 1)
            assert store.load("account:1") == state
        assert store.path.parent == tmp_path / "state/margin-v2"
        assert ":" not in store.path.name
        text = store.path.read_text()
        assert '"1.00"' in text and '"1000.00"' in text
        with pytest.raises(RuntimeError, match="already owns"):
            module.FileMarginStateStore(tmp_path, "account:1")
    finally:
        store.close()
    reopened = module.FileMarginStateStore(tmp_path, "account:1")
    try:
        assert reopened.load("account:1") == states()[-1]
    finally:
        reopened.close()


def test_codec_rejects_untyped_money_duplicate_fact_schema_and_bad_partition():
    document = module.account_document(states()[-1])
    assert module.account_from_document(document) == states()[-1]
    document["account"]["cross_wallet"] = 899.9
    with pytest.raises(ValidationError):
        module.account_from_document(document)
    document = module.account_document(states()[-1])
    document["account"]["ledger"][1]["fact"]["unknown"] = "ignored?"
    with pytest.raises(ValidationError):
        module.account_from_document(document)
    document = module.account_document(states()[-1])
    document["account"]["positions"][1]["margin_mode"] = "cross"
    with pytest.raises(ValidationError):
        module.account_from_document(document)


def test_wrong_cas_and_history_replacement_never_overwrite_committed_state(tmp_path):
    first, second, third, _ = states()
    store = module.FileMarginStateStore(tmp_path, first.account_id)
    try:
        store.save(first, expected_sequence=None)
        before = store.path.read_bytes()
        for candidate, sequence in ((second, None), (second, 10), (third, 0), (first, 0)):
            with pytest.raises(ProtocolError):
                store.save(candidate, expected_sequence=sequence)
            assert store.path.read_bytes() == before
        with pytest.raises(ValidationError):
            store.load("someone_else")
        with pytest.raises(ValidationError):
            store.save(second, expected_sequence=True)
    finally:
        store.close()
    with pytest.raises(StorageError):
        store.load(first.account_id)


def test_corrupt_snapshot_is_preserved_without_silent_empty_account(tmp_path):
    store = module.FileMarginStateStore(tmp_path, "account:1")
    try:
        store.save(initial(), expected_sequence=None)
        envelope = json.loads(store.path.read_bytes())
        envelope["data"]["account"]["cross_wallet"] = "1000000"
        store.path.write_text(json.dumps(envelope))
        before = store.path.read_bytes()
        with pytest.raises(StorageError, match="original preserved"):
            store.load("account:1")
        with pytest.raises(StorageError):
            store.save(initial(), expected_sequence=None)
        assert store.path.read_bytes() == before
    finally:
        store.close()


def test_replacement_ack_loss_loads_exact_candidate_for_explicit_reconcile(tmp_path, monkeypatch):
    first, second, *_ = states()
    store = module.FileMarginStateStore(tmp_path, "account:1")
    actual_write = module.atomic_json
    try:
        store.save(first, expected_sequence=None)

        def lose_ack(path, value):
            actual_write(path, value)
            raise OSError("synthetic ACK lost after actual atomic replacement")

        monkeypatch.setattr(module, "atomic_json", lose_ack)
        with pytest.raises(StorageError, match="unknown"):
            store.save(second, expected_sequence=0)
        assert store.load("account:1") == second
    finally:
        store.close()


def test_missing_parent_or_json_syntax_errors_do_not_change_existing_v1(tmp_path):
    legacy = tmp_path / "state/paper.json"
    legacy.parent.mkdir()
    legacy.write_text('{"paper_v1":"untouched"}')
    original = legacy.read_bytes()
    store = module.FileMarginStateStore(tmp_path, "account:1")
    try:
        store.path.write_text('{"data":1,"data":2}')
        with pytest.raises(StorageError):
            store.load("account:1")
        assert legacy.read_bytes() == original
    finally:
        store.close()


def test_real_file_owner_reconciles_ack_loss_then_restarts_with_exact_state(tmp_path, monkeypatch):
    account = initial()
    store = module.FileMarginStateStore(tmp_path, account.account_id)
    owner = MarginAccountOwner(account, store, create_if_missing=True)
    fact = CollateralTransfer("actual-file-t1", ETH, "s2", D("100"), 1, 2)
    actual_write = module.atomic_json
    try:
        assert owner.snapshot() == account

        def lose_ack(path, value):
            actual_write(path, value)
            raise OSError("synthetic ACK loss after actual replacement")

        monkeypatch.setattr(module, "atomic_json", lose_ack)
        with pytest.raises(MarginCommitUnknown):
            owner.apply_transfer(fact)
        assert owner.status()["state"] == "unknown"
        assert owner.snapshot() == account
        confirmed = owner.reconcile()
        assert confirmed == apply_transfer(account, fact).account
        assert owner.apply_transfer(fact).entry is None
        assert store.load(account.account_id) == confirmed
    finally:
        store.close()
    monkeypatch.setattr(module, "atomic_json", actual_write)
    reopened = module.FileMarginStateStore(tmp_path, account.account_id)
    try:
        restored = MarginAccountOwner(account, reopened)
        assert restored.status()["state"] == "ready"
        assert restored.snapshot() == confirmed
    finally:
        reopened.close()


def test_wrong_fact_quantity_or_wallet_partition_cannot_be_committed(tmp_path):
    first, second, third, _ = states()
    store = module.FileMarginStateStore(tmp_path, first.account_id)
    try:
        store.save(first, expected_sequence=None)
        before = store.path.read_bytes()
        wrong_transfer = replace(
            second,
            cross_wallet=D(950),
            positions=(second.positions[0], replace(second.positions[1], isolated_wallet=D(50))),
        )
        with pytest.raises(ProtocolError, match="appended fact"):
            store.save(wrong_transfer, expected_sequence=0)
        assert store.path.read_bytes() == before
        store.save(second, expected_sequence=0)
        before = store.path.read_bytes()
        wrong_fill = replace(
            third, positions=(replace(third.positions[0], quantity=D(99)), third.positions[1])
        )
        with pytest.raises(ProtocolError, match="appended fact"):
            store.save(wrong_fill, expected_sequence=1)
        assert store.path.read_bytes() == before
    finally:
        store.close()


@pytest.mark.parametrize("linked_directory", [False, True])
def test_linked_snapshot_paths_are_rejected_before_opening_target(tmp_path, linked_directory):
    root, other = tmp_path / "root", tmp_path / "other"
    root.mkdir()
    other.mkdir()
    if linked_directory:
        try:
            (root / "state").symlink_to(other, target_is_directory=True)
        except OSError:
            pytest.skip("creating a local directory symlink is not available on this platform")
        with pytest.raises(StorageError, match="linked"):
            module.FileMarginStateStore(root, "account:1")
        assert list(other.iterdir()) == []
    else:
        target = other / "untouched.json"
        target.write_text("unchanged")
        store = module.FileMarginStateStore(root, "account:1")
        try:
            try:
                store.path.symlink_to(target)
            except OSError:
                pytest.skip("creating a local file symlink is not available on this platform")
            with pytest.raises(StorageError, match="linked"):
                store.save(initial(), expected_sequence=None)
            assert target.read_text() == "unchanged"
        finally:
            store.close()


def test_regular_file_parent_is_not_reported_as_an_absent_wallet(tmp_path):
    (tmp_path / "state").write_text("preserve parent")
    with pytest.raises(StorageError, match="not a directory"):
        module.FileMarginStateStore(tmp_path, "account:1")
    assert (tmp_path / "state").read_text() == "preserve parent"


def test_directory_snapshot_is_an_error_without_initialization(tmp_path):
    store = module.FileMarginStateStore(tmp_path, "account:1")
    try:
        store.path.mkdir()
        with pytest.raises(StorageError):
            store.load("account:1")
        assert store.path.is_dir()
    finally:
        store.close()


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="POSIX FIFO is unavailable on this platform")
def test_fifo_snapshot_is_rejected_without_waiting_for_a_writer(tmp_path):
    store = module.FileMarginStateStore(tmp_path, "account:1")
    try:
        os.mkfifo(store.path)
        with pytest.raises(StorageError, match="regular"):
            store.load("account:1")
        assert store.path.exists()
    finally:
        store.close()
