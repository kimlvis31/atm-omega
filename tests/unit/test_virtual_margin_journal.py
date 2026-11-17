"""Real local append/codec/recovery over synthetic accounts; no broker or SQL."""

import json
import os
from dataclasses import replace
from decimal import Decimal as D
from decimal import localcontext
from hashlib import sha256

import pytest

from omega.application.margin_account import MarginAccountOwner, MarginCommitUnknown
from omega.application.margin_execution import FrozenPendingOrder, prepare_pending_batch
from omega.application.margin_risk import approve_order
from omega.application.virtual_margin import (
    AccountAnchor,
    CancelIntent,
    EntryControl,
    VirtualMarginEngine,
    VirtualMarginRecord,
)
from omega.domain.errors import ProtocolError, StorageError, ValidationError
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
from omega.runtime import virtual_margin_journal as module
from omega.runtime.margin_store import FileMarginStateStore

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def initial():
    return MarginAccount(
        "a", "USDT", D("1000.00"), (MarginPosition(BTC, "s", MarginMode.CROSS, 10, D(100)),)
    )


def bracket():
    return MarginBracketSnapshot(
        BTC, (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),), 0, 0, "test_bracket", "a" * 64
    )


def order():
    reservation = approve_order(
        initial(),
        "order",
        OrderProposal(Side.BUY, D("1.00")),
        instrument=BTC,
        owner_id="s",
        as_of_ms=0,
        marks=(MarkSnapshot(BTC, D(100), 0, 0, "test_mark"),),
        brackets=(bracket(),),
        worst_execution_price=D(100),
        fee_rate=D(".001"),
    )
    return FrozenPendingOrder(reservation, 60000)


def batch():
    return prepare_pending_batch(
        initial(),
        (order(),),
        (Candle(BTC, 60000, 120000, D("100.0"), D(120), D(80), D(110), D(10)),),
        brackets=(bracket(),),
        phase="open",
        as_of_ms=120000,
    )


def record(payload=None, *, sequence=1, previous=None, identity="1"):
    return VirtualMarginRecord(
        "a",
        sequence,
        previous,
        identity * 64,
        AccountAnchor(initial()) if payload is None else payload,
    )


def refresh_hash(document):
    data = json.dumps(
        document["data"], ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    document["sha256"] = sha256(data).hexdigest()
    return json.dumps(document).encode()


@pytest.mark.parametrize(
    "payload",
    [
        None,
        EntryControl(True, "pause"),
        EntryControl(False, "resume"),
        CancelIntent("order", "a" * 64),
    ],
)
def test_typed_record_roundtrip_decimal_scale_and_native_bool(payload):
    value = record(payload)
    result = module.decode_record(module.encode_record(value))
    assert result == value and result.checksum == value.checksum
    if payload is None:
        assert result.payload.account.cross_wallet.as_tuple() == D("1000.00").as_tuple()


def test_pending_and_frozen_batch_roundtrip_without_price_or_prepare_recalculation(monkeypatch):
    pending, prepared = order(), batch()
    assert prepared.facts and len(prepared.prefix_accounts) == 2
    data = module.encode_record(record(prepared))
    text = data.decode()
    assert '"baseline"' in text and '"prefix_accounts"' not in text
    assert text.count('"opening_balance"') == 1
    monkeypatch.setattr(
        "omega.application.margin_execution.prepare_pending_batch",
        lambda *a, **k: pytest.fail("must replay frozen facts only"),
    )
    with localcontext() as context:
        context.prec = 3
        assert module.decode_record(data) == record(prepared)
        assert module.decode_record(module.encode_record(record(pending))) == record(pending)


def test_encode_rejects_invented_prefix_and_cross_account():
    prepared = batch()
    with pytest.raises(ValidationError, match="prefix"):
        module.encode_record(record(replace(prepared, prefix_accounts=(initial(), initial()))))
    with pytest.raises(ValidationError, match="another account"):
        module.encode_record(replace(record(), account_id="b"))
    with pytest.raises(ValidationError):
        module.encode_record(record(EntryControl(1, "pause")))


@pytest.mark.parametrize(
    "mutation",
    [
        "schema_bool",
        "schema_unknown",
        "unknown_key",
        "wrong_type",
        "enum",
        "decimal",
        "checksum",
        "record_checksum",
        "field_bool",
    ],
)
def test_strict_codec_rejects_schema_type_enum_money_and_checksums(mutation):
    document = json.loads(module.encode_record(record(order())))
    original = document["data"]["fields"]["payload"]["fields"]["original"]["fields"]
    if mutation == "schema_bool":
        document["schema_version"] = True
    elif mutation == "schema_unknown":
        document["schema_version"] = 2
    elif mutation == "unknown_key":
        original["unexpected"] = "ignored?"
    elif mutation == "wrong_type":
        original["proposal"]["fields"]["quantity"] = 1.0
    elif mutation == "enum":
        original["margin_mode"]["value"] = "unknown"
    elif mutation == "decimal":
        original["proposal"]["fields"]["quantity"]["value"] = "NaN"
    elif mutation == "checksum":
        document["sha256"] = "f" * 64
    elif mutation == "record_checksum":
        document["record_checksum"] = "f" * 64
    elif mutation == "field_bool":
        original["leverage"] = True
    raw = json.dumps(document).encode() if mutation == "checksum" else refresh_hash(document)
    with pytest.raises(ValidationError):
        module.decode_record(raw)


@pytest.mark.parametrize(
    "raw",
    [b'{"schema_version":1,"schema_version":1}', b"\xff", b"NaN", b"[]", b'{"x":1e500}', b"["],
)
def test_codec_rejects_duplicate_utf8_nonfinite_and_invalid_json(raw):
    with pytest.raises(ValidationError):
        module.decode_record(raw)


def test_record_capacity_and_nested_unknown_type(monkeypatch):
    value = module.encode_record(record())
    monkeypatch.setattr(module, "MAX_RECORD_BYTES", len(value) - 1)
    with pytest.raises(ValidationError, match="capacity"):
        module.encode_record(record())
    with pytest.raises(ValidationError, match="size"):
        module.decode_record(value)


def test_real_append_idempotent_immutable_exclusive_owner_reopen(tmp_path):
    journal = module.FileVirtualMarginJournal(tmp_path, "a")
    value = record()
    try:
        assert journal.read("a") is None
        assert journal.append(value) is Durability.PRIMARY
        target = journal.primary / (value.record_id + ".json")
        original = target.read_bytes()
        assert original == module.encode_record(value)
        assert journal.append(value) is Durability.PRIMARY
        assert target.read_bytes() == original
        assert journal.read("a")[0].record == value
        with pytest.raises(RuntimeError, match="already owns"):
            module.FileVirtualMarginJournal(tmp_path, "a")
        with pytest.raises(ProtocolError, match="identity conflict"):
            journal.append(record(EntryControl(True, "pause")))
        assert target.read_bytes() == original
        with pytest.raises(ValidationError):
            journal.read("b")
    finally:
        journal.close()
    reopened = module.FileVirtualMarginJournal(tmp_path, "a")
    try:
        assert reopened.read("a")[0].record == value
    finally:
        reopened.close()
    with pytest.raises(StorageError, match="closed"):
        reopened.append(value)


def test_primary_ack_loss_preserves_both_exact_records_for_application_dedup(tmp_path, monkeypatch):
    journal = module.FileVirtualMarginJournal(tmp_path, "a")
    original = journal._publish

    def lose_ack(directory, value, payload):
        original(directory, value, payload)
        if directory == journal.primary:
            raise OSError("simulated ACK loss after publication")

    monkeypatch.setattr(journal, "_publish", lose_ack)
    try:
        assert journal.append(record()) is Durability.FALLBACK
        values = journal.read("a")
        assert len(values) == 2 and values[0].record == values[1].record
        assert {value.durability for value in values} == {Durability.PRIMARY, Durability.FALLBACK}
        store = FileMarginStateStore(tmp_path, "a")
        try:
            owner = MarginAccountOwner(initial(), store, create_if_missing=True)
            engine = VirtualMarginEngine(owner, journal)
            assert engine.snapshot().state == "ready"
            assert engine.snapshot().paused is True
        finally:
            store.close()
    finally:
        journal.close()


def test_primary_failure_uses_fallback_and_both_failures_are_volatile(tmp_path, monkeypatch):
    journal = module.FileVirtualMarginJournal(tmp_path, "a")
    original = journal._publish

    def unavailable(directory, *args):
        if directory == journal.primary:
            raise PermissionError("synthetic primary failure")
        return original(directory, *args)

    try:
        monkeypatch.setattr(journal, "_publish", unavailable)
        assert journal.append(record()) is Durability.FALLBACK
        assert journal.read("a")[0].durability is Durability.FALLBACK
        monkeypatch.setattr(
            journal, "_publish", lambda *args: (_ for _ in ()).throw(OSError("both unavailable"))
        )
        assert journal.append(record(EntryControl(True, "p"), identity="2")) is Durability.VOLATILE
        assert len(journal.read("a")) == 1
    finally:
        journal.close()


@pytest.mark.parametrize("fault", ["journal_ack", "account_ack"])
def test_real_engine_frozen_settlement_ack_loss_and_restart_no_new_prepare(
    tmp_path, monkeypatch, fault
):
    journal = module.FileVirtualMarginJournal(tmp_path, "a")
    store = FileMarginStateStore(tmp_path, "a")
    original_publish, original_save = journal._publish, store.save
    try:
        engine = VirtualMarginEngine(
            MarginAccountOwner(initial(), store, create_if_missing=True),
            journal,
            create_if_missing=True,
        )
        assert engine.snapshot().state == "ready"
        engine.manual_resume("resume")
        engine.submit_intent(
            order(), marks=(MarkSnapshot(BTC, D(100), 0, 0, "test_mark"),), brackets=(bracket(),)
        )
        prepared = engine.prepare_batch(batch().candles, as_of_ms=120000, brackets=(bracket(),))

        def publish(directory, value, payload):
            original_publish(directory, value, payload)
            if directory == journal.primary and isinstance(
                value.payload, module.PreparedMarginBatch
            ):
                raise OSError("synthetic journal ACK loss after commit")

        def save(account, *, expected_sequence):
            original_save(account, expected_sequence=expected_sequence)
            if account.ledger:
                raise StorageError("synthetic account ACK loss after commit")

        if fault == "journal_ack":
            monkeypatch.setattr(journal, "_publish", publish)
        else:
            monkeypatch.setattr(store, "save", save)
        with pytest.raises(StorageError if fault == "journal_ack" else MarginCommitUnknown):
            engine.commit_batch(prepared)
        assert engine.snapshot().state == "unknown"
        assert len(store.load("a").ledger) == (0 if fault == "journal_ack" else 1)
    finally:
        store.close()
        journal.close()

    monkeypatch.setattr(
        "omega.application.virtual_margin.prepare_pending_batch",
        lambda *a, **k: pytest.fail("restart cannot evaluate new prices"),
    )
    restored_journal = module.FileVirtualMarginJournal(tmp_path, "a")
    restored_store = FileMarginStateStore(tmp_path, "a")
    try:
        restored = VirtualMarginEngine(
            MarginAccountOwner(initial(), restored_store), restored_journal
        )
        result = restored.snapshot()
        assert result.state == "ready" and result.paused and result.pending == ()
        assert len(result.account.ledger) == 1
        assert tuple(value.fact for value in result.account.ledger) == prepared.facts
        assert result.account.cross_wallet == D("999.9")
        assert result.account.positions[0].quantity == D("1.00")
        assert restored.reconcile().account == result.account
    finally:
        restored_store.close()
        restored_journal.close()


def test_unavailable_custom_fallback_does_not_disable_healthy_primary(tmp_path):
    fallback = tmp_path / "fallback"
    fallback.write_bytes(b"unavailable namespace")
    journal = module.FileVirtualMarginJournal(tmp_path, "a", fallback_root=fallback)
    try:
        assert journal.append(record()) is Durability.PRIMARY
        with pytest.raises(StorageError):
            journal.read("a")
    finally:
        journal.close()


def test_read_corrupt_primary_never_uses_healthy_fallback_to_hide_it(tmp_path):
    journal = module.FileVirtualMarginJournal(tmp_path, "a")
    try:
        value = record()
        journal.append(value)
        journal._publish(journal.fallback, value, module.encode_record(value))
        target = journal.primary / (value.record_id + ".json")
        target.write_bytes(b"{broken")
        with pytest.raises(StorageError, match="corrupt|verified"):
            journal.read("a")
        with pytest.raises(StorageError, match="corrupt"):
            journal.append(value)
        assert target.read_bytes() == b"{broken"
    finally:
        journal.close()


def test_caps_preserve_original_and_no_automatic_deletion(tmp_path, monkeypatch):
    journal = module.FileVirtualMarginJournal(tmp_path, "a")
    try:
        first = record()
        assert journal.append(first) is Durability.PRIMARY
        monkeypatch.setattr(module, "MAX_RECORDS", 1)
        second = record(
            EntryControl(True, "pause"), sequence=2, previous=first.checksum, identity="2"
        )
        assert journal.append(second) is Durability.FALLBACK
        third = record(EntryControl(True, "p2"), sequence=3, previous=second.checksum, identity="3")
        assert journal.append(third) is Durability.VOLATILE
        assert len(journal.read("a")) == 2
        assert (journal.primary / (first.record_id + ".json")).is_file()
    finally:
        journal.close()


def test_orphan_stage_preserved_counted_but_not_replayed(tmp_path, monkeypatch):
    journal = module.FileVirtualMarginJournal(tmp_path, "a")
    try:
        journal.primary.mkdir(parents=True)
        orphan = journal.primary / ".stage-abandoned.json"
        orphan.write_bytes(b"original crash fragment")
        assert journal.read("a") is None
        monkeypatch.setattr(module, "MAX_NAMESPACE_BYTES", len(module.encode_record(record())))
        assert journal.append(record()) is Durability.FALLBACK
        assert orphan.read_bytes() == b"original crash fragment"
    finally:
        journal.close()


def test_other_source_unreadable_and_cross_account_file_are_errors(tmp_path):
    journal = module.FileVirtualMarginJournal(tmp_path, "a")
    try:
        value = record()
        journal.append(value)
        journal.fallback.parent.mkdir(parents=True, exist_ok=True)
        journal.fallback.write_bytes(b"a file instead of a namespace")
        with pytest.raises(StorageError):
            journal.read("a")
        journal.fallback.unlink()
        target = journal.primary / (value.record_id + ".json")
        target.write_bytes(
            module.encode_record(replace(value, account_id="b", payload=EntryControl(True, "p")))
        )
        with pytest.raises(StorageError, match="identity"):
            journal.read("a")
    finally:
        journal.close()


def test_namespace_overlap_and_invalid_identity_rejected(tmp_path):
    with pytest.raises(ValidationError, match="overlap"):
        module.FileVirtualMarginJournal(tmp_path, "a", fallback_root=tmp_path / "state")
    with pytest.raises(ValidationError):
        module.FileVirtualMarginJournal(tmp_path, "../escape")


@pytest.mark.parametrize("linked_part", ["file", "directory"])
def test_links_rejected_and_original_retained(tmp_path, linked_part):
    journal = module.FileVirtualMarginJournal(tmp_path, "a")
    try:
        outside = tmp_path / "outside"
        outside.mkdir()
        if linked_part == "file":
            journal.primary.mkdir(parents=True)
            target = outside / "keep.json"
            target.write_bytes(module.encode_record(record()))
            link = journal.primary / (record().record_id + ".json")
        else:
            journal.primary.parent.mkdir(parents=True)
            target, link = outside, journal.primary
        try:
            link.symlink_to(target, target_is_directory=linked_part == "directory")
        except OSError:
            pytest.skip("local environment cannot create symlinks")
        with pytest.raises(StorageError, match="linked"):
            journal.read("a")
        assert target.exists()
    finally:
        journal.close()


@pytest.mark.skipif(os.name == "nt", reason="FIFO is a POSIX boundary")
def test_fifo_is_rejected_without_blocking(tmp_path):
    journal = module.FileVirtualMarginJournal(tmp_path, "a")
    try:
        journal.primary.mkdir(parents=True)
        os.mkfifo(journal.primary / ("1" * 64 + ".json"))
        with pytest.raises(StorageError):
            journal.read("a")
    finally:
        journal.close()
