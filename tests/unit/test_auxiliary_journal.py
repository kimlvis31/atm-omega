"""Real filesystem journal resumes frozen normalized proposals and committed pins."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from functools import partial
from hashlib import sha256

import pytest

from omega.application.auxiliary_ingestion import AuxiliaryDelivery, PendingAuxiliary
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import decode_observation, encode_observation
from omega.ports.auxiliary import AuxiliaryWriteReceipt
from omega.ports.raw_data import RawArchiveReceipt
from omega.runtime.auxiliary_journal import AuxiliaryJobJournal

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
START = 1_700_000_040_000
BATCH = "b" * 64


def plan(**changes):
    return {
        "instrument": INSTRUMENT.key,
        "source": INSTRUMENT.key + ":aggTrade",
        "start_ms": START,
        "end_ms": START + 120000,
        "limit": 1000,
        "max_pages": 20,
        "destination_id": "d" * 64,
        **changes,
    }


def observation(index=0, **changes):
    opening = START + index * 60000
    return AuxiliaryInput(
        **{
            "channel": "trade_flow",
            "values": (
                ("AGGTRADE_QUANTITYBUY", Decimal("1.00")),
                ("AGGTRADE_QUANTITYSELL", Decimal("2.50")),
                ("AGGTRADE_NOTIONALBUY", Decimal("100.00")),
                ("AGGTRADE_NOTIONALSELL", Decimal("250.00")),
                ("AGGTRADE_NTRADESBUY", 2),
                ("AGGTRADE_NTRADESSELL", 1),
                ("AGGTRADE_EVENTCOUNT", 3),
            ),
            "window_start_ms": opening,
            "window_end_ms": opening + 60000,
            "event_time_ms": opening + 60000,
            "available_at_ms": opening + 60123,
            "source": INSTRUMENT.key + ":aggTrade",
            **changes,
        }
    )


def receipt(value, *, revision=1, inserted=True):
    digest = sha256(encode_observation(INSTRUMENT, value)).hexdigest()
    return AuxiliaryWriteReceipt(revision, digest, inserted)


def pending(index=0, **changes):
    job_id = sha256(
        json.dumps(plan(), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return PendingAuxiliary(
        **{
            "instrument": INSTRUMENT,
            "batch_id": f"{job_id}:{START + index * 60000}",
            "observation": observation(index),
            "original_generation": 4,
            **changes,
        }
    )


def new_journal(root, **changes):
    current = AuxiliaryJobJournal(root, plan(**changes))
    # A typed receipt pins an ID; this test intentionally does not imply a raw file exists.
    current.record_batch(RawArchiveReceipt(BATCH, "c" * 64, START + 60100, True))
    return current


def deliver(current, index=0, *, value=None, committed_previous=None, revision=1):
    value = value or observation(index)
    evidence = receipt(value, revision=revision)
    snapshot = pending(
        index,
        observation=value,
        receipt=evidence,
        expected_previous=committed_previous,
        committed_previous=committed_previous,
    )
    current.stage(snapshot)
    delivery = AuxiliaryDelivery(
        INSTRUMENT, snapshot.batch_id, value, evidence, 7, committed_previous
    )
    current.delivered(delivery)
    return snapshot, delivery


def write_tampered(current, mutate):
    envelope = json.loads(current.path.read_bytes())
    mutate(envelope["data"])
    encoded = json.dumps(
        envelope["data"], ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    envelope["sha256"] = sha256(encoded).hexdigest()
    current.path.write_text(json.dumps(envelope, ensure_ascii=False), encoding="utf-8")


def test_pending_reload_preserves_exact_availability_decimal_dtype_and_original_generation(
    tmp_path,
):
    current = new_journal(tmp_path)
    original = pending(expected_previous="a" * 64)
    current.stage(original)
    before = current.path.read_bytes()
    reordered = dict(reversed(list(plan().items())))
    resumed = AuxiliaryJobJournal(tmp_path, reordered)
    assert resumed.job_id == current.job_id
    assert AuxiliaryJobJournal.load_plan(tmp_path, current.job_id) == plan()
    assert resumed.pending() == original
    assert resumed.path.read_bytes() == before  # Loading a journal does not rewrite it.
    assert resumed.pending().observation.available_at_ms == START + 60123
    assert type(resumed.pending().observation.mapping()["AGGTRADE_NTRADESBUY"]) is int
    assert (
        resumed.pending().observation.mapping()["AGGTRADE_QUANTITYBUY"].as_tuple()
        == Decimal("1.00").as_tuple()
    )
    resumed.snapshot()["pending"] = None
    assert resumed.pending() == original


def test_unknown_commit_and_notification_pending_resume_without_new_availability(tmp_path):
    current = new_journal(tmp_path)
    original = pending()
    current.stage(original)
    unresolved = AuxiliaryJobJournal(tmp_path, plan())
    assert unresolved.pending().receipt is None
    assert unresolved.snapshot()["reason"] == "pending_commit"
    committed = replace(original, receipt=receipt(original.observation))
    unresolved.stage(committed)
    notified_later = AuxiliaryJobJournal(tmp_path, plan())
    assert notified_later.pending() == committed
    assert notified_later.snapshot()["reason"] == "pending_delivery"
    notified_later.delivered(
        AuxiliaryDelivery(
            INSTRUMENT, committed.batch_id, committed.observation, committed.receipt, 9
        )
    )
    assert notified_later.pending() is None
    assert notified_later.snapshot()["next_ms"] == START + 60000
    assert len(notified_later.snapshot()["deliveries"]) == 1
    with pytest.raises(ProtocolError):
        notified_later.delivered(
            AuxiliaryDelivery(
                INSTRUMENT, committed.batch_id, committed.observation, committed.receipt, 9
            )
        )


def test_canonical_stored_decimal_representation_and_actual_predecessor_are_pinned(tmp_path):
    current = new_journal(tmp_path)
    proposed = observation()
    canonical = replace(
        proposed,
        values=tuple(
            (key, value.quantize(Decimal(".0000")) if type(value) is Decimal else value)
            for key, value in proposed.values
        ),
    )
    assert canonical == proposed
    assert encode_observation(INSTRUMENT, canonical) != encode_observation(INSTRUMENT, proposed)
    stored = receipt(canonical, revision=3, inserted=False)
    snapshot = pending(observation=proposed, receipt=stored, committed_previous="a" * 64)
    current.stage(snapshot)  # Pending proposal bytes need not equal the stored canonical bytes yet.
    assert AuxiliaryJobJournal(tmp_path, plan()).pending() == snapshot
    current.delivered(
        AuxiliaryDelivery(INSTRUMENT, snapshot.batch_id, canonical, stored, 8, "a" * 64)
    )
    saved = AuxiliaryJobJournal(tmp_path, plan()).snapshot()["deliveries"][0]
    decoded_instrument, pinned = decode_observation(saved["payload"].encode())
    assert decoded_instrument == INSTRUMENT
    assert encode_observation(INSTRUMENT, pinned) == encode_observation(INSTRUMENT, canonical)
    assert saved["receipt"]["checksum"] == stored.checksum
    assert saved["committed_previous"] == "a" * 64 and saved["expected_previous"] is None


@pytest.mark.parametrize("operation", ["record_batch", "stage", "delivered", "finish"])
def test_atomic_save_failure_preserves_previous_memory_and_file(tmp_path, monkeypatch, operation):
    from omega.runtime import auxiliary_journal

    current = new_journal(tmp_path)
    if operation == "delivered":
        value = pending(receipt=receipt(observation()))
        current.stage(value)
        action = partial(
            current.delivered,
            AuxiliaryDelivery(INSTRUMENT, value.batch_id, value.observation, value.receipt, 4),
        )
    elif operation == "record_batch":
        action = partial(
            current.record_batch, RawArchiveReceipt("e" * 64, "f" * 64, START + 90000, True)
        )
    elif operation == "stage":
        action = partial(current.stage, pending())
    else:
        action = partial(current.finish, "stopped", complete=False)
    before, disk = current.snapshot(), current.path.read_bytes()

    def fail(_path, _value):
        raise OSError("synthetic write failure before atomic publication")

    monkeypatch.setattr(auxiliary_journal, "atomic_json", fail)
    with pytest.raises(StorageError):
        action()
    assert current.snapshot() == before and current.path.read_bytes() == disk


def test_complete_requires_every_contiguous_delivery_and_no_pending(tmp_path):
    current = new_journal(tmp_path)
    with pytest.raises(ValidationError):
        current.finish("complete", complete=True)
    current.stage(pending())
    with pytest.raises(ValidationError):
        current.finish("complete", complete=True)
    deliver(current, 0)
    with pytest.raises(ValidationError):
        current.finish("complete", complete=True)
    deliver(current, 1)
    current.finish("complete", complete=True)
    saved = AuxiliaryJobJournal(tmp_path, plan()).snapshot()
    assert saved["complete"] and saved["next_ms"] == plan()["end_ms"]
    assert len(saved["deliveries"]) == 2 and saved["pending"] is None


@pytest.mark.parametrize(
    "change", ["anchor", "receipt_rollback", "receipt_change", "unknown_batch"]
)
def test_pending_identity_and_known_commit_evidence_cannot_be_rewritten(tmp_path, change):
    current = new_journal(tmp_path)
    original = pending(receipt=receipt(observation()))
    current.stage(original)
    before = current.snapshot()
    if change == "anchor":
        action = partial(current.stage, replace(original, expected_previous="a" * 64))
    elif change == "receipt_rollback":
        action = partial(current.stage, replace(original, receipt=None))
    elif change == "receipt_change":
        other = receipt(original.observation, revision=2)
        action = partial(
            current.delivered,
            AuxiliaryDelivery(
                INSTRUMENT, original.batch_id, original.observation, other, 4, "a" * 64
            ),
        )
    else:
        empty = new_journal(tmp_path / "unknown")
        before = empty.snapshot()
        current = empty
        action = partial(current.stage, replace(original, batch_id="other-job:123"))
    with pytest.raises((ValidationError, ProtocolError)):
        action()
    assert current.snapshot() == before


@pytest.mark.parametrize(
    "change",
    [
        "cursor",
        "plan",
        "complete",
        "numeric_type",
        "delivery_digest",
        "delivery_edge",
        "missing_batch",
    ],
)
def test_valid_outer_checksum_does_not_hide_plan_cursor_typed_or_delivery_corruption(
    tmp_path, change
):
    current = new_journal(tmp_path)
    if change in {"delivery_digest", "delivery_edge"}:
        deliver(current, committed_previous="a" * 64, revision=2)
    else:
        current.stage(pending())

    def mutate(state):
        if change == "cursor":
            state["next_ms"] += 60000
        elif change == "plan":
            state["plan"]["limit"] = 500
        elif change == "complete":
            state["complete"] = True
        elif change == "missing_batch":
            state["batches"] = []
        elif change == "numeric_type":
            payload = json.loads(state["pending"]["payload"])
            for field in payload["observation"]["values"]:
                if field["name"] == "AGGTRADE_NTRADESBUY":
                    field["value"], field["type"] = "2", "decimal"
            state["pending"]["payload"] = json.dumps(payload)
        elif change == "delivery_digest":
            state["deliveries"][0]["receipt"]["checksum"] = "0" * 64
        else:
            state["deliveries"][0]["committed_previous"] = None

    write_tampered(current, mutate)
    with pytest.raises(StorageError):
        AuxiliaryJobJournal(tmp_path, plan())


@pytest.mark.parametrize(
    "changes",
    [
        {"end_ms": START + 660000},
        {"max_pages": 21},
        {"limit": True},
        {"start_ms": START + 1},
        {"source": "wrong"},
        {"destination_id": "secret"},
    ],
)
def test_plan_scope_and_budget_are_validated_before_writing(tmp_path, changes):
    with pytest.raises(ValidationError):
        AuxiliaryJobJournal(tmp_path, plan(**changes))
    assert not list(tmp_path.glob("*.json"))


def test_batch_budget_and_idempotent_repeat_keep_original_manifest(tmp_path):
    current = new_journal(tmp_path, max_pages=1)
    before = deepcopy(current.snapshot())
    current.record_batch(RawArchiveReceipt(BATCH, "c" * 64, START + 60100, False))
    assert current.snapshot() == before
    with pytest.raises(ValidationError):
        current.record_batch(RawArchiveReceipt("e" * 64, "f" * 64, START + 60100, True))
    assert current.snapshot() == before


def test_source_batch_set_is_frozen_after_normalization_starts(tmp_path):
    current = new_journal(tmp_path)
    current.stage(pending())
    before = current.snapshot()
    current.record_batch(RawArchiveReceipt(BATCH, "c" * 64, START + 60100, False))
    assert current.snapshot() == before
    with pytest.raises(ProtocolError):
        current.record_batch(RawArchiveReceipt("e" * 64, "f" * 64, START + 90000, True))
    assert current.snapshot() == before
