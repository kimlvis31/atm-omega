"""Real temporary filesystem checks for exact public-response wire archives."""

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.raw_archive import FileRawArchive
from omega.ports.raw_data import PublicRawResponse

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
WIRE = b'[\r\n {"p":"100.0000","quantity":1e-8,"flag":true}\r\n]\r\n'


def record(**changes):
    return PublicRawResponse(
        **{
            "instrument": INSTRUMENT,
            "channel": "trade_flow",
            "request_parameters": (("limit", "1000"), ("symbol", "ETHUSDT")),
            "received_at_ms": 1_700_000_040_123,
            "body": WIRE,
            "headers": (("content-type", "application/json"), ("x-mbx-used-weight-1m", "7")),
            **changes,
        }
    )


def body_path(root, digest):
    return root / "objects" / digest[:2] / (digest + ".bin")


def receipt_path(root, identity):
    return root / "receipts" / identity[:2] / (identity + ".json")


def test_wire_bytes_crlf_whitespace_and_number_representation_survive_reopen(tmp_path):
    archive = FileRawArchive(tmp_path)
    original = record()
    receipt = archive.put(original)
    assert receipt.inserted is True
    assert receipt.body_sha256 == sha256(WIRE).hexdigest()
    assert receipt.received_at_ms == original.received_at_ms
    assert len(receipt.archive_id) == 64
    assert body_path(tmp_path, receipt.body_sha256).read_bytes() == WIRE
    stored = FileRawArchive(tmp_path).get(receipt.archive_id)
    assert stored.record == original
    assert stored.record.body == WIRE and b"\r\n" in stored.record.body
    assert stored.receipt.archive_id == receipt.archive_id
    assert stored.receipt.body_sha256 == receipt.body_sha256


def test_archive_preserves_non_json_body_bytes_without_normalization(tmp_path):
    original = record(body=b"\xff\x00provider failure\r\n")
    archive = FileRawArchive(tmp_path)
    receipt = archive.put(original)
    assert archive.get(receipt.archive_id).record.body == original.body


def test_exact_record_deduplicates_but_new_received_time_has_separate_receipt(tmp_path):
    archive = FileRawArchive(tmp_path)
    original = record()
    first = archive.put(original)
    duplicate = archive.put(original)
    assert not duplicate.inserted
    assert duplicate.archive_id == first.archive_id
    newer = archive.put(replace(original, received_at_ms=original.received_at_ms + 10))
    assert newer.inserted and newer.archive_id != first.archive_id
    assert newer.body_sha256 == first.body_sha256
    assert len(list((tmp_path / "objects").rglob("*.bin"))) == 1
    assert len(list((tmp_path / "receipts").rglob("*.json"))) == 2
    assert archive.get(first.archive_id).record.received_at_ms == original.received_at_ms


def test_parameter_and_allowed_header_order_does_not_change_canonical_identity(tmp_path):
    archive = FileRawArchive(tmp_path)
    original = record()
    first = archive.put(original)
    reordered = replace(
        original,
        request_parameters=original.request_parameters[::-1],
        headers=original.headers[::-1],
    )
    second = archive.put(reordered)
    assert second.archive_id == first.archive_id and not second.inserted


@pytest.mark.parametrize("target", ["body", "metadata"])
def test_tampered_content_is_rejected_and_never_overwritten_by_retry(tmp_path, target):
    archive = FileRawArchive(tmp_path)
    original = record()
    first = archive.put(original)
    path = (
        body_path(tmp_path, first.body_sha256)
        if target == "body"
        else receipt_path(tmp_path, first.archive_id)
    )
    corrupt = b'{"corrupt":true}\r\n'
    path.write_bytes(corrupt)
    with pytest.raises(StorageError):
        archive.get(first.archive_id)
    with pytest.raises(StorageError):
        archive.put(original)
    assert path.read_bytes() == corrupt


def test_preexisting_wrong_body_at_content_address_is_retained(tmp_path):
    archive = FileRawArchive(tmp_path)
    original = record()
    path = body_path(tmp_path, sha256(original.body).hexdigest())
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"different original at this address")
    with pytest.raises(StorageError):
        archive.put(original)
    assert path.read_bytes() == b"different original at this address"
    assert not list((tmp_path / "receipts").rglob("*.json"))


def test_metadata_publish_failure_returns_no_receipt_and_retry_reuses_durable_body(
    tmp_path, monkeypatch
):
    from omega.infrastructure import raw_archive

    archive = FileRawArchive(tmp_path)
    original = record()
    actual_link = raw_archive.os.link

    def fail_metadata(source, destination, *args, **kwargs):
        if Path(destination).suffix == ".json":
            raise OSError("synthetic metadata publish failure")
        return actual_link(source, destination, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(raw_archive.os, "link", fail_metadata)
        with pytest.raises(StorageError):
            archive.put(original)
    assert body_path(tmp_path, sha256(WIRE).hexdigest()).read_bytes() == WIRE
    assert not list((tmp_path / "receipts").rglob("*.json"))
    assert not list(tmp_path.rglob(".raw-*"))
    receipt = archive.put(original)
    assert receipt.inserted and archive.get(receipt.archive_id).record == original


@pytest.mark.parametrize("stage", ["body", "metadata"])
def test_fsync_failure_never_returns_a_new_receipt_or_removes_existing_records(
    tmp_path, monkeypatch, stage
):
    from omega.infrastructure import raw_archive

    archive = FileRawArchive(tmp_path)
    original = record()
    before = []
    if stage == "metadata":
        first = archive.put(original)
        before = [first.archive_id]
        original = replace(original, received_at_ms=original.received_at_ms + 10)

    def fail(_descriptor):
        raise OSError("synthetic fsync failure")

    with monkeypatch.context() as patch:
        patch.setattr(raw_archive.os, "fsync", fail)
        with pytest.raises(StorageError):
            archive.put(original)
    assert sorted(path.stem for path in (tmp_path / "receipts").rglob("*.json")) == before
    assert not list(tmp_path.rglob(".raw-*"))
    if before:
        assert archive.get(before[0]).record.body == WIRE


def test_concurrent_equal_writers_publish_one_receipt_and_no_partial_files(tmp_path):
    barrier = threading.Barrier(4)

    def publish(_index):
        archive = FileRawArchive(tmp_path)
        barrier.wait(timeout=5)
        return archive.put(record())

    with ThreadPoolExecutor(max_workers=4) as pool:
        receipts = list(pool.map(publish, range(4)))
    assert sum(value.inserted for value in receipts) == 1
    assert len({value.archive_id for value in receipts}) == 1
    assert len(list((tmp_path / "receipts").rglob("*.json"))) == 1
    assert len(list((tmp_path / "objects").rglob("*.bin"))) == 1
    assert not list(tmp_path.rglob(".raw-*"))
    assert FileRawArchive(tmp_path).get(receipts[0].archive_id).record.body == WIRE


@pytest.mark.parametrize("identity", ["../secrets", "a" * 63, "A" * 64, "g" * 64, None, True])
def test_invalid_receipt_identifier_is_rejected_before_path_lookup(tmp_path, identity):
    with pytest.raises(ValidationError):
        FileRawArchive(tmp_path).get(identity)


def test_absent_well_formed_receipt_is_storage_failure(tmp_path):
    with pytest.raises(StorageError):
        FileRawArchive(tmp_path).get("0" * 64)


@pytest.mark.parametrize(
    "field", ["unknown_field", "schema_version", "received_at_ms", "body_sha256"]
)
def test_valid_metadata_hash_does_not_bypass_schema_and_body_integrity(tmp_path, field):
    archive = FileRawArchive(tmp_path)
    original = archive.put(record())
    metadata = json.loads(receipt_path(tmp_path, original.archive_id).read_bytes())
    if field == "unknown_field":
        metadata["unexpected"] = "extra"
    elif field == "schema_version":
        metadata[field] = True
    elif field == "received_at_ms":
        metadata[field] = "1700000040123"
    else:
        metadata[field] = "0" * 64
    encoded = json.dumps(
        metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()
    identity = sha256(encoded).hexdigest()
    path = receipt_path(tmp_path, identity)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encoded)
    with pytest.raises(StorageError):
        archive.get(identity)
    assert path.read_bytes() == encoded
    assert archive.get(original.archive_id).record.body == WIRE


@pytest.mark.parametrize(
    "name", ["apiKey", "api_key", "token", "secret", "password", "signature", "Authorization"]
)
def test_secret_parameter_keys_are_not_archived(tmp_path, name):
    with pytest.raises(ValidationError):
        FileRawArchive(tmp_path).put(record(request_parameters=((name, "redacted-test-value"),)))
    assert not list(tmp_path.rglob("*.bin"))


@pytest.mark.parametrize(
    "header",
    [
        ("authorization", "test"),
        ("set-cookie", "test"),
        ("x-mbx-apikey", "test"),
        ("content-type", "application/json\r\nsecret"),
    ],
)
def test_nonpublic_or_invalid_headers_are_rejected(tmp_path, header):
    with pytest.raises(ValidationError):
        FileRawArchive(tmp_path).put(record(headers=(header,)))


@pytest.mark.parametrize(
    "changes",
    [
        {"body": bytearray(b"x")},
        {"body": b"x" * (8 * 1024 * 1024 + 1)},
        {"received_at_ms": True},
        {"request_parameters": (("note", "x" * 65536),)},
    ],
)
def test_body_metadata_and_timestamp_bounds_fail_before_publication(tmp_path, changes):
    with pytest.raises(ValidationError):
        FileRawArchive(tmp_path).put(record(**changes))
    assert not list(tmp_path.rglob("*.bin"))


def test_metadata_limit_counts_utf8_bytes_not_only_printable_character_count(tmp_path):
    parameters = tuple((f"parameter{index}", "가" * 2048) for index in range(12))
    with pytest.raises(ValidationError, match="metadata exceeds"):
        FileRawArchive(tmp_path).put(record(request_parameters=parameters))
    assert not list(tmp_path.rglob("*.bin"))


@pytest.mark.parametrize("subdirectory", ["objects", "receipts"])
def test_archive_rejects_symlink_subdirectories_and_preserves_external_target(
    tmp_path, subdirectory
):
    root, outside = tmp_path / "archive", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    sentinel = outside / "keep"
    sentinel.write_bytes(b"untouched")
    try:
        os.symlink(outside, root / subdirectory, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("creating a local directory symlink is not available on this platform")
    with pytest.raises(StorageError):
        FileRawArchive(root).put(record())
    assert sentinel.read_bytes() == b"untouched"
