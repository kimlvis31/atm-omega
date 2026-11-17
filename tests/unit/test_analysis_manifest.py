"""Actual bounded segment writes, references, ownership and torn-tail preservation."""

from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from threading import Thread

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market
from omega.ports.analysis_inputs import PreparedMarketFrame
from omega.runtime import analysis_manifest as module
from omega.runtime.analysis_manifest import AnalysisInputManifest, InputPolicy, ManifestReference

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
POLICY = InputPolicy("a" * 64, ())


def test_hot_quota_change_blocks_new_records_without_deleting_pinned_input(tmp_path):
    quota = [4_000_000]
    writer = AnalysisInputManifest(tmp_path, quota_provider=lambda: quota[0])
    try:
        first = writer.stage("b" * 32, "c" * 32, 0, frames(), POLICY)
        writer._append({"schema_version": 1, "orphan_test_record": "x" * 1_500_000})
        before = writer.read(first)
        quota[0] = 2_000_000
        with pytest.raises(StorageError, match="quota reached"):
            writer._append({"schema_version": 1, "orphan_test_record": "x" * 1_000_000})
        assert writer.read(first) == before
        quota[0] = 4_000_000
        writer._append({"schema_version": 1, "orphan_test_record": "x" * 1_000_000})
        assert writer.read(first) == before
    finally:
        writer.close()


def test_directory_sync_failure_releases_owner_and_does_not_publish_a_reference(
    tmp_path, monkeypatch
):
    writer = AnalysisInputManifest(tmp_path)
    original = module._sync_directory
    monkeypatch.setattr(
        module, "_sync_directory", lambda path: (_ for _ in ()).throw(OSError("sync"))
    )
    with pytest.raises(StorageError, match="directory is unavailable"):
        writer.stage("b" * 32, "c" * 32, 0, frames(), POLICY)
    assert writer.owner is None and not list((writer.root / "segments").glob("*.jsonl"))
    monkeypatch.setattr(module, "_sync_directory", original)
    second = AnalysisInputManifest(tmp_path)
    try:
        assert second.stage("b" * 32, "c" * 32, 0, frames(), POLICY)
    finally:
        second.close()


def frames(start=0):
    candle = Candle(
        INSTRUMENT, start, start + 60000, *(Decimal(value) for value in (100, 102, 99, 101, 2))
    )
    return (PreparedMarketFrame(MarketFeatureFrame(candle, candle.close_time_ms), ()),)


def test_shared_source_chunk_is_deduplicated_and_ca_chains_remain_separate(tmp_path):
    writer = AnalysisInputManifest(tmp_path)
    try:
        first = writer.stage("b" * 32, "c" * 32, 0, frames(), POLICY)
        other = writer.stage("d" * 32, "e" * 32, 0, frames(), POLICY)
        head = writer.read(first)
        assert head["source"] == writer.read(other)["source"]
        assert head["previous"] is None and head["end_ms"] == 60000
        following = writer.stage("b" * 32, "c" * 32, 0, frames(60000), POLICY, previous=first)
        assert writer.read(following)["previous"] == first.document()
        with pytest.raises(ValidationError, match="identity changed"):
            writer.stage("b" * 32, "c" * 32, 0, frames(), POLICY, previous=first)
        paths = list((writer.root / "segments").glob("*.jsonl"))
        assert len(paths) == 1 and len(paths[0].read_bytes().splitlines()) == 5
    finally:
        writer.close()
    reader = AnalysisInputManifest(tmp_path)
    assert reader.read(first) == head
    assert reader.owner is None


def test_record_tampering_bad_offsets_and_traversal_never_return_data(tmp_path):
    writer = AnalysisInputManifest(tmp_path)
    reference = writer.stage("b" * 32, "c" * 32, 0, frames(), POLICY)
    writer.close()
    with pytest.raises(ValidationError):
        ManifestReference("../outside", 0, 20, "a" * 64)
    with pytest.raises(StorageError):
        writer.read(replace(reference, offset=reference.offset + 1))
    path = writer.root / "segments" / (reference.segment + ".jsonl")
    data = bytearray(path.read_bytes())
    data[reference.offset + 10] ^= 1
    path.write_bytes(data)
    with pytest.raises(StorageError, match="corrupt"):
        writer.read(reference)


def test_new_owner_preserves_torn_segment_and_uses_a_new_segment(tmp_path):
    first = AnalysisInputManifest(tmp_path)
    reference = first.stage("b" * 32, "c" * 32, 0, frames(), POLICY)
    first.close()
    path = first.root / "segments" / (reference.segment + ".jsonl")
    with path.open("ab") as stream:
        stream.write(b'{"torn":')
    before = path.read_bytes()
    second = AnalysisInputManifest(tmp_path)
    try:
        current = second.stage("b" * 32, "d" * 32, 1, frames(60000), POLICY, previous=reference)
        assert current.segment != reference.segment and path.read_bytes() == before
        assert second.read(reference)["end_ms"] == 60000
    finally:
        second.close()


def test_writer_lock_survives_until_owner_close_and_foreign_thread_cannot_append(tmp_path):
    first, second = AnalysisInputManifest(tmp_path), AnalysisInputManifest(tmp_path)
    first.stage("b" * 32, "c" * 32, 0, frames(), POLICY)
    try:
        with pytest.raises(StorageError, match="busy"):
            second.stage("d" * 32, "e" * 32, 0, frames(), POLICY)
        errors = []

        def attempt():
            try:
                first.stage("d" * 32, "e" * 32, 0, frames(), POLICY)
            except Exception as error:
                errors.append(error)

        thread = Thread(target=attempt)
        thread.start()
        thread.join(2)
        assert not thread.is_alive() and len(errors) == 1 and isinstance(errors[0], StorageError)
    finally:
        first.close()
    try:
        assert (
            second.read(second.stage("d" * 32, "e" * 32, 0, frames(), POLICY))["analysis_id"]
            == "d" * 32
        )
    finally:
        second.close()


def test_unconfirmed_fsync_preserves_old_record_and_next_retry_rotates(tmp_path, monkeypatch):
    writer = AnalysisInputManifest(tmp_path)
    first = writer.stage("b" * 32, "c" * 32, 0, frames(), POLICY)
    original = module.os.fsync
    monkeypatch.setattr(
        module.os, "fsync", lambda descriptor: (_ for _ in ()).throw(OSError("synthetic fsync"))
    )
    with pytest.raises(StorageError, match="confirmed"):
        writer.stage("b" * 32, "c" * 32, 0, frames(60000), POLICY, previous=first)
    monkeypatch.setattr(module.os, "fsync", original)
    assert writer.read(first)["end_ms"] == 60000
    second = writer.stage("b" * 32, "c" * 32, 0, frames(60000), POLICY, previous=first)
    assert second.segment != first.segment
    writer.close()


@pytest.mark.parametrize(
    "value",
    [
        None,
        {},
        {"channels": []},
        {**POLICY.document(), "channels": [{}]},
        {**POLICY.document(), "schema_version": True},
    ],
)
def test_invalid_input_policy_documents_do_not_create_storage(tmp_path, value):
    with pytest.raises(ValidationError):
        InputPolicy.from_document(value)
    assert list(Path(tmp_path).iterdir()) == []
