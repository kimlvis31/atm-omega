"""Actor cache and journal evidence; actual local files, no model/account/DB."""

import json
import threading
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError
from omega.domain.inference import CAUSAL_SCOPE, digest
from omega.runtime.causal_inference_service import CausalInferenceService
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_store_leases import InferenceStoreLeases


def service(root):
    return CausalInferenceService(
        root, core_epoch="retirement-cache", training_job_provider=lambda _: None
    )


def test_final_cache_and_original_join_are_distinct_non_recovery_evidence(tmp_path):
    actor = service(tmp_path)
    assert actor.retirement_snapshot() is None
    assert actor.actor_join_observation() is None
    actor.start()
    original = actor.thread
    actor.close()
    record = actor.retirement_snapshot()
    joined = actor.actor_join_observation()
    assert record.sealed and record.admission_closed and record.physical_confirmed
    assert record.stores.all_confirmed and not record.recording_unknown
    assert record.last_journal_ack is not None
    assert joined.original_thread_dead and joined.join_returned and joined.identity_intact
    assert joined.owner_thread_token == record.owner_thread_token
    assert record.actor_recovery_authorized is joined.actor_recovery_authorized is False
    assert not original.is_alive()


def test_host_cached_read_performs_no_resource_callback_even_from_another_thread(tmp_path):
    actor = service(tmp_path)
    actor.start()
    actor.close()
    original = actor.retirement_snapshot()
    calls = []

    def forbidden():
        calls.append(True)
        raise AssertionError("host must never re-probe retired actor resources")

    actor._file_leases.snapshot = forbidden
    actor.store.retirement_status = forbidden
    values = []
    host = threading.Thread(target=lambda: values.append(actor.retirement_snapshot()))
    host.start()
    host.join(timeout=2)
    assert not host.is_alive() and values == [original] and calls == []


def test_public_thread_substitution_cannot_change_original_join_target(tmp_path):
    actor = service(tmp_path)
    actor.start()
    original = actor.thread
    actor.close()
    actor.thread = threading.Thread(target=lambda: None)
    with pytest.raises(StorageError, match="original actor identity"):
        actor.close()
    result = actor.actor_join_observation()
    assert result.original_thread_dead and not result.identity_intact
    assert not original.is_alive() and not actor.thread.is_alive()
    assert result.actor_recovery_authorized is False


def journal_actor(root):
    actor = service(root)
    actor._data = {
        "scope": CAUSAL_SCOPE,
        "revision": 0,
        "control_revision": 0,
        "operations": {},
        "deployments": {},
    }
    return actor


@pytest.mark.parametrize("failure", [OSError("write failed"), KeyboardInterrupt(), SystemExit(7)])
def test_later_journal_unknown_keeps_original_last_ack_and_revision(tmp_path, failure):
    actor = journal_actor(tmp_path)
    actor._file_leases = InferenceStoreLeases("original-journal")
    actor.store = FileInferenceStateStore(
        tmp_path / "journal.json", leases=actor._file_leases, role="journal"
    )
    actor._original_journal_store = actor.store
    actor._original_journal_binding = actor._file_leases.binding_for(actor.store)
    try:
        actor._save(publish=False)
        acknowledged = actor._retirement_last_ack
        assert acknowledged is not None
        old_data = actor._data

        def deliberate_failure(raw, *, expected_revision):
            raise failure  # Deliberate pre-write callback fault, not an OS fault claim.

        actor.store.save_snapshot = deliberate_failure
        if isinstance(failure, Exception):
            with pytest.raises(StorageError, match="recording is unconfirmed"):
                actor._save(publish=False)
        else:
            with pytest.raises(type(failure)) as caught:
                actor._save(publish=False)
            assert caught.value is failure
        assert actor._retirement_last_ack is acknowledged
        assert actor._stored_revision == 1 and actor._data is old_data
        assert actor._retirement_recording_unknown and actor._retirement_write_pending
    finally:
        actor.store.close()


def test_legacy_duck_ack_save_behavior_is_preserved_without_typed_ack(tmp_path):
    actor = journal_actor(tmp_path)

    def save(raw, *, expected_revision):
        data = json.loads(raw)
        return SimpleNamespace(
            revision=data["revision"], snapshot_sha256=digest(data), durable=True
        )

    actor.store = SimpleNamespace(save_snapshot=save)
    actor._original_journal_store = actor.store
    actor._save(publish=False)
    assert actor._stored_revision == 1 and actor._retirement_last_ack is None
    assert not actor._retirement_recording_unknown and not actor._retirement_write_pending


def test_typed_then_legacy_ack_cannot_reuse_previous_complete_evidence(tmp_path):
    actor = journal_actor(tmp_path)
    actor._file_leases = InferenceStoreLeases("original-journal")
    actor.store = FileInferenceStateStore(
        tmp_path / "journal.json", leases=actor._file_leases, role="journal"
    )
    actor._original_journal_store = actor.store
    actor._original_journal_binding = actor._file_leases.binding_for(actor.store)
    original_save = actor.store.save_snapshot
    try:
        actor._save(publish=False)
        first = actor._retirement_last_ack
        assert first is not None and not actor._retirement_capture_failed

        def legacy_wrapper(raw, *, expected_revision):
            ack = original_save(raw, expected_revision=expected_revision)
            return SimpleNamespace(
                revision=ack.revision, snapshot_sha256=ack.snapshot_sha256, durable=ack.durable
            )

        actor.store.save_snapshot = legacy_wrapper
        actor._save(publish=False)
        assert actor._stored_revision == 2
        assert actor._retirement_last_ack is first
        assert actor._retirement_capture_failed and actor.retirement_snapshot() is None
        assert not actor._retirement_recording_unknown and not actor._retirement_write_pending
    finally:
        actor.store.close()


def test_capture_failure_cannot_publish_an_empty_positive_final_record(tmp_path):
    actor = service(tmp_path)
    actor.start()

    def cannot_capture(**_):
        raise KeyboardInterrupt()

    actor._original_resources.snapshot = cannot_capture
    actor.close()
    assert actor.closed and not actor.thread.is_alive()
    assert actor._retirement_capture_failed and actor.retirement_snapshot() is None


@pytest.mark.parametrize("primary", [KeyboardInterrupt(), SystemExit(9)])
def test_evidence_finalizer_does_not_replace_primary_cleanup_baseexception(tmp_path, primary):
    actor = service(tmp_path)
    # Run this controlled actor loop on its actual bound thread, so the existing
    # provider fence does not introduce an unrelated cleanup ownership error.
    actor.thread = threading.current_thread()
    actor._original_actor_thread = actor.thread
    actor.closing = True
    actor._initialize = lambda: None

    def cleanup():
        raise primary

    actor._cleanup_resources = cleanup
    actor._retire_quarantine = lambda: None
    actor._close_final_file_stores = lambda: None
    with pytest.raises(type(primary)) as caught:
        actor._loop()
    assert caught.value is primary
    assert actor.retirement_snapshot() is None and actor._retirement_capture_failed


@pytest.mark.parametrize(
    "primary", [ValueError("source acquisition"), KeyboardInterrupt(), SystemExit(4)]
)
def test_failed_acquisition_recording_cannot_mask_original_factory_exception(tmp_path, primary):
    actor = service(tmp_path)
    actor.activation = SimpleNamespace()

    def factory(_):
        raise primary

    def cannot_record(_):
        raise SystemExit(99)

    actor.source_factory = factory
    actor._original_resources = SimpleNamespace(
        reserve_run=lambda: ("source", "worker", "owner"),
        enter=lambda *_, **__: None,
        acquisition_failed=cannot_record,
    )
    with pytest.raises(type(primary)) as caught:
        actor._build_owner({"id": "a" * 32})
    assert caught.value is primary and actor._retirement_capture_failed
