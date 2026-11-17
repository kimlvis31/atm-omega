"""Actual temporary journal bytes, deliberate wrapper faults and owner fences."""

import json
import threading
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace

import pytest

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.inference import InferenceCommitAck, decoded, digest, encoded
from omega.runtime.causal_capacity import EvidenceBudgetLimits
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_recovery_records import (
    FeatureMutationRef,
    FeatureMutationRejected,
    FeatureMutationUnknown,
    MutationAckAuthority,
    MutationJournalAck,
    RecoveryJournal,
    canonical_recovery_document,
)


def mutation(number=0, *, kind="pin", args=None):
    args = {"consumer_id": "consumer", "number": number} if args is None else args
    ref = FeatureMutationRef(
        "core", 1, "actor", f"mutation-{number}", kind, f"pin-{number}", digest(args)
    )
    return ref, encoded(args)


def closed(journal):
    proof = journal.close()
    assert proof.all_confirmed
    assert len(proof.bindings) == 1
    assert proof.bindings[0].file_lease_confirmed
    return proof


def state_bytes(root):
    return (root / "state/causal-recovery.json").read_bytes()


def test_prepared_terminal_actual_files_idempotent_and_restart_truth(tmp_path):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    try:
        initial = journal.last_ack
        prepared = journal.prepare_mutation(ref, args)
        assert type(prepared) is MutationJournalAck and prepared.durable
        assert prepared.state == "prepared" and prepared.revision == initial.revision + 1
        assert journal.prepare_mutation(ref, args) == prepared
        terminal = journal.finish_mutation(ref, '{"pin":{"id":"pin-0"}}')
        assert terminal.state == "terminal"
        assert terminal.result_json == '{"pin":{"id":"pin-0"}}'
        before = state_bytes(tmp_path)
        assert journal.finish_mutation(ref, terminal.result_json) == terminal
        assert journal.prepare_mutation(ref, args) == terminal
        assert state_bytes(tmp_path) == before
        assert decoded(journal._store.read())["hot"] == {}
        assert journal.get_mutation(ref).arguments_json == args
        original_scope = journal.lease_scope
    finally:
        closed(journal)
    again = RecoveryJournal(tmp_path)
    try:
        record = again.get_mutation(ref)
        assert record.ack == terminal
        assert record.ref == ref and record.arguments_json == args
        assert again.lease_scope is not original_scope
        assert state_bytes(tmp_path) == before
    finally:
        closed(again)


def test_rpc_unknown_remains_hot_then_exact_original_terminal_settles(tmp_path):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    try:
        journal.prepare_mutation(ref, args)
        unknown = journal.mark_unknown(ref, "dispatch_timeout")
        assert unknown.state == "unknown" and unknown.code == "dispatch_timeout"
        assert not journal.recording_unknown
        assert journal.mark_unknown(ref, "dispatch_timeout") == unknown
        assert ref.sha256 in decoded(journal._store.read())["hot"]
        terminal = journal.finish_mutation(ref, '{"pin":{"id":"pin-0"}}')
        assert journal.mark_unknown(ref, "late_timeout") == terminal
        with pytest.raises(ProtocolError):
            journal.finish_mutation(ref, '{"pin":{"id":"different"}}')
        assert journal.get_mutation(ref).ack == terminal
    finally:
        closed(journal)


def test_same_original_token_changed_arguments_or_kind_conflicts_without_write(tmp_path):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    try:
        journal.prepare_mutation(ref, args)
        before = state_bytes(tmp_path)
        other = replace(ref, arguments_sha256=digest({"other": True}))
        with pytest.raises(ProtocolError):
            journal.prepare_mutation(other, '{"other":true}')
        with pytest.raises(ProtocolError):
            journal.prepare_mutation(replace(ref, kind="release"), args)
        with pytest.raises(ProtocolError):
            journal.prepare_mutation(ref, '{"different":true}')
        assert state_bytes(tmp_path) == before and not journal.recording_unknown
    finally:
        closed(journal)


def test_hot_pin_capacity_reserves32_releases_and_terminal_returns_slot(tmp_path):
    journal = RecoveryJournal(tmp_path)
    try:
        for number in range(96):
            journal.prepare_mutation(*mutation(number))
        before = state_bytes(tmp_path)
        with pytest.raises(FeatureMutationRejected) as caught:
            journal.prepare_mutation(*mutation(96))
        assert not caught.value.recording_unknown and not caught.value.dispatched
        assert state_bytes(tmp_path) == before and not journal.recording_unknown
        for number in range(96, 128):
            journal.prepare_mutation(*mutation(number, kind="release"))
        with pytest.raises(FeatureMutationRejected):
            journal.prepare_mutation(*mutation(128, kind="release"))
        released_ref = mutation(96, kind="release")[0]
        terminal = journal.finish_mutation(released_ref, '{"released":true}')
        # A terminal journal commit retains its issued object until the actual
        # actor receipt handshake, so it cannot silently free the memory slot.
        with pytest.raises(FeatureMutationRejected):
            journal.prepare_mutation(*mutation(128, kind="release"))
        assert journal.retire_mutation_ack(released_ref, terminal)
        journal.prepare_mutation(*mutation(128, kind="release"))
        assert len(decoded(journal._store.read())["hot"]) == 128
    finally:
        closed(journal)


def test_configured_evidence_capacity_is_known_refusal_and_release_reserve(tmp_path):
    limits = EvidenceBudgetLimits(max_records=16)
    journal = RecoveryJournal(tmp_path, limits=limits)
    try:
        for number in range(8):
            ref, args = mutation(number)
            journal.prepare_mutation(ref, args)
            journal.finish_mutation(ref, '{"pin":{"id":"known"}}')
        before = state_bytes(tmp_path)
        with pytest.raises(FeatureMutationRejected):
            journal.prepare_mutation(*mutation(9))
        assert state_bytes(tmp_path) == before and not journal.recording_unknown
        ref, args = mutation(10, kind="release")
        journal.prepare_mutation(ref, args)
        journal.finish_mutation(ref, '{"released":true}')
    finally:
        closed(journal)


@pytest.mark.parametrize("state", ["prepared", "terminal", "unknown"])
def test_real_record_fsync_then_checkpoint_failure_is_sticky_and_orphan_retained(
    tmp_path, monkeypatch, state
):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    if state != "prepared":
        journal.prepare_mutation(ref, args)
    previous = journal.last_ack
    real_save = FileInferenceStateStore.save_snapshot

    def fail_save(self, snapshot_json, *, expected_revision):
        raise OSError("deliberate wrapper secret-path")

    monkeypatch.setattr(FileInferenceStateStore, "save_snapshot", fail_save)
    try:
        with pytest.raises(FeatureMutationUnknown) as caught:
            if state == "prepared":
                journal.prepare_mutation(ref, args)
            elif state == "terminal":
                journal.finish_mutation(ref, '{"pin":{"id":"pin-0"}}')
            else:
                journal.mark_unknown(ref, "dispatch_timeout")
        assert caught.value.ref is ref and caught.value.recording_unknown
        assert "secret" not in str(caught.value)
        assert journal.recording_unknown and journal.last_ack == previous
        monkeypatch.setattr(FileInferenceStateStore, "save_snapshot", real_save)
        # Actual fresh read does not clear this original owner's unconfirmed IO.
        assert journal._store.read() is not None
        with pytest.raises(StorageError):
            journal.prepare_mutation(ref, args)
        assert journal.last_ack == previous
        orphan = journal._event_path(decoded(journal._store.read())["record_count"] + 1)
        original_orphan = orphan.read_bytes()
    finally:
        closed(journal)
    with pytest.raises(StorageError) as reopening:
        RecoveryJournal(tmp_path)
    assert orphan.read_bytes() == original_orphan
    assert reopening.value.lease_scope.close_all().all_confirmed


def test_exact_ack1_then_duck_ack2_is_unknown_despite_actual_committed_bytes(tmp_path, monkeypatch):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    journal.prepare_mutation(ref, args)
    old_ack = journal.last_ack
    original = FileInferenceStateStore.save_snapshot

    def duck_save(self, snapshot_json, *, expected_revision):
        ack = original(self, snapshot_json, expected_revision=expected_revision)
        return SimpleNamespace(
            **{"revision": ack.revision, "snapshot_sha256": ack.snapshot_sha256, "durable": True}
        )

    monkeypatch.setattr(FileInferenceStateStore, "save_snapshot", duck_save)
    try:
        with pytest.raises(FeatureMutationUnknown):
            journal.finish_mutation(ref, '{"pin":{"id":"pin-0"}}')
        assert journal.recording_unknown and journal.last_ack == old_ack
        assert decoded(journal._store.read())["revision"] == old_ack.revision + 1
        assert not journal._store.failed
        with pytest.raises(StorageError):
            journal.get_mutation(ref)
    finally:
        closed(journal)
    monkeypatch.setattr(FileInferenceStateStore, "save_snapshot", original)
    with pytest.raises(StorageError) as reopening:
        RecoveryJournal(tmp_path)
    assert reopening.value.lease_scope.close_all().all_confirmed


@pytest.mark.parametrize("fault", ["wrong_sha", "wrong_revision", "not_durable"])
def test_forged_exact_ack_fields_never_retire_prepared_slot(tmp_path, monkeypatch, fault):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    journal.prepare_mutation(ref, args)
    old_ack = journal.last_ack
    original = FileInferenceStateStore.save_snapshot

    def bad_save(self, snapshot_json, *, expected_revision):
        ack = original(self, snapshot_json, expected_revision=expected_revision)
        return InferenceCommitAck(
            ack.revision + (fault == "wrong_revision"),
            "0" * 64 if fault == "wrong_sha" else ack.snapshot_sha256,
            fault != "not_durable",
        )

    monkeypatch.setattr(FileInferenceStateStore, "save_snapshot", bad_save)
    try:
        with pytest.raises(FeatureMutationUnknown):
            journal.finish_mutation(ref, '{"pin":{"id":"pin-0"}}')
        assert ref.sha256 in journal._snapshot["hot"]
        assert journal.last_ack == old_ack and journal.recording_unknown
    finally:
        closed(journal)


def test_host_expected_revision_cas_and_bounded_native_payload(tmp_path):
    journal = RecoveryJournal(tmp_path)
    try:
        revision, host = journal.read_host()
        assert host == "{}"
        ack = journal.save_host(
            '{"phase":"intent","core_epoch":"old-core"}', expected_revision=revision
        )
        assert type(ack) is InferenceCommitAck and ack.durable
        with pytest.raises(ProtocolError):
            journal.save_host('{"phase":"wrong"}', expected_revision=revision)
        with pytest.raises(ValidationError):
            journal.save_host('{"float":1.25}', expected_revision=ack.revision)
        assert journal.read_host() == (ack.revision, '{"core_epoch":"old-core","phase":"intent"}')
        before = state_bytes(tmp_path)
    finally:
        closed(journal)
    restarted = RecoveryJournal(tmp_path)
    try:
        assert restarted.read_host() == (ack.revision, '{"core_epoch":"old-core","phase":"intent"}')
        assert state_bytes(tmp_path) == before
    finally:
        closed(restarted)


def test_foreign_thread_cannot_read_mutate_or_close_original_owner(tmp_path):
    journal = RecoveryJournal(tmp_path)
    failures = []

    def foreign():
        for callback in (
            journal.read_host,
            journal.close,
            journal.retirement_status,
            lambda: journal.prepare_mutation(*mutation()),
        ):
            try:
                callback()
            except StorageError as error:
                failures.append(str(error))

    try:
        worker = threading.Thread(target=foreign)
        worker.start()
        worker.join(5)
        assert not worker.is_alive() and len(failures) == 4
        assert not journal.recording_unknown
        journal.prepare_mutation(*mutation())
    finally:
        closed(journal)


def test_original_file_scope_exists_before_factory_failure(tmp_path):
    scopes = []

    def fail_factory(path, *, leases, role):
        scopes.append(leases)
        raise OSError("deliberate construction fault")

    with pytest.raises(OSError) as caught:
        RecoveryJournal(tmp_path, store_factory=fail_factory)
    assert caught.value.lease_scope is scopes[0]
    assert scopes[0].close_all().all_confirmed


def test_parent_traversal_rejected_before_file_factory(tmp_path):
    calls = []

    def factory(*args, **kwargs):
        calls.append(args)
        raise AssertionError("must not construct")

    with pytest.raises(ValidationError) as caught:
        RecoveryJournal(tmp_path / "child" / ".." / "escape", store_factory=factory)
    assert calls == []
    assert caught.value.lease_scope.close_all().all_confirmed


def test_changed_actual_immutable_arguments_latches_unknown(tmp_path):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    journal.prepare_mutation(ref, args)
    try:
        path = journal._event_path(1)
        value = json.loads(path.read_text(encoding="utf-8"))
        value["data"]["payload"]["number"] = 999
        value["sha256"] = digest(value["data"])
        path.write_text(encoded(value), encoding="utf-8")
        with pytest.raises(FeatureMutationUnknown):
            journal.get_mutation(ref)
        assert journal.recording_unknown
    finally:
        closed(journal)


def test_completed_direct_lookup_does_not_scan_lifetime_directory(tmp_path, monkeypatch):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    journal.prepare_mutation(ref, args)
    journal.finish_mutation(ref, '{"pin":{"id":"pin-0"}}')
    closed(journal)

    def no_scan(self):
        raise AssertionError("lifetime directory scan forbidden")

    monkeypatch.setattr(type(tmp_path), "iterdir", no_scan)
    again = RecoveryJournal(tmp_path)
    try:
        assert again.get_mutation(ref).ack.state == "terminal"
        again.prepare_mutation(*mutation(1))
    finally:
        closed(again)


@pytest.mark.parametrize(
    "raw", ["[]", '{"x":1,"x":2}', '{"x":NaN}', '{"x":1.0}', '{"x":9223372036854775808}']
)
def test_canonical_document_rejects_non_native_or_duplicate_json(raw):
    with pytest.raises(ValidationError):
        canonical_recovery_document(raw)


def test_reference_and_ack_exact_native_values_and_immutable_records():
    ref, args = mutation()
    assert canonical_recovery_document(' {"b":true, "a":1}') == '{"a":1,"b":true}'
    with pytest.raises(FrozenInstanceError):
        ref.generation = 2
    with pytest.raises(ValidationError):
        replace(ref, generation=True)
    with pytest.raises(ValidationError):
        replace(ref, actor_token="../escape")
    with pytest.raises(ValidationError):
        MutationJournalAck(ref.sha256, "terminal", 1, "1" * 64, "2" * 64, True)
    assert digest(decoded(args)) == ref.arguments_sha256


def test_actual_issued_terminal_ack_identity_only_and_io0_foreign_verifier(tmp_path, monkeypatch):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    authority = journal.mutation_ack_authority()
    journal.prepare_mutation(ref, args)
    terminal = journal.finish_mutation(ref, '{"pin":{"id":"pin-0"}}')
    forged_equal = replace(terminal)
    results = []

    def no_read(self):
        raise AssertionError("authority verifier must perform zero filesystem IO")

    def foreign_verify():
        results.extend(
            [
                authority.verify(ref, terminal, terminal.result_json),
                authority.verify(ref, forged_equal, terminal.result_json),
                authority.verify(ref, terminal, '{"pin":{"id":"different"}}'),
                authority.verify(replace(ref, actor_token="other"), terminal, terminal.result_json),
                authority.verify(
                    ref,
                    SimpleNamespace(**{"state": "terminal", "result_json": terminal.result_json}),
                    terminal.result_json,
                ),
            ]
        )
        try:
            journal.retire_mutation_ack(ref, terminal)
        except StorageError:
            results.append("retire_owner_fenced")

    monkeypatch.setattr(FileInferenceStateStore, "read", no_read)
    try:
        worker = threading.Thread(target=foreign_verify)
        worker.start()
        worker.join(5)
        assert not worker.is_alive()
        assert results == [True, False, False, False, False, "retire_owner_fenced"]
        assert not journal.retire_mutation_ack(ref, forged_equal)
        assert authority.verify(ref, terminal, terminal.result_json)
        assert journal.retire_mutation_ack(ref, terminal)
        assert not authority.verify(ref, terminal, terminal.result_json)
        assert not journal.retire_mutation_ack(ref, terminal)
    finally:
        closed(journal)


def test_constructed_authority_has_no_original_issuer_membership(tmp_path):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    authority = journal.mutation_ack_authority()
    try:
        prepared = journal.prepare_mutation(ref, args)
        assert not authority.verify(ref, prepared, '{"pin":null}')
        with pytest.raises(StorageError):
            authority._issue(object(), ref, prepared)
        terminal = journal.finish_mutation(ref, '{"pin":null}')
        forged = MutationAckAuthority(object())
        assert not forged.verify(ref, terminal, terminal.result_json)
        assert authority.verify(ref, terminal, terminal.result_json)
        assert journal.get_mutation(ref).ack is terminal
        assert journal.mark_unknown(ref, "late_timeout") is terminal
        assert journal.prepare_mutation(ref, args) is terminal
    finally:
        closed(journal)


def test_restarted_readback_never_mints_original_issued_ack_authority(tmp_path):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    journal.prepare_mutation(ref, args)
    original = journal.finish_mutation(ref, '{"pin":null}')
    first_authority = journal.mutation_ack_authority()
    closed(journal)
    restarted = RecoveryJournal(tmp_path)
    try:
        authority = restarted.mutation_ack_authority()
        readback = restarted.get_mutation(ref).ack
        assert readback == original and readback is not original
        assert not authority.verify(ref, original, original.result_json)
        assert not authority.verify(ref, readback, readback.result_json)
        # Explicit finish obtains another actual typed idempotent file ACK;
        # cold startup or a read-only query alone cannot mint this witness.
        reissued = restarted.finish_mutation(ref, readback.result_json)
        assert authority.verify(ref, reissued, reissued.result_json)
        assert not authority.verify(ref, original, original.result_json)
        assert not first_authority.verify(ref, reissued, reissued.result_json)
    finally:
        closed(restarted)


def test_failed_actual_terminal_ack_never_registers_authority(tmp_path, monkeypatch):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    journal.prepare_mutation(ref, args)
    authority = journal.mutation_ack_authority()
    original = FileInferenceStateStore.save_snapshot

    def duck_save(self, snapshot_json, *, expected_revision):
        ack = original(self, snapshot_json, expected_revision=expected_revision)
        return SimpleNamespace(
            revision=ack.revision, snapshot_sha256=ack.snapshot_sha256, durable=True
        )

    monkeypatch.setattr(FileInferenceStateStore, "save_snapshot", duck_save)
    try:
        with pytest.raises(FeatureMutationUnknown):
            journal.finish_mutation(ref, '{"pin":null}')
        actual = decoded(journal._store.read())
        fake = MutationJournalAck(
            ref.sha256,
            "terminal",
            actual["revision"],
            digest(actual),
            actual["record_head"],
            True,
            '{"pin":null}',
        )
        assert not authority.verify(ref, fake, fake.result_json)
        assert journal.recording_unknown
    finally:
        closed(journal)


@pytest.mark.parametrize("attribute", ["_issuer_token", "_thread", "_lock", "_issued", "_pid"])
def test_authority_original_identity_substitution_stays_rejected(tmp_path, attribute):
    journal = RecoveryJournal(tmp_path)
    ref, args = mutation()
    journal.prepare_mutation(ref, args)
    terminal = journal.finish_mutation(ref, '{"pin":null}')
    authority = journal.mutation_ack_authority()
    original = getattr(authority, attribute)
    try:
        setattr(authority, attribute, {} if attribute == "_issued" else object())
        assert not authority.verify(ref, terminal, terminal.result_json)
        setattr(authority, attribute, original)
        assert not authority.verify(ref, terminal, terminal.result_json)
        with pytest.raises(StorageError):
            journal.retire_mutation_ack(ref, terminal)
    finally:
        closed(journal)
