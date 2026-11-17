"""Read-only cold guards retain pending/orphan evidence without minting ACKs."""

import threading

from omega.domain.errors import StorageError
from omega.domain.inference import digest, encoded
from omega.runtime.causal_recovery_records import FeatureMutationRef, RecoveryJournal


def test_prepared_mutation_is_unsettled_after_actual_journal_reopen(tmp_path):
    journal = RecoveryJournal(tmp_path)
    args = {"pin": "test-ref", "consumer_receipt_sha256": "a" * 64}
    ref = FeatureMutationRef("old-core", 0, "actor", "token", "release", "b" * 64, digest(args))
    try:
        assert journal.has_unsettled_mutations() is False
        assert journal.prepare_mutation(ref, encoded(args)).state == "prepared"
        assert journal.has_unsettled_mutations() is True
    finally:
        journal.close()
    restored = RecoveryJournal(tmp_path)
    try:
        assert restored.has_unsettled_mutations() is True
        assert restored.recording_unknown is False
    finally:
        restored.close()


def test_uncheckpointed_next_tail_is_preserved_and_blocks_cold_adoption(tmp_path):
    journal = RecoveryJournal(tmp_path)
    try:
        path = journal._event_path(journal._snapshot["record_count"] + 1)
        path.write_text("{}", encoding="utf-8")  # Deliberate incomplete prefix, not a fake ACK.
        before = path.read_bytes()
        assert journal.has_unsettled_mutations() is True
        assert path.read_bytes() == before and journal.recording_unknown is False
    finally:
        journal.close()


def test_foreign_reader_cannot_use_original_coordinator_guard(tmp_path):
    journal, errors = RecoveryJournal(tmp_path), []

    def foreign():
        try:
            journal.has_unsettled_mutations()
        except Exception as error:
            errors.append(error)

    try:
        thread = threading.Thread(target=foreign)
        thread.start()
        thread.join(2)
        assert not thread.is_alive() and len(errors) == 1
        assert isinstance(errors[0], StorageError) and "original PID and Thread" in str(errors[0])
        assert journal.has_unsettled_mutations() is False
    finally:
        journal.close()
