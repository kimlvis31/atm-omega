"""Registration sealing leaves actual original journal work and cleanup intact."""

import threading

import pytest

from omega.domain.errors import StorageError
from omega.domain.inference import CAUSAL_SCOPE, encoded
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_store_leases import InferenceStoreLeases


def test_scope_seal_registers_no_new_store_and_keeps_original_journal_usable(tmp_path, monkeypatch):
    leases = InferenceStoreLeases("seal-generation")
    store = FileInferenceStateStore(tmp_path / "journal.json", leases=leases, role="journal")
    original_stream = store.lock.stream
    try:
        with monkeypatch.context() as patch:

            def unexpected_close(*args, **kwargs):
                raise AssertionError("registration seal performed cleanup")

            patch.setattr(store, "close", unexpected_close)
            patch.setattr(leases.bindings[0], "close", unexpected_close)
            first = leases.seal_admission()
            assert leases.seal_admission() == first
        assert first.admission_closed and not first.all_confirmed
        assert not first.bindings[0].file_lease_confirmed
        assert store.lock.stream is original_stream and not original_stream.closed
        with pytest.raises(StorageError, match="admission"):
            leases.bind_store(object(), "nn_state")
        value = {"scope": CAUSAL_SCOPE, "revision": 0, "state": ["original"]}
        ack = store.save_snapshot(encoded(value), expected_revision=None)
        assert ack.durable and store.read() == encoded(value)
        assert not original_stream.closed
    finally:
        assert leases.close_all().all_confirmed
    assert original_stream.closed


def test_foreign_seal_rejects_before_scope_mutation(tmp_path):
    leases = InferenceStoreLeases("seal-original-owner")
    before = leases.snapshot()
    errors = []

    def foreign():
        try:
            leases.seal_admission()
        except BaseException as error:
            errors.append(type(error))

    thread = threading.Thread(target=foreign)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive() and errors == [StorageError]
    assert leases.snapshot() == before and not leases.snapshot().admission_closed
    assert leases.close_all().all_confirmed
