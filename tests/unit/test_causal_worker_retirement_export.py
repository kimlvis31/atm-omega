"""Typed original transport evidence; native startup fixtures execute no model."""

import threading
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace

import pytest
import test_causal_nn_retirement as legacy_fixture
import test_causal_worker_transport as transport_fixture

from omega.domain.errors import ProtocolError, StorageError
from omega.runtime import causal_nn_worker as worker_module
from omega.runtime.causal_nn_worker import CausalInferenceProcessWorker
from omega.runtime.causal_process_resources import HostRetirement
from omega.runtime.causal_receipt_channel import ReceiptFailure, ReceiptRetirementStatus
from omega.runtime.causal_worker_retirement import (
    WorkerTransportRetirement,
    validate_worker_transport_retirement,
)


def lazy_worker(tmp_path):
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    return CausalInferenceProcessWorker(
        SimpleNamespace(verify=lambda: None), inputs, tmp_path / "execution"
    )


def test_lazy_original_close_is_distinct_from_a_closed_flag(tmp_path):
    worker = lazy_worker(tmp_path)
    token = worker.transport_worker_token
    first = worker.transport_retirement_snapshot()
    assert type(first) is WorkerTransportRetirement and first.worker_token == token
    assert not first.physical_confirmed and not first.never_started_confirmed
    worker.closed = True
    flag_only = worker.transport_retirement_snapshot()
    assert not flag_only.admission_closed and not flag_only.physical_confirmed
    worker.close()
    final = worker.transport_retirement_snapshot()
    assert final.identity_intact and final.admission_closed
    assert final.never_started_confirmed and final.physical_confirmed
    assert final.host is final.receipt is final.channel_binding_token is None
    assert final.normal_model_cleanup == "not_started"
    assert not final.actor_recovery_authorized and not final.all_kernel_resources_confirmed
    assert worker.transport_retirement_snapshot() == final
    with pytest.raises(FrozenInstanceError):
        final.physical_confirmed = False
    with pytest.raises(TypeError, match="actor_recovery_authorized.*init=False"):
        replace(final, actor_recovery_authorized=True)


def test_foreign_thread_snapshot_and_token_do_not_mutate_original(tmp_path):
    worker = lazy_worker(tmp_path)
    before = vars(worker).copy()
    errors = []

    def foreign():
        for read in (worker.transport_retirement_snapshot, lambda: worker.transport_worker_token):
            try:
                read()
            except BaseException as error:
                errors.append(type(error))

    thread = threading.Thread(target=foreign)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive() and errors == [ProtocolError, ProtocolError]
    assert vars(worker) == before
    worker.close()


def test_foreign_pid_snapshot_does_not_mutate_original(tmp_path, monkeypatch):
    worker = lazy_worker(tmp_path)
    before = vars(worker).copy()
    with monkeypatch.context() as patch:
        patch.setattr(worker_module, "os", SimpleNamespace(getpid=lambda: -1))
        with pytest.raises(ProtocolError, match="original PID"):
            worker.transport_retirement_snapshot()
    assert vars(worker) == before
    worker.close()


def test_uninitialized_legacy_port_keeps_old_close_without_new_evidence_authority():
    process = legacy_fixture.Process()
    worker = legacy_fixture.worker_for(process)
    process.allow_exit = True
    worker.close()
    assert process.closed and worker.retirement_status()["confirmed"]
    assert "_transport_close_attempted" not in vars(worker)
    with pytest.raises(ProtocolError, match="original PID"):
        worker.transport_retirement_snapshot()


@pytest.mark.parametrize("failure_phase", ["first_queue", "second_queue", "process_start"])
@pytest.mark.parametrize("error_type", [OSError, KeyboardInterrupt, SystemExit])
def test_partial_start_never_becomes_lazy_physical_ack(
    tmp_path, monkeypatch, failure_phase, error_type
):
    worker = lazy_worker(tmp_path)
    original_error = error_type("deliberate startup fault")
    created = []

    class Queue:
        def cancel_join_thread(self):
            pass

        def close(self):
            pass

    class Process:
        exitcode = None

        def start(self):
            raise original_error

        def is_alive(self):
            return False

        def close(self):
            pass

    def make_queue(**kwargs):
        if failure_phase == "first_queue" or failure_phase == "second_queue" and created:
            raise original_error
        value = Queue()
        created.append(value)
        return value

    context = SimpleNamespace(Queue=make_queue, Process=lambda **kwargs: Process())
    with monkeypatch.context() as patch:
        patch.setattr(worker_module.multiprocessing, "get_context", lambda name: context)
        with pytest.raises(error_type) as caught:
            worker._start()
        assert caught.value is original_error
    worker.close()  # Legacy bounded cleanup behavior still applies to these fake ports.
    record = worker.transport_retirement_snapshot()
    assert record.start_attempted and not record.never_started_confirmed
    assert not record.physical_confirmed and record.normal_model_cleanup == "unconfirmed"
    assert "transport_started_original_receipts_missing" in record.errors
    assert record.host is record.receipt is None
    assert tuple(created) == tuple(
        value
        for value in (worker._transport_original_incoming, worker._transport_original_outgoing)
        if value is not None
    )


def test_manually_installed_transport_does_not_receive_a_default_ack(tmp_path):
    worker = lazy_worker(tmp_path)
    original = object()
    worker._incoming = original
    record = worker.transport_retirement_snapshot()
    assert not record.identity_intact and not record.physical_confirmed
    worker._incoming = None
    worker.close()
    restored = worker.transport_retirement_snapshot()
    assert not restored.identity_intact and not restored.physical_confirmed


def test_actual_start_close_exports_original_receipts_without_model_authority(
    tmp_path, monkeypatch
):
    with transport_fixture._default_bootstrap(tmp_path, monkeypatch) as (worker, activation):
        worker._start()
        original = worker._process, worker._resources, worker._receipt_channel
        assert activation.entered.is_set() and worker._ownership_transfer_confirmed
        worker.close()
        record = worker.transport_retirement_snapshot()
        assert record.physical_confirmed and not record.never_started_confirmed
        assert record.host is worker._host_retirement
        assert record.host is original[1]._last
        assert record.host.resource_token == worker._transport_capsule_token
        assert record.receipt.join_confirmed and not record.receipt.reader_alive
        assert record.channel_binding_token and record.host.process_handle_closed
        assert tuple(q.name for q in record.host.queues) == ("incoming", "outgoing")
        assert all(q.reader_close_ack and q.writer_close_ack for q in record.host.queues)
        assert record.normal_model_cleanup == "unconfirmed"
        assert not record.all_kernel_resources_confirmed and not record.actor_recovery_authorized
        assert original == (worker._process, worker._resources, worker._receipt_channel)


def test_actual_physical_retirement_does_not_clear_prior_model_cleanup_unknown(
    tmp_path, monkeypatch
):
    with transport_fixture._default_bootstrap(tmp_path, monkeypatch) as (worker, _):
        worker._start()
        worker._cleanup_unconfirmed = True
        with pytest.raises(StorageError, match="model cleanup"):
            worker.close()
        record = worker.transport_retirement_snapshot()
        assert record.physical_confirmed and record.normal_model_cleanup == "unconfirmed"
        assert worker._cleanup_unconfirmed


@pytest.mark.parametrize(
    "part", ["getter", "captured_getter", "reader", "capsule_token", "worker_close"]
)
def test_original_binding_substitution_is_sticky_and_does_not_call_replacement(
    tmp_path, monkeypatch, part
):
    with transport_fixture._default_bootstrap(tmp_path, monkeypatch) as (worker, _):
        worker._start()
        worker.close()
        assert worker.transport_retirement_snapshot().physical_confirmed
        calls = []
        with monkeypatch.context() as patch:
            if part == "getter":
                patch.setattr(worker._receipt_channel, "retirement_status", lambda: calls.append(1))
            elif part == "captured_getter":
                patch.setattr(worker, "_transport_original_getter", lambda: calls.append(1))
            elif part == "reader":
                patch.setattr(worker._receipt_channel, "_reader", object())
            elif part == "capsule_token":
                patch.setattr(worker._resources, "resource_token", "substituted")
            else:
                patch.setattr(worker, "close", lambda: calls.append(1))
            record = worker.transport_retirement_snapshot()
            assert not record.identity_intact and not record.physical_confirmed
            assert calls == []
        restored = worker.transport_retirement_snapshot()
        assert not restored.identity_intact and not restored.physical_confirmed and calls == []


def test_capture_baseexception_does_not_change_startup_or_grant_later_ack(tmp_path, monkeypatch):
    with transport_fixture._default_bootstrap(tmp_path, monkeypatch) as (worker, _):
        original_getter = worker_module.inspect.getattr_static

        def capture_fault(value, name):
            if value is worker._receipt_channel and name == "retirement_status":
                raise KeyboardInterrupt("deliberate evidence-only fault")
            return original_getter(value, name)

        with monkeypatch.context() as patch:
            patch.setattr(worker_module, "inspect", SimpleNamespace(getattr_static=capture_fault))
            worker._start()
        assert worker._ownership_transfer_confirmed and worker._transport_capture_failed
        worker.close()
        record = worker.transport_retirement_snapshot()
        assert not record.physical_confirmed
        assert "transport_evidence_capture_failed" in record.errors
        assert worker._transport_capture_failed


def test_exact_frozen_record_with_mutable_or_oversized_leaves_is_rejected(tmp_path):
    worker = lazy_worker(tmp_path)
    worker.close()
    record = worker.transport_retirement_snapshot()
    for changes in (
        {"errors": []},
        {"identity_intact": 1},
        {"worker_token": "x" * 129},
        {
            "receipt": ReceiptRetirementStatus(
                True,
                True,
                False,
                True,
                True,
                ReceiptFailure("transport", "OSError", "secret/path/" * 40),
            )
        },
        {
            "host": HostRetirement(
                "a" * 32, 1, "unknown", None, None, None, False, [], (), False, (), False
            )
        },
    ):
        with pytest.raises(ValueError):
            replace(record, physical_confirmed=False, never_started_confirmed=False, **changes)
    assert validate_worker_transport_retirement(record) is record
