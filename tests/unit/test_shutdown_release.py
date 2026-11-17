"""Real OP/index CAS corrections; data-only participants and no OS lease release."""

import json
import threading
import time
from copy import deepcopy
from hashlib import sha256
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.runtime import shutdown
from omega.runtime.shutdown import ShutdownService, encoded


class Participant:
    def __init__(self):
        self.reads = 0

    def shutdown_snapshot(self):
        self.reads += 1
        return {
            "owner": "data_only",
            "ready": True,
            "durable": True,
            "supports_flatten": True,
            "positions": [],
            "pending_orders": [],
            "unknown": [],
            "notes": [],
        }


@pytest.fixture
def exiting(tmp_path):
    participant, exited = Participant(), threading.Event()
    service = ShutdownService(
        tmp_path,
        [participant],
        core_epoch="a" * 32,
        pause_entries=lambda identity: None,
        on_exit=exited.set,
        clock_ms=lambda: 120000,
    )
    service.start()
    deadline = time.monotonic() + 3
    while not service.ready and time.monotonic() < deadline:
        time.sleep(0.005)
    assert service.ready
    service.submit(
        "original_exit",
        service.core_epoch,
        service.index["revision"],
        "save_exit",
        {"acknowledge_unmanaged_positions": False},
    )
    assert exited.wait(3)
    service.close()
    yield service, participant
    service.close()


def files(service):
    return service._path("original_exit"), service.root / "index.json"


def documents(service):
    return tuple(service._read(path) for path in files(service))


def legacy_bytes(document):
    envelope = {"data": document, "checksum": sha256(encoded(document)).hexdigest()}
    return json.dumps(envelope, ensure_ascii=False, allow_nan=False, sort_keys=True).encode()


def test_finish_success_keeps_original_terminal_bytes_and_returns_data_only_guard(exiting):
    service, _ = exiting
    row, index = documents(service)
    row.update(state="completed", result={"core_exit_confirmed": True})
    index.update(active=None, last_exit="original_exit", state="closed")
    index["revision"] += 1
    guard = service.finish_exit(successful=True)
    assert tuple(path.read_bytes() for path in files(service)) == (
        legacy_bytes(row),
        legacy_bytes(index),
    )
    assert guard == {
        "operation_id": "original_exit",
        "core_epoch": service.core_epoch,
        "record_sha256": sha256(encoded(row)).hexdigest(),
        "index_sha256": sha256(encoded(index)).hexdigest(),
    }
    assert service.finish_exit(successful=True) == guard


def test_unsuccessful_finish_and_no_active_return_none_without_correction(exiting):
    service, _ = exiting
    assert service.finish_exit(successful=False) is None
    before = tuple(path.read_bytes() for path in files(service))
    assert service.finish_exit(successful=True) is None
    assert tuple(path.read_bytes() for path in files(service)) == before
    with pytest.raises(ValidationError):
        service.fail_root_release({}, root_lease_held=lambda: True)


def test_correction_is_exact_failed_pair_idempotent_and_loadable(exiting):
    service, participant = exiting
    guard = service.finish_exit(successful=True)
    original = documents(service)
    reads = participant.reads
    result = service.fail_root_release(guard, root_lease_held=lambda: True)
    assert result["state"] == "failed"
    assert result["result"] == {"core_exit_confirmed": False}
    assert result["error"] == "root_lease_release_unconfirmed"
    row, index = documents(service)
    assert index == {**original[1], "state": "failed", "revision": original[1]["revision"] + 1}
    assert service.status("original_exit") == row
    assert service.snapshot()["closed"] is True
    committed = tuple(path.read_bytes() for path in files(service))
    assert service.fail_root_release(guard, root_lease_held=lambda: True) == row
    assert tuple(path.read_bytes() for path in files(service)) == committed
    with pytest.raises(StorageError, match="documents changed"):
        service.finish_exit(successful=True)
    assert tuple(path.read_bytes() for path in files(service)) == committed
    assert participant.reads == reads
    assert len(service.cache) == 1 and service.queue == shutdown.deque()
    # A fresh generation reads the failed terminal receipt, never promotes it.
    new = ShutdownService(
        service.root.parent.parent,
        [],
        core_epoch="b" * 32,
        pause_entries=lambda identity: None,
    )
    new._bootstrap()
    new.close()
    assert new.status("original_exit")["state"] == "failed"
    with pytest.raises(ValidationError):
        new.fail_root_release(guard, root_lease_held=lambda: True)


@pytest.mark.parametrize("field", ["operation_id", "core_epoch", "record_sha256", "index_sha256"])
def test_changed_guard_is_rejected_without_writes(exiting, field):
    service, _ = exiting
    guard = service.finish_exit(successful=True)
    before = tuple(path.read_bytes() for path in files(service))
    changed = {**guard, field: "changed"}
    with pytest.raises(ValidationError):
        service.fail_root_release(changed, root_lease_held=lambda: True)
    assert tuple(path.read_bytes() for path in files(service)) == before


@pytest.mark.parametrize("lease", [False, 1, "held", None, "raise"])
def test_unconfirmed_lease_never_writes(exiting, lease):
    service, _ = exiting
    guard = service.finish_exit(successful=True)
    before = tuple(path.read_bytes() for path in files(service))

    def held():
        if lease == "raise":
            raise OSError("unknown native state")
        return lease

    with pytest.raises(StorageError):
        service.fail_root_release(guard, root_lease_held=held)
    assert tuple(path.read_bytes() for path in files(service)) == before


@pytest.mark.parametrize("target", [0, 1])
def test_other_valid_record_or_index_change_is_not_overwritten(exiting, target):
    service, _ = exiting
    guard = service.finish_exit(successful=True)
    changed = deepcopy(documents(service)[target])
    changed["error" if target == 0 else "revision"] = "other" if target == 0 else 999
    service._write(files(service)[target], changed)
    before = tuple(path.read_bytes() for path in files(service))
    with pytest.raises(StorageError, match="documents changed"):
        service.fail_root_release(guard, root_lease_held=lambda: True)
    assert tuple(path.read_bytes() for path in files(service)) == before


@pytest.mark.parametrize("target,after", [(0, False), (0, True), (1, False), (1, True)])
def test_partial_write_and_lost_ack_retry_only_same_original_failed_pair(
    exiting, monkeypatch, target, after
):
    service, participant = exiting
    guard = service.finish_exit(successful=True)
    original = documents(service)
    reads, failed, writes = participant.reads, False, []
    atomic = shutdown.atomic_json

    def fault(path, value):
        nonlocal failed
        writes.append(path)
        if path == files(service)[target] and not failed:
            failed = True
            if after:
                atomic(path, value)
            raise OSError("simulated commit acknowledgement unknown")
        atomic(path, value)

    monkeypatch.setattr(shutdown, "atomic_json", fault)
    with pytest.raises(StorageError, match="not confirmed"):
        service.fail_root_release(guard, root_lease_held=lambda: True)
    partial = documents(service)
    assert partial[0]["state"] == ("completed" if target == 0 and not after else "failed")
    assert partial[1]["revision"] == original[1]["revision"] + int(target == 1 and after)
    before_retry = len(writes)
    row = service.fail_root_release(guard, root_lease_held=lambda: True)
    assert row["result"] == {"core_exit_confirmed": False}
    assert documents(service)[1]["state"] == "failed"
    assert participant.reads == reads
    assert len(writes) - before_retry == (
        2 if target == 0 and not after else 0 if after and target else 1
    )


def test_lease_loss_between_independent_commits_blocks_index_until_confirmed(exiting, monkeypatch):
    service, _ = exiting
    guard = service.finish_exit(successful=True)
    original_index = documents(service)[1]
    held = [True]
    write = service._write

    def release_after_row(path, value):
        write(path, value)
        if path == files(service)[0]:
            held[0] = False

    monkeypatch.setattr(service, "_write", release_after_row)
    with pytest.raises(StorageError, match="no longer confirmed"):
        service.fail_root_release(guard, root_lease_held=lambda: held[0])
    assert documents(service)[0]["state"] == "failed"
    assert documents(service)[1] == original_index
    held[0] = True
    service.fail_root_release(guard, root_lease_held=lambda: held[0])
    assert documents(service)[1]["state"] == "failed"


def test_same_epoch_new_manager_and_released_old_generation_cannot_correct(exiting):
    service, _ = exiting
    guard = service.finish_exit(successful=True)
    other = ShutdownService(
        service.root.parent.parent,
        [],
        core_epoch=service.core_epoch,
        pause_entries=lambda identity: None,
    )
    other.close()
    before = tuple(path.read_bytes() for path in files(service))
    with pytest.raises(ValidationError):
        other.fail_root_release(guard, root_lease_held=lambda: True)
    service.core_epoch = "c" * 32
    with pytest.raises(ValidationError):
        service.fail_root_release(guard, root_lease_held=lambda: True)
    assert tuple(path.read_bytes() for path in files(service)) == before


def test_correction_requires_closed_and_joined_manager(exiting):
    service, _ = exiting
    guard = service.finish_exit(successful=True)
    service.closed = False
    try:
        with pytest.raises(StorageError, match="joined closed"):
            service.fail_root_release(guard, root_lease_held=lambda: True)
    finally:
        service.closed = True
    thread = service.thread
    service.thread = SimpleNamespace(is_alive=lambda: True)
    try:
        with pytest.raises(StorageError, match="joined closed"):
            service.fail_root_release(guard, root_lease_held=lambda: True)
    finally:
        service.thread = thread


@pytest.mark.parametrize("target", [0, 1])
def test_changed_partial_correction_is_not_repaired_over_another_change(
    exiting, monkeypatch, target
):
    service, _ = exiting
    guard = service.finish_exit(successful=True)
    atomic = shutdown.atomic_json
    failed = False

    def row_ack_lost(path, value):
        nonlocal failed
        atomic(path, value)
        if path == files(service)[0] and not failed:
            failed = True
            raise OSError("lost row acknowledgement")

    monkeypatch.setattr(shutdown, "atomic_json", row_ack_lost)
    with pytest.raises(StorageError):
        service.fail_root_release(guard, root_lease_held=lambda: True)
    changed = deepcopy(documents(service)[target])
    changed["error" if target == 0 else "revision"] = "other" if target == 0 else 888
    service._write(files(service)[target], changed)
    before = tuple(path.read_bytes() for path in files(service))
    with pytest.raises(StorageError, match="documents changed"):
        service.fail_root_release(guard, root_lease_held=lambda: True)
    assert tuple(path.read_bytes() for path in files(service)) == before


def test_index_first_pair_is_never_an_authorized_partial_state(exiting):
    service, _ = exiting
    guard = service.finish_exit(successful=True)
    row, index = documents(service)
    index.update(state="failed", revision=index["revision"] + 1)
    service._write(files(service)[1], index)
    with pytest.raises(StorageError, match="documents changed"):
        service.fail_root_release(guard, root_lease_held=lambda: True)
    assert documents(service) == (row, index)


@pytest.mark.parametrize("target,after", [(0, False), (0, True), (1, False), (1, True)])
def test_completion_ack_loss_recovers_same_guard_then_native_release_failure_can_correct(
    exiting, monkeypatch, target, after
):
    service, participant = exiting
    before = documents(service)
    target_row, target_index = deepcopy(before)
    target_row.update(state="completed", result={"core_exit_confirmed": True})
    target_index.update(active=None, last_exit="original_exit", state="closed")
    target_index["revision"] += 1
    reads, failed, writes = participant.reads, False, []
    atomic = shutdown.atomic_json

    def fault(path, value):
        nonlocal failed
        writes.append(path)
        if path == files(service)[target] and not failed:
            failed = True
            if after:
                atomic(path, value)
            raise OSError("completion acknowledgement lost")
        atomic(path, value)

    monkeypatch.setattr(shutdown, "atomic_json", fault)
    with pytest.raises(StorageError, match="not confirmed"):
        service.finish_exit(successful=True)
    intent = service._exit_completion
    assert intent is not None and service._root_release_correction.guard is None
    guard = service.finish_exit(successful=True)
    assert service._exit_completion is intent
    assert documents(service) == (target_row, target_index)
    assert tuple(path.read_bytes() for path in files(service)) == (
        legacy_bytes(target_row),
        legacy_bytes(target_index),
    )
    assert guard["record_sha256"] == sha256(encoded(target_row)).hexdigest()
    count = len(writes)
    assert service.finish_exit(successful=True) == guard
    assert len(writes) == count
    # Same original Core's native release failed before releasing its lease.
    corrected = service.fail_root_release(guard, root_lease_held=lambda: True)
    assert corrected["state"] == "failed"
    assert corrected["result"] == {"core_exit_confirmed": False}
    assert documents(service)[1]["revision"] == target_index["revision"] + 1
    assert participant.reads == reads


@pytest.mark.parametrize("target,after", [(0, False), (0, True), (1, False), (1, True)])
def test_failed_completion_intent_never_promotes_on_later_success(
    exiting, monkeypatch, target, after
):
    service, _ = exiting
    failed, atomic = False, shutdown.atomic_json

    def fault(path, value):
        nonlocal failed
        if path == files(service)[target] and not failed:
            failed = True
            if after:
                atomic(path, value)
            raise OSError("failed completion acknowledgement lost")
        atomic(path, value)

    monkeypatch.setattr(shutdown, "atomic_json", fault)
    with pytest.raises(StorageError):
        service.finish_exit(successful=False)
    assert service.finish_exit(successful=True) is None
    assert documents(service)[0]["state"] == "failed"
    assert documents(service)[0]["result"] == {"core_exit_confirmed": False}
    assert service._root_release_correction.guard is None


def test_successful_partial_intent_with_new_cleanup_failure_is_explicitly_unconfirmed(
    exiting, monkeypatch
):
    service, _ = exiting
    atomic = shutdown.atomic_json

    def fault(path, value):
        if path == files(service)[1]:
            raise OSError("index not acknowledged")
        atomic(path, value)

    monkeypatch.setattr(shutdown, "atomic_json", fault)
    with pytest.raises(StorageError):
        service.finish_exit(successful=True)
    before = tuple(path.read_bytes() for path in files(service))
    with pytest.raises(StorageError, match="outcome changed; unconfirmed"):
        service.finish_exit(successful=False)
    assert tuple(path.read_bytes() for path in files(service)) == before
    assert service._root_release_correction.guard is None


@pytest.mark.parametrize("read_call", [1, 2, 3, 4, 5, 6])
def test_completion_read_failure_never_issues_guard_and_retry_confirms_exact_pair(
    exiting, monkeypatch, read_call
):
    service, _ = exiting
    count, read = 0, service._read

    def uncertain(path):
        nonlocal count
        count += 1
        if count == read_call:
            raise StorageError("completion read unconfirmed")
        return read(path)

    monkeypatch.setattr(service, "_read", uncertain)
    with pytest.raises(StorageError, match="read unconfirmed"):
        service.finish_exit(successful=True)
    assert service._root_release_correction.guard is None
    guard = service.finish_exit(successful=True)
    assert guard["record_sha256"] == sha256(encoded(documents(service)[0])).hexdigest()


@pytest.mark.parametrize("change", ["row", "index", "epoch"])
def test_completion_retry_refuses_foreign_partial_changes(exiting, monkeypatch, change):
    service, _ = exiting
    atomic, failed = shutdown.atomic_json, False

    def lost_index_ack(path, value):
        nonlocal failed
        atomic(path, value)
        if path == files(service)[1] and not failed:
            failed = True
            raise OSError("index acknowledgement lost")

    monkeypatch.setattr(shutdown, "atomic_json", lost_index_ack)
    with pytest.raises(StorageError):
        service.finish_exit(successful=True)
    if change == "epoch":
        service.core_epoch = "d" * 32
    else:
        target = 0 if change == "row" else 1
        value = deepcopy(documents(service)[target])
        value["error" if target == 0 else "revision"] = "foreign" if target == 0 else 777
        service._write(files(service)[target], value)
    before = tuple(path.read_bytes() for path in files(service))
    with pytest.raises((StorageError, ValidationError)):
        service.finish_exit(successful=True)
    assert tuple(path.read_bytes() for path in files(service)) == before
    assert service._root_release_correction.guard is None
