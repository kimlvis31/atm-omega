"""Deterministic status-read and lifecycle races, without framework/child execution."""

import json
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from omega.infrastructure import atomic_files
from omega.research import artifacts
from omega.research import service as module
from omega.research.service import ResearchService

_EXPECTED_METADATA_UNKNOWN = pytest.StashKey[bool]()


@pytest.fixture
def idle_service(tmp_path, monkeypatch, request):
    actual_reader, actual_is_file, actual_sleep = reader(), Path.is_file, module.time.sleep

    def idle(self):
        with self._condition:
            self._condition.wait_for(lambda: self._closed)

    monkeypatch.setattr(ResearchService, "_schedule", idle)
    service = ResearchService(tmp_path / "research")
    job = service.submit("nn_inspect", {"demo": True})
    yield service, job
    monkeypatch.setattr(module, "read_status_json", actual_reader, raising=False)
    monkeypatch.setattr(Path, "is_file", actual_is_file)
    monkeypatch.setattr(module.time, "sleep", actual_sleep)
    if request.node.stash.get(_EXPECTED_METADATA_UNKNOWN, False):
        with pytest.raises(RuntimeError, match="shutdown metadata unavailable"):
            service.close()
        assert not service._thread.is_alive()
        assert service._running_process is None and service._unconfirmed_process is None
    else:
        service.close()


def reader():
    return getattr(module, "read_status_json", module.read_json)


def test_status_uses_opened_regular_file_even_when_is_file_predicate_is_false(
    idle_service, monkeypatch
):
    service, job = idle_service
    path, original = service._job_path(job["id"]), Path.is_file
    before = path.read_bytes()
    monkeypatch.setattr(
        Path, "is_file", lambda target: False if target == path else original(target)
    )
    assert service.status(job["id"])["state"] == "queued"
    assert path.read_bytes() == before


def test_registered_missing_status_is_bounded_and_recovers_only_from_actual_json(
    idle_service, monkeypatch
):
    service, job = idle_service
    actual, calls, delays = reader(), [], []

    def missing_twice(path):
        assert not service._condition._is_owned()
        calls.append(path)
        if len(calls) <= 2:
            raise FileNotFoundError("injected visibility miss")
        return actual(path)

    def sleep(delay):
        assert not service._condition._is_owned()
        delays.append(delay)

    monkeypatch.setattr(module, "read_status_json", missing_twice, raising=False)
    monkeypatch.setattr(module.time, "sleep", sleep)
    before = service._job_path(job["id"]).read_bytes()
    assert service.status(job["id"])["state"] == "queued"
    assert len(calls) == 3 and len(delays) == 2 and sum(delays) <= 0.05
    assert service._job_path(job["id"]).read_bytes() == before


def test_permanent_known_missing_is_unavailable_not_unknown_or_guessed_complete(
    idle_service, monkeypatch
):
    service, job = idle_service
    calls = []

    def missing(path):
        calls.append(path)
        raise FileNotFoundError

    monkeypatch.setattr(module, "read_status_json", missing, raising=False)
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        service.status(job["id"])
    assert len(calls) == 3


def test_unknown_id_remains_key_error_without_retries(idle_service, monkeypatch):
    service, _ = idle_service
    actual, calls = reader(), []

    def observed(path):
        calls.append(path)
        return actual(path)

    monkeypatch.setattr(module, "read_status_json", observed, raising=False)
    with pytest.raises(KeyError):
        service.status("a" * 32)
    assert len(calls) == 1


@pytest.mark.parametrize("failure", [PermissionError("read denied"), ValueError("corrupt JSON")])
def test_permission_or_corruption_is_never_retried(idle_service, monkeypatch, failure):
    service, job = idle_service
    calls = []

    def failed(path):
        calls.append(path)
        raise failure

    monkeypatch.setattr(module, "read_status_json", failed, raising=False)
    with pytest.raises(type(failure)):
        service.status(job["id"])
    assert len(calls) == 1


def test_close_during_missing_read_aborts_without_lock_sleep_or_metadata_rewrite(
    idle_service, monkeypatch
):
    service, job = idle_service
    actual, calls = reader(), []
    before = service._job_path(job["id"]).read_bytes()

    def closing(path):
        calls.append(path)
        # This job is considered active, so close has no queued status to read.
        with service._condition:
            service._pending.clear()
            service._requests.clear()
            service._active = job["id"], threading.Event()
        service.close()
        raise FileNotFoundError

    monkeypatch.setattr(module, "read_status_json", closing, raising=False)
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        service.status(job["id"])
    assert len(calls) == 1 and service._closed
    assert service._job_path(job["id"]).read_bytes() == before
    monkeypatch.setattr(module, "read_status_json", actual)


def test_real_status_helper_rejects_invalid_json_directory_and_overlong_body(tmp_path):
    path = tmp_path / "job.json"
    path.write_text("{broken", encoding="utf-8")
    status_reader = getattr(artifacts, "read_status_json", artifacts.read_json)
    with pytest.raises(ValueError):
        status_reader(path)
    with pytest.raises((ValueError, PermissionError, IsADirectoryError)):
        status_reader(tmp_path)
    path.write_bytes(b" " * 2097153)
    with pytest.raises(ValueError, match="capacity|limit"):
        status_reader(path)


def test_real_status_helper_rejects_symlink_without_reading_target(tmp_path):
    target, link = tmp_path / "target.json", tmp_path / "link.json"
    target.write_text(json.dumps({"state": "complete"}), encoding="utf-8")
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("local file-symlink privilege is unavailable")
    status_reader = getattr(artifacts, "read_status_json", artifacts.read_json)
    with pytest.raises((OSError, ValueError)):
        status_reader(link)


def test_cancel_does_not_overwrite_completion_published_during_its_read(idle_service, monkeypatch):
    service, job = idle_service
    actual = reader()
    path = service._job_path(job["id"])
    complete = dict(job, state="complete", result={"actual": "committed"})
    calls = []

    def completing(target):
        assert not service._condition._is_owned()
        old = actual(target)
        calls.append(target)
        if len(calls) == 1:
            with service._condition:
                service._pending.remove(job["id"])
                service._requests.pop(job["id"])
            artifacts.write_json(path, complete)
        return old

    monkeypatch.setattr(module, "read_status_json", completing)
    assert service.cancel(job["id"]) == complete
    assert artifacts.read_json(path) == complete


def test_close_cleans_up_and_joins_even_if_one_queued_metadata_is_unreadable(
    idle_service, monkeypatch, request
):
    service, job = idle_service
    other = service.submit("nn_inspect", {"demo": True})
    actual = reader()
    path = service._job_path(job["id"])
    before = path.read_bytes()

    def unreadable(target):
        assert not service._condition._is_owned()
        if target == path:
            raise PermissionError("injected read failure")
        return actual(target)

    monkeypatch.setattr(module, "read_status_json", unreadable)
    request.node.stash[_EXPECTED_METADATA_UNKNOWN] = True
    with pytest.raises(RuntimeError, match="shutdown metadata unavailable"):
        service.close()
    assert service._closed and not service._thread.is_alive()
    assert not service._pending and not service._requests
    assert path.read_bytes() == before
    assert artifacts.read_json(service._job_path(other["id"]))["state"] == "cancelled"
    with pytest.raises(RuntimeError, match="closed"):
        service.submit("nn_inspect", {"demo": True})


@pytest.mark.parametrize("transition", ["active", "degraded"])
def test_cancel_rechecks_owner_after_read_without_overwriting_running_or_degraded_metadata(
    idle_service, monkeypatch, transition, request
):
    service, job = idle_service
    actual, calls = reader(), []
    path, cancel = service._job_path(job["id"]), threading.Event()
    running = dict(job, state="running")

    def changed_owner(target):
        assert not service._condition._is_owned()
        old = actual(target)
        calls.append(target)
        if len(calls) == 1:
            with service._condition:
                if transition == "active":
                    service._pending.remove(job["id"])
                    service._requests.pop(job["id"])
                    service._active = job["id"], cancel
                else:
                    service._degraded = "injected persistence failure"
            artifacts.write_json(path, running)
        return old

    monkeypatch.setattr(module, "read_status_json", changed_owner)
    if transition == "degraded":
        request.node.stash[_EXPECTED_METADATA_UNKNOWN] = True
    service.cancel(job["id"])
    assert artifacts.read_json(path) == running
    assert cancel.is_set() is (transition == "active")


@pytest.mark.parametrize("retirement", ["cancel", "close"])
def test_dispatch_reads_without_condition_and_observes_retirement_before_process_creation(
    tmp_path, monkeypatch, retirement
):
    service = ResearchService(tmp_path / "research")
    actual, read_done, spawned = reader(), threading.Event(), []

    def retire_after_read(path):
        assert not service._condition._is_owned()
        status = actual(path)
        if threading.current_thread() is service._thread and not read_done.is_set():
            with service._condition:
                if retirement == "close":
                    service._closed = True
                else:
                    service._active[1].set()
            read_done.set()
        return status

    def process(**parameters):
        spawned.append(parameters)
        raise AssertionError("retired job must not construct a process")

    monkeypatch.setattr(module, "read_status_json", retire_after_read)
    try:
        with service._condition:
            service._context = SimpleNamespace(Event=threading.Event, Process=process)
            job = service.submit("nn_inspect", {"demo": True})
        assert read_done.wait(2)
        service.close()
        assert spawned == [] and not service._thread.is_alive()
        assert actual(service._job_path(job["id"]))["state"] == "cancelled"
    finally:
        service.close()


def test_short_status_read_closes_handle_and_existing_atomic_retry_completes(tmp_path, monkeypatch):
    path = tmp_path / "job.json"
    artifacts.write_json(path, {"state": "queued"})
    descriptor = artifacts._status_descriptor(path)
    assert json.loads(os.read(descriptor, 4096)) == {"state": "queued"}
    first_attempt, closed = threading.Event(), threading.Event()
    replace = atomic_files._replace

    def observed_replace(source, target):
        try:
            return replace(source, target)
        finally:
            first_attempt.set()

    def finish_read():
        first_attempt.wait(2)
        time.sleep(0.01)
        os.close(descriptor)
        closed.set()

    monkeypatch.setattr(atomic_files, "_replace", observed_replace)
    reader_thread = threading.Thread(target=finish_read)
    reader_thread.start()
    try:
        artifacts.write_json(path, {"state": "complete"})
        assert artifacts.read_status_json(path) == {"state": "complete"}
        assert not list(tmp_path.glob(".*.tmp"))
    finally:
        first_attempt.set()
        reader_thread.join(2)
    assert closed.is_set() and not reader_thread.is_alive()


def test_failed_atomic_write_does_not_remove_status_or_leave_temporary_file(tmp_path, monkeypatch):
    path = tmp_path / "job.json"
    artifacts.write_json(path, {"state": "queued"})
    before = path.read_bytes()

    def failed_replace(source, target):
        raise OSError("injected permanent replacement failure")

    monkeypatch.setattr(atomic_files, "_replace", failed_replace)
    with pytest.raises(OSError, match="injected permanent"):
        artifacts.write_json(path, {"state": "complete"})
    assert path.read_bytes() == before
    assert not list(tmp_path.glob(".*.tmp"))


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="requires POSIX FIFO")
def test_status_reader_rejects_fifo_without_waiting_for_writer(tmp_path):
    path = tmp_path / "job.json"
    os.mkfifo(path)
    with pytest.raises(ValueError, match="regular"):
        artifacts.read_status_json(path)


def test_status_reader_closes_descriptor_when_fstat_fails(tmp_path, monkeypatch):
    path = tmp_path / "job.json"
    artifacts.write_json(path, {"state": "queued"})
    descriptor = artifacts._status_descriptor(path)
    monkeypatch.setattr(artifacts, "_status_descriptor", lambda path: descriptor)

    def unavailable(fd):
        raise PermissionError("injected fstat denial")

    with monkeypatch.context() as scoped:
        scoped.setattr(artifacts.os, "fstat", unavailable)
        with pytest.raises(PermissionError, match="fstat denial"):
            artifacts.read_status_json(path)
    with pytest.raises(OSError):
        os.fstat(descriptor)


def test_quarantine_overlay_uses_same_bounded_reader_and_preserves_unknown_gate(
    idle_service, monkeypatch
):
    service, job = idle_service
    path, calls = service._job_path(job["id"]), []
    before = path.read_bytes()
    with service._condition:
        service._quarantine = {"job_id": job["id"]}
        service._degraded = "worker exit unconfirmed"

    def unsafe(path):
        assert not service._condition._is_owned()
        calls.append(path)
        raise ValueError("nonregular metadata")

    monkeypatch.setattr(module, "read_status_json", unsafe)
    result = service.status(job["id"])
    assert result["state"] == result["computation_state"] == "unknown"
    assert not {"result", "result_file", "artifact"} & result.keys()
    assert calls == [path] and path.read_bytes() == before
    with pytest.raises(RuntimeError, match="unavailable"):
        service.submit("nn_inspect", {"demo": True})


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="requires POSIX FIFO")
def test_quarantined_fifo_is_unknown_without_waiting_or_altering_the_path(idle_service):
    service, job = idle_service
    path = service._job_path(job["id"])
    path.unlink()
    os.mkfifo(path)
    with service._condition:
        service._quarantine = {"job_id": job["id"]}
    result = service.status(job["id"])
    assert result["state"] == "unknown"
    import stat

    assert stat.S_ISFIFO(path.lstat().st_mode)


def test_missing_registered_metadata_summary_does_not_hide_other_actual_jobs(
    idle_service, monkeypatch
):
    service, job = idle_service
    other = service.submit("nn_inspect", {"demo": True})
    actual = reader()
    path = service._job_path(job["id"])

    def missing(target):
        if target == path:
            raise FileNotFoundError("injected status visibility miss")
        return actual(target)

    monkeypatch.setattr(module, "read_status_json", missing)
    summaries = {row["id"]: row for row in service.list_job_summaries()}
    assert summaries[job["id"]] == {
        "id": job["id"],
        "state": "unknown",
        "computation_state": "unknown",
        "persistence": "unavailable",
        "error": "job metadata unavailable",
    }
    assert summaries[other["id"]]["state"] == "queued"
    assert summaries[other["id"]]["created_at_ms"] == other["created_at_ms"]
    with pytest.raises(module.ResearchMetadataUnavailable):
        service.status(job["id"])


@pytest.mark.parametrize(
    "failure", [RuntimeError("injected unrelated service defect"), KeyError("unrelated_field")]
)
def test_unrelated_error_still_fails_summary_query(idle_service, monkeypatch, failure):
    service, _ = idle_service

    def broken(target):
        raise failure

    monkeypatch.setattr(module, "read_status_json", broken)
    with pytest.raises(type(failure), match="unrelated"):
        service.list_job_summaries()


def test_actual_missing_registered_file_is_shown_before_normal_jobs_with_limit(idle_service):
    service, job = idle_service
    other = service.submit("nn_inspect", {"demo": True})
    service._job_path(job["id"]).unlink()
    # Restore this fixture's queued file only after the read-only assertions,
    # so fixture shutdown can cancel the deliberately damaged test queue.
    try:
        summaries = service.list_job_summaries()
        assert [row["id"] for row in summaries] == [job["id"], other["id"]]
        assert summaries[0]["state"] == summaries[0]["computation_state"] == "unknown"
        assert summaries[0]["persistence"] == "unavailable"
        assert "created_at_ms" not in summaries[0]
        assert summaries[1]["created_at_ms"] == other["created_at_ms"]
        assert service.list_job_summaries(limit=1) == summaries[:1]
        assert not service._job_path(job["id"]).exists()
    finally:
        artifacts.write_json(service._job_path(job["id"]), job)


def test_registration_retired_during_listing_remains_unknown_only_for_snapshot_id(
    idle_service, monkeypatch
):
    service, job = idle_service
    actual = service.status
    path = service._job_path(job["id"])
    path.unlink()
    calls = []

    def retired(identity):
        calls.append(identity)
        with service._condition:
            service._pending.clear()
            service._requests.clear()
        return actual(identity)

    monkeypatch.setattr(service, "status", retired)
    result = service.list_job_summaries()
    assert result == [
        {
            "id": job["id"],
            "state": "unknown",
            "computation_state": "unknown",
            "persistence": "unavailable",
            "error": "job metadata unavailable",
        }
    ]
    assert calls == [job["id"]]
    monkeypatch.setattr(service, "status", actual)
    with pytest.raises(KeyError):
        service.status(job["id"])


@pytest.mark.parametrize("failure", ["permission", "invalid_json"])
def test_unreadable_summary_is_unknown_while_persisted_failed_stays_failed(
    idle_service, monkeypatch, failure
):
    service, job = idle_service
    failed = service.submit("nn_inspect", {"demo": True})
    failed.update(state="failed", error="actual recorded worker failure")
    with service._condition:
        service._pending.remove(failed["id"])
        service._requests.pop(failed["id"])
    artifacts.write_json(service._job_path(failed["id"]), failed)
    path, actual, reads = service._job_path(job["id"]), reader(), []
    if failure == "invalid_json":
        path.write_bytes(b"{incomplete-json")
    before = path.read_bytes()

    def observed(target):
        reads.append(target)
        if target == path and failure == "permission":
            raise PermissionError("injected metadata permission fault")
        return actual(target)

    monkeypatch.setattr(module, "read_status_json", observed)
    try:
        summaries = {row["id"]: row for row in service.list_job_summaries()}
        assert summaries[job["id"]] == {
            "id": job["id"],
            "state": "unknown",
            "computation_state": "unknown",
            "persistence": "unavailable",
            "error": "unreadable job metadata",
        }
        assert summaries[failed["id"]]["state"] == "failed"
        assert summaries[failed["id"]]["error"] == failed["error"]
        assert reads.count(path) == 1
        assert path.read_bytes() == before
    finally:
        artifacts.write_json(path, job)
