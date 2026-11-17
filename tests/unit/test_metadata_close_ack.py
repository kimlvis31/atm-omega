"""Actual metadata ACK faults, fixed cancellation intents and dispatch budgets."""

import json

import pytest
from test_auxiliary_schedule import configure, setup

from omega.domain.errors import StorageError
from omega.research import service as research_module
from omega.research.artifacts import read_status_json
from omega.research.service import ResearchService
from omega.runtime import auxiliary_schedule as schedule_module


@pytest.fixture
def research(tmp_path, monkeypatch):
    # Real supervisor/thread/submit/files; pause heavy dispatch to keep original
    # queued jobs deterministic. No worker/model handler is executed in this fixture.
    def wait_for_close(self):
        with self._condition:
            self._condition.wait_for(lambda: self._closed)

    monkeypatch.setattr(ResearchService, "_schedule", wait_for_close)
    owner = ResearchService(tmp_path / "research")
    yield owner
    if owner._thread.is_alive():
        try:
            owner.close()
        except RuntimeError:
            pass
    assert not owner._thread.is_alive()


def fault_write(owner, monkeypatch, *, after=False, job_id=None):
    original, calls = research_module.write_json, []

    def write(path, document):
        if (
            path.parent == owner.root / "jobs"
            and document.get("state") == "cancelled"
            and (job_id is None or document["id"] == job_id)
        ):
            calls.append(json.loads(json.dumps(document)))
            if len(calls) == 1:
                if after:
                    original(path, document)
                raise OSError("injected cancellation acknowledgement loss")
        original(path, document)

    monkeypatch.setattr(research_module, "write_json", write)
    return original, calls


@pytest.mark.parametrize("after", [False, True])
def test_schedule_stop_ack_failure_stays_unconfirmed_after_worker_join(
    tmp_path, monkeypatch, after
):
    owner, *_ = setup(tmp_path)
    configure(owner)
    original, calls = schedule_module.atomic_json, []

    def write(path, envelope):
        if path == owner.path:
            calls.append(path)
            if after:
                original(path, envelope)
            raise OSError("injected stop acknowledgement loss")
        original(path, envelope)

    monkeypatch.setattr(schedule_module, "atomic_json", write)
    with pytest.raises(StorageError):
        owner.close()
    saved = owner.path.read_bytes()
    assert owner.closed and owner.storage_failed
    assert owner.thread is None or not owner.thread.is_alive()
    for _ in range(2):
        with pytest.raises(StorageError, match="unconfirmed"):
            owner.close()
    assert len(calls) == 1 and owner.path.read_bytes() == saved
    assert owner.snapshot()["storage_available"] is False


@pytest.mark.parametrize("after", [False, True])
@pytest.mark.parametrize("first", ["close", "cancel"])
def test_before_after_ack_retries_same_fixed_cancel_and_never_dispatches(
    research, monkeypatch, after, first
):
    job = research.submit("nn_inspect", {"demo": True, "label": "0.00100"})
    original, calls = fault_write(research, monkeypatch, after=after)
    clock = [job["created_at_ms"] / 1000 + 1]
    initial_finished = int(clock[0] * 1000)
    monkeypatch.setattr(research_module.time, "time", lambda: clock[0])
    with pytest.raises((RuntimeError, OSError)):
        research.close() if first == "close" else research.cancel(job["id"])
    plan = research._cancel_intents[job["id"]]
    expected = json.loads(json.dumps(plan["target"]))
    assert plan["error_type"] == "OSError" and not plan["confirmed"]
    assert job["id"] not in research._pending and job["id"] not in research._requests
    assert expected["finished_at_ms"] == initial_finished
    clock[0] += 1000
    research.close()
    assert not research._thread.is_alive() and research._closed
    assert research._active is research._running_process is None
    assert read_status_json(research._job_path(job["id"])) == expected
    assert len(calls) == (1 if after else 2)
    assert all(row == expected for row in calls)
    expected_bytes = (
        json.dumps(expected, ensure_ascii=False, sort_keys=True, allow_nan=False, indent=2) + "\n"
    ).encode()
    assert research._job_path(job["id"]).read_bytes() == expected_bytes
    before = list(calls)
    research.close()
    assert calls == before


@pytest.mark.parametrize("after", [False, True])
def test_duplicate_public_cancel_reconciles_original_target_without_new_timestamp(
    research, monkeypatch, after
):
    job = research.submit("nn_inspect", {"demo": True})
    _, calls = fault_write(research, monkeypatch, after=after)
    with pytest.raises(OSError):
        research.cancel(job["id"])
    expected = json.loads(json.dumps(research._cancel_intents[job["id"]]["target"]))
    assert research.cancel(job["id"]) == expected
    assert not research._cancel_intents
    assert research.cancel(job["id"]) == expected
    assert len(calls) == (1 if after else 2)
    research.close()


@pytest.mark.parametrize("change", ["state", "parameters", "epoch", "missing", "malformed"])
def test_foreign_or_missing_document_is_never_overwritten_on_repeat_close(
    research, monkeypatch, change
):
    job = research.submit("nn_inspect", {"demo": True})
    original, calls = fault_write(research, monkeypatch)
    with pytest.raises(RuntimeError):
        research.close()
    plan, path = research._cancel_intents[job["id"]], research._job_path(job["id"])
    foreign = json.loads(json.dumps(plan["original"]))
    if change == "state":
        foreign["state"] = "complete"
    elif change == "parameters":
        foreign["parameters"] = {"demo": False}
    elif change == "epoch":
        foreign["core_epoch"] = "another-generation"
    if change == "missing":
        path.unlink()
        saved = None
    elif change == "malformed":
        path.write_bytes(b"{broken")
        saved = path.read_bytes()
    else:
        original(path, foreign)
        saved = path.read_bytes()
    with pytest.raises(RuntimeError):
        research.close()
    assert (path.read_bytes() if path.exists() else None) == saved
    assert len(calls) == 1 and not plan["confirmed"]
    assert not research._thread.is_alive()
    # Restoring the exact original restores only this original intent's authority.
    original(path, plan["original"])
    research.close()
    assert plan["confirmed"]


def test_first_unreadable_original_during_close_stays_unknown_even_when_file_returns(
    research, monkeypatch
):
    job = research.submit("nn_inspect", {"demo": True})
    path, original = research._job_path(job["id"]), research_module.read_status_json
    saved = path.read_bytes()
    with monkeypatch.context() as patch:

        def fail(path):
            raise OSError("original metadata unreadable")

        patch.setattr(research_module, "read_status_json", fail)
        with pytest.raises(RuntimeError):
            research.close()
    assert research._cancel_intents[job["id"]]["capture_failed"]
    with pytest.raises(RuntimeError):
        research.close()
    assert path.read_bytes() == saved and original(path)["state"] == "queued"
    assert not research._thread.is_alive() and not research._pending and not research._requests


def test_public_capture_failure_does_not_drop_a_queued_job_or_reserve_a_false_intent(
    research, monkeypatch
):
    job = research.submit("nn_inspect", {"demo": True})
    original = research_module.read_status_json
    reads = []

    def read(path):
        reads.append(path)
        if len(reads) == 1:
            raise OSError("capture failed before admission")
        return original(path)

    with monkeypatch.context() as patch:
        patch.setattr(research_module, "read_status_json", read)
        with pytest.raises(OSError):
            research.cancel(job["id"])
    assert research._pending == [job["id"]] and job["id"] in research._requests
    assert not research._cancel_intents
    research.close()


def test_fresh_cancel_refuses_foreign_terminal_metadata_without_dropping_queue(research):
    job = research.submit("nn_inspect", {"demo": True})
    original = research_module.read_status_json(research._job_path(job["id"]))
    changed = dict(original, state="complete")
    research_module.write_json(research._job_path(job["id"]), changed)
    with pytest.raises(RuntimeError, match="foreign terminal"):
        research.cancel(job["id"])
    assert research._pending == [job["id"]] and not research._cancel_intents
    assert research.status(job["id"]) == changed
    research_module.write_json(research._job_path(job["id"]), original)
    research.close()


def test_unconfirmed_cancel_keeps_its_original_queue_budget_until_actual_ack(research, monkeypatch):
    jobs = [research.submit("nn_inspect", {"demo": True, "index": index}) for index in range(32)]
    _, calls = fault_write(research, monkeypatch, job_id=jobs[0]["id"])
    with pytest.raises(OSError):
        research.cancel(jobs[0]["id"])
    assert len(research._pending) == 31 and len(research._cancel_intents) == 1
    with pytest.raises(ValueError, match="queue is full"):
        research.submit("nn_inspect", {"demo": True, "index": "extra"})
    expected = research._cancel_intents[jobs[0]["id"]]["target"]
    assert research.cancel(jobs[0]["id"]) == expected
    assert len(calls) == 2 and not research._cancel_intents
    research.submit("nn_inspect", {"demo": True, "index": "replacement"})
    assert len(research._pending) == 32
    research.close()
    assert not research._thread.is_alive() and not research._pending


def test_close_attempts_other_metadata_without_refunding_unknown_job(research, monkeypatch):
    first, second = [research.submit("nn_inspect", {"demo": True}) for _ in range(2)]
    _, calls = fault_write(research, monkeypatch, job_id=first["id"])
    with pytest.raises(RuntimeError):
        research.close()
    assert not research._cancel_intents[first["id"]]["confirmed"]
    assert research._cancel_intents[second["id"]]["confirmed"]
    assert research.status(second["id"])["state"] == "cancelled"
    research.close()
    assert len(calls) == 2


@pytest.mark.parametrize("action", ["cancel", "close"])
def test_cancellation_file_reads_and_writes_are_outside_dispatch_condition(
    research, monkeypatch, action
):
    job = research.submit("nn_inspect", {"demo": True})
    read, write = research_module.read_status_json, research_module.write_json
    calls = []

    def observe_read(path):
        assert not research._condition._is_owned()
        calls.append("read")
        return read(path)

    def observe_write(path, document):
        assert not research._condition._is_owned()
        calls.append("write")
        return write(path, document)

    monkeypatch.setattr(research_module, "read_status_json", observe_read)
    monkeypatch.setattr(research_module, "write_json", observe_write)
    research.cancel(job["id"]) if action == "cancel" else research.close()
    assert calls.count("write") == 1 and calls.count("read") >= 2
    research.close()
