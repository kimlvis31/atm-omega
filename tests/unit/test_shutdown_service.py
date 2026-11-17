"""Real shutdown OP files with data-only fake participants; no financial execution."""

import json
import threading
import time
from copy import deepcopy
from types import SimpleNamespace

import pytest

from omega.domain.errors import ValidationError
from omega.runtime import shutdown
from omega.runtime.shutdown import ShutdownService, participant_view


class Participant:
    def __init__(self, owner="paper_v1", *, exposed=False):
        self.lock = threading.RLock()
        self.reads, self.drains, self.cancels = 0, [], []
        self.view = {
            "owner": owner,
            "ready": True,
            "durable": True,
            "supports_flatten": True,
            "positions": [
                {
                    "account_id": "demo",
                    "instance": "ts",
                    "instrument": "binance:usdm:BTCUSDT",
                    "quantity": "1",
                }
            ]
            if exposed
            else [],
            "pending_orders": [],
            "unknown": [],
            "notes": ["data_only_test_participant"],
        }

    def shutdown_snapshot(self):
        with self.lock:
            self.reads += 1
            return deepcopy(self.view)

    def shutdown_drain(self, identity, issued_at_ms):
        with self.lock:
            self.drains.append((identity, issued_at_ms))
            if self.view["positions"]:
                self.view["pending_orders"] = [
                    {
                        **self.view["positions"][0],
                        "quantity": "1",
                        "order_id": "reducing-order",
                        "reducing": True,
                    }
                ]
        return {"requested": True}

    def shutdown_cancel(self, identity):
        with self.lock:
            self.cancels.append(identity)
        return {"pending_preserved": True}

    def flat(self):
        with self.lock:
            self.view.update(positions=[], pending_orders=[])


def wait(read, check):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        value = read()
        if check(value):
            return value
        time.sleep(0.005)
    raise AssertionError(value)


def owner(root, *participants, epoch="core-a"):
    result = SimpleNamespace(pauses=[], exits=[], incidents=[], entries_paused=False)

    def pause(identity):
        result.pauses.append(identity)
        result.entries_paused = True

    result.service = ShutdownService(
        root,
        participants or (Participant(),),
        core_epoch=epoch,
        pause_entries=pause,
        on_exit=lambda: result.exits.append("exit_requested"),
        on_incident=result.incidents.append,
        clock_ms=lambda: 120000,
    )
    result.service.start()
    wait(result.service.snapshot, lambda row: row["ready"] or bool(row["error"]))
    return result


def submit(context, identity, kind, parameters=None):
    service = context.service
    return service.submit(
        identity,
        service.core_epoch,
        service.snapshot()["revision"],
        kind,
        {} if parameters is None else parameters,
    )


def finished(context, identity):
    return wait(
        lambda: context.service.status(identity),
        lambda row: row["state"] in shutdown.TERMINAL or row["state"] == "exit_requested",
    )


def draining(context, identity):
    return wait(
        lambda: context.service.status(identity),
        lambda row: row["state"] == "waiting_fill" and row["summary"]["pending_count"] == 1,
    )


def test_review_observes_without_pausing_or_exiting(tmp_path):
    participant = Participant(exposed=True)
    context = owner(tmp_path, participant)
    try:
        submit(context, "review", "review")
        row = finished(context, "review")
        assert row["state"] == "completed" and row["summary"]["positions_count"] == 1
        assert not context.pauses and not context.entries_paused and not context.exits
        assert not participant.drains and not participant.cancels
        assert (tmp_path / "state/shutdown/operations/review.json").is_file()
    finally:
        context.service.close()


def test_save_requires_explicit_unmanaged_ack_and_duplicate_never_exits_twice(tmp_path):
    context = owner(tmp_path, Participant(exposed=True))
    try:
        submit(context, "unconfirmed", "save_exit", {"acknowledge_unmanaged_positions": False})
        assert finished(context, "unconfirmed")["state"] == "review_required"
        assert not context.exits and context.entries_paused
        request = submit(
            context, "confirmed", "save_exit", {"acknowledge_unmanaged_positions": True}
        )
        row = finished(context, "confirmed")
        wait(lambda: len(context.exits), lambda count: count == 1)
        assert row["result"] == {"exit_requested": True, "core_exit_confirmed": False}
        assert (
            context.service.submit(
                "confirmed",
                "core-a",
                request["expected_revision"],
                "save_exit",
                request["parameters"],
            )
            == row
        )
        assert context.service.status("confirmed") == row and len(context.exits) == 1
        with pytest.raises(ValidationError, match="conflicts"):
            context.service.submit(
                "confirmed",
                "core-a",
                request["expected_revision"],
                "save_exit",
                {"acknowledge_unmanaged_positions": False},
            )
        context.service.close()
        context.service.finish_exit(successful=True)
        assert context.service.status("confirmed")["result"] == {"core_exit_confirmed": True}
    finally:
        context.service.close()


def test_flatten_requests_once_then_waits_for_participant_confirmed_flat(tmp_path):
    participant = Participant(exposed=True)
    context = owner(tmp_path, participant)
    try:
        request = submit(context, "flatten", "flatten_exit")
        row = draining(context, "flatten")
        assert not context.exits and context.service.blocks_new_risk
        assert participant.drains == [("flatten", 120000)] and context.pauses == ["flatten"]
        assert (
            context.service.submit(
                "flatten", "core-a", request["expected_revision"], "flatten_exit", {}
            )
            == row
        )
        participant.flat()
        with context.service.condition:
            context.service.condition.notify_all()
        result = finished(context, "flatten")
        wait(lambda: len(context.exits), lambda count: count == 1)
        assert result["state"] == "exit_requested" and result["summary"]["positions_count"] == 0
        assert result["summary"]["pending_count"] == 0 and participant.drains == [
            ("flatten", 120000)
        ]
    finally:
        context.service.close()


@pytest.mark.parametrize("fault", ["unknown", "unsupported", "not_durable"])
def test_all_participants_preflight_before_any_reduction(tmp_path, fault):
    paper, second = Participant(exposed=True), Participant("margin_v2", exposed=True)
    if fault == "unknown":
        second.view.update(ready=False, unknown=["financial_ack_unknown"])
    elif fault == "unsupported":
        second.view["supports_flatten"] = False
    else:
        second.view["durable"] = False
    context = owner(tmp_path, paper, second)
    try:
        submit(context, "flatten", "flatten_exit")
        assert finished(context, "flatten")["state"] == "blocked"
        assert not paper.drains and not second.drains and not context.exits
        assert context.entries_paused
    finally:
        context.service.close()


def test_cancel_keeps_existing_pending_and_does_not_resume_or_exit(tmp_path):
    participant = Participant(exposed=True)
    context = owner(tmp_path, participant)
    try:
        submit(context, "flatten", "flatten_exit")
        draining(context, "flatten")
        pending = deepcopy(participant.view["pending_orders"])
        submit(context, "cancel", "cancel")
        assert finished(context, "cancel")["result"]["pending_preserved"]
        assert context.service.status("flatten")["state"] == "canceled"
        assert participant.view["pending_orders"] == pending and participant.cancels == ["flatten"]
        assert context.entries_paused and context.pauses == ["flatten"] and not context.exits
        participant.flat()
        with context.service.condition:
            context.service.condition.notify_all()
        time.sleep(0.02)
        assert not context.exits
    finally:
        context.service.close()


def test_stale_epoch_revision_and_invalid_pending_identifiers_are_separate(tmp_path):
    context = owner(tmp_path)
    try:
        revision = context.service.snapshot()["revision"]
        path = tmp_path / "state/shutdown/index.json"
        before = path.read_bytes()
        with pytest.raises(ValidationError, match="epoch"):
            context.service.submit("old", "retired-core", revision, "review", {})
        with pytest.raises(ValidationError, match="revision"):
            context.service.submit("future", "core-a", revision + 1, "review", {})
        assert path.read_bytes() == before and not context.pauses and not context.exits
        view = Participant().view
        view["pending_orders"] = [
            {
                "account_id": "demo",
                "instance": "ts",
                "instrument": "binance:usdm:BTCUSDT",
                "quantity": "1",
                "reducing": False,
                "order_id": "entry:prior.valid",
            }
        ]
        assert participant_view(view)["pending_orders"][0]["order_id"] == "entry:prior.valid"
    finally:
        context.service.close()


def test_restart_interrupted_exit_does_not_repeat_callback_or_old_operation(tmp_path):
    first = owner(tmp_path)
    request = submit(first, "save", "save_exit", {"acknowledge_unmanaged_positions": False})
    finished(first, "save")
    wait(lambda: len(first.exits), lambda count: count == 1)
    first.service.close()
    second = owner(tmp_path, epoch="core-b")
    try:
        row = second.service.status("save")
        assert row["state"] == "interrupted" and row["core_epoch"] == "core-a"
        assert (
            not second.exits
            and not second.pauses
            and second.service.snapshot()["operation_id"] is None
        )
        assert (
            second.service.submit(
                "save", "core-b", request["expected_revision"], "save_exit", request["parameters"]
            )
            == row
        )
        assert not second.exits
        with pytest.raises(ValidationError, match="epoch"):
            second.service.submit(
                "save", "core-a", request["expected_revision"], "save_exit", request["parameters"]
            )
    finally:
        second.service.close()


def test_checksum_failure_preserves_original_and_never_exits(tmp_path):
    first = owner(tmp_path)
    first.service.close()
    path = tmp_path / "state/shutdown/index.json"
    value = json.loads(path.read_text())
    value["checksum"] = "0" * 64
    path.write_text(json.dumps(value))
    corrupted = path.read_bytes()
    second = owner(tmp_path, epoch="core-b")
    try:
        assert not second.service.snapshot()["ready"] and second.service.snapshot()["error"]
        assert path.read_bytes() == corrupted and not second.exits and not second.pauses
    finally:
        second.service.close()


def test_exit_index_commit_failure_leaves_unknown_without_callback_or_replay(tmp_path, monkeypatch):
    context = owner(tmp_path)
    original = shutdown.atomic_json

    def fail(path, value):
        if path.name == "index.json" and value["data"]["state"] == "exit_requested":
            raise OSError("synthetic index commit failure")
        return original(path, value)

    monkeypatch.setattr(shutdown, "atomic_json", fail)
    try:
        submit(context, "save", "save_exit", {"acknowledge_unmanaged_positions": False})
        wait(context.service.snapshot, lambda row: bool(row["error"]))
        assert context.service.status("save")["state"] == "unknown"
        assert not context.exits and context.entries_paused and context.service.blocks_new_risk
        path = tmp_path / "state/shutdown/operations/save.json"
        saved = path.read_bytes()
        assert json.loads(saved)["data"]["state"] == "exit_requested"
    finally:
        context.service.close()
    monkeypatch.setattr(shutdown, "atomic_json", original)
    restarted = owner(tmp_path, epoch="core-b")
    try:
        assert restarted.service.status("save")["state"] == "interrupted" and not restarted.exits
    finally:
        restarted.service.close()


def test_completed_receipt_eviction_retains_exact_id_lookup_on_disk(tmp_path):
    participant = Participant()
    context = owner(tmp_path, participant)
    try:
        first = None
        for index in range(34):
            submit(context, f"review-{index}", "review")
            row = finished(context, f"review-{index}")
            if first is None:
                first = row
        assert len(context.service.cache) <= 32 and "review-0" not in context.service.cache
        reads = participant.reads
        path = tmp_path / "state/shutdown/operations/review-0.json"
        before = path.read_bytes()
        assert context.service.status("review-0") == first
        assert (
            context.service.submit("review-0", "core-a", first["expected_revision"], "review", {})
            == first
        )
        assert path.read_bytes() == before and participant.reads == reads
        assert not context.pauses and not context.exits
    finally:
        context.service.close()
