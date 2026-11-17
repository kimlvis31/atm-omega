"""Incident history cannot prove present recovery or disclose request contents."""

import json

from omega.runtime.control import ControlState, incident_error_context


def test_restart_preserves_unconfirmed_critical_and_current_recurrence_updates_same_id(tmp_path):
    now = [100.0]
    path = tmp_path / "control.json"
    first = ControlState(path, clock=lambda: now[0])
    first.incident("CONTROL_TRANSPORT_OR_STORAGE_FAILURE")
    original = first.snapshot()["incidents"][0]
    assert original["observation_scope"] == "current_core"
    now[0] = 50.0  # Wall-clock rollback does not select the current/previous core.
    second = ControlState(path, clock=lambda: now[0])
    old = second.snapshot()["incidents"][0]
    assert old["observation_scope"] == "previous_core" and not old["acknowledged"]
    second.incident("CONTROL_TRANSPORT_OR_STORAGE_FAILURE", context={"stage": "operation"})
    repeated = second.snapshot()["incidents"][0]
    assert repeated["id"] == original["id"] and repeated["timestamp"] == 100
    assert repeated["last_seen_at"] == 50 and repeated["occurrences"] == 2
    assert repeated["origin_epoch"] == first.epoch and repeated["last_seen_epoch"] == second.epoch
    assert repeated["observation_scope"] == "current_core" and repeated["severity"] == "critical"
    assert repeated["revision"] == original["revision"]
    second.acknowledge(repeated["id"])
    acknowledged = second.snapshot()["incidents"][0]
    assert acknowledged["acknowledged"] and "resolved" not in acknowledged
    assert not json.loads(path.read_text())["incidents"][0].get("observation_scope")


def test_legacy_incident_has_unknown_provenance_without_automatic_rewrite(tmp_path):
    path = tmp_path / "control.json"
    first = ControlState(path)
    first.incident("OLD")
    document = json.loads(path.read_text())
    row = document["incidents"][0]
    for key in ("origin_epoch", "last_seen_epoch", "last_seen_at", "occurrences", "context"):
        del row[key]
    path.write_text(json.dumps(document))
    second = ControlState(path)
    assert second.snapshot()["incidents"][0]["observation_scope"] == "unknown"
    assert json.loads(path.read_text())["incidents"][0] == row


def test_context_allowlist_and_critical_recording_failure_keep_safe_state(tmp_path, monkeypatch):
    control = ControlState(tmp_path / "control.json")
    message = "token=private query=account123 password=secret"
    context = incident_error_context(TimeoutError(13, message), stage="operation", operation="POST")
    assert context == {
        "stage": "operation",
        "operation": "POST",
        "error_kind": "timeout",
        "errno": 13,
    }
    monkeypatch.setattr(
        "omega.runtime.control.atomic_json", lambda *_: (_ for _ in ()).throw(OSError(message))
    )
    control.incident("IO", context={**context, "raw": message, "path": message})
    snapshot = control.snapshot()
    assert snapshot["paused"] and not snapshot["storage_available"]
    assert snapshot["incidents"][0]["severity"] == "critical"
    assert snapshot["incidents"][0]["context"] == context
    assert message not in json.dumps(snapshot)
