"""Control replay, privilege and recording failure invariants."""

import json

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.control import ControlState, InstanceLock, load_token


def resume_payload(state):
    snapshot = state.snapshot()
    return {
        "expected_epoch": snapshot["epoch"],
        "expected_revision": snapshot["control_revision"],
        "expires_at_ms": snapshot["server_time_ms"] + 20_000,
    }


def test_instance_owner_is_exclusive_and_releases(tmp_path):
    first = InstanceLock(tmp_path / "core.lock")
    try:
        with pytest.raises(RuntimeError, match="already owns"):
            InstanceLock(tmp_path / "core.lock")
    finally:
        first.close()
    InstanceLock(tmp_path / "core.lock").close()


def test_token_is_stable_and_not_replaced_if_corrupt(tmp_path):
    path = tmp_path / "token"
    token = load_token(path)
    assert len(token) >= 43
    assert load_token(path) == token
    path.write_text("invalid", encoding="utf-8")
    with pytest.raises(ValidationError):
        load_token(path)


def test_command_replay_is_durable_and_payload_conflicts_fail(tmp_path):
    path = tmp_path / "control.json"
    state = ControlState(path)
    calls = []
    command = {"id": "one", "kind": "pause", "payload": {}}

    def execute(kind, payload):
        calls.append(kind)
        return {"paused": True}

    first = state.command("admin", command, execute)
    assert state.command("admin", command, execute) == first
    restarted = ControlState(path)
    assert restarted.command("admin", command, execute) == first
    assert calls == ["pause"]
    with pytest.raises(ValidationError, match="reused"):
        restarted.command("admin", {**command, "payload": {"new": 1}}, execute)


def test_provider_exception_never_retries_and_pauses(tmp_path):
    state = ControlState(tmp_path / "control.json")
    calls = []

    def execute(kind, payload):
        calls.append(kind)
        raise RuntimeError("could have applied the action")

    command = {"id": "one", "kind": "resume", "payload": resume_payload(state)}
    assert state.command("admin", command, execute)["status"] == "unknown"
    assert state.command("admin", command, execute)["status"] == "unknown"
    assert calls == ["resume"]
    assert state.snapshot()["paused"]


def test_state_restarts_paused_even_after_successful_resume(tmp_path):
    path = tmp_path / "control.json"
    state = ControlState(path)
    state.command(
        "admin", {"id": "r", "kind": "resume", "payload": resume_payload(state)}, lambda *_: {}
    )
    assert not state.snapshot()["paused"]
    assert ControlState(path).snapshot()["paused"]


def test_one_use_pairing_permissions_revocation_and_hash_only(tmp_path):
    state = ControlState(tmp_path / "control.json")
    code = state.create_pairing("phone", ["read", "pause"])["code"]
    paired = state.pair(code)
    assert state.authorize(paired["token"], "owner") == paired["device_id"]
    assert paired["token"] not in state.path.read_text()
    with pytest.raises(PermissionError):
        state.pair(code)
    with pytest.raises(PermissionError):
        state.command(
            paired["device_id"], {"id": "r", "kind": "resume", "payload": {}}, lambda *_: {}
        )
    state.revoke(paired["device_id"])
    assert state.authorize(paired["token"], "owner") is None


def test_pairing_expires_and_unknown_permissions_rejected(tmp_path):
    now = [100.0]
    state = ControlState(tmp_path / "control.json", clock=lambda: now[0])
    with pytest.raises(ValidationError):
        state.create_pairing("phone", ["read", "admin"])
    code = state.create_pairing("phone")["code"]
    now[0] += 121
    with pytest.raises(PermissionError):
        state.pair(code)


def test_torn_control_state_does_not_reset_history(tmp_path):
    path = tmp_path / "control.json"
    path.write_text('{"schema_version":1,', encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        ControlState(path)


def test_failed_final_commit_is_unknown_and_resume_is_blocked(tmp_path, monkeypatch):
    import omega.runtime.control as control

    state = ControlState(tmp_path / "control.json")
    original = control.atomic_json
    count = [0]

    def fail_second(path, data):
        count[0] += 1
        if count[0] >= 2:
            raise OSError("disk full")
        original(path, data)

    monkeypatch.setattr(control, "atomic_json", fail_second)
    result = state.command(
        "admin", {"id": "r", "kind": "resume", "payload": resume_payload(state)}, lambda *_: {}
    )
    assert result["status"] == "unknown"
    assert state.snapshot()["paused"]
    assert not state.snapshot()["storage_available"]
    with pytest.raises(ValidationError):
        state.command(
            "admin", {"id": "r2", "kind": "resume", "payload": resume_payload(state)}, lambda *_: {}
        )
    assert (
        state.command("admin", {"id": "p", "kind": "pause", "payload": {}}, lambda *_: {})["status"]
        == "volatile"
    )


def test_resume_requires_fresh_epoch_revision_deadline_and_strips_metadata(tmp_path):
    state = ControlState(tmp_path / "control.json")
    valid = resume_payload(state)
    invalid_payloads = (
        {},
        {**valid, "expected_epoch": "old"},
        {**valid, "expected_revision": 0},
        {**valid, "expires_at_ms": 0},
        {**valid, "expires_at_ms": valid["expires_at_ms"] + 100_000},
    )
    for invalid in invalid_payloads:
        with pytest.raises(ValidationError):
            state.command(
                "admin", {"id": "invalid", "kind": "resume", "payload": invalid}, lambda *_: {}
            )
    calls = []
    command = {"id": "fresh", "kind": "resume", "payload": valid}
    state.command("admin", command, lambda kind, payload: calls.append((kind, payload)) or {})
    state.command("admin", command, lambda *_: pytest.fail("duplicate resume"))
    assert calls == [("resume", {})]
    state.command("admin", {"id": "p", "kind": "pause", "payload": {}}, lambda *_: {})
    with pytest.raises(ValidationError, match="revision"):
        state.command("admin", {**command, "id": "stale"}, lambda *_: {})
    restarted = ControlState(state.path)
    with pytest.raises(ValidationError, match="epoch"):
        restarted.command("admin", command, lambda *_: {})


def test_unserializable_provider_result_is_unknown_and_pauses(tmp_path):
    state = ControlState(tmp_path / "control.json")
    response = state.command(
        "admin",
        {"id": "r", "kind": "resume", "payload": resume_payload(state)},
        lambda *_: {"invalid": object()},
    )
    assert response["status"] == "unknown"
    assert state.snapshot()["paused"]
    assert not state.snapshot()["storage_available"]


def test_emergency_pause_survives_full_history(tmp_path):
    state = ControlState(tmp_path / "control.json")
    state.data["commands"] = {str(index): {} for index in range(4096)}
    calls = []
    response = state.command(
        "admin",
        {"id": "pause", "kind": "pause", "payload": {}},
        lambda kind, payload: calls.append(kind),
    )
    assert response["status"] == "volatile"
    assert state.snapshot()["paused"]
    assert calls == ["pause"]
