"""Actual original lock identity observations, no model or financial authority."""

from omega.runtime.causal_root_identity import capture_managed_root_identity
from omega.runtime.control import InstanceLock


def test_same_actual_lock_reopen_has_same_managed_identity(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "omega.runtime.causal_root_identity._machine_scope", lambda: ("os_instance", "test-machine")
    )
    first = InstanceLock(tmp_path / "state/core.lock")
    original = first.stream
    before = capture_managed_root_identity(tmp_path, first, original_stream=original)
    assert before is not None
    first.close()
    assert capture_managed_root_identity(tmp_path, first, original_stream=original) is None
    second = InstanceLock(tmp_path / "state/core.lock")
    try:
        assert capture_managed_root_identity(tmp_path, second) == before
    finally:
        second.close()


def test_copied_root_or_unavailable_machine_cannot_match(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "omega.runtime.causal_root_identity._machine_scope", lambda: ("os_instance", "test-machine")
    )
    first, second = (
        InstanceLock(tmp_path / "a/state/core.lock"),
        InstanceLock(tmp_path / "b/state/core.lock"),
    )
    try:
        assert capture_managed_root_identity(
            tmp_path / "a", first
        ) != capture_managed_root_identity(tmp_path / "b", second)
        assert capture_managed_root_identity(tmp_path / "a", second) is None
        monkeypatch.setattr("omega.runtime.causal_root_identity._machine_scope", lambda: None)
        assert capture_managed_root_identity(tmp_path / "a", first) is None
    finally:
        first.close()
        second.close()


def test_named_path_must_still_be_the_original_open_descriptor(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "omega.runtime.causal_root_identity._machine_scope", lambda: ("os_instance", "test-machine")
    )
    lock = InstanceLock(tmp_path / "state/core.lock")
    actual_stat = (tmp_path / "state/core.lock").stat()
    try:
        monkeypatch.setattr(
            "omega.runtime.causal_root_identity._named_identity",
            lambda _path: (actual_stat.st_dev, actual_stat.st_ino + 1),
        )
        assert capture_managed_root_identity(tmp_path, lock) is None
    finally:
        lock.close()
