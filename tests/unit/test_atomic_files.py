"""Only a completed rename is retried; financial/state writes are never replayed."""

import errno
import json
import os

import pytest

from omega.infrastructure import atomic_files as atomic
from omega.runtime.settings import SettingsStore


class Clock:
    def __init__(self):
        self.now = 0.0
        self.delays = []

    def monotonic(self):
        return self.now

    def sleep(self, delay):
        self.delays.append(delay)
        self.now += delay


def sharing_error(code):
    error = OSError("synthetic filesystem denial")
    error.winerror = code
    return error


@pytest.fixture
def retry_clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(atomic, "_platform", "nt")
    monkeypatch.setattr(atomic, "_clock", clock.monotonic)
    monkeypatch.setattr(atomic, "_sleep", clock.sleep)
    return clock


@pytest.mark.parametrize("code", [5, 32, 33])
def test_transient_sharing_errors_retry_same_completed_file(
    tmp_path, monkeypatch, retry_clock, code
):
    source, target = tmp_path / "complete.tmp", tmp_path / "state.json"
    source.write_bytes(b"new completed payload")
    target.write_bytes(b"previous payload")
    attempts = []

    def replace(source_path, target_path):
        attempts.append((source_path, target_path, source.read_bytes()))
        assert target.read_bytes() == b"previous payload"
        if len(attempts) < 3:
            raise sharing_error(code)
        os.replace(source_path, target_path)

    monkeypatch.setattr(atomic, "_replace", replace)
    atomic.replace_atomic(source, target)
    assert attempts == [(source, target, b"new completed payload")] * 3
    assert retry_clock.delays == [0.010, 0.020]
    assert target.read_bytes() == b"new completed payload"
    assert not source.exists()


def test_permanent_sharing_denial_is_bounded_and_preserves_both_files(
    tmp_path, monkeypatch, retry_clock
):
    source, target = tmp_path / "complete.tmp", tmp_path / "state.json"
    source.write_bytes(b"completed")
    target.write_bytes(b"previous")
    failure, attempts = sharing_error(5), []

    def denied(*paths):
        attempts.append((retry_clock.now, paths))
        raise failure

    monkeypatch.setattr(atomic, "_replace", denied)
    with pytest.raises(OSError) as caught:
        atomic.replace_atomic(source, target)
    assert caught.value is failure
    assert retry_clock.now == pytest.approx(0.250)
    assert max(retry_clock.delays) <= 0.050
    assert all(timestamp < 0.250 for timestamp, _paths in attempts)
    assert len(attempts) == 7
    assert source.read_bytes() == b"completed"
    assert target.read_bytes() == b"previous"


@pytest.mark.parametrize("platform,code", [("posix", 5), ("posix", 32), ("posix", 33), ("nt", 112)])
def test_other_platforms_and_other_windows_errors_fail_without_wait(
    monkeypatch, retry_clock, platform, code
):
    monkeypatch.setattr(atomic, "_platform", platform)
    failure, attempts = sharing_error(code), []

    def denied(*paths):
        attempts.append(paths)
        raise failure

    monkeypatch.setattr(atomic, "_replace", denied)
    with pytest.raises(OSError) as caught:
        atomic.replace_atomic("source", "target")
    assert caught.value is failure
    assert attempts == [("source", "target")]
    assert retry_clock.delays == []


def test_errno_without_windows_code_does_not_retry(monkeypatch, retry_clock):
    failure = OSError(errno.EACCES, "synthetic access denial")

    def denied(*_paths):
        raise failure

    monkeypatch.setattr(atomic, "_replace", denied)
    with pytest.raises(OSError) as caught:
        atomic.replace_atomic("source", "target")
    assert caught.value is failure
    assert retry_clock.delays == []


def test_time_spent_in_replace_consumes_retry_budget(monkeypatch, retry_clock):
    attempts = []

    def slow_denial(*paths):
        attempts.append(paths)
        retry_clock.now += 0.300
        raise sharing_error(32)

    monkeypatch.setattr(atomic, "_replace", slow_denial)
    with pytest.raises(OSError):
        atomic.replace_atomic("source", "target")
    assert len(attempts) == 1
    assert retry_clock.delays == []


def test_successful_replace_never_sleeps(tmp_path, monkeypatch, retry_clock):
    source, target = tmp_path / "source", tmp_path / "target"
    source.write_bytes(b"complete")
    monkeypatch.setattr(atomic, "_replace", os.replace)
    atomic.replace_atomic(source, target)
    assert target.read_bytes() == b"complete"
    assert retry_clock.delays == []


def test_settings_retry_uses_one_temporary_payload_and_one_revision(
    tmp_path, monkeypatch, retry_clock
):
    target = tmp_path / "settings.json"
    store = SettingsStore(target)
    attempts = []

    def transient(source, destination):
        payload = source.read_bytes()
        attempts.append((source, destination, payload))
        assert json.loads(payload)["revision"] == 1
        assert not target.exists()
        if len(attempts) < 3:
            raise sharing_error(32)
        os.replace(source, destination)

    monkeypatch.setattr(atomic, "_replace", transient)
    result = store.update({"logging": {"retention_days": 7}}, revision=0)
    assert result["revision"] == 1
    assert len(attempts) == 3 and all(attempt == attempts[0] for attempt in attempts)
    assert list(tmp_path.iterdir()) == [target]
    assert json.loads(target.read_bytes()) == store.snapshot()


def test_settings_permanent_denial_preserves_previous_state_and_cleans_temporary(
    tmp_path, monkeypatch, retry_clock
):
    target = tmp_path / "settings.json"
    store = SettingsStore(target)
    monkeypatch.setattr(atomic, "_replace", os.replace)
    previous = store.update({}, revision=0)
    previous_bytes = target.read_bytes()
    failure = sharing_error(5)

    def denied(*_paths):
        raise failure

    monkeypatch.setattr(atomic, "_replace", denied)
    with pytest.raises(OSError) as caught:
        store.update({"logging": {"retention_days": 7}}, revision=1)
    assert caught.value is failure
    assert target.read_bytes() == previous_bytes
    assert store.snapshot() == previous
    assert list(tmp_path.iterdir()) == [target]
    assert retry_clock.now == pytest.approx(0.250)
