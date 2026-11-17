"""Prepared standalone lease checks; injected faults are not OS failure claims."""

import inspect
import json
import sys
import threading
from dataclasses import FrozenInstanceError, asdict
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from omega.runtime import causal_inference_leases as module
from omega.runtime.causal_inference_leases import InferenceLeaseRegistry, LeaseCleanupError


def lease_path(tmp_path):
    return tmp_path / (uuid4().hex + ".lock")


def cleanup(registry):
    receipt = registry.close_all()
    assert receipt.all_confirmed, asdict(receipt)
    assert all(lease.physical_status == "confirmed" for lease in receipt.leases)
    return receipt


class StreamProbe:
    """Actual temp-file wrapper with explicit, deliberate Python fault seams."""

    def __init__(self, native):
        self.native = native
        self.close_calls = self.closed_reads = 0
        self.closed_faults = 0
        self.fail_after_native = False
        self.native_close_returned = False
        self.failure = OSError("injected close secret-path/secret-token")

    def __getattr__(self, name):
        return getattr(self.native, name)

    @property
    def closed(self):
        self.closed_reads += 1
        if self.closed_faults:
            self.closed_faults -= 1
            raise OSError("injected closed observation secret")
        return self.native.closed

    def close(self):
        self.close_calls += 1
        self.native.close()
        self.native_close_returned = True
        if self.fail_after_native:
            raise self.failure


def acquire_probe(tmp_path, monkeypatch, *, registry=None):
    opened = []

    def open_file(path):
        probe = StreamProbe(path.open("a+b"))
        opened.append(probe)
        return probe

    monkeypatch.setattr(module, "_open_file", open_file)
    registry = registry or InferenceLeaseRegistry("probe-generation")
    lease = registry.acquire_file(lease_path(tmp_path), "nn_state")
    return registry, lease, opened[0]


def assert_injected_fixture_clean(probe):
    # Exact original native stream only. A returned test-owned close is separate
    # from the registry's sticky unknown and never repairs its evidence.
    if not probe.native_close_returned:
        probe.native.close()
        probe.native_close_returned = True
    assert probe.native_close_returned and probe.native.closed


def test_actual_file_exclusion_positive_close_and_new_owner(tmp_path):
    path = lease_path(tmp_path)
    first, rejected = InferenceLeaseRegistry("first"), InferenceLeaseRegistry("rejected")
    later = InferenceLeaseRegistry("later")
    try:
        lease = first.acquire_file(path, "journal")
        assert lease.snapshot().lock_acquired and lease.snapshot().validation_complete
        with pytest.raises(OSError):
            rejected.acquire_file(path, "journal")
        failed = rejected.snapshot().leases[0]
        assert failed.acquisition_failed and not failed.lock_acquired
        assert not failed.validation_complete and failed.physical_status == "confirmed"
        assert lease.close().physical_status == "confirmed"
        replacement = later.acquire_file(path, "journal")
        assert replacement.snapshot().lock_acquired
    finally:
        cleanup(first)
        cleanup(rejected)
        cleanup(later)


def test_original_close_once_cached_success_and_immutable_sanitized_receipts(tmp_path, monkeypatch):
    registry, lease, probe = acquire_probe(tmp_path, monkeypatch)
    try:
        first = lease.close()
        again = lease.close()
        assert first.physical_status == again.physical_status == "confirmed"
        assert first.native_close_attempted and first.native_close_returned
        assert first.post_state_closed is True and probe.close_calls == 1
        assert first.lock_acquired and first.validation_complete
        assert not first.durable_write_confirmed and not first.actor_recovery_authorized
        assert not first.all_kernel_resources_confirmed
        with pytest.raises(FrozenInstanceError):
            first.physical_status = "unknown"
        text = json.dumps(asdict(registry.snapshot()))
        assert str(tmp_path) not in text and "secret" not in text and "fileno" not in text
        with pytest.raises(LeaseCleanupError, match="logically closed"):
            _ = lease.stream
    finally:
        cleanup(registry)
        assert_injected_fixture_clean(probe)


@pytest.mark.parametrize("stage", ["initialize", "write", "lock", "fstat"])
@pytest.mark.parametrize("cleanup_failure", [False, True])
def test_acquisition_primary_and_cleanup_failure_both_retained(
    tmp_path, monkeypatch, stage, cleanup_failure
):
    registry = InferenceLeaseRegistry("failed-acquisition")
    opened = []
    primary = RuntimeError("primary injected " + stage + " secret")

    class FailingWriteProbe(StreamProbe):
        def tell(self):
            return 0

        def write(self, value):
            raise primary

    def open_file(path):
        probe_type = FailingWriteProbe if stage == "write" else StreamProbe
        probe = probe_type(path.open("a+b"))
        probe.fail_after_native = cleanup_failure
        opened.append(probe)
        return probe

    def fail(*args):
        assert registry._leases[0]._original_stream is opened[0]
        raise primary

    monkeypatch.setattr(module, "_open_file", open_file)
    if stage == "initialize":
        monkeypatch.setattr(module, "_initialize_file", fail)
    elif stage == "lock":
        monkeypatch.setattr(module, "_acquire_lock", fail)
    elif stage == "fstat":
        monkeypatch.setattr(module.os, "fstat", fail)
    try:
        with pytest.raises(RuntimeError) as caught:
            registry.acquire_file(lease_path(tmp_path), "delivery")
        assert caught.value is primary
        lease = registry._leases[0]
        assert lease._original_stream is opened[0]
        receipt = lease.snapshot()
        assert receipt.acquisition_failed and not receipt.validation_complete
        assert receipt.lock_acquired is (stage == "fstat")
        diagnostics = tuple(lease._diagnostics)
        assert diagnostics[0].stage == "acquisition_failed"
        assert diagnostics[0].error_class == "RuntimeError"
        if cleanup_failure:
            assert receipt.destructive_unknown and receipt.physical_status == "unknown"
            assert diagnostics[-1].stage == "original_native_close_failed"
            assert diagnostics[-1].error_class == "OSError"
            assert not registry.close_all().all_confirmed
            assert opened[0].close_calls == 1
        else:
            assert receipt.physical_status == "confirmed"
            cleanup(registry)
        public = json.dumps(asdict(registry.snapshot()))
        assert "secret" not in public and str(tmp_path) not in public
        assert all(not isinstance(value, BaseException) for value in diagnostics)
    finally:
        assert len(opened) == 1
        assert_injected_fixture_clean(opened[0])


def test_stream_registered_before_close_capture_initialization_and_lock(tmp_path, monkeypatch):
    registry = InferenceLeaseRegistry("registered-first")
    original_initialize, original_lock = module._initialize_file, module._acquire_lock
    order = []

    def initialize(stream):
        assert registry._leases[0]._original_stream is stream
        order.append("initialize")
        original_initialize(stream)

    def lock(stream):
        assert registry._leases[0]._original_stream is stream
        order.append("lock")
        original_lock(stream)

    original_capture = module.FileLease._capture_close

    def capture(lease):
        assert registry._leases[0] is lease
        order.append("capture")
        original_capture(lease)

    monkeypatch.setattr(module, "_initialize_file", initialize)
    monkeypatch.setattr(module, "_acquire_lock", lock)
    monkeypatch.setattr(module.FileLease, "_capture_close", capture)
    try:
        registry.acquire_file(lease_path(tmp_path), "journal")
        assert order == ["capture", "initialize", "lock"]
    finally:
        cleanup(registry)


def test_close_capture_failure_keeps_original_without_invented_cleanup(tmp_path, monkeypatch):
    registry = InferenceLeaseRegistry("capture-failure")
    opened = []
    primary = RuntimeError("injected capture failure")

    def open_file(path):
        opened.append(StreamProbe(path.open("a+b")))
        return opened[-1]

    def fail_capture(lease):
        assert registry._leases[0]._original_stream is opened[0]
        raise primary

    monkeypatch.setattr(module, "_open_file", open_file)
    monkeypatch.setattr(module.FileLease, "_capture_close", fail_capture)
    try:
        with pytest.raises(RuntimeError) as caught:
            registry.acquire_file(lease_path(tmp_path), "delivery")
        assert caught.value is primary
        retained = registry._leases[0]
        assert retained._original_stream is opened[0]
        receipt = retained.snapshot()
        assert receipt.acquisition_failed and not receipt.native_close_attempted
        assert receipt.destructive_unknown and receipt.physical_status == "unknown"
        assert opened[0].close_calls == 0
        assert not registry.close_all().all_confirmed
    finally:
        assert_injected_fixture_clean(opened[0])


def test_destructive_after_real_close_failure_sticky_despite_later_noop(tmp_path, monkeypatch):
    registry, lease, probe = acquire_probe(tmp_path, monkeypatch)
    probe.fail_after_native = True
    try:
        with pytest.raises(OSError) as caught:
            lease.close()
        assert caught.value is probe.failure
        first = lease.snapshot()
        assert probe.native_close_returned and probe.native.closed
        assert first.native_close_attempted and not first.native_close_returned
        assert first.destructive_unknown and first.physical_status == "unknown"
        probe.fail_after_native = False
        for _ in range(3):
            with pytest.raises(LeaseCleanupError) as repeat:
                lease.close()
            assert repeat.value is not caught.value
        final = lease.snapshot()
        assert final.destructive_unknown and final.ever_uncertain
        assert final.physical_status == "unknown" and probe.close_calls == 1
        assert not registry.close_all().all_confirmed
    finally:
        assert_injected_fixture_clean(probe)


def test_preclose_observation_failure_retry_never_pretends_close_happened(tmp_path, monkeypatch):
    registry, lease, probe = acquire_probe(tmp_path, monkeypatch)
    try:
        probe.closed_faults = 1
        with pytest.raises(LeaseCleanupError, match="preclose"):
            lease.close()
        first = lease.snapshot()
        assert not first.native_close_attempted and probe.close_calls == 0
        assert first.ever_uncertain and not first.destructive_unknown
        final = lease.close()
        assert final.physical_status == "confirmed" and final.ever_uncertain
        assert probe.close_calls == 1
        assert first.history[0].code == "preclose_observation_failed"
    finally:
        cleanup(registry)


def test_postreturn_observation_failure_retry_only_observes_original(tmp_path, monkeypatch):
    class PostFaultProbe(StreamProbe):
        def close(self):
            super().close()
            self.closed_faults = 1

    opened = []

    def open_file(path):
        opened.append(PostFaultProbe(path.open("a+b")))
        return opened[-1]

    monkeypatch.setattr(module, "_open_file", open_file)
    registry = InferenceLeaseRegistry("post-return")
    try:
        lease = registry.acquire_file(lease_path(tmp_path), "journal")
        with pytest.raises(LeaseCleanupError, match="observation"):
            lease.close()
        first = lease.snapshot()
        assert first.native_close_returned and first.post_state_closed is None
        assert first.physical_status == "unknown" and not first.destructive_unknown
        final = lease.close()
        assert final.physical_status == "confirmed" and final.ever_uncertain
        assert opened[0].close_calls == 1
    finally:
        cleanup(registry)


@pytest.mark.parametrize("fault", ["external_close", "stream_replacement", "method_substitution"])
def test_untrusted_original_changes_cannot_gain_ack(tmp_path, monkeypatch, fault):
    registry, lease, probe = acquire_probe(tmp_path, monkeypatch)
    substitute = None
    try:
        if fault == "external_close":
            probe.native.close()
            probe.native_close_returned = True
        elif fault == "stream_replacement":
            substitute = lease_path(tmp_path).open("a+b")
            lease._stream = substitute
        else:
            probe.close = lambda: pytest.fail("substituted method must never run")
        with pytest.raises(LeaseCleanupError):
            lease.close()
        first = lease.snapshot()
        assert first.destructive_unknown and first.physical_status == "unknown"
        assert probe.close_calls == 0
        if substitute is not None:
            assert not substitute.closed
        assert not registry.close_all().all_confirmed
        assert probe.close_calls == 0
    finally:
        assert_injected_fixture_clean(probe)
        if substitute is not None:
            substitute.close()
            assert substitute.closed
        assert lease.snapshot().physical_status == "unknown"


@pytest.mark.parametrize("operation", ["acquire", "close", "close_all", "snapshot"])
def test_foreign_real_thread_hard_guard_preserves_all_state(tmp_path, monkeypatch, operation):
    registry, lease, probe = acquire_probe(tmp_path, monkeypatch)
    before = registry.snapshot()
    failures = []

    def foreign():
        try:
            if operation == "acquire":
                registry.acquire_file(lease_path(tmp_path), "delivery")
            elif operation == "close":
                lease.close()
            elif operation == "close_all":
                registry.close_all()
            else:
                registry.snapshot()
        except BaseException as error:
            failures.append(error)

    thread = threading.Thread(target=foreign)
    try:
        thread.start()
        thread.join(1)
        assert not thread.is_alive()
        assert len(failures) == 1 and type(failures[0]) is RuntimeError
        assert registry.snapshot() == before and probe.close_calls == 0
    finally:
        cleanup(registry)


@pytest.mark.parametrize("fault", ["pid", "different_thread_same_ident"])
def test_pid_and_thread_object_fences_not_just_numeric_ident(tmp_path, monkeypatch, fault):
    registry, lease, probe = acquire_probe(tmp_path, monkeypatch)
    before = registry.snapshot()
    with monkeypatch.context() as patch:
        if fault == "pid":
            patch.setattr(module.os, "getpid", lambda: registry._pid + 1)
        else:

            class OtherThread:
                ident = threading.get_ident()

            patch.setattr(module.threading, "current_thread", lambda: OtherThread())
        with pytest.raises(RuntimeError, match="Thread object"):
            lease.close()
    try:
        assert registry.snapshot() == before and probe.close_calls == 0
    finally:
        cleanup(registry)


@pytest.mark.parametrize("generation", [None, True, 1, "", "bad space", "a" * 129, "한글"])
def test_generation_rejected_without_open(generation, monkeypatch):
    monkeypatch.setattr(module, "_open_file", lambda _: pytest.fail("must not open"))
    with pytest.raises(ValueError):
        InferenceLeaseRegistry(generation)


@pytest.mark.parametrize("capacity", [True, 0, -1, 65, 1.0, "16"])
def test_capacity_requires_bounded_native_integer(capacity, monkeypatch):
    monkeypatch.setattr(module, "_open_file", lambda _: pytest.fail("must not open"))
    with pytest.raises(ValueError):
        InferenceLeaseRegistry("generation", capacity=capacity)


@pytest.mark.parametrize("role", [None, True, 1, "Journal", "source_read", "journal "])
def test_role_rejection_precedes_open_and_registry_mutation(tmp_path, monkeypatch, role):
    registry = InferenceLeaseRegistry("generation")
    before = registry.snapshot()
    monkeypatch.setattr(module, "_open_file", lambda _: pytest.fail("must not open"))
    with pytest.raises(ValueError):
        registry.acquire_file(lease_path(tmp_path), role)
    assert registry.snapshot() == before


def test_capacity_unknown_entries_and_reserved_slot_never_evicted(tmp_path, monkeypatch):
    registry = InferenceLeaseRegistry("capacity", capacity=1)
    real_open = module._open_file
    nested = []

    def open_file(path):
        with pytest.raises(LeaseCleanupError, match="capacity"):
            registry.acquire_file(lease_path(tmp_path), "delivery")
        nested.append(registry.snapshot().reserved_slots)
        return real_open(path)

    monkeypatch.setattr(module, "_open_file", open_file)
    try:
        lease = registry.acquire_file(lease_path(tmp_path), "journal")
        assert nested == [1]
        with pytest.raises(LeaseCleanupError, match="capacity"):
            registry.acquire_file(lease_path(tmp_path), "delivery")
        assert lease.close().physical_status == "confirmed"
        # Even confirmed originals stay in this finite registry; no hidden eviction.
        with pytest.raises(LeaseCleanupError, match="capacity"):
            registry.acquire_file(lease_path(tmp_path), "delivery")
    finally:
        cleanup(registry)


def test_close_all_attempts_independent_leases_and_keeps_unknown_originals(tmp_path, monkeypatch):
    registry = InferenceLeaseRegistry("independent", capacity=3)
    registry, failed, first = acquire_probe(tmp_path, monkeypatch, registry=registry)
    registry, good, second = acquire_probe(tmp_path, monkeypatch, registry=registry)
    first.fail_after_native = True
    try:
        result = registry.close_all()
        assert result.admission_closed and not result.all_confirmed
        assert result.leases[0].physical_status == "unknown"
        assert result.leases[1].physical_status == "confirmed"
        assert result.errors == ((failed.token, "original_lease_cleanup_unconfirmed"),)
        assert failed._original_stream is first and good._original_stream is second
        assert first.close_calls == second.close_calls == 1
        with pytest.raises(LeaseCleanupError, match="admission"):
            registry.acquire_file(lease_path(tmp_path), "delivery")
        assert registry.snapshot().errors == result.errors
        assert not registry.close_all().all_confirmed
        assert first.close_calls == second.close_calls == 1
    finally:
        assert_injected_fixture_clean(first)
        assert_injected_fixture_clean(second)


def test_unknown_original_consumes_capacity_without_new_open(tmp_path, monkeypatch):
    registry = InferenceLeaseRegistry("held-capacity", capacity=1)
    registry, lease, probe = acquire_probe(tmp_path, monkeypatch, registry=registry)
    probe.fail_after_native = True
    try:
        with pytest.raises(OSError):
            lease.close()
        monkeypatch.setattr(module, "_open_file", lambda _: pytest.fail("must not open"))
        with pytest.raises(LeaseCleanupError, match="capacity"):
            registry.acquire_file(lease_path(tmp_path), "delivery")
        assert registry._leases == [lease]
        assert lease._original_stream is probe
        assert lease.snapshot().physical_status == "unknown"
    finally:
        assert_injected_fixture_clean(probe)


def test_snapshots_do_not_probe_and_history_and_diagnostics_are_bounded(tmp_path, monkeypatch):
    registry, lease, probe = acquire_probe(tmp_path, monkeypatch)
    try:
        probe.closed_faults = module.HISTORY_LIMIT + 5
        for _ in range(module.HISTORY_LIMIT + 5):
            reads = probe.closed_reads
            before = lease.snapshot()
            assert registry.snapshot().leases[0] == before
            assert probe.closed_reads == reads
            with pytest.raises(LeaseCleanupError):
                lease.close()
        failed = lease.snapshot()
        assert len(failed.history) == len(lease._diagnostics) == module.HISTORY_LIMIT
        assert failed.attempt == module.HISTORY_LIMIT + 5
        assert not failed.native_close_attempted and probe.close_calls == 0
        assert all(len(item.text) <= 160 for item in lease._diagnostics)
        final = lease.close()
        assert final.physical_status == "confirmed" and final.ever_uncertain
        assert len(final.history) == module.HISTORY_LIMIT and probe.close_calls == 1
    finally:
        cleanup(registry)


def test_no_resource_close_is_positive_but_snapshot_cannot_close_admission(tmp_path):
    registry = InferenceLeaseRegistry("empty")
    assert not registry.snapshot().all_confirmed
    result = registry.close_all()
    assert result.all_confirmed and not result.leases and result.admission_closed
    with pytest.raises(LeaseCleanupError, match="admission"):
        registry.acquire_file(lease_path(tmp_path), "journal")


@pytest.mark.parametrize("fault", ["parent_traversal", "directory", "ancestor_bound"])
def test_invalid_paths_fail_before_open(tmp_path, monkeypatch, fault):
    registry = InferenceLeaseRegistry("paths")
    path = lease_path(tmp_path)
    if fault == "parent_traversal":
        path = tmp_path / ".." / "not-opened"
    elif fault == "directory":
        path = tmp_path
    else:
        path = tmp_path.joinpath(*(["a"] * 128))
    monkeypatch.setattr(module, "_open_file", lambda _: pytest.fail("must not open"))
    with pytest.raises(ValueError):
        registry.acquire_file(path, "journal")
    assert not registry.snapshot().leases and registry.snapshot().reserved_slots == 0


def test_reparse_path_rejected_before_open(tmp_path, monkeypatch):
    registry = InferenceLeaseRegistry("reparse-path")
    path = lease_path(tmp_path)
    real_lstat = Path.lstat

    def lstat(value, *args, **kwargs):
        if value == path:
            return SimpleNamespace(st_mode=module.stat.S_IFREG, st_file_attributes=0x400)
        return real_lstat(value, *args, **kwargs)

    monkeypatch.setattr(Path, "lstat", lstat)
    monkeypatch.setattr(module, "_open_file", lambda _: pytest.fail("must not open"))
    with pytest.raises(ValueError, match="reparse"):
        registry.acquire_file(path, "journal")
    assert not registry.snapshot().leases


@pytest.mark.parametrize("fault", ["nonregular", "reparse"])
def test_opened_file_shape_validation_failure_still_closes_original(tmp_path, monkeypatch, fault):
    registry = InferenceLeaseRegistry("opened-shape")
    details = SimpleNamespace(
        st_mode=module.stat.S_IFDIR if fault == "nonregular" else module.stat.S_IFREG,
        st_file_attributes=0x400 if fault == "reparse" else 0,
    )
    monkeypatch.setattr(module.os, "fstat", lambda _: details)
    try:
        with pytest.raises(ValueError, match="opened"):
            registry.acquire_file(lease_path(tmp_path), "journal")
        retained = registry.snapshot().leases[0]
        assert retained.lock_acquired and not retained.validation_complete
        assert retained.acquisition_failed and retained.physical_status == "confirmed"
    finally:
        cleanup(registry)


def test_open_never_returning_object_failure_has_no_invented_resource(tmp_path, monkeypatch):
    registry = InferenceLeaseRegistry("open-failure")
    primary = OSError("injected open failure")

    def fail(path):
        raise primary

    monkeypatch.setattr(module, "_open_file", fail)
    with pytest.raises(OSError) as caught:
        registry.acquire_file(lease_path(tmp_path), "journal")
    assert caught.value is primary
    # No accessible stream is invented, but entering open is not proof that no
    # native resource was created. Its preregistered unknown placeholder remains.
    snapshot = registry.snapshot()
    assert len(snapshot.leases) == 1 and snapshot.reserved_slots == 0
    receipt = snapshot.leases[0]
    assert receipt.open_attempted and not receipt.open_returned
    assert receipt.acquisition_unknown and receipt.physical_status == "unknown"
    assert not receipt.native_close_attempted and not receipt.native_close_returned
    assert registry._leases[0]._original_stream is None
    assert snapshot.acquisition_unknown and snapshot.admission_held
    assert snapshot.admission_closed and not registry.close_all().all_confirmed
    assert not registry.close_all().all_confirmed
    with pytest.raises(LeaseCleanupError, match="admission"):
        registry.acquire_file(lease_path(tmp_path), "delivery")


def test_open_creates_fixture_stream_then_raises_without_transferring_it(tmp_path, monkeypatch):
    registry = InferenceLeaseRegistry("unobserved-return")
    primary = OSError("deliberate open wrapper failure after fixture native open")
    fixture_streams = []

    def open_then_fail(path):
        assert len(registry._leases) == 1
        assert registry._leases[0]._original_stream is None
        assert registry.snapshot().reserved_slots == 1
        fixture_streams.append(path.open("a+b"))
        raise primary

    monkeypatch.setattr(module, "_open_file", open_then_fail)
    try:
        with pytest.raises(OSError) as caught:
            registry.acquire_file(lease_path(tmp_path), "journal")
        assert caught.value is primary
        snapshot = registry.close_all()
        assert snapshot.admission_held and snapshot.acquisition_unknown
        assert not snapshot.all_confirmed
        assert registry._leases[0]._original_stream is None
        assert not snapshot.leases[0].native_close_attempted
        assert len(fixture_streams) == 1 and not fixture_streams[0].closed
    finally:
        # This deliberately hidden stream belongs to the fault fixture. Its
        # explicit native close cannot give the registry an original-object ACK.
        fixture_streams[0].close()
        assert fixture_streams[0].closed
        assert not registry.close_all().all_confirmed


def test_proven_mkdir_failure_before_open_is_distinct_from_unknown_open(tmp_path, monkeypatch):
    registry = InferenceLeaseRegistry("preopen-mkdir")
    primary = OSError("deliberate mkdir failure before open")

    def fail_mkdir(*args, **kwargs):
        raise primary

    monkeypatch.setattr(module, "_open_file", lambda _: pytest.fail("must not open"))
    with monkeypatch.context() as patch:
        patch.setattr(Path, "mkdir", fail_mkdir)
        with pytest.raises(OSError) as caught:
            registry.acquire_file(lease_path(tmp_path), "journal")
    assert caught.value is primary
    snapshot = registry.close_all()
    receipt = snapshot.leases[0]
    assert snapshot.all_confirmed and not snapshot.acquisition_unknown
    assert receipt.acquisition_failed and receipt.physical_status == "not_opened"
    assert not receipt.open_attempted and not receipt.open_returned
    assert not receipt.native_close_attempted and not receipt.native_close_returned
    assert not receipt.lock_acquired and not receipt.validation_complete
    assert registry._leases[0]._original_stream is None


@pytest.mark.parametrize("append_before_failure", [False, True])
def test_placeholder_registration_failure_precedes_open(
    tmp_path, monkeypatch, append_before_failure
):
    registry = InferenceLeaseRegistry("registration-failure", capacity=1)
    primary = MemoryError("deliberate placeholder append failure")

    class FailAppend(list):
        def append(self, value):
            if append_before_failure:
                super().append(value)
            raise primary

    registry._leases = FailAppend()
    monkeypatch.setattr(module, "_open_file", lambda _: pytest.fail("must not open"))
    with pytest.raises(MemoryError) as caught:
        registry.acquire_file(lease_path(tmp_path), "journal")
    assert caught.value is primary
    result = registry.close_all()
    assert result.all_confirmed and result.reserved_slots == 0
    assert not result.acquisition_unknown
    if append_before_failure:
        assert len(result.leases) == 1
        assert result.leases[0].physical_status == "not_opened"
        assert not result.leases[0].native_close_attempted
        assert registry._leases[0]._original_stream is None
    else:
        assert not result.leases


@pytest.mark.parametrize("when", ["native_close", "post_observation"])
def test_same_owner_reentry_cannot_call_twice_or_clear_unknown(tmp_path, monkeypatch, when):
    registry = InferenceLeaseRegistry("reentrant-close")
    state = {"lease": None, "reentered": False, "errors": []}
    opened = []

    def reenter():
        state["reentered"] = True
        try:
            state["lease"].close()
        except LeaseCleanupError as error:
            state["errors"].append(error)

    class ReentrantProbe(StreamProbe):
        def close(self):
            if when == "native_close":
                reenter()
            super().close()

        @property
        def closed(self):
            if when == "post_observation" and self.native_close_returned and not state["reentered"]:
                reenter()
            return super().closed

    def open_file(path):
        opened.append(ReentrantProbe(path.open("a+b")))
        return opened[-1]

    monkeypatch.setattr(module, "_open_file", open_file)
    try:
        lease = registry.acquire_file(lease_path(tmp_path), "journal")
        state["lease"] = lease
        with pytest.raises(LeaseCleanupError):
            lease.close()
        first = lease.snapshot()
        assert state["reentered"] and len(state["errors"]) == 1
        assert opened[0].native_close_returned and opened[0].native.closed
        assert opened[0].close_calls == 1
        assert first.native_close_returned and first.destructive_unknown
        assert first.physical_status == "unknown"
        assert any(event.code == "original_close_reentered" for event in first.history)
        with pytest.raises(LeaseCleanupError):
            lease.close()
        assert not registry.close_all().all_confirmed
        assert opened[0].close_calls == 1
        assert lease.snapshot().physical_status == "unknown"
    finally:
        assert_injected_fixture_clean(opened[0])


def test_interruption_after_native_return_before_bookkeeping_is_sticky(tmp_path, monkeypatch):
    registry, lease, probe = acquire_probe(tmp_path, monkeypatch)
    lines, first_line = inspect.getsourcelines(module.FileLease._close_once)
    target_line = first_line + next(
        index for index, line in enumerate(lines) if line.strip() == "self._returned = True"
    )
    interrupted = KeyboardInterrupt("deliberate Python bookkeeping interruption")
    original_trace = sys.gettrace()
    hits = []

    def trace(frame, event, arg):
        if (
            frame.f_code is module.FileLease._close_once.__code__
            and event == "line"
            and frame.f_lineno == target_line
        ):
            hits.append(True)
            raise interrupted
        return trace

    try:
        try:
            sys.settrace(trace)
            with pytest.raises(KeyboardInterrupt) as caught:
                lease.close()
        finally:
            sys.settrace(original_trace)
        assert caught.value is interrupted and hits == [True]
        assert probe.close_calls == 1 and probe.native_close_returned and probe.native.closed
        first = lease.snapshot()
        assert first.native_close_attempted and not first.native_close_returned
        assert first.destructive_unknown and first.physical_status == "unknown"
        with pytest.raises(LeaseCleanupError):
            lease.close()
        assert not registry.close_all().all_confirmed
        assert probe.close_calls == 1
    finally:
        sys.settrace(original_trace)
        assert_injected_fixture_clean(probe)


def test_attempt_mark_without_return_state_never_invokes_native_again(tmp_path, monkeypatch):
    registry, lease, probe = acquire_probe(tmp_path, monkeypatch)
    # Deliberate bookkeeping-state fixture, not an asynchronous OS fault claim.
    lease._attempted = True
    try:
        with pytest.raises(LeaseCleanupError):
            lease.close()
        assert probe.close_calls == 0
        result = lease.snapshot()
        assert result.destructive_unknown and result.physical_status == "unknown"
        assert result.native_close_attempted and not result.native_close_returned
        assert not registry.close_all().all_confirmed
        assert probe.close_calls == 0
    finally:
        assert_injected_fixture_clean(probe)
        assert lease.snapshot().physical_status == "unknown"


def test_generation_role_and_tokens_are_read_only_and_registry_specific(tmp_path):
    first, second = InferenceLeaseRegistry("same"), InferenceLeaseRegistry("same")
    try:
        a = first.acquire_file(lease_path(tmp_path), "journal")
        b = second.acquire_file(lease_path(tmp_path), "journal")
        assert first.token != second.token and a.token != b.token
        assert a.snapshot().registry_token == first.token
        for target, name in ((first, "generation"), (first, "token"), (a, "role"), (a, "token")):
            with pytest.raises(AttributeError):
                setattr(target, name, "replacement")
    finally:
        cleanup(first)
        cleanup(second)
