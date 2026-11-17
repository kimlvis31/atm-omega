"""Actual temporary state files with deliberate lease faults; no actor recovery."""

import json
import threading
from dataclasses import FrozenInstanceError, asdict
from types import SimpleNamespace
from uuid import uuid4

import pytest

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.inference import CAUSAL_SCOPE, digest, encoded
from omega.runtime import causal_inference_leases as file_module
from omega.runtime import causal_nn as store_module
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_store_leases import (
    InferenceStoreAlreadyOwnedError,
    InferenceStoreConstructionError,
    InferenceStoreLeases,
    StoreLeaseSnapshot,
)


def state_path(tmp_path):
    return tmp_path / (uuid4().hex + ".json")


def state(revision=0):
    return {"scope": CAUSAL_SCOPE, "revision": revision, "state": [revision, "1.2300"]}


def close_confirmed(store):
    value = store.close()
    assert value.file_lease_confirmed, asdict(value)
    assert store.lock.released and store.lock.stream.closed
    return value


class StreamProbe:
    """Actual original temp stream; Python faults do not simulate real OS proof."""

    def __init__(self, native):
        self.native = native
        self.close_calls = self.closed_faults = 0
        self.fail_close = self.post_close_fault = False
        self.native_close_returned = False

    def __getattr__(self, name):
        return getattr(self.native, name)

    @property
    def closed(self):
        if self.closed_faults:
            self.closed_faults -= 1
            raise OSError("deliberate observation secret")
        return self.native.closed

    def close(self):
        self.close_calls += 1
        self.native.close()
        self.native_close_returned = True
        if self.post_close_fault:
            self.closed_faults = 1
        if self.fail_close:
            raise OSError("deliberate native-return wrapper secret-path/secret-token")


def probe_factory(monkeypatch):
    probes = []

    def open_file(path):
        probe = StreamProbe(path.open("a+b"))
        probes.append(probe)
        return probe

    monkeypatch.setattr(file_module, "_open_file", open_file)
    return probes


def assert_fixture_native_cleanup(probe):
    # Positive fact from the exact original native stream. This does not alter
    # any registry/binding's unknown evidence or authorize restoration.
    if not probe.native_close_returned:
        probe.native.close()
        probe.native_close_returned = True
    assert probe.native_close_returned and probe.native.closed


def test_actual_store_exclusion_positive_repeat_close_and_new_owner(tmp_path):
    path = state_path(tmp_path)
    first = FileInferenceStateStore(path)
    rejected_scope = None
    try:
        with pytest.raises(RuntimeError, match="already owns") as caught:
            FileInferenceStateStore(path)
        assert isinstance(caught.value, StorageError)
        assert type(caught.value) is InferenceStoreAlreadyOwnedError
        rejected_scope = caught.value.lease_scope
        rejected = rejected_scope.bindings[0]
        assert rejected.original_store is not first
        assert rejected.snapshot().construction_failed
        assert not rejected.snapshot().file.lock_acquired
        assert rejected.snapshot().file_lease_confirmed
        original_stream = first.lock.stream
        before = first.retirement_status()
        assert before.construction_complete and not before.file_lease_confirmed
        assert first.close().file_lease_confirmed
        assert first.close().file_lease_confirmed and original_stream.closed
        later = FileInferenceStateStore(path)
        close_confirmed(later)
    finally:
        close_confirmed(first)
        if rejected_scope is not None:
            assert rejected_scope.close_all().all_confirmed


def test_original_json_origin_cas_and_verified_ack_remain_exact(tmp_path, monkeypatch):
    path = state_path(tmp_path)
    store = FileInferenceStateStore(path)
    try:
        snapshot = state()
        ack = store.save_snapshot(encoded(snapshot), expected_revision=None)
        envelope = {
            "schema_version": 1,
            "revision": 0,
            "snapshot_sha256": digest(snapshot),
            "snapshot": snapshot,
        }
        expected = json.dumps(envelope, ensure_ascii=False, allow_nan=False, sort_keys=True).encode(
            "utf-8"
        )
        assert path.read_bytes() == expected
        anchor = {
            "schema_version": 1,
            "file_id": digest(path.name),
            "initial_revision": 0,
            "initial_sha256": digest(snapshot),
        }
        assert store.anchor_path.read_bytes() == json.dumps(
            {"data": anchor, "sha256": digest(anchor)},
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
        assert store.read() == encoded(snapshot)
        assert store.retirement_status().last_ack is ack
        assert (ack.revision, ack.snapshot_sha256, ack.durable) == (0, digest(snapshot), True)
        monkeypatch.setattr(store_module, "atomic_json", lambda *_: pytest.fail("no rewrite"))
        same = store.save_snapshot(encoded(snapshot), expected_revision=None)
        assert store.retirement_status().last_ack is same
        assert path.read_bytes() == expected
        with pytest.raises(ProtocolError, match="changed"):
            store.save_snapshot(encoded(state(1)), expected_revision=3)
        assert store.retirement_status().last_ack is same
        assert path.read_bytes() == expected
    finally:
        close_confirmed(store)


def test_store_registered_before_path_conversion_or_constructor_filesystem_work(tmp_path):
    scope = InferenceStoreLeases("before-io")
    path = state_path(tmp_path)
    seen = []

    class OriginalPath:
        def __fspath__(self):
            assert len(scope.bindings) == 1
            binding = scope.bindings[0]
            seen.append(binding.original_store)
            assert binding.role == "journal"
            assert binding.snapshot().no_file_resource
            return str(path)

    store = FileInferenceStateStore(OriginalPath(), leases=scope, role="journal")
    try:
        assert seen and all(value is store for value in seen)
        assert scope.binding_for(store).original_store is store
    finally:
        close_confirmed(store)


@pytest.mark.parametrize("private", [False, True])
def test_constructor_primary_and_cleanup_failure_retained_before_assignment(
    tmp_path, monkeypatch, private
):
    scope = None if private else InferenceStoreLeases("failed-constructor")
    probes = probe_factory(monkeypatch)
    primary = RuntimeError("constructor primary secret")
    original_open = file_module._open_file

    def open_file(path):
        probe = original_open(path)
        probe.fail_close = True
        return probe

    def fail_validation(*args):
        raise primary

    monkeypatch.setattr(file_module, "_open_file", open_file)
    monkeypatch.setattr(file_module, "_validate_open_file", fail_validation)
    assigned = None
    try:
        with pytest.raises(InferenceStoreConstructionError) as caught:
            assigned = FileInferenceStateStore(state_path(tmp_path), leases=scope)
        assert assigned is None and "secret" not in str(caught.value)
        retained_scope = caught.value.lease_scope
        if scope is not None:
            assert retained_scope is scope
        binding = retained_scope.bindings[0]
        original = binding.original_store
        receipt = original.retirement_status()
        assert original.closed and receipt.construction_failed
        assert not receipt.construction_complete and not receipt.file_lease_confirmed
        assert receipt.primary_failure == "inference_store_construction_failed"
        assert receipt.cleanup_failure == "inference_store_cleanup_unconfirmed"
        assert receipt.file.destructive_unknown and receipt.file.lock_acquired
        assert binding._lease is None  # Alpha failed before this assignment returned.
        assert binding._registry._leases[0]._original_stream is probes[0]
        assert ("constructor", "RuntimeError") in binding._diagnostics
        assert probes[0].close_calls == 1
        for _ in range(2):
            with pytest.raises(StorageError, match="cleanup"):
                original.close()
        assert probes[0].close_calls == 1
        assert "secret" not in json.dumps(asdict(retained_scope.snapshot()))
        assert not retained_scope.close_all().all_confirmed
    finally:
        assert_fixture_native_cleanup(probes[0])


def test_private_open_failure_exposes_original_scope_and_unknown_placeholder(tmp_path, monkeypatch):
    def fail_open(path):
        raise OSError("deliberate pre-native fixture error; application cannot prove absence")

    monkeypatch.setattr(file_module, "_open_file", fail_open)
    with pytest.raises(InferenceStoreConstructionError) as caught:
        FileInferenceStateStore(state_path(tmp_path))
    scope = caught.value.lease_scope
    binding = scope.bindings[0]
    status = binding.snapshot()
    assert status.file_acquisition_unknown and not status.file_lease_confirmed
    assert status.file.open_attempted and not status.file.open_returned
    assert binding._registry._leases[0]._original_stream is None
    assert binding.original_store.closed and not scope.close_all().all_confirmed


@pytest.mark.parametrize("kind", [KeyboardInterrupt, SystemExit])
def test_constructor_interrupt_retains_same_family_object_and_scope(tmp_path, monkeypatch, kind):
    probes = probe_factory(monkeypatch)
    primary = kind("deliberate constructor interruption")

    def interrupted(*args):
        raise primary

    monkeypatch.setattr(file_module, "_validate_open_file", interrupted)
    try:
        with pytest.raises(kind) as caught:
            FileInferenceStateStore(state_path(tmp_path))
        assert caught.value is primary
        scope = caught.value.lease_scope
        receipt = scope.bindings[0].snapshot()
        assert receipt.construction_failed and receipt.file_lease_confirmed
        assert receipt.primary_failure and not receipt.recording_failed
        assert scope.close_all().all_confirmed
        assert probes[0].close_calls == 1
    finally:
        assert_fixture_native_cleanup(probes[0])


def test_custom_interrupt_refusing_scope_gets_fixed_retained_scope_fallback(tmp_path, monkeypatch):
    class RefusingInterrupt(BaseException):
        def __setattr__(self, name, value):
            if name == "lease_scope":
                raise TypeError("deliberate attribute refusal")
            super().__setattr__(name, value)

    def interrupted(*args):
        raise RefusingInterrupt("must not escape in public error text")

    monkeypatch.setattr(file_module, "_initialize_file", interrupted)
    with pytest.raises(InferenceStoreConstructionError) as caught:
        FileInferenceStateStore(state_path(tmp_path))
    scope = caught.value.lease_scope
    assert "escape" not in str(caught.value)
    assert ("constructor", "RefusingInterrupt") in scope.bindings[0]._diagnostics
    assert scope.close_all().all_confirmed


def test_post_alpha_constructor_fstat_failure_keeps_successful_original_file(tmp_path, monkeypatch):
    scope = InferenceStoreLeases("store-validation")
    original_fstat, calls = store_module.os.fstat, []

    def fstat(descriptor):
        calls.append(True)
        if len(calls) == 2:
            raise OSError("deliberate store validation secret")
        return original_fstat(descriptor)

    monkeypatch.setattr(store_module.os, "fstat", fstat)
    with pytest.raises(InferenceStoreConstructionError):
        FileInferenceStateStore(state_path(tmp_path), leases=scope)
    binding = scope.bindings[0]
    assert len(calls) == 2
    status = binding.snapshot()
    assert status.construction_failed and not status.construction_complete
    assert status.file.validation_complete and status.file_lease_confirmed
    assert binding.original_store.lock.stream.closed
    assert scope.close_all().all_confirmed


@pytest.mark.parametrize("fault", ["native_after_return", "pre_observation", "post_observation"])
def test_logical_close_retries_preserve_original_evidence(tmp_path, monkeypatch, fault):
    probes = probe_factory(monkeypatch)
    store = FileInferenceStateStore(state_path(tmp_path))
    probe = probes[0]
    if fault == "native_after_return":
        probe.fail_close = True
    elif fault == "pre_observation":
        probe.closed_faults = 1
    else:
        probe.post_close_fault = True
    try:
        with pytest.raises(StorageError, match="cleanup") as caught:
            store.close()
        assert "secret" not in str(caught.value)
        assert store.closed and not store.lock.released
        first = store.retirement_status()
        assert not first.file_lease_confirmed and not first.recording_failed
        if fault == "native_after_return":
            probe.fail_close = False
            for _ in range(2):
                with pytest.raises(StorageError, match="cleanup"):
                    store.close()
            assert probe.close_calls == 1 and first.file.destructive_unknown
            assert not store.retirement_status().file_lease_confirmed
        else:
            if fault == "pre_observation":
                assert probe.close_calls == 0 and not first.file.native_close_attempted
            else:
                assert probe.close_calls == 1 and first.file.native_close_returned
            final = store.close()
            assert final.file_lease_confirmed and final.file.ever_uncertain
            assert final.cleanup_failure == "inference_store_cleanup_unconfirmed"
            assert probe.close_calls == 1 and store.lock.released
    finally:
        assert_fixture_native_cleanup(probe)


@pytest.mark.parametrize("fault", ["lock_view", "store_type"])
def test_public_substitution_never_closes_replacement_or_gains_evidence(tmp_path, fault):
    store = FileInferenceStateStore(state_path(tmp_path))
    original_lock, replacement_calls = store.lock, []

    class ChangedStore(FileInferenceStateStore):
        pass

    if fault == "lock_view":
        store.lock = SimpleNamespace(close=lambda: replacement_calls.append(True), released=True)
    else:
        store.__class__ = ChangedStore
    try:
        with pytest.raises(StorageError):
            store.close()
        assert replacement_calls == []
        status = store.retirement_status()
        assert not status.identity_intact and not status.file_lease_confirmed
        assert status.file.native_close_returned and status.file.post_state_closed
        assert original_lock.stream.closed  # Actual original cleanup, not replacement.
        store.lock = original_lock
        store.__class__ = FileInferenceStateStore
        with pytest.raises(StorageError):
            store.close()
        assert not store.retirement_status().file_lease_confirmed
    finally:
        assert original_lock.stream.closed


def test_compatibility_lock_close_fences_store_without_recursive_store_close(tmp_path, monkeypatch):
    store = FileInferenceStateStore(state_path(tmp_path))
    view = store.lock
    monkeypatch.setattr(store, "close", lambda: pytest.fail("must use captured admission fence"))
    receipt = view.close()
    assert receipt.file_lease_confirmed and store.closed and view.released
    with pytest.raises(StorageError):
        store.read()
    assert view.close().file_lease_confirmed


def test_external_native_stream_close_blocks_read_save_and_never_gains_ack(tmp_path):
    store = FileInferenceStateStore(state_path(tmp_path))
    ack = store.save_snapshot(encoded(state()), expected_revision=None)
    before = store.path.read_bytes()
    original_stream = store.lock.stream
    # Deliberate direct close has positive fixture cleanup evidence, but no
    # trusted alpha callback ACK. It must not leave a writable unleased store.
    original_stream.close()
    assert original_stream.closed
    with pytest.raises(StorageError, match="ownership"):
        store.read()
    with pytest.raises(StorageError):
        store.save_snapshot(encoded(state(1)), expected_revision=0)
    assert store.path.read_bytes() == before
    assert store.retirement_status().last_ack is ack
    with pytest.raises(StorageError):
        store.close()
    result = store.retirement_status()
    assert not result.file_lease_confirmed and result.file.destructive_unknown
    assert not result.file.native_close_returned and original_stream.closed


@pytest.mark.parametrize("after_write", [False, True])
def test_last_verified_ack_and_recording_failure_are_separate_from_close(
    tmp_path, monkeypatch, after_write
):
    store = FileInferenceStateStore(state_path(tmp_path))
    ack = store.save_snapshot(encoded(state()), expected_revision=None)
    original_write = store_module.atomic_json

    def uncertain(path, payload):
        if after_write:
            original_write(path, payload)
        raise OSError("deliberate write acknowledgement secret")

    monkeypatch.setattr(store_module, "atomic_json", uncertain)
    try:
        with pytest.raises(StorageError, match="unconfirmed"):
            store.save_snapshot(encoded(state(1)), expected_revision=0)
        assert store.failed
        assert store.read() == encoded(state(1 if after_write else 0))
        assert store.failed and store.retirement_status().last_ack is ack
        with pytest.raises(StorageError, match="unknown"):
            store.save_snapshot(encoded(state(1)), expected_revision=0)
        receipt = close_confirmed(store)
        assert receipt.recording_failed and receipt.last_ack is ack
        assert not receipt.actor_recovery_authorized
        assert receipt.last_ack.revision == 0
    finally:
        close_confirmed(store)


def test_one_store_close_does_not_close_other_original_bindings(tmp_path, monkeypatch):
    scope = InferenceStoreLeases("independent")
    probes = probe_factory(monkeypatch)
    journal = FileInferenceStateStore(state_path(tmp_path), leases=scope, role="journal")
    state_store = FileInferenceStateStore(state_path(tmp_path), leases=scope, role="nn_state")
    delivery = FileInferenceStateStore(state_path(tmp_path), leases=scope, role="delivery")
    probes[1].fail_close = True
    before = scope.bindings
    try:
        with pytest.raises(StorageError):
            state_store.close()
        assert not journal.closed and not delivery.closed
        assert probes[0].close_calls == probes[2].close_calls == 0
        assert scope.bindings is before
        close_confirmed(journal)
        close_confirmed(delivery)
        result = scope.close_all()  # Explicit test orchestration, not actor release policy.
        assert not result.all_confirmed and len(result.bindings) == 3
        assert [probe.close_calls for probe in probes] == [1, 1, 1]
        assert result.errors == (
            (scope.binding_for(state_store).token, "inference_store_cleanup_unconfirmed"),
        )
    finally:
        for probe in probes:
            assert_fixture_native_cleanup(probe)


@pytest.mark.parametrize("operation", ["read", "save", "close", "snapshot", "scope_close"])
def test_foreign_real_thread_guards_before_state_or_filesystem_mutation(
    tmp_path, monkeypatch, operation
):
    store = FileInferenceStateStore(state_path(tmp_path))
    scope = store.lease_scope
    before = scope.snapshot()
    failures = []

    def foreign():
        try:
            if operation == "read":
                store.read()
            elif operation == "save":
                store.save_snapshot(encoded(state()), expected_revision=None)
            elif operation == "close":
                store.close()
            elif operation == "snapshot":
                store.retirement_status()
            else:
                scope.close_all()
        except BaseException as error:
            failures.append(error)

    with monkeypatch.context() as patch:
        patch.setattr(
            store_module, "validate_artifact_path", lambda *_args, **_kw: pytest.fail("no IO")
        )
        thread = threading.Thread(target=foreign)
        thread.start()
        thread.join(1)
        assert not thread.is_alive()
        assert len(failures) == 1 and isinstance(failures[0], StorageError)
        assert scope.snapshot() == before
    close_confirmed(store)
    closed = scope.snapshot()
    thread = threading.Thread(target=foreign)
    thread.start()
    thread.join(1)
    assert not thread.is_alive() and scope.snapshot() == closed


def test_thread_object_and_pid_fences_not_just_numeric_ident(tmp_path, monkeypatch):
    store = FileInferenceStateStore(state_path(tmp_path))
    before = store.retirement_status()
    for fault in ("pid", "thread"):
        with monkeypatch.context() as patch:
            if fault == "pid":
                patch.setattr(store_module.os, "getpid", lambda: store._pid + 1)
            else:
                fake = SimpleNamespace(ident=threading.get_ident())
                patch.setattr(store_module.threading, "current_thread", lambda value=fake: value)
            with pytest.raises(StorageError, match="Thread object"):
                store.close()
        assert store.retirement_status() == before
    close_confirmed(store)


def test_capacity_and_unknown_constructor_binding_never_evict_or_open_again(tmp_path, monkeypatch):
    scope = InferenceStoreLeases("capacity", capacity=1)
    calls = []

    def unknown_open(path):
        calls.append(True)
        raise OSError("deliberate no-return fixture")

    monkeypatch.setattr(file_module, "_open_file", unknown_open)
    with pytest.raises(InferenceStoreConstructionError):
        FileInferenceStateStore(state_path(tmp_path), leases=scope)
    original = scope.bindings
    with pytest.raises(InferenceStoreConstructionError):
        FileInferenceStateStore(state_path(tmp_path), leases=scope, role="journal")
    assert scope.bindings is original and len(calls) == 1
    assert not scope.close_all().all_confirmed


@pytest.mark.parametrize("capacity", [True, 0, -1, 4097, 512.0])
def test_scope_capacity_is_finite_native_int(capacity):
    with pytest.raises(ValidationError):
        InferenceStoreLeases("generation", capacity=capacity)


def test_cached_sanitized_snapshot_is_immutable_and_cannot_authorize_cleanup(tmp_path):
    scope = InferenceStoreLeases("generation")
    store = FileInferenceStateStore(state_path(tmp_path), leases=scope, role="delivery")
    try:
        snapshot = store.retirement_status()
        assert type(snapshot) is StoreLeaseSnapshot
        assert not snapshot.file_lease_confirmed
        with pytest.raises(FrozenInstanceError):
            snapshot.file_lease_confirmed = True
        with pytest.raises(TypeError):
            scope.binding_for(store).close(snapshot)
        assert not store.closed
        text = json.dumps(asdict(scope.snapshot()))
        assert str(tmp_path) not in text and "secret" not in text
        assert scope.binding_for(store).original_store is store
        with pytest.raises(AttributeError):
            scope.binding_for(store).original_store = object()
    finally:
        close_confirmed(store)
