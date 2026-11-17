"""Original-reference evidence only; no restoration or real account authority."""

import inspect
import threading
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.inference import (
    CAUSAL_SCOPE,
    InferenceCommitAck,
    InferenceModelIdentity,
    InferenceOutputBinding,
    encoded,
)
from omega.runtime import causal_actor_retirement as module
from omega.runtime.causal_actor_retirement import (
    ActorJoinObservation,
    ActorJoinObserver,
    ActorOriginalResources,
    validate_retirement_value,
)
from omega.runtime.causal_inference_ports import ObservedInferenceWorker
from omega.runtime.causal_market_source import CommittedInferenceMarketSource
from omega.runtime.causal_nn import CausalNNFeatureOwner, FileInferenceStateStore
from omega.runtime.causal_nn_worker import CausalInferenceProcessWorker
from omega.runtime.causal_store_leases import InferenceStoreLeases


def registry(*, capacity=1536, default=True):
    value = ActorOriginalResources(
        "actor", "core", threading.current_thread(), owner_thread_token="thread", capacity=capacity
    )
    scope = InferenceStoreLeases("files")
    value.bind_store_scope(scope, default_store_factory=default)
    return value, scope


def finish(value, scope, **options):
    value.seal_admission()
    scope.seal_admission()
    return value.snapshot(sealed=True, stores=scope.snapshot(), **options)


class Legacy:
    def __init__(self):
        self.calls = 0

    def close(self):
        self.calls += 1


def test_slots_are_reserved_atomically_and_capacity_rejects_before_any_factory():
    value, _ = registry(capacity=3)
    keys = value.reserve_run()
    assert tuple(key.role for key in keys) == ("source", "raw_worker", "owner")
    assert len({key.binding_token for key in keys}) == 3
    assert len({key.run_token for key in keys}) == 1
    with pytest.raises(StorageError):
        value.reserve_run()
    assert all(value.original(key) is None for key in keys)


def test_unentered_slots_are_no_actual_resource_not_acquisition_unknown():
    value, scope = registry()
    value.reserve_run()
    record = finish(value, scope)
    assert record.physical_confirmed
    assert all(
        row.acquisition == "reserved" and row.physical == "confirmed" for row in record.resources
    )
    assert record.actor_recovery_authorized is False


@pytest.mark.parametrize("fault", [KeyboardInterrupt, SystemExit, OSError])
def test_entered_factory_fault_cannot_become_unentered_no_resource(fault):
    value, scope = registry()
    key, _, _ = value.reserve_run()
    value.enter(key, CommittedInferenceMarketSource, default=True)
    error = fault("private-fixture")
    try:
        raise error
    except BaseException as actual:
        value.acquisition_failed(key)
        assert actual is error
    record = finish(value, scope)
    assert not record.physical_confirmed
    assert record.resources[0].acquisition == "unknown"
    assert record.resources[1].acquisition == "reserved"


def test_missing_scope_initialization_and_custom_empty_scope_never_confirm():
    missing = ActorOriginalResources(
        "actor", "core", threading.current_thread(), owner_thread_token="thread"
    )
    missing.seal_admission()
    assert not missing.snapshot(sealed=True).physical_confirmed
    custom, scope = registry(default=False)
    record = finish(custom, scope)
    assert not record.factory_coverage_complete and not record.physical_confirmed


def test_default_closed_source_remains_original_after_new_run(tmp_path):
    value, scope = registry()
    key, _, _ = value.reserve_run()
    value.enter(key, CommittedInferenceMarketSource, default=True)
    source = CommittedInferenceMarketSource(tmp_path)
    value.returned(key, source)
    source.close()
    observation = value.observe_resource(source, close_attempted=True, close_returned=True)
    assert observation.physical == "confirmed"
    newer = value.reserve_run()
    assert newer[0].run_token != key.run_token
    assert value.original(key) is source and value.key_for_original(source) is key
    record = finish(value, scope)
    assert record.physical_confirmed and record.resources[0].source.source_closed
    assert source._original_read_scope.snapshot().reads == 0


def test_legacy_source_close_contract_is_not_probed_or_promoted():
    value, scope = registry()
    key, _, _ = value.reserve_run()
    value.enter(key, Legacy, default=False)
    source = Legacy()
    value.returned(key, source)
    source.close()
    observed = value.observe(key, close_attempted=True, close_returned=True)
    assert source.calls == 1 and observed.physical == "unknown"
    assert not finish(value, scope).factory_coverage_complete


def test_explicit_injected_default_class_is_still_incomplete(tmp_path):
    value, scope = registry()
    key, _, _ = value.reserve_run()
    value.enter(key, CommittedInferenceMarketSource, default=False)
    source = CommittedInferenceMarketSource(tmp_path)
    value.returned(key, source)
    source.close()
    assert value.observe(key, close_attempted=True, close_returned=True).physical == "unknown"
    assert not finish(value, scope).physical_confirmed


@pytest.mark.parametrize("operation", ["getter", "close", "scope"])
def test_original_source_substitution_stays_unknown_without_new_getter_call(tmp_path, operation):
    value, scope = registry()
    key, _, _ = value.reserve_run()
    value.enter(key, CommittedInferenceMarketSource, default=True)
    source = CommittedInferenceMarketSource(tmp_path)
    value.returned(key, source)
    source.close()
    called = []
    if operation == "getter":
        source.retirement_status = lambda: called.append(True)
    elif operation == "close":
        source.close = lambda: called.append(True)
    else:
        source._read_scope = object()
    record = value.observe(key, close_attempted=True, close_returned=True)
    assert not record.identity_intact and record.physical == "unknown" and called == []
    assert not finish(value, scope).physical_confirmed


def test_foreign_thread_guard_precedes_getters_and_all_mutation(tmp_path):
    value, scope = registry()
    key, _, _ = value.reserve_run()
    value.enter(key, CommittedInferenceMarketSource, default=True)
    source = CommittedInferenceMarketSource(tmp_path)
    value.returned(key, source)
    before = value.snapshot(stores=scope.snapshot())
    outcomes = []

    def foreign():
        for call in (
            lambda: value.observe(key, close_attempted=True, close_returned=True),
            value.reserve_run,
            value.seal_admission,
            lambda: value.key_for_original(source),
            lambda: value.snapshot(),
        ):
            try:
                call()
            except BaseException as error:
                outcomes.append(type(error))

    thread = threading.Thread(target=foreign)
    thread.start()
    thread.join(timeout=2)
    assert not thread.is_alive() and outcomes == [StorageError] * 5
    after = value.snapshot(stores=scope.snapshot())
    assert replace(before, sequence=after.sequence) == after
    source.close()


def test_foreign_pid_cannot_use_same_numeric_thread_identity(monkeypatch):
    value, _ = registry()
    before = value._bindings
    monkeypatch.setattr(module.os, "getpid", lambda: value._pid + 1)
    with pytest.raises(StorageError):
        value.reserve_run()
    assert value._bindings is before and not value.capture_failed


def test_cloned_equal_key_does_not_borrow_original_reference():
    value, _ = registry()
    key, _, _ = value.reserve_run()
    with pytest.raises(StorageError):
        value.enter(replace(key), Legacy)


@pytest.mark.parametrize("bad", [[], {}, (True,), "not-an-actor-key"])
def test_mutable_or_wrong_typed_values_are_not_cached(bad):
    with pytest.raises(ValidationError):
        validate_retirement_value(bad)


def test_exact_frozen_record_with_mutable_nested_field_is_rejected():
    record = ActorJoinObservation("actor", "thread", False, False, False, True, None)
    object.__setattr__(record, "error_code", [])
    with pytest.raises(ValidationError):
        validate_retirement_value(record)


def test_native_bool_subclass_and_authorization_forgery_are_rejected():
    record = ActorJoinObservation("actor", "thread", False, False, False, True, None)
    with pytest.raises(FrozenInstanceError):
        record.identity_intact = False
    with pytest.raises(TypeError):
        ActorJoinObservation(
            "actor", "thread", False, False, False, True, None, actor_recovery_authorized=True
        )
    for field_name, forged in (("join_returned", 1), ("actor_recovery_authorized", True)):
        malformed = replace(record)
        object.__setattr__(malformed, field_name, forged)
        with pytest.raises(ValidationError):
            validate_retirement_value(malformed)


def test_capture_failure_and_recording_unknown_do_not_turn_into_empty_success():
    value, scope = registry()
    value.mark_capture_failed("injected_capture_failure")
    before = value.snapshot(stores=scope.snapshot(), recording_unknown=True)
    final = finish(value, scope, recording_unknown=False)
    assert before.capture_failed and final.capture_failed and final.recording_unknown
    assert not final.physical_confirmed
    assert value.snapshot(sealed=False) is final
    with pytest.raises(StorageError):
        value.reserve_run()


def test_last_exact_ack_is_independent_from_unknown_and_physical_cleanup(tmp_path):
    value, scope = registry()
    store = FileInferenceStateStore(tmp_path / "journal.json", leases=scope, role="journal")
    ack = store.save_snapshot(
        encoded({"scope": CAUSAL_SCOPE, "revision": 1}), expected_revision=None
    )
    before = value.snapshot(stores=scope.snapshot(), last_journal_ack=ack)
    assert before.last_journal_ack is ack
    store.close()
    final = finish(value, scope, recording_unknown=True)
    assert final.physical_confirmed and final.recording_unknown
    assert final.last_journal_ack is ack and final.actor_recovery_authorized is False


def test_duck_ack_is_legacy_incomplete_without_new_save_rejection():
    value, scope = registry()
    ack = SimpleNamespace(revision=1, snapshot_sha256="a" * 64, durable=True)
    final = finish(value, scope, last_journal_ack=ack)
    assert final.last_journal_ack is None
    assert not final.factory_coverage_complete


def test_unbound_exact_ack_cannot_claim_original_journal_evidence():
    value, scope = registry()
    final = finish(value, scope, last_journal_ack=InferenceCommitAck(1, "a" * 64, True))
    assert final.last_journal_ack is None and not final.factory_coverage_complete


def test_original_actor_join_is_separate_from_resource_evidence():
    event = threading.Event()
    thread = threading.Thread(target=event.wait)
    observer = ActorJoinObserver("actor", "thread", thread)
    thread.start()
    alive = observer.observe(0, current_thread=thread)
    assert alive.join_returned and not alive.original_thread_dead
    event.set()
    dead = observer.observe(2, current_thread=thread)
    assert dead.original_thread_dead and dead.identity_intact
    assert dead.actor_recovery_authorized is False


def test_replacing_public_thread_still_joins_original_but_never_confirms_identity():
    original = threading.Thread(target=lambda: None)
    observer = ActorJoinObserver("actor", "thread", original)
    original.start()
    other = threading.Thread(target=lambda: None)
    observed = observer.observe(2, current_thread=other)
    assert observed.original_thread_dead and not observed.identity_intact
    assert not observer.observe(0, current_thread=original).identity_intact


def test_join_of_never_started_or_own_thread_is_unknown_not_dead():
    for thread in (threading.Thread(target=lambda: None), threading.current_thread()):
        # current_thread may be _MainThread, which is intentionally not an exact Thread.
        if type(thread) is threading.Thread:
            record = ActorJoinObserver("actor", "thread", thread).observe(0)
            assert not record.original_thread_dead and record.error_code


def test_scope_getter_shape_replacement_is_not_invoked():
    value, scope = registry()
    original = scope.snapshot()
    calls = []
    scope.snapshot = lambda: calls.append(True)
    value.seal_admission()
    record = value.snapshot(sealed=True, stores=original)
    assert not record.physical_confirmed and calls == []
    assert inspect.getattr_static(scope, "snapshot") is scope.snapshot


@pytest.mark.parametrize("fault", [KeyboardInterrupt, SystemExit])
def test_capture_baseexception_keeps_original_reference_and_exception(tmp_path, monkeypatch, fault):
    value, _ = registry()
    key, _, _ = value.reserve_run()
    source = CommittedInferenceMarketSource(tmp_path)
    original = CommittedInferenceMarketSource.retirement_status
    error = fault("injected-capture")

    def capture(_):
        raise error

    monkeypatch.setattr(CommittedInferenceMarketSource, "retirement_status", capture)
    value.enter(key, CommittedInferenceMarketSource, default=True)
    value.returned(key, source)
    try:
        with pytest.raises(fault) as caught:
            value.observe(key, close_attempted=False, close_returned=False)
        assert caught.value is error and value.original(key) is source
        assert value.capture_failed
    finally:
        monkeypatch.setattr(CommittedInferenceMarketSource, "retirement_status", original)
        source.close()


def test_capture_reentry_sticks_unknown_without_resource_cleanup(tmp_path, monkeypatch):
    value, scope = registry()
    key, _, _ = value.reserve_run()
    source = CommittedInferenceMarketSource(tmp_path)
    original = CommittedInferenceMarketSource.retirement_status
    calls = []

    def capture(_):
        calls.append(True)
        return value.snapshot(stores=scope.snapshot())

    monkeypatch.setattr(CommittedInferenceMarketSource, "retirement_status", capture)
    value.enter(key, CommittedInferenceMarketSource, default=True)
    value.returned(key, source)
    try:
        with pytest.raises(StorageError):
            value.observe(key, close_attempted=False, close_returned=False)
        assert value.capture_failed and calls == [True] and value.original(key) is source
    finally:
        monkeypatch.setattr(CommittedInferenceMarketSource, "retirement_status", original)
        source.close()


def test_returned_lazy_default_worker_proves_only_original_never_started(tmp_path):
    value, scope = registry()
    _, key, _ = value.reserve_run()
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    worker = CausalInferenceProcessWorker(
        SimpleNamespace(verify=lambda: None), input_root, tmp_path / "execution"
    )
    value.enter(key, CausalInferenceProcessWorker, default=True)
    value.returned(key, worker)
    worker.close()
    observation = value.observe(key, close_attempted=True, close_returned=True)
    assert observation.physical == "confirmed"
    assert observation.worker.never_started_confirmed
    assert observation.worker.normal_model_cleanup == "not_started"
    assert finish(value, scope).physical_confirmed


def test_wrapper_alias_retains_original_raw_when_owner_factory_never_returns():
    value, _ = registry()
    _, key, owner_key = value.reserve_run()
    raw = Legacy()
    value.enter(key, Legacy)
    value.returned(key, raw)
    wrapper = ObservedInferenceWorker(raw, lambda _: None)
    value.attach_wrapper(key, wrapper)
    value.enter(owner_key, Legacy)
    value.acquisition_failed(owner_key)
    wrapper.close()
    observed = value.observe_resource(wrapper, close_attempted=True, close_returned=True)
    assert value.key_for_original(wrapper) is key and value.original(key) is raw
    assert raw.calls == 1 and observed.physical == "unknown"
    assert observed.close_returned is False


def test_wrapper_worker_swap_never_calls_or_confirms_substituted_resource():
    value, _ = registry()
    _, key, _ = value.reserve_run()
    raw = Legacy()
    value.enter(key, Legacy)
    value.returned(key, raw)
    wrapper = ObservedInferenceWorker(raw, lambda _: None)
    value.attach_wrapper(key, wrapper)
    other = Legacy()
    wrapper.worker = other
    observed = value.observe_resource(wrapper, close_attempted=False, close_returned=False)
    assert not observed.identity_intact and raw.calls == other.calls == 0


def test_late_source_shape_change_degrades_old_positive_observation(tmp_path):
    value, scope = registry()
    key, _, _ = value.reserve_run()
    value.enter(key, CommittedInferenceMarketSource, default=True)
    source = CommittedInferenceMarketSource(tmp_path)
    value.returned(key, source)
    source.close()
    original = value.observe(key, close_attempted=True, close_returned=True)
    assert original.physical == "confirmed"
    source.close = lambda: None
    final = finish(value, scope)
    assert not final.physical_confirmed and not final.identity_intact
    assert final.resources[0].physical == "unknown"
    assert original.physical == "confirmed"  # Prior immutable observation preserved.


def test_entered_unknown_is_retained_across_further_bounded_runs():
    value, scope = registry()
    key, _, _ = value.reserve_run()
    value.enter(key, Legacy)
    value.acquisition_failed(key)
    value.reserve_run()  # Evidence export does not introduce a new recovery/factory policy.
    final = finish(value, scope)
    assert len(final.resources) == 6
    assert final.resources[0].acquisition == "unknown" and not final.physical_confirmed


@pytest.mark.parametrize(
    "attribute", ["_process", "_incoming", "_outgoing", "_resources", "_receipt_channel"]
)
def test_late_original_worker_transport_substitution_degrades_positive(tmp_path, attribute):
    value, scope = registry()
    _, key, _ = value.reserve_run()
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    worker = CausalInferenceProcessWorker(
        SimpleNamespace(verify=lambda: None), input_root, tmp_path / "execution"
    )
    value.enter(key, CausalInferenceProcessWorker, default=True)
    value.returned(key, worker)
    worker.close()
    positive = value.observe(key, close_attempted=True, close_returned=True)
    assert positive.physical == "confirmed"
    original = getattr(worker, attribute)
    setattr(worker, attribute, object())
    failed = value.snapshot(stores=scope.snapshot())
    assert not failed.identity_intact and failed.resources[1].physical == "unknown"
    assert positive.physical == "confirmed"
    setattr(worker, attribute, original)
    final = finish(value, scope)
    assert not final.physical_confirmed and not final.identity_intact
    assert final.resources[1].physical == "unknown"


def test_late_worker_getter_substitution_is_not_called(tmp_path):
    value, scope = registry()
    _, key, _ = value.reserve_run()
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    worker = CausalInferenceProcessWorker(
        SimpleNamespace(verify=lambda: None), input_root, tmp_path / "execution"
    )
    value.enter(key, CausalInferenceProcessWorker, default=True)
    value.returned(key, worker)
    worker.close()
    assert value.observe(key, close_attempted=True, close_returned=True).physical == "confirmed"
    calls = []
    worker.transport_retirement_snapshot = lambda: calls.append(True)
    final = finish(value, scope)
    assert calls == [] and not final.physical_confirmed
    assert final.resources[1].physical == "unknown"


def complete_default_run(tmp_path):
    value, scope = registry()
    source_key, worker_key, owner_key = value.reserve_run()
    source = CommittedInferenceMarketSource(tmp_path)
    value.enter(source_key, CommittedInferenceMarketSource, default=True)
    value.returned(source_key, source)
    input_root = tmp_path / "inputs"
    input_root.mkdir()
    worker = CausalInferenceProcessWorker(
        SimpleNamespace(verify=lambda: None), input_root, tmp_path / "execution"
    )
    value.enter(worker_key, CausalInferenceProcessWorker, default=True)
    value.returned(worker_key, worker)
    wrapper = ObservedInferenceWorker(worker, lambda _: None)
    value.attach_wrapper(worker_key, wrapper)
    verification_calls = []

    def verify(_):
        verification_calls.append(True)
        raise AssertionError("no inference is requested in retirement composition fixture")

    model = InferenceModelIdentity("fixture", "a" * 64, "b" * 64, "v1", "c" * 64, 0, 1, 2, "d" * 64)
    value.enter(owner_key, CausalNNFeatureOwner, default=True)
    owner = CausalNNFeatureOwner(
        tmp_path,
        model,
        (InferenceOutputBinding("NN.score", ("values", 0)),),
        worker=wrapper,
        evidence=SimpleNamespace(verify=verify),
        core_epoch="core",
        deployment_epoch="deployment",
        namespace="owner",
        clock_ms=lambda: 1000,
        leases=scope,
    )
    binding = scope.binding_for(owner.store)
    value.returned(owner_key, owner, related_worker=worker, wrapper=wrapper, store_binding=binding)
    owner.close()
    source.close()
    value.observe_resource(owner, close_attempted=True, close_returned=True)
    value.observe_resource(source, close_attempted=True, close_returned=True)
    assert verification_calls == []
    assert worker._process is worker._incoming is worker._outgoing is None
    return value, scope, source, worker, wrapper, owner


def test_complete_default_owner_source_worker_composition_has_exact_physical_receipts(tmp_path):
    value, scope, source, worker, wrapper, owner = complete_default_run(tmp_path)
    value.seal_admission()
    files = scope.close_all()  # Test orchestrator; registry never invokes close.
    final = value.snapshot(sealed=True, stores=files)
    assert final.physical_confirmed and final.factory_coverage_complete
    assert tuple(row.physical for row in final.resources) == ("confirmed",) * 3
    assert final.resources[2].owner_store_binding_token == scope.binding_for(owner.store).token
    assert final.resources[1].worker.never_started_confirmed
    assert final.resources[0].source.read_scope.reads == 0
    assert value.original(final.resources[0].key) is source
    assert value.original(final.resources[1].key) is worker
    assert value.original(final.resources[2].key) is owner
    assert wrapper.worker is worker and not final.actor_recovery_authorized


@pytest.mark.parametrize("changed", ["owner_store", "owner_wrapper", "wrapper_worker"])
def test_late_default_owner_composition_substitution_is_unknown(tmp_path, changed):
    value, scope, source, worker, wrapper, owner = complete_default_run(tmp_path)
    original_store = owner.store
    if changed == "owner_store":
        owner.store = object()
    elif changed == "owner_wrapper":
        owner.worker = object()
    else:
        wrapper.worker = object()
    final = finish(value, scope)
    assert not final.physical_confirmed and not final.identity_intact
    assert final.resources[2].physical == "unknown"
    assert scope.binding_for(original_store).original_store is original_store
    assert value.original(final.resources[0].key) is source
    assert value.original(final.resources[1].key) is worker
    assert value.original(final.resources[2].key) is owner
