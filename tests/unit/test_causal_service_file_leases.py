"""Real actor/file leases with the existing explicitly fake source/model ports.

Persistent unknowns run only in bounded containing hosts. Host exit releases
test OS handles, never confirms the held file lease or authorizes restoration.
"""

import json
import multiprocessing
import os
import threading
import traceback
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import test_causal_feature_delivery as delivery_fixture
import test_causal_inference_service as actor_fixture

from omega.domain.errors import StorageError, TradingBlocked
from omega.runtime import causal_inference_leases as file_leases
from omega.runtime.causal_feature_delivery import CausalFeatureDeliveryProvider
from omega.runtime.causal_inference_service import CausalInferenceService
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_store_leases import InferenceStoreLeases, StoreLeaseSnapshot


class _ObservedStream:
    """Deliberate observer/close faults around one original real file object."""

    def __init__(self, original, mode, release):
        self.original, self.mode, self.release = original, mode, release
        self.close_threads, self.observation_threads = [], []
        self.fault_armed = threading.Event()

    def __getattr__(self, name):
        return getattr(self.original, name)

    @property
    def closed(self):
        self.observation_threads.append(threading.current_thread())
        if (
            self.mode == "pre_observation"
            and self.fault_armed.is_set()
            and not self.release.is_set()
        ):
            raise OSError("explicit preclose observation fault")
        return self.original.closed

    def close(self):
        self.close_threads.append(threading.current_thread())
        self.original.close()
        if self.mode == "after_native_return":
            raise OSError("private-close-fixture-secret")


def _role_for_path(path):
    path = Path(path)
    if path.name == "causal-inference.json.lock":
        return "journal"
    if path.name == "causal-feature-delivery.json.lock":
        return "delivery"
    if path.parent.name == "causal-nn":
        return "nn_state"
    return None


def _fault_original_open(monkeypatch, role, mode, *, constructor_failure=False):
    original_open = file_leases._open_file
    original_validate = file_leases._validate_open_file
    wrappers, release = [], threading.Event()

    def open_original(path):
        original = original_open(path)
        if _role_for_path(path) == role and not wrappers:
            observed = _ObservedStream(original, mode, release)
            wrappers.append(observed)
            return observed
        return original

    def validate_original(stream, path):
        original_validate(stream, path)
        if constructor_failure and wrappers and stream is wrappers[0]:
            raise ValueError("private-constructor-fixture-secret")

    monkeypatch.setattr(file_leases, "_open_file", open_original)
    monkeypatch.setattr(file_leases, "_validate_open_file", validate_original)
    return wrappers, release


def _bindings(service):
    observation = service.file_lease_status()
    assert observation["restoration_authorized"] is False
    return observation, observation["snapshot"]["bindings"]


def _binding(service, role):
    _, values = _bindings(service)
    found = [value for value in values if value["role"] == role]
    assert len(found) == 1
    return found[0]


def _assert_journal_excludes_another_owner(root):
    with pytest.raises((StorageError, RuntimeError), match="already owns"):
        FileInferenceStateStore(root / "state/causal-inference.json")


def test_actor_get_and_private_file_status_use_only_foreign_safe_cache(tmp_path, monkeypatch):
    service, _ = actor_fixture.create(tmp_path, monkeypatch)
    try:
        deployment = actor_fixture.activate(service)
        assert actor_fixture.reset(service, deployment)["state"] == "complete"
        original_snapshot = InferenceStoreLeases.snapshot
        original_open = Path.open
        scope_calls = []

        def owner_snapshot(scope):
            scope_calls.append(threading.current_thread())
            assert threading.current_thread() is service.thread
            return original_snapshot(scope)

        def owner_only_open(path, *args, **kwargs):
            assert threading.current_thread() is service.thread, "cached GET attempted file IO"
            return original_open(path, *args, **kwargs)

        with patch.object(InferenceStoreLeases, "snapshot", owner_snapshot):
            with patch.object(Path, "open", owner_only_open):
                for _ in range(25):
                    assert service.snapshot()["ready"]
                    assert service.status("reset")["state"] == "complete"
                    cached = service.file_lease_status()
                    assert cached["complete_default_file_factories"]
                    assert cached["restoration_authorized"] is False
                cached["snapshot"]["bindings"] = ()
                assert service.file_lease_status()["snapshot"]["bindings"]
        assert all(owner is service.thread for owner in scope_calls)
        _, values = _bindings(service)
        assert {row["role"] for row in values} == {"journal", "nn_state", "delivery"}
        assert all(not row["file_lease_confirmed"] for row in values)
    finally:
        service.close()
    _, values = _bindings(service)
    assert all(row["file_lease_confirmed"] for row in values)


def test_transient_journal_observation_retries_original_actor_and_stream(tmp_path, monkeypatch):
    streams, release = _fault_original_open(monkeypatch, "journal", "pre_observation")
    captured = {}
    original_service = actor_fixture.CausalInferenceService
    service, armed = None, False

    def capture_service(*args, **kwargs):
        captured["service"] = original_service(*args, **kwargs)
        return captured["service"]

    monkeypatch.setattr(actor_fixture, "CausalInferenceService", capture_service)
    try:
        service, state = actor_fixture.create(tmp_path, monkeypatch, close_timeout_s=0.05)
        actor_fixture.activate(service)
        original_thread = service.thread
        original_close = FileInferenceStateStore.close

        def arm_at_original_journal_close(store):
            if store is service.store:
                streams[0].fault_armed.set()
            return original_close(store)

        # Startup and final journal writes see the ordinary stream. The failing
        # observer is armed only at this original store's actual close boundary.
        monkeypatch.setattr(FileInferenceStateStore, "close", arm_at_original_journal_close)
        armed = True
        with pytest.raises(StorageError, match="deadline"):
            service.close()
        assert service.thread is original_thread and original_thread.is_alive()
        original = streams[0]
        assert not original.original.closed and not original.close_threads
        _assert_journal_excludes_another_owner(tmp_path)
        journal = _binding(service, "journal")
        assert journal["logical_closed"] and not journal["file_lease_confirmed"]
        assert not journal["file"]["native_close_attempted"]
        assert _binding(service, "delivery")["file_lease_confirmed"]
        assert _binding(service, "nn_state")["file_lease_confirmed"]
        assert all(owner is original_thread for owner in original.observation_threads)
    finally:
        # Release the failing test observer, not a lease flag or unknown ACK.
        # The same original native stream remains open until explicit retry.
        release.set()
        cleanup = service if service is not None else captured.get("service")
        if cleanup is not None:
            if armed or cleanup.error is not None:
                with pytest.raises(StorageError):
                    cleanup.close()
            else:
                cleanup.close()
            actor_fixture.wait(lambda: cleanup.closed)
    assert service.thread is original_thread and not original_thread.is_alive()
    assert streams[0].close_threads == [original_thread]
    assert _binding(service, "journal")["file_lease_confirmed"]
    assert service.snapshot()["state"] == "recording_unconfirmed"
    assert len(state["workers"]) == 1 and all(worker.closed for worker in state["workers"])
    reopened = FileInferenceStateStore(tmp_path / "state/causal-inference.json")
    reopened.close()


def test_provider_close_failure_does_not_skip_known_file_or_journal_cleanup(tmp_path, monkeypatch):
    service, state = actor_fixture.create(tmp_path, monkeypatch)
    actor_fixture.activate(service)
    events = []
    original_close = FileInferenceStateStore.close

    def observe_close(store):
        events.append((store._lease_binding.role, threading.current_thread()))
        return original_close(store)

    def fail_provider(provider):
        assert provider is service.external_features
        events.append(("provider_error", threading.current_thread()))
        raise OSError("private-provider-fixture-secret")

    monkeypatch.setattr(FileInferenceStateStore, "close", observe_close)
    monkeypatch.setattr(CausalFeatureDeliveryProvider, "_close_actor", fail_provider)
    with pytest.raises(StorageError):
        service.close()
    assert service.closed and not service.thread.is_alive()
    assert all(worker.closed for worker in state["workers"])
    assert any(kind == "source_close" for kind, _ in state["events"])
    _, values = _bindings(service)
    assert all(value["file_lease_confirmed"] for value in values)
    assert all(owner is service.thread for _, owner in events)
    roles = [role for role, _ in events]
    assert roles.index("provider_error") < roles.index("delivery") < roles.index("journal")
    assert service.snapshot()["state"] == "recording_unconfirmed"
    assert "private-provider-fixture-secret" not in json.dumps(service.file_lease_status())
    reopened = FileInferenceStateStore(tmp_path / "state/causal-inference.json")
    reopened.close()


def test_paused_actor_keeps_admitted_pin_readback_and_delivery_lease(tmp_path, monkeypatch):
    service, state, provider, binding, candle, cutoff, snapshot = delivery_fixture.ready(
        tmp_path, monkeypatch
    )
    original_delivery = provider.store
    delivery_token = _binding(service, "delivery")["binding_token"]
    try:
        pin = provider.pin("lease-consumer", "f" * 64, snapshot, candle, decision_at_ms=cutoff)
        actor_fixture.command(service, "pause", "pause", deployment_id=binding.instance_id)
        assert actor_fixture.terminal(service, "pause")["state"] == "complete"
        assert provider.store is original_delivery and not original_delivery.store.closed
        assert provider.store.load_snapshot(snapshot.snapshot_sha256) == snapshot
        assert provider.store.load_pins()[pin.pin_id] == pin
        assert provider._pins[pin.pin_id] == pin
        assert _binding(service, "delivery")["binding_token"] == delivery_token
        assert not _binding(service, "delivery")["file_lease_confirmed"]
        assert _binding(service, "nn_state")["file_lease_confirmed"]
        assert not provider.authorize(pin, snapshot, candle, decision_at_ms=cutoff)
        with pytest.raises(TradingBlocked):
            provider.pin("new-consumer", "e" * 64, snapshot, candle, decision_at_ms=cutoff)
        assert len(state["workers"]) == 1
        with pytest.raises((StorageError, RuntimeError), match="already owns"):
            FileInferenceStateStore(tmp_path / "state/causal-feature-delivery.json")
    finally:
        service.close()
    assert original_delivery.store.closed
    assert _binding(service, "delivery")["file_lease_confirmed"]


def test_typed_file_retirement_is_required_even_when_logically_closed(tmp_path, monkeypatch):
    streams, release = _fault_original_open(monkeypatch, "journal", "pre_observation")
    path = tmp_path / "causal-inference.json"
    store, failed_scope = None, None
    try:
        try:
            store = FileInferenceStateStore(path, role="journal")
        except BaseException as failure:
            failed_scope = getattr(failure, "lease_scope", None)
            raise
        streams[0].fault_armed.set()  # Arm only after the original constructor returned.
        with pytest.raises(StorageError):
            store.close()
        assert store.closed and not streams[0].original.closed
        assert type(store.retirement_status()) is StoreLeaseSnapshot
        assert not CausalInferenceService._physically_retired(store)
        worker = SimpleNamespace(
            retirement_status=lambda: {"confirmed": True, "worker_alive": False}
        )
        owner = SimpleNamespace(worker=worker, store=store)
        assert not CausalInferenceService._physically_retired(owner)
        untyped = SimpleNamespace(
            closed=True, retirement_status=lambda: {"file_lease_confirmed": True}
        )
        assert not CausalInferenceService._physically_retired(
            SimpleNamespace(worker=worker, store=untyped)
        )
        # Earlier fake-port compatibility is retained only without typed probes.
        assert CausalInferenceService._physically_retired(
            SimpleNamespace(worker=worker, store=SimpleNamespace(closed=True))
        )
    finally:
        release.set()
        if store is not None:
            store.close()
        elif failed_scope is not None:
            assert failed_scope.close_all().all_confirmed
    assert CausalInferenceService._physically_retired(store)
    assert CausalInferenceService._physically_retired(owner)


def test_legacy_store_factory_signature_keeps_partial_coverage_explicit(tmp_path, monkeypatch):
    calls = []

    def legacy_factory(path):
        calls.append(threading.current_thread())
        return FileInferenceStateStore(path)

    service, _ = actor_fixture.create(tmp_path, monkeypatch, store_factory=legacy_factory)
    try:
        actor_fixture.activate(service)
        observation, values = _bindings(service)
        assert not observation["complete_default_file_factories"]
        assert observation["restoration_authorized"] is False
        assert {row["role"] for row in values} == {"nn_state", "delivery"}
        assert calls == [service.thread]
    finally:
        service.close()
    assert service.closed and not service.thread.is_alive()


def _write_host_result(path, value):
    with path.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())


def _held_nonjournal_host(root, role, constructor_failure, result_path):
    watchdog = threading.Timer(15, os._exit, args=(93,))
    watchdog.daemon = True
    watchdog.start()
    monkeypatch = pytest.MonkeyPatch()
    state = {
        "clock": [660150],
        "workers": [],
        "events": [],
        "auto": True,
        "provider_calls": 0,
        "source_calls": 0,
        "verify_calls": 0,
    }
    captured = {}
    try:
        streams, _ = _fault_original_open(
            monkeypatch, role, "after_native_return", constructor_failure=constructor_failure
        )
        original_service = actor_fixture.CausalInferenceService

        def capture_service(*args, **kwargs):
            captured["service"] = original_service(*args, **kwargs)
            return captured["service"]

        monkeypatch.setattr(actor_fixture, "CausalInferenceService", capture_service)
        if constructor_failure and role == "delivery":
            with pytest.raises(StorageError):
                actor_fixture.create(root, monkeypatch, state=state, close_timeout_s=0.05)
            service = captured["service"]
            assert service.external_features.store is None
        else:
            service, _ = actor_fixture.create(root, monkeypatch, state=state, close_timeout_s=0.05)
            if constructor_failure:
                actor_fixture.command(
                    service,
                    "activate",
                    "activate",
                    training_job_id=actor_fixture.JOB,
                    deployment_id=None,
                    trusted_module=True,
                    contract=actor_fixture.CONTRACT.document(),
                    bindings=actor_fixture.BINDINGS,
                )
                actor_fixture.wait(lambda: service.error is not None)
            else:
                actor_fixture.activate(service)
        original_thread = service.thread
        with pytest.raises(StorageError, match="deadline"):
            service.close()
        actor_fixture.wait(lambda: bool(service.file_lease_status()))
        unknown = _binding(service, role)
        journal = _binding(service, "journal")
        assert unknown["logical_closed"] and not unknown["file_lease_confirmed"]
        assert unknown["file"]["destructive_unknown"]
        assert unknown["file"]["native_close_attempted"]
        assert not unknown["file"]["native_close_returned"]
        assert unknown["construction_failed"] is constructor_failure
        assert streams[0].original.closed
        assert streams[0].close_threads == [original_thread]
        assert (
            service.thread is original_thread and original_thread.is_alive() and not service.closed
        )
        assert not journal["logical_closed"] and not journal["file_lease_confirmed"]
        assert not journal["file"]["native_close_attempted"]
        _assert_journal_excludes_another_owner(root)
        assert all(worker.closed for worker in state["workers"])
        if state["workers"]:
            assert any(kind == "source_close" for kind, _ in state["events"])
        if constructor_failure:
            assert service.owner is None
        if role == "delivery" and not constructor_failure:
            assert _binding(service, "nn_state")["file_lease_confirmed"]
        encoded = json.dumps(service.file_lease_status())
        assert "private-close-fixture-secret" not in encoded
        assert "private-constructor-fixture-secret" not in encoded
        # Explicit retry does not turn a destructive native-close exception into
        # a later no-op ACK or execute the captured native callback again.
        with pytest.raises(StorageError, match="deadline"):
            service.close()
        assert not _binding(service, role)["file_lease_confirmed"]
        assert streams[0].close_threads == [original_thread]
        _write_host_result(
            result_path,
            {
                "scope": "contained_synthetic_actor_file_fault_no_model_db_or_restore",
                "outcome": "CONTAINED_HELD",
                "role": role,
                "constructor_failure": constructor_failure,
                "unknown_binding": unknown,
                "journal_binding": journal,
                "original_actor_alive": original_thread.is_alive(),
                "journal_lock_excluded": True,
                "native_stream_closed_but_not_confirmed": streams[0].original.closed,
                "host_exit_is_product_ack": False,
            },
        )
        # Do not reset flags, close replacements or retry a raw descriptor. The
        # bounded containing host ends while its product evidence remains HELD.
        os._exit(0)
    except BaseException:
        _write_host_result(result_path, {"outcome": "FAILED", "traceback": traceback.format_exc()})
        os._exit(1)


def _held_generic_source_host(root, result_path):
    watchdog = threading.Timer(15, os._exit, args=(93,))
    watchdog.daemon = True
    watchdog.start()
    monkeypatch = pytest.MonkeyPatch()
    captured = {}
    try:
        original_service = actor_fixture.CausalInferenceService

        def capture_service(*args, **kwargs):
            original_factory = kwargs["source_factory"]

            def source_factory(source_root):
                source = original_factory(source_root)
                source.live_fixture_marker = True
                captured["source"] = source
                original_close = source.close
                close_threads = captured["close_threads"] = []

                def failed_then_noop():
                    close_threads.append(threading.current_thread())
                    original_close()  # Existing fake source logs only; no DB work.
                    if len(close_threads) == 1:
                        raise RuntimeError("explicit fake source close outcome unknown")
                    return None  # No physical probe/ACK; marker remains live.

                source.close = failed_then_noop
                return source

            kwargs["source_factory"] = source_factory
            captured["service"] = original_service(*args, **kwargs)
            return captured["service"]

        monkeypatch.setattr(actor_fixture, "CausalInferenceService", capture_service)
        service, state = actor_fixture.create(root, monkeypatch, close_timeout_s=0.05)
        actor_fixture.activate(service)
        original_thread, original_journal = service.thread, service.store
        source = captured["source"]
        with pytest.raises(StorageError, match="deadline"):
            service.close()
        assert len(captured["close_threads"]) >= 2  # First error and immediate no-op retry.
        assert source.live_fixture_marker is True
        assert any(resource is source for resource in service._retained_resources)
        assert any(resource is source for resource in service._quarantined)
        assert service.thread is original_thread and original_thread.is_alive()
        assert service.store is original_journal and not original_journal.closed
        assert not _binding(service, "journal")["file_lease_confirmed"]
        _assert_journal_excludes_another_owner(root)
        prior_attempts = len(captured["close_threads"])
        with pytest.raises(StorageError, match="deadline"):
            service.close()
        assert len(captured["close_threads"]) > prior_attempts
        assert any(resource is source for resource in service._quarantined)
        assert source.live_fixture_marker and service.store is original_journal
        assert all(owner is original_thread for owner in captured["close_threads"])
        assert len(state["workers"]) == 1 and state["workers"][0].closed
        assert _binding(service, "nn_state")["file_lease_confirmed"]
        _assert_journal_excludes_another_owner(root)
        _write_host_result(
            result_path,
            {
                "scope": "contained_fake_source_noop_is_not_physical_ack",
                "outcome": "CONTAINED_HELD",
                "original_actor_alive": original_thread.is_alive(),
                "journal_lock_excluded": True,
                "same_original_source_retained": True,
                "source_live_fixture_marker": source.live_fixture_marker,
                "host_exit_is_product_ack": False,
                "source_retirement_confirmed": False,
            },
        )
        os._exit(0)
    except BaseException:
        _write_host_result(result_path, {"outcome": "FAILED", "traceback": traceback.format_exc()})
        os._exit(1)


def _observe_held_host(tmp_path, target, *arguments):
    root = tmp_path / "contained-actor"
    root.mkdir()
    result_path = tmp_path / "held-result.json"
    context = multiprocessing.get_context("spawn")
    host = context.Process(
        target=target,
        args=(root, *arguments, result_path),
        daemon=True,
    )
    native_exit = None
    try:
        host.start()
        host.join(timeout=12)
        if host.is_alive():
            host.terminate()
            host.join(timeout=2)
            pytest.fail("bounded containing file-lease host exceeded its observation budget")
        native_exit = host.exitcode
        result = json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else {}
        assert native_exit == 0 and result.get("outcome") == "CONTAINED_HELD", result
        assert result["original_actor_alive"] and result["journal_lock_excluded"]
        assert result["host_exit_is_product_ack"] is False
        if "unknown_binding" in result:
            assert not result["unknown_binding"]["file_lease_confirmed"]
        else:
            assert result["same_original_source_retained"]
            assert result["source_retirement_confirmed"] is False
        # OS process teardown permits a new independent test owner, without
        # retroactively confirming any held receipt from the exited actor.
        store = FileInferenceStateStore(root / "state/causal-inference.json")
        store.close()
    finally:
        if host.pid is not None and not host._closed:
            if host.is_alive():
                host.kill()
                host.join(timeout=2)
            assert not host.is_alive()
            native_exit = host.exitcode
            host.close()
        (tmp_path / "host-native-exit.json").write_text(
            json.dumps({"native_exit": native_exit, "product_retirement_confirmed": False}),
            encoding="utf-8",
        )


@pytest.mark.parametrize("role", ["nn_state", "delivery"])
@pytest.mark.parametrize("constructor_failure", [False, True])
def test_persistent_nonjournal_unknown_holds_original_actor_and_journal_in_containing_host(
    tmp_path, role, constructor_failure
):
    _observe_held_host(tmp_path, _held_nonjournal_host, role, constructor_failure)


def test_generic_source_failed_close_then_noop_never_drops_original_quarantine(tmp_path):
    _observe_held_host(tmp_path, _held_generic_source_host)


@pytest.mark.parametrize("interruption", [KeyboardInterrupt, SystemExit])
def test_actor_store_read_baseexception_records_fault_and_never_returns_clean_ack(
    tmp_path, interruption
):
    stores, read_threads, close_proofs, worker_calls = [], [], [], []

    def journal_factory(path):
        store = FileInferenceStateStore(path, role="journal")
        stores.append(store)
        original_close = store.close

        def interrupted_read():
            read_threads.append(threading.current_thread())
            raise interruption("explicit actor initialization interrupt fixture")

        def observed_close():
            result = original_close()
            close_proofs.append(store.retirement_status())
            return result

        store.read, store.close = interrupted_read, observed_close
        return store

    def forbidden_worker(*args, **kwargs):
        worker_calls.append((args, kwargs))
        pytest.fail("an interrupted actor initialization must not create an NN worker")

    service = CausalInferenceService(
        tmp_path,
        core_epoch="interrupted-core",
        training_job_provider=lambda _: pytest.fail("must not load a training artifact"),
        store_factory=journal_factory,
        worker_factory=forbidden_worker,
    )
    try:
        with pytest.raises(StorageError):
            service.start()
        actor_fixture.wait(lambda: service.closed)
        view = service.snapshot()
        assert view["state"] == "recording_unconfirmed" and not view["ready"]
        assert view["error"]["manual_required"] is True
        assert view["error"]["code"] == "CAUSAL_INFERENCE_UNAVAILABLE"
        assert read_threads == [service.thread]
        assert service.thread is not threading.current_thread()
        assert not worker_calls and service.owner is None
        assert len(stores) == 1 and stores[0].closed
        assert close_proofs and all(proof.file_lease_confirmed for proof in close_proofs)
        assert service.file_lease_status()["restoration_authorized"] is False
    finally:
        with pytest.raises(StorageError):
            service.close()
    assert not service.thread.is_alive()
    reopened = FileInferenceStateStore(tmp_path / "state/causal-inference.json", role="journal")
    reopened.close()


def test_joined_thread_without_final_actor_cleanup_ack_cannot_report_durable_close(tmp_path):
    service = CausalInferenceService(
        tmp_path,
        core_epoch="missing-final-ack",
        training_job_provider=lambda _: pytest.fail("no actor initialization was requested"),
    )
    # Controlled dead thread, never an actor loop: no native/file/model resources
    # exist and no flags are reset to manufacture a cleanup acknowledgement.
    service.thread = threading.Thread(target=lambda: None, daemon=True)
    service.thread.start()
    service.thread.join(timeout=1)
    assert not service.thread.is_alive() and not service.closed
    assert service.store is None and service._file_leases is None
    with pytest.raises(StorageError, match="final cleanup acknowledgement"):
        service.close()
    assert not service.closed and service.error is None
