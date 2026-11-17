"""Real native transport and real actor/files with explicitly fake model/source ports.

Custom spawned producers and a data-only activation exercise transport only.
Actor cases reuse the existing synthetic-source/fake-worker fixture. Neither
group proves model inference, actual PG provenance, finance or restoration.
"""

import json
import multiprocessing
import os
import struct
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from decimal import Decimal as D
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import test_causal_inference_service as actor_fixture

from omega.domain.errors import ProtocolError, StorageError
from omega.domain.inference import CAUSAL_SCOPE, InferenceReceipt, digest, encoded
from omega.domain.models import Candle
from omega.runtime import causal_nn_worker as worker_module
from omega.runtime.causal_nn_proof import wall_ms
from omega.runtime.causal_nn_worker import CausalInferenceProcessWorker
from omega.runtime.causal_process_resources import OriginalProcessResources
from omega.runtime.causal_receipt_channel import CausalReceiptChannel, ReceiptBatch


def _transport_producer(incoming, outgoing, ready, mode, messages, exitcode):
    # Independent lifetime bound survives an abrupt test-host failure. Expiry is
    # a native failure (93), never a successful original-resource cleanup ACK.
    watchdog = threading.Timer(10, os._exit, args=(93,))
    watchdog.daemon = True
    watchdog.start()
    try:
        incoming._writer.close()
        outgoing._reader.close()
        if mode == "complete":
            for message in messages:
                outgoing.put(message)
            outgoing.close()
            outgoing.join_thread()
            ready.set()
        else:
            assert incoming.get() == "transport-only-handshake"
            if mode == "truncated":
                # POSIX only: account for one Queue item, advertise16bytes, send1.
                # This is deliberate raw framing, never normal model Queue.put.
                assert os.name == "posix"
                assert outgoing._sem.acquire(timeout=2)
                outgoing._writer._send(struct.pack("!i", 16) + b"x")
            ready.set()
            while incoming.get() is not None:
                pass
    finally:
        watchdog.cancel()
        watchdog.join(timeout=1)
        if watchdog.is_alive():
            os._exit(94)
    raise SystemExit(exitcode)


def _transport_receipts(count):
    now = wall_ms()
    return tuple(
        InferenceReceipt(
            CAUSAL_SCOPE,
            digest({"transport_fixture": index}),
            digest({"transport_fixture": index}),
            now,
            "complete",
            encoded({"values": [index + 0.25], "state": [index + 1]}),
        )
        for index in range(count)
    )


@contextmanager
def _native_transport(
    tmp_path, *, mode="complete", messages=(), exitcode=0, capacity=8, wakeup=None
):
    input_root = tmp_path / "transport-only-inputs"
    input_root.mkdir()
    # No activation/model verification claim: the custom child receives only
    # transport payloads. The unchanged worker constructor gets a data-only port.
    worker = CausalInferenceProcessWorker(
        SimpleNamespace(verify=lambda: None), input_root, tmp_path / "execution", capacity=capacity
    )
    if wakeup is not None:
        worker.set_wakeup(wakeup)
    context = multiprocessing.get_context("spawn")
    worker._incoming = context.Queue(maxsize=capacity)
    worker._outgoing = context.Queue(maxsize=capacity + 1)
    ready = context.Event()
    process = context.Process(
        target=_transport_producer,
        args=(worker._incoming, worker._outgoing, ready, mode, messages, exitcode),
        daemon=True,
    )
    worker._process = process
    native_receive_entered = threading.Event()
    capsule = channel = final = None
    cleanup_error = None
    try:
        process.start()
        capsule = worker._resources = OriginalProcessResources(
            process, worker._incoming, worker._outgoing
        )
        capsule.close_unused_parent_ends()
        original_recv = worker._outgoing._recv_bytes

        def observed_native_receive():
            native_receive_entered.set()
            return original_recv()

        worker._outgoing._recv_bytes = observed_native_receive
        channel = worker._receipt_channel = CausalReceiptChannel(
            worker._outgoing.get, capacity=capacity, on_available=worker._wakeup
        )
        channel.start()
        if mode != "complete":
            capsule.put_owned("incoming", "transport-only-handshake")
        assert ready.wait(5), "original producer did not publish its native milestone"
        # This fixture manually installs native objects rather than invoking
        # default _start. Its trusted child Event follows role-end closure and
        # complete-frame flush/handshake; it is an explicit fixture-only transfer
        # milestone, not a substitute READY or a model/session cleanup claim.
        worker._ownership_transfer_confirmed = True
        assert native_receive_entered.wait(2)
        yield (
            worker,
            SimpleNamespace(
                process=process,
                capsule=capsule,
                channel=channel,
                native_receive_entered=native_receive_entered,
            ),
        )
    finally:
        try:
            if not process._closed and process.pid is not None:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=2)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=2)
                assert not process.is_alive(), "original producer cleanup remains unconfirmed"
            if channel is not None:
                assert channel.close(2), "original reader cleanup remains unconfirmed"
            if capsule is not None:
                final = capsule.retire(timeout_s=2)
                assert final.status == "physical_retired", final
            else:
                # Startup failed before installing evidence. Test-owned cleanup
                # cannot be reported as a positive capsule retirement receipt.
                if process.pid is not None and not process._closed:
                    process.close()
                for original_queue in (worker._incoming, worker._outgoing):
                    original_queue.close()
                    if original_queue._thread is not None:
                        original_queue._thread.join(timeout=2)
                        assert not original_queue._thread.is_alive()
                    original_queue._reader.close()
                    original_queue._writer.close()
        except BaseException as error:
            cleanup_error = f"{type(error).__name__}: {error}"
            raise
        finally:
            (tmp_path / "native-transport-cleanup.json").write_text(
                json.dumps(
                    {
                        "scope": "transport_only_custom_spawn_no_model_or_db",
                        "platform": sys.platform,
                        "mode": mode,
                        "cleanup_receipt": None if final is None else asdict(final),
                        "cleanup_error": cleanup_error,
                        "test_cleanup_confirmed": cleanup_error is None
                        and final is not None
                        and final.status == "physical_retired",
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )


def _seed_pending(worker, receipts):
    deadline = wall_ms() + 10000
    for receipt in receipts:
        worker._pending[receipt.request_id] = SimpleNamespace(
            request_id=receipt.request_id, deadline_ms=deadline
        )


def _assert_host_retired(worker, original):
    status = worker.retirement_status()
    assert status["confirmed"] and not status["worker_alive"]
    assert status["host_resources_confirmed"] and status["receipt_reader_retired"]
    assert worker._process is original.process
    assert worker._resources is original.capsule
    assert worker._receipt_channel is original.channel
    assert worker._host_retirement.status == "physical_retired"
    assert not worker._host_retirement.normal_model_cleanup_confirmed
    assert not worker._host_retirement.all_kernel_resources_confirmed
    assert all(
        row.reader_close_ack and row.writer_close_ack for row in worker._host_retirement.queues
    )


@pytest.mark.parametrize("exitcode,prior_cleanup_fault", [(0, False), (7, False), (0, True)])
def test_native_complete_fifo_survives_exit_and_existing_cleanup_fault(
    tmp_path, exitcode, prior_cleanup_fault
):
    receipts = _transport_receipts(2)
    messages = tuple((row.request_id, row, None) for row in receipts)
    with _native_transport(tmp_path, messages=messages, exitcode=exitcode) as (worker, original):
        _seed_pending(worker, receipts)
        original.process.join(timeout=5)
        assert original.process.exitcode == exitcode
        assert original.channel._reader_done.wait(2)
        worker._drain()
        assert tuple(worker._results) == tuple(row.request_id for row in receipts)
        assert tuple(worker.receipt(row.request_id) for row in receipts) == receipts
        assert worker.transport_status()["retired"]
        worker._cleanup_unconfirmed = prior_cleanup_fault  # Existing sticky flag, no model claim.
        if exitcode or prior_cleanup_fault:
            with pytest.raises(StorageError):
                worker.close()
            assert not worker.retirement_status()["cleanup_confirmed"]
        else:
            worker.close()
        _assert_host_retired(worker, original)


@pytest.mark.parametrize("mode", ["idle", "truncated"])
def test_live_native_receive_never_blocks_owner_drain_or_original_cancel(tmp_path, mode):
    if mode == "truncated" and os.name != "posix":
        pytest.skip("POSIX raw framing is never injected into Windows message-mode pipes")
    receipt = _transport_receipts(1)[0]
    with _native_transport(tmp_path, mode=mode) as (worker, original):
        _seed_pending(worker, (receipt,))
        assert original.process.is_alive() and original.channel._reader.is_alive()
        assert not original.channel._reader_done.is_set()
        with pytest.raises(ProtocolError):
            worker.set_wakeup(lambda: None)
        foreign_errors = []

        def foreign_owner():
            for action in (lambda: worker.receipt(receipt.request_id), worker.close):
                try:
                    action()
                except ProtocolError as error:
                    foreign_errors.append(str(error))

        foreign = threading.Thread(target=foreign_owner, daemon=True)
        foreign.start()
        foreign.join(timeout=1)
        assert not foreign.is_alive() and len(foreign_errors) == 2
        assert not worker.closed and not worker.retired
        with patch.object(
            worker._outgoing, "get_nowait", side_effect=AssertionError("owner native receive")
        ):
            before = time.monotonic()
            assert worker.receipt(receipt.request_id) is None
            assert worker.transport_status() == {"closed": False, "retired": False, "failure": None}
            assert time.monotonic() - before < 1
            assert receipt.request_id in worker._pending
            worker.cancel(receipt.request_id)
            assert time.monotonic() - before < 5
        assert original.channel._reader_done.wait(2)
        assert worker.receipt(receipt.request_id).status == "unknown"
        worker.close()
        _assert_host_retired(worker, original)
        assert worker._host_retirement.queues[0].feeder == "joined"


def test_dead_producer_does_not_overtake_second_complete_reader_handoff(tmp_path):
    receipts = _transport_receipts(2)
    messages = tuple((row.request_id, row, None) for row in receipts)
    first_handoff, release = threading.Event(), threading.Event()
    notifications = []

    def pause_first_handoff():
        notifications.append(threading.current_thread())
        if len(notifications) == 1:
            first_handoff.set()
            assert release.wait(2), "test owner did not release original first handoff"

    try:
        with _native_transport(
            tmp_path, messages=messages, exitcode=7, wakeup=pause_first_handoff
        ) as (worker, original):
            _seed_pending(worker, receipts)
            try:
                assert first_handoff.wait(2)
                original.process.join(timeout=1)
                assert original.process.exitcode == 7
                assert worker.receipt(receipts[0].request_id) == receipts[0]
                assert worker.receipt(receipts[1].request_id) is None
                assert receipts[1].request_id in worker._pending
                status = worker.transport_status()
                assert not status["retired"] and status["failure"]
                with pytest.raises(ProtocolError):
                    worker.submit(SimpleNamespace(request_id="f" * 64), "null")
                assert worker._process is original.process
            finally:
                release.set()
            assert original.channel._reader_done.wait(2)
            assert worker.receipt(receipts[1].request_id) == receipts[1]
            assert worker.transport_status()["retired"]
            with pytest.raises(StorageError):
                worker.close()
            _assert_host_retired(worker, original)
            assert not worker.retirement_status()["cleanup_confirmed"]
    finally:
        release.set()


@pytest.mark.parametrize("kind", ["malformed", "overflow"])
def test_native_transport_fault_is_unknown_without_replay_or_new_child(tmp_path, kind):
    receipts = _transport_receipts(3 if kind == "overflow" else 1)
    messages = (
        tuple((row.request_id, row, None) for row in receipts)
        if kind == "overflow"
        else ("malformed-complete-envelope",)
    )
    with _native_transport(tmp_path, messages=messages, capacity=1) as (worker, original):
        _seed_pending(worker, receipts)
        original.process.join(timeout=5)
        assert original.process.exitcode == 0
        assert original.channel._reader_done.wait(2)
        if kind == "overflow":
            assert original.channel.retirement_status().error.kind == "overflow"
            assert worker.receipt(receipts[0].request_id) == receipts[0]
            assert worker.receipt(receipts[1].request_id) == receipts[1]
        assert worker.receipt(receipts[-1].request_id).status == "unknown"
        assert not worker._pending and worker.transport_status()["retired"]
        with pytest.raises(ProtocolError):
            worker.submit(SimpleNamespace(request_id="f" * 64), "null")
        assert worker._process is original.process
        worker.close()
        _assert_host_retired(worker, original)


class _HealthWorker(actor_fixture.Worker):
    """Existing fake receipt semantics plus a thread-safe, explicit health port."""

    def __init__(self, *args):
        super().__init__(*args)
        self._health_lock = threading.Lock()
        self._failure, self._retired, self._wakeup = None, False, None
        self._allow_receipts = threading.Event()
        self._post_poll_armed = False

    def set_wakeup(self, callback):
        assert threading.get_ident() == self.thread and self._wakeup is None
        self._wakeup = callback

    def fail_from_reader(self, *, release_complete=False):
        assert threading.get_ident() != self.thread
        with self._health_lock:
            self._failure, self._retired = "explicit fake transport fault", True
            if release_complete:
                self._allow_receipts.set()
        self.state["reader_callback_thread"] = threading.get_ident()
        self._wakeup()

    def receipt(self, key):
        if self.state.get("post_poll_terminal") and not self._post_poll_armed:
            if key in self.requests:
                self._post_poll_armed = True
                return None
        if self.state.get("hold_receipts") and not self._allow_receipts.is_set():
            return None
        return super().receipt(key)

    def transport_status(self):
        assert threading.get_ident() == self.thread
        if self.state.get("malformed_health"):
            return {"closed": "false", "retired": False, "failure": None}
        with self._health_lock:
            if self._post_poll_armed and self.state.get("post_poll_terminal"):
                self._failure, self._retired = "terminal became visible after poll", True
                self._allow_receipts.set()
            return {"closed": self.closed, "retired": self._retired, "failure": self._failure}

    def close(self):
        if self._failure is not None:
            journal = _actor_journal(self.state["root"])
            paused = [row for row in journal["deployments"].values() if row["state"] == "paused"]
            assert len(paused) == 1
            assert paused[0]["error"]["code"] == "CAUSAL_INFERENCE_TRANSPORT_UNAVAILABLE"
            self.state["durable_pause_before_worker_close"] = True
        super().close()


def _actor_journal(root):
    return json.loads((root / "state/causal-inference.json").read_text(encoding="utf-8"))[
        "snapshot"
    ]


def _create_health_actor(tmp_path, monkeypatch):
    monkeypatch.setattr(actor_fixture, "Worker", _HealthWorker)
    service, state = actor_fixture.create(tmp_path, monkeypatch)
    try:
        deployment = actor_fixture.activate(service)
        assert actor_fixture.reset(service, deployment)["state"] == "complete"
        return service, state, deployment, state["workers"][0]
    except BaseException:
        service.close()
        raise


def _reader_fault(worker, *, release_complete=False):
    failures = []

    def fail():
        try:
            worker.fail_from_reader(release_complete=release_complete)
        except BaseException as error:
            failures.append(f"{type(error).__name__}: {error}")

    reader = threading.Thread(target=fail, name="test-receipt-notifier", daemon=True)
    reader.start()
    reader.join(timeout=2)
    assert not reader.is_alive() and not failures
    return reader


def _assert_paused_actor(service, state, deployment, worker):
    actor_fixture.wait(lambda: worker.closed)
    row = next(item for item in service.snapshot()["deployments"] if item["id"] == deployment)
    assert row["state"] == "paused" and row["manual_required"]
    assert row["error"]["code"] == "CAUSAL_INFERENCE_TRANSPORT_UNAVAILABLE"
    journal = _actor_journal(state["root"])
    assert journal["deployments"][deployment]["state"] == "paused"
    assert state["durable_pause_before_worker_close"]
    assert len(state["workers"]) == 1 and not service._pending
    assert service.error is None


def test_idle_actor_reader_notification_wakes_durable_pause_without_polling(tmp_path, monkeypatch):
    service, state, deployment, worker = _create_health_actor(tmp_path, monkeypatch)
    idle_wait = threading.Event()
    original_wait = service.condition.wait

    def observed_wait(timeout=None):
        if (
            threading.current_thread() is service.thread
            and timeout is None
            and not service._pending
        ):
            idle_wait.set()
        return original_wait(timeout)

    try:
        with patch.object(service.condition, "wait", side_effect=observed_wait):
            with service.condition:
                service.condition.notify_all()  # Reach the instrumented idle boundary once.
            assert idle_wait.wait(2)
            assert not service._pending and not worker.requests
            reader = _reader_fault(worker)
            _assert_paused_actor(service, state, deployment, worker)
            assert state["reader_callback_thread"] == reader.ident != service.thread.ident
            assert state["source_calls"] == 0
            assert state.get("submitted_streams", []) == []
    finally:
        service.close()


@pytest.mark.parametrize("post_poll_terminal", [False, True])
def test_actor_preserves_complete_receipt_before_transport_pause_and_blocks_future_features(
    tmp_path, monkeypatch, post_poll_terminal
):
    service, state, deployment, worker = _create_health_actor(tmp_path, monkeypatch)
    provider = service.external_features
    binding = provider.list_bindings()[0]
    candle = Candle(binding.instrument, 600000, 660000, D(100), D(102), D(99), D(101), D(2))
    state["hold_receipts"] = True
    state["post_poll_terminal"] = post_poll_terminal
    try:
        actor_fixture.predict(service, deployment)
        if not post_poll_terminal:
            actor_fixture.wait(lambda: bool(worker.requests))
            _reader_fault(worker, release_complete=True)
        operation = actor_fixture.terminal(service, "predict")
        assert operation["state"] == "complete", operation
        assert operation["result"]["feature"]["values"] == {"NN.score": "0.25"}
        _assert_paused_actor(service, state, deployment, worker)
        journal = _actor_journal(tmp_path)
        assert journal["operations"]["predict"]["state"] == "complete"
        assert journal["operations"]["predict"]["result"] == operation["result"]
        saved = tuple(provider._latest.values())
        assert len(saved) == 1 and saved[0].request_id == operation["result"]["request_id"]
        assert provider.store.load_snapshot(saved[0].snapshot_sha256) == saved[0]
        assert saved[0].values == (("NN.score", D("0.25")),)
        delivery = provider.lookup(binding, candle, decision_at_ms=state["clock"][0])
        assert delivery.disposition == "unavailable" and delivery.snapshot is None
        calls = state["source_calls"]
        actor_fixture.predict(service, deployment, key="blocked-future", opened=660000)
        assert actor_fixture.terminal(service, "blocked-future")["state"] == "failed"
        assert state["source_calls"] == calls and len(state["workers"]) == 1
        assert service.status("predict")["state"] == "complete"
    finally:
        service.close()


def test_actor_malformed_health_is_faulted_instead_of_treated_as_healthy(tmp_path, monkeypatch):
    service, state, _, worker = _create_health_actor(tmp_path, monkeypatch)
    try:
        state["malformed_health"] = True
        # An availability callback is a wakeup only; malformed metadata is read
        # and rejected by the actor rather than interpreted on this caller.
        reader = threading.Thread(target=worker._wakeup, daemon=True)
        reader.start()
        reader.join(timeout=2)
        assert not reader.is_alive()
        actor_fixture.wait(lambda: service.closed)
        assert service.snapshot()["state"] == "recording_unconfirmed"
        assert not service.snapshot()["ready"] and service.error is not None
        assert worker.closed and len(state["workers"]) == 1
        assert state.get("submitted_streams", []) == []
    finally:
        with pytest.raises(StorageError):
            service.close()


@dataclass(frozen=True)
class _BootstrapActivation:
    """Picklable data-only startup input; no model instance or dataset exists."""

    mode: str = "production_entry"
    entered: object = None
    release: object = None

    def verify(self):
        return None


def _bootstrap_producer(activation, input_root, execution_root, incoming, outgoing, nonce):
    watchdog = threading.Timer(10, os._exit, args=(93,))
    watchdog.daemon = True
    watchdog.start()
    try:
        if activation.mode == "production_entry":
            activation.entered.set()
            # A fresh spawned interpreter has the actual production _child;
            # the parent's target monkeypatch is not inherited by spawn. Only
            # trusted entry/READY/None retirement executes, never model input.
            worker_module._child(activation, input_root, execution_root, incoming, outgoing, nonce)
            return
        incoming._writer.close()
        outgoing._reader.close()
        activation.entered.set()
        if activation.mode == "late":
            assert activation.release.wait(timeout=5)
        ready = (worker_module.READY_KIND, os.getpid(), nonce)
        if activation.mode == "wrong_pid":
            ready = (worker_module.READY_KIND, os.getpid() + 1, nonce)
        elif activation.mode == "wrong_nonce":
            ready = (worker_module.READY_KIND, os.getpid(), "wrong:" + nonce)
        elif activation.mode == "wrong_shape":
            ready = {"kind": worker_module.READY_KIND, "pid": os.getpid(), "nonce": nonce}
        elif activation.mode == "wrong_order":
            outgoing.put(("unexpected-before-READY", None, None))
        elif activation.mode == "missing":
            return  # Actual EOF without any ownership marker.
        outgoing.put(ready)
        assert incoming.get() is None  # Only explicit test/worker shutdown is permitted.
    finally:
        # Flush complete test frames before stopping the independent watchdog.
        outgoing.close()
        outgoing.join_thread()
        watchdog.cancel()
        watchdog.join(timeout=1)
        if watchdog.is_alive():
            os._exit(94)


@contextmanager
def _default_bootstrap(tmp_path, monkeypatch, *, mode="production_entry", wakeup=None):
    context = multiprocessing.get_context("spawn")
    activation = _BootstrapActivation(mode, context.Event(), context.Event())
    input_root = tmp_path / "bootstrap-inputs"
    input_root.mkdir()
    worker = CausalInferenceProcessWorker(activation, input_root, tmp_path / "bootstrap-execution")
    if wakeup is not None:
        worker.set_wakeup(wakeup)
    monkeypatch.setattr(worker_module, "_child", _bootstrap_producer)
    close_error = cleanup_error = None
    final = None
    try:
        yield worker, activation
    finally:
        activation.release.set()
        try:
            process = worker._process
            if process is not None and (process._closed or process.pid is not None):
                # Native bootstrap ownership failures remain worker failures.
                # Bounded test containment can still clean its original handles
                # without setting the worker's transfer flag or granting it ACK.
                if not process._closed:
                    activation.entered.wait(timeout=5)
                    try:
                        worker.close()
                    except StorageError as error:
                        close_error = str(error)
                if not process._closed:
                    if process.is_alive():
                        process.terminate()
                        process.join(timeout=2)
                    if process.is_alive():
                        process.kill()
                        process.join(timeout=2)
                    assert not process.is_alive()
                if worker._receipt_channel is not None:
                    assert worker._receipt_channel.close(2)
                if worker._resources is not None:
                    final = worker._resources.retire(timeout_s=2)
                    assert final.status == "physical_retired", final
                    assert final.exitcode == 0, "watchdog/forced native cleanup is not success"
                else:
                    # Constructor failure has no capsule authority. Keep the
                    # cleanup local to the explicitly created test resources.
                    if not process._closed:
                        process.close()
                    for original_queue in (worker._incoming, worker._outgoing):
                        original_queue.close()
                        if original_queue._thread is not None:
                            original_queue._thread.join(timeout=2)
                            assert not original_queue._thread.is_alive()
                        original_queue._reader.close()
                        original_queue._writer.close()
        except BaseException as error:
            cleanup_error = f"{type(error).__name__}: {error}"
            raise
        finally:
            (tmp_path / "bootstrap-original-cleanup.json").write_text(
                json.dumps(
                    {
                        "scope": "startup_transport_only_no_model_or_input",
                        "mode": mode,
                        "worker_ownership_transfer_confirmed": worker._ownership_transfer_confirmed,
                        "worker_close_error": close_error,
                        "original_test_cleanup": None if final is None else asdict(final),
                        "cleanup_error": cleanup_error,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )


def _assert_no_model_input_or_request_put(worker):
    assert not worker._pending and not worker._seen
    assert worker._incoming._thread is None  # No native request feeder/put was started.
    assert tuple(worker.input_root.iterdir()) == ()
    assert tuple(worker.execution_root.iterdir()) == ()


def test_default_start_requires_actual_first_ready_before_any_model_input(tmp_path, monkeypatch):
    with _default_bootstrap(tmp_path, monkeypatch) as (worker, activation):
        worker._start()
        original = worker._process, worker._resources, worker._receipt_channel
        assert activation.entered.is_set()
        assert worker._ownership_transfer_confirmed
        assert not worker.retired and worker.transport_status()["failure"] is None
        _assert_no_model_input_or_request_put(worker)
        worker.close()
        assert worker.retirement_status()["host_resources_confirmed"]
        assert original == (worker._process, worker._resources, worker._receipt_channel)
        assert not worker._host_retirement.normal_model_cleanup_confirmed


@pytest.mark.parametrize(
    "mode", ["wrong_pid", "wrong_nonce", "wrong_shape", "wrong_order", "missing"]
)
def test_default_start_rejects_invalid_or_missing_first_ready_without_request_put(
    tmp_path, monkeypatch, mode
):
    with _default_bootstrap(tmp_path, monkeypatch, mode=mode) as (worker, activation):
        with pytest.raises(StorageError):
            worker._start()
        assert activation.entered.wait(2)
        assert worker.retired and worker._transport_failure
        _assert_no_model_input_or_request_put(worker)
        original = worker._process
        with patch.object(
            worker_module, "read_document", side_effect=AssertionError("model input")
        ):
            with pytest.raises(ProtocolError):
                worker.submit(SimpleNamespace(request_id="f" * 64), "null")
        assert worker._process is original
        _assert_no_model_input_or_request_put(worker)


@pytest.mark.parametrize("prior_cleanup_fault", [False, True])
def test_timed_out_late_ready_is_cleanup_evidence_and_never_resumes_admission(
    tmp_path, monkeypatch, prior_cleanup_fault
):
    monkeypatch.setattr(worker_module, "STARTUP_READY_TIMEOUT_S", 0.05)
    with _default_bootstrap(tmp_path, monkeypatch, mode="late") as (worker, activation):
        worker._cleanup_unconfirmed = prior_cleanup_fault
        with pytest.raises(StorageError):
            worker._start()
        assert worker.retired and not worker._ownership_transfer_confirmed
        assert not worker.retirement_status()["host_resources_confirmed"]
        _assert_no_model_input_or_request_put(worker)
        assert activation.entered.wait(5)
        activation.release.set()

        def received_late_ready():
            worker.transport_status()  # Local memory only; still retired.
            return worker._ownership_transfer_confirmed

        actor_fixture.wait(received_late_ready)
        assert worker.retired and worker._transport_failure
        with pytest.raises(ProtocolError):
            worker.submit(SimpleNamespace(request_id="f" * 64), "null")
        _assert_no_model_input_or_request_put(worker)
        if prior_cleanup_fault:
            with pytest.raises(StorageError):
                worker.close()
        else:
            worker.close()
        assert worker.closed and worker.retired
        assert worker.retirement_status()["host_resources_confirmed"]
        assert worker._cleanup_unconfirmed is prior_cleanup_fault


def test_startup_notifier_failure_cannot_publish_a_healthy_start(tmp_path, monkeypatch):
    calls = []

    def broken_notifier():
        calls.append(threading.current_thread())
        raise RuntimeError("explicit startup-notifier fault")

    with _default_bootstrap(tmp_path, monkeypatch, wakeup=broken_notifier) as (worker, _):
        with pytest.raises(StorageError):
            worker._start()
        assert worker.retired and worker._transport_failure
        assert worker._startup_notification_error == "RuntimeError"
        assert calls == [worker._receipt_channel._reader]
        _assert_no_model_input_or_request_put(worker)
        with pytest.raises(ProtocolError):
            worker.submit(SimpleNamespace(request_id="f" * 64), "null")


def _local_ready_worker(tmp_path, *, duplicate=False):
    input_root = tmp_path / "local-inputs"
    input_root.mkdir()
    worker = CausalInferenceProcessWorker(
        _BootstrapActivation(), input_root, tmp_path / "local-execution"
    )
    # Deterministic scalar/local-channel fixture, not native resource evidence.
    worker._process = SimpleNamespace(pid=12345, is_alive=lambda: True, exitcode=None)
    worker._outgoing = object()
    worker._bootstrap_expected = True
    ready = (worker_module.READY_KIND, 12345, worker._transport_nonce)
    messages = (ready, ready) if duplicate else (ready,)
    worker._receipt_channel = SimpleNamespace(drain=lambda: ReceiptBatch(messages, None, False))
    return worker


def test_ready_observed_after_startup_deadline_acknowledges_only_transfer(tmp_path, monkeypatch):
    worker = _local_ready_worker(tmp_path)
    monkeypatch.setattr(worker_module, "STARTUP_READY_TIMEOUT_S", 3.0)
    with patch.object(worker_module.time, "monotonic", side_effect=[100.0, 104.0]):
        with pytest.raises(StorageError):
            worker._await_transport_ready()
    assert worker._ownership_transfer_confirmed
    assert worker.retired and worker._transport_failure
    assert not worker._pending and worker._incoming is None
    assert tuple(worker.input_root.iterdir()) == ()


def test_duplicate_ready_is_retired_without_admitting_any_request(tmp_path):
    worker = _local_ready_worker(tmp_path, duplicate=True)
    with pytest.raises(StorageError):
        worker._await_transport_ready()
    assert worker._ownership_transfer_confirmed
    assert worker.retired and "duplicate" in worker._transport_failure
    assert not worker._pending and worker._incoming is None


def test_rejected_first_marker_cannot_gain_transfer_from_later_exact_ready(tmp_path):
    worker = _local_ready_worker(tmp_path)
    ready = (worker_module.READY_KIND, worker._process.pid, worker._transport_nonce)
    batches = iter(
        (
            ReceiptBatch((("invalid-first-marker", None, None),), None, False),
            ReceiptBatch((ready,), None, False),
        )
    )
    worker._receipt_channel = SimpleNamespace(drain=lambda: next(batches))
    with pytest.raises(StorageError):
        worker._await_transport_ready()
    assert worker._bootstrap_marker_rejected and worker.retired
    assert not worker._ownership_transfer_confirmed
    worker._drain()
    assert worker._bootstrap_marker_rejected and worker.retired
    assert not worker._ownership_transfer_confirmed
    assert not worker._pending and worker._incoming is None
