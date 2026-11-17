"""Reusable child-only external NN execution for verified causal OHLCV records.

No model runs in the caller. This worker is an injected port, not a scheduler or
TS integration. Cancellation retires the child and marks all its queued work
unknown; recurrent work is never transparently retried in another process.
"""

# Imports
import inspect
import math
import multiprocessing
import os
import queue
import threading
import time
from pathlib import Path
from types import FunctionType, MethodType
from uuid import uuid4

from omega.domain.errors import ProtocolError, StorageError
from omega.domain.inference import CAUSAL_SCOPE, InferenceReceipt, decoded, digest, encoded
from omega.research.artifacts import validate_artifact_path
from omega.research.prediction import PredictionSession
from omega.runtime.causal_nn_proof import read_document, validate_input_document, wall_ms
from omega.runtime.causal_process_resources import OriginalProcessResources
from omega.runtime.causal_receipt_channel import CausalReceiptChannel
from omega.runtime.causal_worker_retirement import (
    WorkerTransportRetirement,
    host_physical_confirmed,
    validate_host_retirement,
    validate_receipt_retirement,
)

# Constants
READY_KIND = "causal_transport_ready_v1"
STARTUP_READY_TIMEOUT_S = 3.0


def _batch(value, shape):
    if not shape:
        if type(value) not in {int, float} or not math.isfinite(value):
            raise ValueError("prepared causal batch must contain finite numeric values")
        return
    if type(value) is not list or len(value) != (1 if shape[0] is None else shape[0]):
        raise ValueError("prepared causal batch shape mismatch")
    for item in value:
        _batch(item, shape[1:])


def _execute(session, activation, input_root, request, previous_state_json):
    """Consume a pinned record, never prepare_run's retrospective CSV pathway."""
    activation.verify()
    path = input_root / (request.request_id + ".json")
    document = read_document(path)
    rows = validate_input_document(document, request, activation, now_ms=wall_ms())
    state = decoded(previous_state_json, maximum=65536)
    if digest(state) != request.previous_state_sha256:
        raise ValueError("causal previous-state identity mismatch")
    contract = activation.contract
    if contract.state_shape is None:
        if state is not None:
            raise ValueError("stateless causal model received recurrent state")
    elif state is not None:
        _batch(state, contract.state_shape)
    # Retain the module hook's existing contract vocabulary. The enclosing input
    # record binds this request to the full three epochs, hash and decision time.
    hook_request = request.document() | {
        "contract": contract.document(),
        "instrument": request.frame.instrument.key,
        "open_time_ms": request.frame.open_time_ms,
    }
    prepared = session.instance.prepare_inference(rows, hook_request, session.context)
    encoded(prepared, maximum=262144)
    if (
        type(prepared) is not dict
        or set(prepared) != {"batch", "used_rows"}
        or type(prepared["used_rows"]) is not list
        or any(type(v) is not int for v in prepared["used_rows"])
        or prepared["used_rows"] != list(range(len(rows)))
    ):
        raise ValueError("causal preprocessor must declare the exact complete observed window")
    _batch(prepared["batch"], contract.input_shape)
    if read_document(path) != document:
        raise ValueError("causal input replaced during preprocessing")
    validate_input_document(document, request, activation, now_ms=wall_ms())
    # The reused loader returns an offline envelope. Only the actual module's
    # values/state are reused; its scope is never promoted into causal evidence.
    result = session.predict(prepared["batch"], state)["prediction"]
    contract.validate_prediction(result, 1)
    encoded(result, maximum=262144)
    activation.verify()
    if read_document(path) != document:
        raise ValueError("causal input replaced during prediction")
    validate_input_document(document, request, activation, now_ms=wall_ms())
    return InferenceReceipt(
        CAUSAL_SCOPE,
        request.request_id,
        request.request_id,
        wall_ms(),
        "complete",
        encoded(result, maximum=262144),
    )


def _child(activation, input_root, execution_root, incoming, outgoing, transport_nonce):
    session = None
    try:
        # Each spawned process owns separate endpoint objects. Unused duplicates
        # must not hide EOF after the actual sender/receiver exits.
        incoming._writer.close()
        outgoing._reader.close()
        # In Windows spawn, unpickling steals parent-side DupHandle copies.
        # Only this trusted entry milestone permits parent-side cancellation.
        outgoing.put((READY_KIND, os.getpid(), transport_nonce))
        while True:
            message = incoming.get()  # Event-driven; no idle polling loop.
            if message is None:
                break
            request, state_json = message
            try:
                manifest = activation.verify()
                # Reject unproven/stale input before importing any external code.
                validate_input_document(
                    read_document(input_root / (request.request_id + ".json")),
                    request,
                    activation,
                    now_ms=wall_ms(),
                )
                if session is None:
                    session = PredictionSession(
                        activation.root,
                        execution_root / uuid4().hex,
                        checkpoint=manifest["checkpoint"],
                        device="cpu",
                        trusted_module=True,
                    )
                    if (
                        session.module_source_sha256 != activation.model.module_sha256
                        or session.artifact["sha256"] != activation.model.checkpoint_sha256
                    ):
                        raise ValueError("restored causal model identity mismatch")
                result = _execute(session, activation, input_root, request, state_json)
                outgoing.put((request.request_id, result, None))
            except Exception as error:
                # A failed trusted instance may have mutated hidden state. Retire
                # this process rather than silently continuing peer requests.
                outgoing.put(
                    (
                        request.request_id,
                        InferenceReceipt(
                            CAUSAL_SCOPE,
                            request.request_id,
                            request.request_id,
                            wall_ms(),
                            "failed",
                            "{}",
                        ),
                        f"{type(error).__name__}: {str(error)[:384]}",
                    )
                )
                break
    finally:
        try:
            if session is not None:
                session.close()
        finally:
            outgoing.put((None, None, "child_retired"))


class CausalInferenceProcessWorker:
    """Bounded parent port; instantiate lazily and never execute plugin code here."""

    def __init__(self, activation, input_root, execution_root, *, capacity=8):
        if type(capacity) is not int or not 1 <= capacity <= 32:
            raise ValueError("causal worker capacity must be 1..32")
        self.activation = activation
        activation.verify()
        self.input_root = validate_artifact_path(Path(input_root))
        self.execution_root = validate_artifact_path(Path(execution_root), allow_missing=True)
        self.execution_root.mkdir(parents=True, exist_ok=False)
        self.capacity, self.closed, self.retired = capacity, False, False
        self._cancelled = False
        self._thread = threading.get_ident()
        self._owner_thread = threading.current_thread()
        self._process = self._incoming = self._outgoing = None
        self._resources = self._receipt_channel = self._host_retirement = None
        self._wakeup = None
        self._transport_failure = None
        self._transport_nonce = uuid4().hex
        self._bootstrap_expected = self._ownership_transfer_confirmed = False
        self._bootstrap_marker_rejected = False
        self._startup_available = threading.Event()
        self._startup_notification_error = None
        self._process_retired, self._channels_closed = False, False
        self._cleanup_unconfirmed = False
        self._pending, self._results, self._seen = {}, {}, {}
        self.errors = {}
        # Independent evidence retains originals; legacy status/cleanup remains
        # unchanged and never supplies this typed record's physical authority.
        self._transport_owner_pid = os.getpid()
        self._transport_owner_thread = threading.current_thread()
        self._transport_worker_token = uuid4().hex
        self._transport_channel_token = None
        self._transport_start_attempted = self._transport_close_attempted = False
        self._transport_capture_failed = self._transport_identity_unknown = False
        self._transport_snapshot_running = False
        self._transport_original_process = self._transport_original_incoming = None
        self._transport_original_outgoing = self._transport_original_resources = None
        self._transport_original_channel = self._transport_original_reader = None
        self._transport_original_receive = self._transport_original_getter = None
        self._transport_original_getter_shape = self._transport_capsule_token = None
        self._transport_retire_shape = self._transport_original_endpoints = None
        self._transport_shapes = tuple(
            (name, inspect.getattr_static(self, name))
            for name in ("close", "_start", "transport_retirement_snapshot")
        )

    def _guard(self):
        if self.closed or not self._on_owner_thread():
            raise ProtocolError("causal worker is closed or outside its owner thread")

    def _on_owner_thread(self):
        owner = getattr(self, "_owner_thread", None)
        return (
            threading.current_thread() is owner
            if owner is not None
            else threading.get_ident() == self._thread
        )

    def set_wakeup(self, callback):
        self._guard()
        if not callable(callback) or self._process is not None:
            raise ProtocolError("causal wakeup must be attached before native startup")
        if self._wakeup is not None:
            raise ProtocolError("causal wakeup cannot be replaced")
        self._wakeup = callback

    def _start(self):
        # A failed first Queue/start must not later look definitively lazy.
        if self._transport_start_attempted:
            self._transport_identity_unknown = True
        self._transport_start_attempted = True
        context = multiprocessing.get_context("spawn")
        self._incoming = context.Queue(maxsize=self.capacity)
        self._remember_transport("incoming")
        self._outgoing = context.Queue(maxsize=self.capacity + 1)
        self._remember_transport("outgoing")
        self._process = context.Process(
            target=_child,
            args=(
                self.activation,
                self.input_root,
                self.execution_root,
                self._incoming,
                self._outgoing,
                self._transport_nonce,
            ),
            daemon=True,
        )
        self._remember_transport("process")
        self._bootstrap_expected = True
        self._process.start()
        self._resources = OriginalProcessResources(self._process, self._incoming, self._outgoing)
        self._remember_transport("capsule")
        self._resources.close_unused_parent_ends()
        self._receipt_channel = CausalReceiptChannel(
            self._outgoing.get, capacity=self.capacity, on_available=self._transport_available
        )
        self._remember_transport("channel")
        self._receipt_channel.start()
        self._await_transport_ready()

    def _transport_owner(self):
        state = vars(self)
        if os.getpid() != state.get(
            "_transport_owner_pid"
        ) or threading.current_thread() is not state.get("_transport_owner_thread"):
            raise ProtocolError("transport evidence requires the original PID and Thread object")

    @property
    def transport_worker_token(self):
        self._transport_owner()
        return self._transport_worker_token

    def _remember_transport(self, phase):
        self._transport_owner()  # Before getter lookup, mutation or callback.
        try:
            names = {
                "incoming": ("_transport_original_incoming", "_incoming"),
                "outgoing": ("_transport_original_outgoing", "_outgoing"),
                "process": ("_transport_original_process", "_process"),
                "capsule": ("_transport_original_resources", "_resources"),
                "channel": ("_transport_original_channel", "_receipt_channel"),
            }
            original_name, current_name = names[phase]
            original, current = getattr(self, original_name), getattr(self, current_name)
            if original is not None:
                if current is not original:
                    self._transport_identity_unknown = True
                return  # A later attempt never overwrites an original binding.
            if phase == "incoming":
                self._transport_original_incoming = self._incoming
            elif phase == "outgoing":
                self._transport_original_outgoing = self._outgoing
            elif phase == "process":
                self._transport_original_process = self._process
            elif phase == "capsule":
                capsule = self._transport_original_resources = self._resources
                if type(capsule) is not OriginalProcessResources:
                    raise ValueError("exact original transport capsule required")
                self._transport_capsule_token = capsule.resource_token
                self._transport_retire_shape = inspect.getattr_static(capsule, "retire")
                self._transport_original_endpoints = tuple(
                    (value._reader, value._writer)
                    for value in (
                        self._transport_original_incoming,
                        self._transport_original_outgoing,
                    )
                )
            elif phase == "channel":
                channel = self._transport_original_channel = self._receipt_channel
                if type(channel) is not CausalReceiptChannel:
                    raise ValueError("exact original receipt channel required")
                self._transport_original_reader = channel._reader
                self._transport_original_receive = channel._receive
                self._transport_original_getter_shape = inspect.getattr_static(
                    channel, "retirement_status"
                )
                self._transport_original_getter = channel.retirement_status
                self._transport_channel_token = uuid4().hex
        except BaseException:
            # Evidence capture cannot replace the original startup operation.
            self._transport_capture_failed = True

    def _transport_identity(self):
        shapes = self._transport_shapes
        if (
            type(shapes) is not tuple
            or len(shapes) != 3
            or any(type(item) is not tuple or len(item) != 2 for item in shapes)
            or any(
                type(name) is not str or type(shape) is not FunctionType for name, shape in shapes
            )
            or tuple(item[0] for item in shapes)
            != ("close", "_start", "transport_retirement_snapshot")
        ):
            return False
        if type(self) is not CausalInferenceProcessWorker or any(
            inspect.getattr_static(self, name) is not shape
            for name, shape in self._transport_shapes
        ):
            return False
        if (
            self._owner_thread is not self._transport_owner_thread
            or self._process is not self._transport_original_process
            or self._incoming is not self._transport_original_incoming
            or self._outgoing is not self._transport_original_outgoing
            or self._resources is not self._transport_original_resources
            or self._receipt_channel is not self._transport_original_channel
        ):
            return False
        capsule = self._transport_original_resources
        if capsule is not None:
            if (
                type(capsule) is not OriginalProcessResources
                or type(capsule.resource_token) is not str
                or capsule.resource_token != self._transport_capsule_token
                or inspect.getattr_static(capsule, "retire") is not self._transport_retire_shape
                or capsule.process is not self._transport_original_process
                or capsule.incoming is not self._transport_original_incoming
                or capsule.outgoing is not self._transport_original_outgoing
            ):
                return False
            endpoints = self._transport_original_endpoints
            if (
                type(endpoints) is not tuple
                or len(endpoints) != 2
                or any(type(pair) is not tuple or len(pair) != 2 for pair in endpoints)
            ):
                return False
            for original, endpoints in zip(
                (self._transport_original_incoming, self._transport_original_outgoing),
                self._transport_original_endpoints,
                strict=True,
            ):
                if original._reader is not endpoints[0] or original._writer is not endpoints[1]:
                    return False
        channel = self._transport_original_channel
        return channel is None or (
            type(channel) is CausalReceiptChannel
            and channel._reader is self._transport_original_reader
            and type(self._transport_original_reader) is threading.Thread
            and channel._receive is self._transport_original_receive
            and inspect.getattr_static(channel, "retirement_status")
            is self._transport_original_getter_shape
            and type(self._transport_original_getter) is MethodType
            and self._transport_original_getter.__self__ is channel
            and self._transport_original_getter.__func__ is self._transport_original_getter_shape
        )

    def transport_retirement_snapshot(self):
        """Owner-only internal evidence; no close/start/native process probe here."""
        self._transport_owner()
        if type(self._transport_snapshot_running) is not bool:
            self._transport_capture_failed = True
            raise ProtocolError("native transport capture marker required")
        if self._transport_snapshot_running:
            self._transport_identity_unknown = True
            raise ProtocolError("transport evidence capture cannot reenter")
        self._transport_snapshot_running = True
        host = receipt = None
        try:
            if any(
                type(value) is not bool
                for value in (
                    self._transport_close_attempted,
                    self._transport_start_attempted,
                    self._transport_capture_failed,
                    self._transport_identity_unknown,
                )
            ):
                raise ValueError("native transport evidence markers required")
            identity = self._transport_identity() and not self._transport_identity_unknown
            if not identity:
                self._transport_identity_unknown = True
            admission = self._transport_close_attempted and self.closed is True
            attempted = self._transport_start_attempted
            if type(attempted) is not bool:
                raise ValueError("native transport start marker required")
            transfer = self._ownership_transfer_confirmed
            if type(transfer) is not bool:
                raise ValueError("native transport ownership marker required")
            if attempted and identity:
                capsule, channel = (
                    self._transport_original_resources,
                    self._transport_original_channel,
                )
                if capsule is not None:
                    host = self._host_retirement
                    if host is not None:
                        validate_host_retirement(host)
                        if (
                            host is not capsule._last
                            or host.resource_token != self._transport_capsule_token
                        ):
                            raise ValueError("original host receipt binding changed")
                if channel is not None:
                    receipt = self._transport_original_getter()
                    validate_receipt_retirement(receipt)
            errors = ()
            if self._transport_capture_failed:
                errors += ("transport_evidence_capture_failed",)
            if not identity:
                errors += ("transport_original_identity_unknown",)
            if attempted and (host is None or receipt is None):
                errors += ("transport_started_original_receipts_missing",)
            never = bool(
                identity
                and admission
                and not attempted
                and not transfer
                and self._transport_original_process is None
                and self._transport_original_incoming is None
                and self._transport_original_outgoing is None
                and self._transport_original_resources is None
                and self._transport_original_channel is None
                and not errors
            )
            physical = never or bool(
                identity
                and admission
                and attempted
                and transfer
                and not errors
                and host is not None
                and host_physical_confirmed(host)
                and receipt is not None
                and receipt.closed
                and receipt.started
                and receipt.reader_done
                and receipt.join_confirmed
                and not receipt.reader_alive
            )
            return WorkerTransportRetirement(
                self._transport_worker_token,
                self._transport_channel_token,
                identity,
                admission,
                attempted,
                never,
                transfer,
                host,
                receipt,
                physical,
                "not_started" if not attempted and identity else "unconfirmed",
                errors,
            )
        except BaseException:
            self._transport_capture_failed = True
            raise
        finally:
            self._transport_snapshot_running = False

    def _transport_available(self):
        # Receipt-reader callback: no native receive, model, DB or owner mutation.
        try:
            if self._wakeup is not None:
                self._wakeup()
        except BaseException as error:
            # Record before waking the owner. Channel error publication occurs
            # after this callback raises, and must not allow a false READY.
            self._startup_notification_error = type(error).__name__[:96]
            self._startup_available.set()
            raise
        self._startup_available.set()

    def _await_transport_ready(self):
        deadline = time.monotonic() + STARTUP_READY_TIMEOUT_S
        while True:
            # Clear before draining so publication between drain/wait is kept.
            self._startup_available.clear()
            self._drain()
            if self._startup_notification_error is not None:
                self._unknown("causal startup notification failed")
            if self.retired or self._transport_failure is not None:
                raise StorageError("causal child startup transport is unavailable")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._unknown("causal child ownership transfer is unconfirmed")
                raise StorageError("causal child ownership transfer is unconfirmed")
            if self._ownership_transfer_confirmed:
                return
            self._startup_available.wait(remaining)

    def submit(self, request, previous_state_json):
        self._guard()
        self._drain()
        now = wall_ms()
        # Once a request's immutable deadline passes, the input verifier rejects
        # its replay independently. Retain only still-eligible duplicate IDs so
        # a healthy long-lived worker does not exhaust a lifetime request quota.
        self._seen = {key: deadline for key, deadline in self._seen.items() if deadline > now}
        if (
            self.retired
            or self._transport_failure is not None
            or len(self._pending) + len(self._results) >= self.capacity
            or len(self._seen) >= 4096
        ):
            raise ProtocolError("causal worker retired or bounded capacity exhausted")
        if request.request_id in self._seen:
            raise ProtocolError("causal worker request ID cannot be replayed")
        document = read_document(self.input_root / (request.request_id + ".json"))
        validate_input_document(document, request, self.activation, now_ms=wall_ms())
        if digest(decoded(previous_state_json, maximum=65536)) != request.previous_state_sha256:
            raise ValueError("causal dispatch state mismatch")
        if self._process is None:
            self._start()
            # Startup notification may race a subsequent terminal message.
            # Preserve transfer evidence without treating it as healthy forever.
            self._drain()
            if self.retired or self._transport_failure is not None:
                raise ProtocolError("causal worker retired during startup")
            if read_document(self.input_root / (request.request_id + ".json")) != document:
                raise ValueError("causal input replaced during child startup")
            validate_input_document(document, request, self.activation, now_ms=wall_ms())
        self._pending[request.request_id] = request
        self._seen[request.request_id] = request.deadline_ms
        try:
            self._resources.put_owned("incoming", (request, previous_state_json))
        except Exception:
            self.cancel(request.request_id)
            raise

    def _unknown(self, reason):
        self.retired = True
        self._transport_failure = str(reason)[:384]
        for key in self._pending:
            self._results[key] = InferenceReceipt(
                CAUSAL_SCOPE, key, key, wall_ms(), "unknown", "{}"
            )
            self.errors[key] = reason
        self._pending.clear()

    def _drain(self):
        if self._outgoing is None:
            return
        channel = getattr(self, "_receipt_channel", None)
        if channel is None:
            self._unknown("causal receipt channel unavailable")
            return
        batch = channel.drain()  # Bounded memory handoff, never native pipe IO.
        messages = batch.messages
        if (
            getattr(self, "_bootstrap_expected", False)
            and not self._ownership_transfer_confirmed
            and not self._bootstrap_marker_rejected
            and messages
        ):
            first, *remaining = messages
            if (
                type(first) is tuple
                and len(first) == 3
                and type(first[0]) is str
                and first[0] == READY_KIND
                and type(first[1]) is int
                and first[1] == self._process.pid
                and type(first[2]) is str
                and first[2] == self._transport_nonce
            ):
                self._ownership_transfer_confirmed = True
                messages = remaining
            else:
                self._bootstrap_marker_rejected = True
                self._unknown("invalid causal child ownership transfer marker")
                messages = ()
        for message in messages:
            if type(message) is not tuple or len(message) != 3:
                self._unknown("malformed causal transport message")
                break
            key, result, error = message
            if type(key) is str and key == READY_KIND:
                self._unknown("duplicate causal child ownership transfer marker")
                break
            if key is None:
                self._unknown(error)
            elif type(key) is not str:
                self._unknown("invalid causal transport request identity")
                break
            elif key in self._pending:
                self._pending.pop(key)
                self._results[key] = result
                if error:
                    self.errors[key] = error
        if batch.error is not None:
            self._unknown(
                f"causal receipt {batch.error.kind}: {batch.error.error_type}: "
                f"{batch.error.message}"
            )
        elif batch.reader_done:
            self._unknown("causal receipt reader ended without terminal evidence")
        if getattr(self, "_startup_notification_error", None) is not None:
            self._unknown("causal transport notification failed")
        if (
            self._process is not None
            and not self._process_retired
            and not self._process.is_alive()
            and self._process.exitcode is not None
        ):
            # Child exit does not order against the reader's local handoff.
            # Preserve full frames still in that original reader/pipe; prevent
            # new dispatch while awaiting its terminal/drained observation.
            if self._transport_failure is None:
                self._transport_failure = "child exited; awaiting original receipt handoff"

    def transport_status(self):
        self._guard()
        self._drain()
        return {
            "closed": self.closed,
            "retired": self.retired,
            "failure": self._transport_failure,
        }

    def receipt(self, request_id):
        self._guard()
        self._drain()
        return self._results.pop(request_id, None)

    def cancel(self, request_id):
        self._guard()
        if request_id not in self._pending:
            return
        self._cancelled = True
        if self._process is not None and self._process.is_alive():
            self._process.terminate()
            self._process.join(timeout=3)
            if self._process.is_alive():
                raise StorageError("causal child cancellation unconfirmed")
        self._unknown("cancelled child; queued peer work also unknown")

    def retirement_status(self):
        """Actual child retirement, never infer death from closed=True alone."""
        if not self._on_owner_thread():
            raise ProtocolError("inspect causal retirement on its owner thread")
        confirmed = self._process_retired or self._process is None and self.closed
        alive = self._process is not None and not confirmed
        if not confirmed and self._process is not None:
            try:
                alive = self._process.is_alive()
                confirmed = not alive and self._process.exitcode is not None
            except ValueError:
                alive, confirmed = True, False  # Closed handle without our retirement ACK.
        channel = getattr(self, "_receipt_channel", None)
        receipt_retired = channel is None and self._process is None
        if channel is not None:
            status = channel.retirement_status()
            receipt_retired = status.join_confirmed and not status.reader_alive
        host = getattr(self, "_host_retirement", None)
        host_confirmed = (
            self.closed and self._process is None and self._incoming is self._outgoing is None
        ) or (
            host is not None
            and host.status == "physical_retired"
            and receipt_retired
            and getattr(self, "_ownership_transfer_confirmed", False)
        )
        return {
            "closed": self.closed,
            "worker_alive": alive,
            "confirmed": confirmed,
            "cleanup_confirmed": confirmed and host_confirmed and not self._cleanup_unconfirmed,
            "host_resources_confirmed": host_confirmed,
            "receipt_reader_retired": receipt_retired,
        }

    def close(self):
        if not self._on_owner_thread():
            raise ProtocolError("close causal worker on its owner thread")
        if os.getpid() == vars(self).get(
            "_transport_owner_pid"
        ) and threading.current_thread() is vars(self).get("_transport_owner_thread"):
            self._transport_close_attempted = True
        resources = getattr(self, "_resources", None)
        if resources is not None:
            return self._close_owned_transport(resources)
        # Lazy/uninitialized ports retain the earlier bounded cleanup. Without
        # an original capsule this path cannot grant a host-resource ACK.
        return self._close_without_capsule()

    def _close_owned_transport(self, resources):
        self.closed = True
        if not resources.owns(self._process, self._incoming, self._outgoing):
            raise StorageError("causal original transport identity is unconfirmed")
        forced = False
        try:
            # A late ready marker only supplies original-resource evidence;
            # closed/retired admission never becomes healthy again.
            self._drain()
            if not self._process_retired:
                if self._process.is_alive():
                    try:
                        resources.put_owned("incoming", None)
                    except queue.Full:
                        pass
                    except Exception:
                        # An uncertain graceful request must not skip the
                        # original child's bounded join/termination attempt.
                        self._cleanup_unconfirmed = True
                    self._process.join(timeout=3)
                    if self._process.is_alive():
                        forced = True
                        self._process.terminate()
                        self._process.join(timeout=3)
                    if self._process.is_alive():
                        raise StorageError("causal child shutdown unconfirmed")
                if forced or self._process.exitcode not in {0, None} and not self._cancelled:
                    self._cleanup_unconfirmed = True
            channel = self._receipt_channel
            if channel is not None and not channel.close(timeout_s=1):
                raise StorageError("causal original receipt reader cleanup unconfirmed")
            self._drain()
            self._host_retirement = resources.retire(timeout_s=1)
            self._process_retired = self._host_retirement.process_handle_closed
            self._channels_closed = self._host_retirement.status == "physical_retired"
            if not self._channels_closed:
                raise StorageError("causal original transport cleanup unconfirmed")
            if not getattr(self, "_ownership_transfer_confirmed", False):
                raise StorageError("causal original ownership transfer cleanup unconfirmed")
            if self._cleanup_unconfirmed:
                raise StorageError("causal model cleanup unconfirmed after child shutdown")
        except (ValueError, OSError, RuntimeError):
            raise StorageError("causal child retirement observation is unconfirmed") from None
        finally:
            self._unknown("causal worker closed")

    def _close_without_capsule(self):
        if self._process_retired and self._channels_closed:
            if self._cleanup_unconfirmed:
                raise StorageError("causal model cleanup remains unconfirmed")
            return
        self.closed = True  # No new submissions, including during a failed join.
        forced = False
        try:
            if self._process is not None and not self._process_retired:
                if self._process.is_alive():
                    if not self._channels_closed:
                        try:
                            self._incoming.put_nowait(None)
                        except queue.Full:
                            pass
                    self._process.join(timeout=3)
                    if self._process.is_alive():
                        forced = True
                        self._process.terminate()
                        self._process.join(timeout=3)
                    if self._process.is_alive():
                        raise StorageError("causal child shutdown unconfirmed")
                failed_cleanup = self._process.exitcode not in {0, None} and not self._cancelled
                self._process.close()
                self._process_retired = True
                if forced or failed_cleanup:
                    self._cleanup_unconfirmed = True
                    raise StorageError("causal model cleanup unconfirmed after child shutdown")
            elif self._process is None:
                self._process_retired = True
        except (ValueError, OSError, RuntimeError):
            raise StorageError("causal child retirement observation is unconfirmed") from None
        finally:
            self._unknown("causal worker closed")
            if self._process_retired and not self._channels_closed:
                for channel in (self._incoming, self._outgoing):
                    if channel is not None:
                        channel.cancel_join_thread()
                        channel.close()
                self._channels_closed = True
