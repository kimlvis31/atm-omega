"""Bounded original actor references and internal-only retirement evidence.

No cleanup, model execution, state recovery or public telemetry is authorized.
Nested original receipts may contain bounded private diagnostic strings.
"""

# Imports
import inspect
import os
import re
import threading
import types
from dataclasses import dataclass, field, fields, replace
from typing import Literal, Union, get_args, get_origin
from uuid import uuid4

from omega.domain.errors import StorageError, ValidationError
from omega.domain.inference import InferenceCommitAck
from omega.infrastructure.causal_market_read import ReadReceipt, ReadScopeReceipt
from omega.runtime.causal_inference_leases import FileLeaseReceipt, LeaseEvent
from omega.runtime.causal_inference_ports import ObservedInferenceWorker
from omega.runtime.causal_market_source import (
    CommittedInferenceMarketSource,
    SourceReadRetirementSnapshot,
)
from omega.runtime.causal_nn import CausalNNFeatureOwner
from omega.runtime.causal_nn_worker import CausalInferenceProcessWorker
from omega.runtime.causal_process_resources import HostRetirement, QueueRetirement
from omega.runtime.causal_receipt_channel import ReceiptFailure, ReceiptRetirementStatus
from omega.runtime.causal_store_leases import (
    InferenceStoreLeases,
    StoreLeaseBinding,
    StoreLeaseSnapshot,
    StoreLeasesSnapshot,
)
from omega.runtime.causal_worker_retirement import (
    WorkerTransportRetirement,
    validate_worker_transport_retirement,
)


# Types
@dataclass(frozen=True)
class ActorResourceKey:
    actor_token: str
    run_token: str
    binding_token: str
    role: Literal["source", "raw_worker", "owner"]


@dataclass(frozen=True)
class ActorResourceObservation:
    key: ActorResourceKey
    acquisition: Literal["reserved", "entered", "returned", "unknown"]
    factory_default: bool
    identity_intact: bool
    close_attempted: bool
    close_returned: bool
    physical: Literal["unobserved", "confirmed", "unknown"]
    cleanup_code: str | None
    source: SourceReadRetirementSnapshot | None = None
    worker: WorkerTransportRetirement | None = None
    owner_store_binding_token: str | None = None
    actor_recovery_authorized: bool = field(default=False, init=False)


@dataclass(frozen=True)
class ActorRetirementSnapshot:
    actor_token: str
    core_epoch: str
    owner_pid: int
    owner_thread_token: str
    sequence: int
    sealed: bool
    admission_closed: bool
    capacity: int
    resources: tuple[ActorResourceObservation, ...]
    stores: StoreLeasesSnapshot | None
    factory_coverage_complete: bool
    identity_intact: bool
    capture_failed: bool
    last_journal_ack: InferenceCommitAck | None
    journal_write_pending: bool
    recording_unknown: bool
    actor_error_code: str | None
    physical_confirmed: bool
    errors: tuple[str, ...]
    actor_recovery_authorized: bool = field(default=False, init=False)


@dataclass(frozen=True)
class ActorJoinObservation:
    actor_token: str
    owner_thread_token: str
    join_attempted: bool
    join_returned: bool
    original_thread_dead: bool
    identity_intact: bool
    error_code: str | None
    actor_recovery_authorized: bool = field(default=False, init=False)


# Constants
_DEFAULTS = {
    "source": CommittedInferenceMarketSource,
    "raw_worker": CausalInferenceProcessWorker,
    "owner": CausalNNFeatureOwner,
}
_RECORDS = frozenset(
    {
        ActorResourceKey,
        ActorResourceObservation,
        ActorRetirementSnapshot,
        ActorJoinObservation,
        InferenceCommitAck,
        ReadReceipt,
        ReadScopeReceipt,
        SourceReadRetirementSnapshot,
        FileLeaseReceipt,
        LeaseEvent,
        StoreLeaseSnapshot,
        StoreLeasesSnapshot,
        HostRetirement,
        QueueRetirement,
        ReceiptFailure,
        ReceiptRetirementStatus,
        WorkerTransportRetirement,
    }
)


def _token(value):
    if type(value) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value):
        raise ValidationError("bounded exact actor evidence identity required")
    return value


def _code(value):
    if value is not None and (
        type(value) is not str or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", value)
    ):
        raise ValidationError("bounded exact actor evidence code required")
    return value


def validate_retirement_value(value):
    """Reject forged/mutable leaves without formatting or arbitrary conversion."""
    if type(value) not in _RECORDS:
        raise ValidationError("exact retirement evidence record root required")
    budget = [1048576]

    def visit(item, expected, depth=0):
        budget[0] -= 1
        if depth > 16 or budget[0] < 0:
            raise ValidationError("retirement evidence bounds exceeded")
        origin, arguments = get_origin(expected), get_args(expected)
        if origin in {types.UnionType, Union}:
            for option in arguments:
                try:
                    visit(item, option, depth + 1)
                    return
                except ValidationError:
                    pass
            raise ValidationError("retirement evidence union shape invalid")
        if origin is Literal:
            if any(type(item) is type(option) and item == option for option in arguments):
                return
            raise ValidationError("retirement evidence literal invalid")
        if expected is type(None):
            valid = item is None
        elif expected is bool:
            valid = type(item) is bool
        elif expected is int:
            valid = type(item) is int and -(1 << 63) <= item < 1 << 63
        elif expected is str:
            valid = type(item) is str and len(item) <= 4096
        elif origin is tuple:
            if type(item) is not tuple or len(item) > 4096:
                raise ValidationError("retirement evidence tuple invalid")
            if len(arguments) == 2 and arguments[1] is Ellipsis:
                for member in item:
                    visit(member, arguments[0], depth + 1)
            else:
                if len(item) != len(arguments):
                    raise ValidationError("retirement evidence tuple shape invalid")
                for member, annotation in zip(item, arguments, strict=True):
                    visit(member, annotation, depth + 1)
            return
        elif expected in _RECORDS and type(item) is expected:
            for definition in fields(expected):
                member = object.__getattribute__(item, definition.name)
                visit(member, definition.type, depth + 1)
                if (
                    definition.name
                    in {
                        "actor_recovery_authorized",
                        "all_kernel_resources_confirmed",
                        "durable_state_confirmed",
                        "durable_write_confirmed",
                    }
                    and member is not False
                ):
                    raise ValidationError("retirement evidence cannot grant new authority")
            if expected is ReceiptFailure and (
                len(item.message) > 384 or len(item.error_type) > 128
            ):
                raise ValidationError("receipt diagnostic exceeds original bounds")
            if expected is WorkerTransportRetirement:
                try:
                    validate_worker_transport_retirement(item)
                except ValueError:
                    raise ValidationError("worker evidence leaf is malformed") from None
            if expected in {ReadReceipt, FileLeaseReceipt} and len(item.history) > 16:
                raise ValidationError("original receipt history exceeds its bound")
            if expected is ReadScopeReceipt and len(item.history) > 64:
                raise ValidationError("original read history exceeds its bound")
            if expected is ActorResourceKey:
                for name in ("actor_token", "run_token", "binding_token"):
                    _token(object.__getattribute__(item, name))
            return
        else:
            valid = False
        if not valid:
            raise ValidationError("exact immutable retirement evidence required")

    visit(value, type(value))
    return value


class _Binding:
    def __init__(self, key):
        self.key, self.state, self.factory, self.default = key, "reserved", None, False
        self.original = self.original_type = self.getter = self.getter_shape = None
        self.close_shape = self.scope = self.scope_token = None
        self.raw_key = self.wrapper = self.wrapper_close_shape = self.store_binding = None
        self.worker_token = None
        self.identity_unknown = False
        self.observation = ActorResourceObservation(
            key, "reserved", False, True, False, False, "unobserved", None
        )


class ActorOriginalResources:
    """Owner-only registry. It never closes or starts any retained resource."""

    def __init__(self, actor_token, core_epoch, owner_thread, *, owner_thread_token, capacity=1536):
        if owner_thread is not threading.current_thread():
            raise StorageError("actor registry requires its original owner Thread")
        if type(capacity) is not int or not 3 <= capacity <= 1536:
            raise ValidationError("actor evidence capacity must be within [3,1536]")
        self.actor_token, self.core_epoch = _token(actor_token), _token(core_epoch)
        self.owner_thread_token = _token(owner_thread_token)
        self._pid, self._thread, self.capacity = os.getpid(), owner_thread, capacity
        self._bindings, self._scope = (), None
        self._scope_token = self._scope_getter = self._scope_shape = None
        self._default_store_factory = False
        self.admission_closed = self.capture_failed = self.recording_unknown = False
        self._errors, self._sequence, self._sealed, self._collecting = (), 0, False, False
        self._last_ack = self._cached = None

    def _owner(self):
        if os.getpid() != self._pid or threading.current_thread() is not self._thread:
            raise StorageError("actor evidence requires original PID and Thread object")

    def mark_capture_failed(self, code="actor_capture_unknown"):
        self._owner()
        self.capture_failed = True
        if type(code) is str and code not in self._errors:
            self._errors = (*self._errors[-15:], _code(code))

    def bind_store_scope(self, scope, *, default_store_factory):
        self._owner()
        if type(default_store_factory) is not bool:
            raise ValidationError("exact store factory coverage flag required")
        if (
            self._scope is not None
            or self.admission_closed
            or type(scope) is not InferenceStoreLeases
        ):
            raise StorageError("original actor store scope cannot be replaced")
        self._scope = scope
        self._default_store_factory = default_store_factory
        self._scope_token = scope.token
        self._scope_shape = inspect.getattr_static(scope, "snapshot")
        self._scope_getter = scope.snapshot

    def reserve_run(self):
        self._owner()
        if self.admission_closed or self._sealed or len(self._bindings) + 3 > self.capacity:
            raise StorageError("actor lifetime evidence admission unavailable")
        run = uuid4().hex
        values = tuple(
            _Binding(ActorResourceKey(self.actor_token, run, uuid4().hex, role))
            for role in _DEFAULTS
        )
        self._bindings = (*self._bindings, *values)
        return tuple(value.key for value in values)

    def _binding(self, key):
        self._owner()
        if type(key) is not ActorResourceKey:
            raise StorageError("exact original actor resource key required")
        for value in self._bindings:
            if value.key is key:
                return value
        raise StorageError("actor evidence binding is not original")

    def enter(self, key, factory, *, default=False):
        value = self._binding(key)
        if self.admission_closed or self._sealed or value.state != "reserved":
            raise StorageError("actor resource acquisition admission unavailable")
        value.factory = factory
        value.default = default is True and factory is _DEFAULTS[key.role]
        value.state = "entered"

    def acquisition_failed(self, key):
        value = self._binding(key)
        if value.state != "reserved":
            value.state, value.identity_unknown = "unknown", True
            value.observation = ActorResourceObservation(
                key, "unknown", value.default, False, False, False, "unknown", "acquisition_unknown"
            )

    def returned(self, key, resource, *, related_worker=None, wrapper=None, store_binding=None):
        value = self._binding(key)
        if value.state != "entered" or value.original is not None:
            raise StorageError("original actor resource return cannot be replaced")
        value.original, value.original_type = resource, type(resource)
        value.state = "returned"
        if type(resource) is not _DEFAULTS[key.role]:
            value.default = False
        try:
            value.close_shape = inspect.getattr_static(resource, "close")
            method = (
                "transport_retirement_snapshot" if key.role == "raw_worker" else "retirement_status"
            )
            if key.role != "owner" and value.default:
                value.getter_shape = inspect.getattr_static(resource, method)
                value.getter = getattr(resource, method)
            if (
                key.role == "source"
                and value.default
                and type(resource) is CommittedInferenceMarketSource
            ):
                value.scope = resource._original_read_scope
                value.scope_token = value.scope.token
            elif (
                key.role == "raw_worker"
                and value.default
                and type(resource) is CausalInferenceProcessWorker
            ):
                value.worker_token = _token(resource.transport_worker_token)
            elif key.role == "owner":
                raw = self._binding(
                    related_worker
                    if type(related_worker) is ActorResourceKey
                    else self.key_for_original(related_worker)
                )
                value.raw_key, value.wrapper, value.store_binding = raw.key, wrapper, store_binding
                if (
                    wrapper is not raw.wrapper
                    or type(wrapper) is not ObservedInferenceWorker
                    or wrapper.worker is not raw.original
                ):
                    raise StorageError("owner wrapper does not bind original raw worker")
                value.wrapper_close_shape = inspect.getattr_static(wrapper, "close")
        except BaseException:
            value.identity_unknown = True
            self.mark_capture_failed()
            raise

    def key_for_original(self, resource):
        self._owner()
        if resource is None:
            return None
        return next(
            (
                value.key
                for value in self._bindings
                if value.original is resource
                or value.key.role == "raw_worker"
                and value.wrapper is resource
            ),
            None,
        )

    def attach_wrapper(self, worker_key, wrapper):
        value = self._binding(worker_key)
        if (
            self._sealed
            or value.key.role != "raw_worker"
            or value.state != "returned"
            or value.wrapper is not None
        ):
            raise StorageError("original worker wrapper cannot be replaced")
        value.wrapper = wrapper  # Retain before inspecting a possibly invalid wrapper.
        try:
            if type(wrapper) is not ObservedInferenceWorker or wrapper.worker is not value.original:
                raise StorageError("wrapper must bind original raw worker")
            value.wrapper_close_shape = inspect.getattr_static(wrapper, "close")
        except BaseException:
            value.identity_unknown = True
            self.mark_capture_failed()
            raise

    def original(self, key):
        return self._binding(key).original

    def _shape(self, value):
        if value.identity_unknown or value.original is None:
            return False
        try:
            resource, role = value.original, value.key.role
            intact = (
                type(resource) is value.original_type
                and inspect.getattr_static(resource, "close") is value.close_shape
            )
            if role != "owner" and value.default:
                method = (
                    "transport_retirement_snapshot" if role == "raw_worker" else "retirement_status"
                )
                intact = intact and inspect.getattr_static(resource, method) is value.getter_shape
            if role == "source" and value.default:
                intact = (
                    intact
                    and resource._read_scope is resource._original_read_scope is value.scope
                    and resource.store_factory is resource._original_store_factory
                    and resource._default_store_factory is True
                )
            if role == "raw_worker" and value.wrapper is not None:
                intact = (
                    intact
                    and type(value.wrapper) is ObservedInferenceWorker
                    and value.wrapper.worker is resource
                    and inspect.getattr_static(value.wrapper, "close") is value.wrapper_close_shape
                )
            if role == "owner":
                raw = self._binding(value.raw_key)
                intact = (
                    intact
                    and resource.worker is value.wrapper
                    and value.wrapper.worker is raw.original
                )
                intact = (
                    intact
                    and inspect.getattr_static(value.wrapper, "close") is value.wrapper_close_shape
                )
                if value.default:
                    intact = (
                        intact
                        and type(value.store_binding) is StoreLeaseBinding
                        and value.store_binding.original_store is resource.store
                        and value.store_binding in self._scope.bindings
                    )
            if not intact:
                value.identity_unknown = True
            return intact
        except BaseException:
            value.identity_unknown = True
            return False

    def observe(self, key, *, close_attempted, close_returned):
        value = self._binding(key)
        if type(close_attempted) is not bool or type(close_returned) is not bool:
            raise ValidationError("exact cleanup observation flags required")
        if self._sealed:
            raise StorageError("sealed actor evidence cannot be changed")
        if self._collecting:
            self.mark_capture_failed("actor_capture_reentry")
            raise StorageError("actor evidence collection reentered")
        self._collecting = True
        source = worker = None
        confirmed = False
        intact = self._shape(value)
        try:
            if value.state == "reserved":
                intact, confirmed = True, True  # No factory was entered.
            elif value.state == "returned" and intact and value.default:
                if key.role == "source":
                    source = value.getter()
                    validate_retirement_value(source)
                    confirmed = (
                        type(source) is SourceReadRetirementSnapshot
                        and source.scope_token == value.scope_token
                        and source.read_scope.scope_token == value.scope_token
                        and source.complete_default_factory
                        and not source.legacy_read_attempted
                        and source.identity_intact
                        and source.physical_confirmed
                    )
                elif key.role == "raw_worker":
                    worker = value.getter()
                    validate_retirement_value(worker)
                    confirmed = (
                        type(worker) is WorkerTransportRetirement
                        and worker.worker_token == value.worker_token
                        and worker.identity_intact
                        and worker.physical_confirmed
                    )
                else:
                    raw = self._binding(value.raw_key)
                    record = value.store_binding.snapshot()
                    validate_retirement_value(record)
                    confirmed = (
                        raw.observation.physical == "confirmed"
                        and type(record) is StoreLeaseSnapshot
                        and record.binding_token == value.store_binding.token
                        and record.scope_token == self._scope_token
                        and record.file_lease_confirmed
                    )
            value.observation = ActorResourceObservation(
                key,
                value.state,
                value.default,
                intact,
                close_attempted,
                close_returned,
                "confirmed" if confirmed else "unknown",
                None if confirmed else "actor_resource_unconfirmed",
                source,
                worker,
                value.store_binding.token
                if type(value.store_binding) is StoreLeaseBinding
                else None,
            )
            validate_retirement_value(value.observation)
            return value.observation
        except BaseException:
            value.identity_unknown = True
            self.mark_capture_failed()
            raise
        finally:
            self._collecting = False

    def observe_resource(self, resource, *, close_attempted, close_returned):
        self._owner()
        key = self.key_for_original(resource)
        if key is None:
            return None
        value = self._binding(key)
        if key.role == "owner" and value.raw_key is not None:
            self.observe(value.raw_key, close_attempted=close_attempted, close_returned=False)
        return self.observe(
            key,
            close_attempted=close_attempted,
            close_returned=close_returned if resource is value.original else False,
        )

    def seal_admission(self):
        self._owner()
        self.admission_closed = True

    def _current_observation(self, value):
        if value.state == "reserved":
            return ActorResourceObservation(
                value.key, "reserved", False, True, False, False, "confirmed", None
            )
        if value.state == "returned" and not self._shape(value):
            return replace(
                value.observation,
                acquisition=value.state,
                identity_intact=False,
                physical="unknown",
                cleanup_code="original_identity_unknown",
            )
        if value.observation.acquisition != value.state:
            current = ActorResourceObservation(
                value.key,
                value.state,
                value.default,
                not value.identity_unknown,
                False,
                False,
                "unknown",
                "resource_not_observed",
            )
        else:
            current = value.observation
        if value.state == "returned" and value.default and value.key.role == "raw_worker":
            # The original adapter validates all original process/queue/capsule/
            # reader bindings here. This cached-scalar getter performs no close,
            # start or native process probe. Never reuse an old positive after
            # those private references may have changed between cleanup phases.
            worker = value.getter()
            validate_retirement_value(worker)
            intact = (
                type(worker) is WorkerTransportRetirement
                and worker.worker_token == value.worker_token
                and worker.identity_intact
            )
            if not intact:
                value.identity_unknown = True
            value.observation = replace(
                current,
                worker=worker,
                identity_intact=intact,
                physical="confirmed" if intact and worker.physical_confirmed else "unknown",
                cleanup_code=None
                if intact and worker.physical_confirmed
                else "actor_resource_unconfirmed",
            )
            return value.observation
        if value.key.role == "owner" and value.raw_key is not None:
            raw = self._binding(value.raw_key)
            if raw.observation.physical != "confirmed":
                return replace(
                    current, physical="unknown", cleanup_code="original_worker_unconfirmed"
                )
        return current

    def snapshot(
        self,
        *,
        sealed=False,
        stores=None,
        last_journal_ack=None,
        journal_write_pending=False,
        recording_unknown=False,
        actor_error_code=None,
    ):
        self._owner()
        if self._sealed:
            return self._cached
        if self._collecting:
            self.mark_capture_failed("actor_capture_reentry")
            raise StorageError("actor evidence collection reentered")
        self._collecting = True
        try:
            if any(
                type(flag) is not bool
                for flag in (sealed, journal_write_pending, recording_unknown)
            ):
                raise ValidationError("exact actor evidence flags required")
            _code(actor_error_code)
            self.recording_unknown = self.recording_unknown or recording_unknown
            if stores is not None:
                validate_retirement_value(stores)
            scope_intact = (
                self._scope is not None
                and inspect.getattr_static(self._scope, "snapshot") is self._scope_shape
                and self._scope.token == self._scope_token
            )
            scope_complete = (
                scope_intact
                and type(stores) is StoreLeasesSnapshot
                and stores.scope_token == self._scope_token
                and stores == self._scope_getter()
            )
            ack_complete = last_journal_ack is None
            if type(last_journal_ack) is InferenceCommitAck:
                validate_retirement_value(last_journal_ack)
                ack_complete = (
                    scope_complete
                    and self._default_store_factory
                    and any(
                        record.role == "journal" and record.last_ack is last_journal_ack
                        for record in stores.bindings
                    )
                )
                if ack_complete:
                    self._last_ack = last_journal_ack
            observations = tuple(self._current_observation(value) for value in self._bindings)
            complete = (
                scope_complete
                and self._default_store_factory
                and ack_complete
                and all(value.state == "reserved" or value.default for value in self._bindings)
            )
            intact = scope_intact and all(not value.identity_unknown for value in self._bindings)
            physical = (
                sealed
                and self.admission_closed
                and complete
                and intact
                and not self.capture_failed
                and stores.all_confirmed
                and all(value.physical == "confirmed" for value in observations)
            )
            self._sequence += 1
            record = ActorRetirementSnapshot(
                self.actor_token,
                self.core_epoch,
                self._pid,
                self.owner_thread_token,
                self._sequence,
                sealed,
                self.admission_closed,
                self.capacity,
                observations,
                stores,
                complete,
                intact,
                self.capture_failed,
                self._last_ack,
                journal_write_pending,
                self.recording_unknown,
                actor_error_code,
                physical,
                self._errors,
            )
            validate_retirement_value(record)
            self._cached = record
            self._sealed = sealed
            return record
        except BaseException:
            self.mark_capture_failed()
            raise
        finally:
            self._collecting = False


class ActorJoinObserver:
    """Host joins only the original Thread; never probes its owned resources."""

    def __init__(self, actor_token, owner_thread_token, thread):
        if type(thread) is not threading.Thread:
            raise ValidationError("exact original actor Thread required")
        self.actor_token, self.owner_thread_token = _token(actor_token), _token(owner_thread_token)
        self._pid, self._thread, self._lock = os.getpid(), thread, threading.Lock()
        self._join, self._alive = thread.join, thread.is_alive
        self._join_shape, self._alive_shape = (
            inspect.getattr_static(thread, "join"),
            inspect.getattr_static(thread, "is_alive"),
        )
        self._identity_unknown = False

    def observe(self, timeout_s, *, current_thread=None):
        if os.getpid() != self._pid:
            raise StorageError("actor join observation requires original process")
        if type(timeout_s) not in {int, float} or not 0 <= timeout_s <= 60:
            raise ValidationError("bounded actor join timeout required")
        with self._lock:
            if threading.current_thread() is self._thread:
                return ActorJoinObservation(
                    self.actor_token,
                    self.owner_thread_token,
                    False,
                    False,
                    False,
                    not self._identity_unknown,
                    "actor_cannot_join_itself",
                )
            intact = (
                (current_thread is None or current_thread is self._thread)
                and inspect.getattr_static(self._thread, "join") is self._join_shape
                and inspect.getattr_static(self._thread, "is_alive") is self._alive_shape
            )
            self._identity_unknown = self._identity_unknown or not intact
            attempted = returned = dead = False
            code = None
            try:
                attempted = True
                self._join(timeout=timeout_s)
                returned, dead = True, not self._alive()
            except BaseException:
                code = "original_actor_join_unknown"
            return ActorJoinObservation(
                self.actor_token,
                self.owner_thread_token,
                attempted,
                returned,
                dead,
                not self._identity_unknown,
                code,
            )
