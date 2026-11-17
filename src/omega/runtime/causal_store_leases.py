"""Original inference stores and their independently owned file-lease evidence.

This scope provides no actor restoration, database-source or durable-write
authority. One store's close never closes another binding, including a journal.
"""

# Imports
import inspect
import os
import re
import threading
from collections import deque
from dataclasses import dataclass
from uuid import uuid4

from omega.domain.errors import StorageError, ValidationError
from omega.domain.inference import InferenceCommitAck
from omega.runtime.causal_inference_leases import (
    ROLES,
    FileLeaseReceipt,
    InferenceLeaseRegistry,
)


class InferenceStoreConstructionError(StorageError):
    """Sanitized construction failure retaining an independently owned scope."""

    def __init__(self, lease_scope):
        super().__init__("inference store construction failed; original resources retained")
        self.lease_scope = lease_scope


class InferenceStoreAlreadyOwnedError(InferenceStoreConstructionError, RuntimeError):
    def __init__(self, lease_scope):
        StorageError.__init__(self, "an Omega inference store already owns this state")
        self.lease_scope = lease_scope


@dataclass(frozen=True)
class StoreLeaseSnapshot:
    generation: str
    scope_token: str
    binding_token: str
    role: str
    construction_complete: bool
    construction_failed: bool
    logical_closed: bool
    recording_failed: bool
    identity_intact: bool
    file_lease_confirmed: bool
    file_acquisition_unknown: bool
    no_file_resource: bool
    file: FileLeaseReceipt | None
    last_ack: InferenceCommitAck | None
    primary_failure: str | None
    cleanup_failure: str | None
    scope: str = "original_inference_store_file_lease_v1"
    actor_recovery_authorized: bool = False


@dataclass(frozen=True)
class StoreLeasesSnapshot:
    generation: str
    scope_token: str
    capacity: int
    admission_closed: bool
    bindings: tuple[StoreLeaseSnapshot, ...]
    all_confirmed: bool
    errors: tuple[tuple[str, str], ...]


class _StoreLockView:
    """Inspection compatibility; all cleanup returns to the original binding."""

    def __init__(self, binding, stream):
        self._binding, self._original_stream = binding, stream

    @property
    def stream(self):
        self._binding._owner()
        return self._original_stream

    @property
    def released(self):
        return self._binding.snapshot().file_lease_confirmed

    def close(self):
        return self._binding.close()


class StoreLeaseBinding:
    def __init__(self, scope, store, role):
        self._scope, self._original_store, self._store_type = scope, store, type(store)
        self._role, self._token = role, uuid4().hex
        self._registry = InferenceLeaseRegistry(scope.generation, capacity=1)
        self._fence = store._close_admission
        self._fence_shape = inspect.getattr_static(store, "_close_admission")
        if not callable(self._fence):
            raise ValidationError("owner-only inference admission callback required")
        self._lease = self._view = None
        self._acquisition_started = self._constructed = self._construction_failed = False
        self._logical_closed = self._closing = self._identity_unknown = False
        self._primary_failure = self._cleanup_failure = None
        self._diagnostics = deque(maxlen=16)

    @property
    def role(self):
        return self._role

    @property
    def token(self):
        return self._token

    @property
    def original_store(self):
        self._owner()
        return self._original_store

    def _owner(self):
        self._scope._owner()
        if not any(binding is self for binding in self._scope._bindings):
            raise StorageError("inference binding does not belong to its original scope")

    def _shape(self):
        store = self._original_store
        try:
            valid = (
                type(store) is self._store_type
                and store._lease_scope is self._scope
                and store._lease_binding is self
                and inspect.getattr_static(store, "_close_admission") is self._fence_shape
                and (not self._constructed or store.lock is self._view)
            )
        except Exception:
            valid = False
        if not valid:
            self._identity_unknown = True
        return valid and not self._identity_unknown

    def guard_admission(self, store):
        self._owner()
        if store is not self._original_store or not self._shape():
            raise StorageError("original inference store binding identity changed")
        if self._logical_closed or vars(self._original_store).get("closed") is True:
            raise StorageError("inference store admission is closed")
        if self._view is not None:
            try:
                open_stream = self._view._original_stream.closed is False
            except Exception:
                open_stream = False
            if not open_stream or self._registry.snapshot().admission_closed:
                self._identity_unknown = True
                raise StorageError("original inference store file ownership is unavailable")

    def acquire_lock(self, path):
        self._owner()
        self.guard_admission(self._original_store)
        if self._logical_closed or self._acquisition_started:
            raise StorageError("original inference store lock cannot be acquired again")
        self._acquisition_started = True
        # This capacity1 registry belongs only to this original store. Even when
        # assignment below never happens, its failed-open placeholder is retained.
        self._lease = self._registry.acquire_file(path, self._role)
        self._view = _StoreLockView(self, self._lease.stream)
        return self._view

    def constructed(self):
        self._owner()
        if self._logical_closed:
            raise StorageError("inference store construction was retired")
        self._constructed = True
        if not self._shape():
            self._constructed = False
            raise StorageError("original inference store binding identity changed")

    def construction_failed(self, error):
        self._owner()
        self._construction_failed = True
        self._primary_failure = "inference_store_construction_failed"
        self._diagnostics.append(("constructor", type(error).__name__[:64]))

    def construction_error(self, error):
        self._owner()
        values = self._registry.snapshot().leases
        failed = values[0] if len(values) == 1 else None
        if (
            isinstance(error, OSError)
            and failed is not None
            and failed.acquisition_stage == "initialized"
            and not failed.lock_acquired
        ):
            return InferenceStoreAlreadyOwnedError(self._scope)
        return InferenceStoreConstructionError(self._scope)

    def snapshot(self):
        self._owner()
        registry = self._registry.snapshot()
        file = registry.leases[0] if len(registry.leases) == 1 else None
        # This is a cached scalar read: no path, stream or store callback is probed.
        store = vars(self._original_store)
        identity = not self._identity_unknown
        return StoreLeaseSnapshot(
            self._scope.generation,
            self._scope.token,
            self._token,
            self._role,
            self._constructed,
            self._construction_failed,
            self._logical_closed or store.get("closed") is True,
            store.get("failed") is True,
            identity,
            registry.all_confirmed and identity and not self._closing,
            registry.acquisition_unknown,
            not registry.leases or all(not value.open_attempted for value in registry.leases),
            file,
            store.get("_last_ack"),
            self._primary_failure,
            self._cleanup_failure,
        )

    def close(self):
        self._owner()
        if self._closing:
            self._identity_unknown = True
            self._cleanup_failure = "inference_store_cleanup_unconfirmed"
            raise StorageError("inference store file lease cleanup is unconfirmed")
        self._closing = True
        self._logical_closed = True
        errors = False
        try:
            if not self._shape():
                errors = True
            try:
                self._fence()  # Captured simple flag fence; never store.close().
            except BaseException as error:
                self._identity_unknown = True
                self._diagnostics.append(("admission_fence", type(error).__name__[:64]))
                errors = True
            try:
                result = self._registry.close_all()
            except BaseException as error:
                self._diagnostics.append(("file_cleanup", type(error).__name__[:64]))
                errors = True
            else:
                if not result.all_confirmed:
                    errors = True
            if errors or self._identity_unknown:
                self._cleanup_failure = "inference_store_cleanup_unconfirmed"
                raise StorageError("inference store file lease cleanup is unconfirmed")
        finally:
            self._closing = False
        return self.snapshot()


class InferenceStoreLeases:
    """Finite original bindings; explicit orchestration belongs to the actor."""

    def __init__(self, generation, *, capacity=512):
        if type(generation) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", generation
        ):
            raise ValidationError("bounded exact inference lease generation required")
        if type(capacity) is not int or not 1 <= capacity <= 4096:
            raise ValidationError("native inference store lease capacity must be within [1,4096]")
        self._generation, self._token = generation, uuid4().hex
        self._pid, self._thread = os.getpid(), threading.current_thread()
        self._capacity, self._bindings, self._admission_closed = capacity, (), False
        self._last_errors = ()

    @property
    def generation(self):
        return self._generation

    @property
    def token(self):
        return self._token

    @property
    def bindings(self):
        self._owner()
        return self._bindings

    def _owner(self):
        if os.getpid() != self._pid or threading.current_thread() is not self._thread:
            raise StorageError(
                "inference store leases require their original PID and Thread object"
            )

    def bind_store(self, store, role):
        self._owner()
        if type(role) is not str or role not in ROLES:
            raise ValidationError("exact inference store role required")
        if self._admission_closed or len(self._bindings) >= self._capacity:
            raise StorageError("inference store lease admission unavailable")
        if any(binding._original_store is store for binding in self._bindings):
            raise StorageError("original inference store already has a binding")
        binding = StoreLeaseBinding(self, store, role)
        self._bindings = (*self._bindings, binding)
        return binding

    def binding_for(self, store):
        self._owner()
        for binding in self._bindings:
            if binding._original_store is store:
                return binding
        raise StorageError("original inference store is not registered in this scope")

    def snapshot(self):
        self._owner()
        bindings = tuple(binding.snapshot() for binding in self._bindings)
        return StoreLeasesSnapshot(
            self._generation,
            self._token,
            self._capacity,
            self._admission_closed,
            bindings,
            self._admission_closed and all(value.file_lease_confirmed for value in bindings),
            self._last_errors,
        )

    def close_all(self):
        self._owner()
        self._admission_closed = True
        errors = []
        for binding in self._bindings:
            try:
                binding.close()
            except BaseException:
                errors.append((binding.token, "inference_store_cleanup_unconfirmed"))
        self._last_errors = tuple(errors)
        return self.snapshot()

    def seal_admission(self):
        """Seal future registrations only; existing stores retain their own fences."""
        self._owner()
        self._admission_closed = True
        return self.snapshot()
