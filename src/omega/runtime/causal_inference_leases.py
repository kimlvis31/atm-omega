"""Standalone original-file lease evidence; no store/actor/recovery integration.

Confirmed evidence covers the original file stream and its OS lock only. It is
independent of durable writes, model cleanup and any actor restoration authority.
"""

# Imports
import inspect
import os
import re
import stat
import threading
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

# Constants
ROLES = frozenset({"journal", "nn_state", "delivery"})
HISTORY_LIMIT = 16
MAX_ATTEMPTS = (1 << 63) - 1
SCOPE = "original_inference_file_stream_lock_v1"


class LeaseCleanupError(RuntimeError):
    """Fixed public failure; bounded original diagnostics remain internal."""


@dataclass(frozen=True)
class LeaseEvent:
    attempt: int
    code: str


@dataclass(frozen=True)
class FileLeaseReceipt:
    registry_token: str
    generation: str
    resource_token: str
    role: str
    acquisition_stage: str
    open_attempted: bool
    open_returned: bool
    acquisition_unknown: bool
    lock_acquired: bool
    validation_complete: bool
    acquisition_failed: bool
    logical_state: str
    native_close_attempted: bool
    native_close_returned: bool
    post_state_closed: bool | None
    physical_status: str
    destructive_unknown: bool
    ever_uncertain: bool
    attempt: int
    history: tuple[LeaseEvent, ...]
    scope: str = SCOPE
    durable_write_confirmed: bool = False
    actor_recovery_authorized: bool = False
    all_kernel_resources_confirmed: bool = False


@dataclass(frozen=True)
class RegistrySnapshot:
    registry_token: str
    generation: str
    capacity: int
    admission_closed: bool
    admission_held: bool
    acquisition_unknown: bool
    reserved_slots: int
    leases: tuple[FileLeaseReceipt, ...]
    all_confirmed: bool
    errors: tuple[tuple[str, str], ...]
    scope: str = SCOPE


@dataclass(frozen=True)
class _Diagnostic:
    stage: str
    error_class: str
    text: str


def _diagnostic(stage, error):
    try:
        text = str(error)[:160]
    except BaseException:
        text = "<unavailable>"
    return _Diagnostic(stage, type(error).__name__[:64], text)


def _validate_path(path):
    value = Path(path)
    if ".." in value.parts or len(str(value)) > 4096:
        raise ValueError("bounded file lease path required")
    value = Path(os.path.abspath(value))
    if len(value.parents) > 127:
        raise ValueError("file lease path exceeds ancestor bound")
    for index, current in enumerate((value, *value.parents)):
        try:
            details = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
            raise ValueError("file lease path cannot contain links/reparse points")
        if index == 0 and not stat.S_ISREG(details.st_mode):
            raise ValueError("file lease must be a regular file")
    return value


def _open_file(path):
    return path.open("a+b")


def _initialize_file(stream):
    if stream.tell() == 0:
        stream.write(b"0")
        stream.flush()
    stream.seek(0)


def _acquire_lock(stream):
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _validate_open_file(stream, path):
    details = os.fstat(stream.fileno())
    if not stat.S_ISREG(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
        raise ValueError("opened file lease must be regular and non-reparse")
    _validate_path(path)


class FileLease:
    """Registry-created original stream; close has one destructive invocation."""

    def __init__(self, registry, role):
        # Construct all bookkeeping before opening a stream. After open returns,
        # registration assigns original references before any capture/IO checks.
        self._registry = registry
        self._stream = self._original_stream = None
        self._role, self._token = role, uuid4().hex
        self._generation, self._registry_token = registry.generation, registry.token
        self._original_close = self._close_shape = None
        self._original_type = None
        self._stage = "registered"
        self._open_attempted = self._open_returned = self._acquisition_unknown = False
        self._acquisition_pending = True
        self._lock_acquired = self._validation_complete = self._acquisition_failed = False
        self._logical_closed = self._attempted = self._returned = False
        self._closing = False
        self._post_closed = None
        self._physical = "pending"
        self._destructive_unknown = self._ever_uncertain = False
        self._attempt = 0
        self._history = deque(maxlen=HISTORY_LIMIT)
        self._diagnostics = deque(maxlen=HISTORY_LIMIT)

    @property
    def stream(self):
        self._owner()
        if self._logical_closed or not self._validation_complete:
            raise LeaseCleanupError("original file lease is logically closed")
        return self._original_stream

    @property
    def role(self):
        return self._role

    @property
    def token(self):
        return self._token

    def _capture_close(self):
        self._close_shape = inspect.getattr_static(self._original_stream, "close")
        self._original_close = self._original_stream.close
        if not callable(self._original_close):
            raise TypeError("original file close must be callable")
        self._stage = "close_captured"

    def _owner(self):
        self._registry._owner()
        if (
            self._generation != self._registry.generation
            or self._registry_token != self._registry.token
            or not any(lease is self for lease in self._registry._leases)
        ):
            raise RuntimeError("original file lease registry identity changed")

    def _event(self, code, error=None):
        self._ever_uncertain = True
        self._physical = "unknown"
        self._history.append(LeaseEvent(self._attempt, code))
        if error is not None:
            self._diagnostics.append(_diagnostic(code, error))

    def _identity(self):
        if (
            self._stream is not self._original_stream
            or type(self._stream) is not self._original_type
        ):
            self._destructive_unknown = True
            self._event("original_stream_identity_changed")
            return False
        try:
            shape = inspect.getattr_static(self._original_stream, "close")
        except BaseException as error:
            self._event("close_identity_observation_failed", error)
            return False
        if self._original_close is None or shape is not self._close_shape:
            self._destructive_unknown = True
            self._event("original_close_identity_changed")
            return False
        return True

    def _post_observation(self):
        if self._destructive_unknown or self._acquisition_unknown:
            self._event("sticky_cleanup_unknown")
            raise LeaseCleanupError("original file destructive cleanup unconfirmed")
        try:
            closed = self._original_stream.closed
        except BaseException as error:
            self._post_closed = None
            self._event("postclose_observation_failed", error)
            raise LeaseCleanupError("original file close observation unconfirmed") from None
        self._post_closed = closed if type(closed) is bool else None
        if self._destructive_unknown or self._acquisition_unknown:
            self._event("sticky_cleanup_unknown")
            raise LeaseCleanupError("original file destructive cleanup unconfirmed")
        if closed is not True:
            self._event("close_return_without_closed_observation")
            raise LeaseCleanupError("original file close observation unconfirmed")
        self._physical = "confirmed"
        self._history.append(LeaseEvent(self._attempt, "original_close_confirmed"))

    def snapshot(self):
        """Cached scalar-only evidence; never probes or closes the stream."""
        self._owner()
        return FileLeaseReceipt(
            self._registry_token,
            self._generation,
            self._token,
            self._role,
            self._stage,
            self._open_attempted,
            self._open_returned,
            self._acquisition_unknown,
            self._lock_acquired,
            self._validation_complete,
            self._acquisition_failed,
            "closed" if self._logical_closed else "open",
            self._attempted,
            self._returned,
            self._post_closed,
            self._physical,
            self._destructive_unknown,
            self._ever_uncertain,
            self._attempt,
            tuple(self._history),
        )

    def close(self):
        self._owner()  # Hard guard before attempts, history or original-resource mutation.
        if self._attempt == MAX_ATTEMPTS:
            raise LeaseCleanupError("file lease attempt capacity exhausted")
        self._attempt += 1
        self._logical_closed = True
        if self._closing:
            self._destructive_unknown = True
            self._event("original_close_reentered")
            raise LeaseCleanupError("original file destructive cleanup unconfirmed")
        self._closing = True
        try:
            return self._close_once()
        finally:
            self._closing = False

    def _close_once(self):
        if self._attempted and not self._returned:
            self._destructive_unknown = True
            self._event("original_close_attempt_without_return")
            raise LeaseCleanupError("original file destructive cleanup unconfirmed")
        if self._original_stream is None:
            if not self._open_attempted:
                self._physical = "not_opened"
                return self.snapshot()  # Positive absence; no invented close callback ACK.
            self._acquisition_unknown = True
            self._registry._hold_acquisition()
            self._event("original_open_outcome_unknown")
            raise LeaseCleanupError("original file acquisition remains unconfirmed")
        if not self._identity() or self._destructive_unknown:
            raise LeaseCleanupError("original file destructive cleanup unconfirmed") from None
        if self._returned:
            self._post_observation()  # Retry observation only, never the native close.
            return self.snapshot()
        try:
            closed = self._original_stream.closed
        except BaseException as error:
            self._event("preclose_observation_failed", error)
            raise LeaseCleanupError("original file preclose observation unconfirmed") from None
        if closed is not False:
            self._destructive_unknown = True
            self._event("closed_without_trusted_callback")
            raise LeaseCleanupError("original file was closed without trusted callback")
        self._attempted = True
        try:
            self._original_close()
            self._returned = True
        except BaseException as error:
            self._destructive_unknown = True
            self._event("original_native_close_failed", error)
            raise
        self._post_observation()
        return self.snapshot()


class InferenceLeaseRegistry:
    """Finite owner-only strong references, created before any lease acquisition."""

    def __init__(self, generation, *, capacity=16):
        if type(generation) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", generation
        ):
            raise ValueError("bounded exact generation identity required")
        if type(capacity) is not int or not 1 <= capacity <= 64:
            raise ValueError("native integer lease capacity must be within [1,64]")
        self._generation, self._token = generation, uuid4().hex
        self._pid, self._thread = os.getpid(), threading.current_thread()
        self._capacity = capacity
        self._leases = []
        self._admission_closed = False
        self._admission_held = self._acquisition_unknown = False
        self._last_errors = ()

    @property
    def generation(self):
        return self._generation

    @property
    def token(self):
        return self._token

    @property
    def capacity(self):
        return self._capacity

    def _owner(self):
        if os.getpid() != self._pid or threading.current_thread() is not self._thread:
            raise RuntimeError("file lease operation requires its original PID and Thread object")

    def _hold_acquisition(self):
        self._admission_closed = self._admission_held = self._acquisition_unknown = True

    def acquire_file(self, path, role):
        self._owner()
        if type(role) is not str or role not in ROLES:
            raise ValueError("exact inference file lease role required")
        if self._admission_closed:
            raise LeaseCleanupError("file lease admission is closed")
        if len(self._leases) >= self._capacity:
            raise LeaseCleanupError("file lease registry capacity exhausted")
        path = _validate_path(path)
        lease = FileLease(self, role)
        try:
            # Allocate/register the placeholder BEFORE mkdir/open. Append failure
            # cannot lose a returned stream because no open has been attempted.
            self._leases.append(lease)
            path.parent.mkdir(parents=True, exist_ok=True)
            if self._admission_closed or lease._logical_closed:
                raise LeaseCleanupError("file lease admission closed before open")
            lease._stage = "open_attempted"
            lease._open_attempted = True
            # Store the returned object directly into its already-owned record.
            # An interruption before STORE_ATTR remains an unknown open outcome.
            lease._original_stream = _open_file(path)
            lease._open_returned = True
            lease._stream = lease._original_stream
            lease._original_type = type(lease._original_stream)
            if lease._original_stream is None:
                lease._acquisition_unknown = True
                self._hold_acquisition()
                raise LeaseCleanupError("original file acquisition returned no stream")
            lease._capture_close()
            if self._admission_closed or lease._logical_closed or lease._acquisition_unknown:
                raise LeaseCleanupError("file lease acquisition interrupted by cleanup")
            _initialize_file(lease._original_stream)
            lease._stage = "initialized"
            _acquire_lock(lease._original_stream)
            lease._lock_acquired = True
            lease._stage = "lock_acquired"
            _validate_open_file(lease._original_stream, path)
            lease._validation_complete = True
            lease._stage = "validation_complete"
        except BaseException as primary:
            if any(value is lease for value in self._leases):
                lease._acquisition_failed = True
                if lease._open_attempted and not lease._open_returned:
                    lease._acquisition_unknown = True
                    self._hold_acquisition()
                lease._event("acquisition_failed", primary)
                try:
                    lease.close()
                except BaseException:
                    # Preserve primary precedence, including independent cleanup failure.
                    pass
            raise
        finally:
            lease._acquisition_pending = False
        return lease

    def snapshot(self):
        self._owner()
        leases = tuple(lease.snapshot() for lease in self._leases)
        reserved = sum(lease._acquisition_pending for lease in self._leases)
        return RegistrySnapshot(
            self._token,
            self._generation,
            self._capacity,
            self._admission_closed,
            self._admission_held,
            self._acquisition_unknown,
            reserved,
            leases,
            self._admission_closed
            and not self._acquisition_unknown
            and reserved == 0
            and all(lease.physical_status in {"confirmed", "not_opened"} for lease in leases),
            self._last_errors,
        )

    def close_all(self):
        self._owner()
        self._admission_closed = True
        errors = []
        for lease in self._leases:
            try:
                lease.close()
            except BaseException:
                errors.append((lease.token, "original_lease_cleanup_unconfirmed"))
        self._last_errors = tuple(errors)
        return self.snapshot()
