"""Standalone original read-connection scope; no source/actor integration.

Physical cleanup is independent of query success, durable state and recovery.
Native driver calls are synchronous and are not made preemptible by this scope.
"""

import inspect
import os
import re
import threading
from collections import deque
from dataclasses import dataclass
from itertools import islice
from uuid import uuid4


class ReadOperationError(RuntimeError):
    pass


class ReadCleanupError(RuntimeError):
    pass


@dataclass(frozen=True)
class ReadReceipt:
    generation: str
    scope_token: str
    read_token: str
    stage: str
    connect_entered: bool
    connect_returned: bool
    readonly_set_returned: bool
    schema_verified: bool
    operation_returned: bool
    candidate_count: int
    close_attempted: bool
    close_returned: bool
    post_closed: bool | None
    physical_status: str
    acquisition_unknown: bool
    destructive_unknown: bool
    ever_uncertain: bool
    primary_failure: str | None
    cleanup_failure: str | None
    history: tuple[str, ...]


@dataclass(frozen=True)
class ReadScopeReceipt:
    generation: str
    scope_token: str
    admission_closed: bool
    admission_held: bool
    read_in_progress: bool
    reads: int
    retired: int
    failed_reads: int
    active: ReadReceipt | None
    history: tuple[ReadReceipt, ...]
    physical_confirmed: bool
    scope: str = "original_inference_read_connections_v1"
    durable_state_confirmed: bool = False
    actor_recovery_authorized: bool = False
    all_kernel_resources_confirmed: bool = False


def _connect(settings):
    from omega.infrastructure.candles import connect_database

    return connect_database(settings)


def _verify(connection):
    from omega.infrastructure.postgres.schema import verify_schema

    verify_schema(connection)


def _initialize_read_only(connection):
    connection.execute("SET default_transaction_read_only=on")


class ReadConnectionLease:
    def __init__(self, scope):
        self._scope, self._token = scope, uuid4().hex
        self._original_connection = self._connection = None
        self._type = self._close = self._close_shape = None
        self._entered = self._returned = self._readonly = self._verified = False
        self._operation_returned = False
        self._count = 0
        self._attempted = self._close_returned = self._closing = False
        self._post_closed = None
        self._acquisition_unknown = self._destructive_unknown = self._ever_uncertain = False
        self._status, self._stage = "pending", "registered"
        self._primary = self._cleanup = None
        self._history, self._diagnostics = deque(maxlen=16), deque(maxlen=16)

    @property
    def original_connection(self):
        self._owner()
        return self._original_connection

    def _owner(self):
        self._scope._owner()
        if self._scope._active is not self:
            raise ReadCleanupError("original read lease is no longer active")

    def _error(self, code, error=None):
        self._ever_uncertain, self._status = True, "unknown"
        self._history.append(code)
        if error is not None:
            self._diagnostics.append((code, type(error).__name__[:64]))

    def _capture(self):
        self._connection = self._original_connection
        self._type = type(self._original_connection)
        self._close_shape = inspect.getattr_static(self._original_connection, "close")
        self._close = self._original_connection.close
        if not callable(self._close):
            raise TypeError("original connection close must be callable")

    def _identity(self):
        if (
            self._connection is not self._original_connection
            or type(self._original_connection) is not self._type
            or self._close is None
        ):
            self._destructive_unknown = True
            self._error("original_connection_identity_changed")
            raise ReadCleanupError("original read connection identity unconfirmed")
        try:
            shape = inspect.getattr_static(self._original_connection, "close")
        except AttributeError as error:
            # Known deletion is an identity failure, not a transient observer
            # outage that a later restored method can turn into a close ACK.
            self._destructive_unknown = True
            self._error("original_close_method_missing", error)
            raise ReadCleanupError("original read close method is missing") from None
        if shape is not self._close_shape:
            self._destructive_unknown = True
            self._error("original_connection_identity_changed")
            raise ReadCleanupError("original read connection identity unconfirmed")

    def snapshot(self):
        self._owner()
        return ReadReceipt(
            self._scope.generation,
            self._scope.token,
            self._token,
            self._stage,
            self._entered,
            self._returned,
            self._readonly,
            self._verified,
            self._operation_returned,
            self._count,
            self._attempted,
            self._close_returned,
            self._post_closed,
            self._status,
            self._acquisition_unknown,
            self._destructive_unknown,
            self._ever_uncertain,
            self._primary,
            self._cleanup,
            tuple(self._history),
        )

    def _observe_closed(self):
        closed = self._original_connection.closed
        self._post_closed = closed if type(closed) is bool else None
        if closed is not True:
            raise ReadCleanupError("original read connection close observation unconfirmed")
        if self._destructive_unknown or self._acquisition_unknown:
            raise ReadCleanupError("original read connection cleanup remains unknown")
        self._status = "confirmed"

    def close(self):
        self._owner()
        if self._closing:
            self._destructive_unknown = True
            self._error("original_close_reentered")
            raise ReadCleanupError("original read connection cleanup remains unknown")
        self._closing = True
        try:
            if self._attempted and not self._close_returned:
                self._destructive_unknown = True
                raise ReadCleanupError("original close attempt has no trusted return")
            if self._original_connection is None:
                self._acquisition_unknown = True
                raise ReadCleanupError("original connect outcome is unknown")
            self._identity()
            if self._destructive_unknown or self._acquisition_unknown:
                raise ReadCleanupError("original read connection cleanup remains unknown")
            if not self._close_returned:
                if self._original_connection.closed is not False:
                    self._destructive_unknown = True
                    raise ReadCleanupError("original connection closed without trusted callback")
                self._attempted = True
                try:
                    self._close()
                    self._close_returned = True
                except BaseException:
                    self._destructive_unknown = True
                    raise
            self._observe_closed()  # Retry only observation after a trusted return.
        except BaseException as error:
            self._cleanup = "original_connection_cleanup_unconfirmed"
            self._error(self._cleanup, error)
            raise
        finally:
            self._closing = False
        return self.snapshot()


class InferenceReadScope:
    def __init__(self, generation, *, connect=None, verify=None, history_limit=16):
        if type(generation) is not str or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", generation
        ):
            raise ValueError("bounded exact read generation required")
        if type(history_limit) is not int or not 1 <= history_limit <= 64:
            raise ValueError("native read history limit must be within [1,64]")
        if (
            connect is not None
            and not callable(connect)
            or verify is not None
            and not callable(verify)
        ):
            raise TypeError("read connection callbacks must be callable")
        self._generation, self._token = generation, uuid4().hex
        self._pid, self._thread = os.getpid(), threading.current_thread()
        self._connect = _connect if connect is None else connect
        self._verify = _verify if verify is None else verify
        self._active = None
        self._history = deque(maxlen=history_limit)
        self._reads = self._retired = self._failed = 0
        self._in_read = self._closed = self._held = False

    @property
    def generation(self):
        return self._generation

    @property
    def token(self):
        return self._token

    def _owner(self):
        if os.getpid() != self._pid or threading.current_thread() is not self._thread:
            raise RuntimeError("read scope requires its original PID and Thread object")

    def _retain_interrupt(self, error, fallback_type):
        try:
            error.read_scope = self
        except BaseException:
            fallback = fallback_type("original read interruption; scope retained")
            fallback.read_scope = self
            raise fallback from None

    @property
    def active(self):
        self._owner()
        return self._active

    def snapshot(self):
        self._owner()
        return ReadScopeReceipt(
            self.generation,
            self.token,
            self._closed,
            self._held,
            self._in_read,
            self._reads,
            self._retired,
            self._failed,
            None if self._active is None else self._active.snapshot(),
            tuple(self._history),
            self._closed
            and not self._in_read
            and self._active is None
            and self._reads == self._retired,
        )

    def _retire_active(self):
        lease = self._active
        receipt = lease.close()
        if receipt.physical_status != "confirmed":
            raise ReadCleanupError("original read connection cleanup remains unknown")
        self._history.append(receipt)  # Only confirmed originals may leave the active slot.
        self._retired += 1
        self._active = None

    def read(self, settings, operation, *, max_rows=4097):
        self._owner()
        if not callable(operation) or type(max_rows) is not int or not 1 <= max_rows <= 4097:
            raise ValueError("bounded callable read operation and native row limit required")
        if self._closed or self._held or self._active is not None or self._in_read:
            raise ReadOperationError("read connection admission is closed or held")
        if self._reads >= (1 << 63) - 1:
            raise ReadOperationError("read connection count capacity exhausted")
        lease = ReadConnectionLease(self)
        self._active = lease  # Preregister BEFORE connect; no post-connect append/allocation.
        self._reads += 1
        self._in_read = True
        try:
            try:
                lease._stage, lease._entered = "connect", True
                lease._original_connection = self._connect(settings)
                lease._returned = True
                lease._capture()
                lease._stage = "read_only"
                _initialize_read_only(lease._original_connection)
                lease._readonly = True
                lease._stage = "verify"
                self._verify(lease._original_connection)
                lease._verified = True
                lease._stage = "query"
                values = operation(lease._original_connection)
                lease._operation_returned = True
                candidate = tuple(islice(iter(values), max_rows + 1))
                lease._count = len(candidate)
                if len(candidate) > max_rows:
                    raise ValueError("read operation exceeded row bound")
                if self._closed or self._held:
                    raise ReadOperationError("read admission closed during operation")
            except BaseException as primary:
                self._failed += 1
                lease._primary = "read_" + lease._stage + "_failed"
                lease._error(lease._primary, primary)
                if lease._entered and not lease._returned:
                    lease._acquisition_unknown = True
                try:
                    self._retire_active()
                except BaseException:
                    self._held = True
                if not isinstance(primary, Exception):
                    self._retain_interrupt(primary, ReadOperationError)
                    raise
                raise ReadOperationError("committed inference read unavailable") from None
            try:
                self._retire_active()
            except BaseException as error:
                self._held = True
                if not isinstance(error, Exception):
                    self._retain_interrupt(error, ReadCleanupError)
                    raise
                raise ReadCleanupError("committed inference read cleanup unconfirmed") from None
            if self._closed or self._held:
                raise ReadOperationError("read admission closed before publication")
            return candidate
        finally:
            self._in_read = False

    def close(self):
        self._owner()
        self._closed = True
        if self._in_read:
            self._held = True
            raise ReadCleanupError("original read is still in progress")
        if self._active is not None:
            try:
                self._retire_active()
            except BaseException as error:
                self._held = True
                if not isinstance(error, Exception):
                    self._retain_interrupt(error, ReadCleanupError)
                    raise
                raise ReadCleanupError("committed inference read cleanup unconfirmed") from None
        return self.snapshot()
