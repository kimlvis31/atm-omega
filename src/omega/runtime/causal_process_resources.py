"""Original CPython host IPC ownership, role closure and retirement evidence.

Physical retirement is independent of normal model cleanup or durable state ACK.
This capsule never starts, stops, terminates, replaces or replays a child/request.
"""

# Imports
import os
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from multiprocessing.process import BaseProcess
from multiprocessing.queues import Queue
from uuid import uuid4


@dataclass(frozen=True)
class QueueRetirement:
    name: str
    reader_closed: bool
    writer_closed: bool
    feeder: str  # not_created, joined, unknown
    reader_close_ack: bool
    writer_close_ack: bool
    destructive_errors: tuple[str, ...]


@dataclass(frozen=True)
class HostRetirement:
    resource_token: str
    attempt: int
    status: str  # physical_retired or unknown
    pid: int | None
    exitcode: int | None
    death_observed_at_ns: int | None
    process_handle_closed: bool
    queues: tuple[QueueRetirement, ...]
    errors: tuple[str, ...]
    ever_uncertain: bool
    history: tuple[tuple[int, tuple[str, ...]], ...]
    process_destructive_unknown: bool
    scope: str = "original_cpython_host_process_feeder_endpoints_v1"
    normal_model_cleanup_confirmed: bool = False
    all_kernel_resources_confirmed: bool = False


class _EndpointCloseEvidence:
    """The only cross-thread mutable telemetry, installed before a local feeder."""

    def __init__(self, endpoint):
        self.endpoint, self.original_close = endpoint, endpoint.close
        self.lock = threading.RLock()
        self.returned, self.destructive_unknown = False, False
        self.failure_record = None
        self.errors = deque(maxlen=16)
        self.call_threads = deque(maxlen=16)

        def close():
            with self.lock:
                if self.destructive_unknown:
                    raise RuntimeError(
                        "original endpoint destructive close remains unconfirmed"
                    ) from None
                if self.returned:
                    if self.endpoint.closed is not True:
                        raise RuntimeError("returned endpoint close lacks closed observation")
                    return None
                if self.endpoint.closed is not False:
                    self.destructive_unknown = True
                    self.errors.append("closed_without_trusted_close_callback")
                    raise RuntimeError("original endpoint was closed without trusted callback ACK")
                self.call_threads.append(threading.get_ident())
                try:
                    result = self.original_close()
                except BaseException as error:
                    # Connection.close clears _handle even if native _close
                    # raises. Never invoke a later no-op/raw numeric fd close.
                    self.destructive_unknown = True
                    try:
                        detail = str(error)[:256]
                    except BaseException:
                        detail = "<exception text unavailable>"
                    self.failure_record = (
                        type(error).__name__[:128],
                        detail,
                        "original_endpoint_close_callback",
                    )
                    self.errors.append("native_close_exception:" + type(error).__name__)
                    raise
                self.returned = True
                if self.endpoint.closed is not True:
                    self.destructive_unknown = True
                    self.errors.append("close_return_without_closed_observation")
                    raise RuntimeError("original endpoint close returned without closed state")
                return result

        self.wrapper = close

    def snapshot(self):
        with self.lock:
            closed = self.endpoint.closed
            if closed is True and not self.returned:
                self.destructive_unknown = True
                if "closed_without_trusted_close_callback" not in self.errors:
                    self.errors.append("closed_without_trusted_close_callback")
            return (
                closed,
                self.returned and not self.destructive_unknown and closed is True,
                tuple(self.errors),
            )


class OriginalProcessResources:
    """Owner-thread capsule, retaining every original reference across retries.

    Construct after Process.start with open endpoints and no local feeders.
    Later local puts use put_owned; an
    externally appeared/replaced feeder never receives an inferred identity ACK.
    """

    def __init__(self, process, incoming, outgoing):
        if sys.implementation.name != "cpython" or sys.version_info[:2] != (3, 13):
            raise ValueError("Gate1 requires the reviewed CPython3.13 queue adapter")
        if not isinstance(process, BaseProcess) or any(
            type(value) is not Queue for value in (incoming, outgoing)
        ):
            raise ValueError(
                "original multiprocessing Process and two exact Queue objects required"
            )
        if incoming is outgoing:
            raise ValueError("incoming and outgoing must be separate original queues")
        # Complete preflight before installing any endpoint wrapper.
        if (
            process._closed
            or process._popen is None
            or process._parent_pid != os.getpid()
            or type(process.pid) is not int
            or process.pid <= 0
        ):
            raise ValueError("original Process.start must have completed with a positive PID")
        endpoint_pairs = tuple((value._reader, value._writer) for value in (incoming, outgoing))
        if len({id(endpoint) for pair in endpoint_pairs for endpoint in pair}) != 4:
            raise ValueError("constructor requires four distinct original endpoints")
        if any(value._thread is not None or value._closed for value in (incoming, outgoing)):
            raise ValueError("constructor requires two open queues before any local feeder")
        if any(endpoint.closed is not False for pair in endpoint_pairs for endpoint in pair):
            raise ValueError("constructor requires all four original endpoints open")
        if any(
            getattr(endpoint.close, "__self__", None) is not endpoint
            for pair in endpoint_pairs
            for endpoint in pair
        ):
            raise ValueError(
                "constructor requires original endpoint close methods, not prior wrappers"
            )
        finalizer = getattr(process._popen, "finalizer", None)
        if finalizer is None or finalizer.still_active() is not True:
            raise ValueError("constructor requires the original active native process finalizer")
        self.process, self.incoming, self.outgoing = process, incoming, outgoing
        self._process, self._queues = process, (incoming, outgoing)
        self._native = process._popen
        self._initial_pid = process.pid
        self._native_finalizer = getattr(self._native, "finalizer", None)
        self._owner_pid, self._owner_thread = os.getpid(), threading.current_thread()
        self.resource_token = uuid4().hex
        self._endpoints = tuple((value._reader, value._writer) for value in self._queues)
        self._feeders = [value._thread for value in self._queues]
        self._put = tuple(value.put for value in self._queues)
        self._attempt, self._history = 0, deque(maxlen=16)
        self._ever_uncertain, self._process_closed = False, False
        self._close_returned = False
        self._process_destructive_unknown = False
        self._original_process_close = process.close
        self._known_pid = self._exitcode = self._death_ns = None
        self._last = None
        self._pending_errors = deque(maxlen=16)
        self._endpoint_evidence = tuple(
            tuple(_EndpointCloseEvidence(endpoint) for endpoint in pair) for pair in self._endpoints
        )
        for pair in self._endpoint_evidence:
            for evidence in pair:
                evidence.endpoint.close = evidence.wrapper
        # Keep exact endpoints/thread objects; ids alone do not own resources.
        self._shape()

    def _owner(self):
        if os.getpid() != self._owner_pid or threading.current_thread() is not self._owner_thread:
            raise RuntimeError("capsule observations/mutations require the original owner thread")

    def owns(self, process, incoming, outgoing):
        """Read-only identity fence; it grants no foreign-thread mutation authority."""
        if (
            process is not self._process
            or incoming is not self._queues[0]
            or outgoing is not self._queues[1]
        ):
            return False
        try:
            self._shape()
        except Exception:
            return False
        return True

    def _shape(self):
        if (
            self.process is not self._process
            or self.incoming is not self._queues[0]
            or self.outgoing is not self._queues[1]
        ):
            raise RuntimeError("original process/queue identity substituted")
        expected_native = None if self._close_returned else self._native
        if self._process._popen is not expected_native:
            raise RuntimeError("original native process handle substituted or closed externally")
        if self._native is not None and self._native.finalizer is not self._native_finalizer:
            raise RuntimeError("original native process finalizer substituted")
        for index, value in enumerate(self._queues):
            reader, writer = self._endpoints[index]
            if value._reader is not reader or value._writer is not writer:
                raise RuntimeError("original queue endpoint identity substituted")
            if value._thread is not self._feeders[index]:
                raise RuntimeError("original feeder identity substituted or uncaptured")
            if self._feeders[index] is not None and not isinstance(
                self._feeders[index], threading.Thread
            ):
                raise RuntimeError("unsupported original feeder shape")
            for endpoint, evidence in zip(
                (reader, writer), self._endpoint_evidence[index], strict=True
            ):
                if endpoint.close is not evidence.wrapper or type(endpoint.closed) is not bool:
                    raise RuntimeError("unsupported original endpoint shape")

    def put_owned(self, name, payload, *, block=False, timeout=None):
        """Delegate an original put once; never recast, retry, duplicate or replay."""
        self._owner()
        self._shape()
        if self._process_closed or self._attempt:
            raise RuntimeError("retirement has started; no additional queue writes")
        if name not in {"incoming", "outgoing"}:
            raise ValueError("queue name must be incoming or outgoing")
        index = 0 if name == "incoming" else 1
        queue = self._queues[index]
        before = self._feeders[index]
        try:
            return self._put[index](payload, block=block, timeout=timeout)
        except BaseException as error:
            self._pending_errors.append("put_failed:" + type(error).__name__)
            self._ever_uncertain = True
            raise
        finally:
            # Queue.put may create its feeder before failing. Capture that exact
            # thread only within this originally delegated operation boundary.
            current = queue._thread
            if before is None and isinstance(current, threading.Thread):
                self._feeders[index] = current
            elif current is not before:
                self._pending_errors.append("put_feeder_identity_changed")
                self._ever_uncertain = True

    def close_unused_parent_ends(self):
        """Close original parent incoming reader/outgoing writer before the first put.

        Positive callbacks remain idempotent. This is role-specific endpoint
        evidence, never a process/model retirement or permission to replace it.
        """
        self._owner()
        self._shape()
        if self._process_closed or self._attempt:
            raise RuntimeError("retirement has started; no role endpoint transition")
        selected = (self._endpoint_evidence[0][0], self._endpoint_evidence[1][1])
        if all(evidence.snapshot()[1] for evidence in selected):
            return True, True
        if any(feeder is not None for feeder in self._feeders):
            raise RuntimeError("unused parent ends must close before the first local put")
        try:
            for evidence in selected:
                evidence.wrapper()
            acknowledged = tuple(evidence.snapshot()[1] for evidence in selected)
            if acknowledged != (True, True):
                raise RuntimeError("unused original parent endpoint close ACK unknown")
            return acknowledged
        except BaseException as error:
            self._ever_uncertain = True
            self._pending_errors.append("unused_parent_end_close_failed:" + type(error).__name__)
            raise

    def retire(self, *, timeout_s=1.0):
        """Attempt physical retirement within one total process/thread join budget.

        Connection/Queue/Process.close themselves expose no timeout in CPython;
        this draft does not claim a hard deadline around those OS calls.
        """
        self._owner()  # Hard affinity guard before even diagnostic state mutates.
        if type(timeout_s) not in {int, float} or not 0 <= timeout_s <= 5:
            raise ValueError("retirement join budget must be finite0..5seconds")
        self._attempt += 1
        errors = list(self._pending_errors)
        self._pending_errors.clear()
        queue_states = []
        deadline = time.monotonic() + timeout_s

        def remaining():
            return max(0.0, deadline - time.monotonic())

        def note(code, action):
            try:
                return True, action()
            except BaseException as error:
                errors.append(code + ":" + type(error).__name__)
                return False, None

        shape_ok, _ = note("identity_unknown", self._shape)
        if shape_ok:
            if self._process_destructive_unknown:
                errors.append("process_destructive_close_sticky_unknown")
                death_ok = False
            elif not self._close_returned:

                def observe_death():
                    pid = self._process.pid
                    alive = self._process.is_alive()
                    exitcode = self._process.exitcode
                    if (
                        type(pid) is not int
                        or pid <= 0
                        or pid != self._initial_pid
                        or alive is not False
                        or type(exitcode) is not int
                    ):
                        raise RuntimeError("original child is alive, unstarted or outcome unknown")
                    if self._known_pid is not None and (pid, exitcode) != (
                        self._known_pid,
                        self._exitcode,
                    ):
                        raise RuntimeError("original child identity/outcome changed")
                    if self._native_finalizer.still_active() is not True:
                        self._process_destructive_unknown = True
                        raise RuntimeError(
                            "native finalizer consumed without trusted Process.close ACK"
                        )
                    self._known_pid, self._exitcode = pid, exitcode
                    if self._death_ns is None:
                        self._death_ns = time.time_ns()

                death_ok, _ = note("child_death_unknown", observe_death)
                if death_ok:
                    joined, _ = note(
                        "process_join_unknown",
                        lambda: self._process.join(timeout=remaining()),
                    )
                    confirmed, _ = (
                        note("child_after_join_unknown", observe_death) if joined else (False, None)
                    )
                    death_ok = joined and confirmed
            else:
                # Only this capsule's own successful close may establish this
                # retained ACK; arbitrary externally closed handles never do.
                death_ok = self._known_pid is not None and self._exitcode is not None

            if death_ok:
                for index, queue in enumerate(self._queues):
                    name = ("incoming", "outgoing")[index]
                    reader, writer = self._endpoints[index]
                    feeder = self._feeders[index]
                    note(name + "_cancel_join_error", queue.cancel_join_thread)
                    note(name + "_queue_close_error", queue.close)
                    # A queue with no local feeder has no close finalizer. For a
                    # blocked feeder, closing these owned endpoints may break its
                    # send. Only a later actual join/not-alive proves retirement.
                    for label, endpoint in (("reader", reader), ("writer", writer)):
                        note(name + "_" + label + "_close_error", endpoint.close)
                    feeder_state = "not_created"
                    if feeder is not None:
                        joined, _ = note(
                            name + "_feeder_join_unknown",
                            lambda feeder=feeder: feeder.join(timeout=remaining()),
                        )
                        observed, alive = note(
                            name + "_feeder_observation_unknown", feeder.is_alive
                        )
                        feeder_state = (
                            "joined" if joined and observed and alive is False else "unknown"
                        )
                        if feeder_state == "unknown":
                            errors.append(name + "_feeder_not_retired")
                    reader_ok, reader_observation = note(
                        name + "_reader_state_unknown",
                        self._endpoint_evidence[index][0].snapshot,
                    )
                    writer_ok, writer_observation = note(
                        name + "_writer_state_unknown",
                        self._endpoint_evidence[index][1].snapshot,
                    )
                    reader_closed, reader_ack, reader_errors = (
                        reader_observation if reader_ok else (False, False, ())
                    )
                    writer_closed, writer_ack, writer_errors = (
                        writer_observation if writer_ok else (False, False, ())
                    )
                    if not reader_ok or reader_closed is not True:
                        errors.append(name + "_reader_not_closed")
                    if not writer_ok or writer_closed is not True:
                        errors.append(name + "_writer_not_closed")
                    if not reader_ack or not writer_ack:
                        errors.append(name + "_endpoint_destructive_ack_unknown")
                    queue_states.append(
                        QueueRetirement(
                            name,
                            reader_ok and reader_closed is True,
                            writer_ok and writer_closed is True,
                            feeder_state,
                            reader_ack,
                            writer_ack,
                            reader_errors + writer_errors,
                        )
                    )
                shape_after, _ = note("identity_after_cleanup_unknown", self._shape)
                if shape_after and not self._close_returned:
                    # Native pid/exit/death evidence above predates handle.close.
                    closed, _ = note("process_close_unknown", self._original_process_close)
                    self._close_returned = closed
                    if not closed:
                        self._process_destructive_unknown = True
                if self._close_returned:

                    def observe_own_closed_handle():
                        if (
                            self._process._popen is not None
                            or self._process._closed is not True
                            or self._native_finalizer is None
                            or self._native_finalizer.still_active() is not False
                        ):
                            raise RuntimeError("original native Process.close ACK incomplete")

                    self._process_closed, _ = note(
                        "native_close_ack_unknown", observe_own_closed_handle
                    )
        if errors:
            self._ever_uncertain = True
        self._history.append((self._attempt, tuple(errors)))
        ready = (
            not errors
            and self._process_closed
            and len(queue_states) == 2
            and all(
                value.reader_closed
                and value.writer_closed
                and value.reader_close_ack
                and value.writer_close_ack
                and value.feeder in {"joined", "not_created"}
                for value in queue_states
            )
        )
        self._last = HostRetirement(
            self.resource_token,
            self._attempt,
            "physical_retired" if ready else "unknown",
            self._known_pid,
            self._exitcode,
            self._death_ns,
            self._process_closed,
            tuple(queue_states),
            tuple(errors),
            self._ever_uncertain,
            tuple(self._history),
            self._process_destructive_unknown,
        )
        return self._last
