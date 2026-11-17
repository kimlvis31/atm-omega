"""Complete-message handoff; no native receive on the owner thread.

The resource owner must unblock receive and retire its original process/queues.
This channel never closes those resources and never authorizes result acceptance.
"""

# Imports
import os
import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class ReceiptFailure:
    kind: Literal["eof", "transport", "overflow"]
    error_type: str
    message: str


@dataclass(frozen=True, slots=True)
class ReceiptBatch:
    messages: tuple[object, ...]
    error: ReceiptFailure | None
    reader_done: bool


@dataclass(frozen=True, slots=True)
class ReceiptRetirementStatus:
    closed: bool
    started: bool
    reader_alive: bool
    reader_done: bool
    join_confirmed: bool
    error: ReceiptFailure | None


class CausalReceiptChannel:
    """One original receive callable, one daemon reader, one bounded memory inbox.

    Public calls belong to the constructor's original process/thread. start is
    single-use, including after a failed start. close is explicitly retryable.
    """

    def __init__(
        self,
        receive: Callable[[], object],
        *,
        capacity: int = 8,
        on_available: Callable[[], None] | None = None,
    ) -> None:
        if type(capacity) is not int or not 1 <= capacity <= 32:
            raise ValueError("receipt channel capacity must be a native int in 1..32")
        if not callable(receive):
            raise TypeError("receipt channel receive must be callable")
        if on_available is not None and not callable(on_available):
            raise TypeError("receipt channel notifier must be callable or None")
        self._owner_pid = os.getpid()
        self._owner_thread = threading.current_thread()
        self._capacity = capacity
        self._receive = receive
        self._on_available = on_available
        self._inbox: queue.Queue[object] = queue.Queue(maxsize=capacity + 1)
        self._state_lock = threading.Lock()
        self._reader_done = threading.Event()
        self._error: ReceiptFailure | None = None
        self._start_attempted = False
        self._started = False
        self._closed = False
        self._join_confirmed = False
        # Original callbacks are arguments to this one thread, never fetched
        # again from mutable worker/queue/channel attributes in the receive loop.
        self._reader = threading.Thread(
            target=self._receive_loop,
            args=(receive, on_available),
            name="causal-receipt-reader",
            daemon=True,
        )

    def _guard(self) -> None:
        if os.getpid() != self._owner_pid or threading.current_thread() is not self._owner_thread:
            raise RuntimeError("receipt channel call outside original owner process/thread")

    def _record_error(
        self, kind: Literal["eof", "transport", "overflow"], error: BaseException
    ) -> None:
        # Retain only bounded plain metadata, never an exception/traceback graph.
        try:
            message = str(error)[:384]
        except BaseException:
            message = "exception text unavailable"
        failure = ReceiptFailure(kind, type(error).__name__[:96], message)
        with self._state_lock:
            if self._error is None:
                self._error = failure

    def _notify(self, on_available: Callable[[], None] | None) -> bool:
        if on_available is None:
            return True
        try:
            # Called only after releasing memory-inbox and metadata locks. The
            # injected callback must be a brief thread-safe condition wakeup.
            on_available()
        except BaseException as error:
            self._record_error("transport", error)
            return False
        return True

    def _receive_loop(
        self, receive: Callable[[], object], on_available: Callable[[], None] | None
    ) -> None:
        notifier_failed = False
        try:
            while True:
                try:
                    message = receive()  # Event-driven native wait lives here only.
                except EOFError as error:
                    self._record_error("eof", error)
                    return
                except BaseException as error:
                    self._record_error("transport", error)
                    return
                try:
                    self._inbox.put_nowait(message)
                except queue.Full as error:
                    # The extra complete message cannot be retained. Stop at the
                    # first overflow and report loss; never drop-and-continue.
                    self._record_error("overflow", error)
                    return
                if not self._notify(on_available):
                    notifier_failed = True
                    return
        finally:
            self._reader_done.set()
            if not notifier_failed:
                # A failed message notification ends the reader without retry.
                # EOF/transport/overflow stays first even if final wakeup fails.
                self._notify(on_available)

    # Owner operations
    def start(self) -> None:
        self._guard()
        if self._closed or self._start_attempted or self._reader_done.is_set():
            raise RuntimeError("receipt channel cannot start again or after closing")
        self._start_attempted = True
        try:
            self._reader.start()
        except BaseException as error:
            self._record_error("transport", error)
            raise
        self._started = True

    def drain(self) -> ReceiptBatch:
        """Take at most capacity+1 local objects, preserving FIFO and identity."""
        self._guard()
        # Snapshot terminal metadata first: a newly published EOF/error must not
        # overtake a final complete frame enqueued after our last Empty read.
        # A terminal observed here follows all its earlier bounded inbox puts.
        with self._state_lock:
            error = self._error
            done = self._reader_done.is_set()
        messages = []
        for _ in range(self._capacity + 1):
            try:
                messages.append(self._inbox.get_nowait())
            except queue.Empty:
                break
        return ReceiptBatch(tuple(messages), error, done)

    def close(self, timeout_s: float = 1) -> bool:
        """Close admission and bounded-join this reader; external owner unblocks it.

        False retains the original reader/callable/inbox for an explicit retry.
        Thread.join's budget does not establish an OS-wide hard time guarantee.
        """
        self._guard()
        if type(timeout_s) not in {int, float} or not 0 <= timeout_s <= 5:
            raise ValueError("receipt close timeout must be a finite native number in 0..5")
        self._closed = True
        if not self._start_attempted:
            self._join_confirmed = True  # Definitively never started; start now prohibited.
            return True
        if self._reader.ident is None:
            # An interrupted/failed start can leave native launch uncertain.
            # Only never-attempted start above proves no reader could appear.
            return False
        self._reader.join(timeout=timeout_s)
        self._join_confirmed = not self._reader.is_alive()
        return self._join_confirmed

    def retirement_status(self) -> ReceiptRetirementStatus:
        self._guard()
        with self._state_lock:
            error = self._error
            done = self._reader_done.is_set()
        return ReceiptRetirementStatus(
            self._closed,
            self._started or self._reader.ident is not None,
            self._reader.is_alive(),
            done,
            self._join_confirmed,
            error,
        )
