"""Complete-message channel lifecycle and thread-safe wakeup boundaries."""

import multiprocessing
import os
import queue
import threading
from dataclasses import FrozenInstanceError
from unittest.mock import patch

import pytest

from omega.runtime.causal_receipt_channel import CausalReceiptChannel


class ControlledReceive:
    """Test owner explicitly releases each receive; no timing-based partial IO."""

    def __init__(self):
        self.entered = threading.Event()
        self.items = queue.Queue()
        self.call_threads = []

    def __call__(self):
        self.call_threads.append(threading.current_thread())
        self.entered.set()
        value = self.items.get()
        if isinstance(value, BaseException):
            raise value
        return value


def test_blocked_receive_drain_and_retry_retain_original_resources():
    receive = ControlledReceive()
    channel = CausalReceiptChannel(receive)
    original = channel._reader, channel._receive, channel._inbox
    try:
        channel.start()
        assert receive.entered.wait(2)
        assert channel._reader.daemon
        # Receive remains blocked until this owner releases it below. Reaching
        # these assertions demonstrates drain never called the native receiver.
        assert channel.drain().messages == ()
        assert receive.call_threads == [original[0]]
        with pytest.raises(RuntimeError):
            channel.start()
        assert channel.close(0) is False
        status = channel.retirement_status()
        assert status.closed and status.reader_alive and not status.join_confirmed
        assert original == (channel._reader, channel._receive, channel._inbox)
        with pytest.raises(RuntimeError):
            channel.start()
    finally:
        receive.items.put(EOFError("external owner unblocked original receive"))
        assert channel.close(2)
    status = channel.retirement_status()
    assert status.closed and status.join_confirmed and not status.reader_alive
    assert status.reader_done and status.error.kind == "eof"
    assert original == (channel._reader, channel._receive, channel._inbox)


def test_fifo_malformed_values_and_identity_are_passed_without_interpretation():
    values = [object(), None, ["malformed"], {"arbitrary": "value"}]
    receive = ControlledReceive()
    for value in values:
        receive.items.put(value)
    receive.items.put(EOFError("last writer closed"))
    channel = CausalReceiptChannel(receive, capacity=4)
    try:
        channel.start()
        assert channel._reader_done.wait(2)
        batch = channel.drain()
        assert len(batch.messages) == len(values)
        assert all(
            actual is expected for actual, expected in zip(batch.messages, values, strict=True)
        )
        assert batch.reader_done and batch.error.kind == "eof"
        assert channel.drain().error is batch.error
        with pytest.raises(FrozenInstanceError):
            batch.reader_done = False
        with pytest.raises(FrozenInstanceError):
            batch.error.message = "cleared"
        assert not channel.retirement_status().join_confirmed
    finally:
        assert channel.close(2)


def test_overflow_stops_after_first_lost_message_and_failure_stays_sticky():
    receive = ControlledReceive()
    values = [object() for _ in range(4)]
    for value in values:
        receive.items.put(value)
    channel = CausalReceiptChannel(receive, capacity=1)
    try:
        channel.start()
        assert channel._reader_done.wait(2)
        batch = channel.drain()
        assert batch.messages == tuple(values[:2])  # capacity+1 memory inbox.
        assert len(receive.call_threads) == 3  # Never consume the fourth object.
        assert batch.error.kind == "overflow" and batch.reader_done
        assert channel.drain().error is batch.error
        assert channel.drain().messages == ()
        with pytest.raises(RuntimeError):
            channel.start()
    finally:
        assert channel.close(2)


@pytest.mark.parametrize("error_type", [OSError, RuntimeError, SystemExit])
def test_transport_failure_is_bounded_plain_state_and_not_cleared(error_type):
    receive = ControlledReceive()
    receive.items.put(error_type("x" * 1000))
    channel = CausalReceiptChannel(receive)
    try:
        channel.start()
        assert channel._reader_done.wait(2)
        failure = channel.drain().error
        assert failure.kind == "transport"
        assert failure.error_type == error_type.__name__
        assert failure.message == "x" * 384
        assert not isinstance(failure, BaseException)
        assert channel.drain().error is failure
    finally:
        assert channel.close(2)
    assert channel.retirement_status().error is failure


def test_exception_with_broken_text_still_finishes_with_transport_failure():
    class BrokenTextError(Exception):
        def __str__(self):
            raise ValueError("text failure")

    receive = ControlledReceive()
    receive.items.put(BrokenTextError())
    channel = CausalReceiptChannel(receive)
    try:
        channel.start()
        assert channel._reader_done.wait(2)
        assert channel.drain().error.message == "exception text unavailable"
    finally:
        assert channel.close(2)


def test_never_started_close_confirms_only_reader_absence_and_prohibits_start():
    channel = CausalReceiptChannel(lambda: pytest.fail("never started"))
    assert not channel.retirement_status().join_confirmed
    assert channel.close(0)
    status = channel.retirement_status()
    assert status.closed and status.join_confirmed
    assert not status.started and not status.reader_alive and not status.reader_done
    with pytest.raises(RuntimeError):
        channel.start()


def test_start_failure_is_sticky_and_cannot_create_a_replacement_reader():
    channel = CausalReceiptChannel(lambda: pytest.fail("failed start"))
    original = channel._reader
    with patch.object(original, "start", side_effect=RuntimeError("start failed")):
        with pytest.raises(RuntimeError, match="start failed"):
            channel.start()
    assert channel.drain().error.kind == "transport"
    assert not channel.drain().reader_done  # Only reader finally may set done.
    with pytest.raises(RuntimeError):
        channel.start()
    assert channel.close(0) is False  # Failed start retains conservative launch uncertainty.
    assert not channel.retirement_status().join_confirmed
    assert channel._reader is original


@pytest.mark.parametrize("method", ["start", "drain", "close", "retirement_status"])
def test_foreign_thread_rejected_before_state_mutation(method):
    channel = CausalReceiptChannel(lambda: pytest.fail("foreign receive"))
    before = channel.retirement_status()
    errors = []

    def foreign():
        try:
            getattr(channel, method)()
        except RuntimeError as error:
            errors.append(str(error))

    other = threading.Thread(target=foreign, daemon=True)
    other.start()
    other.join(2)
    assert not other.is_alive() and len(errors) == 1
    assert channel.retirement_status() == before
    assert channel.close(0)


@pytest.mark.parametrize("method", ["start", "drain", "close", "retirement_status"])
def test_foreign_pid_rejected_before_state_mutation(method):
    channel = CausalReceiptChannel(lambda: pytest.fail("foreign receive"))
    before = channel.retirement_status()
    with patch("omega.runtime.causal_receipt_channel.os.getpid", return_value=os.getpid() + 1):
        with pytest.raises(RuntimeError):
            getattr(channel, method)()
    assert channel.retirement_status() == before
    assert channel.close(0)


@pytest.mark.parametrize("capacity", [True, False, 0, 33, 1.0, "8", None])
def test_invalid_capacity_rejected(capacity):
    with pytest.raises(ValueError):
        CausalReceiptChannel(lambda: None, capacity=capacity)


def test_noncallable_receive_rejected():
    with pytest.raises(TypeError):
        CausalReceiptChannel(None)


@pytest.mark.parametrize(
    "timeout", [True, False, -1, 5.001, 10**1000, float("inf"), float("nan"), "1", None]
)
def test_invalid_close_budget_does_not_close_or_mutate(timeout):
    channel = CausalReceiptChannel(lambda: None)
    before = channel.retirement_status()
    with pytest.raises(ValueError):
        channel.close(timeout)
    assert channel.retirement_status() == before
    assert channel.close(0)


def _spawn_complete_message(outgoing):
    # Child role ends: original write-only Queue; no model, DB or Omega imports.
    outgoing._reader.close()
    outgoing.put(("original", {"value": 7}))
    outgoing.close()
    outgoing.join_thread()


def test_actual_spawn_queue_complete_frame_then_role_end_eof():
    context = multiprocessing.get_context("spawn")
    outgoing = context.Queue(maxsize=2)
    producer = context.Process(target=_spawn_complete_message, args=(outgoing,), daemon=True)
    channel = CausalReceiptChannel(outgoing.get, capacity=2)
    try:
        producer.start()
        # Spawn delivered the original child handles before parent role closure.
        outgoing._writer.close()
        channel.start()
        producer.join(5)
        assert not producer.is_alive() and producer.exitcode == 0
        assert channel._reader_done.wait(5)
        batch = channel.drain()
        assert batch.messages == (("original", {"value": 7}),)
        assert batch.error.kind == "eof" and batch.reader_done
        assert channel.close(2)
    finally:
        if producer.pid is not None:
            if producer.is_alive():
                producer.terminate()
                producer.join(2)
            if producer.is_alive():
                producer.kill()
                producer.join(2)
            assert not producer.is_alive()
            producer.close()
        # Explicit external resource-owner actions; never channel-side closure.
        outgoing._writer.close()
        assert channel.close(2)
        outgoing.close()
        outgoing._reader.close()


@pytest.mark.parametrize("notifier", [False, 1, "wake", object()])
def test_invalid_availability_notifier_rejected(notifier):
    with pytest.raises(TypeError):
        CausalReceiptChannel(lambda: None, on_available=notifier)


def test_availability_notifier_identity_and_reader_thread_are_captured_once():
    receive = ControlledReceive()
    values = [object(), object()]
    for value in values:
        receive.items.put(value)
    receive.items.put(EOFError("original EOF"))
    notifications = []

    def original_notifier():
        notifications.append(threading.current_thread())

    channel = CausalReceiptChannel(receive, on_available=original_notifier)
    # No supported swapping seam exists. Even private attribute replacement
    # cannot change the original callback passed to the one reader's arguments.
    channel._on_available = lambda: pytest.fail("replacement notifier called")
    try:
        channel.start()
        assert channel._reader_done.wait(2)
        assert channel.close(2)  # Includes completion of the terminal callback.
        assert notifications == [channel._reader] * 3
        batch = channel.drain()
        assert batch.messages == tuple(values)
        assert batch.error.kind == "eof" and batch.reader_done
    finally:
        assert channel.close(2)


def test_notifier_runs_after_releasing_inbox_and_metadata_locks():
    receive = ControlledReceive()
    receive.items.put(object())
    receive.items.put(EOFError("done"))
    lock_observations = []

    def notify():
        states = []
        for lock in (channel._inbox.mutex, channel._state_lock):
            acquired = lock.acquire(blocking=False)
            states.append(acquired)
            if acquired:
                lock.release()
        lock_observations.append(tuple(states))

    channel = CausalReceiptChannel(receive, on_available=notify)
    try:
        channel.start()
        assert channel._reader_done.wait(2)
        assert channel.close(2)
        assert lock_observations == [(True, True), (True, True)]
    finally:
        assert channel.close(2)


def test_condition_waiting_notifier_does_not_invert_owner_drain_lock_order():
    receive = ControlledReceive()
    value = object()
    receive.items.put(value)
    actor_condition = threading.Condition()
    notifying = threading.Event()
    notifications = []

    def notify():
        notifying.set()
        with actor_condition:
            notifications.append(threading.current_thread())
            actor_condition.notify_all()

    channel = CausalReceiptChannel(receive, on_available=notify)
    try:
        with actor_condition:
            channel.start()
            assert notifying.wait(2)
            # Reader is waiting for our condition. Inspect local locks without
            # blocking first so a regression fails without hanging this owner.
            for lock in (channel._inbox.mutex, channel._state_lock):
                acquired = lock.acquire(blocking=False)
                if acquired:
                    lock.release()
                assert acquired
            assert channel.drain().messages == (value,)
    finally:
        receive.items.put(EOFError("external resource owner released receive"))
        assert channel.close(2)
    assert notifications == [channel._reader, channel._reader]


def test_first_message_notifier_failure_stops_without_retry_or_losing_queued_value():
    receive = ControlledReceive()
    value, later = object(), object()
    receive.items.put(value)
    receive.items.put(later)
    calls = []

    def notify():
        calls.append(threading.current_thread())
        raise RuntimeError("notifier failed " + "x" * 1000)

    channel = CausalReceiptChannel(receive, on_available=notify)
    try:
        channel.start()
        assert channel._reader_done.wait(2)
        assert channel.close(2)
        batch = channel.drain()
        assert batch.messages == (value,)
        assert batch.error.kind == "transport"
        assert batch.error.error_type == "RuntimeError"
        assert len(batch.error.message) == 384
        assert batch.reader_done and len(receive.call_threads) == 1
        assert calls == [channel._reader]  # No terminal retry of failed callback.
        assert channel.drain().error is batch.error
        assert channel.retirement_status().error is batch.error
        assert receive.items.get_nowait() is later
    finally:
        assert channel.close(2)


@pytest.mark.parametrize("failure", [EOFError("original EOF"), OSError("original transport")])
def test_terminal_notifier_failure_preserves_first_transport_failure(failure):
    receive = ControlledReceive()
    receive.items.put(failure)
    calls = []

    def notify():
        calls.append(threading.current_thread())
        raise RuntimeError("secondary notifier error")

    channel = CausalReceiptChannel(receive, on_available=notify)
    try:
        channel.start()
        assert channel._reader_done.wait(2)
        assert channel.close(2)
        batch = channel.drain()
        assert batch.messages == ()
        assert batch.error.error_type == type(failure).__name__
        assert batch.error.message == str(failure)
        assert calls == [channel._reader]
        assert channel.drain().error is batch.error
    finally:
        assert channel.close(2)


def test_terminal_notifier_failure_preserves_first_overflow():
    receive = ControlledReceive()
    values = [object() for _ in range(3)]
    for value in values:
        receive.items.put(value)
    calls = []

    def notify():
        calls.append(threading.current_thread())
        if len(calls) == 3:
            raise RuntimeError("terminal notifier failed after overflow")

    channel = CausalReceiptChannel(receive, capacity=1, on_available=notify)
    try:
        channel.start()
        assert channel._reader_done.wait(2)
        assert channel.close(2)
        batch = channel.drain()
        assert batch.messages == tuple(values[:2])
        assert batch.error.kind == "overflow" and batch.reader_done
        assert calls == [channel._reader] * 3
        assert channel.drain().error is batch.error
    finally:
        assert channel.close(2)


def test_final_frame_and_eof_after_observed_empty_wait_for_next_batch():
    receive = ControlledReceive()
    final_value = object()
    channel = CausalReceiptChannel(receive)
    original_get = channel._inbox.get_nowait
    observed_empty = []

    def empty_then_publish_final_frame():
        try:
            return original_get()
        except queue.Empty:
            observed_empty.append(True)
            receive.items.put(final_value)
            receive.items.put(EOFError("terminal after final frame"))
            assert channel._reader_done.wait(2)
            raise

    try:
        channel.start()
        assert receive.entered.wait(2)
        with patch.object(channel._inbox, "get_nowait", side_effect=empty_then_publish_final_frame):
            first = channel.drain()
        assert observed_empty == [True]
        assert first.messages == ()
        assert first.error is None and not first.reader_done
        second = channel.drain()
        assert len(second.messages) == 1 and second.messages[0] is final_value
        assert second.error.kind == "eof" and second.reader_done
        assert channel.drain().error is second.error
        assert channel.drain().messages == ()
    finally:
        receive.items.put(EOFError("external owner cleanup"))
        assert channel.close(2)
