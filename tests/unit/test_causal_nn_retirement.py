"""Physical child retirement is independent of a port's closed flag."""

import multiprocessing
import threading

import pytest

from omega.domain.errors import StorageError
from omega.runtime.causal_nn_worker import CausalInferenceProcessWorker


class Channel:
    def __init__(self):
        self.closed = False

    def put_nowait(self, value):
        pass

    def cancel_join_thread(self):
        pass

    def close(self):
        self.closed = True


class Process:
    def __init__(self):
        self.alive, self.allow_exit, self.closed = True, False, False
        self.exitcode, self.terminations = None, 0

    def is_alive(self):
        if self.closed:
            raise ValueError("process handle already closed")
        return self.alive

    def join(self, timeout):
        if self.allow_exit:
            self.alive, self.exitcode = False, 0

    def terminate(self):
        self.terminations += 1

    def close(self):
        assert not self.alive
        self.closed = True


def worker_for(process):
    worker = object.__new__(CausalInferenceProcessWorker)
    worker.closed, worker.retired, worker._cancelled = False, False, False
    worker._thread = threading.get_ident()
    worker._process = process
    worker._process_retired, worker._channels_closed = False, False
    worker._cleanup_unconfirmed = False
    worker._incoming, worker._outgoing = Channel(), Channel()
    worker._pending, worker._results, worker.errors = {}, {}, {}
    return worker


def test_failed_join_retains_channels_and_actual_process_for_a_second_close():
    process = Process()
    worker = worker_for(process)
    with pytest.raises(StorageError, match="shutdown unconfirmed"):
        worker.close()
    assert worker.closed
    assert worker.retirement_status()["worker_alive"]
    assert not worker.retirement_status()["confirmed"]
    assert not worker._incoming.closed and not process.closed
    process.allow_exit = True
    worker.close()
    assert process.closed and worker._incoming.closed
    assert worker.retirement_status()["confirmed"]
    assert not worker.retirement_status()["worker_alive"]
    worker.close()


def test_unexpected_closed_handle_is_not_assumed_to_have_a_retirement_ack():
    process = Process()
    process.closed = True
    worker = worker_for(process)
    assert worker.retirement_status()["worker_alive"]
    assert not worker.retirement_status()["confirmed"]
    with pytest.raises(StorageError, match="observation is unconfirmed"):
        worker.close()
    assert not worker._process_retired and not worker._channels_closed


def _idle_child(channel):
    channel.get()


def test_real_spawned_child_is_joined_and_closed_handle_remains_confirmed():
    context = multiprocessing.get_context("spawn")
    incoming, outgoing = context.Queue(), context.Queue()
    process = context.Process(target=_idle_child, args=(incoming,))
    process.start()
    worker = worker_for(process)
    worker._incoming, worker._outgoing = incoming, outgoing
    assert worker.retirement_status()["worker_alive"]
    try:
        worker.close()
        assert worker.retirement_status()["confirmed"]
        assert not worker.retirement_status()["worker_alive"]
        # multiprocessing.Process.is_alive itself raises after Process.close;
        # the worker's explicit retirement ACK remains the actual evidence.
        with pytest.raises(ValueError):
            process.is_alive()
        worker.close()
    finally:
        if not worker._process_retired:
            process.terminate()
            process.join(timeout=3)
            process.close()
        incoming.close()
        outgoing.close()
