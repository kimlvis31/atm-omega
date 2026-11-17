"""Original resource vectors plus role-closed complete frames and owner objects."""

import json
import multiprocessing
import sys
import threading
import time
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest

from omega.runtime.causal_process_resources import OriginalProcessResources


def _exchange_then_exit(incoming, outgoing, exitcode):
    payload = incoming.get(timeout=5)
    outgoing.put(("original", payload))
    raise SystemExit(exitcode)


def _wait_then_exit(ready, finish, exitcode):
    ready.set()
    finish.wait(timeout=5)
    raise SystemExit(exitcode)


def _close_unused(queue, native_returned_fixture=None):
    queue.cancel_join_thread()
    queue.close()
    # Let an ordinary Queue feeder finish its saved native close callbacks
    # before parent endpoint.close can race them (notably on Windows).
    if queue._thread is not None:
        queue._thread.join(timeout=1)
        assert not queue._thread.is_alive(), (
            "Original feeder remains alive after bounded test cleanup; "
            "verification is failed/held, not a retirement success"
        )
    for endpoint in (queue._reader, queue._writer):
        if native_returned_fixture is not None and endpoint is native_returned_fixture[0]:
            marker = native_returned_fixture[1]
            assert marker["native_close_returned"] is True
            assert marker["deliberate_post_close_exception"] is True
            assert endpoint.closed is True
            # Test-only positive native-return evidence. Keep its failed wrapper
            # and capsule sticky state intact; do not invoke/re-close this endpoint.
        else:
            endpoint.close()


def _stop_owned(process, queues, native_returned_fixture=None):
    # Test cleanup only; the production capsule never performs these actions.
    if not process._closed:
        if process.is_alive():
            process.terminate()
        process.join(timeout=3)
        assert not process.is_alive()
        process.close()
    for queue in queues:
        _close_unused(queue, native_returned_fixture)


def _finish_actual(
    tmp_path,
    name,
    process,
    queues,
    receipts,
    native_returned_fixture=None,
    native_process_fixture=None,
):
    confirmed, error = False, None
    try:
        if native_process_fixture is not None:
            assert native_process_fixture["native_close_returned"] is True
            assert native_process_fixture["deliberate_post_close_exception"] is True
        _stop_owned(process, queues, native_returned_fixture)
        confirmed = True
    except BaseException as failure:
        error = repr(failure)
        raise
    finally:
        (tmp_path / (name + ".json")).write_text(
            json.dumps(
                {
                    "scope": "actual_gate1_process_feeder_endpoint_vector",
                    "platform": sys.platform,
                    "python": sys.version,
                    "first": asdict(receipts[0]) if receipts else None,
                    "final": asdict(receipts[-1]) if receipts else None,
                    "attempts": [asdict(receipt) for receipt in receipts],
                    "test_cleanup_confirmed": confirmed,
                    "test_cleanup_error": error,
                    "injected_endpoint_native_return_evidence": (
                        native_returned_fixture[1] if native_returned_fixture is not None else None
                    ),
                    "injected_process_native_return_evidence": native_process_fixture,
                },
                indent=2,
                allow_nan=False,
            ),
            encoding="utf-8",
        )


@pytest.mark.parametrize("exitcode", [0, 7])
def test_actual_spawn_original_process_normal_feeder_and_no_feeder_outgoing(exitcode, tmp_path):
    context = multiprocessing.get_context("spawn")
    incoming, outgoing = context.Queue(2), context.Queue(2)
    process = context.Process(target=_exchange_then_exit, args=(incoming, outgoing, exitcode))
    process.start()
    capsule = OriginalProcessResources(process, incoming, outgoing)
    pid = process.pid
    receipts = []
    try:
        payload = {"untouched": [1, "two", None]}
        capsule.put_owned("incoming", payload)
        assert outgoing.get(timeout=5) == ("original", payload)
        process.join(timeout=5)
        assert process.exitcode == exitcode
        receipt = capsule.retire(timeout_s=1)
        receipts.append(receipt)
        assert receipt.status == "physical_retired"
        assert receipt.pid == pid and receipt.exitcode == exitcode
        assert receipt.death_observed_at_ns is not None and receipt.process_handle_closed
        assert [item.feeder for item in receipt.queues] == ["joined", "not_created"]
        assert all(item.reader_closed and item.writer_closed for item in receipt.queues)
        assert not receipt.normal_model_cleanup_confirmed
        assert not receipt.all_kernel_resources_confirmed
        assert capsule._process is process and capsule._queues == (incoming, outgoing)
        assert capsule.owns(process, incoming, outgoing)
        # Process APIs now reject the closed handle; only our prior original
        # death/close observations survive. An idempotent attempt retains them.
        with pytest.raises(ValueError):
            process.is_alive()
        again = capsule.retire(timeout_s=0)
        receipts.append(again)
        assert again.status == "physical_retired" and again.resource_token == receipt.resource_token
    finally:
        _finish_actual(
            tmp_path,
            f"actual-spawn-exit{exitcode}",
            process,
            (incoming, outgoing),
            receipts,
        )


def dead_fixture(*, make_capsule=True):
    context = multiprocessing.get_context("spawn")
    incoming, outgoing = context.Queue(2), context.Queue(2)
    ready, finish = context.Event(), context.Event()
    process = context.Process(target=_wait_then_exit, args=(ready, finish, 9))
    process.start()
    assert ready.wait(timeout=5)
    capsule = OriginalProcessResources(process, incoming, outgoing) if make_capsule else None
    finish.set()
    process.join(timeout=5)
    assert process.exitcode == 9
    return process, incoming, outgoing, capsule


def test_actual_dead_child_queues_with_no_local_feeder_have_owned_endpoints_closed(
    tmp_path,
):
    process, incoming, outgoing, capsule = dead_fixture()
    receipts = []
    try:
        # Plain Queue.close has no feeder finalizer and leaves these open.
        incoming.close()
        assert incoming._reader.closed is False and incoming._writer.closed is False
        result = capsule.retire(timeout_s=0.5)
        receipts.append(result)
        assert result.status == "physical_retired"
        assert all(item.feeder == "not_created" for item in result.queues)
        assert incoming._reader.closed and incoming._writer.closed
    finally:
        _finish_actual(tmp_path, "actual-no-local-feeders", process, (incoming, outgoing), receipts)


def test_actual_large_buffer_dead_child_bounded_join_or_explicit_unknown_not_fake_ack(
    tmp_path,
):
    process, incoming, outgoing, capsule = dead_fixture()
    receipts = []
    try:
        capsule.put_owned("incoming", b"x" * (4 * 1024 * 1024))
        feeder = incoming._thread
        started = time.monotonic()
        result = capsule.retire(timeout_s=0.25)
        receipts.append(result)
        assert time.monotonic() - started < 2
        assert capsule._feeders[0] is feeder
        if result.status == "unknown":
            assert result.errors and result.ever_uncertain
            result = capsule.retire(timeout_s=1)
            receipts.append(result)
        if result.status == "physical_retired":
            assert not feeder.is_alive()
            assert result.queues[0].feeder == "joined"
            assert incoming._reader.closed and incoming._writer.closed
        else:
            # This is an explicitly held resource outcome, not successful host
            # retirement. Two-OS validation must report the actual disposition.
            assert result.status == "unknown" and result.errors
            assert result.queues[0].feeder == "unknown" or any(
                "error" in item for item in result.errors
            )
            assert capsule._queues[0] is incoming and capsule._feeders[0] is feeder
    finally:
        _finish_actual(
            tmp_path,
            "actual-large-buffer-dead-child",
            process,
            (incoming, outgoing),
            receipts,
        )


@pytest.mark.parametrize("fault", ["join", "read", "queue_close"])
def test_same_original_fault_then_explicit_retry_retains_uncertainty_history(fault):
    process, incoming, outgoing, _unused = dead_fixture(make_capsule=False)
    active = [True]
    if fault == "join":
        original = process.join

        def operation(*args, **kwargs):
            if active[0]:
                raise RuntimeError("simulated bounded join failure")
            return original(*args, **kwargs)

        process.join = operation
    elif fault == "read":
        original = process.is_alive

        def operation():
            if active[0]:
                raise ValueError("simulated native observation failure")
            return original()

        process.is_alive = operation
    elif fault == "queue_close":
        original = incoming.close

        def operation():
            if active[0]:
                raise OSError("simulated Queue.close failure")
            return original()

        incoming.close = operation
    capsule = OriginalProcessResources(process, incoming, outgoing)
    try:
        first = capsule.retire(timeout_s=0.1)
        assert first.status == "unknown" and first.errors and first.ever_uncertain
        active[0] = False
        second = capsule.retire(timeout_s=0.5)
        assert second.status == "physical_retired" and second.ever_uncertain
        assert second.history[0][1] == first.errors
        assert capsule._process is process and capsule._endpoints[0][0] is incoming._reader
    finally:
        active[0] = False
        _stop_owned(process, (incoming, outgoing))


def test_original_native_endpoint_close_failure_clears_handle_but_is_sticky_unknown(
    tmp_path,
):
    process, incoming, outgoing, capsule = dead_fixture()
    endpoint = incoming._reader
    native_close, calls = endpoint._close, []
    marker = {"native_close_returned": False, "deliberate_post_close_exception": True}
    receipts = []

    def destructive_close():
        calls.append("native_close")
        native_close()  # Safe controlled fixture: native resource really closed.
        marker["native_close_returned"] = True
        raise OSError("failure after native close; capsule cannot infer that ordering")

    endpoint._close = destructive_close
    try:
        first = capsule.retire(timeout_s=0.5)
        receipts.append(first)
        assert first.status == "unknown" and endpoint.closed is True
        assert first.queues[0].reader_closed is True
        assert first.queues[0].reader_close_ack is False
        assert "native_close_exception:OSError" in first.queues[0].destructive_errors
        endpoint._close = native_close
        second = capsule.retire(timeout_s=0.5)
        receipts.append(second)
        assert second.status == "unknown" and second.ever_uncertain
        assert second.queues[0].reader_close_ack is False
        assert calls == ["native_close"]  # No re-close of a reused numeric fd/handle.
    finally:
        endpoint._close = native_close
        _finish_actual(
            tmp_path,
            "injected-endpoint-destructive-failure",
            process,
            (incoming, outgoing),
            receipts,
            native_returned_fixture=(endpoint, marker),
        )


def test_consumed_native_process_finalizer_failure_is_permanently_unknown(tmp_path):
    process, incoming, outgoing, capsule = dead_fixture()
    finalizer = capsule._native_finalizer
    callback, calls = finalizer._callback, []
    marker = {"native_close_returned": False, "deliberate_post_close_exception": True}
    receipts = []

    def failing_callback(*args, **kwargs):
        calls.append("native_process_close")
        callback(*args, **kwargs)  # Fixture knows the actual native close succeeded.
        marker["native_close_returned"] = True
        raise OSError("finalizer failure after close; worker outcome remains unknown")

    finalizer._callback = failing_callback
    try:
        first = capsule.retire(timeout_s=0.5)
        receipts.append(first)
        assert first.status == "unknown" and first.process_destructive_unknown is True
        assert finalizer.still_active() is False and process._closed is False
        second = capsule.retire(timeout_s=0.5)
        receipts.append(second)
        assert second.status == "unknown" and second.process_destructive_unknown is True
        assert second.process_handle_closed is False
        assert calls == ["native_process_close"]
    finally:
        _finish_actual(
            tmp_path,
            "injected-process-finalizer-destructive-failure",
            process,
            (incoming, outgoing),
            receipts,
            native_process_fixture=marker,
        )


def _feeder_close_fault_host(report_path):
    """Contain one deliberately held daemon feeder; never call it retired."""
    process, incoming, outgoing, capsule = dead_fixture()
    reader = incoming._reader
    original_native_close = reader._close

    def injected_native_close_failure():
        original_native_close()
        raise OSError("TEST injected exception after actual native close")

    reader._close = injected_native_close_failure
    capsule.put_owned("incoming", "bounded original payload")
    incoming.close()  # Actual Queue._feed invokes the previously saved wrapper.
    evidence = capsule._endpoint_evidence[0][0]
    deadline = time.monotonic() + 3
    while not evidence.snapshot()[2] and time.monotonic() < deadline:
        time.sleep(0.01)
    first = capsule.retire(timeout_s=0.05)
    final = capsule.retire(timeout_s=0.05)
    alive = incoming._thread.is_alive()
    Path(report_path).write_text(
        json.dumps(
            {
                "classification": "CONTAINED_HELD",
                "scope": (
                    "TEST injected native-close exception in actual Queue feeder; "
                    "not real OS failure"
                ),
                "platform": sys.platform,
                "first": asdict(first),
                "final": asdict(final),
                "original_feeder_alive_at_observation": alive,
                "normal_model_cleanup_confirmed": False,
                "inner_process_handle_closed": process._closed,
                "implicit_queue_join_cancelled": incoming._joincancelled
                and outgoing._joincancelled,
                "no_sentinel_reinjection_or_buffer_clear_or_error_callback_replacement": True,
            },
            indent=2,
            allow_nan=False,
        ),
        encoding="utf-8",
    )
    assert first.status == final.status == "unknown" and alive
    assert final.queues[0].reader_close_ack is False
    assert not final.normal_model_cleanup_confirmed
    assert process._closed and incoming._joincancelled and outgoing._joincancelled
    # The host exits normally; its original daemon feeder was explicitly HELD,
    # not joined or classified as a capsule success. Raw feeder stderr is kept.


def test_actual_feeder_native_close_failure_is_sticky_in_contained_host(tmp_path):
    report_path = tmp_path / "contained-feeder-close-fault.json"
    cleanup_path = tmp_path / "contained-host-native-cleanup.json"
    host = multiprocessing.get_context("spawn").Process(
        target=_feeder_close_fault_host, args=(str(report_path),)
    )
    forced = False
    host.start()
    try:
        host.join(timeout=12)
        assert not host.is_alive(), "Contained test-host exceeded bounded native deadline"
        assert host.exitcode == 0 and report_path.is_file()
        result = json.loads(report_path.read_text(encoding="utf-8"))
        assert result["classification"] == "CONTAINED_HELD"
        assert result["first"]["status"] == result["final"]["status"] == "unknown"
        assert result["final"]["queues"][0]["reader_closed"] is True
        assert result["final"]["queues"][0]["reader_close_ack"] is False
        assert result["original_feeder_alive_at_observation"] is True
        assert result["normal_model_cleanup_confirmed"] is False
        assert result["final"]["normal_model_cleanup_confirmed"] is False
        assert result["inner_process_handle_closed"] is True
        assert result["implicit_queue_join_cancelled"] is True
    finally:
        if host.is_alive():
            forced = True
            host.terminate()  # Owned TEST-HOST timeout cleanup only; never a pass.
            host.join(timeout=3)
        alive, exitcode = host.is_alive(), host.exitcode
        assert not alive
        host.close()
        cleanup_path.write_text(
            json.dumps(
                {
                    "host_native_exit": exitcode,
                    "host_not_alive": not alive,
                    "host_handle_closed": host._closed,
                    "forced_timeout_cleanup": forced,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        assert forced is False, "Forced test-host cleanup is failed/held verification"


@pytest.mark.parametrize(
    "fault", ["already_closed_endpoint", "existing_feeder", "already_closed_queue"]
)
def test_initial_close_or_feeder_preflight_rejected_before_wrapper_installation(fault):
    process, incoming, outgoing, _unused = dead_fixture(make_capsule=False)
    original_outgoing_reader_close = outgoing._reader.close
    try:
        if fault == "already_closed_endpoint":
            incoming._reader.close()
        elif fault == "already_closed_queue":
            incoming.close()
        else:
            incoming.put("existing original feeder")
        with pytest.raises(ValueError):
            OriginalProcessResources(process, incoming, outgoing)
        assert outgoing._reader.close == original_outgoing_reader_close
        assert getattr(outgoing._reader.close, "__self__", None) is outgoing._reader
    finally:
        _stop_owned(process, (incoming, outgoing))


def test_closed_external_handle_unknown_and_unstarted_preflight_rejected():
    process, incoming, outgoing, capsule = dead_fixture()
    process.close()
    try:
        result = capsule.retire(timeout_s=0.1)
        assert result.status == "unknown" and result.process_handle_closed is False
        assert not incoming._reader.closed
    finally:
        _stop_owned(process, (incoming, outgoing))
    context = multiprocessing.get_context("spawn")
    process, incoming, outgoing = context.Process(), context.Queue(), context.Queue()
    with pytest.raises(ValueError, match="Process.start"):
        OriginalProcessResources(process, incoming, outgoing)
    process.close()
    _close_unused(incoming)
    _close_unused(outgoing)


def test_live_original_child_stays_unknown_without_join_close_or_terminate():
    context = multiprocessing.get_context("spawn")
    incoming, outgoing = context.Queue(), context.Queue()
    ready, finish = context.Event(), context.Event()
    process = context.Process(target=_wait_then_exit, args=(ready, finish, 3))
    process.start()
    assert ready.wait(timeout=5)
    capsule = OriginalProcessResources(process, incoming, outgoing)
    try:
        result = capsule.retire(timeout_s=0)
        assert result.status == "unknown" and process.is_alive()
        assert not incoming._reader.closed and not outgoing._writer.closed
        finish.set()
        process.join(timeout=5)
        result = capsule.retire(timeout_s=0.5)
        assert result.status == "physical_retired" and result.ever_uncertain
    finally:
        finish.set()
        _stop_owned(process, (incoming, outgoing))


@pytest.mark.parametrize("fault", ["queue", "reader", "feeder"])
def test_attribute_substitution_cannot_ack_or_close_substitute_and_original_retry(
    fault,
):
    process, incoming, outgoing, capsule = dead_fixture()
    extra = multiprocessing.get_context("spawn").Queue()
    original_reader = incoming._reader
    try:
        if fault == "queue":
            capsule.incoming = extra
        elif fault == "reader":
            incoming._reader = extra._reader
        else:
            incoming._thread = threading.Thread(target=lambda: None)
        result = capsule.retire(timeout_s=0.1)
        assert result.status == "unknown" and not extra._reader.closed
        assert not capsule.owns(process, incoming, outgoing)
        assert not original_reader.closed and capsule._queues[0] is incoming
        capsule.incoming, incoming._reader, incoming._thread = (
            incoming,
            original_reader,
            None,
        )
        result = capsule.retire(timeout_s=0.5)
        assert result.status == "physical_retired" and result.ever_uncertain
        assert capsule.owns(process, incoming, outgoing)
        assert not capsule.owns(process, extra, outgoing)
    finally:
        capsule.incoming, incoming._reader, incoming._thread = (
            incoming,
            original_reader,
            None,
        )
        _stop_owned(process, (incoming, outgoing))
        _close_unused(extra)


def test_external_lazy_feeder_is_not_silently_adopted():
    process, incoming, outgoing, capsule = dead_fixture()
    try:
        incoming.put("external")
        result = capsule.retire(timeout_s=0.1)
        assert result.status == "unknown" and capsule._feeders[0] is None
        with pytest.raises(RuntimeError, match="uncaptured"):
            capsule.put_owned("incoming", "never-written")
        incoming.get(timeout=1)
    finally:
        _stop_owned(process, (incoming, outgoing))


def test_partial_original_put_failure_captures_feeder_without_replaying_payload():
    process, incoming, outgoing, _unused = dead_fixture(make_capsule=False)
    original_put, calls = incoming.put, []

    def partial_put(payload, **kwargs):
        calls.append(payload)
        original_put(payload, **kwargs)
        raise OSError("simulated failure after original put created its feeder")

    incoming.put = partial_put
    capsule = OriginalProcessResources(process, incoming, outgoing)
    try:
        payload = {"one": "original request"}
        with pytest.raises(OSError):
            capsule.put_owned("incoming", payload)
        assert calls == [payload] and calls[0] is payload
        assert capsule._feeders[0] is incoming._thread
        assert incoming.get(timeout=1) == payload
        first = capsule.retire(timeout_s=0.5)
        assert first.status == "unknown" and "put_failed:OSError" in first.errors
        second = capsule.retire(timeout_s=0.5)
        assert second.status == "physical_retired" and second.ever_uncertain
        assert calls == [payload]
    finally:
        _stop_owned(process, (incoming, outgoing))


def test_simulated_original_feeder_alive_or_join_failure_never_becomes_ack():
    process, incoming, outgoing, capsule = dead_fixture()
    capsule.put_owned("incoming", "payload")
    assert incoming.get(timeout=1) == "payload"
    feeder = incoming._thread
    original_join, original_alive = feeder.join, feeder.is_alive
    feeder.join = lambda timeout: None
    feeder.is_alive = lambda: True
    try:
        first = capsule.retire(timeout_s=0.01)
        assert first.status == "unknown" and first.queues[0].feeder == "unknown"
        assert capsule._feeders[0] is feeder
        feeder.join, feeder.is_alive = original_join, original_alive
        second = capsule.retire(timeout_s=0.5)
        assert second.status == "physical_retired" and second.ever_uncertain
    finally:
        feeder.join, feeder.is_alive = original_join, original_alive
        _stop_owned(process, (incoming, outgoing))


@pytest.mark.parametrize("timeout", [True, -1, 6, float("nan"), float("inf")])
def test_invalid_join_budget_is_rejected(timeout):
    process, incoming, outgoing, capsule = dead_fixture()
    try:
        with pytest.raises(ValueError):
            capsule.retire(timeout_s=timeout)
    finally:
        _stop_owned(process, (incoming, outgoing))


def test_foreign_thread_rejected_before_diagnostics_or_original_resources_mutate():
    process, incoming, outgoing, capsule = dead_fixture()
    calls, failures = [], []
    original_incoming_close, original_outgoing_close = incoming.close, outgoing.close

    def incoming_close():
        calls.append("incoming.close")
        return original_incoming_close()

    def outgoing_close():
        calls.append("outgoing.close")
        return original_outgoing_close()

    incoming.close, outgoing.close = incoming_close, outgoing_close
    before = (
        capsule._attempt,
        tuple(capsule._history),
        tuple(capsule._pending_errors),
        capsule._ever_uncertain,
        capsule._last,
        capsule._known_pid,
        capsule._death_ns,
        capsule._exitcode,
        capsule._process_closed,
        capsule._close_returned,
        tuple(capsule._feeders),
        capsule._native,
        capsule._endpoints,
    )

    def foreign_retire():
        try:
            capsule.retire(timeout_s=0.5)
        except BaseException as error:
            failures.append(error)

    thread = threading.Thread(target=foreign_retire)
    try:
        thread.start()
        thread.join(timeout=2)
        assert not thread.is_alive()
        assert len(failures) == 1 and isinstance(failures[0], RuntimeError)
        assert "original owner thread" in str(failures[0])
        assert calls == []
        assert (
            capsule._attempt,
            tuple(capsule._history),
            tuple(capsule._pending_errors),
            capsule._ever_uncertain,
            capsule._last,
            capsule._known_pid,
            capsule._death_ns,
            capsule._exitcode,
            capsule._process_closed,
            capsule._close_returned,
            tuple(capsule._feeders),
            capsule._native,
            capsule._endpoints,
        ) == before
        assert all(not endpoint.closed for pair in capsule._endpoints for endpoint in pair)
        assert process._closed is False and process._popen is capsule._native
        assert capsule.owns(process, incoming, outgoing)
        result = capsule.retire(timeout_s=0.5)
        assert result.status == "physical_retired" and result.attempt == 1
        assert result.ever_uncertain is False and result.history == ((1, ()),)
        assert calls == ["incoming.close", "outgoing.close"]
    finally:
        _stop_owned(process, (incoming, outgoing))


def _role_closed_exchange(incoming, outgoing):
    incoming._writer.close()
    outgoing._reader.close()
    for index in range(3):
        payload = incoming.get(timeout=5)
        outgoing.put((index, payload))


def test_real_role_closed_parent_ends_preserve_complete_fifo_and_allow_eof(tmp_path):
    context = multiprocessing.get_context("spawn")
    incoming, outgoing = context.Queue(8), context.Queue(8)
    process = context.Process(target=_role_closed_exchange, args=(incoming, outgoing))
    process.start()
    capsule = OriginalProcessResources(process, incoming, outgoing)
    receipts = []
    try:
        assert capsule.close_unused_parent_ends() == (True, True)
        assert incoming._reader.closed and outgoing._writer.closed
        assert not incoming._writer.closed and not outgoing._reader.closed
        selected = capsule._endpoint_evidence[0][0], capsule._endpoint_evidence[1][1]
        assert all(evidence.snapshot()[1] for evidence in selected)
        assert [len(evidence.call_threads) for evidence in selected] == [1, 1]
        assert capsule.close_unused_parent_ends() == (True, True)
        assert [len(evidence.call_threads) for evidence in selected] == [1, 1]
        payloads = [{"native": "one"}, [2, None], "three"]
        for payload in payloads:
            capsule.put_owned("incoming", payload)
        assert [outgoing.get(timeout=5) for _ in payloads] == list(enumerate(payloads))
        process.join(timeout=5)
        assert not process.is_alive() and process.exitcode == 0
        poll_observation = "ready"
        if sys.platform == "win32":
            try:
                assert outgoing._reader.poll(1) is True
            except BrokenPipeError as error:
                # Windows message-mode PeekNamedPipe may expose EOF directly.
                assert error.winerror == 109
                poll_observation = "BrokenPipeError109"
        else:
            assert outgoing._reader.poll(1) is True
        with pytest.raises((EOFError, OSError)) as eof:
            outgoing.get_nowait()
        (tmp_path / "role-eof-observation.json").write_text(
            json.dumps(
                {
                    "platform": sys.platform,
                    "poll": poll_observation,
                    "get_exception": type(eof.value).__name__,
                    "get_winerror": getattr(eof.value, "winerror", None),
                }
            ),
            encoding="utf-8",
        )
        receipt = capsule.retire(timeout_s=1)
        receipts.append(receipt)
        assert receipt.status == "physical_retired"
        assert all(row.reader_close_ack and row.writer_close_ack for row in receipt.queues)
    finally:
        _finish_actual(
            tmp_path, "role-closed-complete-fifo-eof", process, (incoming, outgoing), receipts
        )


def test_first_role_close_after_put_is_rejected_without_closing_active_endpoints():
    process, incoming, outgoing, capsule = dead_fixture()
    try:
        capsule.put_owned("incoming", "before-role-close")
        with pytest.raises(RuntimeError, match="before the first local put"):
            capsule.close_unused_parent_ends()
        assert not incoming._reader.closed and not outgoing._writer.closed
        assert not incoming._writer.closed and not outgoing._reader.closed
    finally:
        _stop_owned(process, (incoming, outgoing))


def test_role_close_native_failure_stays_unknown_and_never_closes_active_ends(tmp_path):
    process, incoming, outgoing, capsule = dead_fixture()
    endpoint = outgoing._writer
    native_close, calls = endpoint._close, []
    marker = {"native_close_returned": False, "deliberate_post_close_exception": True}
    receipts = []

    def failure_after_native_close():
        native_close()
        marker["native_close_returned"] = True
        calls.append("original unused writer native close")
        raise OSError("injected post-close role failure")

    endpoint._close = failure_after_native_close
    try:
        with pytest.raises(OSError):
            capsule.close_unused_parent_ends()
        assert incoming._reader.closed and outgoing._writer.closed
        assert not incoming._writer.closed and not outgoing._reader.closed
        with pytest.raises(RuntimeError, match="remains unconfirmed"):
            capsule.close_unused_parent_ends()
        assert calls == ["original unused writer native close"]
        first = capsule.retire(timeout_s=0.5)
        second = capsule.retire(timeout_s=0.5)
        receipts.extend((first, second))
        assert first.status == second.status == "unknown"
        assert second.queues[1].writer_close_ack is False
    finally:
        endpoint._close = native_close
        _finish_actual(
            tmp_path,
            "role-native-close-unknown",
            process,
            (incoming, outgoing),
            receipts,
            native_returned_fixture=(endpoint, marker),
        )


def test_original_thread_object_fence_rejects_a_reused_numeric_ident(monkeypatch):
    process, incoming, outgoing, capsule = dead_fixture()
    owner = threading.current_thread()
    assert capsule._owner_thread is owner
    replacement = SimpleNamespace(ident=owner.ident)
    before = (
        capsule._attempt,
        tuple(capsule._history),
        tuple(capsule._pending_errors),
        capsule._ever_uncertain,
        capsule._last,
    )
    try:
        # Simulate numeric ID reuse while keeping a different Thread OBJECT.
        with monkeypatch.context() as patch:
            patch.setattr(threading, "current_thread", lambda: replacement)
            assert threading.current_thread().ident == owner.ident
            for operation in (
                lambda: capsule.retire(timeout_s=0),
                capsule.close_unused_parent_ends,
                lambda: capsule.put_owned("incoming", "never-sent"),
            ):
                with pytest.raises(RuntimeError, match="original owner thread"):
                    operation()
        assert (
            capsule._attempt,
            tuple(capsule._history),
            tuple(capsule._pending_errors),
            capsule._ever_uncertain,
            capsule._last,
        ) == before
        assert all(not endpoint.closed for pair in capsule._endpoints for endpoint in pair)
        assert capsule.retire(timeout_s=0.5).status == "physical_retired"
    finally:
        _stop_owned(process, (incoming, outgoing))
