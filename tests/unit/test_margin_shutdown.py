"""Real-file Session inspection on the owner; candle source is synthetic, not PG."""

import threading
import time
from decimal import Decimal

import pytest

from omega.application.margin_execution import FrozenPendingOrder
from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration
from omega.runtime.margin_trading import MarginTradingService

BTC = Instrument("binance", Market.USDM, "BTCUSDT")


def wait(read, check):
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline:
        value = read()
        if check(value):
            return value
        time.sleep(0.01)
    raise AssertionError(value)


def command(service, identity, kind, parameters):
    service.submit(identity, kind, parameters)
    return wait(
        lambda: service.status(identity), lambda row: row["state"] not in {"queued", "running"}
    )


def configuration(quantity="1", margin_mode="cross"):
    return {
        "account": {
            "account_id": "shutdown-test",
            "balance": "1000",
            "positions": [
                {
                    "instrument": BTC.key,
                    "owner_id": "target",
                    "margin_mode": margin_mode,
                    **({"isolated_collateral": "100"} if margin_mode == "isolated" else {}),
                    "leverage": 10,
                    "budget_margin": "100",
                }
            ],
        },
        "strategies": [
            {
                "id": "target",
                "instrument": BTC.key,
                "module": "target_quantity",
                "configuration": {"quantity": quantity},
                "analysis": {},
                "constraints": {"leverage": "10", "can_short": True},
            }
        ],
        "start_ms": 0,
        "brackets": [
            {
                "instrument": BTC.key,
                "observed_at_ms": 0,
                "available_at_ms": 0,
                "rows": [
                    {
                        "notional_floor": "0",
                        "notional_cap": "1000000",
                        "maintenance_rate": "0.01",
                        "maintenance_amount": "0",
                        "max_leverage": 20,
                    }
                ],
            }
        ],
    }


class Source:
    def __init__(self):
        self.calls, self.block, self.entered, self.release = (
            [],
            None,
            threading.Event(),
            threading.Event(),
        )

    def read(self, instruments, start, end, **kwargs):
        self.calls.append((instruments, start, end, kwargs))
        if self.block == start:
            self.entered.set()
            assert self.release.wait(5)
        return (Candle(BTC, start, end, *(Decimal(v) for v in (100, 101, 99, 100, 10))),)

    def close(self):
        pass


def start(root, *, source=None, factory=None, quantity="1", margin_mode="cross"):
    source = source or Source()
    clock = [60000]
    guard = threading.RLock()
    service = MarginTradingService(
        root,
        None,
        lambda: source,
        core_epoch="shutdown-core",
        clock_ms=lambda: clock[0],
        mutation_lock=guard,
        session_factory=factory,
    )
    service.start()
    wait(service.snapshot, lambda row: row["state"] == "ready")
    row = command(service, "configure", "configure", configuration(quantity, margin_mode))
    return service, source, clock, guard, row


def files(root):
    return {
        p.relative_to(root): p.read_bytes()
        for p in root.rglob("*")
        if p.is_file() and not p.name.endswith(".lock")
    }


def test_constructor_and_unavailable_inspection_have_no_io(tmp_path):
    service = MarginTradingService(
        tmp_path / "unused", None, lambda: pytest.fail("source opened"), core_epoch="core"
    )
    assert service.inspect_shutdown()["unknown"] == ["owner_unavailable"]
    assert not (tmp_path / "unused").exists()
    for timeout in (True, 0, -1, float("inf"), 31):
        with pytest.raises(ValidationError):
            service.inspect_shutdown(timeout)
    service.close()
    assert service.inspect_shutdown()["unknown"] == ["owner_closed"]


def test_routine_owner_inspections_do_not_write_fetch_or_consume_operations(tmp_path):
    service, source, _, _, row = start(tmp_path)
    try:
        assert row["state"] == "completed"
        before, operations = files(tmp_path), service.snapshot()["operations"]
        for _ in range(20):
            result = service.inspect_shutdown()
            assert result["owner"] == "margin_v2"
            assert result["ready"] and result["durable"] and result["supports_flatten"]
            assert (
                not result["positions"] and not result["pending_orders"] and not result["unknown"]
            )
        assert files(tmp_path) == before and source.calls == []
        assert service.snapshot()["operations"] == operations
    finally:
        service.close()


@pytest.mark.parametrize("quantity,side", [("1", "BUY"), ("-1", "SELL")])
def test_inspection_waits_for_inflight_fill_then_reads_finance_not_cached_gui(
    tmp_path, quantity, side
):
    service, source, clock, guard, _ = start(tmp_path, quantity=quantity)
    results, finished = [], threading.Event()
    try:
        assert (
            command(service, "resume", "resume", {"account_id": "shutdown-test"})["state"]
            == "completed"
        )
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 0)
        pending = service.inspect_shutdown()
        assert pending["ready"] and pending["durable"] and pending["supports_flatten"]
        assert pending["pending_orders"][0]["quantity"] == "1"
        assert pending["pending_orders"][0]["side"] == side
        assert pending["pending_orders"][0]["reducing"] is False
        source.block, clock[0] = 60000, 120000
        with service.condition:
            service.condition.notify_all()
        assert source.entered.wait(3)

        def inspect():
            results.append(service.inspect_shutdown(timeout=4))
            finished.set()

        observer = threading.Thread(target=inspect)
        observer.start()
        assert not finished.wait(0.05)
        assert guard.acquire(timeout=0.2)
        guard.release()
        source.release.set()
        observer.join(5)
        assert not observer.is_alive()
        actual = results[0]
        assert actual["ready"] and actual["durable"] and actual["supports_flatten"]
        assert actual["positions"][0]["quantity"] == quantity and actual["pending_orders"] == []
        with service.condition:
            service.views["shutdown-test"] = {
                "account_id": "shutdown-test",
                "state": "ready",
                "pending_orders": [],
                "account": {"account": {"positions": []}},
            }
        before, calls = files(tmp_path), len(source.calls)
        assert service.inspect_shutdown()["positions"] == actual["positions"]
        assert files(tmp_path) == before and len(source.calls) == calls
    finally:
        source.release.set()
        service.close()


def test_model_failure_is_a_note_when_financial_state_is_confirmed(tmp_path):
    service, _, _, _, _ = start(tmp_path)
    try:
        session = service.sessions["shutdown-test"]
        original = session.snapshot
        session.snapshot = lambda: (
            original() | {"state": "recovery", "graph_errors": {"target": "missing_input"}}
        )
        result = service.inspect_shutdown()
        assert result["ready"] and result["durable"] and result["unknown"] == []
        assert "account_model_unready:shutdown-test" in result["notes"]
    finally:
        service.close()


def test_continuous_inspection_does_not_starve_paused_pending_settlement(tmp_path):
    service, _, clock, _, _ = start(tmp_path)
    stop = threading.Event()
    errors, observed = [], []

    def read():
        try:
            while not stop.is_set():
                observed.append(service.inspect_shutdown(timeout=0.5))
        except BaseException as error:
            errors.append(error)

    readers = [threading.Thread(target=read) for _ in range(2)]
    try:
        command(service, "resume", "resume", {"account_id": "shutdown-test"})
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 0)
        command(service, "pause", "pause", {"account_id": "shutdown-test"})
        clock[0] = 120000
        for reader in readers:
            reader.start()
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 60000)
        assert service.snapshot()["accounts"][0]["paused"]
        final = service.inspect_shutdown()
        assert final["positions"][0]["quantity"] == "1" and not final["pending_orders"]
        assert observed and not errors
    finally:
        stop.set()
        for reader in readers:
            if reader.ident is not None:
                reader.join(2)
        service.close()


def test_financial_ack_unknown_and_quarantine_are_not_hidden_or_reconciled(tmp_path):
    lost = []

    class Factory:
        prepare_configuration = staticmethod(prepare_margin_configuration)

        def __call__(self, *args, **kwargs):
            session = MarginTradingSession(*args, **kwargs)
            append = session.execution._journal.append

            def lose_ack(record):
                result = append(record)
                if isinstance(record.payload, FrozenPendingOrder) and not lost:
                    lost.append(record.record_id)
                    raise OSError("synthetic intent ACK loss")
                return result

            session.execution._journal.append = lose_ack
            return session

    service, _, _, _, _ = start(tmp_path, factory=Factory())
    try:
        command(service, "resume", "resume", {"account_id": "shutdown-test"})
        wait(service.snapshot, lambda row: row["accounts"][0].get("manual_recovery_required"))
        before = files(tmp_path)
        result = service.inspect_shutdown()
        assert not result["ready"] and not result["durable"] and not result["supports_flatten"]
        assert "account_quarantined:shutdown-test" in result["unknown"]
        assert "account_financial_unknown:shutdown-test" in result["unknown"]
        assert files(tmp_path) == before
        # A failed control receipt remains unknown even after current finance is
        # explicitly reconciled. It is history, not permanent financial doubt.
        assert (
            command(service, "blocked-resume", "resume", {"account_id": "shutdown-test"})["state"]
            == "unknown"
        )
        assert (
            command(service, "reconcile", "reconcile", {"account_id": "shutdown-test"})["state"]
            == "completed"
        )
        recovered = service.inspect_shutdown()
        assert recovered["ready"] and recovered["durable"] and recovered["unknown"] == []
        assert "operation_outcome_unknown:blocked-resume" in recovered["notes"]
        assert service.status("blocked-resume")["state"] == "unknown"
        assert recovered["pending_orders"] and recovered["supports_flatten"]
    finally:
        service.close()


def test_creating_account_missing_session_and_unknown_operation_remain_visible(tmp_path):
    class Factory:
        prepare_configuration = staticmethod(prepare_margin_configuration)

        def __call__(self, *args, **kwargs):
            raise RuntimeError("synthetic creation interrupted")

    service, _, _, _, row = start(tmp_path, factory=Factory())
    try:
        assert row["state"] == "unknown"
        result = service.inspect_shutdown()
        assert not result["ready"] and not result["durable"]
        assert {
            "account_creating:shutdown-test",
            "account_session_unavailable:shutdown-test",
            "operation_unknown:configure",
        } <= set(result["unknown"])
    finally:
        service.close()


def slow_inspection(service):
    entered, release = threading.Event(), threading.Event()
    execution = service.sessions["shutdown-test"].execution
    original = execution.snapshot

    def slow():
        entered.set()
        assert release.wait(5)
        return original()

    execution.snapshot = slow
    return entered, release


def test_observation_queue_is_bounded_and_timeout_never_claims_flat(tmp_path):
    service, _, _, _, _ = start(tmp_path)
    entered, release = slow_inspection(service)
    results, observers = [], []
    try:
        for _ in range(8):
            thread = threading.Thread(
                target=lambda: results.append(service.inspect_shutdown(timeout=4))
            )
            thread.start()
            observers.append(thread)
        assert entered.wait(2)
        wait(lambda: len(service._shutdown_waiters), lambda count: count == 8)
        assert service.inspect_shutdown()["unknown"] == ["inspection_queue_full"]
        release.set()
        for thread in observers:
            thread.join(5)
        assert len(results) == 8 and all(row["ready"] for row in results)
    finally:
        release.set()
        for thread in observers:
            thread.join(5)
        service.close()


def test_timeout_and_close_wake_inflight_observation_without_caller_mutation_lock(tmp_path):
    service, _, _, _, _ = start(tmp_path)
    entered, release = slow_inspection(service)
    try:
        result = service.inspect_shutdown(timeout=0.05)
        assert entered.is_set() and result["unknown"] == ["inspection_timeout"]
        results = []
        observer = threading.Thread(
            target=lambda: results.append(service.inspect_shutdown(timeout=4))
        )
        observer.start()
        wait(lambda: len(service._shutdown_waiters), lambda count: count == 1)
        closer = threading.Thread(target=service.close)
        closer.start()
        observer.join(1)
        assert not observer.is_alive() and results[0]["unknown"] == ["owner_closed"]
        assert closer.is_alive()
        release.set()
        closer.join(5)
        assert not closer.is_alive()
    finally:
        release.set()
        service.close()


def test_queued_owner_mutation_is_unknown_until_it_finishes(tmp_path):
    service, _, _, _, _ = start(tmp_path)
    entered, release = slow_inspection(service)
    results = []
    observer = threading.Thread(target=lambda: results.append(service.inspect_shutdown(timeout=4)))
    try:
        observer.start()
        assert entered.wait(2)
        service.submit("later-pause", "pause", {"account_id": "shutdown-test"})
        release.set()
        observer.join(5)
        assert not observer.is_alive()
        assert "operation_pending:later-pause" in results[0]["unknown"]
        assert not results[0]["ready"] and not results[0]["durable"]
        wait(lambda: service.status("later-pause"), lambda row: row["state"] == "completed")
        assert service.inspect_shutdown()["ready"]
    finally:
        release.set()
        observer.join(5)
        service.close()


def test_cleanup_uncertainty_is_reported_after_close(tmp_path):
    class Factory:
        prepare_configuration = staticmethod(prepare_margin_configuration)

        def __call__(self, *args, **kwargs):
            session = MarginTradingSession(*args, **kwargs)
            original = session.close

            def fail():
                original()
                raise StorageError("synthetic cleanup failure")

            session.close = fail
            return session

    service, _, _, _, _ = start(tmp_path, factory=Factory())
    with pytest.raises(StorageError, match="cleanup"):
        service.close()
    assert service.inspect_shutdown()["unknown"] == ["owner_cleanup_unconfirmed"]


@pytest.mark.parametrize("quantity", ["1", "-1"])
@pytest.mark.parametrize("margin_mode", ["cross", "isolated"])
def test_owner_drains_on_next_eligible_minute_and_cancel_keeps_reduction(
    tmp_path, quantity, margin_mode
):
    service, _, clock, _, configured = start(tmp_path, quantity=quantity, margin_mode=margin_mode)
    try:
        assert configured["state"] == "completed"
        assert (
            command(service, "resume", "resume", {"account_id": "shutdown-test"})["state"]
            == "completed"
        )
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 0)
        clock[0] = 120100
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 60000)
        assert service.inspect_shutdown()["positions"][0]["quantity"] == quantity
        assert service.shutdown_drain("exit-request", 120100)["accepted"]
        assert service.inspect_shutdown()["pending_orders"][0]["reducing"]
        assert service.shutdown_cancel("exit-request")["pending_preserved"]
        original = service.inspect_shutdown()["pending_orders"][0]["order_id"]
        clock[0] = 180000
        wait(service.snapshot, lambda row: row["accounts"][0]["shutdown"]["next_open_ms"] == 180000)
        current = service.inspect_shutdown()
        assert current["positions"][0]["quantity"] == quantity
        assert current["pending_orders"][0]["order_id"] == original
        clock[0] = 240000
        wait(
            service.inspect_shutdown, lambda row: not row["positions"] and not row["pending_orders"]
        )
        snapshot = service.snapshot()["accounts"][0]
        assert snapshot["paused"] and snapshot["shutdown"]["recovery_required"]
        assert (
            command(service, "wrong-resume", "resume", {"account_id": "shutdown-test"})["state"]
            != "completed"
        )
    finally:
        service.close()


def test_owner_shutdown_timeout_is_unknown_and_closed_owner_never_accepts(tmp_path):
    service, source, clock, guard, _ = start(tmp_path)
    try:
        with pytest.raises(ValidationError):
            service.shutdown_drain("exit", True)
        command(service, "resume", "resume", {"account_id": "shutdown-test"})
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 0)
        source.block, clock[0] = 60000, 120000
        assert source.entered.wait(3)
        with pytest.raises(StorageError, match="unconfirmed"):
            service.shutdown_drain("expired-exit", 120000, timeout=0.02)
        assert guard.acquire(timeout=0.2)
        guard.release()
        source.release.set()
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 60000)
        assert service.snapshot()["accounts"][0]["shutdown"]["operation_id"] is None
    finally:
        source.release.set()
        service.close()
    with pytest.raises(StorageError, match="unavailable"):
        service.shutdown_drain("late-exit", 120000)
