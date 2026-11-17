"""Worker/entry-control/source/retirement boundaries; no broker or network."""

import threading
import time
from copy import deepcopy
from decimal import Decimal

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.margin_trading import MarginTradingService


class Session:
    @staticmethod
    def prepare_configuration(*args, **kwargs):
        pass

    def __init__(self, root, account, strategies, start_ms, **options):
        self.account_id = account["account_id"]
        self.paused, self.next, self.calls, self.closed = True, start_ms, [], False

    def snapshot(self):
        return {
            "account_id": self.account_id,
            "state": "ready",
            "paused": self.paused,
            "next_open_ms": self.next,
            "instruments": ["binance:usdm:BTCUSDT"],
            "pending_frame": False,
        }

    def resume(self, identity):
        self.paused = False

    def pause(self, identity):
        self.paused = True

    def reconcile(self):
        pass

    def run_once(self, candles, **parameters):
        self.calls.append((candles, deepcopy(parameters)))
        self.next += 60000

    def close(self):
        self.closed = True


class Source:
    def __init__(self, rows=()):
        self.rows = rows
        self.calls = []
        self.closed = False

    def read(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.rows

    def close(self):
        self.closed = True


def test_control_snapshot_preserves_source_metadata_without_copying_old_financial_flags(tmp_path):
    runtime = MarginTradingService(tmp_path, None, lambda: None, core_epoch="metadata")
    session = Session(tmp_path, {"account_id": "demo"}, [], 60000)
    runtime.sessions["demo"] = session
    runtime.data = {"accounts": {"demo": {"feed_enabled": True}}}
    runtime.views["demo"] = {
        "feed_state": "running",
        "feed_reason": None,
        "paused": False,
        "manual_protection_required": True,
    }
    runtime._refresh("demo")
    view = runtime.views["demo"]
    assert view["feed_state"] == "running" and view["feed_reason"] is None
    assert view["paused"] is True and "manual_protection_required" not in view
    runtime._refresh("demo", feed_state="waiting", feed_reason="missing_candles")
    runtime._refresh("demo")
    assert runtime.views["demo"]["feed_state"] == "waiting"
    assert runtime.views["demo"]["feed_reason"] == "missing_candles"


def test_funding_option_is_forwarded_without_turning_on_external_inference(tmp_path):
    runtime = MarginTradingService(tmp_path, None, lambda: None, core_epoch="funding")
    parameters = {"strategies": [], "funding": {"explicit": "scenario"}}
    assert runtime._session_options(parameters) == {"funding": parameters["funding"]}
    assert runtime._feature_options(parameters) == {}
    assert runtime._session_options({"strategies": []}) == {}
    assert runtime._waiting_external("demo", parameters, ()) is False


def await_value(read, check):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        value = read()
        if check(value):
            return value
        time.sleep(0.01)
    raise AssertionError(value)


def service(tmp_path, **options):
    result = MarginTradingService(
        tmp_path,
        None,
        options.pop("source_factory", lambda: Source()),
        core_epoch="epoch",
        clock_ms=lambda: 180000,
        session_factory=Session,
        **options,
    )
    result.start()
    await_value(result.snapshot, lambda row: row["state"] == "ready")
    return result


def configure(result):
    parameters = {
        "account": {"account_id": "demo"},
        "strategies": [],
        "start_ms": 60000,
        "brackets": [],
    }
    result.submit("configure", "configure", parameters)
    return await_value(lambda: result.status("configure"), lambda row: row["state"] != "queued")


def test_explicit_strategy_resume_uses_original_operation_id_and_keeps_account_paused(
    tmp_path, monkeypatch
):
    calls = []

    def resume_strategy(session, identity, strategy_id):
        calls.append((identity, strategy_id))
        session.paused = True

    monkeypatch.setattr(Session, "resume_strategy", resume_strategy, raising=False)
    result = service(tmp_path)
    try:
        assert configure(result)["state"] == "completed"
        parameters = {"account_id": "demo", "strategy_id": "selected"}
        result.submit("clear-local", "resume_strategy", parameters)
        receipt = await_value(
            lambda: result.status("clear-local"), lambda row: row["state"] == "completed"
        )
        assert calls == [("clear-local", "selected")]
        assert receipt["result"] == {
            "account_id": "demo",
            "strategy_id": "selected",
            "account_paused": True,
        }
        assert result.submit("clear-local", "resume_strategy", parameters) == receipt
        assert calls == [("clear-local", "selected")]
        assert result.snapshot()["accounts"][0]["paused"]
        assert result.snapshot()["capabilities"]["module_local_resume"] is True
        assert not result.data["accounts"]["demo"]["feed_enabled"]
        with pytest.raises(ValidationError, match="conflicts"):
            result.submit("clear-local", "resume_strategy", {**parameters, "strategy_id": "other"})
        result.submit("bad-target", "resume_strategy", {**parameters, "strategy_id": "../other"})
        rejected = await_value(
            lambda: result.status("bad-target"), lambda row: row["state"] == "failed"
        )
        assert rejected["error"] == "ValidationError" and len(calls) == 1
    finally:
        result.close()


def test_configure_does_not_fetch_and_restart_requires_manual_resume(tmp_path):
    source = Source()
    result = service(tmp_path, source_factory=lambda: source)
    try:
        assert configure(result)["state"] == "completed"
        assert result.snapshot()["accounts"][0]["paused"]
        assert source.calls == []
        assert (
            result.submit(
                "configure",
                "configure",
                {
                    "account": {"account_id": "demo"},
                    "strategies": [],
                    "start_ms": 60000,
                    "brackets": [],
                },
            )["state"]
            == "completed"
        )
        with pytest.raises(ValidationError, match="conflicts"):
            result.submit("configure", "pause", {"account_id": "demo"})
    finally:
        result.close()
    restored = service(tmp_path, source_factory=lambda: source)
    try:
        assert restored.snapshot()["accounts"][0]["paused"]
        assert source.calls == []
        assert restored.status("configure")["core_epoch"] == "epoch"
    finally:
        restored.close()


def test_incomplete_common_minute_never_reaches_financial_session(tmp_path):
    source = Source()
    result = service(tmp_path, source_factory=lambda: source)
    try:
        configure(result)
        result.submit("resume", "resume", {"account_id": "demo"})
        await_value(lambda: result.status("resume"), lambda row: row["state"] == "completed")
        await_value(result.snapshot, lambda row: row["accounts"][0].get("feed_state") == "waiting")
        assert result.sessions["demo"].calls == []
    finally:
        result.close()
    assert source.closed and result.sessions["demo"].closed


def test_source_fetch_does_not_hold_core_mutation_fence_and_close_fences_late_result(tmp_path):
    entered, release = threading.Event(), threading.Event()

    class Slow(Source):
        def read(self, *args, **kwargs):
            entered.set()
            assert release.wait(5)
            return ()

    guard = threading.RLock()
    source = Slow()
    result = service(tmp_path, source_factory=lambda: source, mutation_lock=guard)
    configure(result)
    result.submit("resume", "resume", {"account_id": "demo"})
    assert entered.wait(5)
    assert guard.acquire(timeout=0.5)
    guard.release()
    # Retirement can become visible before the blocked source returns.
    with result.condition:
        result.closed = True
        result.condition.notify_all()
    release.set()
    result.close()
    assert not result.snapshot()["worker_alive"]
    assert result.sessions["demo"].calls == [] and source.closed


def test_core_close_can_join_worker_waiting_for_same_mutation_fence(tmp_path):
    entered, release = threading.Event(), threading.Event()
    candle = Candle(
        Instrument("binance", Market.USDM, "BTCUSDT"),
        60000,
        120000,
        *(Decimal(v) for v in (100, 101, 99, 100, 10)),
    )

    class Slow(Source):
        def read(self, *args, **kwargs):
            entered.set()
            assert release.wait(5)
            return (candle,)

    guard = threading.RLock()
    source = Slow()
    result = service(tmp_path, source_factory=lambda: source, mutation_lock=guard)
    configure(result)
    result.submit("resume", "resume", {"account_id": "demo"})
    assert entered.wait(5)
    with guard:
        release.set()
        started = time.monotonic()
        result.close()
        assert time.monotonic() - started < 2
        assert not result.snapshot()["worker_alive"]
    assert result.sessions["demo"].calls == []


def test_invalid_preflight_does_not_reserve_account_slot(tmp_path):
    class Validate(Session):
        @staticmethod
        def prepare_configuration(account, *args, **kwargs):
            if account.get("balance") == "bad":
                raise ValidationError("bad balance")

    result = MarginTradingService(
        tmp_path, None, lambda: Source(), core_epoch="epoch", session_factory=Validate
    )
    result.start()
    await_value(result.snapshot, lambda row: row["state"] == "ready")
    try:
        bad = {
            "account": {"account_id": "demo", "balance": "bad"},
            "strategies": [],
            "start_ms": 60000,
            "brackets": [],
        }
        result.submit("invalid", "configure", bad)
        assert (
            await_value(lambda: result.status("invalid"), lambda row: row["state"] != "queued")[
                "state"
            ]
            == "failed"
        )
        assert result.data["accounts"] == {} and result.snapshot()["accounts"] == []
        result.submit(
            "corrected", "configure", {**bad, "account": {"account_id": "demo", "balance": "1000"}}
        )
        assert (
            await_value(lambda: result.status("corrected"), lambda row: row["state"] != "queued")[
                "state"
            ]
            == "completed"
        )
        assert len(result.snapshot()["accounts"]) == 1
    finally:
        result.close()


def test_financial_recording_failure_alerts_once_and_needs_explicit_recovery(tmp_path):
    fault = [True]
    incidents = []
    candle = Candle(
        Instrument("binance", Market.USDM, "BTCUSDT"),
        60000,
        120000,
        *(Decimal(v) for v in (100, 101, 99, 100, 10)),
    )

    class Fault(Session):
        def run_once(self, candles, **parameters):
            if fault[0]:
                raise OSError("test recording unavailable")
            return super().run_once(candles, **parameters)

    result = MarginTradingService(
        tmp_path,
        None,
        lambda: Source((candle,)),
        core_epoch="epoch",
        clock_ms=lambda: 180000,
        session_factory=Fault,
        on_incident=lambda *args: incidents.append(args),
    )
    result.start()
    await_value(result.snapshot, lambda row: row["state"] == "ready")
    try:
        configure(result)
        result.submit("resume", "resume", {"account_id": "demo"})
        await_value(result.snapshot, lambda row: row["accounts"][0].get("manual_recovery_required"))
        assert incidents == [("margin_recording_unavailable", "demo")]
        assert "demo" in result.blocked_accounts
        fault[0] = False
        result.submit("recover", "reconcile", {"account_id": "demo"})
        await_value(lambda: result.status("recover"), lambda row: row["state"] == "completed")
        assert result.snapshot()["accounts"][0]["paused"]
        assert "demo" not in result.blocked_accounts
        assert len(incidents) == 1
    finally:
        result.close()


def test_unconfirmed_worker_join_is_an_error_not_successful_shutdown(tmp_path):
    class Unfinished:
        def join(self, timeout):
            pass

        def is_alive(self):
            return True

    result = MarginTradingService(tmp_path, None, lambda: Source(), core_epoch="epoch")
    result.thread = Unfinished()
    with pytest.raises(StorageError, match="not confirmed"):
        result.close()
    assert result.snapshot()["worker_alive"]
    assert result.snapshot()["error"] == "cleanup_unconfirmed"


def test_finished_worker_cannot_hide_source_cleanup_failure(tmp_path):
    import subprocess
    import sys
    from pathlib import Path

    # Permanent, unobservable legacy failure must retain its original owner.
    # Isolate that deliberate pending daemon; process exit is recovery, not a
    # successful close receipt. Preserve this accepted test ID and strengthen
    # its no-false-cleanup/no-financial-reentry assertions.
    script = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[2])
from test_margin_trading import Source, service, configure, await_value
from omega.domain.errors import StorageError
class FailingClose(Source):
    def close(self):
        raise OSError('cleanup failed')
result = service(Path(sys.argv[1]), source_factory=lambda: FailingClose())
configure(result)
result.submit('resume', 'resume', {'account_id': 'demo'})
await_value(result.snapshot, lambda row: row['accounts'][0].get('feed_state') == 'waiting')
for attempt in range(2):
    try:
        result.close()
    except StorageError as error:
        assert 'resource cleanup' in str(error)
    else:
        raise AssertionError('pending cleanup was promoted to success')
    assert result.snapshot()['worker_alive']
    assert result.snapshot()['error'] == 'cleanup_unconfirmed'
    assert result.sessions['demo'].closed
assert result.cleanup_status()['state'] == 'pending'
assert not result.sessions['demo'].calls
print('pending owner preserved; process recovery only')
"""
    observed = subprocess.run(
        [sys.executable, "-I", "-c", script, str(tmp_path), str(Path(__file__).parent)],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert observed.returncode == 0, observed.stderr
    assert observed.stdout.strip() == "pending owner preserved; process recovery only"


def test_restart_with_known_open_position_alerts_without_fetching_data(tmp_path):
    class Held(Session):
        def snapshot(self):
            return {
                **super().snapshot(),
                "account": {"account": {"positions": [{"quantity": "1"}]}},
            }

    incidents = []

    def forbidden():
        pytest.fail("restart requires manual feed resume")

    def open_service():
        result = MarginTradingService(
            tmp_path,
            None,
            forbidden,
            core_epoch="epoch",
            session_factory=Held,
            on_incident=lambda *args: incidents.append(args),
        )
        result.start()
        await_value(result.snapshot, lambda row: row["state"] == "ready")
        return result

    initial = open_service()
    configure(initial)
    initial.close()
    assert incidents == []
    restored = open_service()
    try:
        assert incidents == [("margin_open_positions_on_restart", "demo")]
        assert restored.snapshot()["accounts"][0]["manual_protection_required"]
        assert restored.snapshot()["accounts"][0]["paused"]
    finally:
        restored.close()
