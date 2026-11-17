"""Independent real-file probes for account-wide shutdown admission and ACK loss."""

import threading

import pytest
from test_margin_shutdown import command, configuration, files, start, wait

from omega.domain.errors import TradingBlocked
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration


def test_all_accounts_preflight_before_any_financial_drain(tmp_path):
    blocked = threading.Event()

    class Session(MarginTradingSession):
        def shutdown_preflight(self, *args, **kwargs):
            result = super().shutdown_preflight(*args, **kwargs)
            if self.account_id == "zzz-secondary" and blocked.is_set():
                result.update(ready=False, reason="synthetic unsupported participant")
            return result

    class Factory:
        prepare_configuration = staticmethod(prepare_margin_configuration)

        def __call__(self, *args, **kwargs):
            return Session(*args, **kwargs)

    service, _, clock, _, configured = start(tmp_path, factory=Factory())
    try:
        assert configured["state"] == "completed"
        second = configuration()
        second["account"]["account_id"] = "zzz-secondary"
        assert command(service, "configure-second", "configure", second)["state"] == "completed"
        for identity in ("shutdown-test", "zzz-secondary"):
            assert (
                command(service, "resume-" + identity, "resume", {"account_id": identity})["state"]
                == "completed"
            )
        wait(
            service.snapshot,
            lambda row: all(account["cursor_ms"] == 0 for account in row["accounts"]),
        )
        clock[0] = 120000
        wait(
            service.snapshot,
            lambda row: all(account["cursor_ms"] == 60000 for account in row["accounts"]),
        )
        blocked.set()
        observed = service.inspect_shutdown()
        assert observed["ready"] and observed["durable"] and len(observed["positions"]) == 2
        assert not observed["supports_flatten"]
        original = files(tmp_path)
        with pytest.raises(TradingBlocked, match="preflight"):
            service.shutdown_drain("exit", 120000)
        assert files(tmp_path) == original
        assert not list(tmp_path.glob("state/margin-shutdown/*.json"))
    finally:
        service.close()


def test_registry_commit_ack_loss_preserves_ts_fence_and_quarantines_before_intent(
    tmp_path, monkeypatch
):
    service, _, clock, _, _ = start(tmp_path)
    try:
        command(service, "resume", "resume", {"account_id": "shutdown-test"})
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 0)
        clock[0] = 120000
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 60000)
        original = service._write
        fired = []

        def write(data):
            original(data)
            if data["accounts"]["shutdown-test"].get("shutdown_control") and not fired:
                fired.append(True)
                raise OSError("synthetic owner registry ACK loss")

        monkeypatch.setattr(service, "_write", write)
        with pytest.raises(OSError):
            service.shutdown_drain("exit", 120000)
        observed = service.inspect_shutdown()
        assert not observed["ready"] and not observed["durable"]
        assert "account_quarantined:shutdown-test" in observed["unknown"]
        assert not observed["pending_orders"] and observed["positions"]
        assert not list(tmp_path.glob("state/margin-shutdown/*.json"))
        assert (
            service.data["accounts"]["shutdown-test"]["shutdown_control"]["operation_id"] == "exit"
        )
        # Neither a volatile error nor a persisted fence authorizes normal TS fallback.
        clock[0] = 180000
        assert service.inspect_shutdown()["positions"]
    finally:
        service.close()
