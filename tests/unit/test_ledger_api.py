"""Owner-only observation does not acquire the financial mutation fence."""

import threading

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.service import CoreService


class Backend:
    def __init__(self):
        self.calls = []

    def read_ledger(self, account, **query):
        self.calls.append((account, query))
        return {
            "scope": "virtual_margin_v2_ledger",
            "state": "unavailable",
            "reason_code": "not_ready",
        }

    def close(self):
        pass


def test_owner_get_decode_once_and_epoch_scope(tmp_path):
    backend = Backend()
    core = CoreService(tmp_path, margin_trading=backend)
    try:
        result = core.request(
            "GET",
            "/api/margin-trading/accounts/a%3Ab/ledger?offset=1&limit=2&start_ms=0",
            core.token,
        )
        assert backend.calls == [("a:b", {"offset": 1, "limit": 2, "start_ms": 0})]
        assert result["scope"] == "virtual_margin_v2_ledger"
        assert result["core_epoch"] == core.control.epoch
    finally:
        core.close()


@pytest.mark.parametrize(
    "suffix",
    [
        "a/ledger?offset=-1",
        "a/ledger?offset=1.0",
        "a/ledger?offset=1&offset=2",
        "a/ledger?limit=1001",
        "a/ledger?snapshot=",
        "a/ledger?other=1",
        "a/ledger?start_ms=2&end_ms=1",
        "a/ledger?end_ms=253402300799001",
        "a%2Fb/ledger",
        "a%252Fb/ledger",
        "a%/ledger",
        "a%FF/ledger",
        "a/ledger?x=1&x=2&x=3&x=4&x=5&x=6&x=7",
    ],
)
def test_hostile_query_rejected_before_worker(tmp_path, suffix):
    backend = Backend()
    core = CoreService(tmp_path, margin_trading=backend)
    try:
        with pytest.raises(ValidationError):
            core.request("GET", "/api/margin-trading/accounts/" + suffix, core.token)
        assert backend.calls == []
    finally:
        core.close()


def test_device_cannot_read_financial_owner_history(tmp_path):
    backend = Backend()
    core = CoreService(tmp_path, margin_trading=backend)
    try:
        pairing = core.control.create_pairing("phone")
        device = core.control.pair(pairing["code"])
        with pytest.raises(PermissionError):
            core.request("GET", "/api/margin-trading/accounts/a/ledger", device["token"])
        assert backend.calls == []
    finally:
        core.close()


def test_slow_history_observation_does_not_block_global_pause(tmp_path):
    backend = Backend()
    entered, release = threading.Event(), threading.Event()

    def read(_account, **_query):
        entered.set()
        assert release.wait(2)
        return {"scope": "virtual_margin_v2_ledger", "state": "unavailable"}

    backend.read_ledger = read
    core = CoreService(tmp_path, margin_trading=backend)
    errors = []

    def observe():
        try:
            core.request("GET", "/api/margin-trading/accounts/a/ledger", core.token)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=observe)
    try:
        thread.start()
        assert entered.wait(2)
        reply = core.request(
            "POST", "/api/commands", core.token, {"id": "p", "kind": "pause", "payload": {}}
        )
        assert reply["status"] == "completed"
    finally:
        release.set()
        thread.join(3)
        core.close()
    assert not thread.is_alive() and errors == []
