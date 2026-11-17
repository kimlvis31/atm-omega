"""Depth control is owner-only, explicit and fenced by core and generation."""

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.service import CoreService


class Depth:
    def __init__(self):
        self.generation = 0
        self.calls = []

    def snapshot(self):
        return {"generation": self.generation, "running": bool(self.calls), "instruments": []}

    def start(self, configurations, *, resume=False):
        self.generation += 1
        self.calls.append(("start", configurations, resume))
        return self.snapshot()

    def request_stop(self):
        self.calls.append(("request_stop",))
        return self.snapshot()

    def close(self):
        self.calls.append(("close",))


def test_depth_start_retries_do_not_open_a_second_connection_and_stop_is_nonblocking(tmp_path):
    backend = Depth()
    core = CoreService(tmp_path, depth_collection=backend)
    try:
        snapshot = core.request("GET", "/api/depth", core.token)
        body = {
            "id": "start",
            "core_epoch": snapshot["core_epoch"],
            "expected_generation": 0,
            "kind": "start",
            "parameters": {
                "subscriptions": [
                    {
                        "instrument": "binance:usdm:BTCUSDT",
                        "price_precision": 2,
                        "quote_precision": 8,
                    }
                ],
                "resume": False,
            },
        }
        result = core.request("POST", "/api/depth", core.token, body)
        assert result["state"] == "requested" and result["generation"] == 1
        assert core.request("POST", "/api/depth", core.token, body) == result
        assert len(backend.calls) == 1
        for changed in ({**body, "id": "stale"}, {**body, "id": "old", "core_epoch": "old"}):
            with pytest.raises(ValidationError):
                core.request("POST", "/api/depth", core.token, changed)
        stop = {
            "id": "stop",
            "core_epoch": snapshot["core_epoch"],
            "expected_generation": 1,
            "kind": "stop",
            "parameters": {},
        }
        assert core.request("POST", "/api/depth", core.token, stop)["state"] == "requested"
        assert backend.calls[-1] == ("request_stop",)
    finally:
        core.close()


@pytest.mark.parametrize("path", ["/api/depth", "/api/margin-trading"])
def test_mobile_permissions_cannot_enable_data_workers_or_configure_financial_sessions(
    tmp_path, path
):
    backend = Depth()
    core = CoreService(tmp_path, depth_collection=backend, margin_trading=backend)
    try:
        issue = core.request(
            "POST",
            "/api/commands",
            core.token,
            {
                "id": "pair",
                "kind": "issue_pairing",
                "payload": {"name": "phone", "permissions": ["read", "pause"]},
            },
        )
        paired = core.request(
            "POST", "/api/pair", "", {"code": issue["result"]["code"], "name": "phone"}
        )
        for method in ("GET", "POST"):
            with pytest.raises(PermissionError):
                core.request(method, path, paired["token"], {})
        assert backend.calls == []
    finally:
        core.close()
