"""Only current-core owners can authorize v2 session operations."""

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.service import CoreService


class Backend:
    def __init__(self):
        self.calls = []

    def snapshot(self):
        return {"state": "ready", "accounts": []}

    def submit(self, *args):
        self.calls.append(args)
        return {"id": args[0], "state": "queued", "core_epoch": "old"}

    def status(self, identity):
        return {"id": identity, "state": "completed", "core_epoch": "historical"}

    def close(self):
        self.calls.append("close")


def test_current_epoch_required_and_historical_operation_origin_retained(tmp_path):
    backend = Backend()
    core = CoreService(tmp_path, margin_trading=backend)
    try:
        epoch = core.control.snapshot()["epoch"]
        body = {
            "id": "resume",
            "kind": "resume",
            "parameters": {"account_id": "a"},
            "core_epoch": "old",
        }
        with pytest.raises(ValidationError, match="another core"):
            core.request("POST", "/api/margin-trading", core.token, body)
        assert backend.calls == []
        result = core.request(
            "POST", "/api/margin-trading", core.token, {**body, "core_epoch": epoch}
        )
        assert result["state"] == "queued" and result["core_epoch"] == epoch
        assert result["operation_core_epoch"] == "old"
        cached = core.request("GET", "/api/margin-trading/resume", core.token)
        assert cached["operation_core_epoch"] == "historical" and cached["core_epoch"] == epoch
        assert core.state()["core"]["capabilities"]["margin_trading"]
        assert core.request("GET", "/api/margin-trading", core.token)["accounts"] == []
    finally:
        core.close()
    assert backend.calls[-1] == "close"


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("GET", "/api/margin-trading?x=1", None),
        ("GET", "/api/margin-trading/nested/id", None),
        ("POST", "/api/margin-trading", {"kind": "resume"}),
    ],
)
def test_malformed_margin_api_cannot_dispatch(tmp_path, method, path, body):
    backend = Backend()
    core = CoreService(tmp_path, margin_trading=backend)
    try:
        with pytest.raises(ValidationError):
            core.request(method, path, core.token, body)
        assert backend.calls == []
    finally:
        core.close()
