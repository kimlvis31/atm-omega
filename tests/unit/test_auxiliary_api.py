"""Owner-only auxiliary routes validate envelopes and preserve attempt fences."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.runtime.service import CoreService


class Jobs:
    def __init__(self):
        self.calls = []
        self.row = {"id": "public-job", "execution_id": "a" * 32, "state": "unknown"}

    def snapshot(self):
        return {"schema_version": 1, "jobs": [deepcopy(self.row)]}

    def status(self, identity):
        self.calls.append(("status", identity))
        return deepcopy(self.row)

    def submit(self, identity, kind, parameters):
        self.calls.append(("submit", identity, kind, parameters))
        return deepcopy(self.row)

    def cancel(self, identity, *, expected_execution_id):
        self.calls.append(("cancel", identity, expected_execution_id))
        return deepcopy(self.row)

    def resume(self, identity, *, expected_execution_id):
        self.calls.append(("resume", identity, expected_execution_id))
        return deepcopy(self.row)

    def close(self):
        return {"closed": True, "durable": True, "worker_alive": False}


@pytest.fixture
def service(tmp_path):
    core = CoreService(tmp_path, auxiliary_service=Jobs())
    yield core
    core.close()


def test_auxiliary_list_reports_real_capability_and_core_epoch(service):
    result = service.request("GET", "/api/auxiliary-jobs", service.token)
    assert result["jobs"][0]["id"] == "public-job"
    assert result["core_epoch"] == service.state()["core"]["epoch"]
    assert service.state()["core"]["capabilities"]["auxiliary_jobs"]
    service.request("GET", "/api/auxiliary-jobs/public-job", service.token)
    assert service.auxiliary_service.calls == [("status", "public-job")]


def test_valid_submit_and_cancel_resume_pass_the_exact_execution_fence(service):
    payload = {
        "id": "public-job",
        "kind": "current_oi",
        "parameters": {"instrument": "binance:usdm:ETHUSDT"},
    }
    service.request("POST", "/api/auxiliary-jobs", service.token, payload)
    for kind in ("cancel", "resume"):
        service.request(
            "POST",
            f"/api/auxiliary-jobs/public-job/{kind}",
            service.token,
            {"execution_id": "a" * 32},
        )
    assert service.auxiliary_service.calls == [
        ("submit", "public-job", payload["kind"], payload["parameters"]),
        ("cancel", "public-job", "a" * 32),
        ("resume", "public-job", "a" * 32),
    ]


@pytest.mark.parametrize(
    "method,path,body",
    [
        ("GET", "/api/auxiliary-jobs?unknown=1", None),
        ("POST", "/api/auxiliary-jobs", {"kind": "current_oi"}),
        ("POST", "/api/auxiliary-jobs/public-job/cancel", None),
        ("POST", "/api/auxiliary-jobs/public-job/resume", {"execution_id": None}),
        ("POST", "/api/auxiliary-jobs/public-job/cancel", {"execution_id": "a"}),
        (
            "POST",
            "/api/auxiliary-jobs/public-job/resume",
            {"execution_id": "a" * 32, "new_plan": {}},
        ),
        ("POST", "/api/auxiliary-jobs/public-job/delete", {"execution_id": "a" * 32}),
    ],
)
def test_invalid_envelopes_never_dispatch(service, method, path, body):
    with pytest.raises(ValidationError):
        service.request(method, path, service.token, body)
    assert service.auxiliary_service.calls == []


def test_mobile_device_cannot_read_or_mutate_auxiliary_jobs(service):
    pairing = service.control.create_pairing("phone")
    device = service.request("POST", "/api/pair", "", {"code": pairing["code"], "name": "phone"})
    for method in ("GET", "POST"):
        with pytest.raises(PermissionError):
            service.request(method, "/api/auxiliary-jobs", device["token"], {})


def test_auxiliary_shutdown_recording_failure_is_critical_and_releases_core_owner(tmp_path):
    core = CoreService(tmp_path)
    receipt = {"durable": False, "worker_alive": False}
    receipts = []

    def close_auxiliary():
        receipts.append(deepcopy(receipt))
        return deepcopy(receipt)

    resource = SimpleNamespace(close=close_auxiliary)
    core.auxiliary_service = resource
    try:
        with pytest.raises(StorageError, match="could not be recorded"):
            core.close()
        assert receipts == [{"durable": False, "worker_alive": False}]
        assert core.auxiliary_service is resource and core.instance.released is False
        assert core.notifications.stopped.is_set() and core.notifications.speech.closed
        with pytest.raises(RuntimeError, match="already owns this root"):
            CoreService(tmp_path)
        assert any(
            row["code"] == "AUXILIARY_RECORDING_UNAVAILABLE" and row["severity"] == "critical"
            for row in core.control.snapshot()["incidents"]
        )
        # A stopped worker alone is not a durable recording acknowledgement.
        # Keep the original owner until its same resource explicitly confirms.
        receipt["durable"] = True
        core.close()
        assert receipts == [
            {"durable": False, "worker_alive": False},
            {"durable": True, "worker_alive": False},
        ]
        assert core.instance.released is True
        recovered = CoreService(tmp_path)
        try:
            assert any(
                row["code"] == "AUXILIARY_RECORDING_UNAVAILABLE" and row["severity"] == "critical"
                for row in recovered.control.snapshot()["incidents"]
            )
        finally:
            recovered.close()
    finally:
        receipt["durable"] = True
        core.close()
