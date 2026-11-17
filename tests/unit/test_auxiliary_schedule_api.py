"""Owner API uses durable scheduler revisions; no public HTTP is started."""

from types import SimpleNamespace

import pytest

from omega.domain.errors import ProtocolError, ValidationError
from omega.runtime.auxiliary_schedule import AuxiliarySchedule
from omega.runtime.service import CoreService

INSTRUMENT = "binance:usdm:BTCUSDT"


@pytest.fixture
def service(tmp_path):
    scheduler = AuxiliarySchedule(
        tmp_path,
        destination_id="a" * 64,
        resource_factory=lambda _: pytest.fail("unexpected market IO"),
        clock_ms=lambda: 900000,
    )
    core = CoreService(tmp_path, auxiliary_schedule=scheduler)
    try:
        yield core
    finally:
        core.paper = None
        core.close()


def request(service, method="GET", path="", body=None):
    return service.request(method, "/api/auxiliary-schedule" + path, service.token, body)


def test_owner_configure_stop_response_shape_and_stale_revision_fence(service):
    original = request(service)
    assert original["manual_apply_required"] and original["strategy_demand"] == []
    body = {
        "expected_revision": 0,
        "subscriptions": [{"instrument": INSTRUMENT, "channels": ["trade_flow"]}],
    }
    configured = request(service, "POST", "/configure", body)
    assert configured["enabled"] and configured["core_epoch"] == original["core_epoch"]
    assert configured["revision"] == 1 and configured["strategy_demand"] == []
    with pytest.raises(ProtocolError):
        request(service, "POST", "/configure", body)
    stopped = request(service, "POST", "/stop", {"expected_generation": configured["generation"]})
    assert not stopped["enabled"] and stopped["manual_apply_required"]
    assert stopped["core_epoch"] == original["core_epoch"]
    assert service.state()["core"]["capabilities"]["auxiliary_schedule"]


def test_strategy_demands_union_and_unsupported_are_explicit(service):
    rows = {
        "s0": {"instrument": INSTRUMENT, "channels": ["trade_flow"]},
        "s1": {"instrument": INSTRUMENT, "channels": ["metric", "trade_flow"]},
    }
    service.paper = SimpleNamespace(feed_snapshot=lambda: {"analysis_inputs": rows})
    view = request(service)
    assert view["strategy_demand"] == [
        {"instrument": INSTRUMENT, "channels": ["metric", "trade_flow"]}
    ]
    result = request(
        service, "POST", "/configure", {"expected_revision": 0, "from_strategies": True}
    )
    assert result["subscriptions"] == view["strategy_demand"]
    rows["depth_strategy"] = {"instrument": INSTRUMENT, "channels": ["depth"]}
    view = request(service)
    assert view["unsupported_demand"] == [{"instance": "depth_strategy", **rows["depth_strategy"]}]
    with pytest.raises(ValidationError, match="unsupported"):
        request(service, "POST", "/configure", {"expected_revision": 1, "from_strategies": True})


@pytest.mark.parametrize(
    "path,body",
    [
        ("/configure", {"expected_revision": 0, "from_strategies": False}),
        ("/configure", {"expected_revision": 0, "subscriptions": [], "from_strategies": True}),
        ("/configure", {"expected_revision": 0, "subscriptions": [], "unrecognized": 1}),
        ("/stop", {}),
        ("/resume", {"instrument": INSTRUMENT, "channel": "trade_flow"}),
    ],
)
def test_invalid_mutation_never_changes_schedule(service, path, body):
    before = request(service)
    with pytest.raises(ValidationError):
        request(service, "POST", path, body)
    assert request(service) == before


def test_schedule_is_not_exposed_to_registered_mobile_device(service):
    result = service.request(
        "POST",
        "/api/commands",
        service.token,
        {
            "id": "pair",
            "kind": "issue_pairing",
            "payload": {"name": "phone", "permissions": ["read", "pause"]},
        },
    )
    paired = service.request(
        "POST", "/api/pair", "", {"code": result["result"]["code"], "name": "phone"}
    )
    for method, path, body in (
        ("GET", "", None),
        ("POST", "/configure", {"expected_revision": 0, "subscriptions": []}),
    ):
        with pytest.raises(PermissionError):
            service.request(method, "/api/auxiliary-schedule" + path, paired["token"], body)


def test_restart_retains_demand_but_requires_explicit_apply_and_queries_nothing(tmp_path):
    def factory(_):
        pytest.fail("unexpected market IO")

    first = AuxiliarySchedule(
        tmp_path, destination_id="a" * 64, resource_factory=factory, clock_ms=lambda: 900000
    )
    first.configure([{"instrument": INSTRUMENT, "channels": ["trade_flow"]}], expected_revision=0)
    first.close()
    second = AuxiliarySchedule(
        tmp_path, destination_id="a" * 64, resource_factory=factory, clock_ms=lambda: 900000
    )
    try:
        second.start()
        state = second.snapshot()
        assert not state["enabled"] and state["manual_apply_required"]
        assert state["subscriptions"] == [{"instrument": INSTRUMENT, "channels": ["trade_flow"]}]
    finally:
        second.close()
