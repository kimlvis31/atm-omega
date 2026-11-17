"""API authentication and provider boundaries without exchange/network access."""

from io import BytesIO

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.service import CoreService


@pytest.fixture
def service(tmp_path):
    instance = CoreService(tmp_path)
    yield instance
    instance.close()


def test_api_requires_authentication_and_reports_no_fake_market_data(service):
    with pytest.raises(PermissionError):
        service.request("GET", "/api/state", "wrong")
    state = service.request("GET", "/api/state", service.token)
    assert state["mode"] == "paper"
    assert state["core"]["paused"]
    assert not state["core"]["live_orders_enabled"]
    assert service.request("GET", "/api/candles", service.token)["candles"] == []
    with pytest.raises(ValidationError):
        service.request("GET", "/api/candles?limit=10001", service.token)


@pytest.mark.parametrize("scope", [True, None, "other_version", "minute_boundary_scenario_rate_v1"])
def test_scenario_funding_capability_needs_exact_native_scope_without_hiding_core(service, scope):
    from types import SimpleNamespace

    service.research = SimpleNamespace(
        health=lambda: {"state": "ready"},
        list_jobs=lambda: [],
        close=lambda: None,
        supports=lambda kind: kind == "simulate_margin",
        handler_scope=lambda *args: scope,
    )
    state = service.request("GET", "/api/state", service.token)
    capabilities = state["core"]["capabilities"]
    assert capabilities["margin_simulation"] is True
    assert capabilities["margin_funding"] is (scope == "minute_boundary_scenario_rate_v1")
    assert not state["core"]["live_orders_enabled"] and state["core"]["paused"]


def test_bad_optional_research_scope_does_not_remove_emergency_control_state(service):
    from types import SimpleNamespace

    def broken_scope(*args):
        raise RuntimeError("private source details")

    service.research = SimpleNamespace(
        health=lambda: {"state": "ready"},
        list_jobs=lambda: [],
        close=lambda: None,
        supports=lambda kind: kind == "simulate_margin",
        handler_scope=broken_scope,
    )
    state = service.request("GET", "/api/state", service.token)
    assert state["core"]["paused"] and state["core"]["capabilities"]["margin_funding"] is False
    assert state["core"]["capabilities"]["margin_funding_scope"] is None
    assert state["core"]["capabilities"]["margin_cost_policy"] is False
    assert state["core"]["capabilities"]["margin_cost_policy_scope"] is None


@pytest.mark.parametrize("scope", [True, None, "other_model", "fixed_adverse_full_fill_delay_v1"])
def test_cost_scope_requires_exact_native_registered_contract(service, scope):
    from types import SimpleNamespace

    service.research = SimpleNamespace(
        health=lambda: {"state": "ready"},
        list_jobs=lambda: [],
        close=lambda: None,
        supports=lambda kind: kind == "simulate_margin",
        handler_scope=lambda kind, feature: scope if feature == "cost_policy" else None,
    )
    value = service.state()["core"]["capabilities"]
    assert value["margin_cost_policy"] is (scope == "fixed_adverse_full_fill_delay_v1")
    assert value["margin_funding"] is False
    assert service.state(owner=False)["core"]["capabilities"]["margin_cost_policy"] is False


def test_research_status_failure_preserves_paper_state_and_is_owner_only(service):
    from types import SimpleNamespace

    def broken():
        raise AttributeError("private provider details")

    service.research = SimpleNamespace(
        health=lambda: {"state": "degraded", "quarantined": True},
        list_jobs=broken,
        close=lambda: None,
    )
    state = service.request("GET", "/api/state", service.token)
    assert state["core"]["paused"] and state["mode"] == "paper"
    assert state["jobs"] == []
    assert state["research"] == {
        "state": "degraded",
        "quarantined": True,
        "error": "RESEARCH_STATUS_UNAVAILABLE",
    }
    assert service.state(owner=False)["research"] == {}


def test_research_unknown_dispatch_fields_survive_bounded_owner_summary(service):
    from types import SimpleNamespace

    service.research = SimpleNamespace(
        health=lambda: {"state": "degraded", "quarantined": True},
        list_jobs=lambda: [
            {
                "id": "job",
                "state": "unknown",
                "dispatch_state": "blocked",
                "computation_state": "unknown",
            }
        ],
        close=lambda: None,
    )
    state = service.state()
    assert state["research"]["quarantined"] is True
    assert state["jobs"][0]["computation_state"] == "unknown"
    assert state["jobs"][0]["dispatch_state"] == "blocked"


def test_device_can_pause_but_cannot_read_owner_settings_or_submit_jobs(service):
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
    code = result["result"]["code"]
    paired = service.request("POST", "/api/pair", "", {"code": code, "name": "phone"})
    token = paired["token"]
    assert service.request("GET", "/api/state", token)["settings"] == {}
    for path in ("/api/jobs", "/api/modules", "/api/devices", "/api/candles"):
        with pytest.raises(PermissionError):
            service.request("GET", path, token)
    pause = {"id": "pause", "kind": "pause", "payload": {}}
    assert service.request("POST", "/api/commands", token, pause)["status"] == "completed"
    with pytest.raises(PermissionError):
        service.request("POST", "/api/commands", token, {**pause, "kind": "resume"})
    service.request(
        "POST",
        "/api/commands",
        service.token,
        {"id": "revoke", "kind": "revoke_device", "payload": {"device_id": paired["device_id"]}},
    )
    with pytest.raises(PermissionError):
        service.request("GET", "/api/state", token)


def test_pairing_invalid_attempts_are_limited(service):
    for _ in range(10):
        with pytest.raises(PermissionError):
            service.request("POST", "/api/pair", "", {"code": "bad"})
    with pytest.raises(PermissionError, match="limited"):
        service.request("POST", "/api/pair", "", {"code": "bad"})


def test_market_health_is_supplied_and_failures_do_not_break_state(tmp_path):
    class Market:
        def health(self):
            return {"status": "ready", "source": "database", "demo": False}

    provider = Market()
    service = CoreService(tmp_path, candles_provider=provider)
    try:
        assert service.state()["market"]["status"] == "ready"
        provider.health = lambda: (_ for _ in ()).throw(OSError("unavailable"))
        assert service.state()["market"] == {
            "status": "not_connected",
            "source": "database",
            "demo": False,
            "reason": "health_unavailable",
        }
    finally:
        service.close()


def test_volatile_ack_does_not_reissue_failing_paper_pause(tmp_path, monkeypatch):
    import omega.runtime.control as module

    class Paper:
        failing = False

        def command(self, *_):
            if self.failing:
                raise OSError("recording unavailable")
            return {}

        def snapshot(self):
            return {"accounts": [], "strategies": []}

    paper = Paper()
    service = CoreService(tmp_path, paper_provider=paper)
    try:
        service.control.incident("ALL_RECORDING_UNAVAILABLE")
        incident = service.control.snapshot()["incidents"][0]
        service.control.storage_failed = True
        paper.failing = True
        monkeypatch.setattr(
            module, "atomic_json", lambda *_: (_ for _ in ()).throw(OSError("full"))
        )
        response = service.request(
            "POST",
            "/api/commands",
            service.token,
            {
                "id": "ack",
                "kind": "incident.acknowledge",
                "payload": {"incident_id": incident["id"]},
            },
        )
        assert response["status"] == "volatile"
        assert all(row["acknowledged"] for row in service.control.snapshot()["incidents"])
        assert service.control.snapshot()["paused"]
    finally:
        service.close()


def test_strategy_detail_is_owner_only_and_separate_from_state(tmp_path):
    class Paper:
        def command(self, *_):
            return {}

        def snapshot(self):
            return {"accounts": [], "strategies": [{"instance": "s", "status": "ready"}]}

        def detail(self, instance):
            return {"instance": instance, "fills": [{"price": "100"}]}

    service = CoreService(tmp_path, paper_provider=Paper())
    try:
        assert "fills" not in service.state()["strategies"][0]
        assert service.request("GET", "/api/strategies/s", service.token)["fills"]
        device = service.control.pair(service.control.create_pairing("phone")["code"])
        with pytest.raises(PermissionError):
            service.request("GET", "/api/strategies/s", device["token"])
    finally:
        service.close()


def test_operation_lookup_is_read_only_and_scoped_to_device(service):
    command = {"id": "owner-command", "kind": "pause", "payload": {}}
    result = service.request("POST", "/api/commands", service.token, command)
    assert service.request("GET", "/api/operations/owner-command", service.token) == result
    paired = service.control.pair(service.control.create_pairing("phone")["code"])
    with pytest.raises(LookupError):
        service.request("GET", "/api/operations/owner-command", paired["token"])
    device_command = {**command, "id": "phone-command"}
    device_result = service.request("POST", "/api/commands", paired["token"], device_command)
    assert service.request("GET", "/api/operations/phone-command", paired["token"]) == device_result


def test_paper_provider_calls_are_idempotent_and_restart_pauses(tmp_path):
    class Paper:
        def __init__(self):
            self.calls = []

        def command(self, kind, payload):
            self.calls.append(kind)
            return {"kind": kind}

        def snapshot(self):
            return {"accounts": [{"id": "paper"}], "strategies": []}

    paper = Paper()
    service = CoreService(tmp_path, paper_provider=paper, modules_provider=lambda: [{"id": "tef"}])
    try:
        command = {"id": "create", "kind": "paper.account.create", "payload": {}}
        service.request("POST", "/api/commands", service.token, command)
        service.request("POST", "/api/commands", service.token, command)
        assert paper.calls == ["pause", "paper.account.create"]
        assert service.state()["accounts"] == [{"id": "paper"}]
        assert service.request("GET", "/api/modules", service.token)["ts"] == [{"id": "tef"}]
    finally:
        service.close()


def test_research_jobs_have_no_trading_provider_access(tmp_path):
    class Research:
        def list_jobs(self):
            return []

        def submit(self, kind, parameters):
            return {"id": "job", "kind": kind, "parameters": parameters}

        def status(self, identity):
            return {"id": identity, "status": "completed"}

        def close(self):
            self.closed = True

    research = Research()
    service = CoreService(tmp_path, research=research)
    try:
        assert (
            service.request(
                "POST", "/api/jobs", service.token, {"kind": "export", "parameters": {}}
            )["id"]
            == "job"
        )
        assert service.request("GET", "/api/jobs/job", service.token)["status"] == "completed"
    finally:
        service.close()
    assert research.closed


def test_job_state_and_list_are_bounded_summaries_without_training_parameters(tmp_path):
    class Research:
        def list_jobs(self):
            return [
                {
                    "id": str(index),
                    "kind": "train",
                    "state": "queued",
                    "parameters": {"huge": "x" * 100_000},
                    "trace": "private",
                    "error": "e" * 1000,
                }
                for index in range(101)
            ]

        def close(self):
            pass

    service = CoreService(tmp_path, research=Research())
    try:
        listed = service.request("GET", "/api/jobs", service.token)["jobs"]
        assert len(listed) == 100
        assert "parameters" not in listed[0] and "trace" not in listed[0]
        assert len(listed[0]["error"]) == 256
        assert service.state()["jobs"] == listed
    finally:
        service.close()


@pytest.fixture
def wire(service, monkeypatch):
    import omega.runtime.service as module

    class Server:
        server_port = 8765

        def serve_forever(self):
            pass

        def shutdown(self):
            pass

        def server_close(self):
            pass

    monkeypatch.setattr(module, "BoundedHTTPServer", lambda *_: Server())
    service.start()

    def send(raw):
        class Connection:
            def __init__(self):
                self.output = bytearray()

            def makefile(self, *_args):
                return BytesIO(raw)

            def sendall(self, data):
                self.output.extend(data)

        connection = Connection()
        service.handler_class(connection, ("127.0.0.1", 1), service.server)
        return bytes(connection.output)

    return send


def test_http_wire_authenticated_json_and_no_store_headers(service, wire):
    response = wire(
        (
            "GET /api/state HTTP/1.1\r\nHost: 127.0.0.1:8765\r\n"
            f"Authorization: Bearer {service.token}\r\n\r\n"
        ).encode()
    )
    assert response.startswith(b"HTTP/1.0 200")
    assert b"Cache-Control: no-store" in response
    assert b'"mode": "paper"' in response
    assert service.token.encode() not in response


@pytest.mark.parametrize(
    "path,content_type",
    [
        ("/mobile", b"text/html"),
        ("/mobile/", b"text/html"),
        ("/mobile/app.js", b"text/javascript"),
        ("/mobile/style.css", b"text/css"),
        ("/mobile/pwa.js", b"text/javascript"),
        ("/mobile/sw.js", b"text/javascript"),
        ("/mobile/manifest.webmanifest", b"application/manifest+json"),
        ("/mobile/icon.svg", b"image/svg+xml"),
        ("/mobile/icon-192.png", b"image/png"),
        ("/mobile/icon-512.png", b"image/png"),
        ("/mobile/apple-touch-icon.png", b"image/png"),
    ],
)
def test_mobile_assets_are_public_but_csp_protected_and_allowlisted(wire, path, content_type):
    response = wire(f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1:8765\r\n\r\n".encode())
    assert response.startswith(b"HTTP/1.0 200")
    assert b"Content-Security-Policy:" in response
    assert b"manifest-src 'self'" in response and b"worker-src 'self'" in response
    assert b"X-Content-Type-Options: nosniff" in response
    assert content_type in response
    traversal = wire(b"GET /mobile/../secrets/core_token HTTP/1.1\r\nHost: 127.0.0.1:8765\r\n\r\n")
    assert traversal.startswith(b"HTTP/1.0 404")


@pytest.mark.parametrize(
    "headers", ["Host: evil.example", "Host: 127.0.0.1:8765\r\nOrigin: https://evil.example"]
)
def test_http_wire_rejects_dns_rebinding_and_cross_origin(service, wire, headers):
    response = wire(
        (
            f"GET /api/state HTTP/1.1\r\n{headers}\r\nAuthorization: Bearer {service.token}\r\n\r\n"
        ).encode()
    )
    assert response.startswith(b"HTTP/1.0 403")


@pytest.mark.parametrize(
    "extra,body",
    [
        ("Content-Length: 262145", b""),
        ("Content-Length: 2\r\nContent-Length: 2", b"{}"),
        ("Transfer-Encoding: chunked", b""),
        ("Content-Length: 3", b"{}"),
        ("Content-Length: 3", b"NaN"),
    ],
)
def test_http_wire_rejects_oversized_ambiguous_and_nonfinite_json(service, wire, extra, body):
    response = wire(
        (
            "POST /api/commands HTTP/1.1\r\nHost: 127.0.0.1:8765\r\n"
            f"Authorization: Bearer {service.token}\r\n"
            f"Content-Type: application/json\r\n{extra}\r\n\r\n"
        ).encode()
        + body
    )
    assert response.startswith(b"HTTP/1.0 400")
