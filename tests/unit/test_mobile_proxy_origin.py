"""Explicit proxy origins preserve loopback binding and device/owner authority."""

import json
from copy import deepcopy

import pytest

from omega.domain.errors import AuthorizationDenied, ValidationError
from omega.runtime.mobile_origin import authorize_mobile_headers, canonical_mobile_origin
from omega.runtime.settings import DEFAULT_SETTINGS, SettingsStore
from test_core_service import service as service
from test_core_service import wire as wire

ORIGIN = "https://omega.private.example"
HOST = "omega.private.example"


@pytest.mark.parametrize("value", [ORIGIN, ORIGIN + ":8443", "https://192.168.1.5"])
def test_canonical_explicit_https_origin(value):
    assert canonical_mobile_origin(value) == value


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        5,
        "http://omega.private.example",
        "//omega.private.example",
        ORIGIN + "/",
        ORIGIN + "/mobile",
        ORIGIN + "?",
        ORIGIN + "#",
        "https://user@omega.private.example",
        "https://user:password@omega.private.example",
        "https://*.private.example",
        "https://OMEGA.private.example",
        " " + ORIGIN,
        ORIGIN + "\n",
        "https://omega.private.example\\evil",
        "https://omega%2eprivate.example",
        ORIGIN + ":443",
        ORIGIN + ":0",
        ORIGIN + ":65536",
        ORIGIN + ":08443",
        "https://omega.private.example.",
        "https://[::1]",
        "https://127.000.0.1",
        "https://한글.example",
        "https://" + "x" * 513,
    ],
)
def test_unsafe_or_ambiguous_origin_is_rejected_not_normalized(value):
    with pytest.raises(ValidationError):
        canonical_mobile_origin(value)


def test_default_empty_origin_keeps_exact_local_allowlist():
    assert canonical_mobile_origin("") == ""
    authorize_mobile_headers("127.0.0.1:8765", "http://127.0.0.1:8765", port=8765)
    with pytest.raises(AuthorizationDenied):
        authorize_mobile_headers(HOST, ORIGIN, port=8765)
    with pytest.raises(AuthorizationDenied):
        authorize_mobile_headers("127.0.0.1:8765", ORIGIN, port=8765)


def test_configuring_proxy_never_changes_loopback_server_binding(service, monkeypatch):
    import omega.runtime.service as module

    addresses = []

    class Server:
        server_port = 8765

        def serve_forever(self):
            pass

        def shutdown(self):
            pass

        def server_close(self):
            pass

    def server(address, _handler):
        addresses.append(address)
        return Server()

    monkeypatch.setattr(module, "BoundedHTTPServer", server)
    enable(service)
    service.start()
    assert addresses == [("127.0.0.1", 0)]


def test_previous_settings_migrate_disabled_and_incomplete_mobile_is_not_repaired(tmp_path):
    path = tmp_path / "settings.json"
    old = deepcopy(DEFAULT_SETTINGS)
    old.pop("mobile")
    path.write_text(json.dumps(old), encoding="utf-8")
    assert SettingsStore(path).snapshot()["mobile"] == {"https_origin": ""}
    old["mobile"] = {}
    path.write_text(json.dumps(old), encoding="utf-8")
    with pytest.raises(ValidationError):
        SettingsStore(path)


@pytest.mark.parametrize("mobile_present", [False, True])
def test_exact_execution_analysis_legacy_migration_is_independent_of_mobile(
    tmp_path,
    mobile_present,
):
    path = tmp_path / "settings.json"
    value = deepcopy(DEFAULT_SETTINGS)
    value.pop("execution")
    value.pop("analysis")
    if not mobile_present:
        value.pop("mobile")
    value["revision"] = 7
    path.write_text(json.dumps(value), encoding="utf-8")
    original = path.read_bytes()
    restored = SettingsStore(path).snapshot()
    assert restored["execution"] == DEFAULT_SETTINGS["execution"]
    assert restored["analysis"] == DEFAULT_SETTINGS["analysis"]
    assert restored["mobile"] == DEFAULT_SETTINGS["mobile"]
    assert restored["revision"] == 7
    assert path.read_bytes() == original
    # One missing category is malformed, rather than a known historic schema.
    value["execution"] = deepcopy(DEFAULT_SETTINGS["execution"])
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValidationError):
        SettingsStore(path)


def test_invalid_origin_update_leaves_original_settings_and_revision(tmp_path):
    store = SettingsStore(tmp_path / "settings.json")
    before = store.snapshot()
    with pytest.raises(ValidationError):
        store.update({"mobile": {"https_origin": "http://unsafe.example"}}, revision=0)
    assert store.snapshot() == before


def request_wire(wire, *, host=HOST, origin=ORIGIN, token="", path="/api/state", extra=""):
    return wire(
        (
            f"GET {path} HTTP/1.1\r\nHost: {host}\r\nOrigin: {origin}\r\n"
            f"Authorization: Bearer {token}\r\n{extra}\r\n"
        ).encode()
    )


def enable(service):
    service.settings.update({"mobile": {"https_origin": ORIGIN}}, revision=0)


def test_configured_proxy_and_local_desktop_both_require_original_auth(service, wire):
    enable(service)
    assert request_wire(wire).startswith(b"HTTP/1.0 403")
    assert request_wire(wire, token=service.token).startswith(b"HTTP/1.0 200")
    assert request_wire(
        wire, host="127.0.0.1:8765", origin="http://127.0.0.1:8765", token=service.token
    ).startswith(b"HTTP/1.0 200")
    # A trusted proxy may rewrite Host to its local upstream while preserving Origin.
    assert request_wire(wire, host="127.0.0.1:8765", token=service.token).startswith(
        b"HTTP/1.0 200"
    )


def test_clearing_origin_immediately_removes_proxy_acceptance(service, wire):
    enable(service)
    assert request_wire(wire, token=service.token).startswith(b"HTTP/1.0 200")
    service.settings.update({"mobile": {"https_origin": ""}}, revision=1)
    assert request_wire(wire, token=service.token).startswith(b"HTTP/1.0 403")


@pytest.mark.parametrize(
    "host,origin,extra",
    [
        ("evil.example", ORIGIN, "X-Forwarded-Host: omega.private.example\r\n"),
        (HOST, "https://evil.example", "Forwarded: host=omega.private.example;proto=https\r\n"),
        (HOST, ORIGIN, "Host: evil.example\r\n"),
        (HOST, ORIGIN, "Origin: https://evil.example\r\n"),
    ],
)
def test_foreign_and_duplicate_headers_stay_forbidden_even_with_forwarded_headers(
    service, wire, host, origin, extra
):
    enable(service)
    response = request_wire(wire, host=host, origin=origin, extra=extra, token=service.token)
    assert response.startswith(b"HTTP/1.0 403")
    assert b'"error": "PERMISSION_DENIED"' in response


def test_proxy_public_shell_keeps_csp_and_never_grants_owner_api_to_paired_device(service, wire):
    enable(service)
    shell = request_wire(wire, path="/mobile/")
    assert shell.startswith(b"HTTP/1.0 200") and b"Content-Security-Policy:" in shell
    assert b"connect-src 'self'" in shell
    pairing = service.request(
        "POST",
        "/api/commands",
        service.token,
        {
            "id": "proxy-pair",
            "kind": "issue_pairing",
            "payload": {"name": "synthetic phone", "permissions": ["read", "pause"]},
        },
    )
    device = service.request("POST", "/api/pair", "", {"code": pairing["result"]["code"]})
    state = request_wire(wire, token=device["token"])
    assert state.startswith(b"HTTP/1.0 200") and b'"settings": {}' in state
    denied = request_wire(wire, token=device["token"], path="/api/settings")
    assert denied.startswith(b"HTTP/1.0 403")
