"""Configuration reads expose provenance, never grant financial activation."""

from copy import deepcopy

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.console import ControlConsole
from omega.runtime.margin_configuration import configuration_response
from omega.runtime.margin_configuration_client import configuration_url, validate_configuration
from omega.runtime.service import CoreService

EPOCH = "a" * 32


def parameters():
    return {
        "account": {
            "account_id": "a:b",
            "balance": "1000.00",
            "positions": [
                {
                    "instrument": "binance:usdm:BTCUSDT",
                    "owner_id": "ts",
                    "margin_mode": "cross",
                    "leverage": 10,
                    "budget_margin": "100",
                }
            ],
        },
        "strategies": [
            {
                "id": "ts",
                "instrument": "binance:usdm:BTCUSDT",
                "module": "target_quantity",
                "configuration": {"quantity": "1"},
                "analysis": {},
                "constraints": {},
            }
        ],
        "brackets": [],
        "start_ms": 60000,
    }


def ready():
    return {**configuration_response("a:b", parameters=parameters()), "core_epoch": EPOCH}


def test_hash_preserves_decimal_spelling_and_result_is_detached():
    source = ready()
    reply = validate_configuration(source, account_id="a:b", core_epoch=EPOCH)
    assert reply["parameters"]["account"]["balance"] == "1000.00"
    reply["parameters"]["account"]["balance"] = "2000"
    assert source["parameters"]["account"]["balance"] == "1000.00"
    changed = deepcopy(source)
    changed["parameters"]["account"]["balance"] = "1000"
    with pytest.raises(ValidationError, match="checksum"):
        validate_configuration(changed, account_id="a:b")


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", True),
        ("scope", "paper_shadow_only"),
        ("state", []),
        ("account_id", "other"),
        ("core_epoch", ""),
        ("configuration_sha256", "0" * 64),
        ("reason_code", "failure"),
        ("parameters", None),
    ],
)
def test_malformed_or_cross_scope_response_is_rejected(field, value):
    reply = ready()
    reply[field] = value
    with pytest.raises(ValidationError):
        validate_configuration(reply, account_id="a:b", core_epoch=EPOCH)


def test_unknown_is_not_an_empty_configuration_and_old_epoch_cannot_replace_draft():
    reply = {**configuration_response("a:b", reason_code="not_ready"), "core_epoch": EPOCH}
    assert validate_configuration(reply, account_id="a:b")["parameters"] is None
    with pytest.raises(ValidationError, match="epoch"):
        validate_configuration(ready(), account_id="a:b", core_epoch="b" * 32)
    reply["parameters"] = {}
    with pytest.raises(ValidationError):
        validate_configuration(reply, account_id="a:b")


class Backend:
    def __init__(self):
        self.calls = []

    def read_configuration(self, identity):
        self.calls.append(identity)
        return configuration_response(identity, reason_code="not_ready")

    def close(self):
        pass


@pytest.fixture
def core(tmp_path):
    backend = Backend()
    service = CoreService(tmp_path, margin_trading=backend)
    try:
        yield service, backend
    finally:
        service.close()


def test_owner_read_decodes_once_and_uses_no_financial_mutation_fence(core):
    service, backend = core

    class ForbiddenFence:
        def acquire(self, *args, **kwargs):
            raise AssertionError("configuration GET acquired the financial mutation fence")

    original = service.owner_mutation_lock
    service.owner_mutation_lock = ForbiddenFence()
    try:
        reply = service.request("GET", configuration_url("a:b"), service.token)
    finally:
        service.owner_mutation_lock = original
    assert backend.calls == ["a:b"]
    validate_configuration(reply, account_id="a:b", core_epoch=service.control.epoch)


@pytest.mark.parametrize(
    "segment",
    [
        "",
        "a/b",
        "a%2Fb",
        "a%252Fb",
        "a%",
        "a%FF",
        "a?x=1",
        "a#bad",
        "a" * 129,
    ],
)
def test_hostile_identity_rejected_before_cache_read(core, segment):
    service, backend = core
    with pytest.raises(ValidationError):
        service.request(
            "GET", "/api/margin-trading/accounts/" + segment + "/configuration", service.token
        )
    assert backend.calls == []


def test_query_and_paired_device_cannot_read_owner_configuration(core):
    service, backend = core
    with pytest.raises(ValidationError):
        service.request("GET", configuration_url("a:b") + "?copy=true", service.token)
    pairing = service.control.create_pairing("phone")
    device = service.control.pair(pairing["code"])
    with pytest.raises(PermissionError):
        service.request("GET", configuration_url("a:b"), device["token"])
    assert backend.calls == []


def test_console_exposes_read_only_workflow_and_rejects_options_before_transport(tmp_path):
    console = ControlConsole(tmp_path)
    calls = []

    def request(method, path):
        calls.append((method, path))
        return ready()

    console.request = request
    assert console.execute("margin-configuration a:b")["parameters"] == parameters()
    assert calls == [("GET", configuration_url("a:b"))]
    for command in (
        "margin-configuration",
        "margin-configuration a:b --resume",
        "margin-configuration a/b",
    ):
        with pytest.raises((ValueError, ValidationError)):
            console.execute(command)
    assert len(calls) == 1
