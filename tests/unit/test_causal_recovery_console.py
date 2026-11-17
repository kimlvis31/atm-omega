"""Explicit terminal consent and lost-response identity use the same recovery API."""

from copy import deepcopy

import pytest

from omega.runtime.causal_recovery_client import PATH
from omega.runtime.console import ControlConsole


def observation():
    return {
        "schema_version": 1,
        "scope": "committed_inference_service",
        "recovery_scope": "verified_pins0_reservations0",
        "core_epoch": "epoch",
        "live_orders_enabled": False,
        "state": "HELD",
        "generation": 2,
        "actor_token": "a" * 32,
        "control_revision": 7,
        "retirement_sha256": None,
        "historical_carry_available": False,
        "cold_interrupted_adoption_available": False,
        "automatic_inference_started": False,
        "financial_resumed": False,
        "blockers": [],
        "current_recovery_id": None,
    }


def test_cli_lost_response_exposes_original_id_and_query_never_reposts(tmp_path):
    output, calls = [], []
    console = ControlConsole(tmp_path, output=output.append)

    def transport(method, path, body=None):
        calls.append((method, path, deepcopy(body)))
        if method == "POST":
            raise TimeoutError("response lost after admission")
        return observation() if path == PATH else {"id": path.rsplit("/", 1)[1], "state": "unknown"}

    console.request = transport
    with pytest.raises(TimeoutError):
        console.execute("causal-recover --confirm")
    operation = calls[1][2]["id"]
    assert calls[1][2]["parameters"] == {
        "core_epoch": "epoch",
        "expected_generation": 2,
        "expected_actor_token": "a" * 32,
        "expected_control_revision": 7,
        "expected_retirement_sha256": None,
        "state_policy": "reset",
        "confirm": True,
    }
    assert operation in output[0] and "causal-recovery " + operation in output[1]
    result = console.execute("causal-recovery " + operation)
    assert result["state"] == "unknown"
    assert [row[0] for row in calls] == ["GET", "POST", "GET"]


@pytest.mark.parametrize(
    "text",
    [
        "causal-recover",
        "causal-recover --confirm yes",
        "causal-recover --id retry",
        "causal-recovery ../bad",
    ],
)
def test_cli_invalid_consent_or_identity_never_contacts_core(tmp_path, text):
    console = ControlConsole(tmp_path)
    console.request = lambda *_a: pytest.fail("invalid input must not reach transport")
    with pytest.raises(ValueError):
        console.execute(text)


@pytest.mark.parametrize(
    "key,value",
    [
        ("blockers", ["RECORDING_UNKNOWN"]),
        ("generation", True),
        ("live_orders_enabled", True),
        ("current_recovery_id", "already-admitted"),
        ("recovery_scope", "unverified"),
        ("actor_token", None),
    ],
)
def test_cli_unsafe_observation_never_submits(tmp_path, key, value):
    console, calls = ControlConsole(tmp_path), []
    current = observation()
    current[key] = value

    def transport(method, path, body=None):
        calls.append((method, path))
        return current

    console.request = transport
    with pytest.raises(ValueError):
        console.execute("causal-recover --confirm")
    assert calls == [("GET", PATH)]
