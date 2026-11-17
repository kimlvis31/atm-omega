"""Explicit reset requests from a fresh cached observation; no automatic retries."""

import re

PATH = "/api/causal-inference/recovery"


def recovery_url(operation_id=None):
    if operation_id is None:
        return PATH
    if type(operation_id) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,96}", operation_id) is None:
        raise ValueError("Invalid NN recovery operation ID")
    return PATH + "/" + operation_id


def recovery_reset_body(operation_id, observation):
    """The server rechecks every fence; this check grants no recovery authority."""
    recovery_url(operation_id)
    if type(observation) is not dict:
        raise ValueError("Invalid NN recovery observation")
    epoch, actor = observation.get("core_epoch"), observation.get("actor_token")
    generation, revision = observation.get("generation"), observation.get("control_revision")
    checksum = observation.get("retirement_sha256")
    if (
        observation.get("scope") != "committed_inference_service"
        or observation.get("live_orders_enabled") is not False
        or observation.get("schema_version") != 1
        or type(observation.get("schema_version")) is not int
        or type(observation.get("recovery_scope")) is not str
        or type(observation.get("historical_carry_available")) is not bool
        or (observation.get("recovery_scope"), observation.get("historical_carry_available"))
        not in {("verified_pins0_reservations0", False), ("verified_historical_pin_carry", True)}
        or type(observation.get("cold_interrupted_adoption_available")) is not bool
        or observation.get("automatic_inference_started") is not False
        or observation.get("financial_resumed") is not False
        or type(observation.get("state")) is not str
        or observation.get("state")
        not in {
            "MANUAL_REQUIRED",
            "ACTIVE",
            "RECOVERY_HELD",
            "OLD_RETIRED",
            "HELD",
        }
        or observation.get("blockers") != []
        or observation.get("current_recovery_id") is not None
        or type(epoch) is not str
        or not 1 <= len(epoch) <= 128
        or type(actor) is not str
        or not 1 <= len(actor) <= 128
        or type(generation) is not int
        or not 0 <= generation <= 2**63 - 1
        or type(revision) is not int
        or not 0 <= revision <= 2**63 - 1
        or checksum is not None
        and (type(checksum) is not str or re.fullmatch(r"[a-f0-9]{64}", checksum) is None)
    ):
        raise ValueError("NN recovery is unavailable for this observation; inspect its blockers")
    return {
        "id": operation_id,
        "parameters": {
            "core_epoch": epoch,
            "expected_generation": generation,
            "expected_control_revision": revision,
            "expected_actor_token": actor,
            "expected_retirement_sha256": checksum,
            "state_policy": "reset",
            "confirm": True,
        },
    }
