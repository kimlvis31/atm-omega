"""Small owner-control contract for explicit cold adoption; no runtime imports."""

import re
from copy import deepcopy

PATH = "/api/causal-inference/recovery/actions/adopt"
STATUS_PATH = "/api/causal-inference/recovery/"


def _identity(value):
    return type(value) is str and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is not None


def _sha(value):
    return type(value) is str and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def adoption_url(operation_id=None):
    if operation_id is None:
        return PATH
    if type(operation_id) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,96}", operation_id) is None:
        raise ValueError("Invalid cold adoption request ID")
    return STATUS_PATH + operation_id


def validate_adoption_snapshot(value):
    if (
        type(value) is not dict
        or value.get("scope") != "committed_inference_service"
        or value.get("adoption_scope") != "cold_startup_observation"
        or value.get("live_orders_enabled") is not False
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 1
        or not _identity(value.get("core_epoch"))
        or not _identity(value.get("prior_phase"))
        or type(value.get("eligible")) is not bool
        or type(value.get("blockers")) is not list
        or len(value["blockers"]) > 64
        or any(not _identity(code) for code in value["blockers"])
        or not _sha(value.get("observation_sha256"))
        or value.get("requires_confirmation") is not True
        or any(
            value.get(key) is not False
            for key in (
                "factory_authority",
                "physical_close_claimed",
                "automatic_inference_started",
                "financial_resumed",
                "old_uncertainty_cleared",
            )
        )
    ):
        raise ValueError("Invalid cold adoption observation")
    generation, actor = value.get("candidate_generation"), value.get("candidate_actor_token")
    if (
        generation is not None
        and (type(generation) is not int or not 0 <= generation < 2**63)
        or actor is not None
        and not _identity(actor)
        or (generation is None) != (actor is None)
        or any(
            value.get(key) is not None and not _sha(value[key])
            for key in (
                "registry_sha256",
                "startup_certificate_sha256",
                "heads_sha256",
            )
        )
        or value["eligible"]
        and (
            value["blockers"]
            or generation is None
            or any(
                value.get(key) is None
                for key in (
                    "registry_sha256",
                    "startup_certificate_sha256",
                    "heads_sha256",
                )
            )
        )
    ):
        raise ValueError("Unconfirmed cold adoption identity or heads")
    return deepcopy(value)


def adoption_body(operation_id, observation):
    adoption_url(operation_id)
    observation = validate_adoption_snapshot(observation)
    if not observation["eligible"]:
        raise ValueError("Cold adoption is held; inspect the cached blockers")
    return {
        "id": operation_id,
        "parameters": {
            "core_epoch": observation["core_epoch"],
            "expected_observation_sha256": observation["observation_sha256"],
            "state_policy": "reset",
            "confirm": True,
        },
    }
