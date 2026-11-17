"""Runtime settings affect newly submitted jobs, and are frozen into their input."""

from copy import deepcopy

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.service import CoreService


class Research:
    def __init__(self):
        self.calls = []

    def submit(self, kind, parameters):
        self.calls.append((kind, parameters))
        return {"id": "job", "kind": kind, "state": "queued"}

    def close(self):
        pass

    def cancel(self, _id):
        return {
            "id": "job",
            "kind": "train",
            "state": "cancelled",
            "parameters": {"huge": "x" * 100000},
        }


@pytest.fixture
def core(tmp_path):
    service = CoreService(tmp_path, research=Research())
    yield service
    service.close()


def test_global_simulation_preferences_are_frozen_and_explicit_job_choices_win(core):
    core.settings.update(
        {"simulation": {"backend": "gpu", "intrabar": "random", "seed": 123}}, revision=0
    )
    body = {"kind": "simulate", "parameters": {"dataset": "dataset.json"}}
    original = deepcopy(body)
    core.request("POST", "/api/jobs", core.token, body)
    assert body == original
    captured = core.research.calls[-1][1]
    assert captured["spec"] == {"backend": "gpu", "path": "random", "seed": 123}
    assert captured["settings_revision"] == 1
    core.settings.update({"simulation": {"seed": 456}}, revision=1)
    assert captured["spec"]["seed"] == 123
    explicit = core.job_parameters(
        "simulate", {"spec": {"backend": "cpu", "path": "O-L-H-C", "seed": 42}}
    )
    assert explicit["spec"] == {"backend": "cpu", "path": "O-L-H-C", "seed": 42}


def test_training_recording_preferences_map_to_plugin_contract_and_allow_override(core):
    core.settings.update(
        {"training": {"recording": "light", "optional_recording_failure": "stop"}}, revision=0
    )
    result = core.job_parameters("train", {})
    assert result["recording"] == {"profile": "lightweight", "optional_detail_failure": "stop"}
    result = core.job_parameters("train", {"recording": {"profile": "detailed"}})
    assert result["recording"] == {"profile": "detailed", "optional_detail_failure": "stop"}


def test_unsupported_conservative_mode_is_not_silently_claimed_as_implemented(core):
    core.settings.update({"simulation": {"intrabar": "conservative"}}, revision=0)
    with pytest.raises(ValidationError, match="not implemented"):
        core.request("POST", "/api/jobs", core.token, {"kind": "simulate", "parameters": {}})
    assert not core.research.calls
    assert (
        core.job_parameters("simulate", {"spec": {"path": "O-H-L-C"}})["spec"]["path"] == "O-H-L-C"
    )


@pytest.mark.parametrize("routes", [{}, {"bullish": "O-L-H-C"}])
def test_empty_or_partial_routes_cannot_bypass_missing_conservative_engine(core, routes):
    core.settings.update({"simulation": {"intrabar": "conservative"}}, revision=0)
    with pytest.raises(ValidationError):
        core.request(
            "POST",
            "/api/jobs",
            core.token,
            {"kind": "simulate", "parameters": {"spec": {"routes": routes}}},
        )
    assert not core.research.calls


def test_empty_routes_preserve_random_default_and_full_routes_explicitly_override_it(core):
    from omega.research.simulation import SimulationSpec

    core.settings.update({"simulation": {"intrabar": "random"}}, revision=0)
    result = core.job_parameters("simulate", {"spec": {"routes": {}}})
    assert SimulationSpec.parse(result["spec"]).path == "random"
    routes = {"bullish": "O-H-L-C", "bearish": "O-L-H-C", "doji": "O-H-L-C"}
    for preference in ("random", "conservative"):
        core.settings.update(
            {"simulation": {"intrabar": preference}}, revision=core.settings.snapshot()["revision"]
        )
        result = core.job_parameters("simulate", {"spec": {"routes": routes}})
        parsed = SimulationSpec.parse(result["spec"])
        assert parsed.routes == routes and parsed.path != "random"
    result = core.job_parameters(
        "simulate", {"spec": {"path": "O-L-H-C", "routes": {"bullish": "O-H-L-C"}}}
    )
    assert SimulationSpec.parse(result["spec"]).path == "O-L-H-C"


@pytest.mark.parametrize(
    "spec",
    [
        {"routes": None},
        {"routes": []},
        {"routes": {"up": "O-H-L-C"}},
        {"routes": {"bullish": "random"}},
        {"routes": {"bullish": []}},
        {"path": "unknown"},
        {"path": []},
        {"path": "random", "routes": {"bullish": "O-H-L-C"}},
    ],
)
def test_invalid_route_preferences_fail_before_job_submission(core, spec):
    with pytest.raises(ValidationError):
        core.request(
            "POST", "/api/jobs", core.token, {"kind": "simulate", "parameters": {"spec": spec}}
        )
    assert not core.research.calls


@pytest.mark.parametrize(
    "kind,parameters", [("simulate", {"spec": []}), ("train", {"recording": []})]
)
def test_malformed_nested_preferences_fail_before_job_submission(core, kind, parameters):
    with pytest.raises(ValidationError):
        core.request("POST", "/api/jobs", core.token, {"kind": kind, "parameters": parameters})
    assert not core.research.calls


def test_cancel_command_does_not_store_large_job_parameters_in_command_journal(core):
    response = core.request(
        "POST",
        "/api/commands",
        core.token,
        {"id": "cancel", "kind": "job.cancel", "payload": {"job_id": "job"}},
    )
    assert response["status"] == "completed"
    assert response["result"] == {"id": "job", "kind": "train", "state": "cancelled"}
