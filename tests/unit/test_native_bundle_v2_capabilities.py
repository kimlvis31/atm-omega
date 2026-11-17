"""Optional native V2 metadata is exact, bounded and unavailable to devices."""

from types import SimpleNamespace

import pytest

from omega.runtime.service import CoreService, research_feature_scope


@pytest.mark.parametrize(
    "scope", [None, True, "native_trade_flow_available_cutoff_v1", "recorded_native_inputs_v2"]
)
def test_bundle_v2_scope_is_job_specific_and_cannot_enable_other_native_replay(tmp_path, scope):
    calls = []

    def reader(job, feature):
        calls.append((job, feature))
        if (job, feature) == ("export_auxiliary", "native_bundle_v2"):
            return scope
        return "native_trade_flow_available_cutoff_v1" if feature == "auxiliary_replay" else None

    service = CoreService(
        tmp_path,
        research=SimpleNamespace(
            health=lambda: {"state": "ready"},
            list_jobs=lambda: [],
            close=lambda: None,
            supports=lambda kind: kind in {"export_auxiliary", "simulate_margin"},
            handler_scope=reader,
        ),
    )
    try:
        state = service.state()
        caps = state["core"]["capabilities"]
        assert caps["native_observation_bundle_v2"] is (scope == "recorded_native_inputs_v2")
        assert caps["margin_native_observation_replay"] is True
        assert caps["margin_native_observation_replay_v2"] is False
        assert ("export_auxiliary", "native_bundle_v2") in calls
        count = len(calls)
        device = service.state(owner=False)["core"]["capabilities"]
        assert len(calls) == count
        assert not device["native_observation_bundle_v2"]
        assert not device["margin_native_observation_replay_v2"]
        assert device["native_observation_bundle_v2_scope"] is None
        assert device["margin_native_observation_replay_v2_scope"] is None
        assert state["core"]["paused"] and not state["core"]["live_orders_enabled"]
    finally:
        service.close()


@pytest.mark.parametrize(
    "scope", [None, "recorded_native_inputs_v2", "x" * 129, "native_auxiliary_available_cutoff_v2"]
)
def test_replay_v2_scope_is_independent_of_export_and_v1(tmp_path, scope):
    service = CoreService(
        tmp_path,
        research=SimpleNamespace(
            health=lambda: {"state": "ready"},
            list_jobs=lambda: [],
            close=lambda: None,
            supports=lambda _kind: True,
            handler_scope=lambda job, feature: scope if feature == "auxiliary_replay_v2" else None,
        ),
    )
    try:
        caps = service.state()["core"]["capabilities"]
        assert caps["margin_native_observation_replay_v2"] is (
            scope == "native_auxiliary_available_cutoff_v2"
        )
        assert (
            caps["native_observation_bundle_v2"]
            is caps["margin_native_observation_replay"]
            is False
        )
    finally:
        service.close()


def test_optional_scope_failure_and_original_two_argument_api_remain_safe():
    calls = []
    provider = SimpleNamespace(
        handler_scope=lambda job, feature: calls.append((job, feature)) or "old"
    )
    assert research_feature_scope(provider, "funding") == "old"
    assert calls == [("simulate_margin", "funding")]

    def fail(*_args):
        raise RuntimeError("private metadata")

    assert (
        research_feature_scope(
            SimpleNamespace(handler_scope=fail), "native_bundle_v2", "export_auxiliary"
        )
        is None
    )
