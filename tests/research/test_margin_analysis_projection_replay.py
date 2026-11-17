"""Real native V2 preflight and financial replay use the same explicit selected graph."""

from copy import deepcopy

from test_native_bundle_v2_margin_replay import configuration, replay, source

from omega.runtime.margin_research import run_margin_simulation_job
from omega.runtime.margin_session import MarginTradingSession


def test_native_v2_replay_keeps_full_ca_and_does_not_require_its_unused_observation_sources(
    tmp_path,
):
    _bundle, manifest, rows, records = source(tmp_path / "source", count=16, start=0, varied=True)
    settings = configuration(0)
    settings["strategies"][0].update(
        module="tef",
        configuration={"model": "MMACDDEFAULT", "fullStopLossImmediate": ".05"},
        analysis={
            "moving_averages": [
                {"interval_id": 0, "ema_period": 1000, "mmacd_periods": [2, 3], "signal_period": 2}
            ],
            "market_sentiment": [
                {"interval_id": 0, "nSamples": 2, "maximum_metric_age_ms": 330000}
            ],
            "order_flow": [{"interval_id": 0, "analysis_code": "WOI", "nSamples": 2}],
        },
        analysis_execution={"version": 1, "policy": "required_fields"},
    )
    original = deepcopy(settings)
    sessions = []

    def factory(*args, **kwargs):
        session = MarginTradingSession(*args, **kwargs)
        sessions.append(session)
        return session

    result = replay(
        tmp_path / "replay", manifest, rows, records, settings=settings, factory=factory
    )
    assert result["fills"], "Unconsumed auxiliary definitions must not hold a ready MMACD strategy"
    assert result["auxiliary_replay"]["graph_configuration"]["required_channels"] == {}
    assert result["auxiliary_replay"]["graph_configuration"]["metric_policies"] == {}
    assert sessions[0].analysis_input_channels == {}
    assert sessions[0]._meta["configuration"]["strategies"] == original["strategies"]
    assert settings == original
    assert (
        run_margin_simulation_job.omega_feature_scopes["analysis_execution"] == "required_fields_v1"
    )
