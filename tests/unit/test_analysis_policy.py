"""Pure provider declarations determine frozen local input policy, without DB IO."""

from dataclasses import replace

import pytest

from omega.domain.errors import ValidationError
from omega.infrastructure.postgres.config import DatabaseSettings, database_destination_id
from omega.runtime import analysis_policy as module

SETTINGS = DatabaseSettings("127.0.0.1", 55433, "omega_test", "omega_app", "synthetic-fixture")


def saved(configuration):
    return {"instrument": "binance:usdm:ETHUSDT", "configuration": configuration}


def test_ohlcv_only_needs_no_database_configuration(tmp_path, monkeypatch):
    def forbidden(_path):
        raise AssertionError("no DB config lookup for an OHLCV-only plan")

    monkeypatch.setattr(module, "load_database_settings", forbidden)
    for config in (
        {"moving_averages": [{"interval_id": 0, "ema_period": 2}]},
        {"volume_profile": [{"interval_id": 0}]},
    ):
        assert module.policy_for_saved_analysis(tmp_path, saved(config)) is None


def test_native_metric_policy_preserves_requested_channels_and_strictest_consumer_age(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(module, "load_database_settings", lambda path: SETTINGS)
    policy = module.policy_for_saved_analysis(
        tmp_path,
        saved(
            {
                "market_sentiment": [
                    {"interval_id": 0, "maximum_metric_age_ms": 330000},
                    {"interval_id": 2, "maximum_metric_age_ms": 60000},
                ]
            }
        ),
    )
    assert policy.channels == ("metric", "trade_flow")
    assert policy.maximum_metric_age_ms == 60000
    assert policy.destination_id == database_destination_id(SETTINGS)
    assert "synthetic-fixture" not in str(policy.document())
    monkeypatch.setattr(
        module, "load_database_settings", lambda path: replace(SETTINGS, password="other-fixture")
    )
    assert (
        module.policy_for_saved_analysis(
            tmp_path,
            saved(
                {
                    "market_sentiment": [
                        {"interval_id": 0, "maximum_metric_age_ms": 330000},
                        {"interval_id": 2, "maximum_metric_age_ms": 60000},
                    ]
                }
            ),
        ).checksum
        == policy.checksum
    )


@pytest.mark.parametrize(
    "configuration",
    [
        {"order_flow": [{"interval_id": 0, "analysis_code": "WOI"}]},
        {"short_term_scalar": [{"interval_id": 0}]},
    ],
)
def test_depth_policy_expresses_required_inputs_without_claiming_collection_ready(
    tmp_path, monkeypatch, configuration
):
    monkeypatch.setattr(module, "load_database_settings", lambda path: SETTINGS)
    policy = module.policy_for_saved_analysis(tmp_path, saved(configuration))
    assert "depth" in policy.channels


def test_order_flow_only_does_not_require_metric_channel(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "load_database_settings", lambda path: SETTINGS)
    result = module.policy_for_saved_analysis(
        tmp_path, saved({"order_flow": [{"interval_id": 0, "analysis_code": "NES"}]})
    )
    assert result.channels == ("trade_flow",)


def test_remote_database_cannot_be_silently_selected(tmp_path, monkeypatch):
    monkeypatch.setattr(
        module, "load_database_settings", lambda path: replace(SETTINGS, host="example.invalid")
    )
    with pytest.raises(ValidationError, match="local"):
        module.policy_for_saved_analysis(
            tmp_path, saved({"order_flow": [{"interval_id": 0, "analysis_code": "NES"}]})
        )
