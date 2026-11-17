"""Installed TPD shares graph aggregation, declarations and durable restoration."""

from decimal import Decimal

import pytest

from omega.analysis.catalog import configured_output_fields, create_features, validate_configuration
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market

INSTRUMENT = Instrument("binance", Market.SPOT, "BTCUSDT")
DEFINITION = {"interval_id": 0, "viewLength": 3, "nSamples": 4, "nSamplesMA": 2, "line_index": 1}
FIELDS = frozenset("0_TPD_1_" + suffix for suffix in ("TPD", "TPDABSMA", "TPDABSMAREL"))


def candle(index):
    price = Decimal(100 + index % 7)
    return Candle(
        INSTRUMENT, index * 60000, (index + 1) * 60000, price, price, price, price, Decimal(1)
    )


def test_saved_tpd_declaration_and_restored_mixed_graph_continue_exactly():
    configuration = {
        "temporal_probability": [DEFINITION],
        "moving_averages": [
            {"interval_id": 0, "ema_period": 3, "mmacd_periods": [], "signal_period": 2}
        ],
    }
    assert FIELDS <= configured_output_fields(configuration)
    required = FIELDS | {"0_EMA_0_EMA"}
    original = create_features(INSTRUMENT, required, configuration)
    for index in range(25):
        original.update(candle(index))
    assert original.update(candle(24))["0_TPD_1_TPD"] is not None
    restored = create_features(INSTRUMENT, required, configuration).restored(
        original.checkpoint(), INSTRUMENT
    )
    for index in range(25, 40):
        assert restored.update(candle(index)) == original.update(candle(index))
    assert restored.checkpoint() == original.checkpoint()


@pytest.mark.parametrize(
    "change", [{"viewLength": True}, {"nSamplesMA": 1}, {"line_index": 10}, {"bias": 1}]
)
def test_invalid_saved_tpd_never_becomes_an_installed_feature(change):
    with pytest.raises(ValidationError):
        validate_configuration({"temporal_probability": [{**DEFINITION, **change}]})


def test_duplicate_line_does_not_publish_conflicting_fields():
    with pytest.raises(ValidationError, match="duplicate"):
        validate_configuration(
            {"temporal_probability": [DEFINITION, {**DEFINITION, "nSamples": 5}]}
        )


def test_saved_tpd_runs_through_owner_library_and_durable_ca_without_auxiliary_db(tmp_path):
    from test_analysis_execution_service import Source

    from omega.runtime.analysis_execution import AnalysisExecution
    from omega.runtime.analysis_library import AnalysisLibrary
    from omega.runtime.service import CoreService

    core, source = CoreService(tmp_path), Source()
    core.analysis_execution = AnalysisExecution(
        tmp_path, lambda: source, clock_ms=lambda: 600000, background=False
    )
    core.analysis_library = AnalysisLibrary(
        tmp_path / "state/analysis-library.json", references_provider=lambda: []
    )

    def command(kind, payload, identity):
        result = core.request(
            "POST", "/api/commands", core.token, {"id": identity, "kind": kind, "payload": payload}
        )
        assert result["status"] == "completed", result
        return result["result"]

    try:
        template = command(
            "analysis.template.create",
            {
                "expected_revision": 0,
                "name": "TPD",
                "configuration": {
                    "temporal_probability": [
                        {**DEFINITION, "viewLength": 2, "nSamples": 2, "nSamplesMA": 2}
                    ]
                },
            },
            "template",
        )["item"]
        ca = command(
            "analysis.instance.create",
            {
                "expected_revision": 1,
                "name": "CA",
                "instrument": "binance:usdm:BTCUSDT",
                "template_id": template["id"],
            },
            "ca",
        )["item"]
        command(
            "analysis.execution.start",
            {"analysis_id": ca["id"], "expected_revision": ca["revision"], "warmup_minutes": 8},
            "start",
        )
        core.analysis_execution.run_once()
        reply = core.request("GET", "/api/analysis-execution?analysis_id=" + ca["id"], core.token)
        row = reply["instances"][0]
        assert row["ready"] and row["state"] == "running"
        assert {field: row["outputs"][field] for field in FIELDS} == dict.fromkeys(FIELDS, 0.0)
        assert len(source.calls) == 1 and core.state()["strategies"] == []
        assert not (tmp_path / "database.config.toml").exists()
    finally:
        core.close()
