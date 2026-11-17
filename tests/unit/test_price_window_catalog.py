"""Saved price windows compose without changing previously frozen calculations."""

import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from omega.analysis.catalog import (
    FEATURE_CODES,
    configured_output_fields,
    create_features,
    validate_configuration,
)
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market

INSTRUMENT = Instrument("binance", Market.SPOT, "BTCUSDT")
DEFINITION = {
    "window_averages": [
        {"interval_id": 0, "analysis_code": "SMA", "nSamples": 3},
        {"interval_id": 0, "analysis_code": "WMA", "nSamples": 3},
    ],
    "bollinger": [{"interval_id": 0, "nSamples": 3}],
}
FIELDS = frozenset(
    {"0_SMA_0_SMA", "0_WMA_0_WMA", "0_BOL_0_BOLLOW", "0_BOL_0_BOLHIGH", "0_BOL_0_MA"}
)


def candle(index):
    price = Decimal("100.005") + Decimal(index % 11) * Decimal(".123")
    return Candle(
        INSTRUMENT, index * 60000, (index + 1) * 60000, price, price, price, price, Decimal(1)
    )


@pytest.mark.parametrize("index", [0, 1])
def test_frozen_old_configuration_outputs_and_checkpoint_bytes_are_preserved(index):
    fixture = json.loads(
        (Path(__file__).parents[1] / "fixtures/price-window-previous.json").read_text(
            encoding="utf-8"
        )
    )["cases"][index]
    assert validate_configuration(fixture["configuration"]) == fixture["canonical"]
    assert not {"window_averages", "bollinger"}.intersection(fixture["canonical"])
    graph = create_features(INSTRUMENT, set(fixture["required"]), fixture["configuration"])
    assert [graph.update(candle(i)) for i in range(37)] == fixture["trace"]
    assert graph.checkpoint() == fixture["checkpoint"]
    restored = graph.restored(fixture["checkpoint"], INSTRUMENT)
    assert [restored.update(candle(i)) for i in range(37, 45)] == fixture["future"]
    assert restored.checkpoint() == fixture["final_checkpoint"]


def test_installed_declarations_keep_sma_wma_and_bollinger_distinct():
    assert {"SMA", "WMA", "BOL"} <= set(FEATURE_CODES)
    canonical = validate_configuration(DEFINITION)
    assert canonical["moving_averages"] == canonical["momentum"] == []
    assert canonical["bollinger"][0] == {
        "interval_id": 0,
        "nSamples": 3,
        "MAType": "SMA",
        "bandWidth": 2.0,
        "line_index": 0,
        "price_precision": 8,
    }
    assert configured_output_fields(DEFINITION) == FIELDS | {"CLOSED_0"}
    assert validate_configuration(canonical) == canonical


def test_saved_price_groups_consume_one_shared_bar_aggregation(monkeypatch):
    from omega.analysis.aggregation import CandleAggregation

    calls, original = [], CandleAggregation.update

    def observed(aggregation, minute):
        calls.append(minute.open_time_ms)
        return original(aggregation, minute)

    monkeypatch.setattr(CandleAggregation, "update", observed)
    graph = create_features(INSTRUMENT, FIELDS, DEFINITION)
    for index in range(12):
        graph.update(candle(index))
    assert calls == [index * 60000 for index in range(12)]
    for node in graph._nodes:
        if node.name in {"window_averages", "bollinger"}:
            assert node.dependencies == ("bars",)
            assert node.feature._aggregation._last is None


@pytest.mark.parametrize("group", ["window_averages", "bollinger"])
def test_same_output_identity_cannot_change_parameters_by_adding_a_second_row(group):
    value = deepcopy(DEFINITION)
    value[group].append({**value[group][0], "nSamples": 5})
    with pytest.raises(ValidationError, match="duplicate"):
        validate_configuration(value)


@pytest.mark.parametrize(
    "group,row",
    [
        ("window_averages", {"interval_id": 0, "analysis_code": "BOL"}),
        ("window_averages", {"interval_id": 0, "analysis_code": "SMA", "MAType": "EMA"}),
        ("bollinger", {"interval_id": 0, "analysis_code": "BOL"}),
        ("bollinger", {"interval_id": 0, "MAType": "sma"}),
        ("bollinger", {"interval_id": 0, "MAType": True}),
        ("bollinger", {"interval_id": 0, "bandWidth": True}),
        ("bollinger", {"interval_id": 0, "bandWidth": "2.0"}),
        ("bollinger", {"interval_id": 0, "bandWidth": float("inf")}),
        ("bollinger", {"interval_id": 0, "bandWidth": float("nan")}),
        ("bollinger", {"interval_id": 0, "nSamples": True}),
    ],
)
def test_saved_wire_rejects_irrelevant_and_untyped_parameters(group, row):
    with pytest.raises(ValidationError):
        validate_configuration({group: [row]})


@pytest.mark.parametrize("width", [-2.0, -0.0, 0, 0.5, 2])
def test_signed_and_zero_bollinger_widths_survive_canonical_save(width):
    value = {"bollinger": [{"interval_id": 0, "bandWidth": width}]}
    canonical = validate_configuration(value)
    assert canonical["bollinger"][0]["bandWidth"] == width
    assert validate_configuration(json.loads(json.dumps(canonical))) == canonical


def test_window_and_band_groups_share_one_retained_state_budget():
    value = {
        "window_averages": [
            {"interval_id": 0, "analysis_code": "SMA", "line_index": line, "nSamples": 20000}
            for line in range(4)
        ],
        "bollinger": [{"interval_id": 0, "nSamples": 19985}],
    }
    # Retained entries: 4*(20000+3)+(19985+3) = 100000 across both groups.
    assert len(validate_configuration(value)["window_averages"]) == 4
    value["bollinger"][0]["nSamples"] += 1
    with pytest.raises(ValidationError):
        validate_configuration(value)


def test_global_definition_limit_also_counts_both_price_window_groups():
    rows = [
        {"interval_id": interval, "analysis_code": "SMA", "line_index": line, "nSamples": 2}
        for interval in range(6)
        for line in range(10)
    ]
    bands = [{"interval_id": 0, "line_index": line, "nSamples": 2} for line in range(5)]
    value = {"window_averages": rows, "bollinger": bands[:4]}
    assert len(validate_configuration(value)["window_averages"]) == 60
    value["bollinger"] = bands
    with pytest.raises(ValidationError):
        validate_configuration(value)


@pytest.mark.parametrize("prefix", [0, 1, 8, 14, 15, 16, 30])
def test_mixed_old_and_window_graph_restores_during_warmup_preview_and_commit(prefix):
    value = {
        "moving_averages": [{"interval_id": 0, "ema_period": 3, "mmacd_periods": []}],
        "window_averages": [
            {"interval_id": 1, "analysis_code": "SMA", "nSamples": 3},
            {"interval_id": 2, "analysis_code": "WMA", "nSamples": 2},
        ],
        "bollinger": [{"interval_id": 1, "nSamples": 3, "MAType": "EMA", "bandWidth": -2.0}],
    }
    required = configured_output_fields(value)
    graph = create_features(INSTRUMENT, required, value)
    for index in range(prefix):
        graph.update(candle(index))
    state = graph.checkpoint()
    restored = create_features(INSTRUMENT, required, value).restored(state, INSTRUMENT)
    assert restored.checkpoint() == state
    for index in range(prefix, prefix + 17):
        assert restored.update(candle(index)) == graph.update(candle(index))
    assert restored.checkpoint() == graph.checkpoint()


def test_saved_windows_execute_through_owner_ca_with_only_candle_source(tmp_path):
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
            {"expected_revision": 0, "name": "Price windows", "configuration": DEFINITION},
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
        result = core.request("GET", "/api/analysis-execution?analysis_id=" + ca["id"], core.token)
        row = result["instances"][0]
        assert row["ready"] and row["state"] == "running"
        assert {field: row["outputs"][field] for field in FIELDS} == dict.fromkeys(FIELDS, 11.0)
        assert len(source.calls) == 1 and core.state()["strategies"] == []
        assert not (tmp_path / "database.config.toml").exists()
    finally:
        core.close()
