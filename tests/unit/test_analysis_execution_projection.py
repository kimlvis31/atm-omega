"""Opt-in definition/line selection; whole saved profiles remain independent."""

import json
from copy import deepcopy
from decimal import Decimal as D
from hashlib import sha256
from pathlib import Path

import pytest

from omega.analysis import market_catalog
from omega.analysis.catalog import (
    EmptyFeatures,
    create_execution_features,
    create_features,
    validate_configuration,
)
from omega.analysis.market_graph import MarketFeatureGraph
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
POLICY = {"version": 1, "policy": "required_fields"}
MMACD = "0_MMACD_MSDELTAABSMAREL"


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def candle(index):
    close = D(100) + D(index % 7) / 10
    return Candle(
        BTC, index * 60000, (index + 1) * 60000, close, close + 1, close - 1, close, D(10)
    )


def ma_profile(*, ema=None, periods=(2, 3), signal=2):
    return {
        "moving_averages": [
            {
                "interval_id": 0,
                "ema_period": ema,
                "mmacd_periods": list(periods),
                "signal_period": signal,
            }
        ]
    }


def full_profile():
    return ma_profile() | {
        "market_sentiment": [{"interval_id": 5, "nSamples": 2, "maximum_metric_age_ms": 330000}],
        "order_flow": [{"interval_id": 0, "analysis_code": "WOI", "nSamples": 2}],
    }


def metric_frame(*, event=60000):
    bar = candle(2)
    flow = AuxiliaryInput(
        "trade_flow",
        (("AGGTRADE_NOTIONALBUY", D(800)), ("AGGTRADE_NOTIONALSELL", D(200))),
        bar.open_time_ms,
        bar.close_time_ms,
        bar.close_time_ms,
        bar.close_time_ms,
        source="fixture_flow",
    )
    metric = AuxiliaryInput(
        "metric",
        (("METRIC_OPENINTEREST", D(1000)), ("METRIC_LONGSHORTRATIO", D("1.1"))),
        bar.open_time_ms,
        bar.close_time_ms,
        event,
        event,
        quality="carried",
        source="fixture_metric_view",
    )
    return MarketFeatureFrame(bar, bar.close_time_ms, (flow, metric))


def test_unused_mst_woi_do_not_change_ohlcv_outputs_or_full_profile_evaluation():
    full = full_profile()
    before = encoded(full)
    graph, plan = create_execution_features(BTC, {MMACD}, full, POLICY)
    plain = create_features(BTC, {MMACD}, ma_profile())
    independent = create_features(BTC, {MMACD}, full)
    initial_checkpoint = independent.checkpoint()
    assert isinstance(independent, MarketFeatureGraph)
    assert independent.descriptor.required_fields == {"OHLCV", "trade_flow", "metric", "depth"}
    assert graph.descriptor.required_fields == {"OHLCV"}
    assert plan["raw_inputs"] == ["OHLCV"]
    assert plan["selected_configuration"]["market_sentiment"] == []
    assert plan["selected_configuration"]["order_flow"] == []
    for index in range(10):
        assert graph.update(candle(index)) == plain.update(candle(index))
        independent.update_frame(MarketFeatureFrame(candle(index), candle(index).close_time_ms))
    assert graph.output_values[MMACD] is not None
    assert not independent.input_ready and independent.output_values[MMACD] is not None
    assert encoded(full) == before
    restored = create_features(BTC, {MMACD}, full).restored(initial_checkpoint, BTC)
    assert restored.checkpoint() == initial_checkpoint


def test_unused_ivp_is_validated_but_never_constructed(monkeypatch):
    original_configuration, original_provider = market_catalog.GROUPS["volume_profile"]
    calls = []

    class UnusedProfile:
        @staticmethod
        def validate_configurations(configurations):
            return original_provider.validate_configurations(configurations)

        def __init__(self, *_args, **_kwargs):
            calls.append("constructed")
            raise AssertionError("unselected optional IVP must not be instantiated")

    monkeypatch.setitem(
        market_catalog.GROUPS, "volume_profile", (original_configuration, UnusedProfile)
    )
    full = full_profile() | {"volume_profile": [{"interval_id": 0, "nSamples": 2}]}
    graph, plan = create_execution_features(BTC, {MMACD}, full, POLICY)
    assert graph.descriptor.required_fields == {"OHLCV"}
    assert plan["selected_configuration"]["volume_profile"] == [] and calls == []


@pytest.mark.parametrize(
    "required,full,minimal,component",
    [
        ({MMACD}, ma_profile(ema=20000), ma_profile(), "MMACD"),
        (
            {"0_EMA_0_EMA"},
            ma_profile(ema=2, periods=(1000, 2000), signal=20000),
            ma_profile(ema=2, periods=(), signal=20000),
            "EMA",
        ),
    ],
)
def test_ma_independent_components_do_not_retain_unused_history(required, full, minimal, component):
    before = deepcopy(full)
    projected, plan = create_execution_features(BTC, required, full, POLICY)
    reference = create_features(BTC, required, minimal)
    assert (
        plan["selected_configuration"]["moving_averages"]
        == validate_configuration(minimal)["moving_averages"]
    )
    assert plan["selected_definitions"][0]["components"] == [component]
    for index in range(12):
        assert projected.update(candle(index)) == reference.update(candle(index))
    assert all(projected.output_values[field] is not None for field in required)
    assert full == before


def test_both_ma_components_and_duplicate_requirements_keep_one_configured_kernel():
    full = ma_profile(ema=2)
    graph, plan = create_execution_features(
        BTC, [MMACD, "0_EMA_0_EMA", MMACD, "CLOSED_0"], full, POLICY
    )
    assert len(plan["selected_definitions"]) == 1
    assert plan["selected_definitions"][0]["components"] == ["EMA", "MMACD"]
    assert plan["required_fields"] == sorted({MMACD, "0_EMA_0_EMA", "CLOSED_0"})
    assert plan["closure_intervals"] == [0]
    assert [node.name for node in graph._nodes] == ["bars", "ma"]
    assert graph._nodes[0].feature.intervals == (0,)


def test_closure_only_consumer_uses_real_bars_without_retaining_its_unused_mst():
    full = {"market_sentiment": [{"interval_id": 5, "nSamples": 2}]}
    graph, plan = create_execution_features(BTC, {"CLOSED_5"}, full, POLICY)
    assert plan["closure_intervals"] == [5] and plan["selected_definitions"] == []
    assert graph.descriptor.required_fields == {"OHLCV"}
    assert [node.name for node in graph._nodes] == ["bars"]
    for index in range(60):
        assert graph.update(candle(index))["CLOSED_5"] is (index == 59)


def test_required_mst_lineage_alone_sets_source_age_but_required_tight_line_still_gates():
    full = {
        "market_sentiment": [
            {"interval_id": 0, "line_index": 0, "nSamples": 2, "maximum_metric_age_ms": 330000},
            {"interval_id": 0, "line_index": 1, "nSamples": 2, "maximum_metric_age_ms": 30000},
        ]
    }
    field = "0_MST_0_MST"
    loose, plan = create_execution_features(BTC, {field}, full, POLICY)
    both, both_plan = create_execution_features(BTC, {field, "0_MST_1_MST"}, full, POLICY)
    assert loose.maximum_metric_age_ms == both.maximum_metric_age_ms == 330000
    assert [row["line_index"] for row in plan["selected_configuration"]["market_sentiment"]] == [0]
    assert len(both_plan["selected_definitions"]) == 2
    loose.update_frame(metric_frame())
    both.update_frame(metric_frame())
    assert loose.input_ready and not both.input_ready
    assert both.input_quality["market_sentiment"]["details"]["0_MST_1"]["metric"] == "stale"
    assert both.output_values[field] is None


def test_selected_disabled_metric_age_stays_zero_and_unready():
    full = {
        "market_sentiment": [
            {"interval_id": 0, "line_index": 0, "nSamples": 2, "maximum_metric_age_ms": 0},
            {"interval_id": 0, "line_index": 1, "nSamples": 2, "maximum_metric_age_ms": 330000},
        ]
    }
    graph, plan = create_execution_features(BTC, {"0_MST_0_MST"}, full, POLICY)
    assert graph.maximum_metric_age_ms == 0
    assert plan["selected_configuration"]["market_sentiment"][0]["maximum_metric_age_ms"] == 0
    graph.update_frame(metric_frame())
    assert not graph.input_ready


@pytest.mark.parametrize(
    "required", [{"CLOSED_7"}, {"0_MMACD_UNKNOWN"}, {"0_MST_7_MST"}, {"NN.signal"}]
)
def test_unknown_output_undeclared_closure_and_unowned_external_field_reject(required):
    with pytest.raises(ValidationError, match="required fields"):
        create_execution_features(BTC, required, ma_profile(), POLICY)


@pytest.mark.parametrize(
    "configuration",
    [
        {},
        {"ema_period": 2},
        {"moving_averages": [], "unknown": []},
        {"moving_averages": None},
        ma_profile() | {"market_sentiment": [{"interval_id": 0, "maximum_metric_age_ms": True}]},
    ],
)
def test_invalid_complete_profiles_are_not_hidden_by_projection(configuration):
    with pytest.raises(ValidationError):
        create_execution_features(BTC, set(), configuration, POLICY)


@pytest.mark.parametrize(
    "policy",
    [
        None,
        {},
        {"version": True, "policy": "required_fields"},
        {"version": 1.0, "policy": "required_fields"},
        {"version": 2, "policy": "required_fields"},
        {"version": 1, "policy": "all"},
        POLICY | {"extra": True},
    ],
)
def test_execution_policy_is_explicit_exact_and_native(policy):
    with pytest.raises(ValidationError, match="policy"):
        create_execution_features(BTC, {MMACD}, ma_profile(), policy)


@pytest.mark.parametrize(
    "configuration",
    [
        {"moving_averages": [], "momentum": []},
        {"market_sentiment": []},
    ],
)
def test_explicit_typed_empty_profile_is_only_for_no_remaining_analysis_fields(configuration):
    graph, plan = create_execution_features(BTC, frozenset(), configuration, POLICY)
    assert isinstance(graph, EmptyFeatures) and plan["raw_inputs"] == []
    assert (
        plan["required_fields"] == plan["selected_definitions"] == plan["closure_intervals"] == []
    )
    with pytest.raises(ValidationError):
        create_execution_features(BTC, {MMACD}, configuration, POLICY)
    with pytest.raises(ValidationError):
        create_features(BTC, frozenset(), configuration)


def test_plan_is_deterministic_detached_and_binds_the_full_validated_profile():
    full = full_profile()
    graph, plan = create_execution_features(BTC, {MMACD}, full, POLICY)
    _, again = create_execution_features(
        BTC, [MMACD, MMACD], deepcopy(full), dict(reversed(list(POLICY.items())))
    )
    assert plan == again
    assert set(plan) == {
        "version",
        "policy",
        "configuration_sha256",
        "required_fields",
        "selected_configuration",
        "selected_definitions",
        "closure_intervals",
        "raw_inputs",
    }
    assert plan["configuration_sha256"] == sha256(encoded(validate_configuration(full))).hexdigest()
    plan["selected_configuration"]["moving_averages"][0]["mmacd_periods"].append(99)
    full["moving_averages"][0]["signal_period"] = 200
    reference = create_features(BTC, {MMACD}, ma_profile())
    for index in range(9):
        assert graph.update(candle(index)) == reference.update(candle(index))
    _, changed = create_execution_features(BTC, {MMACD}, full, POLICY)
    assert changed["configuration_sha256"] != again["configuration_sha256"]


@pytest.mark.parametrize(
    "model,channels,intervals",
    [
        ("cgalpha", {"OHLCV", "trade_flow", "depth"}, [0, 5, 7]),
        ("pathfinder", {"OHLCV", "trade_flow", "depth", "metric"}, [0, 5]),
    ],
)
def test_complete_builtin_required_fields_keep_their_real_raw_dependencies(
    model, channels, intervals
):
    fixture = Path(__file__).parents[1] / "fixtures" / f"tef-{model}-native-inputs.json"
    profile = json.loads(fixture.read_text(encoding="utf-8"))
    full = profile["analysis"]
    full.setdefault("market_sentiment", []).append(
        {"interval_id": 0, "line_index": 9, "nSamples": 2, "maximum_metric_age_ms": 0}
    )
    before = deepcopy(full)
    graph, plan = create_execution_features(BTC, set(profile["required_fields"]), full, POLICY)
    assert graph.descriptor.required_fields == channels
    assert plan["raw_inputs"] == sorted(channels)
    assert plan["required_fields"] == sorted(profile["required_fields"])
    assert len(plan["required_fields"]) == (8 if model == "cgalpha" else 4)
    assert plan["closure_intervals"] == intervals
    assert all(row.get("line_index") != 9 for row in plan["selected_definitions"])
    assert full == before
    graph.update_frame(MarketFeatureFrame(candle(0), candle(0).close_time_ms))
    assert not graph.input_ready
    assert graph.output_values["CLOSED_0"] is True
