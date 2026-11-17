"""Saved market producers share canonical contracts without pretending data exists."""

import json
from copy import deepcopy
from decimal import Decimal

import pytest

from omega.analysis.catalog import configured_output_fields, create_features, validate_configuration
from omega.analysis.market_catalog import GROUPS
from omega.analysis.market_graph import MarketFeatureGraph
from omega.analysis.order_flow import DEPTH_FIELDS
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")

BUDGET_CASES = (
    (
        "order_flow",
        [{"interval_id": 0, "analysis_code": "NES", "nSamples": 100000}],
        [
            {"interval_id": 0, "analysis_code": "NES", "nSamples": 100000},
            {"interval_id": 1, "analysis_code": "WOI", "nSamples": 2},
        ],
    ),
    (
        "market_sentiment",
        [{"interval_id": 0, "nSamples": 20000}],
        [{"interval_id": 0, "nSamples": 20000}, {"interval_id": 1, "nSamples": 2}],
    ),
    (
        "short_term_scalar",
        [
            {
                "interval_id": 0,
                "MMACDSlow": 20000,
                "MFINSamples": 20000,
                "DMIxADXNSamples": 20000,
                "WOINSamples": 20000,
            },
            {
                "interval_id": 1,
                "MMACDSlow": 5000,
                "MFINSamples": 5000,
                "DMIxADXNSamples": 5000,
                "WOINSamples": 5000,
            },
        ],
        [
            {
                "interval_id": 0,
                "MMACDSlow": 20000,
                "MFINSamples": 20000,
                "DMIxADXNSamples": 20000,
                "WOINSamples": 20000,
            },
            {
                "interval_id": 1,
                "MMACDSlow": 5001,
                "MFINSamples": 5000,
                "DMIxADXNSamples": 5000,
                "WOINSamples": 5000,
            },
        ],
    ),
    (
        "volume_profile",
        [{"interval_id": 0, "max_divisions": 32768}],
        [{"interval_id": 0, "max_divisions": 32768}, {"interval_id": 1, "max_divisions": 8192}],
    ),
    (
        "volume_profile",
        [{"interval_id": 0, "nSamples": 20000}],
        [{"interval_id": 0, "nSamples": 20000}, {"interval_id": 1, "nSamples": 2}],
    ),
)


def configuration():
    return {
        "moving_averages": [{"interval_id": 0, "ema_period": 2, "mmacd_periods": []}],
        "order_flow": [
            {"interval_id": 0, "analysis_code": code, "nSamples": 2} for code in ("NES", "WOI")
        ],
        "market_sentiment": [{"interval_id": 0, "nSamples": 2}],
        "short_term_scalar": [
            {
                "interval_id": 0,
                "MMACDFast": 2,
                "MMACDSlow": 3,
                "MMACDSignal": 2,
                "DMIxADXNSamples": 2,
                "MFINSamples": 2,
                "WOINSamples": 2,
                "WOIBaseNSamples": 2,
            }
        ],
        "volume_profile": [{"interval_id": 0, "nSamples": 3}],
    }


def frame(index, *, include=True):
    price = 100 + index % 3
    candle = Candle(
        INSTRUMENT,
        index * 60000,
        (index + 1) * 60000,
        Decimal(price),
        Decimal(price + 1),
        Decimal(price - 1),
        Decimal(price) + Decimal(".5"),
        Decimal(10),
    )
    end = candle.close_time_ms
    inputs = []
    if include:
        for channel, values in (
            (
                "trade_flow",
                (
                    ("AGGTRADE_NOTIONALBUY", Decimal(3 + index)),
                    ("AGGTRADE_NOTIONALSELL", Decimal(2)),
                ),
            ),
            (
                "metric",
                (
                    ("METRIC_OPENINTEREST", Decimal(100 + index)),
                    ("METRIC_LONGSHORTRATIO", Decimal("1.1")),
                ),
            ),
            (
                "depth",
                tuple(
                    (key, Decimal(1 + index + offset)) for offset, key in enumerate(DEPTH_FIELDS)
                ),
            ),
        ):
            inputs.append(
                AuxiliaryInput(
                    channel, values, candle.open_time_ms, end, end, end, source="frozen_" + channel
                )
            )
    return MarketFeatureFrame(candle, end, tuple(inputs))


def test_canonical_contract_retains_defaults_and_exact_original_names():
    config = configuration()
    before = deepcopy(config)
    canonical = validate_configuration(config)
    assert config == before and validate_configuration(canonical) == canonical
    fields = configured_output_fields(config)
    assert {
        "0_EMA_0_EMA",
        "0_NES_0_NES",
        "0_WOI_0_WOI",
        "0_MST_0_MST",
        "0_STS_S",
        "0_IVP_NB0",
        "0_IVP_NB9",
        "CLOSED_0",
    } <= fields
    assert "0_STS_0_S" not in fields
    assert canonical["market_sentiment"][0]["maximum_metric_age_ms"] == 330000


def test_mixed_saved_graph_has_unique_public_fields_and_exact_frame_checkpoint():
    pytest.importorskip("scipy")
    config = configuration()
    graph = create_features(INSTRUMENT, {"CLOSED_0"}, config)
    assert isinstance(graph, MarketFeatureGraph)
    assert graph.descriptor.required_fields == frozenset({"OHLCV", "trade_flow", "metric", "depth"})
    for index in range(12):
        result = graph.update_frame(frame(index))
    assert set(result) == configured_output_fields(config)
    assert graph.input_ready and result["0_STS_S"] is not None and result["0_MST_0_MST"] is not None
    saved = json.loads(json.dumps(graph.checkpoint()))
    resumed = create_features(INSTRUMENT, {"CLOSED_0"}, config).restored(saved, INSTRUMENT)
    assert resumed.checkpoint() == saved
    assert resumed.update_frame(frame(12)) == graph.update_frame(frame(12))


def test_absent_auxiliary_inputs_mask_held_values_without_hiding_ohlcv_outputs():
    config = configuration()
    config.pop("volume_profile")
    graph = create_features(INSTRUMENT, {"CLOSED_0"}, config)
    for index in range(12):
        graph.update_frame(frame(index))
    result = graph.update_frame(frame(12, include=False))
    assert not graph.input_ready and result["0_EMA_0_EMA"] is not None
    assert result["CLOSED_0"] is True
    assert all(
        value is None
        for key, value in result.items()
        if any(code in key for code in ("_NES_", "_WOI_", "_STS_", "_MST_"))
    )


@pytest.mark.parametrize(
    "group", ["order_flow", "market_sentiment", "short_term_scalar", "volume_profile"]
)
def test_unknown_parameters_and_duplicate_outputs_cannot_be_silently_dropped(group):
    config = configuration()
    config[group][0]["unknown"] = 1
    with pytest.raises(ValidationError):
        validate_configuration(config)
    config = configuration()
    config[group].append(deepcopy(config[group][0]))
    with pytest.raises(ValidationError):
        validate_configuration(config)


def test_empty_base_groups_and_missing_required_fields_stay_explicit():
    config = {"order_flow": [{"interval_id": 0, "analysis_code": "NES"}]}
    graph = create_features(INSTRUMENT, {"0_NES_0_NES"}, config)
    assert isinstance(graph, MarketFeatureGraph)
    assert graph.update(frame(0).candle)["0_NES_0_NES"] is None
    with pytest.raises(ValidationError, match="does not provide"):
        create_features(INSTRUMENT, {"0_NES_0_NONEXISTENT"}, config)


@pytest.mark.parametrize("group,boundary,exceeded", BUDGET_CASES)
def test_saved_configuration_and_provider_share_total_collection_budgets(group, boundary, exceeded):
    configuration_type, provider_type = GROUPS[group]
    canonical = validate_configuration({group: boundary})
    assert validate_configuration(canonical) == canonical
    definitions = tuple(configuration_type(**row) for row in canonical[group])
    assert provider_type.validate_configurations(definitions) == definitions
    with pytest.raises(ValidationError):
        validate_configuration({group: exceeded})
    definitions = tuple(configuration_type(**row) for row in exceeded)
    with pytest.raises(ValidationError):
        provider_type.validate_configurations(definitions)
    with pytest.raises(ValidationError):
        provider_type(INSTRUMENT, definitions)


def test_canonical_collection_validation_constructs_no_provider_or_graph(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("canonical validation must not construct a provider")

    for _, provider_type in GROUPS.values():
        monkeypatch.setattr(provider_type, "__init__", forbidden)
    canonical = validate_configuration(configuration())
    assert canonical["volume_profile"][0]["nSamples"] == 3
    assert canonical["short_term_scalar"][0]["MMACDFast"] == 2


def test_explicit_empty_extra_groups_remain_valid_with_an_installed_base_indicator():
    value = {
        "moving_averages": [{"interval_id": 0, "ema_period": 2}],
        **{group: [] for group in GROUPS},
    }
    canonical = validate_configuration(value)
    assert all(canonical[group] == [] for group in GROUPS)
