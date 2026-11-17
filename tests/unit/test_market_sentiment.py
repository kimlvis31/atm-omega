"""Original MST AST fixtures, explicit five-minute metric age, and causal state."""

import json
import math
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from omega.analysis.market_sentiment import (
    FLOW_FIELDS,
    METRIC_FIELDS,
    LegacyMarketSentimentFeatures,
    SentimentConfiguration,
)
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

REFERENCE = json.loads(Path("tests/fixtures/market-sentiment-legacy.json").read_text())
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
LOG_CHAIN_FIELDS = frozenset({"LSLOG_MA", "LS_DABSMA"})


def assert_original_raw(actual, expected):
    """Only unrounded libm log-chain fields admit a measured 16-local-ULP budget.

    Windows/Linux all-1440-frame diagnostics found maxima of 8/2 ULP for these
    two fields. Public rounded values, mode, raw inputs and every other field
    remain exact. Same-platform checkpoint comparisons never use this helper.
    """
    assert set(actual) == set(expected)
    for name, value in expected.items():
        observed = actual[name]
        assert type(observed) is type(value), name
        if name in LOG_CHAIN_FIELDS and type(value) is float:
            assert math.isfinite(observed) and math.isfinite(value), name
            assert abs(observed - value) <= 16 * max(math.ulp(observed), math.ulp(value)), name
        else:
            assert observed == value, name


def frame(
    index,
    *,
    metric="fixture",
    flow="fixture",
    as_of_ms=None,
    event_time_ms=None,
    metric_quality=None,
    coverage=Decimal(1),
):
    source = REFERENCE["inputs"][index]
    price = Decimal(source["price"])
    candle = Candle(
        INSTRUMENT, index * 60000, (index + 1) * 60000, price, price, price, price, Decimal(1)
    )
    end = candle.close_time_ms
    inputs = []
    flow = source["trade_flow"] if flow == "fixture" else flow
    metric = source["metric"] if metric == "fixture" else metric
    if flow is not None:
        inputs.append(
            AuxiliaryInput(
                channel="trade_flow",
                values=tuple(zip(FLOW_FIELDS, map(Decimal, flow), strict=True)),
                window_start_ms=candle.open_time_ms,
                window_end_ms=end,
                event_time_ms=end,
                available_at_ms=end,
                source="frozen_flow",
            )
        )
    if metric is not None:
        event = source["metric_event_ms"] if event_time_ms is None else event_time_ms
        inputs.append(
            AuxiliaryInput(
                channel="metric",
                values=tuple(
                    (key, Decimal(value))
                    for key, value in zip(METRIC_FIELDS, metric, strict=True)
                    if value is not None
                ),
                window_start_ms=candle.open_time_ms,
                window_end_ms=end,
                event_time_ms=event,
                available_at_ms=max(event, end),
                coverage=coverage,
                quality=metric_quality
                or ("carried" if event < candle.open_time_ms else "observed"),
                source="frozen_metric",
            )
        )
    return MarketFeatureFrame(
        candle=candle, as_of_ms=end if as_of_ms is None else as_of_ms, inputs=tuple(inputs)
    )


def configuration(case):
    return SentimentConfiguration(
        **{key: case[key] for key in ("interval_id", "nSamples", "quote_precision")}
    )


@pytest.mark.parametrize(
    "case", REFERENCE["cases"], ids=lambda case: f"i{case['interval_id']}-n{case['nSamples']}"
)
def test_1440_frozen_original_mst_previews_and_closed_raw_states(case):
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, (configuration(case),))
    for index, expected in enumerate(case["expected"]):
        before = provider.state["kernels"]
        result = provider.update_frame(frame(index))
        assert {key: result[key] for key in expected} == expected
        closed = (index + 1) % case["minutes"] == 0
        assert result[f"CLOSED_{case['interval_id']}"] is closed
        if closed:
            actual_raw = provider.state["kernels"][(case["interval_id"], 0)]["previous"]
        else:
            assert provider.state["kernels"] == before
            kernel = provider._kernels[(case["interval_id"], 0)]
            bucket = provider._buckets[provider._bucket_key(kernel.config)]
            actual_raw = kernel.preview(
                bucket.price, bucket.metric, bucket.flow if bucket.flow_complete else None
            )
        assert_original_raw(actual_raw, case["raw"][index])
        provider.validate_checkpoint()


@pytest.mark.parametrize("value", [1.0, -1.0])
def test_unrounded_log_comparison_has_a_hard_16_ulp_boundary(value):
    direction = math.copysign(math.inf, value)
    for name in LOG_CHAIN_FIELDS:
        assert_original_raw({name: math.nextafter(value, direction, steps=16)}, {name: value})
        with pytest.raises(AssertionError):
            assert_original_raw({name: math.nextafter(value, direction, steps=17)}, {name: value})


@pytest.mark.parametrize(
    "name,actual,expected",
    [
        ("PRICE", math.nextafter(1.0, math.inf), 1.0),
        ("LS_DREL", math.nextafter(1.0, math.inf), 1.0),
        ("mode", 2, 1),
        ("LSLOG_MA", 1, 1.0),
        ("LSLOG_MA", 0.0, None),
        ("LSLOG_MA", math.nan, 1.0),
        ("LS_DABSMA", math.inf, 1.0),
        ("LSLOG_MA", 1.00000001, 1.0),
    ],
)
def test_log_chain_budget_never_hides_other_field_type_none_or_nonfinite_faults(
    name, actual, expected
):
    with pytest.raises(AssertionError):
        assert_original_raw({name: actual}, {name: expected})


def test_raw_comparison_still_requires_the_exact_original_schema():
    with pytest.raises(AssertionError):
        assert_original_raw({"LSLOG_MA": 1.0, "extra": 0}, {"LSLOG_MA": 1.0})


@pytest.mark.parametrize("split", [0, 1, 6, 14, 36, 82, 116])
def test_data_only_state_restores_across_seed_hold_partial_and_native_metrics(split):
    definitions = tuple(
        replace(configuration(case), line_index=int(case["nSamples"] == 7))
        for case in REFERENCE["cases"]
    )
    original = LegacyMarketSentimentFeatures(INSTRUMENT, definitions)
    for index in range(split):
        original.update_frame(frame(index))
    saved = json.loads(json.dumps(original.checkpoint()))
    resumed = LegacyMarketSentimentFeatures(INSTRUMENT, definitions).restored(saved, INSTRUMENT)
    for index in range(split, 180):
        assert resumed.update_frame(frame(index)) == original.update_frame(frame(index))
        assert resumed.state == original.state
        resumed.validate_checkpoint()


def test_native_metric_fresh_carry_is_usable_but_later_cutoff_only_changes_quality():
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(0, 2),))
    for index in range(5):
        result = provider.update_frame(frame(index))
        assert provider.input_ready
    assert result["0_MST_0_MST"] is not None
    assert provider.input_quality["0_MST_0"]["metric"] == "carried"
    before = provider.state
    later = replace(frame(4), as_of_ms=330001)
    assert provider.update_frame(later) == result
    assert not provider.input_ready
    assert provider.input_quality["0_MST_0"]["metric"] == "stale"
    assert provider.state["kernels"] == before["kernels"]
    assert provider.state["buckets"] == before["buckets"]
    assert provider.state["calculated_asof_ms"] == before["calculated_asof_ms"]
    provider.validate_checkpoint()
    with pytest.raises(ValidationError, match="backwards"):
        provider.update_frame(frame(4))


def test_configured_metric_age_is_independent_per_line_and_zero_is_not_valid_metric():
    provider = LegacyMarketSentimentFeatures(
        INSTRUMENT,
        (
            SentimentConfiguration(0, 2, maximum_metric_age_ms=60000),
            SentimentConfiguration(0, 2, line_index=1, maximum_metric_age_ms=330000),
        ),
    )
    provider.update_frame(frame(0))
    provider.update_frame(frame(1))
    assert not provider.input_quality["0_MST_0"]["ready"]
    assert provider.input_quality["0_MST_1"]["ready"]
    provider.update_frame(frame(2, metric=("0", "-1"), event_time_ms=180000))
    assert all(row["metric"] == "nonpositive" for row in provider.input_quality.values())
    assert all(
        kernel["previous"]["OI"] > 0 and kernel["previous"]["LS"] > 0
        for kernel in provider.state["kernels"].values()
    )


def test_first_missing_metric_never_becomes_zero_or_a_seeded_average():
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(0, 2),))
    for index in range(3):
        assert provider.update_frame(frame(index, metric=None))["0_MST_0_MST"] is None
    raw = provider.state["kernels"][(0, 0)]["previous"]
    assert raw["OI"] is None and raw["LS"] is None and raw["mode"] == 0
    for index in range(3, 6):
        result = provider.update_frame(frame(index))
    assert result["0_MST_0_MST"] is not None


def test_zero_taker_notionals_are_real_observations_and_have_finite_zero_mst():
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(0, 2),))
    for index in range(3):
        current = frame(index, metric=("100", "1"), flow=("0", "0"))
        price = Decimal(10)
        current = replace(
            current, candle=replace(current.candle, open=price, high=price, low=price, close=price)
        )
        result = provider.update_frame(current)
    assert result["0_MST_0_MST"] == 0
    assert provider.input_ready


def test_missing_trade_minute_never_seeds_a_partial_higher_bucket():
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(1, 2),))
    for index in range(9):
        provider.update_frame(frame(index, flow=None if index == 1 else "fixture"))
    raw = provider.state["kernels"][(1, 0)]["previous"]
    assert raw["mode"] == 1 and raw["MST"] is None  # First two complete sums only at buckets1,2.
    for index in range(9, 12):
        result = provider.update_frame(frame(index))
    assert result["1_MST_0_MST"] is not None


def test_incomplete_trade_sum_holds_taker_ema_without_committing_a_smaller_amount():
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(1, 2),))
    for index in range(9):
        provider.update_frame(frame(index))
    before = provider.state["kernels"][(1, 0)]["previous"]
    for index in range(9, 12):
        result = provider.update_frame(frame(index, flow=None if index == 10 else "fixture"))
    after = provider.state["kernels"][(1, 0)]["previous"]
    assert after["NB"] is None and after["NS"] is None
    assert after["NB_MA"] == before["NB_MA"] and after["NS_MA"] == before["NS_MA"]
    assert result["1_MST_0_MST"] is not None and not provider.input_ready
    provider.validate_checkpoint()


def test_missing_or_ohlcv_only_input_is_explicitly_unready():
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(0, 2),))
    for index in range(5):
        result = provider.update(frame(index).candle)
    assert result["0_MST_0_MST"] is None and not provider.input_ready
    assert provider.input_quality["0_MST_0"]["metric"] == "missing"
    assert provider.input_quality["0_MST_0"]["trade_flow"] == "missing"


def test_initial_partial_prefix_does_not_seed_or_commit():
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(2, 2),))
    for index in range(2, 5):
        assert provider.update_frame(frame(index))["2_MST_0_MST"] is None
        assert not provider.state["kernels"][(2, 0)]["history"]
        provider.validate_checkpoint()


@pytest.mark.parametrize(
    "changes",
    [
        {"metric_quality": "partial", "coverage": Decimal(".5")},
        {"metric": (None, "1")},
        {"event_time_ms": 60001, "as_of_ms": 60001},
    ],
)
def test_partial_missing_field_or_future_metric_is_not_ready(changes):
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(0, 2),))
    provider.update_frame(frame(0, **changes))
    assert not provider.input_ready
    provider.validate_checkpoint()


def test_duplicate_changed_input_gap_and_overflow_preserve_state_atomically():
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(0, 2),))
    result = provider.update_frame(frame(0))
    before = provider.state
    assert provider.update_frame(frame(0)) == result and provider.state == before
    for invalid in (
        frame(0, metric=("123", "1")),
        frame(2),
        frame(1, flow=("1e999", "0")),
        frame(1, metric=("1e999", "1")),
    ):
        with pytest.raises(ValidationError):
            provider.update_frame(invalid)
        assert provider.state == before
    exposed = provider.state
    exposed["kernels"].clear()
    provider.input_quality["0_MST_0"]["ready"] = False
    assert provider.state == before


@pytest.mark.parametrize(
    "changes",
    [
        {"interval_id": True},
        {"interval_id": 12},
        {"nSamples": True},
        {"nSamples": 1},
        {"nSamples": 20001},
        {"line_index": 10},
        {"quote_precision": 17},
        {"maximum_metric_age_ms": True},
        {"maximum_metric_age_ms": -1},
        {"maximum_metric_age_ms": 86400001},
    ],
)
def test_invalid_configuration(changes):
    with pytest.raises(ValidationError):
        SentimentConfiguration(**({"interval_id": 0} | changes))


def test_provider_budget_and_duplicate_outputs_are_rejected():
    definition = SentimentConfiguration(0, 2)
    with pytest.raises(ValidationError):
        LegacyMarketSentimentFeatures(INSTRUMENT, (definition, definition))
    with pytest.raises(ValidationError):
        LegacyMarketSentimentFeatures(
            INSTRUMENT, (SentimentConfiguration(0, 20000), SentimentConfiguration(1, 2))
        )


def pair_field(encoded, name):
    return next(pair for pair in encoded["pairs"] if pair[0] == name)


def shift_frame_times(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "as_of_ms":
                value[key] += 60 * 60000
            else:
                shift_frame_times(item)
    elif isinstance(value, list):
        if (
            len(value) == 2
            and isinstance(value[0], str)
            and value[0]
            in {
                "open_time_ms",
                "close_time_ms",
                "window_start_ms",
                "window_end_ms",
                "event_time_ms",
                "available_at_ms",
            }
        ):
            value[1] += 60 * 60000
        else:
            for item in value:
                shift_frame_times(item)


@pytest.mark.parametrize(
    "case",
    [
        "mode",
        "negative_oi",
        "history_scalar",
        "history_bound",
        "taker_relative",
        "delta_strength",
        "negative_absolute",
        "bounded_result",
        "bucket_shape",
        "bucket_price",
        "bucket_time",
        "bucket_bool_key",
        "bucket_partial",
        "bucket_flow_flag",
        "missing_frame",
        "shifted_frame",
        "calculated_time",
        "cached_value",
        "cached_boolean",
        "cached_key",
        "closure",
        "quality_flag",
        "quality_age_type",
        "quality_shape",
        "quality_metric",
    ],
)
def test_valid_checksum_semantic_tamper_is_rejected_atomically(case):
    definitions = (SentimentConfiguration(0, 2), SentimentConfiguration(1, 3))
    provider = LegacyMarketSentimentFeatures(INSTRUMENT, definitions)
    for index in range(30):
        provider.update_frame(frame(index))
    saved = deepcopy(provider.checkpoint())
    state = saved["payload"]["state"]
    kernel = state["_kernels"][0][1]["state"]
    previous = kernel["previous"]
    bucket = state["_buckets"]["pairs"][0][1]["sentiment_bucket"]
    if case == "mode":
        pair_field(previous, "mode")[1] = 2
    elif case == "negative_oi":
        pair_field(previous, "OI")[1] = -1
    elif case == "history_scalar":
        pair_field(kernel["history"]["deque"][-1], "OI")[1] = "nan"
    elif case == "history_bound":
        kernel["history"]["maxlen"] = 999
    elif case == "taker_relative":
        pair_field(previous, "TAKER_REL")[1] = 987
    elif case == "delta_strength":
        pair_field(previous, "PRICE_STR")[1] = 987
    elif case == "negative_absolute":
        pair_field(previous, "OI_DABSMA")[1] = -1
    elif case == "bounded_result":
        pair_field(previous, "MST")[1] = 987
    elif case == "bucket_shape":
        pair_field(bucket, "flow")[1] = {"tuple": [1]}
    elif case == "bucket_price":
        pair_field(bucket, "price")[1] = 987
    elif case == "bucket_time":
        pair_field(bucket, "open_time_ms")[1] += 60000
    elif case == "bucket_bool_key":
        state["_buckets"]["pairs"][0][0]["tuple"][0] = False
    elif case == "bucket_partial":
        pair_field(bucket, "valid_prefix")[1] = False
    elif case == "bucket_flow_flag":
        pair_field(bucket, "flow_complete")[1] = False
    elif case == "missing_frame":
        state["_last_frame"] = None
    elif case == "shifted_frame":
        shift_frame_times(state["_last_frame"])
    elif case == "calculated_time":
        state["_calculated_asof_ms"] += 60000
    elif case == "cached_value":
        pair_field(state["_result"], "0_MST_0_MST")[1] = 987
    elif case == "cached_boolean":
        pair_field(state["_result"], "0_MST_0_MST")[1] = False
    elif case == "cached_key":
        state["_result"]["pairs"].append(["FAKE", 1])
    elif case == "closure":
        pair_field(state["_result"], "CLOSED_0")[1] = False
    else:
        quality = state["_input_quality"]["pairs"][0][1]
        if case == "quality_flag":
            pair_field(quality, "ready")[1] = False
        elif case == "quality_age_type":
            pair_field(quality, "metric_age_ms")[1] = True
        elif case == "quality_shape":
            quality["pairs"].append(["extra", 1])
        else:
            pair_field(quality, "metric")[1] = "observed"
    saved["sha256"] = sha256(
        json.dumps(
            saved["payload"], sort_keys=True, allow_nan=False, separators=(",", ":")
        ).encode()
    ).hexdigest()
    before = provider.checkpoint()
    with pytest.raises(ValidationError):
        provider.restored(saved, INSTRUMENT)
    assert provider.checkpoint() == before


def test_later_asof_staleness_and_incomplete_flow_remain_exact_after_json_restore():
    definitions = (SentimentConfiguration(1, 2),)
    original = LegacyMarketSentimentFeatures(INSTRUMENT, definitions)
    for index in range(9):
        original.update_frame(frame(index, flow=None if index == 7 else "fixture"))
    delayed = replace(frame(8), as_of_ms=frame(8).as_of_ms + 86400000)
    original.update_frame(delayed)
    assert not original.input_ready
    saved = json.loads(json.dumps(original.checkpoint()))
    recovered = LegacyMarketSentimentFeatures(INSTRUMENT, definitions).restored(saved, INSTRUMENT)
    assert recovered.state == original.state
    assert recovered.input_quality["1_MST_0"]["metric"] == "stale"
    assert recovered.input_quality["1_MST_0"]["trade_flow"] == "incomplete"
    for index in range(9, 19):
        current = replace(frame(index), as_of_ms=frame(index).as_of_ms + 86400000)
        assert recovered.update_frame(current) == original.update_frame(current)
        assert recovered.state == original.state
    assert not recovered.input_ready


def test_partial_prefix_missing_and_zero_metric_seed_states_have_valid_checkpoints():
    definitions = (SentimentConfiguration(2, 2),)
    original = LegacyMarketSentimentFeatures(INSTRUMENT, definitions)
    for index in (None, 2, 3, 4):
        if index is not None:
            original.update_frame(frame(index, metric=("0", "0"), flow=None))
        saved = json.loads(json.dumps(original.checkpoint()))
        recovered = LegacyMarketSentimentFeatures(INSTRUMENT, definitions).restored(
            saved, INSTRUMENT
        )
        assert recovered.state == original.state
