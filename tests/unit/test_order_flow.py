"""Frozen original AST arithmetic and minute aggregation; no market network or credentials."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from omega.analysis.order_flow import (
    DEPTH_FIELDS,
    FLOW_FIELDS,
    LegacyOrderFlowFeatures,
    OrderFlowConfiguration,
)
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

REFERENCE = json.loads(Path("tests/fixtures/order-flow-legacy.json").read_text())
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def candle(index):
    return Candle(
        INSTRUMENT,
        index * 60000,
        (index + 1) * 60000,
        *(Decimal(value) for value in ("10", "12", "8", "11", "4")),
    )


def observation(index, channel, values, **changes):
    fields = FLOW_FIELDS if channel == "trade_flow" else DEPTH_FIELDS
    return AuxiliaryInput(
        **{
            "channel": channel,
            "values": tuple(zip(fields, map(Decimal, values), strict=True)),
            "window_start_ms": index * 60000,
            "window_end_ms": (index + 1) * 60000,
            "event_time_ms": (index + 1) * 60000,
            "available_at_ms": (index + 1) * 60000,
            "source": "frozen_observation",
            **changes,
        }
    )


def frame(index):
    return MarketFeatureFrame(
        candle=candle(index),
        as_of_ms=(index + 1) * 60000,
        inputs=tuple(
            observation(index, channel, values)
            for channel, values in REFERENCE["inputs"][index].items()
            if values is not None
        ),
    )


def config(case):
    return OrderFlowConfiguration(
        **{
            key: case[key]
            for key in ("interval_id", "analysis_code", "nSamples", "quote_precision")
        }
    )


@pytest.mark.parametrize(
    "case",
    REFERENCE["cases"],
    ids=lambda row: f"{row['analysis_code']}-i{row['interval_id']}-n{row['nSamples']}",
)
def test_frozen_original_normal_arithmetic_and_explicit_higher_flow_gap_deviation(case):
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (config(case),))
    key = (case["interval_id"], case["analysis_code"], 0)
    higher_flow = case["analysis_code"] == "NES" and case["minutes"] > 1
    tainted, deviations = False, 0
    for index, expected in enumerate(case["expected"]):
        before = provider.state["kernels"]
        result = provider.update_frame(frame(index))
        # Frozen legacy aggregation silently used partial sums after a missing
        # constituent minute. Preserve its normal-data oracle, not that bug or
        # its recursive EMA descendants. The fixture itself remains unchanged.
        if higher_flow and REFERENCE["inputs"][index].get("trade_flow") is None:
            tainted = True
        actual = {field: result[field] for field in expected}
        if not tainted:
            assert actual == expected
        else:
            deviations += actual != expected
        complete = (index + 1) % case["minutes"] == 0
        assert result[f"CLOSED_{case['interval_id']}"] is complete
        if complete:
            previous = provider.state["kernels"][key]["previous"]
            if not tainted:
                assert previous == case["raw"][index]
            else:
                start = index + 1 - case["minutes"]
                missing = any(
                    REFERENCE["inputs"][minute].get("trade_flow") is None
                    for minute in range(start, index + 1)
                )
                assert previous["NETNOTIONAL"] == (
                    None if missing else case["raw"][index]["NETNOTIONAL"]
                )
            provider._kernels[key].validate_checkpoint()
        else:
            assert provider.state["kernels"] == before
    if higher_flow:
        assert deviations > 0  # Intentional, visible bug fix rather than silent parity relaxation.


@pytest.mark.parametrize("split", [0, 1, 5, 26, 39, 100, 131])
def test_data_only_partial_seed_preview_and_missing_state_restores_exactly(split):
    definitions = tuple(
        replace(config(case), line_index=int(case["nSamples"] == 7)) for case in REFERENCE["cases"]
    )
    original = LegacyOrderFlowFeatures(INSTRUMENT, definitions)
    for index in range(split):
        original.update_frame(frame(index))
    saved = json.loads(json.dumps(original.checkpoint()))
    resumed = LegacyOrderFlowFeatures(INSTRUMENT, definitions).restored(saved, INSTRUMENT)
    for index in range(split, 180):
        assert resumed.update_frame(frame(index)) == original.update_frame(frame(index))
        assert resumed.state == original.state
        assert resumed.input_ready == original.input_ready
        resumed.validate_checkpoint()


@pytest.mark.parametrize("code,channel", [("NES", "trade_flow"), ("WOI", "depth")])
def test_missing_after_seed_may_hold_but_always_reports_unready(code, channel):
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (OrderFlowConfiguration(0, code, 2),))
    for index in range(3):
        provider.update_frame(frame(index))
    old = provider._result[f"0_{code}_0_{code}"]
    result = provider.update(candle(3))
    assert result[f"0_{code}_0_{code}"] == old
    assert provider.input_ready is False
    assert provider.input_quality[f"0_{code}_0"] == {
        "channel": channel,
        "ready": False,
        "reason": "missing",
    }


def test_ohlcv_only_does_not_invent_zero_notionals():
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (OrderFlowConfiguration(0, "NES", 2),))
    for index in range(20):
        assert provider.update(candle(index))["0_NES_0_NES"] is None
    assert not provider.input_ready
    assert provider.state["kernels"][(0, "NES", 0)]["history"] == (None,)


def test_exact_zero_is_valid_and_relative_zero_is_finite():
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (OrderFlowConfiguration(0, "NES", 2),))
    for index in range(3):
        sample = observation(index, "trade_flow", ("0", "0"))
        result = provider.update_frame(
            MarketFeatureFrame(candle=candle(index), as_of_ms=(index + 1) * 60000, inputs=(sample,))
        )
    assert result["0_NES_0_NES"] == 0
    assert result["0_NES_0_NESABSMAREL"] == 0
    assert provider.input_ready


def test_initial_partial_prefix_never_commits_reference_state():
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (OrderFlowConfiguration(2, "NES", 2),))
    for index in range(2, 5):
        assert provider.update_frame(frame(index))["2_NES_0_NES"] is None
        assert not provider.input_ready
        assert not provider.state["kernels"][(2, "NES", 0)]["history"]
    for index in range(5, 15):
        result = provider.update_frame(frame(index))
    # The fixture is also missing minute5: bucket5..9 must not seed a partial
    # sum merely because this initial-prefix test has reached the next bucket.
    assert result["2_NES_0_NES"] is None
    for index in range(15, 20):
        result = provider.update_frame(frame(index))
    assert result["2_NES_0_NES"] is not None


def test_flow_sum_and_last_valid_depth_have_separate_quality_rules():
    provider = LegacyOrderFlowFeatures(
        INSTRUMENT,
        (
            OrderFlowConfiguration(1, "NES", 2),
            OrderFlowConfiguration(1, "WOI", 2),
        ),
    )
    for index in range(3):
        if index == 1:
            sample = replace(frame(index), inputs=())
        else:
            sample = frame(index)
        provider.update_frame(sample)
    assert not provider.input_quality["1_NES_0"]["ready"]  # Incomplete sum, no fabricated minute.
    assert provider.input_quality["1_WOI_0"]["ready"]  # Latest actual snapshot is available.


def flow_frame(index, net):
    inputs = () if net is None else (observation(index, "trade_flow", (str(net), "0")),)
    return MarketFeatureFrame(candle=candle(index), as_of_ms=(index + 1) * 60000, inputs=inputs)


@pytest.mark.parametrize("split", [0, 2, 3, 5, 8])
def test_missing_higher_flow_sum_never_seeds_next_valid_bucket_and_restores_midway(split):
    definition = OrderFlowConfiguration(1, "NES", 2)
    samples = (1000, None, 1000, 10, 10, 10, 20, 20, 20)
    original = LegacyOrderFlowFeatures(INSTRUMENT, (definition,))
    frames = [flow_frame(index, value) for index, value in enumerate(samples)]
    expected = []
    for sample in frames:
        expected.append(original.update_frame(sample))
        original.validate_checkpoint()
    key, output = (1, "NES", 0), "1_NES_0_NES"
    assert expected[2][output] is None and expected[5][output] is None
    assert expected[8][output] == 45  # Seed only the two complete sums30 and60, not partial2000.

    interrupted = LegacyOrderFlowFeatures(INSTRUMENT, (definition,))
    for sample in frames[:split]:
        interrupted.update_frame(sample)
    saved = json.loads(json.dumps(interrupted.checkpoint()))
    resumed = LegacyOrderFlowFeatures(INSTRUMENT, (definition,)).restored(saved, INSTRUMENT)
    for index, sample in enumerate(frames[split:], start=split):
        assert resumed.update_frame(sample) == expected[index]
        resumed.validate_checkpoint()
        if index == 2:
            # Keep the received partial sum for bucket validation/evidence only.
            assert resumed.state["buckets"][(1, "NES", 8)]["values"] == (2000, 0)
            assert resumed.state["kernels"][key]["history"] == (None,)
            assert not resumed.input_ready
        if index == 5:
            assert resumed.input_ready  # Input is valid; numerical warmup is a separate condition.
            assert resumed.state["kernels"][key]["history"] == (30,)
    assert resumed.state == original.state


@pytest.mark.parametrize("missing_minute", [6, 7, 8])
def test_seeded_higher_flow_gap_holds_ema_and_next_bucket_uses_only_complete_sum(missing_minute):
    definition = OrderFlowConfiguration(1, "NES", 2)
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (definition,))
    for index in range(6):
        seeded = provider.update_frame(flow_frame(index, 10))
    assert seeded["1_NES_0_NES"] == 30
    for index in range(6, 9):
        value = None if index == missing_minute else 1000
        result = provider.update_frame(flow_frame(index, value))
        provider.validate_checkpoint()
        if index >= missing_minute:
            assert result["1_NES_0_NES"] == 30
            assert not provider.input_ready
    state = provider.state["kernels"][(1, "NES", 0)]
    assert state["history"] == (None,)
    assert state["previous"]["NETNOTIONAL"] is None
    assert provider.state["buckets"][(1, "NES", 8)]["values"] == (2000, 0)
    restored = provider.restored(json.loads(json.dumps(provider.checkpoint())), INSTRUMENT)
    for index in range(9, 12):
        sample = flow_frame(index, 20)
        result = provider.update_frame(sample)
        assert restored.update_frame(sample) == result
        restored.validate_checkpoint()
    assert result["1_NES_0_NES"] == 50  # alpha2/3 of complete60 plus prior30/3.
    assert provider.input_ready


def test_restored_partial_flow_cannot_claim_legacy_partial_sum_as_committed_raw():
    definition = OrderFlowConfiguration(1, "NES", 2)
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (definition,))
    for index, value in enumerate((1000, None, 1000)):
        provider.update_frame(flow_frame(index, value))
    kernel = provider._kernels[(1, "NES", 0)]
    kernel.history[-1] = 2000
    kernel.previous["NETNOTIONAL"] = 2000
    # This was the old coherent-but-contaminated committed scalar. The retained
    # incomplete bucket proves it must instead be None; never silently accept it.
    saved = json.loads(json.dumps(provider.checkpoint()))
    with pytest.raises(ValidationError, match="bucket disagree"):
        LegacyOrderFlowFeatures(INSTRUMENT, (definition,)).restored(saved, INSTRUMENT)


@pytest.mark.parametrize("quality,coverage", [("partial", ".5"), ("carried", "1")])
def test_partial_or_carried_input_never_seeds_as_a_new_observation(quality, coverage):
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (OrderFlowConfiguration(0, "NES", 2),))
    for index in range(3):
        sample = observation(
            index, "trade_flow", ("100", "30"), quality=quality, coverage=Decimal(coverage)
        )
        result = provider.update_frame(
            MarketFeatureFrame(candle=candle(index), as_of_ms=(index + 1) * 60000, inputs=(sample,))
        )
    assert result["0_NES_0_NES"] is None and not provider.input_ready


def test_stale_input_and_missing_required_fields_do_not_seed_or_look_ready():
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (OrderFlowConfiguration(0, "NES", 2),))
    for index in range(1, 4):
        sample = observation(index, "trade_flow", ("100", "30"), event_time_ms=0)
        result = provider.update_frame(
            MarketFeatureFrame(candle=candle(index), as_of_ms=(index + 1) * 60000, inputs=(sample,))
        )
    assert result["0_NES_0_NES"] is None
    assert provider.input_quality["0_NES_0"]["reason"] == "stale"
    sample = replace(
        observation(4, "trade_flow", ("100", "30")), values=(("UNRELATED", Decimal(1)),)
    )
    provider.update_frame(
        MarketFeatureFrame(candle=candle(4), as_of_ms=5 * 60000, inputs=(sample,))
    )
    assert provider.input_quality["0_NES_0"]["reason"] == "missing_fields"


def test_duplicate_later_cutoff_is_idempotent_but_changed_input_and_gap_are_atomic():
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (OrderFlowConfiguration(0, "NES", 2),))
    value = provider.update_frame(frame(0))
    state = provider.state
    assert provider.update_frame(replace(frame(0), as_of_ms=120000)) == value
    assert provider.state == state
    changed = observation(0, "trade_flow", ("2", "1"))
    with pytest.raises(ValidationError, match="immutable"):
        provider.update_frame(replace(frame(0), inputs=(changed,)))
    with pytest.raises(ValidationError, match="contiguous"):
        provider.update_frame(frame(2))
    assert provider.state == state


@pytest.mark.parametrize("value", ["-1", "1e9999"])
def test_bad_notional_failure_is_atomic_across_providers(value):
    provider = LegacyOrderFlowFeatures(
        INSTRUMENT,
        (
            OrderFlowConfiguration(0, "NES", 2),
            OrderFlowConfiguration(0, "WOI", 2),
        ),
    )
    provider.update_frame(frame(0))
    before = provider.state
    flow = observation(1, "trade_flow", ("2", "1"))
    depth = observation(1, "depth", [value] * 12)
    with pytest.raises(ValidationError, match="finite"):
        provider.update_frame(
            MarketFeatureFrame(candle=candle(1), as_of_ms=120000, inputs=(flow, depth))
        )
    assert provider.state == before
    assert provider.update_frame(frame(1)) == deepcopy(provider)._result


def test_state_and_quality_exposure_cannot_mutate_live_calculation():
    provider = LegacyOrderFlowFeatures(INSTRUMENT, (OrderFlowConfiguration(0, "NES", 2),))
    provider.update_frame(frame(0))
    before = provider.state
    provider.state["kernels"].clear()
    provider.input_quality["0_NES_0"]["ready"] = False
    assert provider.state == before


@pytest.mark.parametrize(
    "changes",
    [
        {"interval_id": True},
        {"interval_id": 12},
        {"analysis_code": "MST"},
        {"nSamples": True},
        {"nSamples": 1},
        {"nSamples": 100001},
        {"line_index": 10},
        {"quote_precision": 17},
    ],
)
def test_invalid_configuration(changes):
    with pytest.raises(ValidationError):
        OrderFlowConfiguration(**({"interval_id": 0, "analysis_code": "NES"} | changes))


def test_duplicate_configuration_is_rejected():
    definition = OrderFlowConfiguration(0, "NES", 2)
    with pytest.raises(ValidationError):
        LegacyOrderFlowFeatures(INSTRUMENT, (definition, definition))


def test_total_seed_budget_is_checked_before_provider_allocation():
    with pytest.raises(ValidationError, match="bounded"):
        LegacyOrderFlowFeatures(
            INSTRUMENT,
            (
                OrderFlowConfiguration(0, "NES", 50001),
                OrderFlowConfiguration(0, "WOI", 50000),
            ),
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
        "history_scalar",
        "history_bound",
        "bucket_shape",
        "bucket_time",
        "bucket_bool_key",
        "bucket_partial",
        "shifted_frame",
        "missing_frame",
        "cached_value",
        "cached_boolean",
        "cached_key",
        "closure",
        "quality_flag",
        "quality_shape",
        "quality_reason",
        "relative",
        "negative_absolute",
    ],
)
def test_valid_checksum_semantic_tamper_is_rejected_without_mutating_active_provider(case):
    definitions = (OrderFlowConfiguration(0, "NES", 2), OrderFlowConfiguration(1, "WOI", 3))
    provider = LegacyOrderFlowFeatures(INSTRUMENT, definitions)
    for index in range(36):
        provider.update_frame(frame(index))
    saved = deepcopy(provider.checkpoint())
    state = saved["payload"]["state"]
    kernel = state["_kernels"][0][1]["state"]
    bucket = state["_buckets"]["pairs"][0][1]["order_flow_bucket"]
    if case == "mode":
        pair_field(kernel["previous"], "mode")[1] = 2
    elif case == "history_scalar":
        kernel["history"]["deque"][-1] = "not-a-number"
    elif case == "history_bound":
        kernel["history"]["maxlen"] = 999
    elif case == "bucket_shape":
        pair_field(bucket, "values")[1] = {"tuple": [1]}
    elif case == "bucket_time":
        pair_field(bucket, "open_time_ms")[1] += 60000
    elif case == "bucket_bool_key":
        state["_buckets"]["pairs"][0][0]["tuple"][0] = False
    elif case == "bucket_partial":
        pair_field(bucket, "valid_prefix")[1] = False
    elif case == "shifted_frame":
        shift_frame_times(state["_last_frame"])
    elif case == "missing_frame":
        state["_last_frame"] = None
    elif case == "cached_value":
        pair_field(state["_result"], "0_NES_0_NES")[1] = 987654321
    elif case == "cached_boolean":
        pair_field(state["_result"], "0_NES_0_NES")[1] = False
    elif case == "cached_key":
        state["_result"]["pairs"].append(["FAKE", 1])
    elif case == "closure":
        pair_field(state["_result"], "CLOSED_0")[1] = False
    elif case.startswith("quality"):
        quality = state["_input_quality"]["pairs"][0][1]
        if case == "quality_flag":
            pair_field(quality, "ready")[1] = False
        elif case == "quality_shape":
            quality["pairs"].append(["extra", 1])
        else:
            pair_field(quality, "reason")[1] = "made_up"
    elif case == "relative":
        pair_field(kernel["previous"], "NES_ABSMAREL")[1] = 900
    else:
        pair_field(kernel["previous"], "NES_ABSMA")[1] = -1
    saved["sha256"] = sha256(
        json.dumps(
            saved["payload"], sort_keys=True, allow_nan=False, separators=(",", ":")
        ).encode()
    ).hexdigest()
    before = provider.checkpoint()
    with pytest.raises(ValidationError):
        provider.restored(saved, INSTRUMENT)
    assert provider.checkpoint() == before


def test_uninitialized_and_missing_partial_prefix_checkpoints_are_valid():
    definitions = (OrderFlowConfiguration(2, "WOI", 3),)
    provider = LegacyOrderFlowFeatures(INSTRUMENT, definitions)
    for index in (None, 2, 3, 4):
        if index is not None:
            provider.update(candle(index))
        saved = json.loads(json.dumps(provider.checkpoint()))
        restored = LegacyOrderFlowFeatures(INSTRUMENT, definitions).restored(saved, INSTRUMENT)
        assert restored.state == provider.state
        restored.validate_checkpoint()
