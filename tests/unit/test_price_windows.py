"""Frozen actual Eta AST results, causal preview and exact window recovery."""

import json
import math
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from omega.analysis.aggregation import INTERVAL_MINUTES, CandleAggregation
from omega.analysis.price_windows import (
    BollingerConfiguration,
    LegacyPriceWindowFeatures,
    WindowAverageConfiguration,
)
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

FIXTURES = Path("tests/fixtures/eta_price_windows")
REFERENCE = json.loads((FIXTURES / "cases.json").read_text())
INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def json_form(value):
    return json.loads(json.dumps(value, allow_nan=False))


def candle(minute, close="100.125"):
    price = Decimal(close)
    return Candle(
        INSTRUMENT, minute * 60000, (minute + 1) * 60000, price, price, price, price, Decimal(10)
    )


def provider(*configs):
    return LegacyPriceWindowFeatures(
        INSTRUMENT, configs or (WindowAverageConfiguration(0, "SMA", 2),)
    )


def configuration(row):
    kwargs = {key: row[key] for key in ("interval_id", "nSamples", "line_index", "price_precision")}
    if row["family"] == "BOL":
        return BollingerConfiguration(**kwargs, MAType=row["MAType"], bandWidth=row["bandWidth"])
    return WindowAverageConfiguration(**kwargs, analysis_code=row["family"])


def test_frozen_oracle_precedes_implementation_and_records_actual_sources():
    assert (
        sha256((FIXTURES / "cases.json").read_bytes()).hexdigest()
        == "3a0a581487282fbcca5c5142a95b4db8b4db6a8f589764fc14471bcddde5f9e0"
    )
    assert (
        sha256((FIXTURES / "manifest.json").read_bytes()).hexdigest()
        == "55c289d2f3d4322ffbc320fe6a981a4e95e97ac39f84d827a4e8d75cef0013e6"
    )
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    provenance = json.loads((FIXTURES / "provenance.json").read_text())
    assert manifest["case_count"] == len(REFERENCE["cases"]) == 72
    assert manifest["trace_count"] == 5736 and manifest["negative_band_inversions"] == 430
    assert set(provenance) == {"SMA", "WMA", "BOL", "helpers", "constants"}
    for family in ("SMA", "WMA", "BOL"):
        assert set(provenance[family]["segment_sha256"]) == {"generate", "linearize"}
        assert provenance[family]["source_sha256"] == manifest["source_hashes"][family]
    assert set(provenance["helpers"]["segment_sha256"]) == {
        "getTimestampList_byNTicks",
        "getNextIntervalTickTimestamp",
    }


@pytest.mark.parametrize("case", REFERENCE["cases"], ids=lambda case: case["name"])
def test_every_raw_public_and_committed_window_equals_frozen_actual_eta(case):
    config = configuration(case["configuration"])
    current = provider(config)
    key = (config.interval_id, case["configuration"]["family"], config.line_index)
    closed, closes = [], []
    for expected in case["trace"]:
        minute = expected["minute"]
        close = REFERENCE["inputs"][case["configuration"]["pattern"]][minute]["close"]
        before = current.state["kernels"]
        result = current.update(candle(minute, close))
        assert result.pop(f"CLOSED_{config.interval_id}") is expected["closed"]
        assert json_form(current.state["raw"][key]) == expected["raw"], (case["name"], minute)
        assert result == (
            expected["public"] if expected["public"] is not None else dict.fromkeys(result)
        )
        if expected["closed"]:
            closed.append(expected["raw"])
            closes.append(float(close))
        else:
            assert current.state["kernels"] == before
        kernel = current.state["kernels"][key]
        assert json_form(kernel["history"]) == closed[-2:]
        assert json_form(kernel["previous"]) == (closed[-1] if closed else {})
        assert kernel["closes"] == tuple(closes[-config.nSamples - 1 :])
        assert kernel["count"] == len(closed)
        current.validate_checkpoint()


def test_shared_aggregation_frame_adapter_mixed_identity_and_detached_state():
    configs = (
        WindowAverageConfiguration(1, "SMA", 2, 0),
        WindowAverageConfiguration(1, "WMA", 2, 0),
        BollingerConfiguration(1, 2, "EMA", -2, 0),
    )
    direct, shared, framed = (provider(*configs) for _ in range(3))
    assert shared.uses_shared_aggregation is True
    aggregation = CandleAggregation(INSTRUMENT, (1,))
    for minute in range(32):
        row = candle(minute, str(100 + minute / 8))
        expected = direct.update(row)
        assert shared.update_with(row, {"bars": aggregation.update(row)}) == {
            key: value for key, value in expected.items() if not key.startswith("CLOSED_")
        }
        assert framed.update_frame(MarketFeatureFrame(row, row.close_time_ms)) == expected
        assert direct.state == shared.state == framed.state
        assert shared._aggregation._last is None and shared._aggregation._buckets == {}
        shared.validate_checkpoint()
        checkpoint = direct.checkpoint()
        assert direct.update(row) == expected and direct.checkpoint() == checkpoint
    detached = direct.state
    detached["kernels"][(1, "SMA", 0)]["previous"]["SMA"] = -99
    assert direct.state != detached
    with pytest.raises(ValidationError):
        direct.update_frame(candle(32))


@pytest.mark.parametrize("iid", INTERVAL_MINUTES)
def test_all_supported_intervals_commit_only_at_the_actual_boundary(iid):
    current = provider(WindowAverageConfiguration(iid, "WMA", 2, price_precision=16))
    period = INTERVAL_MINUTES[iid]
    for minute in range(2 * period):
        before = current.state["kernels"]
        result = current.update(candle(minute))
        complete = (minute + 1) % period == 0
        assert result[f"CLOSED_{iid}"] is complete
        if not complete:
            assert current.state["kernels"] == before
    assert current._kernels[(iid, "WMA", 0)].count == 2
    assert result[f"{iid}_WMA_0_WMA"] == 100.125
    current.validate_checkpoint()


@pytest.mark.parametrize("width", [-2.0, -0.0, 0.0, 0.5, 2.0])
def test_signed_zero_bandwidth_and_negative_lower_band_are_normal_outputs(width):
    current = provider(BollingerConfiguration(0, 2, bandWidth=width, price_precision=16))
    current.update(candle(0, "1"))
    result = current.update(candle(1, "100"))
    low, high, average = (result[f"0_BOL_0_{name}"] for name in ("BOLLOW", "BOLHIGH", "MA"))
    assert average == 50.5
    if width < 0:
        assert low > high
    elif width == 0:
        assert low == high == average
    elif width == 2:
        assert low < 0 < high
    current.validate_checkpoint()


@pytest.mark.parametrize(
    "fault", ["gap", "conflict", "instrument", "overflow", "underflow", "band_overflow"]
)
def test_invalid_input_and_arithmetic_overflow_leave_exact_checkpoint_unchanged(fault):
    current = provider(
        BollingerConfiguration(0, 2, bandWidth=1e308 if fault == "band_overflow" else 2)
    )
    current.update(candle(0, "1"))
    before = current.checkpoint()
    row = candle(2) if fault == "gap" else candle(1)
    if fault == "conflict":
        row = candle(0, "2")
    elif fault == "instrument":
        row = replace(row, instrument=Instrument("binance", Market.SPOT, "ETHUSDT"))
    elif fault == "overflow":
        row = candle(1, "1e1000")
    elif fault == "underflow":
        row = candle(1, "1e-1000")
    with pytest.raises(ValidationError):
        current.update(row)
    assert current.checkpoint() == before


@pytest.mark.parametrize("selected", ["SMA", "WMA", "EMA"])
def test_restore_every_seed_slide_preview_and_initial_partial_boundary(selected):
    configs = (WindowAverageConfiguration(0, "WMA", 2), BollingerConfiguration(2, 2, selected, -2))
    current = provider(*configs)
    for minute in range(1, 36):
        current.update(candle(minute, str(100 + minute / 7)))
        saved = json_form(current.checkpoint())
        restored = provider(*configs).restored(saved, INSTRUMENT)
        assert restored.checkpoint() == saved and restored.state == current.state
        future = candle(minute + 1, str(100 + (minute + 1) / 7))
        assert restored.update(future) == deepcopy(current).update(future)


@pytest.mark.parametrize(
    "fault",
    ["count", "sum", "history", "mode", "public", "raw", "frame", "closure", "configuration"],
)
def test_recomputed_checksum_cannot_authorize_inconsistent_checkpoint(fault):
    current = provider(BollingerConfiguration(1, 2, "WMA"))
    for minute in range(16):
        current.update(candle(minute, str(100 + minute / 7)))
    saved, damaged = current.checkpoint(), deepcopy(current)
    key = (1, "BOL", 0)
    kernel = damaged._kernels[key]
    if fault == "count":
        kernel.count += 1
    elif fault == "sum":
        simple, weighted = kernel.previous["MACOMPUTATION"]
        kernel.previous["MACOMPUTATION"] = (simple + 1, weighted)
        kernel.history[-1] = deepcopy(kernel.previous)
    elif fault == "history":
        kernel.closes.pop()
    elif fault == "mode":
        kernel.previous["mode"] = 0
        kernel.history[-1] = deepcopy(kernel.previous)
    elif fault == "public":
        damaged._result["1_BOL_0_MA"] += 1
    elif fault == "raw":
        damaged._raw[key]["MA"] += 1
    elif fault == "frame":
        damaged._frames[1] = replace(damaged._frames[1], open_time_ms=0)
    elif fault == "closure":
        damaged._result["CLOSED_1"] = True
    else:
        kernel.config = replace(kernel.config, nSamples=3)
    with pytest.raises(ValidationError):
        provider(BollingerConfiguration(1, 2, "WMA")).restored(damaged.checkpoint(), INSTRUMENT)
    assert current.checkpoint() == saved


@pytest.mark.parametrize(
    "kwargs",
    [
        {"interval_id": True},
        {"interval_id": 12},
        {"nSamples": True},
        {"nSamples": 1},
        {"nSamples": 20001},
        {"line_index": -1},
        {"line_index": 10},
        {"price_precision": -1},
        {"price_precision": 17},
        {"analysis_code": "EMA"},
    ],
)
def test_invalid_window_configuration_is_rejected(kwargs):
    with pytest.raises(ValidationError):
        WindowAverageConfiguration(**({"interval_id": 0, "analysis_code": "SMA"} | kwargs))


@pytest.mark.parametrize(
    "kwargs",
    [
        {"MAType": "UNKNOWN"},
        {"MAType": True},
        {"bandWidth": True},
        {"bandWidth": "2"},
        {"bandWidth": Decimal(2)},
        {"bandWidth": float("inf")},
        {"bandWidth": float("nan")},
        {"bandWidth": 10**1000},
    ],
)
def test_invalid_bollinger_configuration_is_rejected(kwargs):
    with pytest.raises(ValidationError):
        BollingerConfiguration(0, **kwargs)


def test_combined_configuration_identity_and_resource_caps():
    with pytest.raises(ValidationError):
        provider(WindowAverageConfiguration(0, "SMA", 2), WindowAverageConfiguration(0, "SMA", 3))
    with pytest.raises(ValidationError):
        LegacyPriceWindowFeatures(INSTRUMENT, ())
    with pytest.raises(ValidationError):
        LegacyPriceWindowFeatures(INSTRUMENT, ({"interval_id": 0},))
    rows = [WindowAverageConfiguration(index // 10, "SMA", 2, index % 10) for index in range(65)]
    assert len(provider(*rows[:64])._kernels) == 64
    with pytest.raises(ValidationError):
        provider(*rows)
    rows = [WindowAverageConfiguration(0, "SMA", 19997, line) for line in range(5)]
    assert (
        sum(
            kernel.closes.maxlen + kernel.history.maxlen
            for kernel in provider(*rows)._kernels.values()
        )
        == 100000
    )
    with pytest.raises(ValidationError):
        provider(*[replace(row, nSamples=20000) for row in rows])


def test_partial_dependency_with_fake_complete_flag_fails_before_mutation():
    current = provider(WindowAverageConfiguration(2, "SMA", 2))
    aggregation = CandleAggregation(INSTRUMENT, (2,))
    row = candle(1)
    frames = aggregation.update(row)
    frames["BAR_2"] = replace(frames["BAR_2"], complete=True)
    saved = current.checkpoint()
    with pytest.raises(ValidationError):
        current.update_with(row, {"bars": frames})
    assert current.checkpoint() == saved


def test_failure_in_later_window_rolls_back_prior_window_and_aggregation_together():
    current = provider(
        WindowAverageConfiguration(0, "SMA", 2),
        BollingerConfiguration(0, 2, bandWidth=1e308),
    )
    current.update(candle(0, "1"))
    saved = current.checkpoint()
    with pytest.raises(ValidationError, match="overflow"):
        current.update(candle(1, "100"))
    assert current.checkpoint() == saved
    # A valid retry at the same next minute remains possible after rollback.
    result = current.update(candle(1, "1"))
    assert result["0_SMA_0_SMA"] == result["0_BOL_0_MA"] == 1
    current.validate_checkpoint()


def test_initial_partial_prefix_does_not_create_a_committed_anchor():
    current = provider(BollingerConfiguration(2, 2, "EMA"))
    for minute in range(1, 5):
        result = current.update(candle(minute))
        assert not result["CLOSED_2"] and result["2_BOL_0_MA"] is None
        kernel = current.state["kernels"][(2, "BOL", 0)]
        assert kernel["count"] == 0 and kernel["first_open_ms"] is None
        restored = provider(BollingerConfiguration(2, 2, "EMA")).restored(
            current.checkpoint(), INSTRUMENT
        )
        assert restored.state == current.state
    for minute in range(5, 10):
        current.update(candle(minute))
    kernel = current.state["kernels"][(2, "BOL", 0)]
    assert kernel["count"] == 1 and kernel["first_open_ms"] == 5 * 60000
    current.validate_checkpoint()


def test_rounded_selected_center_is_preserved_for_bollinger_deviation():
    current = provider(BollingerConfiguration(0, 2, "WMA", 2, price_precision=0))
    current.update(candle(0, "1.1"))
    result = current.update(candle(1, "1.4"))
    selected = result["0_BOL_0_MA"]
    assert selected == 1.0
    expected_sd = math.sqrt(((1.4 - selected) ** 2 + (1.1 - selected) ** 2) / 2)
    assert result["0_BOL_0_BOLLOW"] == round(selected - 2 * expected_sd, 0)
    assert result["0_BOL_0_BOLHIGH"] == round(selected + 2 * expected_sd, 0)
