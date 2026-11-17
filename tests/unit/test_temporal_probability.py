"""Frozen actual Eta TPD function traces, causal aggregation and strict recovery."""

import builtins
import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from omega.analysis.aggregation import CandleAggregation
from omega.analysis.temporal_probability import (
    LegacyTemporalProbabilityFeatures,
    TemporalProbabilityConfiguration,
)
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

PATH = Path("tests/fixtures/temporal-probability-legacy.json")
REFERENCE = json.loads(PATH.read_text())
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def candle(index, *, flat=False):
    values = ["100", "102", "98", "100", "10"] if flat else REFERENCE["inputs"][index]
    return Candle(INSTRUMENT, index * 60000, (index + 1) * 60000, *map(Decimal, values))


def provider(configuration=None):
    return LegacyTemporalProbabilityFeatures(
        INSTRUMENT,
        (
            configuration
            or TemporalProbabilityConfiguration(0, viewLength=3, nSamples=5, nSamplesMA=3),
        ),
    )


def test_reference_identifies_actual_original_functions_and_is_frozen():
    assert (
        sha256(PATH.read_bytes()).hexdigest()
        == "430241081d79f20dab9506282c090c095e25fbddb12972330f4dc64909911f04"
    )
    assert REFERENCE["source"].endswith("analysis/temporal_probability_distribution.py")
    assert set(REFERENCE["function_sha256"]) == {"generate", "linearize"}
    assert set(REFERENCE["helper_function_sha256"]) == {
        "getNextIntervalTickTimestamp",
        "getTimestampList_byNTicks",
    }
    assert len(REFERENCE["inputs"]) == 180 and len(REFERENCE["cases"]) == 7


@pytest.mark.parametrize("case", REFERENCE["cases"], ids=lambda case: case["name"])
def test_all_raw_fields_and_closed_kernel_histories_equal_actual_eta_function(case):
    config = TemporalProbabilityConfiguration(**case["configuration"])
    current = provider(config)
    committed, closes = [], []
    previous = current.state
    for index, expected in enumerate(case["trace"], start=case["offset"]):
        result = current.update(candle(index, flat=case["flat"]))
        raw = current.state["raw"][(config.interval_id, 0)]
        assert raw == expected["raw"], (case["name"], index)
        assert result[f"CLOSED_{config.interval_id}"] is expected["closed"]
        for public, original in (
            ("TPD", "TPD"),
            ("TPDABSMA", "TPD_ABSMA"),
            ("TPDABSMAREL", "TPD_ABSMAREL"),
        ):
            assert result[f"{config.interval_id}_TPD_0_{public}"] == (
                None if raw is None else raw[original]
            )
        if expected["closed"]:
            committed.append(expected["raw"])
            closes.append(expected["close"])
        else:
            assert current.state["kernels"] == previous["kernels"]
        assert current.state["kernels"][(config.interval_id, 0)] == {
            "closes": tuple(closes[-config.viewLength - 1 :]),
            "history": tuple(committed[-max(config.nSamples, config.nSamplesMA) - 1 :]),
            "previous": committed[-1] if committed else {},
        }
        current.validate_checkpoint()
        previous = current.state


def test_zero_returns_and_warmup_boundaries_preserve_none_zero_and_legacy_name():
    current = provider(TemporalProbabilityConfiguration(0, viewLength=2, nSamples=2, nSamplesMA=2))
    outputs = [current.update(candle(index, flat=True)) for index in range(8)]
    assert all(row["0_TPD_0_TPD"] is None for row in outputs[:4])
    assert outputs[4]["0_TPD_0_TPD"] == 0.0 and outputs[4]["0_TPD_0_TPDABSMA"] is None
    assert outputs[5]["0_TPD_0_TPDABSMA"] == outputs[5]["0_TPD_0_TPDABSMAREL"] == 0.0
    assert current.state["raw"][(0, 0)]["LASTERM_PD"] == 0


def test_tpd_is_signed_return_not_a_probability_clamped_to_unit_interval():
    current = provider(TemporalProbabilityConfiguration(0, viewLength=2, nSamples=2, nSamplesMA=2))
    outputs = [current.update(candle(index))["0_TPD_0_TPD"] for index in range(180)]
    assert min(value for value in outputs if value is not None) < 0
    assert max(value for value in outputs if value is not None) > 1


def test_shared_aggregation_and_market_frame_adapters_have_identical_public_values():
    config = TemporalProbabilityConfiguration(
        1, viewLength=2, nSamples=3, nSamplesMA=4, line_index=7
    )
    direct, shared, framed = provider(config), provider(config), provider(config)
    aggregation = CandleAggregation(INSTRUMENT, (1,))
    for index in range(70):
        row = candle(index)
        expected = direct.update(row)
        values = shared.update_with(row, {"common": aggregation.update(row)})
        assert values == {
            key: value for key, value in expected.items() if not key.startswith("CLOSED_")
        }
        assert framed.update_frame(MarketFeatureFrame(row, row.close_time_ms)) == expected
        assert direct.state == shared.state == framed.state
        shared.validate_checkpoint()
    with pytest.raises(ValidationError):
        framed.update_frame(candle(70))


def test_partial_previews_and_repeated_same_minute_never_mutate_closed_kernel():
    current = provider(TemporalProbabilityConfiguration(2, viewLength=2, nSamples=3, nSamplesMA=2))
    for index in range(50):
        before = current.state["kernels"]
        row = candle(index)
        result = current.update(row)
        if not result["CLOSED_2"]:
            assert before == current.state["kernels"]
        saved = current.checkpoint()
        assert current.update(row) == result and current.checkpoint() == saved
    snapshot = current.state
    snapshot["kernels"][(2, 0)]["previous"]["BIAS"] = 99
    assert current.state != snapshot


@pytest.mark.parametrize("fault", ["gap", "conflict", "instrument", "overflow", "underflow"])
def test_rejected_inputs_leave_all_state_and_checkpoint_bytes_unchanged(fault):
    current = provider()
    current.update(candle(0))
    before = current.checkpoint()
    row = candle(2) if fault == "gap" else candle(1)
    if fault == "conflict":
        row = replace(candle(0), close=Decimal("100"))
    elif fault == "instrument":
        row = replace(row, instrument=Instrument("binance", Market.USDM, "BTCUSDT"))
    elif fault in {"overflow", "underflow"}:
        price = Decimal("1e1000") if fault == "overflow" else Decimal("1e-1000")
        row = replace(row, open=price, high=price, low=price, close=price)
    with pytest.raises(ValidationError):
        current.update(row)
    assert current.checkpoint() == before


def test_checkpoint_resumes_every_seed_and_first_ema_boundary_with_fresh_provider():
    current = provider()
    for index in range(80):
        row = candle(index)
        result = current.update(row)
        if index < 15 or index % 7 == 0:
            saved = json.loads(json.dumps(current.checkpoint(), allow_nan=False))
            restored = provider().restored(saved, INSTRUMENT)
            assert restored.checkpoint() == saved and restored.state == current.state
            if index + 1 < 80:
                expected = deepcopy(current).update(candle(index + 1))
                assert restored.update(candle(index + 1)) == expected
            assert current.update(row) == result


@pytest.mark.parametrize(
    "fault",
    [
        "count",
        "return",
        "sum",
        "bias",
        "ema",
        "absolute",
        "relative",
        "history",
        "public",
        "configuration",
    ],
)
def test_valid_checksum_corrupted_semantic_state_cannot_replace_active_graph(fault):
    current = provider()
    for index in range(40):
        current.update(candle(index))
    before = current.checkpoint()
    damaged = deepcopy(current)
    kernel = damaged._kernels[(0, 0)]
    fields = {
        "count": "analysisCount",
        "return": "LASTERM_PD",
        "sum": "TERMSUM_INCREMENTAL",
        "bias": "BIAS",
        "ema": "TPD",
        "absolute": "TPD_ABSMA",
        "relative": "TPD_ABSMAREL",
    }
    if fault in fields:
        key = fields[fault]
        kernel.previous[key] += 1
        kernel.history[-1][key] += 1
    elif fault == "history":
        kernel.history.pop()
    elif fault == "public":
        damaged._result["0_TPD_0_TPD"] = 999.0
    else:
        kernel.config = replace(kernel.config, nSamples=6)
    with pytest.raises(ValidationError):
        current.restored(damaged.checkpoint(), INSTRUMENT)
    assert current.checkpoint() == before


@pytest.mark.parametrize(
    "changes",
    [
        {"interval_id": True},
        {"interval_id": 12},
        {"viewLength": 1},
        {"nSamples": True},
        {"nSamplesMA": 20001},
        {"line_index": 10},
    ],
)
def test_configuration_is_native_and_bounded(changes):
    with pytest.raises(ValidationError):
        TemporalProbabilityConfiguration(**({"interval_id": 0} | changes))


def test_definition_budget_and_stdlib_only_computation(monkeypatch):
    config = TemporalProbabilityConfiguration(0)
    with pytest.raises(ValidationError):
        LegacyTemporalProbabilityFeatures(INSTRUMENT, (config, config))
    large = [
        TemporalProbabilityConfiguration(index, viewLength=20000, nSamples=20000)
        for index in range(3)
    ]
    with pytest.raises(ValidationError):
        LegacyTemporalProbabilityFeatures(INSTRUMENT, large)
    original_import = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.split(".")[0] in {"torch", "triton", "tensorflow", "numpy", "scipy"}:
            raise AssertionError("TPD must not import optional numerical/model frameworks")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    current = provider()
    for index in range(40):
        current.update(candle(index))
    assert (
        current.checkpoint() == provider().restored(current.checkpoint(), INSTRUMENT).checkpoint()
    )


@pytest.mark.parametrize("shared", [False, True])
@pytest.mark.parametrize("boundary", [False, True])
def test_raw_counter_cannot_claim_next_or_committed_frame_at_wrong_interval_boundary(
    shared, boundary
):
    config = TemporalProbabilityConfiguration(1, viewLength=3, nSamples=5, nSamplesMA=3)
    current = provider(config)
    aggregation = CandleAggregation(INSTRUMENT, (1,))
    for index in range(3 if boundary else 4):
        row = candle(index)
        if shared:
            current.update_with(row, {"shared": aggregation.update(row)})
        else:
            current.update(row)
    before = current.checkpoint()
    damaged = deepcopy(current)
    damaged._raw[(1, 0)]["analysisCount"] += 1 if boundary else -1
    with pytest.raises(ValidationError, match="cursor"):
        current.restored(damaged.checkpoint(), INSTRUMENT)
    assert current.checkpoint() == before


def test_closed_one_minute_seed_cannot_contain_future_preview_with_valid_checksum():
    current = provider()
    current.update(candle(0))
    before = current.checkpoint()
    damaged = deepcopy(current)
    damaged._raw[(0, 0)]["analysisCount"] = 1
    with pytest.raises(ValidationError, match="cursor"):
        current.restored(damaged.checkpoint(), INSTRUMENT)
    assert current.checkpoint() == before


@pytest.mark.parametrize("shared", [False, True])
def test_initial_partial_prefix_at_boundary_restores_without_fabricated_close(shared):
    config = TemporalProbabilityConfiguration(2, viewLength=2, nSamples=3, nSamplesMA=2)
    current = provider(config)
    aggregation = CandleAggregation(INSTRUMENT, (2,))
    for index in range(1, 5):
        row = candle(index)
        if shared:
            current.update_with(row, {"shared": aggregation.update(row)})
        else:
            current.update(row)
    assert current.state["raw"][(2, 0)] is None
    if not shared:
        assert current._result["CLOSED_2"] is False
    saved = current.checkpoint()
    assert current.restored(saved, INSTRUMENT).checkpoint() == saved


@pytest.mark.parametrize("fault", ["closure", "oversized_integer"])
def test_invalid_checkpoint_flags_and_oversized_scalars_are_validation_failures(fault):
    current = provider()
    for index in range(40):
        current.update(candle(index))
    damaged = deepcopy(current)
    if fault == "closure":
        damaged._result["CLOSED_0"] = False
    else:
        damaged._kernels[(0, 0)].previous["TPD_ABSMA"] = 10**400
        damaged._kernels[(0, 0)].history[-1]["TPD_ABSMA"] = 10**400
    with pytest.raises(ValidationError):
        current.restored(damaged.checkpoint(), INSTRUMENT)
