"""Independent Eta AST fixtures and causal shared-kernel STS composition."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from omega.analysis.order_flow import DEPTH_FIELDS
from omega.analysis.short_term_scalar import LegacyShortTermScalarFeatures, ScalarConfiguration
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

REFERENCE_PATH = Path("tests/fixtures/short-term-scalar-legacy.json")
REFERENCE = json.loads(REFERENCE_PATH.read_text())
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def frame(index, *, depth="fixture", quality="observed"):
    item = REFERENCE["inputs"][index]
    bar = Candle(INSTRUMENT, index * 60000, (index + 1) * 60000, *map(Decimal, item["ohlcv"]))
    depth = item["depth"] if depth == "fixture" else depth
    inputs = (
        ()
        if depth is None
        else (
            AuxiliaryInput(
                channel="depth",
                values=tuple(zip(DEPTH_FIELDS, map(Decimal, depth), strict=True)),
                window_start_ms=bar.open_time_ms,
                window_end_ms=bar.close_time_ms,
                event_time_ms=bar.close_time_ms,
                available_at_ms=bar.close_time_ms,
                source="frozen_actual_depth",
                quality=quality,
            ),
        )
    )
    return MarketFeatureFrame(bar, bar.close_time_ms, inputs)


def provider(configuration=None):
    return LegacyShortTermScalarFeatures(
        INSTRUMENT, (ScalarConfiguration(0) if configuration is None else configuration,)
    )


def test_original_ast_fixture_is_frozen_and_provenance_is_explicit():
    assert (
        sha256(REFERENCE_PATH.read_bytes()).hexdigest()
        == "84519dafa58617aa0416cf319c46943f3f023c7b1af5cd79bef045fca44015f2"
    )
    assert REFERENCE["source"].endswith("analysis/short_term_scalar.py")
    assert len(REFERENCE["ast_sha256"]) == 64
    assert len(REFERENCE["inputs"]) == 240


@pytest.mark.parametrize("case", REFERENCE["cases"], ids=lambda case: case["name"])
def test_all_raw_fields_match_original_ast_for_every_preview_and_closed_bar(case):
    config = ScalarConfiguration(**case["configuration"])
    current = provider(config)
    previous = None
    for index, expected in enumerate(case["trace"]):
        result = current.update_frame(frame(index))
        assert current._raw[config.interval_id] == expected["raw"], index
        assert result[f"CLOSED_{config.interval_id}"] is expected["closed"]
        for field in ("S", "M", "F", "D", "W"):
            assert result[f"{config.interval_id}_STS_{field}"] == expected["raw"][field]
        current.validate_checkpoint()
        if previous is not None and not expected["closed"]:
            assert current.state["kernels"] == previous["kernels"]
            assert current.state["baselines"] == previous["baselines"]
        previous = current.state


def test_default_first_full_scalar_is_bar_41_and_baseline_does_not_require_240_bars():
    current = provider()
    first = None
    for index in range(50):
        result = current.update_frame(frame(index))
        if result["0_STS_S"] is not None and first is None:
            first = index + 1
    assert first == 41
    assert current._baselines[0] is not None


def test_same_minute_is_idempotent_but_changed_inputs_gap_and_reverse_are_atomic():
    current = provider()
    first = frame(0)
    result = current.update_frame(first)
    before = current.state
    assert current.update_frame(replace(first, as_of_ms=first.as_of_ms + 1)) == result
    assert current.state == before
    for invalid in (frame(0, depth=["1"] * 12), frame(2)):
        with pytest.raises(ValidationError):
            current.update_frame(invalid)
        assert current.state == before
    current.update_frame(frame(1))
    before = current.state
    with pytest.raises(ValidationError):
        current.update_frame(first)
    assert current.state == before


def test_missing_depth_never_becomes_observed_zero_even_when_woi_weight_is_zero():
    config = ScalarConfiguration(0, WWOI=0.0)
    current = provider(config)
    for index in range(60):
        result = current.update(frame(index).candle)
    assert result["0_STS_M"] is not None and result["0_STS_F"] is not None
    assert result["0_STS_D"] is not None
    assert result["0_STS_W"] is None and result["0_STS_S"] is None
    assert not current.input_ready and current.input_quality["0_STS"]["reason"] == "missing"
    assert current._raw[0]["WOI_IMBALANCE"] is None
    assert current._kernels[0, "WOI"].previous["mode"] == 0


def test_actual_zero_depth_seeds_woi_and_missing_after_seed_holds_with_unready_quality():
    config = ScalarConfiguration(0, WOINSamples=2)
    current = provider(config)
    for index in range(4):
        current.update_frame(frame(index, depth=["0"] * 12))
    assert current.input_ready and current._raw[0]["WOI"] == 0.0
    current.update_frame(frame(4, depth=None))
    assert current._raw[0]["WOI_IMBALANCE"] is None and current._raw[0]["WOI"] == 0.0
    assert not current.input_ready and current.input_quality["0_STS"]["reason"] == "missing"


def test_partial_prefix_does_not_seed_any_component_and_next_full_bucket_starts_clean():
    config = ScalarConfiguration(1, WOINSamples=2)
    current = provider(config)
    for index in (1, 2):
        result = current.update_frame(frame(index))
        assert all(result[f"1_STS_{field}"] is None for field in ("S", "M", "F", "D", "W"))
        assert not result["CLOSED_1"] and not current.input_ready
        assert current._kernels[1, "MMACD"].count == 0
        current.validate_checkpoint()
    for index in (3, 4, 5):
        current.update_frame(frame(index))
    assert current._kernels[1, "MMACD"].count == 1
    assert current._kernels[1, "MFI"].analysisCount == 1
    assert current._raw[1]["analysisCount"] == 0


def test_missing_last_depth_in_higher_bucket_retains_last_snapshot_and_marks_unready():
    current = provider(ScalarConfiguration(1, WOINSamples=2))
    current.update_frame(frame(0))
    current.update_frame(frame(1))
    snapshot = current._buckets[1].values
    result = current.update_frame(frame(2, depth=None))
    assert result["CLOSED_1"] and current._buckets[1].values == snapshot
    assert current._kernels[1, "WOI"].history[-1] is not None
    assert not current.input_ready
    current.validate_checkpoint()


def test_partial_depth_is_not_accepted_as_a_complete_snapshot():
    current = provider()
    current.update_frame(frame(0, quality="partial"))
    assert not current.input_ready and current._raw[0]["WOI_IMBALANCE"] is None
    assert current.input_quality["0_STS"]["reason"] == "partial"


def test_nonfinite_legacy_conversion_fails_without_advancing_any_closed_state():
    current = provider()
    current.update_frame(frame(0))
    before = current.state
    next_frame = frame(1, depth=["1e999"] * 12)
    with pytest.raises(ValidationError):
        current.update_frame(next_frame)
    assert current.state == before


@pytest.mark.parametrize(
    "changes",
    [
        {"MMACDFast": True},
        {"MMACDFast": 20},
        {"MMACDSlow": 20001},
        {"WOIBaseNSamples": 1},
        {"OSCScale": 0},
        {"OSCScale": float("nan")},
        {"WMMACD": -1},
        {"WWOI": True},
        {"price_precision": 17},
        {"WMMACD": 0, "WMFI": 0, "WDMIxADX": 0, "WWOI": 0},
    ],
)
def test_configuration_rejects_invalid_periods_weights_and_precision(changes):
    with pytest.raises(ValidationError):
        ScalarConfiguration(0, **changes)


def test_interval_uniqueness_and_total_kernel_work_are_bounded():
    with pytest.raises(ValidationError):
        LegacyShortTermScalarFeatures(INSTRUMENT, (ScalarConfiguration(0), ScalarConfiguration(0)))
    with pytest.raises(ValidationError):
        LegacyShortTermScalarFeatures(
            INSTRUMENT,
            tuple(
                ScalarConfiguration(
                    iid,
                    MMACDSlow=20000,
                    DMIxADXNSamples=20000,
                    MFINSamples=20000,
                    WOINSamples=20000,
                )
                for iid in (0, 1)
            ),
        )


def test_data_only_state_is_an_independent_snapshot():
    current = provider()
    current.update_frame(frame(0))
    state = current.state
    state["raw"][0]["analysisCount"] = 999
    state["kernels"][0, "WOI"]["previous"]["mode"] = 999
    assert current._raw[0]["analysisCount"] == 0
    assert current._kernels[0, "WOI"].previous["mode"] == 0
    copy = deepcopy(current)
    for index in range(1, 8):
        assert copy.update_frame(frame(index)) == current.update_frame(frame(index))


@pytest.mark.parametrize("split", [0, 1, 5, 39, 40, 80, 239])
def test_json_checkpoint_preserves_all_shared_kernels_and_preview_continuity(split):
    configurations = tuple(
        ScalarConfiguration(**case["configuration"]) for case in REFERENCE["cases"]
    )
    original = LegacyShortTermScalarFeatures(INSTRUMENT, configurations)
    for index in range(split):
        original.update_frame(frame(index))
    saved = json.loads(json.dumps(original.checkpoint()))
    resumed = LegacyShortTermScalarFeatures(INSTRUMENT, configurations).restored(saved, INSTRUMENT)
    assert resumed.state == original.state
    for index in range(split, 240):
        assert resumed.update_frame(frame(index)) == original.update_frame(frame(index))
        assert resumed.state == original.state
        resumed.validate_checkpoint()


@pytest.mark.parametrize(
    "mutation",
    [
        "count",
        "raw_schema",
        "mode",
        "quality",
        "baseline",
        "cursor",
        "history",
        "boolean",
        "raw_container",
        "bar_values",
        "ready_forgery",
    ],
)
def test_valid_checksum_tampering_is_rejected_without_mutating_active_state(mutation):
    current = provider()
    for index in range(45):
        current.update_frame(frame(index))
    corrupted = deepcopy(current)
    if mutation == "count":
        corrupted._kernels[0, "MMACD"].count += 1
    elif mutation == "raw_schema":
        corrupted._raw[0]["unknown"] = 1.0
    elif mutation == "mode":
        corrupted._raw[0]["WOI_MODE"] = True
    elif mutation == "quality":
        corrupted._input_quality["0_STS"]["ready"] = False
    elif mutation == "baseline":
        corrupted._baselines[0] = 1
    elif mutation == "cursor":
        old = corrupted._kernels[0, "DMI"].previous_bar
        corrupted._kernels[0, "DMI"].previous_bar = replace(
            old, close_time_ms=old.close_time_ms - 60000
        )
    elif mutation == "history":
        corrupted._kernels[0, "MFI"].history[-1]["UNKNOWN"] = 1.0
    elif mutation == "boolean":
        corrupted._kernels[0, "MFI"].history[-1]["TP"] = True
    elif mutation == "raw_container":
        corrupted._raw[0]["MMACD_SIGNAL"] = []
    elif mutation == "bar_values":
        old = corrupted._kernels[0, "DMI"].previous_bar
        corrupted._kernels[0, "DMI"].previous_bar = replace(old, high=Decimal(1))
    else:
        corrupted._buckets[0].ready = False
        corrupted._input_quality["0_STS"].update(ready=False, reason="missing")
    # Packing a manipulated data-only state creates a valid envelope checksum;
    # semantic validation must still reject it on a clean target copy.
    saved = json.loads(json.dumps(corrupted.checkpoint()))
    before = current.checkpoint()
    with pytest.raises(ValidationError):
        current.restored(saved, INSTRUMENT)
    assert current.checkpoint() == before


def test_coarse_price_rounding_to_zero_is_a_valid_legacy_ma_state():
    current = provider(ScalarConfiguration(0, price_precision=0))
    for index in range(45):
        value = frame(index)
        bar = value.candle
        bar = replace(
            bar,
            open=bar.open / 100000,
            high=bar.high / 100000,
            low=bar.low / 100000,
            close=bar.close / 100000,
        )
        current.update_frame(replace(value, candle=bar))
        current.validate_checkpoint()
    assert current._kernels[0, "MMACD"].mas[10] == 0.0
    restored = provider(ScalarConfiguration(0, price_precision=0)).restored(
        current.checkpoint(), INSTRUMENT
    )
    assert restored.state == current.state
