"""Exact frozen Eta OHLCV profile/rebin/peak output and bounded recovery state."""

import builtins
import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from omega.analysis.volume_profile import (
    LegacyVolumeProfileFeatures,
    ProfileConfiguration,
    _libraries,
)
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

PATH = Path("tests/fixtures/volume-profile-legacy.json")
REFERENCE = json.loads(PATH.read_text())
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def candle(index):
    return Candle(
        INSTRUMENT, index * 60000, (index + 1) * 60000, *map(Decimal, REFERENCE["inputs"][index])
    )


def provider(configuration=None):
    return LegacyVolumeProfileFeatures(
        INSTRUMENT,
        (ProfileConfiguration(0, nSamples=3) if configuration is None else configuration,),
    )


def vector_hash(value):
    return (
        None
        if value is None
        else sha256(json.dumps(value, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    )


def test_original_fixture_is_frozen_and_identifies_real_ohlcv_algorithm():
    assert (
        sha256(PATH.read_bytes()).hexdigest()
        == "185e22a14e53f9a5eeae65295173e07bc60518cb064349b513f9d5860b16aa75"
    )
    assert REFERENCE["source"].endswith("analysis/interpreted_volume_profile.py")
    assert len(REFERENCE["inputs"]) == 600 and len(REFERENCE["cases"]) == 4


@pytest.mark.parametrize("case", REFERENCE["cases"], ids=lambda case: case["name"])
def test_profile_decay_rebin_gaussian_and_all_boundary_fields_match_original(case):
    config = ProfileConfiguration(**case["configuration"])
    current = provider(config)
    previous = None
    for index, expected in enumerate(case["trace"]):
        result = current.update(candle(index))
        raw = current._raw[config.interval_id]
        assert vector_hash(raw["volumePriceLevelProfile"]) == expected["profile_sha256"], index
        assert (
            vector_hash(raw["volumePriceLevelProfile_Filtered"]) == expected["filtered_sha256"]
        ), index
        actual = {
            key: list(value) if isinstance(value, tuple) else value
            for key, value in raw.items()
            if key not in {"volumePriceLevelProfile", "volumePriceLevelProfile_Filtered"}
        }
        assert actual == expected["raw"], index
        assert result[f"CLOSED_{config.interval_id}"] is expected["closed"]
        assert [result[f"{config.interval_id}_IVP_NB{i}"] for i in range(10)] == list(
            raw["volumePriceLevelProfile_NearBoundaries"]
        )
        current.validate_checkpoint()
        if previous is not None and not expected["closed"]:
            assert current.state["kernels"] == previous["kernels"]
        previous = current.state


def test_seed_requires_n_bars_but_true_zero_volume_produces_no_false_boundaries():
    current = provider()
    for index in range(3):
        result = current.update(candle(index))
        assert all(result[f"0_IVP_NB{i}"] is None for i in range(10))
        profile = current._raw[0]["volumePriceLevelProfile"]
        assert (profile is None) == (index < 2)
    assert current.input_ready and sum(profile) == 0
    assert current._raw[0]["volumePriceLevelProfile_Filtered"] is None
    assert current._kernels[0].history == [] or len(current._kernels[0].history) == 0


def test_partial_prefix_never_seeds_profile_and_full_bucket_starts_cleanly():
    current = provider(ProfileConfiguration(1, nSamples=2))
    for index in (1, 2):
        result = current.update(candle(index))
        assert not current.input_ready and current._raw[1] is None
        assert not result["CLOSED_1"] and not current._kernels[1].previous
        current.validate_checkpoint()
    for index in (3, 4, 5):
        current.update(candle(index))
    assert current._kernels[1].previous["analysisCount"] == 0
    assert current._kernels[1].previous["volumePriceLevelProfile"] is None


def test_same_candle_is_idempotent_and_gap_reverse_conflict_fail_atomically():
    current = provider()
    result = current.update(candle(0))
    before = current.state
    assert current.update(candle(0)) == result and current.state == before
    for invalid in (candle(2), replace(candle(0), volume=Decimal(1))):
        with pytest.raises(ValidationError):
            current.update(invalid)
        assert current.state == before
    current.update(candle(1))
    before = current.state
    with pytest.raises(ValidationError):
        current.update(candle(0))
    assert current.state == before


def test_ohlcv_frame_needs_no_depth_or_trade_channel():
    current = provider()
    plain = provider()
    for index in range(4):
        bar = candle(index)
        assert current.update_frame(MarketFeatureFrame(bar, bar.close_time_ms)) == plain.update(bar)
    assert current.state == plain.state


def test_missing_optional_libraries_are_a_clear_error_and_leave_seed_state_intact(monkeypatch):
    current = provider(ProfileConfiguration(0, nSamples=2))
    current.update(candle(0))
    before = current.state
    original = builtins.__import__

    def without_analysis_dependencies(name, *args, **kwargs):
        if name == "numpy" or name.startswith("scipy"):
            raise ImportError("synthetic missing optional analysis dependency")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_analysis_dependencies)
    with pytest.raises(ValidationError, match="optional analysis"):
        _libraries()
    with pytest.raises(ValidationError, match="optional analysis"):
        current.update(candle(1))
    assert current.state == before


def test_profile_budget_exhaustion_fails_without_replacing_any_state():
    current = provider(ProfileConfiguration(0, nSamples=2, max_divisions=2))
    current.update(candle(0))
    before = current.state
    with pytest.raises(ValidationError, match="division budget"):
        current.update(candle(1))
    assert current.state == before


def test_price_drop_cannot_expand_an_existing_all_time_profile_past_the_budget():
    current = provider(ProfileConfiguration(0, nSamples=2, gammaFactor=0.1, max_divisions=250))
    for index in range(2):
        current.update(
            Candle(
                INSTRUMENT,
                index * 60000,
                (index + 1) * 60000,
                *(Decimal(v) for v in (100, 102, 99, 101, 1)),
            )
        )
    before = current.state
    assert current._kernels[0].previous["priceMax"] == 102.0
    assert current._kernels[0].previous["volumePriceLevelProfile"] is not None
    with pytest.raises(ValidationError, match="division budget"):
        current.update(
            Candle(INSTRUMENT, 120000, 180000, *(Decimal(v) for v in (1, 2, ".5", 1, 1)))
        )
    assert current.state == before


def test_partial_prefix_data_only_checkpoint_restores_without_seed_advancement():
    config = ProfileConfiguration(2, nSamples=2)
    current = provider(config)
    current.update(candle(2))
    restored = provider(config).restored(json.loads(json.dumps(current.checkpoint())), INSTRUMENT)
    assert restored.state == current.state
    for index in range(3, 15):
        assert restored.update(candle(index)) == current.update(candle(index))
        assert restored.state == current.state


def test_rounded_zero_beta_is_rejected_instead_of_inventing_a_tick():
    current = provider(ProfileConfiguration(0, nSamples=2, price_precision=0))

    def tiny(index):
        bar = candle(index)
        return replace(
            bar,
            open=bar.open / 100000,
            high=bar.high / 100000,
            low=bar.low / 100000,
            close=bar.close / 100000,
        )

    current.update(tiny(0))
    before = current.state
    with pytest.raises(ValidationError, match="zero/invalid profile scale"):
        current.update(tiny(1))
    assert current.state == before


@pytest.mark.parametrize(
    "changes",
    [
        {"nSamples": True},
        {"nSamples": 1},
        {"nSamples": 20001},
        {"gammaFactor": 0.001},
        {"gammaFactor": float("nan")},
        {"deltaFactor": 11},
        {"prominence": 0},
        {"height": 1.1},
        {"distance": 0},
        {"distance": True},
        {"price_precision": 17},
        {"max_divisions": 32769},
    ],
)
def test_configuration_is_typed_bounded_and_preserves_original_valid_ranges(changes):
    with pytest.raises(ValidationError):
        ProfileConfiguration(0, **changes)


def test_duplicate_interval_and_global_profile_seed_memory_limits_are_enforced():
    with pytest.raises(ValidationError):
        LegacyVolumeProfileFeatures(INSTRUMENT, (ProfileConfiguration(0), ProfileConfiguration(0)))
    with pytest.raises(ValidationError):
        LegacyVolumeProfileFeatures(
            INSTRUMENT, (ProfileConfiguration(0, nSamples=20000), ProfileConfiguration(1))
        )
    with pytest.raises(ValidationError):
        LegacyVolumeProfileFeatures(
            INSTRUMENT, (ProfileConfiguration(0, max_divisions=32768), ProfileConfiguration(1))
        )


def test_returned_profile_state_is_independent_of_the_active_arrays():
    current = provider()
    for index in range(10):
        current.update(candle(index))
    state = current.state
    state["kernels"][0]["previous"]["volumePriceLevelProfile"][0] = 1e99
    assert current._kernels[0].previous["volumePriceLevelProfile"][0] != 1e99
    repeated = deepcopy(current)
    for index in range(10, 15):
        assert repeated.update(candle(index)) == current.update(candle(index))
        assert repeated.state == current.state


@pytest.mark.parametrize("split", [0, 1, 2, 14, 189, 350, 499])
def test_json_checkpoint_preserves_seed_all_time_maximum_and_full_profile(split):
    configs = tuple(
        ProfileConfiguration(**case["configuration"])
        for case in REFERENCE["cases"]
        if case["name"] != "rebin-minute"
    )
    original = LegacyVolumeProfileFeatures(INSTRUMENT, configs)
    for index in range(split):
        original.update(candle(index))
    saved = json.loads(json.dumps(original.checkpoint()))
    resumed = LegacyVolumeProfileFeatures(INSTRUMENT, configs).restored(saved, INSTRUMENT)
    assert resumed.state == original.state
    for index in range(split, min(split + 16, 600)):
        assert resumed.update(candle(index)) == original.update(candle(index))
        assert resumed.state == original.state
        resumed.validate_checkpoint()


@pytest.mark.parametrize(
    "mutation",
    [
        "negative_profile",
        "filtered_shape",
        "boundary_type",
        "near",
        "counter",
        "cursor",
        "history",
        "profile_shape",
    ],
)
def test_valid_checksum_semantic_damage_is_rejected_without_changing_active_state(mutation):
    current = provider()
    for index in range(25):
        current.update(candle(index))
    corrupted = deepcopy(current)
    kernel, raw = corrupted._kernels[0], corrupted._raw[0]
    if mutation == "negative_profile":
        kernel.previous["volumePriceLevelProfile"][0] = -1.0
    elif mutation == "filtered_shape":
        raw["volumePriceLevelProfile_Filtered"].pop()
    elif mutation == "boundary_type":
        raw["volumePriceLevelProfile_Boundaries"] = [True]
    elif mutation == "near":
        near = list(raw["volumePriceLevelProfile_NearBoundaries"])
        near[0] = 1.0
        raw["volumePriceLevelProfile_NearBoundaries"] = tuple(near)
    elif mutation == "counter":
        raw["analysisCount"] = True
    elif mutation == "cursor":
        kernel.previous["closed_time_ms"] -= 1
    elif mutation == "history":
        kernel.history.append((1.0, 1.0, 2.0))
    else:
        kernel.previous["volumePriceLevelProfile"].append(0.0)
    saved = json.loads(json.dumps(corrupted.checkpoint()))
    before = current.checkpoint()
    with pytest.raises(ValidationError):
        current.restored(saved, INSTRUMENT)
    assert current.checkpoint() == before
