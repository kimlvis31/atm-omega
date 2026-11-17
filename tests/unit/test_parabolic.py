"""Frozen Eta PSAR reference, causal previews and exact data-only recovery."""

import json
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from omega.analysis.catalog import create_features, validate_configuration
from omega.analysis.parabolic import LegacyParabolicFeatures, ParabolicConfiguration
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market

REFERENCE = json.loads(Path("tests/fixtures/parabolic-legacy.json").read_text())
INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def bar(index):
    return Candle(
        INSTRUMENT,
        index * 60000,
        (index + 1) * 60000,
        *(Decimal(value) for value in REFERENCE["inputs"][index]),
    )


def configuration(case):
    return ParabolicConfiguration(
        **{
            key: value
            for key, value in case.items()
            if key in {"interval_id", "start", "acceleration", "maximum", "price_precision"}
        }
    )


@pytest.mark.parametrize(
    "case", REFERENCE["cases"], ids=lambda row: f"i{row['interval_id']}-af{row['acceleration']}"
)
def test_psar_matches_1920_frozen_original_frames_including_higher_bar_previews(case):
    provider = LegacyParabolicFeatures(INSTRUMENT, (configuration(case),))
    prefix = f"{case['interval_id']}_PSAR_0_"
    for index, expected in enumerate(case["expected"]):
        result = provider.update(bar(index))
        assert result[prefix + "PSAR"] == expected["PSAR"]
        assert result[prefix + "DCC"] == expected["DCC"]
        assert result[f"CLOSED_{case['interval_id']}"] == ((index + 1) % case["minutes"] == 0)


@pytest.mark.parametrize("split", [0, 1, 2, 3, 17, 38, 91])
def test_psar_checkpoint_preserves_modes_and_partial_buckets(split):
    config = (ParabolicConfiguration(0), ParabolicConfiguration(1, line_index=3))
    original = LegacyParabolicFeatures(INSTRUMENT, config)
    for index in range(split):
        original.update(bar(index))
    saved = json.loads(json.dumps(original.checkpoint()))
    restored = LegacyParabolicFeatures(INSTRUMENT, config).restored(saved, INSTRUMENT)
    for index in range(split, 120):
        assert restored.update(bar(index)) == original.update(bar(index))
        assert restored.state == original.state


def test_catalog_graph_and_standalone_psar_share_outputs_and_checkpoint_recovery():
    config = {"parabolic": [{"interval_id": 2, "line_index": 4, "acceleration": 0.005}]}
    canonical = validate_configuration(config)
    row = canonical["parabolic"][0]
    assert (row["start"], row["acceleration"], row["maximum"]) == (0.02, 0.005, 0.2)
    graph = create_features(INSTRUMENT, {"2_PSAR_4_PSAR", "2_PSAR_4_DCC"}, canonical)
    original = LegacyParabolicFeatures(INSTRUMENT, (ParabolicConfiguration(**row),))
    for index in range(42):
        assert graph.update(bar(index)) == original.update(bar(index))
    recovered = create_features(INSTRUMENT, {"2_PSAR_4_PSAR"}, canonical).restored(
        json.loads(json.dumps(graph.checkpoint())), INSTRUMENT
    )
    for index in range(42, 70):
        assert recovered.update(bar(index)) == graph.update(bar(index))


def test_unaligned_prefix_is_not_used_as_a_complete_reference_bar():
    provider = LegacyParabolicFeatures(INSTRUMENT, (ParabolicConfiguration(1),))
    for index in range(1, 9):
        assert provider.update(bar(index))["1_PSAR_0_PSAR"] is None
    assert provider.update(bar(9))["1_PSAR_0_PSAR"] is not None


def test_gap_conflict_and_duplicate_do_not_change_psar_state():
    provider = LegacyParabolicFeatures(INSTRUMENT, (ParabolicConfiguration(0),))
    for index in range(6):
        result = provider.update(bar(index))
    before = provider.checkpoint()
    assert provider.update(bar(5)) == result
    with pytest.raises(ValidationError):
        provider.update(bar(7))
    assert provider.checkpoint() == before


@pytest.mark.parametrize(
    "bad",
    [
        {"start": 0},
        {"start": True},
        {"start": float("nan")},
        {"acceleration": -1},
        {"acceleration": float("inf")},
        {"maximum": 0.02},
        {"interval_id": True},
        {"line_index": 10},
        {"price_precision": 17},
    ],
)
def test_bad_psar_configuration_is_rejected_before_state_creation(bad):
    with pytest.raises(ValidationError):
        ParabolicConfiguration(**({"interval_id": 0} | bad))


@pytest.mark.parametrize("field,value", [("mode", 3), ("AF", 0.01)])
def test_valid_checksum_does_not_allow_invalid_psar_state_to_mutate_the_graph(field, value):
    provider = LegacyParabolicFeatures(INSTRUMENT, (ParabolicConfiguration(0),))
    for index in range(8):
        provider.update(bar(index))
    saved = deepcopy(provider.checkpoint())
    previous = saved["payload"]["state"]["_kernels"][0][1]["state"]["previous"]["pairs"]
    for pair in previous:
        if pair[0] == field:
            pair[1] = value
    saved["sha256"] = sha256(
        json.dumps(
            saved["payload"], sort_keys=True, allow_nan=False, separators=(",", ":")
        ).encode()
    ).hexdigest()
    fresh = LegacyParabolicFeatures(INSTRUMENT, (ParabolicConfiguration(0),))
    before = fresh.checkpoint()
    with pytest.raises(ValidationError):
        fresh.restored(saved, INSTRUMENT)
    assert fresh.checkpoint() == before
