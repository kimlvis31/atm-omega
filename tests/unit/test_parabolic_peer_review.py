"""Independent PSAR review of closed state, strict threshold ties and partial-prefix recovery."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from omega.analysis.parabolic import LegacyParabolicFeatures, ParabolicConfiguration
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market

REFERENCE = json.loads(Path("tests/fixtures/parabolic-legacy.json").read_text())
INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def reference_bar(index):
    return Candle(
        INSTRUMENT,
        index * 60000,
        (index + 1) * 60000,
        *(Decimal(value) for value in REFERENCE["inputs"][index]),
    )


def price_bar(index, high, low):
    high, low = Decimal(str(high)), Decimal(str(low))
    middle = (high + low) / 2
    return Candle(
        INSTRUMENT, index * 60000, (index + 1) * 60000, middle, high, low, middle, Decimal("1")
    )


@pytest.mark.parametrize(
    "case",
    REFERENCE["cases"],
    ids=lambda case: f"closed-i{case['interval_id']}-af{case['acceleration']}",
)
def test_raw_closed_state_matches_original_af_ep_direction_dcc_and_keeps_two_bars(case):
    keys = ("interval_id", "start", "acceleration", "maximum", "price_precision")
    provider = LegacyParabolicFeatures(
        INSTRUMENT, (ParabolicConfiguration(**{key: case[key] for key in keys}),)
    )
    duration = case["minutes"]
    for index in range(len(REFERENCE["inputs"])):
        before = provider.state
        provider.update(reference_bar(index))
        state = provider.state[(case["interval_id"], 0)]
        if (index + 1) % duration:
            assert provider.state == before  # Higher-bar previews must not advance AF/DCC.
            continue
        assert state["previous"] == case["raw"][index]
        completed = (index + 1) // duration
        assert len(state["history"]) == min(2, completed)
        first_reference = max(0, completed - 2) * duration
        expected_history = tuple(
            (
                max(float(REFERENCE["inputs"][i][1]) for i in range(start, start + duration)),
                min(float(REFERENCE["inputs"][i][2]) for i in range(start, start + duration)),
            )
            for start in range(first_reference, index + 1, duration)
        )
        assert state["history"] == expected_history


@pytest.mark.parametrize("upward", [True, False])
def test_exact_stop_threshold_does_not_reverse_but_crossing_does_and_resets_af_dcc(upward):
    provider = LegacyParabolicFeatures(INSTRUMENT, (ParabolicConfiguration(0),))
    rows = (
        [(11, 9), (12, 10), (13, 11), (13, 9.06), (13, 9.05)]
        if upward
        else [(11, 9), (10, 8), (9, 7), (10.94, 7), (10.95, 7.2)]
    )
    for index, (high, low) in enumerate(rows):
        output = provider.update(price_bar(index, high, low))
        state = provider.state[(0, 0)]["previous"]
        if index == 3:
            assert state["PD"] is upward
            assert state["PDReversed"] is False and state["DCC"] == 1
            assert state["AF"] == 0.025
        if index == 4:
            assert state["PD"] is not upward
            assert state["PDReversed"] is True and state["DCC"] == 0
            assert state["AF"] == 0.02
            assert output["0_PSAR_0_PSAR"] == (13 if upward else 7)


def test_mid_bucket_initial_prefix_and_partial_third_bar_restore_have_no_future_commit():
    configuration = (ParabolicConfiguration(3, line_index=7),)
    original = LegacyParabolicFeatures(INSTRUMENT, configuration)
    for index in range(7, 45):
        result = original.update(reference_bar(index))
        assert result["3_PSAR_7_PSAR"] is None
        if index < 29:
            assert not original.state[(3, 7)]["history"]
    assert original.state[(3, 7)]["previous"]["mode"] == 1
    result = original.update(reference_bar(45))
    assert result["3_PSAR_7_PSAR"] is not None and not result["CLOSED_3"]
    assert original.state[(3, 7)]["previous"]["PSAR"] is None
    saved = json.loads(json.dumps(original.checkpoint()))
    restored = LegacyParabolicFeatures(INSTRUMENT, configuration).restored(saved, INSTRUMENT)
    for index in range(46, 90):
        assert restored.update(reference_bar(index)) == original.update(reference_bar(index))
        assert restored.state == original.state


def test_duplicate_immutable_gap_and_changed_bar_failure_keep_full_checkpoint_unchanged():
    provider = LegacyParabolicFeatures(
        INSTRUMENT, (ParabolicConfiguration(0), ParabolicConfiguration(2))
    )
    for index in range(20):
        provider.update(reference_bar(index))
    saved = provider.checkpoint()
    assert provider.update(reference_bar(19)) == provider._result
    assert provider.checkpoint() == saved
    with pytest.raises(ValidationError, match="contiguous"):
        provider.update(reference_bar(21))
    assert provider.checkpoint() == saved
    with pytest.raises(ValidationError, match="contiguous"):
        provider.update(replace(reference_bar(19), high=reference_bar(19).high + 1))
    assert provider.checkpoint() == saved
    clean = deepcopy(provider)
    assert provider.update(reference_bar(20)) == clean.update(reference_bar(20))


def test_readonly_state_and_wrong_configuration_restore_do_not_mutate_active_provider():
    configuration = (ParabolicConfiguration(0),)
    provider = LegacyParabolicFeatures(INSTRUMENT, configuration)
    for index in range(20):
        provider.update(reference_bar(index))
    saved = provider.checkpoint()
    exposed = provider.state
    exposed[(0, 0)]["previous"]["AF"] = 123
    assert provider.checkpoint() == saved
    wrong = LegacyParabolicFeatures(INSTRUMENT, (ParabolicConfiguration(0, acceleration=0.01),))
    pristine = wrong.checkpoint()
    with pytest.raises(ValidationError):
        wrong.restored(json.loads(json.dumps(saved)), INSTRUMENT)
    assert wrong.checkpoint() == pristine and provider.checkpoint() == saved
