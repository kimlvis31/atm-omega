"""AST-extracted Eta arithmetic fixtures and independent causal SWING contracts."""

import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from omega.analysis.catalog import create_features, validate_configuration
from omega.analysis.swing import LegacySwingFeatures, SwingConfiguration
from omega.application.paper import PaperTradingService
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.paper_feed import PaperFeedSupervisor
from omega.strategies.registry import StrategyRegistry
from omega.strategies.tef.module import TefModule

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")
REFERENCE = json.loads(Path("tests/fixtures/swing-legacy.json").read_text(encoding="utf-8"))


def bar(index, high="110", low="100", *, instrument=INSTRUMENT):
    high, low = Decimal(high), Decimal(low)
    middle = (high + low) / 2
    return Candle(
        instrument, index * 60000, (index + 1) * 60000, middle, high, low, middle, Decimal(1)
    )


@pytest.mark.parametrize(
    "case", REFERENCE["cases"], ids=lambda item: f"{item['interval_id']}-{item['swing_range']}"
)
def test_ast_extracted_legacy_closed_and_higher_interval_preview_parity(case):
    feature = LegacySwingFeatures(
        INSTRUMENT, (SwingConfiguration(case["interval_id"], swing_range=case["swing_range"]),)
    )
    prefix = f"{case['interval_id']}_SWING_0_"
    for index, (row, expected) in enumerate(
        zip(REFERENCE["inputs"], case["expected"], strict=True)
    ):
        candle = Candle(
            INSTRUMENT, index * 60000, (index + 1) * 60000, *(Decimal(value) for value in row)
        )
        result = feature.update(candle)
        for suffix in ("LSTIMESTAMP", "LSPRICE", "LSTYPE"):
            assert result[prefix + suffix] == expected[suffix], (index, suffix, result, expected)
    state = feature.state[(case["interval_id"], 0)]["search"]
    for key, value in case["committed_search"].items():
        expected_value = value * 1000 if key.endswith("_ts") else value
        assert state[key] == expected_value


def test_extreme_update_precedes_same_bar_reversal_and_threshold_is_strict():
    feature = LegacySwingFeatures(INSTRUMENT, (SwingConfiguration(0, swing_range=0.1),))
    seed = feature.update(bar(0))
    lower = feature.update(bar(1, "110", "99"))
    equal = feature.update(bar(2, "108.9", "99"))
    assert all(row["0_SWING_0_LSPRICE"] is None for row in (seed, lower, equal))
    confirmed = feature.update(bar(3, "109", "99"))
    assert confirmed["0_SWING_0_LSTIMESTAMP"] == 60
    assert confirmed["0_SWING_0_LSPRICE"] == 99
    assert confirmed["0_SWING_0_LSTYPE"] == -1
    assert confirmed["0_SWING_0_CONFIRMED_AT_MS"] == 240000
    new_high = feature.update(bar(4, "120", "98"))
    equality = feature.update(bar(5, "120", "108"))
    assert new_high["0_SWING_0_LSTYPE"] == equality["0_SWING_0_LSTYPE"] == -1
    high_confirmed = feature.update(bar(6, "120", "107"))
    assert high_confirmed["0_SWING_0_LSTIMESTAMP"] == 240
    assert high_confirmed["0_SWING_0_LSPRICE"] == 120
    assert high_confirmed["0_SWING_0_LSTYPE"] == 1
    assert high_confirmed["0_SWING_0_CONFIRMED_AT_MS"] == 420000
    assert seed["0_SWING_0_LSPRICE"] is None  # No retrospective publication at pivot minute.


def test_gap_or_conflict_never_mutates_state_and_duplicate_is_a_noop():
    feature = LegacySwingFeatures(INSTRUMENT, (SwingConfiguration(0),))
    feature.update(bar(0))
    expected = feature.update(bar(1))
    state = feature.state
    assert feature.update(bar(1)) == expected and feature.state == state
    for invalid in (bar(3), bar(1, "111", "100")):
        with pytest.raises(ValidationError, match="contiguous"):
            feature.update(invalid)
        assert feature.state == state
    feature.update(bar(2))


def test_deepcopy_restart_matches_uninterrupted_and_fresh_replay():
    definitions = (SwingConfiguration(0), SwingConfiguration(2, 3, 0.03))
    first = LegacySwingFeatures(INSTRUMENT, definitions)
    rows = [
        Candle(INSTRUMENT, index * 60000, (index + 1) * 60000, *(Decimal(value) for value in row))
        for index, row in enumerate(REFERENCE["inputs"])
    ]
    for row in rows[:100]:
        first.update(row)
    cloned = deepcopy(first)
    replay = LegacySwingFeatures(INSTRUMENT, definitions)
    for row in rows[:100]:
        replay.update(row)
    for row in rows[100:]:
        assert first.update(row) == cloned.update(row) == replay.update(row)
    assert first.state == cloned.state == replay.state
    detached = first.state
    detached[(0, 0)]["search"]["min"] = -1
    assert first.state[(0, 0)]["search"]["min"] > 0


def test_incomplete_initial_higher_bar_never_seeds_search():
    feature = LegacySwingFeatures(INSTRUMENT, (SwingConfiguration(2),))
    for minute in range(1, 5):
        output = feature.update(bar(minute))
        assert output["2_SWING_0_LSPRICE"] is None and not output["CLOSED_2"]
    assert feature.state[(2, 0)]["search"] is None
    for minute in range(5, 10):
        feature.update(bar(minute))
    assert feature.state[(2, 0)]["search"] is not None


def test_overflow_is_rejected_without_advancing_search_or_aggregation():
    feature = LegacySwingFeatures(INSTRUMENT, (SwingConfiguration(0, swing_range="1e308"),))
    feature.update(bar(0))
    before = feature.state
    with pytest.raises(ValidationError, match="overflow"):
        feature.update(bar(1))
    assert feature.state == before
    assert feature._last.open_time_ms == feature._aggregation._last.open_time_ms == 0


@pytest.mark.parametrize(
    "parameters",
    [
        {"interval_id": True},
        {"interval_id": 13},
        {"interval_id": 0, "line_index": 10},
        {"interval_id": 0, "line_index": True},
        {"interval_id": 0, "swing_range": "NaN"},
        {"interval_id": 0, "swing_range": True},
        {"interval_id": 0, "swing_range": "0.00001"},
    ],
)
def test_invalid_configuration_is_rejected(parameters):
    with pytest.raises(ValidationError):
        SwingConfiguration(**parameters)


def test_saved_schema_coverage_line_index_and_old_canonical_hash_compatibility():
    old = validate_configuration({"moving_averages": [{"interval_id": 0, "ema_period": 2}]})
    assert set(old) == {"moving_averages", "momentum"}
    value = {"swing": [{"interval_id": 0, "line_index": 3, "swing_range": "0.01"}]}
    canonical = validate_configuration(value)
    assert canonical["swing"][0]["swing_range"] == 0.01
    required = {"0_SWING_3_LSPRICE", "0_SWING_3_LSTYPE"}
    graph = create_features(INSTRUMENT, required, value)
    for minute in range(3):
        result = graph.update(bar(minute))
    assert result["0_SWING_3_LSPRICE"] is not None and "0_SWING_0_LSPRICE" not in result
    with pytest.raises(ValidationError, match="does not provide"):
        create_features(INSTRUMENT, {"0_SWING_0_LSPRICE"}, value)
    with pytest.raises(ValidationError, match="duplicate"):
        validate_configuration({"swing": [value["swing"][0], value["swing"][0]]})


def test_spddefault_required_fields_are_now_resolved_by_the_real_catalog():
    module = TefModule(INSTRUMENT, {"model": "SPDDEFAULT"})
    graph = create_features(INSTRUMENT, module.descriptor.required_fields)
    assert graph.update(bar(0))["0_SWING_0_LSPRICE"] is None
    output = graph.update(bar(1))
    assert output["CLOSED_0"] is True and output["0_SWING_0_LSTYPE"] == -1
    assert module.model.evaluate(bar(1), output) is not None


def test_saved_swing_to_spd_module_to_real_paper_feed_without_warmup_trades():
    paper = PaperTradingService(StrategyRegistry().create, create_features)
    paper.command("paper.account.create", {"account_id": "a", "balance": "10000", "fee_rate": "0"})
    paper.command(
        "paper.strategy.add",
        {
            "account_id": "a",
            "instance": "s",
            "instrument": INSTRUMENT.key,
            "module_id": "tef",
            "budget": "1000",
            "configuration": {"model": "SPDDEFAULT"},
            "analysis": {"swing": [{"interval_id": 0, "line_index": 0, "swing_range": 0.005}]},
        },
    )
    rows = [bar(minute) for minute in range(4)]

    class Source:
        def read(self, keys, start, end):
            return tuple(row for row in rows if start <= row.open_time_ms < end)

        def close(self):
            pass

    now = [180000]
    paper.feed_configure(True, start_ms=120000, warmup_minutes=2)
    feed = PaperFeedSupervisor(paper, Source(), clock_ms=lambda: now[0])
    paper.command("resume", {})
    feed.run_once()
    assert not paper.data["candles"] and paper.detail("s")["pending"] is None
    feed.run_once()
    assert paper.detail("s")["status"] == "ready" and paper.detail("s")["pending"] is not None
    now[0] = 240000
    feed.run_once()
    assert len(paper.detail("s")["fills"]) == 1
    assert paper.snapshot()["feed"]["provenance"][INSTRUMENT.key] == "database_committed"
    feed.close()
