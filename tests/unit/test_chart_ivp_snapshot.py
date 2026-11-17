"""Selected IVP is a bounded replay of the unchanged common producer."""

import json
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from omega.analysis.volume_profile import LegacyVolumeProfileFeatures, ProfileConfiguration
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.chart_series import ChartSeriesService, parameters

MINUTE = 60000
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def spec(**changes):
    return {
        "instrument": INSTRUMENT.key,
        "kind": "IVP",
        "start_ms": 0,
        "end_ms": 30 * MINUTE,
        "period_minutes": 1,
        "nSamples": 3,
        "gammaFactor": 0.02,
        "deltaFactor": 1.0,
        "prominence": 0.1,
        "distance": 5,
        "height": 0.5,
        "price_precision": 8,
        "max_divisions": 2048,
        **changes,
    }


def candles(count=40, volume=10):
    return tuple(
        Candle(
            INSTRUMENT,
            i * MINUTE,
            (i + 1) * MINUTE,
            Decimal(100 + i % 3),
            Decimal(103 + i % 3),
            Decimal(99 + i % 3),
            Decimal(101 + i % 3),
            Decimal(volume),
        )
        for i in range(count)
    )


class Source:
    def __init__(self, rows):
        self.rows, self.calls, self.closed = rows, [], False

    def read(self, keys, start, end, *, limit, as_of_ms):
        assert keys == (INSTRUMENT.key,) and limit <= 512
        self.calls.append((start, end, as_of_ms))
        return tuple(c for c in self.rows if start <= c.open_time_ms < end)

    def close(self):
        self.closed = True


def calculate(rows, configuration=None):
    source = Source(rows)
    service = ChartSeriesService(lambda: source, clock_ms=lambda: 100 * MINUTE)
    try:
        result = service._calculate(parameters(configuration or spec()))
        assert source.closed
        return result, source
    finally:
        service.close()


@pytest.mark.parametrize("period,interval", [(1, 0), (3, 1), (5, 2)])
def test_selected_profile_exact_common_kernel_and_no_future_rows(period, interval):
    pytest.importorskip("scipy")
    configuration = spec(period_minutes=period)
    result, source = calculate(candles(), configuration)
    config = ProfileConfiguration(interval, nSamples=3, gammaFactor=0.02, max_divisions=2048)
    graph = LegacyVolumeProfileFeatures(INSTRUMENT, (config,))
    for candle in candles(30):
        graph.update(candle)
    expected = graph.state["raw"][interval]
    selected = result["rows"][0]
    assert selected["state"] == "ready"
    for public, original in (
        ("filtered", "volumePriceLevelProfile_Filtered"),
        ("maximum", "volumePriceLevelProfile_Filtered_Max"),
        ("boundaries", "volumePriceLevelProfile_Boundaries"),
        ("near_boundaries", "volumePriceLevelProfile_NearBoundaries"),
    ):
        assert (
            selected[public] == list(expected[original])
            if public == "near_boundaries"
            else selected[public] == expected[original]
        )
    assert selected["close_time_ms"] == 30 * MINUTE
    assert result["replay_start_ms"] == 0 and len(result["rows"]) == 1
    assert max(call[1] for call in source.calls) == 30 * MINUTE
    assert result["quality"]["observed_minutes"] == 30
    assert result["knowledge_mode"] == "retrospective_display"
    assert calculate(candles(30), configuration)[0] == result


@pytest.mark.parametrize(
    "state,rows,configuration",
    [
        ("seed", candles(2), spec(end_ms=2 * MINUTE, nSamples=5)),
        ("zero", candles(4, volume=0), spec(end_ms=4 * MINUTE)),
        ("missing", candles(29), spec()),
    ],
)
def test_seed_zero_and_missing_are_not_ready_or_manufactured_zero(state, rows, configuration):
    pytest.importorskip("scipy")
    result, _ = calculate(rows, configuration)
    row = result["rows"][0]
    assert row["state"] == state and row["filtered"] is row["maximum"] is None
    assert (row["divisions"] is None) == (state != "zero")


def test_gap_resets_producer_and_does_not_inherit_old_distribution():
    pytest.importorskip("scipy")
    rows = candles()
    result, _ = calculate(rows[:10] + rows[20:])
    suffix, _ = calculate(rows[20:], spec(start_ms=20 * MINUTE))
    assert result["rows"] == suffix["rows"]
    assert result["quality"]["missing_minutes"] == 10
    assert result["quality"]["reset_count"] == 1
    assert result["source_checksum"] != suffix["source_checksum"]


@pytest.mark.parametrize(
    "changes",
    [
        {"max_divisions": 2049},
        {"max_divisions": True},
        {"distance": False},
        {"gammaFactor": float("nan")},
        {"gammaFactor": 10**400},
        {"nSamples": 1},
        {"period_minutes": 3, "end_ms": 29 * MINUTE},
        {"extra": 1},
        {"start_ms": -MINUTE},
        {"end_ms": 10001 * MINUTE},
    ],
)
def test_bad_display_request_rejected_before_io(changes):
    with pytest.raises(ValidationError):
        parameters(spec(**changes))


def test_optional_dependency_failure_and_capacity_are_explicit_job_failures(monkeypatch):
    source = Source(candles())
    owner = ChartSeriesService(lambda: source, clock_ms=lambda: 100 * MINUTE)

    def unavailable():
        raise ValidationError("optional numerical dependency unavailable")

    monkeypatch.setattr("omega.analysis.volume_profile._libraries", unavailable)
    try:
        owner.submit("ivp", spec())
        with owner.condition:
            assert owner.condition.wait_for(lambda: owner.jobs["ivp"]["state"] == "failed", 5)
        assert owner.status("ivp")["result"] is None and source.closed
    finally:
        owner.close()


def test_profile_cache_counts_bins_not_only_selected_rows():
    owner = ChartSeriesService(lambda: None, maximum_cached_rows=10000)
    for index in range(6):
        owner.jobs[str(index)] = {
            "state": "complete",
            "result": {"rows": [{"filtered": [1.0] * 2048}]},
        }
    owner._expire_results()
    assert [row["state"] for row in owner.jobs.values()] == ["expired"] * 2 + ["complete"] * 4
    owner.close()


@pytest.mark.parametrize("name", ["rebin-minute", "five-minute"])
def test_selected_snapshot_matches_frozen_original_eta_distribution_hash(name):
    pytest.importorskip("scipy")
    fixture = json.loads(Path("tests/fixtures/volume-profile-legacy.json").read_text())
    case = next(case for case in fixture["cases"] if case["name"] == name)
    config = dict(case["configuration"])
    period = {0: 1, 2: 5}[config.pop("interval_id")]
    rows = tuple(
        Candle(INSTRUMENT, i * MINUTE, (i + 1) * MINUTE, *map(Decimal, values))
        for i, values in enumerate(fixture["inputs"])
    )
    source = Source(rows)
    owner = ChartSeriesService(lambda: source, clock_ms=lambda: 601 * MINUTE)
    try:
        output = owner._calculate(
            parameters(spec(end_ms=600 * MINUTE, period_minutes=period, **config))
        )
        row = output["rows"][0]
        assert (
            sha256(json.dumps(row["filtered"], separators=(",", ":")).encode()).hexdigest()
            == case["trace"][-1]["filtered_sha256"]
        )
        assert row["divisionHeight"] == case["trace"][-1]["raw"]["divisionHeight"]
        assert (
            row["near_boundaries"]
            == case["trace"][-1]["raw"]["volumePriceLevelProfile_NearBoundaries"]
        )
        assert source.closed
    finally:
        owner.close()
