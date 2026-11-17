"""Prepared display-path parity, bounds, causal pins and owner cleanup checks."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from omega.analysis.aggregation import INTERVAL_MINUTES, CandleAggregation
from omega.domain.candle_content import candle_content_pin
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.chart_price_windows import (
    calculate_price_windows,
    validate_price_window_settings,
)

MINUTE = 60000
INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")
FIXTURE = json.loads(
    (Path(__file__).parents[1] / "fixtures/eta_price_windows/cases.json").read_text()
)


def candle(minute, close="100.125"):
    price = Decimal(close)
    return Candle(
        INSTRUMENT,
        minute * MINUTE,
        (minute + 1) * MINUTE,
        price,
        price,
        price,
        price,
        Decimal(1),
    )


def definition(code="SMA", line=0, **settings):
    return {
        "analysis_code": code,
        "line_index": line,
        "nSamples": 2,
        "price_precision": 8,
        **({"MAType": "SMA", "bandWidth": 2} if code == "BOL" else {}),
        **settings,
    }


def specification(start=0, end=12, period=1, display=None, definitions=None):
    return {
        "instrument": INSTRUMENT.key,
        "kind": "PRICE_WINDOWS",
        "start_ms": start * MINUTE,
        "end_ms": end * MINUTE,
        "display_start_ms": (start if display is None else display) * MINUTE,
        "period_minutes": period,
        "definitions": [definition()] if definitions is None else definitions,
    }


class Source:
    def __init__(self, candles):
        self.candles, self.reads, self.closes = tuple(candles), [], 0

    def read(self, keys, start, end, *, limit, as_of_ms):
        self.reads.append((keys, start, end, limit, as_of_ms))
        return tuple(row for row in self.candles if start <= row.open_time_ms < end)

    def close(self):
        self.closes += 1

    def get_auxiliary(self, *args, **kwargs):
        pytest.fail("candle-only overlays must not read auxiliary inputs")


class Owner:
    def __init__(self, source, now=1_000_000_000):
        self.source, self.now, self.factories, self.stops = source, now, 0, 0

    def source_factory(self):
        self.factories += 1
        return self.source

    def _now(self):
        return self.now

    def _check_stop(self):
        self.stops += 1


def calculate(spec, candles):
    source = Source(candles)
    result = calculate_price_windows(Owner(source), spec, INSTRUMENT)
    assert source.closes == 1
    return result


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda case: case["name"])
def test_price_main_frozen_oracle_closed_rows(case):
    configuration = case["configuration"]
    settings = definition(
        configuration["family"],
        configuration["line_index"],
        **{key: configuration[key] for key in ("nSamples", "price_precision")},
        **(
            {key: configuration[key] for key in ("MAType", "bandWidth")}
            if configuration["family"] == "BOL"
            else {}
        ),
    )
    trace = case["trace"]
    spec = specification(
        trace[0]["minute"],
        trace[-1]["minute"] + 1,
        INTERVAL_MINUTES[configuration["interval_id"]],
        definitions=[settings],
    )
    inputs = FIXTURE["inputs"][configuration["pattern"]]
    result = calculate(
        spec, (candle(row["minute"], inputs[row["minute"]]["close"]) for row in trace)
    )
    expected = [row for row in trace if row["closed"]]
    assert len(result["rows"]) == len(expected)
    for actual, frozen in zip(result["rows"], expected, strict=True):
        assert actual["open_time_ms"] == frozen["bucket_open_ms"]
        public = frozen["public"]
        assert actual["values"] == [public[key] for key in result["columns"]]
        assert actual["ready"] == [all(value is not None for value in actual["values"])]


def test_price_main_batch_one_aggregation(monkeypatch):
    calls, original = [], CandleAggregation.update

    def observe(aggregation, row):
        calls.append(row.open_time_ms)
        return original(aggregation, row)

    monkeypatch.setattr(CandleAggregation, "update", observe)
    definitions = [definition("WMA", 3), definition("SMA", 1), definition("BOL", 2)]
    spec = specification(period=3, definitions=definitions)
    source, before = Source(candle(i) for i in range(12)), deepcopy(spec)
    owner = Owner(source)
    result = calculate_price_windows(owner, spec, INSTRUMENT)
    assert calls == [i * MINUTE for i in range(12)]
    assert spec == before and owner.factories == source.closes == 1
    assert result["definitions"] == [definitions[i] for i in (2, 1, 0)]
    assert result["columns"] == [
        "1_BOL_2_BOLLOW",
        "1_BOL_2_BOLHIGH",
        "1_BOL_2_MA",
        "1_SMA_1_SMA",
        "1_WMA_3_WMA",
    ]
    assert result["rows"][0]["values"] == [None] * 5
    assert result["rows"][0]["ready"] == [False] * 3
    assert result["rows"][-1]["ready"] == [True] * 3


@pytest.mark.parametrize("period", INTERVAL_MINUTES.values())
def test_every_supported_period_requires_actual_complete_boundary(period):
    result = calculate(
        specification(end=2 * period + 1, period=period),
        (candle(i) for i in range(2 * period + 1)),
    )
    expected = 2 * period + 1 if period == 1 else 2
    assert len(result["rows"]) == expected
    assert result["rows"][1]["values"] == [100.125]


def test_origin_gap_partial_and_display_motion_do_not_reseed():
    candles = [candle(i, str(100 + i)) for i in range(1, 23) if i != 10]
    spec = specification(start=1, end=23, period=3)
    result = calculate(spec, candles)
    assert [(row["open_time_ms"] // MINUTE, row["segment"]) for row in result["rows"]] == [
        (3, 0),
        (6, 0),
        (12, 1),
        (15, 1),
        (18, 1),
    ]
    assert result["rows"][0]["values"] == [None]
    assert result["rows"][2]["values"] == [None]
    assert result["quality"] == {
        "requested_minutes": 22,
        "observed_minutes": 21,
        "missing_minutes": 1,
        "reset_count": 1,
        "complete_bars": 5,
    }
    moved = calculate({**spec, "display_start_ms": 15 * MINUTE}, candles)
    assert moved["rows"] == result["rows"][-2:]
    assert moved["source_checksum"] == result["source_checksum"]
    assert moved["display_content_pin"] != result["display_content_pin"]


@pytest.mark.parametrize("width", [-2, 0, 2])
def test_signed_band_names_zero_and_negative_lower_prices(width):
    result = calculate(
        specification(end=2, definitions=[definition("BOL", bandWidth=width)]),
        [candle(0, "1"), candle(1, "100")],
    )
    low, high, center = result["rows"][-1]["values"]
    assert center == 50.5 and result["rows"][-1]["ready"] == [True]
    if width < 0:
        assert low > high
    elif width == 0:
        assert low == high == center
    else:
        assert low < 0 < high


def test_valid_rounded_zero_is_ready_and_empty_source_is_absence():
    result = calculate(
        specification(end=2, definitions=[definition(price_precision=0)]),
        [candle(0, ".1"), candle(1, ".1")],
    )
    assert result["rows"][-1]["values"] == [0.0]
    assert result["rows"][-1]["ready"] == [True]
    empty = calculate(specification(), [])
    assert empty["rows"] == [] and empty["quality"]["missing_minutes"] == 12


@pytest.mark.parametrize("width", [True, "2", float("nan"), float("inf"), 10**1000])
def test_invalid_native_bandwidth_rejected_before_source(width):
    owner = Owner(Source([]))
    with pytest.raises(ValidationError):
        calculate_price_windows(
            owner, specification(definitions=[definition("BOL", bandWidth=width)]), INSTRUMENT
        )
    assert owner.factories == 0


@pytest.mark.parametrize(
    "settings",
    [
        {"nSamples": True},
        {"nSamples": 1},
        {"nSamples": 20001},
        {"price_precision": 17},
        {"price_precision": False},
        {"line_index": -1},
        {"line_index": 10},
        {"line_index": 0.0},
        {"visible": True},
        {"analysis_code": "EMA"},
    ],
)
def test_strict_definition_fields_and_native_types(settings):
    with pytest.raises(ValidationError):
        validate_price_window_settings(specification(definitions=[definition(**settings)]))


def test_canonical_order_detached_types_duplicates_and_bounded_work():
    spec = specification(definitions=[definition("BOL", bandWidth=-0.0), definition("WMA")])
    checked = validate_price_window_settings(spec)
    assert type(checked["definitions"][0]["bandWidth"]) is float
    checked["definitions"][0]["nSamples"] = 99
    assert spec["definitions"][0]["nSamples"] == 2
    for definitions in ([], [definition(), definition()], [definition()] * 31):
        with pytest.raises(ValidationError):
            validate_price_window_settings(specification(definitions=definitions))
    maximum = [definition("SMA", line, nSamples=20000) for line in range(4)]
    maximum.append(definition("BOL", nSamples=19985))
    validate_price_window_settings(specification(definitions=maximum))
    maximum[-1]["nSamples"] += 1
    with pytest.raises(ValidationError, match="retained state"):
        validate_price_window_settings(specification(definitions=maximum))
    validate_price_window_settings(
        specification(end=10000, definitions=[definition(), definition("WMA")])
    )
    with pytest.raises(ValidationError, match="scalar slots"):
        validate_price_window_settings(specification(end=6667, definitions=[definition("BOL")]))


@pytest.mark.parametrize("display", [-MINUTE, True, 1, 12 * MINUTE])
def test_invalid_display_start_is_rejected(display):
    with pytest.raises(ValidationError):
        validate_price_window_settings({**specification(), "display_start_ms": display})


def test_exact_content_pins_keep_values_beyond_display_float_precision():
    first, other = "100.00000000000000000001", "100.00000000000000000002"
    assert float(first) == float(other)
    spec = specification(end=2, display=1)
    result = calculate(spec, [candle(0), candle(1, first)])
    changed = calculate(spec, [candle(0), candle(1, other)])
    equal = calculate(spec, [candle(0, "100.12500"), candle(1, first + "00")])
    assert result["display_content_pin"] != changed["display_content_pin"]
    assert result["display_content_pin"] == equal["display_content_pin"]
    rows = [(MINUTE, *([Decimal(first)] * 4), Decimal(1))]
    assert result["display_content_pin"] == candle_content_pin(
        INSTRUMENT.key, MINUTE, 2 * MINUTE, rows
    )


def test_one_fixed_cutoff_across_bounded_chunks_and_actual_completion_time():
    source, clock = Source(candle(i) for i in range(1025)), [2_000_000_000]
    owner = Owner(source)
    owner._now = lambda: clock[0]
    original = source.read

    def read(*args, **kwargs):
        result = original(*args, **kwargs)
        clock[0] += 123
        return result

    source.read = read
    result = calculate_price_windows(owner, specification(end=1025), INSTRUMENT)
    assert len(source.reads) == 3 and {row[4] for row in source.reads} == {2_000_000_000}
    assert all(row[3] == 512 for row in source.reads)
    assert result["produced_at_ms"] == 2_000_000_369
    assert all(row["available_at_ms"] == result["produced_at_ms"] for row in result["rows"])
    assert source.closes == 1


@pytest.mark.parametrize(
    "fault", ["duplicate", "order", "instrument", "type", "timestamp", "value"]
)
def test_malformed_source_fails_and_closes_exact_original_owner(fault):
    rows = [candle(0), candle(1)]
    if fault == "duplicate":
        rows[1] = rows[0]
    elif fault == "order":
        rows.reverse()
    elif fault == "instrument":
        rows[1] = replace(rows[1], instrument=Instrument("binance", Market.SPOT, "ETHUSDT"))
    elif fault == "type":
        rows[1] = None
    elif fault == "timestamp":
        object.__setattr__(rows[1], "open_time_ms", float(MINUTE))
    else:
        object.__setattr__(rows[1], "close", Decimal("NaN"))
    source = Source([])
    source.read = lambda *args, **kwargs: tuple(rows)
    with pytest.raises(ProtocolError):
        calculate_price_windows(Owner(source), specification(end=2), INSTRUMENT)
    assert source.closes == 1


@pytest.mark.parametrize("fault", ["list", "over_limit", "outside_chunk", "non_minute"])
def test_source_batch_shape_and_exact_range_are_strict(fault):
    source = Source([])
    rows = [candle(0)]
    if fault == "over_limit":
        rows *= 3
    elif fault == "outside_chunk":
        rows = [candle(2)]
    elif fault == "non_minute":
        object.__setattr__(rows[0], "open_time_ms", 1)
    source.read = lambda *args, **kwargs: rows if fault == "list" else tuple(rows)
    with pytest.raises(ProtocolError):
        calculate_price_windows(Owner(source), specification(end=2), INSTRUMENT)
    assert source.closes == 1


@pytest.mark.parametrize("selected", ["sma", True, None, []])
def test_bollinger_center_enum_is_explicit(selected):
    with pytest.raises(ValidationError):
        validate_price_window_settings(
            specification(definitions=[definition("BOL", MAType=selected)])
        )


@pytest.mark.parametrize("fault", ["read", "cancel", "pin", "clock", "producer", "close"])
def test_failure_and_cancellation_always_close_once(monkeypatch, fault):
    from omega.runtime import chart_price_windows as module

    source, spec = Source([candle(0)]), specification(end=1)
    owner = Owner(source)

    def fail(*args, **kwargs):
        raise StorageError("injected boundary failure")

    if fault == "read":
        source.read = fail
    elif fault == "cancel":
        owner._check_stop = lambda: (_ for _ in ()).throw(InterruptedError())
    elif fault == "pin":
        monkeypatch.setattr(module, "candle_content_pin", fail)
    elif fault == "clock":
        clocks = iter([10 * MINUTE, 9 * MINUTE])
        owner._now = lambda: next(clocks)
    elif fault == "producer":

        class InvalidGraph:
            def update(self, candle):
                return {"CLOSED_0": True, "0_SMA_0_SMA": float("inf")}

        monkeypatch.setattr(module, "create_features", lambda *args: InvalidGraph())
    else:

        def close():
            source.closes += 1
            fail()

        source.close = close
    with pytest.raises((StorageError, InterruptedError, ValidationError, ProtocolError)):
        calculate_price_windows(owner, spec, INSTRUMENT)
    assert source.closes == 1


def test_clock_before_target_never_opens_source():
    owner = Owner(Source([]), now=MINUTE - 1)
    with pytest.raises(ValidationError):
        calculate_price_windows(owner, specification(end=1), INSTRUMENT)
    assert owner.factories == 0
