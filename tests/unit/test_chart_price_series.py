"""PSAR and SWING display jobs reuse common producers with causal confirmations."""

from decimal import Decimal

import pytest

from omega.analysis.catalog import create_features
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.chart_series import ChartSeriesService, parameters

MINUTE = 60000
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def specification(kind, period=1):
    return {
        "instrument": INSTRUMENT.key,
        "start_ms": 0,
        "end_ms": 60 * MINUTE,
        "period_minutes": period,
        "kind": kind,
        **(
            {"start": 0.02, "acceleration": 0.005, "maximum": 0.2, "price_precision": 8}
            if kind == "PSAR"
            else {"swing_range": 0.005}
        ),
    }


def candles():
    return tuple(
        Candle(
            INSTRUMENT,
            i * MINUTE,
            (i + 1) * MINUTE,
            Decimal(100 + i % 7),
            Decimal(103 + i % 7),
            Decimal(98 + i % 7),
            Decimal(101 + i % 7),
            Decimal(10),
        )
        for i in range(60)
    )


class Source:
    def __init__(self, rows):
        self.rows, self.closed = rows, False

    def read(self, _keys, start, end, *, limit, as_of_ms):
        assert limit <= 512 and as_of_ms == 61 * MINUTE
        return tuple(row for row in self.rows if start <= row.open_time_ms < end)

    def close(self):
        self.closed = True


@pytest.mark.parametrize("kind", ["PSAR", "SWING"])
@pytest.mark.parametrize("period,intervalID", [(1, 0), (3, 1), (5, 2)])
def test_common_producer_exact_confirmed_target_bars(kind, period, intervalID):
    spec, source = specification(kind, period), Source(candles())
    service = ChartSeriesService(lambda: source, clock_ms=lambda: 61 * MINUTE)
    fields = (
        ("PSAR", "DCC")
        if kind == "PSAR"
        else ("LSTIMESTAMP", "LSPRICE", "LSTYPE", "CONFIRMED_AT_MS")
    )
    public = tuple(f"{intervalID}_{kind}_0_{field}" for field in fields)
    group = "parabolic" if kind == "PSAR" else "swing"
    config = (
        {
            key: spec[key]
            for key in ("start", "acceleration", "maximum", "price_precision")
            if kind == "PSAR"
        }
        if kind == "PSAR"
        else {"swing_range": 0.005}
    )
    graph = create_features(
        INSTRUMENT, public, {group: [{"interval_id": intervalID, "line_index": 0, **config}]}
    )
    try:
        service.submit("one", spec)
        with service.condition:
            assert service.condition.wait_for(lambda: service.jobs["one"]["state"] == "complete", 5)
        result = service.status("one")["result"]
        expected = []
        for candle in source.rows:
            output = graph.update(candle)
            if output[f"CLOSED_{intervalID}"]:
                expected.append(tuple(output[key] for key in public))
        assert [tuple(row[field] for field in fields) for row in result["rows"]] == expected
        assert result["knowledge_mode"] == "retrospective_display"
        assert all(
            row["available_at_ms"] == result["produced_at_ms"] and row["segment"] == 0
            for row in result["rows"]
        )
        if kind == "SWING":
            assert any(row["LSTIMESTAMP"] is not None for row in result["rows"])
            assert all(
                row["CONFIRMED_AT_MS"] <= row["close_time_ms"]
                for row in result["rows"]
                if row["LSTIMESTAMP"] is not None
            )
        assert source.closed
    finally:
        service.close()


@pytest.mark.parametrize("kind", ["PSAR", "SWING"])
def test_gap_segments_reset_kernel_and_omit_partial_prefix(kind):
    rows = candles()
    source = Source(rows[:18] + rows[21:])
    service = ChartSeriesService(lambda: source, clock_ms=lambda: 61 * MINUTE)
    try:
        result = service._calculate(specification(kind, 5))
        assert result["quality"]["missing_minutes"] == 3
        assert result["quality"]["reset_count"] == 1
        assert {row["open_time_ms"] for row in result["rows"]}.isdisjoint(
            {15 * MINUTE, 20 * MINUTE}
        )
        assert [row["segment"] for row in result["rows"]][:3] == [0, 0, 0]
        assert all(
            row["segment"] == 1 for row in result["rows"] if row["open_time_ms"] >= 25 * MINUTE
        )
        field = "PSAR" if kind == "PSAR" else "LSTIMESTAMP"
        assert (
            next(row for row in result["rows"] if row["open_time_ms"] == 25 * MINUTE)[field] is None
        )
    finally:
        service.close()


@pytest.mark.parametrize(
    "kind,changes",
    [
        ("PSAR", {"start": 0.2}),
        ("PSAR", {"acceleration": True}),
        ("PSAR", {"price_precision": 17}),
        ("PSAR", {"maximum": float("nan")}),
        ("SWING", {"swing_range": 0.00001}),
        ("SWING", {"swing_range": False}),
        ("SWING", {"period_minutes": 4320}),
        ("SWING", {"nSamples": 14}),
    ],
)
def test_invalid_configuration_rejected_before_worker(kind, changes):
    with pytest.raises(ValidationError):
        parameters({**specification(kind), **changes})


@pytest.mark.parametrize(
    "kind,field",
    [("PSAR", "start"), ("PSAR", "acceleration"), ("PSAR", "maximum"), ("SWING", "swing_range")],
)
@pytest.mark.parametrize(
    "value", [True, Decimal("0.02"), "0.02", float("nan"), float("inf"), 10**1000]
)
def test_numeric_boundary_rejects_non_json_types_before_hash_or_worker(kind, field, value):
    with pytest.raises(ValidationError):
        parameters({**specification(kind), field: value})
