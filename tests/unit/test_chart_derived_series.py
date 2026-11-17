"""Canonical NES committed frames and TPD candle-only display contracts."""

import json
import threading
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from omega.analysis.catalog import create_features
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market
from omega.ports.analysis_inputs import PreparedMarketFrame
from omega.ports.auxiliary import StoredAuxiliary
from omega.runtime.chart_series import ChartSeriesService, parameters

MINUTE = 60000
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def spec(kind="NES", **changes):
    return {
        "instrument": INSTRUMENT.key,
        "kind": kind,
        "start_ms": 0,
        "end_ms": 30 * MINUTE,
        "period_minutes": 1,
        **(
            {"nSamples": 3, "quote_precision": 8}
            if kind == "NES"
            else {"viewLength": 3, "nSamples": 5, "nSamplesMA": 3}
        ),
        **changes,
    }


def candle(index):
    return Candle(
        INSTRUMENT,
        index * MINUTE,
        (index + 1) * MINUTE,
        *map(Decimal, (100 + index % 3, 104 + index % 3, 98 + index % 3, 102 + index % 3, 10)),
    )


def prepared(index, as_of, *, missing=False, partial=False, zero=False, revision=1):
    c = candle(index)
    observation = AuxiliaryInput(
        "trade_flow",
        ()
        if missing
        else (
            ("AGGTRADE_NOTIONALBUY", Decimal(0 if zero else index * 3 + 10)),
            ("AGGTRADE_NOTIONALSELL", Decimal(0 if zero else index + 5)),
        ),
        c.open_time_ms,
        c.close_time_ms,
        c.close_time_ms,
        as_of if missing else c.close_time_ms + 1,
        "missing" if missing else "partial" if partial else "observed",
        Decimal(0) if missing else Decimal("0.5") if partial else Decimal(1),
        INSTRUMENT.key + ":aggTrade",
    )
    pins = (
        ()
        if missing
        else (
            StoredAuxiliary(
                INSTRUMENT,
                observation,
                revision,
                str(revision) * 64,
                None if revision == 1 else "1" * 64,
            ),
        )
    )
    return PreparedMarketFrame(MarketFeatureFrame(c, as_of, (observation,), c.close_time_ms), pins)


class Source:
    def __init__(self, count=30, *, missing=(), partial=(), omitted=(), zero=False, revision=1):
        self.count, self.missing, self.partial, self.omitted, self.zero, self.revision = (
            count,
            missing,
            partial,
            omitted,
            zero,
            revision,
        )
        self.closed = False
        self.frame_calls, self.candle_calls = [], []

    def read_frames(
        self,
        instrument,
        start,
        end,
        *,
        as_of_ms,
        channels,
        maximum_metric_age_ms,
        retrospective_until_ms,
        limit,
    ):
        assert (
            instrument == INSTRUMENT and channels == ("trade_flow",) and maximum_metric_age_ms == 0
        )
        assert retrospective_until_ms >= end and limit <= 64
        self.frame_calls.append((start, end, as_of_ms, threading.get_ident()))
        return tuple(
            prepared(
                i,
                as_of_ms,
                missing=i in self.missing,
                partial=i in self.partial,
                zero=self.zero,
                revision=self.revision,
            )
            for i in range(self.count)
            if start <= i * MINUTE < end and i not in self.omitted
        )

    def read(self, keys, start, end, *, limit, as_of_ms):
        assert keys == (INSTRUMENT.key,) and limit <= 512
        self.candle_calls.append((start, end, as_of_ms))
        return tuple(
            candle(i)
            for i in range(self.count)
            if start <= i * MINUTE < end and i not in self.omitted
        )

    def close(self):
        self.closed = True


def calculate(source, configuration=None, clock=100 * MINUTE):
    owner = ChartSeriesService(lambda: source, clock_ms=lambda: clock)
    try:
        output = owner._calculate(parameters(configuration or spec()))
        assert source.closed
        return output
    finally:
        owner.close()


@pytest.mark.parametrize("period,iid", [(1, 0), (3, 1), (5, 2)])
def test_nes_three_fields_exact_common_frames_and_missing_held_masking(period, iid):
    source = Source(missing=(12,), partial=(19,))
    config = spec(period_minutes=period)
    output = calculate(source, config)
    public = tuple(f"{iid}_NES_0_{key}" for key in ("NES", "NESABSMA", "NESABSMAREL"))
    graph = create_features(
        INSTRUMENT,
        public,
        {
            "order_flow": [
                {
                    "interval_id": iid,
                    "analysis_code": "NES",
                    "nSamples": 3,
                    "quote_precision": 8,
                    "line_index": 0,
                }
            ]
        },
    )
    expected = []
    for i in range(30):
        values = graph.update_frame(
            prepared(i, 100 * MINUTE, missing=i == 12, partial=i == 19).frame
        )
        if values[f"CLOSED_{iid}"]:
            expected.append(tuple(values[field] for field in public))
    assert [
        tuple(row[field] for field in ("NES", "ABSMA", "ABSMAREL")) for row in output["rows"]
    ] == expected
    assert source.frame_calls and not source.candle_calls
    tainted = [row for row in output["rows"] if not row["input_ready"]]
    assert tainted and all(row["NES"] is row["ABSMA"] is row["ABSMAREL"] is None for row in tainted)
    assert output["quality"]["auxiliary_missing_minutes"] == 1
    assert output["quality"]["auxiliary_partial_minutes"] == 1
    assert output["auxiliary_source"] == INSTRUMENT.key + ":aggTrade"


def test_zero_flow_is_observed_zero_after_seed_not_missing():
    output = calculate(Source(zero=True))
    assert output["rows"][0]["NES"] is None
    assert output["rows"][-1]["NES"] == 0 and output["rows"][-1]["ABSMAREL"] == 0
    assert all(row["input_ready"] for row in output["rows"])


def test_candle_gap_resets_while_missing_auxiliary_preserves_masked_internal_hold():
    output = calculate(Source(omitted=(12, 13, 14)))
    assert output["quality"]["reset_count"] == 1 and output["quality"]["missing_minutes"] == 3
    assert next(row for row in output["rows"] if row["open_time_ms"] == 15 * MINUTE)["NES"] is None
    held = calculate(Source(missing=(12,)))
    assert held["quality"]["reset_count"] == 0
    assert held["rows"][12]["NES"] is None and held["rows"][13]["NES"] is not None


def test_revision_identity_and_actual_source_availability_enter_snapshot_checksum():
    first = calculate(Source(revision=1))
    revised = calculate(Source(revision=2))
    assert first["source_checksum"] != revised["source_checksum"]
    assert first["rows"][0]["auxiliary_checksum"] != revised["rows"][0]["auxiliary_checksum"]
    assert first["rows"][0]["source_available_at_ms"] == MINUTE + 1
    missing = calculate(Source(missing=tuple(range(30))))
    assert all(row["source_available_at_ms"] is None for row in missing["rows"])


def test_chunk_bound_one_cutoff_source_owner_close_and_explicit_nes_range():
    source = Source(count=140)
    owner = ChartSeriesService(lambda: source, clock_ms=lambda: 141 * MINUTE)
    try:
        owner.submit("nes", spec(end_ms=140 * MINUTE))
        with owner.condition:
            assert owner.condition.wait_for(
                lambda: owner.jobs["nes"]["state"] in {"complete", "failed"}, 5
            )
        assert owner.status("nes")["state"] == "complete"
        assert [(a // MINUTE, b // MINUTE) for a, b, *_ in source.frame_calls] == [
            (0, 64),
            (64, 128),
            (128, 140),
        ]
        assert all(
            call[2] == 141 * MINUTE and call[3] == owner.thread.ident for call in source.frame_calls
        )
        assert source.closed
    finally:
        owner.close()
    with pytest.raises(ValidationError, match="1440"):
        parameters(spec(end_ms=1441 * MINUTE))


@pytest.mark.parametrize(
    "kind,changes",
    [
        ("NES", {"nSamples": True}),
        ("NES", {"quote_precision": 17}),
        ("NES", {"source": "other"}),
        ("TPD", {"viewLength": 1}),
        ("TPD", {"nSamplesMA": False}),
        ("TPD", {"nSamples": 20001}),
    ],
)
def test_bad_perkind_settings_are_validation_errors(kind, changes):
    with pytest.raises(ValidationError):
        parameters(spec(kind, **changes))


@pytest.mark.parametrize("bad", ["list", "cutoff", "no_pin", "instrument", "source", "reorder"])
def test_bad_frame_or_revision_identity_fails_and_closes_source(bad):
    source = Source()
    original = source.read_frames

    def broken(*args, **kwargs):
        rows = original(*args, **kwargs)
        if bad == "list":
            return list(rows)
        first = rows[0]
        if bad == "cutoff":
            first = replace(first, frame=replace(first.frame, as_of_ms=first.frame.as_of_ms + 1))
        elif bad == "no_pin":
            first = replace(first, pins=())
        elif bad == "instrument":
            first = replace(
                first,
                frame=replace(
                    first.frame,
                    candle=replace(
                        first.frame.candle, instrument=Instrument("binance", Market.SPOT, "ETHUSDT")
                    ),
                ),
            )
        elif bad == "source":
            changed = replace(first.frame.inputs[0], source="not_canonical")
            first = replace(first, frame=replace(first.frame, inputs=(changed,)), pins=())
        elif bad == "reorder":
            return tuple(reversed(rows))
        return (first, *rows[1:])

    source.read_frames = broken
    with pytest.raises(ProtocolError):
        calculate(source)
    assert source.closed


@pytest.mark.parametrize("name", ["minute-normal", "three-minute-normal"])
def test_tpd_uses_unchanged_frozen_eta_trace_and_candle_only_source(name):
    fixture = json.loads(Path("tests/fixtures/temporal-probability-legacy.json").read_text())
    expected_name = "default-minute" if name == "minute-normal" else "three-minute"
    case = next(case for case in fixture["cases"] if case["name"] == expected_name)
    config = dict(case["configuration"])
    iid = config.pop("interval_id")
    config.pop("line_index", None)
    period = {0: 1, 1: 3, 2: 5}[iid]
    source = Source(count=len(case["trace"]))

    def read(keys, start, end, *, limit, as_of_ms):
        source.candle_calls.append((start, end, as_of_ms))
        return tuple(
            Candle(INSTRUMENT, i * MINUTE, (i + 1) * MINUTE, *map(Decimal, values))
            for i, values in enumerate(fixture["inputs"][: len(case["trace"])])
            if start <= i * MINUTE < end
        )

    source.read = read
    output = calculate(
        source,
        spec("TPD", end_ms=len(case["trace"]) * MINUTE, period_minutes=period, **config),
        clock=181 * MINUTE,
    )
    expected = [row["raw"] for row in case["trace"] if row["closed"]]
    assert [(row["TPD"], row["ABSMA"], row["ABSMAREL"]) for row in output["rows"]] == [
        tuple(raw.get(field) if raw else None for field in ("TPD", "TPD_ABSMA", "TPD_ABSMAREL"))
        for raw in expected
    ]
    assert source.candle_calls and not source.frame_calls


def test_nes_cancelled_inflight_read_closes_on_worker_and_publishes_no_partial_rows():
    source = Source()
    entered, release = threading.Event(), threading.Event()
    original = source.read_frames

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(4)
        return original(*args, **kwargs)

    source.read_frames = blocked
    owner = ChartSeriesService(lambda: source, clock_ms=lambda: 100 * MINUTE)
    closer = None
    try:
        owner.submit("nes", spec())
        assert entered.wait(2)
        closer = threading.Thread(target=owner.close)
        closer.start()
        with owner.condition:
            assert owner.condition.wait_for(lambda: owner.closed, 2)
        release.set()
        closer.join(3)
        assert not closer.is_alive() and not owner.thread.is_alive() and source.closed
        assert owner.status("nes")["state"] == "cancelled" and owner.status("nes")["result"] is None
    finally:
        release.set()
        if closer:
            closer.join(4)
        owner.close()
