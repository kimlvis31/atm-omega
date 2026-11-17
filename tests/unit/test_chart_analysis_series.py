"""Native batch selection, existing producer parity, and quiescent display cancellation."""

import threading
import time
from dataclasses import asdict, replace
from decimal import Decimal
from hashlib import sha256

import pytest

from omega.analysis.catalog import create_features
from omega.analysis.order_flow import DEPTH_FIELDS
from omega.analysis.short_term_scalar import ScalarConfiguration
from omega.application.auxiliary_codec import encode_observation
from omega.application.market_frame_builder import MarketFrameBuilder
from omega.domain.chart_analysis import ANALYSIS_FIELDS, analysis_parameters
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.chart_analysis_frames import ChartAnalysisFrameSource, _RevisionIndex
from omega.ports.auxiliary import AuxiliaryReadRange, StoredAuxiliary
from omega.runtime.chart_analysis_series import _definition
from omega.runtime.chart_series import ChartSeriesService, parameters
from omega.runtime.service import CoreService

MINUTE = 60000
BTC = Instrument("binance", Market.USDM, "BTCUSDT")
SOURCES = {
    "trade_flow": "aggTrade",
    "depth": "binance_depth_ws_v1",
    "open_interest": "open_interest_history",
    "long_short_ratio": "long_short_ratio",
}


def spec(kind="WOI", **changes):
    extra = (
        {
            key: value
            for key, value in asdict(ScalarConfiguration(0)).items()
            if key != "interval_id"
        }
        if kind == "STS"
        else {"definitions": [{"line_index": 0, "nSamples": 3}], "quote_precision": 8}
    )
    if kind == "MST":
        extra["maximum_metric_age_ms"] = 330000
    return {
        "instrument": BTC.key,
        "kind": kind,
        "start_ms": 0,
        "end_ms": 90 * MINUTE,
        "period_minutes": 1,
        **extra,
        **changes,
    }


def candle(index, instrument=BTC):
    base = 100 + index % 7
    return Candle(
        instrument,
        index * MINUTE,
        (index + 1) * MINUTE,
        *map(Decimal, (base, base + 4, base - 2, base + 1, 10)),
    )


def pin(
    channel,
    index,
    *,
    revision=1,
    previous=None,
    event=None,
    available=None,
    quality="observed",
    instrument=BTC,
):
    start, end = index * MINUTE, (index + 1) * MINUTE
    event = (end if channel == "trade_flow" else end - 1) if event is None else event
    fields = {
        "trade_flow": (
            ("AGGTRADE_NOTIONALBUY", Decimal(index * 3 + 10)),
            ("AGGTRADE_NOTIONALSELL", Decimal(index + 5)),
        ),
        "depth": tuple(
            zip(DEPTH_FIELDS, (Decimal(index + offset + 1) for offset in range(12)), strict=True)
        ),
        "open_interest": (("METRIC_OPENINTEREST", Decimal(1000 + index)),),
        "long_short_ratio": (("METRIC_LONGSHORTRATIO", Decimal("1.2") + Decimal(index) / 1000),),
    }[channel]
    observation = AuxiliaryInput(
        channel,
        fields,
        start,
        end,
        event,
        max(end + 1, event) if available is None else available,
        quality,
        Decimal(1),
        instrument.key + ":" + SOURCES[channel],
    )
    checksum = sha256(encode_observation(instrument, observation)).hexdigest()
    return StoredAuxiliary(instrument, observation, revision, checksum, previous)


class Source:
    def __init__(
        self, count=90, *, missing=(), partial=(), omitted=(), revised=False, instrument=BTC
    ):
        self.count, self.missing, self.partial, self.omitted = (
            count,
            set(missing),
            set(partial),
            set(omitted),
        )
        self.revised, self.instrument, self.closed = revised, instrument, False
        self.calls, self.candle_calls = [], []
        self.entered = self.release = None
        self.on_read = None
        self.close_failure = False
        self.close_calls = 0

    def records(self, channel, start, end, as_of):
        for index in range(max(0, start // MINUTE), min(self.count, end // MINUTE)):
            if (channel, index) in self.missing:
                continue
            if channel in {"open_interest", "long_short_ratio"} and index % 5:
                continue
            first = pin(
                channel,
                index,
                instrument=self.instrument,
                quality="partial" if (channel, index) in self.partial else "observed",
            )
            if first.observation.available_at_ms <= as_of:
                yield first
            if self.revised:
                revised = replace(
                    first.observation, available_at_ms=first.observation.available_at_ms + 1
                )
                second = StoredAuxiliary(
                    self.instrument,
                    revised,
                    2,
                    sha256(encode_observation(self.instrument, revised)).hexdigest(),
                    first.checksum,
                )
                if second.observation.available_at_ms <= as_of:
                    yield second

    def read(self, keys, start, end, *, limit, as_of_ms):
        assert keys == (self.instrument.key,) and limit <= 512
        self.candle_calls.append((start, end, as_of_ms))
        return tuple(
            candle(index, self.instrument)
            for index in range(start // MINUTE, min(self.count, end // MINUTE))
            if index not in self.omitted
        )

    def read_revisions(self, ranges, *, as_of_ms, limit, check_stop):
        assert (
            limit == 10000 and sum((row.end_ms - row.start_ms) // MINUTE for row in ranges) <= limit
        )
        self.calls.append((ranges, as_of_ms, threading.get_ident()))
        if self.entered:
            self.entered.set()
            assert self.release.wait(4)
        if self.on_read:
            self.on_read()
        check_stop()
        return tuple(
            record
            for row in ranges
            for record in self.records(row.channel, row.start_ms, row.end_ms, as_of_ms)
        )

    def get(self, instrument, channel, source, open_time_ms, *, as_of_ms):
        assert instrument == self.instrument and source == instrument.key + ":" + SOURCES[channel]
        return max(
            self.records(channel, open_time_ms, open_time_ms + MINUTE, as_of_ms),
            key=lambda row: row.revision,
            default=None,
        )

    def latest_sample(self, instrument, channel, source, *, event_before_ms, as_of_ms, max_age_ms):
        assert instrument == self.instrument and source == instrument.key + ":" + SOURCES[channel]
        rows = self.records(
            channel,
            max(0, event_before_ms - max_age_ms) // MINUTE * MINUTE,
            event_before_ms,
            as_of_ms,
        )
        result = max(
            (
                row
                for row in rows
                if max(0, event_before_ms - max_age_ms)
                <= row.observation.event_time_ms
                < event_before_ms
            ),
            key=lambda row: (row.observation.event_time_ms, row.revision),
            default=None,
        )
        if result is not None and result.observation.quality != "observed":
            raise StorageError("stored auxiliary native sample failed integrity checks")
        return result

    def close(self):
        self.close_calls += 1
        self.closed = True
        if self.close_failure:
            raise StorageError("source cleanup failed")


def calculate(source, configuration):
    owner = ChartSeriesService(lambda: source, clock_ms=lambda: 10000 * MINUTE)
    try:
        return owner._calculate(parameters(configuration))
    finally:
        owner.close()


@pytest.mark.parametrize("kind", ("WOI", "MST", "STS"))
def test_typed_high_timeframe_capacity_retains_the_entire_explicit_origin(kind):
    result = analysis_parameters(kind, spec(kind, end_ms=128 * 1440 * MINUTE, period_minutes=1440))
    assert result["start_ms"] == 0 and result["end_ms"] == 184320 * MINUTE
    with pytest.raises(ValidationError):
        analysis_parameters(kind, {**result, "end_ms": result["end_ms"] + MINUTE})
    with pytest.raises(ValidationError):
        analysis_parameters(kind, {**result, "period_minutes": True})


def test_nested_definitions_canonical_and_strict_provider_sample_budget():
    request = spec(
        definitions=[{"line_index": 9, "nSamples": 10}, {"line_index": 0, "nSamples": 3}]
    )
    result = analysis_parameters("WOI", request)
    assert [row["line_index"] for row in result["definitions"]] == [0, 9]
    assert request["definitions"][0]["line_index"] == 9
    for changes in (
        {"definitions": [{"line_index": 0, "nSamples": True}]},
        {
            "definitions": [
                {"line_index": 0, "nSamples": 60000},
                {"line_index": 1, "nSamples": 60000},
            ]
        },
        {"extra": 1},
    ):
        with pytest.raises(ValidationError):
            analysis_parameters("WOI", {**request, **changes})
    with pytest.raises(ValidationError):
        analysis_parameters(
            "MST",
            spec(
                "MST",
                definitions=[
                    {"line_index": 0, "nSamples": 20000},
                    {"line_index": 1, "nSamples": 2},
                ],
            ),
        )
    with pytest.raises(ValidationError):
        analysis_parameters("STS", spec("STS", OSCScale=10**1000))


@pytest.mark.parametrize(
    "kind,period", [(kind, period) for kind in ANALYSIS_FIELDS for period in (1, 3)]
)
def test_rows_exact_existing_provider_graph_and_native_quality(kind, period):
    source = Source(
        count=150,
        missing=(("depth", 30), ("depth", 32), ("trade_flow", 30)),
        partial=(("depth", 40),),
    )
    request = spec(kind, end_ms=150 * MINUTE, period_minutes=period)
    if kind != "STS":
        request["definitions"].append({"line_index": 9, "nSamples": 7})
    output = calculate(source, request)
    iid = {1: 0, 3: 1}[period]
    configuration, fields = _definition(request, iid)
    graph = create_features(
        BTC, tuple(key for row in fields.values() for key in row.values()), configuration
    )
    builder = MarketFrameBuilder(
        source, maximum_metric_age_ms=request.get("maximum_metric_age_ms", 0)
    )
    expected = []
    for index in range(150):
        frame = builder.prepare(
            candle(index),
            as_of_ms=10000 * MINUTE,
            channels=("metric", "trade_flow") if kind == "MST" else ("depth",),
            retrospective=True,
        )
        values = graph.update_frame(frame.frame)
        if values[f"CLOSED_{iid}"]:
            expected.append(
                {
                    line: {name: values[key] for name, key in mapping.items()}
                    for line, mapping in fields.items()
                }
            )
    assert [row["values"] for row in output["rows"]] == expected
    assert source.closed and len(source.calls) == 1
    assert output["quality"]["complete_bars"] == 150 // period
    unavailable = [row for row in output["rows"] if not row["input_quality"]["0"]["ready"]]
    assert unavailable and all(
        value is None
        for row in unavailable
        for values in row["values"].values()
        for value in values.values()
    )


def test_metric_revisions_select_event_then_revision_not_latest_only():
    span = AuxiliaryReadRange(BTC, "open_interest", BTC.key + ":open_interest_history", 0, MINUTE)
    first = pin("open_interest", 0, event=50000, available=MINUTE + 1)
    second = pin(
        "open_interest", 0, revision=2, previous=first.checksum, event=40000, available=MINUTE + 2
    )
    index = _RevisionIndex((first, second), (span,), 2 * MINUTE, lambda: None)
    assert index.get(BTC, "open_interest", span.source, 0, as_of_ms=2 * MINUTE) == second
    assert (
        index.latest_sample(
            BTC,
            "open_interest",
            span.source,
            event_before_ms=MINUTE,
            as_of_ms=2 * MINUTE,
            max_age_ms=MINUTE,
        )
        == first
    )
    malformed = replace(second, previous_checksum="f" * 64)
    with pytest.raises(ProtocolError, match="predecessor"):
        _RevisionIndex((first, malformed), (span,), 2 * MINUTE, lambda: None)
    with pytest.raises(ProtocolError, match="predecessor"):
        _RevisionIndex((second,), (span,), 2 * MINUTE, lambda: None)


def test_native_bad_selected_metric_is_storage_failure_not_partial_display():
    selected = pin("open_interest", 0, quality="partial")
    span = AuxiliaryReadRange(BTC, "open_interest", selected.observation.source, 0, MINUTE)
    index = _RevisionIndex((selected,), (span,), 2 * MINUTE, lambda: None)
    with pytest.raises(StorageError, match="integrity"):
        index.latest_sample(
            BTC,
            "open_interest",
            span.source,
            event_before_ms=MINUTE,
            as_of_ms=2 * MINUTE,
            max_age_ms=MINUTE,
        )


def test_bulk_lookback_chunk_boundary_and_no_point_query_fallback():
    source = Source(1025)
    output = calculate(source, spec("MST", end_ms=1025 * MINUTE))
    assert len(source.calls) == 3 and len(source.candle_calls) == 3
    assert all(call[1] == 10000 * MINUTE for call in source.calls)
    second_ranges = source.calls[1][0]
    assert len(second_ranges) == 3 and min(row.start_ms for row in second_ranges) == 506 * MINUTE
    assert output["rows"][512]["input_quality"]["0"]["metric_reason"] == "carried"


def test_revision_and_actual_availability_change_both_checksums():
    first = calculate(Source(), spec())
    second = calculate(Source(revised=True), spec())
    assert first["source_checksum"] != second["source_checksum"]
    assert first["rows"][0]["auxiliary_checksum"] != second["rows"][0]["auxiliary_checksum"]


@pytest.mark.parametrize("kind", ("WOI", "STS"))
def test_depth_latest_snapshot_readiness_differs_from_complete_minute_flow(kind):
    output = calculate(
        Source(15, missing=(("depth", 6), ("depth", 11))),
        spec(kind, end_ms=15 * MINUTE, period_minutes=3),
    )
    early_missing, final_missing = output["rows"][2:4]
    assert early_missing["input_quality"]["0"]["ready"]
    assert not final_missing["input_quality"]["0"]["ready"]
    assert all(value is None for value in final_missing["values"]["0"].values())


def test_default_hour_sts_replays_41_seed_bars_and_one_subsequent_output(record_property):
    source = Source(42 * 60)
    started = time.perf_counter()
    output = calculate(source, spec("STS", end_ms=42 * 60 * MINUTE, period_minutes=60))
    elapsed = time.perf_counter() - started
    record_property("provider_replay_elapsed_seconds", elapsed)
    record_property("scope", "typed memory native facts; existing provider; no DB or network")
    assert len(output["rows"]) == 42
    assert all(row["values"]["0"]["S"] is None for row in output["rows"][:40])
    assert all(row["values"]["0"]["S"] is not None for row in output["rows"][40:])
    assert len(source.calls) == len(source.candle_calls) == 5
    assert all({span.channel for span in call[0]} == {"depth"} for call in source.calls)
    assert source.closed


@pytest.mark.parametrize("fault", ("checksum", "duplicate", "capacity"))
def test_injected_batch_integrity_and_capacity_fail_closed(fault):
    source = Source()
    original = source.read_revisions

    def changed(*args, **kwargs):
        rows = original(*args, **kwargs)
        if fault == "checksum":
            return (replace(rows[0], checksum="f" * 64), *rows[1:])
        if fault == "duplicate":
            return (*rows, rows[0])
        return (rows[0],) * 10001

    source.read_revisions = changed
    adapter = ChartAnalysisFrameSource(source, check_stop=lambda: None)
    try:
        with pytest.raises(ProtocolError):
            adapter.read_frames(
                BTC,
                0,
                MINUTE,
                as_of_ms=2 * MINUTE,
                channels=("depth",),
                maximum_metric_age_ms=0,
                retrospective_until_ms=MINUTE,
                limit=1,
            )
    finally:
        adapter.close()
    assert source.closed


def test_spot_metric_unsupported_is_masked_with_actual_flow_only():
    instrument = Instrument("binance", Market.SPOT, "BTCUSDT")
    source = Source(instrument=instrument)
    output = calculate(source, spec("MST", instrument=instrument.key))
    assert all(
        row["input_quality"]["0"]["metric_reason"] == "unsupported" for row in output["rows"]
    )
    assert all(value is None for row in output["rows"] for value in row["values"]["0"].values())
    assert {span.channel for span in source.calls[0][0]} == {"trade_flow"}


def wait_terminal(owner, identity):
    with owner.condition:
        assert owner.condition.wait_for(
            lambda: owner.jobs[identity]["state"] in {"complete", "cancelled", "failed"}, 6
        )
    return owner.status(identity)


def test_queued_and_running_cancel_stay_truthful_until_source_quiesces():
    source = Source()
    source.entered, source.release = threading.Event(), threading.Event()
    owner = ChartSeriesService(lambda: source, clock_ms=lambda: 10000 * MINUTE)
    try:
        owner.submit("running", spec())
        assert source.entered.wait(2)
        owner.submit("queued", spec())
        assert owner.cancel("queued")["state"] == "cancelled"
        assert owner.cancel("running")["state"] == "cancelling"
        assert not source.closed
        source.release.set()
        assert wait_terminal(owner, "running")["state"] == "cancelled"
        assert source.closed and not owner._cancelled.intersection({"running"})
    finally:
        source.release.set()
        owner.close()


def test_cancel_cannot_hide_source_cleanup_failure():
    source = Source()
    source.entered, source.release, source.close_failure = (
        threading.Event(),
        threading.Event(),
        True,
    )
    owner = ChartSeriesService(lambda: source, clock_ms=lambda: 10000 * MINUTE)
    try:
        owner.submit("one", spec())
        assert source.entered.wait(2)
        owner.cancel("one")
        source.release.set()
        result = wait_terminal(owner, "one")
        assert (
            result["state"] == "failed"
            and "cleanup" in result["error"]
            and result["result"] is None
        )
        with pytest.raises(StorageError, match="cleanup remains incomplete"):
            owner.submit("after-failure", spec())
    finally:
        source.release.set()
        with pytest.raises(StorageError, match="cleanup remains incomplete"):
            owner.close()
    assert not owner.thread.is_alive()
    assert source.close_calls == 1


def test_monotonic_sixty_second_deadline_closes_source_without_complete_rows():
    elapsed = [0]
    source = Source()
    source.on_read = lambda: elapsed.__setitem__(0, 60)
    owner = ChartSeriesService(
        lambda: source, clock_ms=lambda: 10000 * MINUTE, monotonic=lambda: elapsed[0]
    )
    try:
        owner.submit("deadline", spec())
        result = wait_terminal(owner, "deadline")
        assert result["state"] == "failed" and "deadline" in result["error"]
        assert result["result"] is None and source.closed
    finally:
        owner.close()


def test_cleanup_failure_blocks_prequeued_native_job_before_another_source_opens():
    source = Source()
    source.entered, source.release = threading.Event(), threading.Event()
    source.close_failure = True
    factories = []

    def factory():
        factories.append(1)
        return source

    owner = ChartSeriesService(factory, clock_ms=lambda: 10000 * MINUTE)
    try:
        owner.submit("first", spec())
        assert source.entered.wait(2)
        owner.submit("prequeued", spec("MST"))
        source.release.set()
        assert wait_terminal(owner, "first")["state"] == "failed"
        second = wait_terminal(owner, "prequeued")
        assert second["state"] == "failed" and "cleanup remains incomplete" in second["error"]
        assert factories == [1] and source.close_calls == 1
    finally:
        source.release.set()
        with pytest.raises(StorageError, match="cleanup remains incomplete"):
            owner.close()


def test_cache_counts_all_family_lines_including_none():
    owner = ChartSeriesService(Source)
    try:
        result = {"kind": "WOI", "definitions": [{}] * 10, "rows": [{}] * 1440}
        owner.jobs["old"] = {"state": "complete", "result": result}
        owner.jobs["new"] = {"state": "complete", "result": result}
        owner._expire_results()
        assert owner.jobs["old"]["state"] == "expired" and owner.jobs["new"]["state"] == "complete"
    finally:
        owner.close()


def test_real_chart_service_advertises_new_kinds_through_core_capabilities(tmp_path):
    worker = ChartSeriesService(Source)
    core = CoreService(tmp_path, chart_series=worker)
    try:
        assert worker.analysis_kinds == ("WOI", "MST", "STS")
        assert core.state()["core"]["capabilities"]["chart_analysis_kinds"] == list(
            worker.analysis_kinds
        )
    finally:
        core.close()
