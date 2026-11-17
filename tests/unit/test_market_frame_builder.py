"""Requested-source selection, causal native metrics and immutable revision pins."""

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal

import pytest

from omega.analysis.graph import FeatureNode
from omega.analysis.market_graph import MarketFeatureGraph
from omega.analysis.market_sentiment import LegacyMarketSentimentFeatures, SentimentConfiguration
from omega.application.market_frame_builder import MarketFrameBuilder, PreparedMarketFrame
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.ports.auxiliary import StoredAuxiliary

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
START = 600_000
END = START + 60_000


def test_zero_metric_age_has_no_eligible_native_point_and_performs_no_auxiliary_query():
    store = FakeAuxStore()
    prepared = MarketFrameBuilder(store, maximum_metric_age_ms=0).prepare(
        candle(), as_of_ms=END, channels=("metric",)
    )
    assert prepared.pins == () and store.calls == []
    assert prepared.frame.input("metric").quality == "missing"


def candle(start=START, instrument=INSTRUMENT):
    return Candle(instrument, start, start + 60_000, *(Decimal(v) for v in (100, 102, 99, 101, 2)))


def sample(channel, *, event=START, available=None, instrument=INSTRUMENT, revision=1, **changes):
    suffix, fields = {
        "open_interest": (
            "open_interest_history",
            (("METRIC_OPENINTEREST", Decimal(1000)), ("SOURCE_PERIOD_MS", 300000)),
        ),
        "long_short_ratio": (
            "long_short_ratio",
            (("METRIC_LONGSHORTRATIO", Decimal("1.2")), ("SOURCE_PERIOD_MS", 300000)),
        ),
        "trade_flow": (
            "aggTrade",
            (("AGGTRADE_NOTIONALBUY", Decimal(100)), ("AGGTRADE_NOTIONALSELL", Decimal(10))),
        ),
    }[channel]
    opening = event // 60_000 * 60_000 if channel != "trade_flow" else event - 60_000
    observation = AuxiliaryInput(
        **{
            "channel": channel,
            "values": fields,
            "window_start_ms": opening,
            "window_end_ms": opening + 60_000,
            "event_time_ms": event,
            "available_at_ms": event + 1 if available is None else available,
            "source": instrument.key + ":" + suffix,
            **changes,
        }
    )
    return StoredAuxiliary(
        instrument,
        observation,
        revision,
        str(revision % 10) * 64,
        "a" * 64 if revision > 1 else None,
    )


class FakeAuxStore:
    def __init__(self, *rows):
        self.rows, self.calls, self.overrides, self.failure = list(rows), [], {}, None

    def get(self, instrument, channel, source, open_time_ms, *, as_of_ms):
        self.calls.append(("get", instrument, channel, source, open_time_ms, as_of_ms))
        if self.failure:
            raise self.failure
        if channel in self.overrides:
            return self.overrides[channel]
        eligible = [
            row
            for row in self.rows
            if row.instrument == instrument
            and row.observation.channel == channel
            and row.observation.source == source
            and row.observation.window_start_ms == open_time_ms
            and row.observation.available_at_ms <= as_of_ms
        ]
        return max(eligible, key=lambda row: row.revision, default=None)

    def latest_sample(
        self, instrument, channel, source, *, event_before_ms, as_of_ms, max_age_ms=330000
    ):
        self.calls.append(
            ("latest_sample", instrument, channel, source, event_before_ms, as_of_ms, max_age_ms)
        )
        if self.failure:
            raise self.failure
        if channel in self.overrides:
            return self.overrides[channel]
        eligible = [
            row
            for row in self.rows
            if row.instrument == instrument
            and row.observation.channel == channel
            and row.observation.source == source
            and event_before_ms - max_age_ms <= row.observation.event_time_ms < event_before_ms
            and row.observation.available_at_ms <= as_of_ms
        ]
        return max(
            eligible, key=lambda row: (row.observation.event_time_ms, row.revision), default=None
        )


def metric(prepared):
    return prepared.frame.input("metric")


def test_ohlcv_only_never_queries_even_a_failed_auxiliary_store():
    store = FakeAuxStore()
    store.failure = StorageError("synthetic unavailable auxiliary storage")
    prepared = MarketFrameBuilder(store).prepare(candle(), as_of_ms=END)
    assert prepared.frame.candle == candle() and prepared.frame.inputs == ()
    assert prepared.pins == () and store.calls == []


def test_only_requested_sources_are_read_without_a_three_channel_barrier():
    flow = sample("trade_flow", event=END)
    store = FakeAuxStore(flow)
    prepared = MarketFrameBuilder(store).prepare(
        candle(), as_of_ms=END + 2, channels=("trade_flow",)
    )
    assert prepared.frame.inputs == (flow.observation,) and prepared.pins == (flow,)
    assert [call[0] for call in store.calls] == ["get"]
    store.calls.clear()
    prepared = MarketFrameBuilder(store).prepare(candle(), as_of_ms=END + 2, channels=("metric",))
    assert metric(prepared).quality == "missing" and metric(prepared).values == ()
    assert [call[2] for call in store.calls] == ["open_interest", "long_short_ratio"]


def test_two_actual_native_samples_form_a_conservative_view_with_original_pins():
    oi = sample("open_interest", event=START - 60_000, available=START + 50)
    ls = sample("long_short_ratio", event=START + 10, available=END + 1)
    store = FakeAuxStore(oi, ls)
    result = MarketFrameBuilder(store).prepare(candle(), as_of_ms=END + 2, channels=("metric",))
    view = metric(result)
    assert view.quality == "carried" and view.coverage == 1
    assert view.window_start_ms == START and view.window_end_ms == END
    assert view.event_time_ms == oi.observation.event_time_ms
    assert view.available_at_ms == ls.observation.available_at_ms
    assert view.mapping()["METRIC_OPENINTEREST"] == Decimal(1000)
    assert view.mapping()["METRIC_LONGSHORTRATIO"] == Decimal("1.2")
    assert view.mapping()["SOURCE_LONGSHORTRATIO_TIME_MS"] == START + 10
    assert view.source.startswith("metric_view:") and len(view.source) <= 96
    assert result.pins == (oi, ls) and result.pins[0] is oi
    assert oi.observation.window_start_ms == START - 60_000
    assert oi.observation.mapping()["SOURCE_PERIOD_MS"] == 300000
    assert "SOURCE_PERIOD_MS" not in view.mapping()
    with pytest.raises(FrozenInstanceError):
        result.pins[0].revision = 9
    with pytest.raises(FrozenInstanceError):
        result.frame = None
    store.rows.clear()
    assert result.pins == (oi, ls)  # No mutable query result/list is retained.


def test_in_minute_samples_are_observed_and_real_zero_values_are_not_missing():
    oi = sample("open_interest", values=(("METRIC_OPENINTEREST", Decimal(0)),))
    ls = sample("long_short_ratio", values=(("METRIC_LONGSHORTRATIO", Decimal(0)),))
    result = MarketFrameBuilder(FakeAuxStore(oi, ls)).prepare(
        candle(), as_of_ms=END, channels=("metric",)
    )
    assert metric(result).quality == "observed" and metric(result).coverage == 1
    assert metric(result).mapping()["METRIC_OPENINTEREST"] == 0
    assert metric(result).mapping()["METRIC_LONGSHORTRATIO"] == 0
    assert result.pins == (oi, ls)


def test_latest_event_revision_and_availability_selection_are_pinned():
    old = sample("open_interest", event=START - 60_000)
    latest = sample("open_interest", event=START)
    repaired = sample("open_interest", event=START, available=END + 5, revision=2)
    unavailable = sample("open_interest", event=START + 10, available=END + 100)
    boundary = sample("open_interest", event=END, available=END + 1)
    ls = sample("long_short_ratio")
    builder = MarketFrameBuilder(FakeAuxStore(old, latest, repaired, unavailable, boundary, ls))
    early = builder.prepare(candle(), as_of_ms=END + 2, channels=("metric",))
    later = builder.prepare(candle(), as_of_ms=END + 6, channels=("metric",))
    assert early.pins[0] is latest and later.pins[0] is repaired
    assert metric(later).event_time_ms == START
    assert all(pin.observation.event_time_ms < END for pin in later.pins)


@pytest.mark.parametrize("age", [329999, 330000, 330001])
def test_actual_analysis_cutoff_rechecks_ttl_after_native_search(age):
    oi = sample("open_interest")
    ls = sample("long_short_ratio")
    store = FakeAuxStore(oi, ls)
    prepared = MarketFrameBuilder(store).prepare(
        candle(), as_of_ms=START + age, channels=("metric",)
    )
    assert metric(prepared).quality == ("observed" if age <= 330000 else "missing")
    assert prepared.pins == (oi, ls)
    if age > 330000:
        assert metric(prepared).values == () and metric(prepared).coverage == 0
    assert all(call[4] == END and call[-1] == 330000 for call in store.calls)


def test_configured_native_lookback_excludes_old_sources_without_fabricating_values():
    store = FakeAuxStore(sample("open_interest", event=START - 120_000), sample("long_short_ratio"))
    prepared = MarketFrameBuilder(store, maximum_metric_age_ms=60_000).prepare(
        candle(), as_of_ms=END, channels=("metric",)
    )
    assert metric(prepared).quality == "missing" and metric(prepared).values == ()
    assert len(prepared.pins) == 1 and prepared.pins[0].observation.channel == "long_short_ratio"


def test_spot_metric_is_explicitly_unsupported_without_any_metric_io():
    spot = replace(INSTRUMENT, market=Market.SPOT)
    store = FakeAuxStore()
    store.failure = StorageError("must not be queried for unsupported metrics")
    prepared = MarketFrameBuilder(store).prepare(
        candle(instrument=spot), as_of_ms=END, channels=("metric",)
    )
    assert metric(prepared).quality == "unsupported" and metric(prepared).coverage == 0
    assert not metric(prepared).values and not prepared.pins and not store.calls


def test_native_current_oi_dataset_can_be_selected_explicitly():
    oi = sample(
        "open_interest",
        source=INSTRUMENT.key + ":open_interest",
        values=(("METRIC_OPENINTEREST", Decimal(1000)),),
    )
    ls = sample("long_short_ratio")
    store = FakeAuxStore(oi, ls)
    result = MarketFrameBuilder(store, open_interest_source_suffix="open_interest").prepare(
        candle(), as_of_ms=END, channels=("metric",)
    )
    assert result.pins == (oi, ls)
    assert store.calls[0][3] == INSTRUMENT.key + ":open_interest"


@pytest.mark.parametrize("channel", ["trade_flow", "metric"])
def test_storage_failure_is_propagated_instead_of_becoming_missing(channel):
    store = FakeAuxStore()
    store.failure = StorageError("synthetic query outcome unknown")
    with pytest.raises(StorageError):
        MarketFrameBuilder(store).prepare(candle(), as_of_ms=END, channels=(channel,))


@pytest.mark.parametrize("kind", ["instrument", "source", "availability", "boundary", "window"])
def test_untrusted_native_port_identity_or_time_violations_are_rejected(kind):
    oi = sample("open_interest")
    if kind == "instrument":
        oi = replace(oi, instrument=replace(INSTRUMENT, symbol="BTCUSDT"))
    elif kind == "source":
        oi = replace(oi, observation=replace(oi.observation, source="another_dataset"))
    elif kind == "availability":
        oi = replace(oi, observation=replace(oi.observation, available_at_ms=END + 1))
    elif kind == "boundary":
        oi = sample("open_interest", event=END, available=END)
    else:
        oi = replace(
            oi,
            observation=replace(oi.observation, window_start_ms=START - 60000, window_end_ms=START),
        )
    store = FakeAuxStore(sample("long_short_ratio"))
    store.overrides["open_interest"] = oi
    with pytest.raises(ProtocolError):
        MarketFrameBuilder(store).prepare(candle(), as_of_ms=END, channels=("metric",))


def test_wrong_trade_minute_is_not_reassigned_to_the_candle():
    store = FakeAuxStore()
    store.overrides["trade_flow"] = sample("trade_flow", event=START)
    with pytest.raises(ProtocolError):
        MarketFrameBuilder(store).prepare(candle(), as_of_ms=END, channels=("trade_flow",))


@pytest.mark.parametrize("maximum_age", [-1, True, 86_400_001, 330000.0])
def test_freshness_configuration_is_bounded_and_typed(maximum_age):
    with pytest.raises(ValidationError):
        MarketFrameBuilder(FakeAuxStore(), maximum_metric_age_ms=maximum_age)


@pytest.mark.parametrize(
    "as_of,channels",
    [(END - 1, ()), (True, ()), (END, "metric"), (END, ("funding",)), (END, ("metric", "metric"))],
)
def test_cutoff_and_requested_channel_validation_precedes_queries(as_of, channels):
    store = FakeAuxStore()
    with pytest.raises(ValidationError):
        MarketFrameBuilder(store).prepare(candle(), as_of_ms=as_of, channels=channels)
    assert store.calls == []


def test_prepared_frame_drives_existing_mst_graph_and_missing_flow_gates_it():
    store = FakeAuxStore()
    builder = MarketFrameBuilder(store)
    graph = MarketFeatureGraph(
        INSTRUMENT,
        (
            FeatureNode(
                "mst",
                LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(0, nSamples=2),)),
            ),
        ),
    )
    for index in range(4):
        start = START + index * 60000
        bar = candle(start)
        store.rows.extend(
            (
                sample("trade_flow", event=bar.close_time_ms),
                sample("open_interest", event=start),
                sample("long_short_ratio", event=start),
            )
        )
        prepared = builder.prepare(
            bar, as_of_ms=bar.close_time_ms + 1, channels=("trade_flow", "metric")
        )
        result = graph.update_frame(prepared.frame)
        assert len(prepared.pins) == 3
    assert graph.input_ready and result["0_MST_0_MST"] is not None
    missing = builder.prepare(
        candle(START + 4 * 60000), as_of_ms=START + 5 * 60000, channels=("trade_flow", "metric")
    )
    assert missing.frame.input("trade_flow").quality == "missing"
    assert graph.update_frame(missing.frame)["0_MST_0_MST"] is None and not graph.input_ready


def test_prepared_contract_rejects_mutable_or_untyped_pins():
    frame = MarketFrameBuilder(FakeAuxStore()).prepare(candle(), as_of_ms=END).frame
    with pytest.raises(ValidationError):
        PreparedMarketFrame(frame, [])
    with pytest.raises(ValidationError):
        PreparedMarketFrame(frame, (object(),))


def test_retrospective_freshness_keeps_real_late_availability_and_original_pins():
    received = END + 7 * 86_400_000
    oi = sample("open_interest", available=received)
    ls = sample("long_short_ratio", available=received)
    store = FakeAuxStore(oi, ls)
    builder = MarketFrameBuilder(store)
    live = builder.prepare(candle(), as_of_ms=received, channels=("metric",))
    replay = builder.prepare(candle(), as_of_ms=received, channels=("metric",), retrospective=True)
    assert not live.retrospective and live.frame.reference_time_ms is None
    assert live.frame.feature_time_ms == received and metric(live).quality == "missing"
    assert replay.retrospective and replay.frame.reference_time_ms == END
    assert replay.frame.feature_time_ms == END and metric(replay).quality == "observed"
    assert metric(replay).event_time_ms == START
    assert metric(replay).available_at_ms == replay.frame.as_of_ms == received
    assert replay.pins == live.pins == (oi, ls)
    assert all(row.observation.available_at_ms == received for row in replay.pins)
    assert all(call[5] == received for call in store.calls)


@pytest.mark.parametrize("retrospective", [False, True])
def test_real_knowledge_cutoff_and_strict_event_boundary_apply_in_both_modes(retrospective):
    received = END + 7 * 86_400_000
    unavailable = sample("open_interest", available=received + 1)
    boundary = sample("open_interest", event=END, available=received)
    future = sample("open_interest", event=END + 1, available=received)
    ls = sample("long_short_ratio", available=received)
    store = FakeAuxStore(unavailable, boundary, future, ls)
    replay = MarketFrameBuilder(store).prepare(
        candle(), as_of_ms=received, channels=("metric",), retrospective=retrospective
    )
    assert metric(replay).quality == "missing" and metric(replay).values == ()
    assert replay.pins == (ls,)
    assert store.calls[0][4:6] == (END, received)
    # A broken port cannot bypass either invariant even in explicit replay mode.
    for invalid in (unavailable, boundary, future):
        store.overrides["open_interest"] = invalid
        with pytest.raises(ProtocolError):
            MarketFrameBuilder(store).prepare(
                candle(), as_of_ms=received, channels=("metric",), retrospective=retrospective
            )


def test_retrospective_reference_does_not_make_old_native_metrics_fresh():
    received = END + 7 * 86_400_000
    store = FakeAuxStore(
        sample("open_interest", event=START - 360_000, available=received),
        sample("long_short_ratio", available=received),
    )
    replay = MarketFrameBuilder(store).prepare(
        candle(), as_of_ms=received, channels=("metric",), retrospective=True
    )
    assert replay.retrospective and metric(replay).quality == "missing"
    assert len(replay.pins) == 1


def test_retrospective_downloaded_history_can_seed_mst_without_backdating_any_sample():
    received = END + 7 * 86_400_000
    store = FakeAuxStore()
    builder = MarketFrameBuilder(store)
    graph = MarketFeatureGraph(
        INSTRUMENT,
        (
            FeatureNode(
                "mst",
                LegacyMarketSentimentFeatures(INSTRUMENT, (SentimentConfiguration(0, nSamples=2),)),
            ),
        ),
    )
    for index in range(4):
        start = START + index * 60000
        bar = candle(start)
        store.rows.extend(
            (
                sample("trade_flow", event=bar.close_time_ms, available=received),
                sample("open_interest", event=start, available=received),
                sample("long_short_ratio", event=start, available=received),
            )
        )
        prepared = builder.prepare(
            bar,
            as_of_ms=received,
            channels=("trade_flow", "metric"),
            retrospective=True,
        )
        assert prepared.retrospective and prepared.frame.as_of_ms == received
        assert prepared.frame.feature_time_ms == bar.close_time_ms
        assert all(pin.observation.available_at_ms == received for pin in prepared.pins)
        result = graph.update_frame(prepared.frame)
    assert graph.input_ready and result["0_MST_0_MST"] is not None
    assert all(row.observation.available_at_ms == received for row in store.rows)


@pytest.mark.parametrize("mode", [0, 1, None, "historical"])
def test_retrospective_is_an_explicit_boolean_mode(mode):
    store = FakeAuxStore()
    with pytest.raises(ValidationError):
        MarketFrameBuilder(store).prepare(candle(), as_of_ms=END, retrospective=mode)
    assert store.calls == []
