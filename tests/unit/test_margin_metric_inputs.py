"""Exact sparse native metric selection/derivation; no DB, source query or trading."""

import json
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D
from hashlib import sha256

import pytest

from omega.analysis.graph import FeatureNode
from omega.analysis.market_graph import MarketFeatureGraph
from omega.analysis.market_sentiment import LegacyMarketSentimentFeatures, SentimentConfiguration
from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.application.market_frame_builder import MarketFrameBuilder
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market
from omega.ports.auxiliary import StoredAuxiliary
from omega.runtime.margin_metric_inputs import MetricInputPolicy, rebuild_metric, validate_metric

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
BAR = Candle(BTC, 600000, 660000, *(D(value) for value in (100, 101, 99, 100, 2)))
POLICY = MetricInputPolicy(330000)


def sample(channel, *, event=360000, available=None, value=None, revision=1, **changes):
    if channel == "open_interest":
        suffix, field, number = "open_interest_history", "METRIC_OPENINTEREST", D(1000)
        extras = (("METRIC_OPENINTERESTVALUE", D(100000)),)
    else:
        suffix, field, number = "long_short_ratio", "METRIC_LONGSHORTRATIO", D("1.2")
        extras = (("METRIC_LONGACCOUNT", D(".6")), ("METRIC_SHORTACCOUNT", D(".4")))
    observation = AuxiliaryInput(
        channel,
        ((field, number if value is None else value), *extras, ("SOURCE_PERIOD_MS", 300000)),
        event // 60000 * 60000,
        event // 60000 * 60000 + 60000,
        event,
        event + 1 if available is None else available,
        source=BTC.key + ":" + suffix,
    )
    observation = replace(observation, **changes)
    return StoredAuxiliary(
        BTC,
        observation,
        revision,
        observation_checksum(encode_observation(BTC, observation)),
        "a" * 64 if revision > 1 else None,
    )


def native_pair(**kwargs):
    return (sample("open_interest", **kwargs), sample("long_short_ratio", **kwargs))


def rebuilt(pins=None, *, policy=POLICY, as_of_ms=660000, candle=BAR):
    return rebuild_metric(
        candle, as_of_ms=as_of_ms, policy=policy, pins=native_pair() if pins is None else pins
    )


def graph(age):
    return MarketFeatureGraph(
        BTC,
        (
            FeatureNode(
                "mst",
                LegacyMarketSentimentFeatures(
                    BTC, (SentimentConfiguration(0, nSamples=2, maximum_metric_age_ms=age),)
                ),
            ),
        ),
    )


def with_flow(prepared):
    candle = prepared.frame.candle
    flow = AuxiliaryInput(
        "trade_flow",
        (("AGGTRADE_NOTIONALBUY", D(1500)), ("AGGTRADE_NOTIONALSELL", D(500))),
        candle.open_time_ms,
        candle.close_time_ms,
        candle.close_time_ms,
        prepared.frame.as_of_ms,
        source=BTC.key + ":aggTrade",
    )
    return MarketFeatureFrame(candle, prepared.frame.as_of_ms, (*prepared.frame.inputs, flow))


def test_policy_is_explicit_fixed_versioned_json_and_has_no_global_default_age():
    policy = MetricInputPolicy(330000)
    document = policy.document()
    assert document == {
        "version": 1,
        "maximum_metric_age_ms": 330000,
        "open_interest_source_suffix": "open_interest_history",
        "long_short_source_suffix": "long_short_ratio",
    }
    assert MetricInputPolicy.from_document(json.loads(json.dumps(document))) == policy
    document["maximum_metric_age_ms"] = 0
    assert policy.maximum_metric_age_ms == 330000
    with pytest.raises(FrozenInstanceError):
        policy.maximum_metric_age_ms = 0
    with pytest.raises(TypeError):
        MetricInputPolicy()


@pytest.mark.parametrize("age", [True, False, -1, 86400001, "330000", 1.0, None])
def test_non_native_or_out_of_range_policy_age_rejects(age):
    with pytest.raises(ValidationError):
        MetricInputPolicy(age)


@pytest.mark.parametrize(
    "changed",
    [
        {"version": True},
        {"version": 2},
        {"open_interest_source_suffix": "open_interest"},
        {"long_short_source_suffix": "positions_ratio"},
        {"unknown": 1},
    ],
)
def test_policy_document_cannot_change_native_dataset_identity(changed):
    with pytest.raises(ValidationError):
        MetricInputPolicy.from_document(POLICY.document() | changed)


def test_sparse_native_pins_rebuild_exact_original_builder_view_and_source_time_metadata():
    pins = (sample("open_interest", revision=2), sample("long_short_ratio", event=420000))
    calls = []

    class SelectedStore:
        def latest_sample(self, instrument, channel, source, **kwargs):
            calls.append((instrument, channel, source, kwargs))
            return next(pin for pin in pins if pin.observation.channel == channel)

    original = MarketFrameBuilder(SelectedStore(), maximum_metric_age_ms=330000).prepare(
        BAR, as_of_ms=660003, channels=("metric",)
    )
    result = validate_metric(
        BAR, original.frame.input("metric"), as_of_ms=660003, policy=POLICY, pins=original.pins
    )
    assert result == original and len(calls) == 2
    observation = result.frame.input("metric")
    assert observation.quality == "carried" and observation.event_time_ms == 360000
    assert observation.available_at_ms == 420001
    assert observation.mapping()["SOURCE_OPENINTEREST_TIME_MS"] == 360000
    assert observation.mapping()["SOURCE_LONGSHORTRATIO_AVAILABLE_MS"] == 420001
    expected_source = (
        "metric_view:"
        + sha256(
            (BTC.key + ":open_interest_history\0" + BTC.key + ":long_short_ratio").encode()
        ).hexdigest()
    )
    assert observation.source == expected_source
    for original_pin, selected_pin in zip(pins, result.pins, strict=True):
        assert encode_observation(BTC, original_pin.observation) == encode_observation(
            BTC, selected_pin.observation
        )
        assert (selected_pin.revision, selected_pin.checksum, selected_pin.previous_checksum) == (
            original_pin.revision,
            original_pin.checksum,
            original_pin.previous_checksum,
        )


def test_age_boundary_and_actual_late_cutoff_are_distinct_and_do_not_discard_selected_pins():
    pins = native_pair()
    policy = MetricInputPolicy(300000)
    at_close = rebuilt(pins, policy=policy)
    assert at_close.frame.input("metric").quality == "carried"
    late = rebuilt(pins, policy=policy, as_of_ms=660001)
    assert late.frame.input("metric").quality == "missing"
    assert (
        late.frame.input("metric").available_at_ms == 660001
        and not late.frame.input("metric").values
    )
    assert late.pins == pins
    validate_metric(BAR, late.frame.input("metric"), as_of_ms=660001, policy=policy, pins=pins)
    with pytest.raises(ProtocolError, match="eligibility"):
        rebuilt(native_pair(event=359999), policy=policy)


def test_missing_source_and_non_full_source_coverage_remain_missing_with_original_pins():
    oi = sample("open_interest")
    for pins in (
        (),
        (oi,),
        (
            replace(
                oi,
                observation=replace(oi.observation, coverage=D(".5")),
                checksum=observation_checksum(
                    encode_observation(BTC, replace(oi.observation, coverage=D(".5")))
                ),
            ),
            sample("long_short_ratio"),
        ),
    ):
        result = rebuilt(pins)
        assert (
            result.frame.input("metric").quality == "missing"
            and not result.frame.input("metric").values
        )
        assert result.pins == pins


def test_disabled_age_and_spot_do_not_acquire_futures_pins_or_fabricate_values():
    disabled = rebuilt((), policy=MetricInputPolicy(0))
    assert disabled.pins == () and disabled.frame.input("metric").quality == "missing"
    spot = replace(BAR, instrument=Instrument("binance", Market.SPOT, "BTCUSDT"))
    unavailable = rebuilt((), candle=spot)
    assert unavailable.pins == () and unavailable.frame.input("metric").quality == "unsupported"
    for candle, policy in ((BAR, MetricInputPolicy(0)), (spot, POLICY)):
        with pytest.raises(ProtocolError):
            rebuilt(candle=candle, policy=policy)


def test_carried_metrics_use_real_mst_age_quality_not_generic_observed_only_readiness():
    result = rebuilt()
    metric = result.frame.input("metric")
    assert metric.quality == "carried" and not metric.usable_at(660000)
    frame = with_flow(result)
    strict, loose = graph(299999), graph(330000)
    strict.update_frame(frame)
    loose.update_frame(frame)
    assert not strict.input_ready and loose.input_ready
    assert all(
        value is None for key, value in strict._output.items() if not key.startswith("CLOSED_")
    )
    zero = rebuilt(native_pair(value=D(0)))
    zero_graph = graph(330000)
    zero_graph.update_frame(with_flow(zero))
    assert zero.frame.input("metric").mapping()["METRIC_OPENINTEREST"] == 0
    assert not zero_graph.input_ready


@pytest.mark.parametrize(
    "fault",
    [
        "source",
        "channel",
        "foreign",
        "duplicate",
        "checksum",
        "revision",
        "predecessor",
        "event_close",
        "too_old",
        "late_receipt",
        "wrong_window",
        "partial",
        "field_missing",
        "negative",
        "int",
    ],
)
def test_bad_native_pin_is_not_reinterpreted_as_an_available_metric(fault):
    oi, ls = native_pair()
    observation = oi.observation
    if fault == "source":
        observation = replace(observation, source=BTC.key + ":open_interest")
    elif fault == "channel":
        observation = replace(observation, channel="metric")
    elif fault == "foreign":
        oi = replace(oi, instrument=Instrument("binance", Market.USDM, "ETHUSDT"))
    elif fault == "duplicate":
        with pytest.raises(ProtocolError, match="duplicate"):
            rebuilt((oi, oi))
        return
    elif fault == "checksum":
        oi = replace(oi, checksum="0" * 64)
    elif fault == "revision":
        object.__setattr__(oi, "revision", True)
    elif fault == "predecessor":
        object.__setattr__(oi, "previous_checksum", "0" * 64)
    elif fault in {"event_close", "too_old"}:
        oi = sample("open_interest", event=660000 if fault == "event_close" else 329999)
        observation = oi.observation
    elif fault == "late_receipt":
        oi = sample("open_interest", available=660001)
        observation = oi.observation
    elif fault == "wrong_window":
        observation = replace(observation, window_start_ms=300000, window_end_ms=360000)
    elif fault == "partial":
        observation = replace(observation, quality="partial", coverage=D(".5"))
    else:
        values = observation.mapping()
        if fault == "field_missing":
            del values["METRIC_OPENINTEREST"]
        else:
            values["METRIC_OPENINTEREST"] = D(-1) if fault == "negative" else 1
        observation = replace(observation, values=tuple(values.items()))
    if observation != oi.observation:
        oi = replace(
            oi,
            observation=observation,
            checksum=observation_checksum(encode_observation(BTC, observation)),
        )
    with pytest.raises((ProtocolError, ValidationError)):
        rebuilt((oi, ls))


@pytest.mark.parametrize("fault", ["value", "source_time", "source", "quality", "pin_order"])
def test_changed_derived_view_or_native_pin_order_cannot_replace_recorded_selection(fault):
    original = rebuilt()
    observation, pins = original.frame.input("metric"), original.pins
    if fault == "value":
        values = observation.mapping()
        values["METRIC_OPENINTEREST"] += D(1)
        observation = replace(observation, values=tuple(values.items()))
    elif fault == "source_time":
        values = observation.mapping()
        values["SOURCE_OPENINTEREST_TIME_MS"] += 1
        observation = replace(observation, values=tuple(values.items()))
    elif fault == "source":
        observation = replace(observation, source="metric_view:" + "0" * 64)
    elif fault == "quality":
        observation = replace(observation, quality="observed")
    else:
        pins = tuple(reversed(pins))
    with pytest.raises(ProtocolError):
        validate_metric(BAR, observation, as_of_ms=660000, policy=POLICY, pins=pins)


def test_non_native_cutoff_policy_and_candle_reject_before_rebuild():
    for kwargs in (
        {"as_of_ms": True},
        {"as_of_ms": 659999},
        {"policy": POLICY.document()},
        {"candle": replace(BAR, instrument=Instrument("upbit", Market.SPOT, "BTC-KRW"))},
    ):
        with pytest.raises((ProtocolError, ValidationError)):
            rebuilt(**kwargs)


def test_wrong_native_window_end_rejects_even_with_independently_recomputed_checksum():
    oi, ls = native_pair()
    original_payload = encode_observation(BTC, oi.observation)
    original_state = (oi.observation.window_start_ms, oi.observation.window_end_ms, oi.checksum)
    forged = json.loads(original_payload)
    forged["observation"]["window_end_ms"] += 60000
    forged_payload = json.dumps(
        forged, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode()
    forged_pin = replace(oi, checksum=sha256(forged_payload).hexdigest())
    unsafe = replace(oi.observation)
    object.__setattr__(unsafe, "window_end_ms", forged["observation"]["window_end_ms"])
    object.__setattr__(forged_pin, "observation", unsafe)
    with pytest.raises((ProtocolError, ValidationError)):
        rebuilt((forged_pin, ls))
    assert (
        oi.observation.window_start_ms,
        oi.observation.window_end_ms,
        oi.checksum,
    ) == original_state
    assert encode_observation(BTC, oi.observation) == original_payload
