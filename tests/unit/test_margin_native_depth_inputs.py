"""Native channel/pin freezing only; actual builder, no DB/network/financial mutation."""

import json
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from omega.analysis.order_flow import DEPTH_FIELDS, FLOW_FIELDS
from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.application.market_frame_builder import DEPTH_SOURCE_SUFFIX, MarketFrameBuilder
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market
from omega.ports.analysis_inputs import PreparedMarketFrame
from omega.ports.auxiliary import StoredAuxiliary
from omega.ports.modules import ModuleDescriptor
from omega.runtime.margin_session_inputs import MarginSessionInputs, _encoded, unavailable_frames

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
BAR = Candle(BTC, 60000, 120000, *(D(value) for value in (100, 101, 99, 100, 2)))
CUTOFF = 120010
DESTINATION = "d" * 64


def helper(channels=("depth", "trade_flow")):
    graph = SimpleNamespace(
        descriptor=ModuleDescriptor("native_fixture", "1", frozenset({"OHLCV", *channels}), True)
    )
    return MarginSessionInputs([{"id": "ts", "instrument": BTC}], {"ts": graph})


def native(channel, *, quality="observed", revision=1, **changes):
    fields = FLOW_FIELDS if channel == "trade_flow" else DEPTH_FIELDS
    empty = quality in {"missing", "unsupported"}
    values = () if empty else tuple((field, D(index + 1)) for index, field in enumerate(fields))
    if channel == "depth" and not empty:
        values += (("DEPTH_UPDATE_ID", 57), ("DEPTH_PROFILE_VERSION", 1))
    event = BAR.close_time_ms if channel == "trade_flow" or empty else BAR.close_time_ms - 1
    observation = AuxiliaryInput(
        channel,
        values,
        BAR.open_time_ms,
        BAR.close_time_ms,
        event,
        CUTOFF,
        quality,
        D(0) if empty else D(".5") if quality == "partial" else D(1),
        BTC.key + ":" + ("aggTrade" if channel == "trade_flow" else DEPTH_SOURCE_SUFFIX),
    )
    observation = replace(observation, **changes)
    checksum = observation_checksum(encode_observation(BTC, observation))
    return StoredAuxiliary(BTC, observation, revision, checksum, "b" * 64 if revision > 1 else None)


def prepared(*pins, omit=(), cutoff=CUTOFF):
    return PreparedMarketFrame(
        MarketFeatureFrame(BAR, cutoff, tuple(pin.observation for pin in pins)),
        tuple(pin for pin in pins if pin.observation.channel not in omit),
    )


def freeze(inputs, value):
    return inputs.freeze((value,), (BAR,), as_of_ms=CUTOFF, destination=DESTINATION)


def test_exact_required_channels_union_per_instrument_and_metrics_stay_unsupported():
    rows = [{"id": name, "instrument": BTC} for name in ("flow", "depth")]
    graphs = {
        name: SimpleNamespace(descriptor=ModuleDescriptor(name, "1", frozenset({channel}), True))
        for name, channel in (("flow", "trade_flow"), ("depth", "depth"))
    }
    inputs = MarginSessionInputs(rows, graphs)
    assert inputs.channels == {BTC.key: ("depth", "trade_flow")}
    assert inputs.strategies == {"flow", "depth"}
    with pytest.raises(ValidationError, match="trade_flow and depth"):
        helper(("metric",))


def test_actual_builder_freezes_full_original_channels_and_native_revision_pins_canonically():
    flow, depth = native("trade_flow"), native("depth", revision=2)
    calls = []

    class Store:
        def get(self, instrument, channel, source, opening, *, as_of_ms):
            calls.append((instrument, channel, source, opening, as_of_ms))
            return {"trade_flow": flow, "depth": depth}[channel]

    value = MarketFrameBuilder(Store(), maximum_metric_age_ms=0).prepare(
        BAR, as_of_ms=CUTOFF, channels=("trade_flow", "depth")
    )
    inputs = helper()
    document, selected = freeze(inputs, value)
    assert selected[BTC.key] == value.frame
    assert [
        json.loads(item)["observation"]["channel"] for item in document["frames"][0]["inputs"]
    ] == ["depth", "trade_flow"]
    assert document["frames"][0]["pins"][0] == {
        "revision": 2,
        "checksum": depth.checksum,
        "previous_checksum": "b" * 64,
        "payload": encode_observation(BTC, depth.observation).decode(),
    }
    assert len(calls) == 2 and {call[1] for call in calls} == {"depth", "trade_flow"}
    copied = json.loads(
        _encoded(document)
    )  # Same persisted representation, no new source selection.
    assert helper().restore(copied, (BAR,), as_of_ms=CUTOFF, expected=DESTINATION) == selected
    assert depth.observation.event_time_ms == BAR.close_time_ms - 1
    assert selected[BTC.key].input("depth").mapping()["DEPTH_UPDATE_ID"] == 57


def test_flow_only_document_and_status_remain_exact_original_encoding():
    flow = native("trade_flow")
    inputs = helper(("trade_flow",))
    document, selected = freeze(inputs, prepared(flow))
    payload = encode_observation(BTC, flow.observation).decode()
    expected = {
        "destination": DESTINATION,
        "frames": [
            {
                "candle": {
                    "instrument": BTC.key,
                    "open_time_ms": 60000,
                    "close_time_ms": 120000,
                    "values": ["100", "101", "99", "100", "2"],
                },
                "as_of_ms": CUTOFF,
                "inputs": [payload],
                "pins": [
                    {
                        "revision": 1,
                        "checksum": flow.checksum,
                        "previous_checksum": None,
                        "payload": payload,
                    }
                ],
            }
        ],
    }
    assert _encoded(document) == _encoded(expected)
    assert inputs.status(document) == {
        "state": "ready",
        "reason": None,
        "destination": DESTINATION,
        "quality": ["observed"],
    }
    assert inputs.restore(expected, (BAR,), as_of_ms=CUTOFF) == selected
    (missing,) = unavailable_frames((BAR,), {BTC.key: ("trade_flow",)}, as_of_ms=CUTOFF)
    assert missing.frame.inputs == (
        AuxiliaryInput(
            "trade_flow", (), 60000, 120000, 120000, CUTOFF, "missing", D(0), BTC.key + ":aggTrade"
        ),
    )


@pytest.mark.parametrize("quality", ["partial", "missing", "unsupported"])
def test_unavailable_depth_stays_unavailable_and_flow_keeps_its_original_measurement(quality):
    depth, flow = native("depth", quality=quality), native("trade_flow")
    value = prepared(depth, flow, omit=("depth",) if quality != "partial" else ())
    document, selected = freeze(helper(), value)
    assert selected[BTC.key].input("depth").quality == quality
    assert selected[BTC.key].input("trade_flow") == flow.observation
    status = helper().status(document)
    assert status["state"] == "unavailable" and status["quality"] == [quality, "observed"]
    assert status["channel_quality"] == [
        {"instrument": BTC.key, "channel": "depth", "quality": quality},
        {"instrument": BTC.key, "channel": "trade_flow", "quality": "observed"},
    ]
    assert helper().restore(document, (BAR,), as_of_ms=CUTOFF) == selected


def test_native_missing_depth_pin_and_fallback_empty_channels_both_keep_the_original_clock():
    inputs = helper()
    depth, flow = native("depth", quality="missing"), native("trade_flow")
    document, selected = freeze(inputs, prepared(depth, flow))
    assert len(document["frames"][0]["pins"]) == 2
    assert selected[BTC.key].input("depth").event_time_ms == BAR.close_time_ms
    (missing,) = unavailable_frames((BAR,), inputs.channels, as_of_ms=CUTOFF)
    document, selected = freeze(inputs, missing)
    assert missing.pins == () and document["frames"][0]["pins"] == []
    assert [value.channel for value in selected[BTC.key].inputs] == ["depth", "trade_flow"]
    assert all(
        value.quality == "missing" and not value.values and value.coverage == 0
        for value in selected[BTC.key].inputs
    )


@pytest.mark.parametrize(
    "fault", ["source", "event", "missing_field", "negative", "wrong_type", "carried"]
)
def test_bad_native_depth_is_rejected_without_reinterpreting_a_channel(fault):
    changes = {}
    original = native("depth").observation
    if fault == "source":
        changes["source"] = BTC.key + ":other_depth"
    elif fault == "event":
        changes["event_time_ms"] = BAR.close_time_ms
    elif fault == "carried":
        changes["quality"] = "carried"
    else:
        values = dict(original.values)
        if fault == "missing_field":
            del values[DEPTH_FIELDS[0]]
        else:
            values[DEPTH_FIELDS[0]] = D(-1) if fault == "negative" else 1
        changes["values"] = tuple(values.items())
    depth = native("depth", **changes)
    with pytest.raises((ProtocolError, ValidationError)):
        freeze(helper(), prepared(depth, native("trade_flow")))


@pytest.mark.parametrize(
    "fault", ["missing", "duplicate", "foreign", "checksum", "revision", "orphan"]
)
def test_native_pins_are_channel_matched_unique_and_checksum_exact(fault):
    depth, flow = native("depth"), native("trade_flow")
    value = prepared(depth, flow)
    if fault == "missing":
        value = replace(value, pins=(flow,))
    elif fault == "duplicate":
        value = replace(value, pins=(depth, depth))
    elif fault == "foreign":
        value = replace(
            value,
            pins=(replace(depth, instrument=Instrument("binance", Market.USDM, "ETHUSDT")), flow),
        )
    elif fault == "checksum":
        value = replace(value, pins=(replace(depth, checksum="0" * 64), flow))
    elif fault == "revision":
        object.__setattr__(depth, "revision", True)
    else:
        orphan = replace(depth, observation=replace(depth.observation, channel="metric"))
        value = replace(value, pins=(orphan, flow))
    with pytest.raises((ProtocolError, ValidationError)):
        freeze(helper(), value)


def test_cutoff_channel_set_replay_and_destination_cannot_replace_frozen_inputs():
    inputs = helper()
    flow, depth = native("trade_flow"), native("depth")
    document, _selected = freeze(inputs, prepared(depth, flow))
    with pytest.raises(ProtocolError):
        freeze(inputs, prepared(depth))
    with pytest.raises(ProtocolError):
        freeze(inputs, prepared(depth, flow, cutoff=CUTOFF + 1))
    with pytest.raises(ProtocolError, match="destination"):
        inputs.restore(document, (BAR,), as_of_ms=CUTOFF, expected="e" * 64)
    changed = native(
        "depth",
        revision=2,
        values=tuple(
            (key, value + D(1) if key in DEPTH_FIELDS else value)
            for key, value in depth.observation.values
        ),
    )
    with pytest.raises(ProtocolError, match="replaced"):
        inputs.check_replay(
            (prepared(changed, flow),), DESTINATION, document, (BAR,), as_of_ms=CUTOFF
        )
    inputs.check_replay(None, None, document, (BAR,), as_of_ms=CUTOFF)
    with pytest.raises(ProtocolError):
        helper(("depth",)).restore(document, (BAR,), as_of_ms=CUTOFF)
