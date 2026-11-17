"""Actual shallow native depth must stay partial while financial protection continues."""

import json
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from test_margin_native_depth_strategy import BTC, PinStore, Registry, bar, config, pin, prepared

from omega.analysis.order_flow import DEPTH_FIELDS, FLOW_FIELDS
from omega.application.auxiliary_codec import encode_observation
from omega.application.market_frame_builder import MarketFrameBuilder
from omega.domain.errors import ProtocolError
from omega.infrastructure.binance.depth_book import BookView
from omega.infrastructure.binance.depth_sampling import DepthMinuteSampler
from omega.runtime.margin_session import MarginTradingSession
from omega.runtime.margin_session_inputs import MarginSessionInputs

DESTINATION = "d" * 64
KNOWN_BINS = {"DEPTH_BIDS0", "DEPTH_BIDS1", "DEPTH_ASKS0", "DEPTH_ASKS1"}


def sampled_partial(index, *, low="99", lag=0):
    candle = bar(index, low=low)
    sampler = DepthMinuteSampler(BTC, price_precision=2, quote_precision=8)
    sampler.begin(1, started_at_ms=candle.open_time_ms)
    view = BookView(
        57,
        ((D("99.9"), D(2)), (D(99), D(2))),
        ((D("100.1"), D(1)), (D(101), D(1))),
        D(99),
        D(101),
    )
    event = candle.close_time_ms - 1
    assert sampler.observe(
        view,
        event_time_ms=event,
        received_at_ms=event + lag,
        accepted_at_ms=event + lag,
        generation=1,
        raw_refs=("a" * 64,),
    )
    minute = sampler.finalize(candle.open_time_ms, as_of_ms=candle.close_time_ms + lag)
    depth = minute.observation
    assert depth.quality == "partial" and 0 < depth.coverage < 1
    assert set(depth.mapping()) & set(DEPTH_FIELDS) == KNOWN_BINS
    assert "DEPTH_BIDS5" not in depth.mapping() and "DEPTH_ASKS5" not in depth.mapping()
    flow = prepared(index, low=low).frame.input("trade_flow")
    value = MarketFrameBuilder(PinStore((pin(flow), pin(depth)))).prepare(
        candle, as_of_ms=candle.close_time_ms + lag, channels=("depth", "trade_flow")
    )
    return value, depth, minute


def input_reader():
    return MarginSessionInputs(
        [{"id": "native", "instrument": BTC}],
        {
            "native": SimpleNamespace(
                descriptor=SimpleNamespace(required_fields={"OHLCV", "depth", "trade_flow"})
            )
        },
    )


def freeze(reader, value):
    return reader.freeze(
        (value,), (value.frame.candle,), as_of_ms=value.frame.as_of_ms, destination=DESTINATION
    )


def test_real_shallow_sampler_subset_is_frozen_restored_unready_without_fabricated_bins():
    value, original, minute = sampled_partial(0, lag=3)
    reader = input_reader()
    document, selected = freeze(reader, value)
    restored = reader.restore(
        json.loads(json.dumps(document)),
        (value.frame.candle,),
        as_of_ms=60003,
        expected=DESTINATION,
    )
    assert selected == restored and selected[BTC.key] == value.frame
    assert reader.status(document)["state"] == "unavailable"
    assert selected[BTC.key].input("depth") == original
    assert original.event_time_ms == 59999 and original.available_at_ms == 60003
    assert original.mapping()["DEPTH_RECEIVED_AT_MS"] == 60002
    assert minute.raw_refs == ("a" * 64,)
    record = document["frames"][0]["pins"][0]
    assert record["payload"].encode() == encode_observation(BTC, original)
    assert record["checksum"] == value.pins[0].checksum
    assert not {"DEPTH_BIDS5", "DEPTH_ASKS5"} & original.mapping().keys()


def test_real_partial_depth_preserves_accepted_fill_local_stop_and_next_healthy_recovery(tmp_path):
    account, strategies, brackets, _ = config()
    session = MarginTradingSession(
        tmp_path, account, strategies, 0, registry=Registry(), create=True
    )
    try:
        session.resume("explicit_partial_depth_fixture_resume")
        for index in range(5):
            value = (
                sampled_partial(index, low="90" if index == 2 else "99")[0]
                if index in {2, 3}
                else prepared(index)
            )
            view = session.run_once(
                (value.frame.candle,),
                as_of_ms=value.frame.as_of_ms,
                brackets=brackets,
                prepared_frames=(value,),
                auxiliary_destination=DESTINATION,
            )
            assert view["graph_errors"] == {} and view["state"] == "ready"
            ledger = session.execution.snapshot().account.ledger
            if index == 1:
                assert view["pending_orders"] and not ledger
            elif index == 2:
                assert view["market_inputs"]["state"] == "unavailable"
                assert len(ledger) == 1 and ledger[0].fact.delta_qty == D(1)
                assert view["pending_orders"]  # Independent local reduction, no partial entry.
                assert session._meta["last_frame"]["graph_ready"]["flow"] is False
            elif index == 3:
                assert [entry.fact.delta_qty for entry in ledger] == [D(1), D(-1)]
                assert not view["pending_orders"]
                assert session.execution.snapshot().account.positions[0].quantity == 0
            elif index == 4:
                assert view["market_inputs"]["state"] == "ready"
                assert session._meta["last_frame"]["graph_ready"]["flow"] is True
                assert view["pending_orders"]  # Existing healthy graph can admit entries again.
        assert [entry.fact.event_time_ms for entry in ledger] == [120000, 180000]
    finally:
        session.close()


@pytest.mark.parametrize("bad", [D(-1), 1])
def test_present_partial_depth_bin_still_requires_nonnegative_native_decimal(bad):
    value, depth, _ = sampled_partial(0)
    values = depth.mapping()
    values["DEPTH_BIDS0"] = bad
    changed = replace(depth, values=tuple(values.items()))
    row = MarketFrameBuilder(
        PinStore((pin(value.frame.input("trade_flow")), pin(changed)))
    ).prepare(value.frame.candle, as_of_ms=value.frame.as_of_ms, channels=("depth", "trade_flow"))
    with pytest.raises(ProtocolError, match="nonnegative Decimal"):
        freeze(input_reader(), row)


def test_observed_depth_missing_bins_and_partial_flow_missing_field_stay_strict():
    value, depth, _ = sampled_partial(0)
    observed = replace(depth, quality="observed", coverage=D(1))
    flow = value.frame.input("trade_flow")
    incomplete_flow = replace(
        flow,
        quality="partial",
        coverage=D(".5"),
        values=tuple((key, number) for key, number in flow.values if key != FLOW_FIELDS[0]),
    )
    for selected_depth, selected_flow in (
        (observed, flow),
        (prepared(0).frame.input("depth"), incomplete_flow),
    ):
        row = MarketFrameBuilder(PinStore((pin(selected_flow), pin(selected_depth)))).prepare(
            value.frame.candle, as_of_ms=value.frame.as_of_ms, channels=("depth", "trade_flow")
        )
        with pytest.raises(ProtocolError, match="nonnegative Decimal"):
            freeze(input_reader(), row)


def test_actual_metadata_only_zero_coverage_partial_keeps_its_pin_and_unavailable_state():
    candle = bar(0)
    sampler = DepthMinuteSampler(BTC, price_precision=2, quote_precision=8)
    sampler.begin(1, started_at_ms=0)
    shallow = BookView(58, ((D("99.999"), D(2)),), ((D("100.001"), D(1)),), D("99.99"), D("100.01"))
    assert sampler.observe(
        shallow,
        event_time_ms=59999,
        received_at_ms=60001,
        accepted_at_ms=60002,
        generation=1,
        raw_refs=("b" * 64,),
    )
    minute = sampler.finalize(0, as_of_ms=60003)
    depth = minute.observation
    assert depth.quality == "partial" and depth.coverage == 0
    assert depth.values and not set(depth.mapping()) & set(DEPTH_FIELDS)
    assert depth.mapping()["DEPTH_CENTER_PRICE"] == D(100)
    assert depth.mapping()["DEPTH_RECEIVED_AT_MS"] == 60001
    flow = prepared(0).frame.input("trade_flow")
    value = MarketFrameBuilder(PinStore((pin(flow), pin(depth)))).prepare(
        candle, as_of_ms=60003, channels=("depth", "trade_flow")
    )
    reader = input_reader()
    document, selected = freeze(reader, value)
    restored = reader.restore(
        json.loads(json.dumps(document)), (candle,), as_of_ms=60003, expected=DESTINATION
    )
    assert selected == restored and selected[BTC.key].input("depth") == depth
    assert reader.status(document)["state"] == "unavailable"
    saved = document["frames"][0]["pins"][0]
    assert saved["payload"].encode() == encode_observation(BTC, depth)
    assert saved["checksum"] == value.pins[0].checksum and saved["revision"] == 1
    assert len(document["frames"][0]["pins"]) == 2
