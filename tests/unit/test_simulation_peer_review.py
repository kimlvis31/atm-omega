"""Independent replay cancellation and shared-analysis integration review."""

from decimal import Decimal as D

import pytest

from omega.analysis.catalog import create_features, validate_configuration
from omega.domain.models import Candle, Instrument, Market
from omega.research.datasets import MarketRow
from omega.research.simulation import (
    CancelPendingRequest,
    OrderRequest,
    SimulationSpec,
    simulate,
)
from omega.research.strategy_binding import build_features

BTC = "binance:usdm:BTCUSDT"
ETH = "binance:usdm:ETHUSDT"


def rows(count=4, instruments=(BTC, ETH)):
    return [
        MarketRow(instrument, minute * 60000, D(100), D(101), D(99), D(100), D(10))
        for minute in range(count)
        for instrument in instruments
    ]


def test_cancel_one_instrument_keeps_other_pending_and_replacement_is_next_minute():
    class Callback:
        def __init__(self):
            self.acknowledged = []

        def __call__(self, view):
            if view.timestamp_ms == 60000:
                return [
                    OrderRequest(BTC, D(1), "limit", D(50)),
                    OrderRequest(ETH, D(1), "limit", D(50)),
                ]
            if view.timestamp_ms == 120000:
                assert view.pending_instruments == {BTC, ETH}
                return [CancelPendingRequest(BTC)]
            if view.timestamp_ms == 180000:
                assert view.positions[BTC] == 1 and view.positions[ETH] == 0
                assert view.pending_instruments == {ETH}
            return []

        def on_cancellation_confirmed(self, instrument):
            self.acknowledged.append(instrument)
            return [OrderRequest(instrument, D(1), tag="replacement")]

    callback = Callback()
    result = simulate(rows(), callback, SimulationSpec(fee_rate=D(0)))
    assert callback.acknowledged == [BTC]
    assert len(result["fills"]) == 1
    assert result["fills"][0]["timestamp_ms"] == "120000"
    assert result["fills"][0]["tag"] == "replacement"
    assert result["pending_orders"] == 1
    cancellation = [event for event in result["events"] if event["kind"] == "orders_canceled"]
    assert cancellation == [
        {"kind": "orders_canceled", "instrument": BTC, "timestamp_ms": 120000, "count": 1}
    ]


def test_cancel_cannot_undo_a_fill_that_preceded_the_closed_bar_callback():
    observed = []

    def callback(view):
        if view.timestamp_ms == 60000:
            return [OrderRequest(BTC, D(1), "limit", D(100))]
        if view.timestamp_ms == 120000:
            observed.append((view.positions[BTC], view.pending_instruments))
            return [CancelPendingRequest(BTC)]
        return []

    with pytest.raises(ValueError, match="no pending order"):
        simulate(rows(instruments=(BTC,)), callback, SimulationSpec())
    assert observed == [(D(1), frozenset())]


def test_plain_callback_without_ack_hook_can_cancel_without_replacement():
    def callback(view):
        if view.timestamp_ms == 60000:
            return [OrderRequest(BTC, D(1), "limit", D(50))]
        if view.timestamp_ms == 120000:
            return [CancelPendingRequest(BTC)]
        return []

    result = simulate(rows(instruments=(BTC,)), callback, SimulationSpec())
    assert result["fills"] == [] and result["pending_orders"] == 0


def test_callback_failure_after_cancel_propagates_instead_of_claiming_complete():
    class Callback:
        def __call__(self, view):
            if view.timestamp_ms == 60000:
                return [OrderRequest(BTC, D(1), "limit", D(50))]
            if view.timestamp_ms == 120000:
                return [CancelPendingRequest(BTC)]
            return []

        def on_cancellation_confirmed(self, instrument):
            raise RuntimeError("injected replacement planner failure")

    with pytest.raises(RuntimeError, match="replacement planner failure"):
        simulate(rows(instruments=(BTC,)), Callback(), SimulationSpec())


def test_acknowledgement_is_after_pending_removal_not_a_pretend_confirmation():
    class Callback:
        def __call__(self, view):
            if view.timestamp_ms == 60000:
                return [OrderRequest(BTC, D(1), "limit", D(50))]
            if view.timestamp_ms == 120000:
                return [CancelPendingRequest(BTC)]
            return []

        def on_cancellation_confirmed(self, instrument):
            # If removal had not happened, this second cancellation could be accepted.
            return [CancelPendingRequest(instrument)]

    with pytest.raises(ValueError, match="no pending order"):
        simulate(rows(instruments=(BTC,)), Callback(), SimulationSpec())


def test_research_shared_analysis_values_match_catalog_with_swing_and_higher_bars():
    instrument = Instrument("binance", Market.USDM, "BTCUSDT")
    configuration = {
        "moving_averages": [
            {"interval_id": 0, "ema_period": 3},
            {"interval_id": 1, "ema_period": 2},
        ],
        "swing": [{"interval_id": 0, "line_index": 0, "swing_range": 0.005}],
    }
    replay = build_features(instrument, configuration)
    shared = create_features(
        instrument, {"CLOSED_0", "CLOSED_1"}, validate_configuration(configuration)
    )
    for minute in range(30):
        opened = D(100 + minute % 5)
        candle = Candle(
            instrument,
            minute * 60000,
            (minute + 1) * 60000,
            opened,
            opened + 2,
            opened - 2,
            opened + 1,
            D(10),
        )
        assert replay.update(candle) == shared.update(candle)
    assert replay.last_open_time_ms == 29 * 60000
