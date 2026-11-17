"""TEF family sizing and local protection with the real private MMACD model."""

from dataclasses import replace
from decimal import Decimal

import pytest

from omega.application.trading import TradingEngine
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext
from omega.strategies.tef.module import TefModule

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def context(index=0, close="100", feature=1.0, **changes):
    price = Decimal(close)
    candle = Candle(
        INSTRUMENT, index * 60_000, (index + 1) * 60_000, price, price, price, price, Decimal(1)
    )
    return replace(
        TradeContext(
            INSTRUMENT,
            price,
            candle.close_time_ms,
            PositionView(),
            BudgetView(Decimal(100), Decimal(100)),
            ExecutionConstraints(quantity_step=Decimal("0.001"), can_short=True),
            candle,
            {"0_MMACD_MSDELTAABSMAREL": feature},
        ),
        **changes,
    )


@pytest.mark.parametrize(
    "gate",
    [
        {"state_certain": False},
        {"control_owned": False},
        {"has_pending_order": True},
        {"entries_allowed": False},
    ],
)
def test_blocked_minutes_advance_private_model_and_do_not_leave_a_replay_gap(gate):
    engine = TradingEngine(TefModule(INSTRUMENT))
    assert not engine.evaluate(context(0, feature=0.2, **gate)).orders
    assert not engine.evaluate(context(1, feature=0.4, **gate)).orders
    assert engine.module.model.state["la_mmacd_msDeltaAbsMARel_prev"] == 0.4
    decision = engine.evaluate(context(2, feature=-1.0))
    assert decision.orders[0].side == Side.SELL
    assert engine.module.model.state["la_mmacd_msDeltaAbsMARel_prev"] == -1.0


def test_missing_features_preserve_held_position_instead_of_zero_target_exit():
    engine = TradingEngine(TefModule(INSTRUMENT))
    held = PositionView(Decimal(1), Decimal(100), 1)
    assert engine.evaluate(context(0, position=held, feature=1.0)).orders
    warmup = engine.evaluate(context(1, position=held, features={}))
    assert not warmup.orders
    assert warmup.reason == "indicator_warmup"
    assert engine.module.model.last_direction == "LONG"
    assert engine.evaluate(context(2, position=held, feature=-1.0)).reason == "CLEAR"


@pytest.mark.parametrize("quantity,feature,side", [("1", 1.0, Side.BUY), ("-1", -1.0, Side.SELL)])
def test_incremental_margin_uses_entry_price_and_available_budget(quantity, feature, side):
    module = TefModule(INSTRUMENT, {"max_analysis_deviation": "1"})
    # tanh(1) rounds to 0.761594: desired margin 76.1594, existing margin 25.
    result = TradingEngine(module).evaluate(
        context(
            close="100",
            feature=feature,
            position=PositionView(Decimal(quantity), Decimal(50), 1),
            budget=BudgetView(Decimal(100), Decimal(10)),
            constraints=ExecutionConstraints(leverage=Decimal(2), can_short=True),
        )
    )
    assert result.reason == "ENTRY"
    assert result.orders[0].side == side
    assert result.orders[0].quantity == Decimal("0.200")


def test_downsize_uses_entry_price_not_current_mark_and_survives_entry_pause():
    result = TradingEngine(TefModule(INSTRUMENT)).evaluate(
        context(
            close="100",
            position=PositionView(Decimal(4), Decimal(50), 1),
            budget=BudgetView(Decimal(100), Decimal(0)),
            entries_allowed=False,
            constraints=ExecutionConstraints(leverage=Decimal(2), can_short=True),
        )
    )
    # (100 - 76.1594) * 2 / 50 = 0.953624, floored to quantity step.
    assert result.reason == "EXIT"
    assert result.orders[0].quantity == Decimal("0.953")
    assert result.orders[0].reducing


def test_zero_signal_closes_entire_position_without_reversing():
    result = TradingEngine(TefModule(INSTRUMENT)).evaluate(
        context(
            feature=0.0,
            position=PositionView(Decimal("0.3"), Decimal(100), 1),
            entries_allowed=False,
        )
    )
    assert result.reason == "EXIT"
    assert result.orders[0].quantity == Decimal("0.3")
    assert result.orders[0].reducing


def test_adaptive_reversal_closes_market_before_opening_new_limit_direction():
    engine = TradingEngine(
        TefModule(INSTRUMENT, {"order_type": "ADAPTIVE", "order_offset": "0.001"})
    )
    sample = context(feature=-1.0, position=PositionView(Decimal(1), Decimal(100), 1))
    close = engine.evaluate(sample)
    assert close.reason == "CLEAR"
    assert close.orders[0].kind == OrderKind.MARKET
    assert close.orders[0].quantity == 1
    assert close.orders[0].reducing
    entry = engine.evaluate(replace(sample, position=PositionView(revision=2)))
    assert entry.reason == "ENTRY"
    assert entry.orders[0].kind == OrderKind.LIMIT
    assert entry.orders[0].price == Decimal("100.10")
    assert entry.orders[0].side == Side.SELL
    assert not entry.orders[0].reducing


@pytest.mark.parametrize("quantity,price,side", [("1", "95", Side.SELL), ("-1", "105", Side.BUY)])
def test_stop_threshold_is_inclusive_market_protection_even_in_warmup(quantity, price, side):
    engine = TradingEngine(
        TefModule(INSTRUMENT, {"order_type": "LIMIT", "fullStopLossImmediate": "0.05"})
    )
    result = engine.evaluate(
        context(
            close=price,
            features={},
            entries_allowed=False,
            position=PositionView(Decimal(quantity), Decimal(100), 1),
        )
    )
    assert result.reason == "FSLIMMED"
    assert result.orders[0].kind == OrderKind.MARKET
    assert result.orders[0].side == side
    assert result.orders[0].reducing
    assert not engine.module.snapshot()["halted"]  # A proposal is not a confirmed fill.


@pytest.mark.parametrize(
    "last_changed,expected",
    [(0, "FSLCLOSE"), (1, "FSLCLOSE"), (60000, "FSLCLOSE"), (60001, "indicator_warmup")],
)
def test_close_stop_requires_position_to_exist_by_the_observed_close(last_changed, expected):
    engine = TradingEngine(TefModule(INSTRUMENT, {"fullStopLossClose": "0.05"}))
    result = engine.evaluate(
        context(
            close="95",
            features={},
            observed_at_ms=max(60000, last_changed),
            position=PositionView(Decimal(1), Decimal(100), 1, last_changed),
        )
    )
    assert result.reason == expected
    assert bool(result.orders) == (expected == "FSLCLOSE")


def stop_then_flat(engine, *, revision=2):
    engine.evaluate(
        context(close="90", position=PositionView(Decimal(1), Decimal(100), 1), features={})
    )
    return engine.evaluate(context(1, position=PositionView(revision=revision)))


def test_stop_halt_requires_newer_flat_revision_and_manual_resume():
    engine = TradingEngine(TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"}))
    stop_then_flat(engine, revision=1)
    assert not engine.module.snapshot()["halted"]
    confirmed = engine.evaluate(context(2, position=PositionView(revision=2)))
    assert confirmed.reason == "stopped_after_local_stop"
    assert engine.module.snapshot()["pending_stop"] is None
    engine.module.resume()
    # A new observation re-evaluates after explicit module-local resume.
    resumed = engine.evaluate(context(3, position=PositionView(revision=2)))
    assert resumed.reason == "ENTRY"
    assert engine.module.snapshot()["sl_exited"] is None


@pytest.mark.parametrize(
    "reenter,expected", [(False, "post_stop_reentry_blocked"), (True, "ENTRY")]
)
def test_reentry_option_controls_same_direction_after_confirmed_stop(reenter, expected):
    engine = TradingEngine(
        TefModule(
            INSTRUMENT,
            {
                "fullStopLossImmediate": "0.05",
                "stopTradeOnFSL": False,
                "postStopLossReentry": reenter,
            },
        )
    )
    result = stop_then_flat(engine)
    assert result.reason == expected
    assert not engine.module.snapshot()["halted"]
    assert engine.module.snapshot()["sl_exited"][0] == "LONG"


def test_opposite_direction_only_unlocks_reentry_after_stop_timestamp():
    engine = TradingEngine(
        TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05", "stopTradeOnFSL": False})
    )
    engine.evaluate(
        context(close="90", features={}, position=PositionView(Decimal(1), Decimal(100), 1))
    )
    # Eta records the stop's bar timestamp, not the later wall-clock observation.
    later = engine.evaluate(context(1, feature=-1.0, position=PositionView(revision=2)))
    assert later.reason == "ENTRY"
    assert later.orders[0].side == Side.SELL
    assert engine.module.snapshot()["sl_exited"] is None


@pytest.mark.parametrize("quantity,low,high", [("1", "94", "102"), ("-1", "98", "106")])
@pytest.mark.parametrize("changed,expected", [(0, "FSLIMMED"), (1, "indicator_warmup")])
def test_immediate_stop_uses_wick_only_if_position_existed_before_bar(
    quantity, low, high, changed, expected
):
    engine = TradingEngine(TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"}))
    sample = context(
        features={}, position=PositionView(Decimal(quantity), Decimal(100), 1, changed)
    )
    sample = replace(
        sample, closed_candle=replace(sample.closed_candle, low=Decimal(low), high=Decimal(high))
    )
    assert engine.evaluate(sample).reason == expected


def test_manual_stop_latch_takes_priority_over_automatic_reentry_option():
    engine = TradingEngine(
        TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05", "postStopLossReentry": True})
    )
    assert stop_then_flat(engine).reason == "stopped_after_local_stop"
    assert engine.module.snapshot()["halted"]


@pytest.mark.parametrize(
    "gate", [{"state_certain": False}, {"control_owned": False}, {"has_pending_order": True}]
)
def test_stop_does_not_latch_unsubmitted_protection_while_control_is_blocked(gate):
    engine = TradingEngine(TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"}))
    sample = context(
        close="90", features={}, position=PositionView(Decimal(1), Decimal(100), 1), **gate
    )
    assert not engine.evaluate(sample).orders
    assert engine.module.snapshot()["pending_stop"] is None
    cleared = replace(sample, state_certain=True, control_owned=True, has_pending_order=False)
    assert engine.evaluate(cleared).reason == "FSLIMMED"


@pytest.mark.parametrize("feature,expected", [(1.0, "99.87"), (-1.0, "100.13")])
def test_limit_offsets_round_away_from_crossing_market(feature, expected):
    engine = TradingEngine(
        TefModule(INSTRUMENT, {"order_type": "LIMIT", "order_offset": "0.00125"})
    )
    proposal = engine.evaluate(context(feature=feature)).orders[0]
    assert proposal.kind == OrderKind.LIMIT
    assert proposal.price == Decimal(expected)


def test_close_stop_uses_completed_loss_even_after_market_price_recovers():
    engine = TradingEngine(TefModule(INSTRUMENT, {"fullStopLossClose": "0.05"}))
    result = engine.evaluate(
        context(close="95", price=Decimal(100), position=PositionView(Decimal(1), Decimal(100), 1))
    )
    assert result.reason == "FSLCLOSE"
    assert result.orders[0].reducing


@pytest.mark.parametrize(
    "age,price,expected",
    [
        (120_000, "100", "ENTRY"),
        (120_001, "100", "expired_or_price_diverged_signal"),
        (0, "100.5", "expired_or_price_diverged_signal"),
    ],
)
def test_signal_age_and_price_deviation_boundaries(age, price, expected):
    result = TradingEngine(TefModule(INSTRUMENT)).evaluate(
        context(observed_at_ms=60_000 + age, price=Decimal(price))
    )
    assert result.reason == expected
