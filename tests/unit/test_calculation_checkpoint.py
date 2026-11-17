"""Exact restart parity across partial higher bars, indicators and TEF latches."""

import json
import math
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.strategies.registry import StrategyRegistry
from omega.strategies.tef.module import TefModule
from omega.strategies.tef.strategy import TefModel

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")
CONFIG = {
    "moving_averages": [
        {"interval_id": 0, "ema_period": 3, "mmacd_periods": [2, 4], "signal_period": 2},
        {"interval_id": 2, "ema_period": 4},
    ],
    "momentum": [
        {"interval_id": 0, "analysis_code": "MFI", "nSamples": 3},
        {"interval_id": 1, "analysis_code": "DMIxADX", "nSamples": 3},
    ],
    "swing": [{"interval_id": 0, "swing_range": 0.01}],
}
REQUIRED = {"0_MMACD_MSDELTAABSMAREL", "0_SWING_0_LSPRICE", "1_DMIxADX_0_DMIxADX"}


def candle(index):
    close = Decimal(str(100 + 10 * math.sin(index / 5)))
    return Candle(
        INSTRUMENT,
        index * 60000,
        (index + 1) * 60000,
        close,
        close + 2,
        close - 2,
        close,
        Decimal(10 + index),
    )


@pytest.mark.parametrize("split", [0, 1, 2, 13, 47, 91])
def test_json_roundtrip_restores_exact_feature_state_without_rewarming(split):
    original = create_features(INSTRUMENT, REQUIRED, CONFIG)
    for index in range(split):
        original.update(candle(index))
    state = json.loads(json.dumps(original.checkpoint()))
    restored = create_features(INSTRUMENT, REQUIRED, CONFIG).restored(state, INSTRUMENT)
    assert restored.checkpoint() == original.checkpoint()
    for index in range(split, 120):
        assert restored.update(candle(index)) == original.update(candle(index))


def test_checkpoint_corruption_or_changed_analysis_cannot_mutate_fresh_graph():
    original = create_features(INSTRUMENT, REQUIRED, CONFIG)
    original.update(candle(0))
    state = original.checkpoint()
    fresh = create_features(INSTRUMENT, REQUIRED, CONFIG)
    before = fresh.checkpoint()
    state["payload"]["type"] = "pickle.loads"
    with pytest.raises(ValidationError):
        fresh.restored(state, INSTRUMENT)
    assert fresh.checkpoint() == before
    other = deepcopy(CONFIG)
    other["moving_averages"][0]["ema_period"] = 5
    with pytest.raises(ValidationError, match="configuration"):
        create_features(INSTRUMENT, REQUIRED, other).restored(original.checkpoint(), INSTRUMENT)


def make_paper(state=None, persist=lambda state: None):
    return PaperTradingService(
        StrategyRegistry().create, create_features, state=state, persist=persist
    )


def prepare(paper):
    paper.command("paper.account.create", {"account_id": "a", "balance": "10000"})
    paper.command(
        "paper.strategy.add",
        {
            "instance": "tef",
            "account_id": "a",
            "instrument": INSTRUMENT.key,
            "module_id": "tef",
            "budget": "1000",
            "analysis": CONFIG,
            "configuration": {"fullStopLossImmediate": "0.01", "stopTradeOnFSL": False},
        },
    )
    paper.command("resume", {})


def step(paper, index):
    bar = candle(index)
    return paper.command(
        "paper.step",
        {
            "instrument": INSTRUMENT.key,
            "candle": {
                "open_time_ms": bar.open_time_ms,
                **{
                    key: str(getattr(bar, key))
                    for key in ("open", "high", "low", "close", "volume")
                },
            },
        },
    )


def test_paper_checkpoint_restart_keeps_exact_features_and_tef_model_after_manual_recovery():
    paper = make_paper()
    prepare(paper)
    for index in range(40):
        step(paper, index)
    # Submitted orders remain owned; explicit cancellation is necessary before recovery.
    paper.command("paper.order.cancel", {"instance": "tef"})
    state = json.loads(json.dumps(paper.data))
    restarted = make_paper(state)
    assert restarted.snapshot()["paused"]
    assert restarted.detail("tef")["status"] == "recovery_required"
    response = restarted.command("paper.strategy.recover", {"instance": "tef"})
    assert response["restored_checkpoint"]
    assert restarted.engines["tef"].module.checkpoint() == paper.engines["tef"].module.checkpoint()
    assert restarted.features["tef"].checkpoint() == paper.features["tef"].checkpoint()
    restarted.command("resume", {})
    for index in range(40, 70):
        step(paper, index)
        step(restarted, index)
        assert restarted.detail("tef") == paper.detail("tef")
        assert restarted.data["accounts"] == paper.data["accounts"]


def test_tef_halt_and_reentry_checkpoint_are_not_reset_by_reconstruction():
    module = TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"})
    module._halted = True
    module._sl_exited = ("LONG", 60000)
    restored = TefModule(INSTRUMENT, module.config)
    restored.restore(json.loads(json.dumps(module.checkpoint())))
    assert restored.snapshot() == module.snapshot()
    corrupt = module.checkpoint()
    corrupt["payload"]["halted"] = False
    with pytest.raises(ValidationError):
        restored.restore(corrupt)
    assert restored.snapshot()["halted"]


@pytest.mark.parametrize(
    "case",
    json.loads(Path("tests/fixtures/tef-legacy.json").read_text()),
    ids=lambda row: row["model"],
)
def test_all_six_tef_model_trackers_continue_exactly_after_json_checkpoint(case):
    model = TefModel(case["model"], INSTRUMENT)
    resumed = None
    for index, row in enumerate(case["steps"]):
        price = Decimal(str(row["features"]["KLINE_CLOSEPRICE"]))
        bar = Candle(
            INSTRUMENT, index * 60000, (index + 1) * 60000, price, price, price, price, Decimal(1)
        )
        signal = model.evaluate(bar, row["features"])
        if index in {3, 97, 181}:
            resumed = TefModel(case["model"], INSTRUMENT)
            resumed.restore(json.loads(json.dumps(model.checkpoint())))
        if resumed is not None:
            assert resumed.evaluate(bar, row["features"]) == signal
            assert resumed.checkpoint() == model.checkpoint()


def test_recover_on_an_already_running_strategy_also_requires_fresh_manual_resume():
    paper = make_paper()
    prepare(paper)
    assert not paper.data["paused"]
    paper.command("paper.strategy.recover", {"instance": "tef", "reset_model": True})
    assert paper.data["paused"]
    assert not paper.data["feed"]["ready"]


def test_paper_local_stop_cancels_only_pending_order_before_confirmed_replacement():
    paper = make_paper()
    prepare(paper)
    row = paper.data["strategies"]["tef"]
    row.update(quantity="1", entry_price="100", revision=1, last_changed_ms=0)
    row["pending"] = {
        "side": "BUY",
        "quantity": "0.5",
        "kind": "LIMIT",
        "price": "50",
        "reducing": False,
        "reason": "ENTRY",
        "eligible_at_ms": 0,
        "reserved": "25.025",
    }
    paper.command("pause", {})
    paper.command(
        "paper.step",
        {
            "instrument": INSTRUMENT.key,
            "candle": {
                "open_time_ms": 0,
                "open": "100",
                "high": "102",
                "low": "94",
                "close": "100",
                "volume": "10",
            },
        },
    )
    stop = paper.detail("tef")["pending"]
    assert stop["reducing"] and stop["side"] == "SELL" and stop["reason"] == "FSLIMMED"
    assert stop["quantity"] == "1"
    assert not paper.detail("tef")["fills"]  # No canceled entry is fabricated as a fill.


def test_recovery_refuses_a_checkpoint_skipped_by_manual_history_while_inactive():
    paper = make_paper()
    prepare(paper)
    for index in range(15):
        step(paper, index)
    paper.command("paper.order.cancel", {"instance": "tef"})
    restarted = make_paper(json.loads(json.dumps(paper.data)))
    step(restarted, 15)
    with pytest.raises(ValidationError, match="behind portfolio history"):
        restarted.command("paper.strategy.recover", {"instance": "tef"})
    assert restarted.detail("tef")["status"] == "recovery_required"
    assert restarted.data["paused"]
    assert (
        restarted.command("paper.strategy.recover", {"instance": "tef", "reset_model": True})[
            "restored_checkpoint"
        ]
        is False
    )
