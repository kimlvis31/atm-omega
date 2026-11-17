"""Fresh TS calculation priming is atomic with analysis and never executes history."""

import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService, candle_row
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.ports.modules import ModuleDescriptor
from omega.runtime.paper_feed import PaperFeedSupervisor
from omega.strategies.registry import StrategyRegistry
from omega.strategies.target_quantity.module import TargetQuantityModule
from omega.strategies.tef.module import TefModule

MINUTE = 60_000
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
ANALYSIS = {"mmacd_periods": [2, 4], "signal_period": 2}
TRAJECTORIES = json.loads(Path("tests/fixtures/tef-legacy.json").read_text())
TRACKERS = json.loads(Path("tests/fixtures/tef-priming-trackers.json").read_text())


def bar(minute, price=None):
    price = Decimal(price) if price is not None else Decimal(100) + Decimal(minute % 17) / 10
    return Candle(
        INSTRUMENT,
        minute * MINUTE,
        (minute + 1) * MINUTE,
        price,
        price + 1,
        price - 1,
        price,
        Decimal(10),
    )


def make(
    *,
    model="MMACDDEFAULT",
    analysis=None,
    feature_factory=create_features,
    state=None,
    persist=lambda _s: None,
):
    paper = PaperTradingService(
        StrategyRegistry().create, feature_factory, state=state, persist=persist
    )
    if state is None:
        paper.command("paper.account.create", {"account_id": "a", "balance": "10000"})
        paper.command(
            "paper.strategy.add",
            {
                "instance": "s",
                "account_id": "a",
                "instrument": INSTRUMENT.key,
                "module_id": "tef",
                "budget": "1000",
                "configuration": {"model": model, "fullStopLossImmediate": "0.05"},
                "analysis": analysis or ANALYSIS,
            },
        )
    return paper


class Source:
    def read(self, keys, low, high):
        assert keys == [INSTRUMENT.key]
        return tuple(bar(index) for index in range(low // MINUTE, high // MINUTE))

    def close(self):
        pass


def prepare_long(paper, *, minutes=2001, end=6000, ticks=10):
    paper.feed_configure(True, start_ms=end * MINUTE, warmup_minutes=minutes)
    feed = PaperFeedSupervisor(paper, Source(), clock_ms=lambda: (end + 1) * MINUTE)
    for _ in range(ticks):
        feed.run_once()
        if not paper.feed_snapshot()["warmup"]:
            break
    return feed


def test_short_warmup_commits_fresh_model_and_graph_with_identical_last_minute_without_orders():
    paper = make()
    paper.feed_configure(True, start_ms=100 * MINUTE, warmup_minutes=12)
    rows = tuple(bar(index) for index in range(88, 100))
    before = deepcopy(paper.data["accounts"])
    paper.feed_warmup({"s": rows}, expected=paper.feed_snapshot(), now_ms=101 * MINUTE)
    module = paper.engines["s"].module
    assert module.history_priming_ready and module.last_open_time_ms == 99 * MINUTE
    assert paper.features["s"].last_open_time_ms == module.last_open_time_ms
    assert paper.data["accounts"] == before and paper.data["candles"] == {}
    assert paper.detail("s")["pending"] is None and not paper.detail("s")["fills"]


def test_public_history_readiness_is_optional_and_does_not_expose_model_checkpoints():
    paper = make()
    assert paper.detail("s")["model_history_ready"] is False
    assert "model_history_unready" in paper.detail("s")["model_history_warning"]
    assert paper.snapshot()["strategies"][0]["model_history_ready"] is False
    prepare_long(paper)
    detail = paper.detail("s")
    assert detail["model_history_ready"] is True and detail["model_history_warning"] is None
    assert "runtime_checkpoint" not in detail and "module_checkpoint" not in detail
    restarted = make(state=deepcopy(paper.data))
    assert restarted.detail("s")["model_history_ready"] is None
    assert restarted.snapshot()["strategies"][0]["model_history_ready"] is None


@pytest.mark.parametrize("reference", TRACKERS, ids=lambda row: row["model"])
def test_short_priming_all_six_models_matches_independently_extracted_eta_tracker(reference):
    trajectory = next(row for row in TRAJECTORIES if row["model"] == reference["model"])

    class FrozenFeatures:
        def update(self, candle):
            return dict(trajectory["steps"][candle.open_time_ms // MINUTE]["features"])

    paper = make(model=reference["model"], feature_factory=lambda *_: FrozenFeatures())
    paper.feed_configure(True, start_ms=240 * MINUTE, warmup_minutes=240)
    rows = tuple(
        bar(index, str(step["features"]["KLINE_CLOSEPRICE"]))
        for index, step in enumerate(trajectory["steps"])
    )
    paper.feed_warmup({"s": rows}, expected=paper.feed_snapshot(), now_ms=241 * MINUTE)
    module = paper.engines["s"].module
    assert module.snapshot()["model_state"] == reference["tracker"]
    assert module.history_priming_ready and module.last_open_time_ms == 239 * MINUTE
    assert paper.data["candles"] == {} and not paper.detail("s")["fills"]
    assert module.snapshot()["pending_stop"] is None and not module.snapshot()["halted"]


PATH_CASES = next(row for row in TRACKERS if row["model"] == "PATHFINDER")["partial_cases"]


@pytest.mark.parametrize("case", PATH_CASES, ids=lambda row: row["name"])
@pytest.mark.parametrize("split", [1, 2, 3])
def test_path_partial_closed_inputs_and_checkpoint_match_original_independent_caches(case, split):
    module = TefModule(INSTRUMENT, {"model": "PATHFINDER"})
    for index, expected in enumerate(case["steps"]):
        if index == split:
            state = json.loads(json.dumps(module.checkpoint()))
            module = TefModule(INSTRUMENT, {"model": "PATHFINDER"})
            module.restore(state)
        module.prime_history(
            bar(index, str(expected["features"]["KLINE_CLOSEPRICE"])), expected["features"]
        )
        assert module.snapshot()["model_state"] == expected["tracker"]
        assert module.history_priming_ready == expected["ready"]
        assert not module.snapshot()["halted"] and module.snapshot()["pending_stop"] is None


def test_feature_preview_without_a_confirmed_model_anchor_does_not_complete_preparation():
    config = {
        "moving_averages": [
            {"interval_id": 11, "ema_period": 2},
            {"interval_id": 5, "mmacd_periods": [2, 4], "signal_period": 2},
        ]
    }
    paper = make(model="BSCALPHA", analysis=config)
    before = paper.engines["s"].module.checkpoint()
    prepare_long(paper, end=4 * 1440 + 720)
    plan = paper.data["feed"]["warmup_plans"]["s"]
    assert plan["state"] == "not_ready" and plan["priming_mode"] == "model"
    candidate = TefModule(INSTRUMENT, paper.data["strategies"]["s"]["configuration"])
    candidate.restore(plan["module_checkpoint"])
    assert not candidate.history_priming_ready
    assert paper.engines["s"].module.checkpoint() == before
    assert not paper.data["feed"]["warmed"] and not paper.data["candles"]


def test_partial_staged_model_restores_every_prefix_and_commits_only_at_completion():
    paper = make()
    paper.feed_configure(True, start_ms=6000 * MINUTE, warmup_minutes=2001)
    active = paper.engines["s"].module.checkpoint()
    feed = PaperFeedSupervisor(paper, Source(), clock_ms=lambda: 6001 * MINUTE)
    feed.run_once()
    stage = paper.data["feed"]["warmup_plans"]["s"]
    assert stage["version"] == 2 and stage["priming_mode"] == "model"
    assert stage["module_checkpoint"] and stage["checkpoint"]
    assert paper.engines["s"].module.checkpoint() == active
    restarted = make(state=json.loads(json.dumps(paper.data)))
    restarted.command("paper.strategy.recover", {"instance": "s"})
    assert not restarted.data["feed"]["warmed"]
    resumed = PaperFeedSupervisor(restarted, Source(), clock_ms=lambda: 6001 * MINUTE)
    while restarted.feed_snapshot()["warmup"]:
        resumed.run_once()
    uninterrupted = make()
    prepare_long(uninterrupted)
    assert (
        restarted.engines["s"].module.checkpoint() == uninterrupted.engines["s"].module.checkpoint()
    )
    assert restarted.features["s"].checkpoint() == uninterrupted.features["s"].checkpoint()
    assert restarted.engines["s"].module.history_priming_ready
    assert restarted.data["feed"]["warmup_plans"]["s"]["module_checkpoint"] is None


def test_old_feature_only_partial_plan_requires_a_new_plan_instead_of_tail_only_priming():
    paper = make()
    prepare_long(paper, ticks=1)
    state = deepcopy(paper.data)
    stage = state["feed"]["warmup_plans"]["s"]
    stage["version"] = 1
    stage.pop("priming_mode")
    stage.pop("module_checkpoint")
    restarted = make(state=state)
    with pytest.raises(ValidationError, match="no model history"):
        restarted.command("paper.strategy.recover", {"instance": "s"})
    assert restarted.snapshot()["paused"] and not restarted.data["feed"]["warmed"]


def test_staged_module_cursor_must_match_its_graph_checkpoint_cursor():
    paper = make()
    prepare_long(paper, ticks=1)
    state = deepcopy(paper.data)
    state["feed"]["warmup_plans"]["s"]["module_checkpoint"] = paper.engines["s"].module.checkpoint()
    restarted = make(state=state)
    with pytest.raises(ValidationError, match="model checkpoint"):
        restarted.command("paper.strategy.recover", {"instance": "s"})
    assert not restarted.data["feed"]["warmed"]


def test_existing_live_model_and_confirmed_stop_latch_are_preserved_by_short_feature_replay():
    paper = make()
    paper.command("resume", {})
    rows = []
    for index in range(20):
        candle = bar(10_000 + index, str(100 + index * index / 10))
        rows.append(candle)
        paper.command("paper.step", {"instrument": INSTRUMENT.key, "candle": candle_row(candle)})
    assert Decimal(paper.detail("s")["quantity"]) > 0
    paper.command("pause", {})
    paper.command("paper.order.cancel", {"instance": "s"})
    for index in (20, 21):
        candle = bar(10_000 + index, "70")
        rows.append(candle)
        paper.command("paper.step", {"instrument": INSTRUMENT.key, "candle": candle_row(candle)})
    module = paper.engines["s"].module
    assert module.snapshot()["halted"] and Decimal(paper.detail("s")["quantity"]) == 0
    before = module.checkpoint()
    paper.feed_configure(True, start_ms=10_022 * MINUTE, warmup_minutes=22)
    paper.feed_warmup({"s": tuple(rows)}, expected=paper.feed_snapshot(), now_ms=10_023 * MINUTE)
    assert paper.engines["s"].module.checkpoint() == before
    assert paper.engines["s"].module.snapshot()["halted"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda stage: stage.update(module_checkpoint=None),
        lambda stage: stage.update(module_checkpoint=[]),
        lambda stage: stage.update(priming_mode="invalid"),
        lambda stage: stage.update(priming_mode=[]),
    ],
)
def test_invalid_staged_model_metadata_is_rejected_before_recovery(mutate):
    paper = make()
    prepare_long(paper, ticks=1)
    state = deepcopy(paper.data)
    mutate(state["feed"]["warmup_plans"]["s"])
    with pytest.raises(ValidationError):
        make(state=state)


def test_recording_failure_rolls_back_staged_model_graph_and_cursor_together():
    fail = [False]

    def persist(_state):
        if fail[0]:
            raise OSError("recording unavailable")

    paper = make(persist=persist)
    paper.feed_configure(True, start_ms=6000 * MINUTE, warmup_minutes=2001)
    before = deepcopy(paper.data["feed"]["warmup_plans"])
    fail[0] = True
    PaperFeedSupervisor(paper, Source(), clock_ms=lambda: 6001 * MINUTE).run_once()
    assert paper.data["feed"]["warmup_plans"] == before
    assert paper.recording_health()["manual_resume_required"]


def test_new_ts_without_optional_history_hooks_is_explicitly_feature_only():
    class NoHook(TargetQuantityModule):
        descriptor = ModuleDescriptor("no_hook", "1", frozenset({"0_MMACD_MSDELTAABSMAREL"}), True)

        def __init__(self, *args):
            super().__init__(*args)
            self.evaluations = 0

        def evaluate(self, context):
            self.evaluations += 1
            return super().evaluate(context)

    paper = PaperTradingService(
        lambda _id, instrument, config: NoHook(instrument, config), create_features
    )
    paper.command("paper.account.create", {"account_id": "a"})
    paper.command(
        "paper.strategy.add",
        {
            "instance": "s",
            "account_id": "a",
            "instrument": INSTRUMENT.key,
            "module_id": "no_hook",
            "budget": "1000",
            "configuration": {"quantity": "1"},
            "analysis": ANALYSIS,
        },
    )
    prepare_long(paper)
    plan = paper.snapshot()["feed"]["warmup_plans"]["s"]
    assert plan["state"] == "complete" and plan["priming_mode"] == "features_only"
    assert paper.detail("s")["history_preparation"] == "features_only"
    assert paper.engines["s"].module.evaluations == 0
    assert paper.detail("s")["model_history_ready"] is None


def test_existing_model_has_no_historical_reapplication_during_long_preparation():
    paper = make()
    paper.feed_configure(True, start_ms=6000 * MINUTE, warmup_minutes=12)
    paper.feed_warmup(
        {"s": tuple(bar(index) for index in range(5988, 6000))},
        expected=paper.feed_snapshot(),
        now_ms=6001 * MINUTE,
    )
    before = paper.engines["s"].module.checkpoint()
    prepare_long(paper)
    assert paper.engines["s"].module.checkpoint() == before
    assert paper.snapshot()["feed"]["warmup_plans"]["s"]["priming_mode"] == "preserve"
    assert paper.detail("s")["history_preparation"] == "preserve"


def test_active_model_changed_while_staged_job_was_disabled_cannot_be_overwritten():
    paper = make()
    prepare_long(paper, ticks=1)
    paper.feed_configure(False, start_ms=6000 * MINUTE, warmup_minutes=2001)
    paper.command("paper.step", {"instrument": INSTRUMENT.key, "candle": candle_row(bar(6000))})
    active = paper.engines["s"].module.checkpoint()
    stage = deepcopy(paper.data["feed"]["warmup_plans"]["s"])
    paper.command("paper.feed.continue", {})
    PaperFeedSupervisor(paper, Source(), clock_ms=lambda: 6002 * MINUTE).run_once()
    assert paper.engines["s"].module.checkpoint() == active
    assert paper.data["feed"]["warmup_plans"]["s"] == stage
    assert paper.snapshot()["paused"]
