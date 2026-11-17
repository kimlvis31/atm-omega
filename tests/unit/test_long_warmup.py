"""Bounded staged preparation; synthetic committed observations, no network or orders."""

import json
import threading
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal

import pytest

from omega.analysis.catalog import create_features
from omega.analysis.warmup import CHUNK_MINUTES, MAX_WARMUP, aligned_window
from omega.application.paper import PaperTradingService
from omega.application.paper_state import validate_state
from omega.domain.errors import CommandRejected, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.paper_feed import PaperFeedSupervisor
from omega.strategies.registry import StrategyRegistry
from omega.strategies.tef.module import TefModule

MINUTE = 60_000
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
ANALYSIS = {"mmacd_periods": [2, 4], "signal_period": 2}


def bar(minute, instrument=INSTRUMENT):
    price = Decimal(100) + Decimal(minute % 97) / 100
    return Candle(
        instrument,
        minute * MINUTE,
        (minute + 1) * MINUTE,
        price,
        price + 1,
        price - 1,
        price,
        Decimal(10),
    )


class Source:
    def __init__(self):
        self.calls = []
        self.omit = set()
        self.changed = {}

    def read(self, keys, start, end):
        self.calls.append((keys, start, end))
        _, market, symbol = keys[0].split(":")
        instrument = Instrument("binance", Market(market), symbol)
        return tuple(
            self.changed.get(index, bar(index, instrument))
            for index in range(start // MINUTE, end // MINUTE)
            if index not in self.omit
        )

    def close(self):
        pass


def make(*, count=1, model="MMACDDEFAULT", analysis=None, persist=lambda _s: None, state=None):
    paper = PaperTradingService(
        StrategyRegistry().create, create_features, persist=persist, state=state
    )
    if state is not None:
        return paper
    paper.command("paper.account.create", {"account_id": "a", "balance": "10000"})
    for index in range(count):
        instrument = INSTRUMENT if index == 0 else Instrument("binance", Market.USDM, "BTCUSDT")
        paper.command(
            "paper.strategy.add",
            {
                "instance": f"s{index}",
                "account_id": "a",
                "instrument": instrument.key,
                "module_id": "tef",
                "budget": "1000",
                "configuration": {"model": model},
                "analysis": analysis or ANALYSIS,
            },
        )
    return paper


def configure(paper, *, minutes=2001, end=6000):
    return paper.feed_configure(True, start_ms=end * MINUTE, warmup_minutes=minutes)


def run_chunks(paper, source, *, now=6001, limit=100):
    supervisor = PaperFeedSupervisor(paper, source, clock_ms=lambda: now * MINUTE)
    for _ in range(limit):
        if not paper.feed_snapshot()["warmup"]:
            break
        supervisor.run_once()
    return supervisor


def test_daily_ema20_prepares_in_small_chunks_with_exact_single_pass_parity():
    config = {
        "moving_averages": [
            {"interval_id": 11, "ema_period": 20},
            {"interval_id": 5, "mmacd_periods": [2, 4], "signal_period": 2},
        ]
    }
    paper = make(model="BSCALPHA", analysis=config)
    end = 31 * 1440 + 123
    configure(paper, minutes=20 * 1440, end=end)
    plan = paper.feed_snapshot()["warmup"]["s0"]
    assert plan["alignment_minutes"] == 1440
    assert plan["start_ms"] % (1440 * MINUTE) == 0
    assert plan["total_minutes"] == 20 * 1440 + 123
    before_accounts, before_model = (
        deepcopy(paper.data["accounts"]),
        paper.engines["s0"].module.checkpoint(),
    )
    source = Source()
    PaperFeedSupervisor(paper, source, clock_ms=lambda: (end + 1) * MINUTE).run_once()
    assert paper.engines["s0"].module.checkpoint() == before_model
    run_chunks(paper, source, now=end + 1)
    assert paper.data["feed"]["warmed"] == {"s0": True}
    assert max((high - low) // MINUTE for _, low, high in source.calls) <= CHUNK_MINUTES + 1
    assert paper.data["candles"] == {} and paper.data["accounts"] == before_accounts
    assert not paper.detail("s0")["fills"] and paper.detail("s0")["pending"] is None
    required = paper.engines["s0"].module.descriptor.required_fields
    reference = create_features(INSTRUMENT, required, config)
    reference_model = TefModule(INSTRUMENT, {"model": "BSCALPHA"})
    for minute in range(plan["start_ms"] // MINUTE, end):
        output = reference.update(bar(minute))
        reference_model.prime_history(bar(minute), output)
    assert all(output.get(field) is not None for field in required)
    assert paper.features["s0"].checkpoint() == reference.checkpoint()
    assert paper.engines["s0"].module.checkpoint() == reference_model.checkpoint()
    validate_state(paper.data)
    assert "checkpoint" not in json.dumps(paper.snapshot()["feed"])


def test_alignment_uses_common_utc_boundary_for_four_and_six_hour_bars():
    graph = create_features(
        INSTRUMENT,
        {"7_EMA_0_EMA", "8_EMA_0_EMA"},
        {
            "moving_averages": [
                {"interval_id": 7, "ema_period": 2},
                {"interval_id": 8, "ema_period": 2},
            ]
        },
    )
    assert graph.warmup_alignment_minutes == 720
    low, high = aligned_window(10_003 * MINUTE, 2001, graph.warmup_alignment_minutes)
    assert low % (720 * MINUTE) == 0 and high == 10_003 * MINUTE


@pytest.mark.parametrize("change", ["unpaused", "position", "pending"])
def test_long_prepare_is_rejected_before_mutation_unless_entire_portfolio_is_paused_flat(change):
    paper = make()
    row = paper.data["strategies"]["s0"]
    if change == "unpaused":
        paper.command("resume", {})
    elif change == "position":
        row.update(quantity="1", entry_price="100", status="quarantined")
    else:
        row["pending"] = {"already_submitted": True}
    previous = deepcopy(paper.data)
    with pytest.raises(CommandRejected):
        configure(paper)
    assert paper.data == previous


def test_partial_chunk_does_not_mutate_active_state_and_restart_requires_manual_recovery():
    paper = make()
    configure(paper)
    before_feature = paper.features["s0"].checkpoint()
    source, supervisor = (
        Source(),
        PaperFeedSupervisor(paper, Source(), clock_ms=lambda: 6001 * MINUTE),
    )
    supervisor.source = source
    supervisor.run_once()
    progress = paper.feed_snapshot()["warmup"]["s0"]
    assert progress["processed_minutes"] == CHUNK_MINUTES
    assert paper.features["s0"].checkpoint() == before_feature
    assert "s0" not in paper.data["feed"]["warmed"]
    saved = json.loads(json.dumps(paper.data))
    validate_state(saved)
    restarted = make(state=saved)
    next_supervisor = PaperFeedSupervisor(restarted, source, clock_ms=lambda: 6001 * MINUTE)
    calls = len(source.calls)
    next_supervisor.run_once()
    assert len(source.calls) == calls
    assert next_supervisor.status()["reason"] == "manual_recovery_required"
    restarted.command("paper.strategy.recover", {"instance": "s0"})
    assert "s0" not in restarted.data["feed"]["warmed"]
    next_supervisor.run_once()
    assert restarted.feed_snapshot()["warmup"]["s0"]["processed_minutes"] == 2 * CHUNK_MINUTES
    assert restarted.snapshot()["paused"] and restarted.data["candles"] == {}


def test_pause_continue_keeps_target_and_generation_but_new_apply_replaces_them():
    paper = make()
    configure(paper)
    PaperFeedSupervisor(paper, Source(), clock_ms=lambda: 6001 * MINUTE).run_once()
    before = paper.snapshot()["feed"]["warmup_plans"]["s0"]
    paper.feed_configure(False, start_ms=6200 * MINUTE, warmup_minutes=2001)
    result = paper.command("paper.feed.continue", {})
    assert isinstance(result["revision"], int)
    assert paper.snapshot()["feed"]["warmup_plans"]["s0"] == before
    assert paper.feed_snapshot()["enabled"]
    with pytest.raises(CommandRejected):
        paper.command("resume", {})
    configure(paper, end=6200)
    after = paper.snapshot()["feed"]["warmup_plans"]["s0"]
    assert after["plan_id"] != before["plan_id"] and after["target_end_ms"] == 6200 * MINUTE


def test_missing_minute_and_changed_prior_cursor_do_not_advance_the_staged_graph():
    paper, source = make(), Source()
    configure(paper)
    supervisor = PaperFeedSupervisor(paper, source, clock_ms=lambda: 6001 * MINUTE)
    start = paper.feed_snapshot()["warmup"]["s0"]["start_ms"] // MINUTE
    source.omit.add(start + 5)
    before = deepcopy(paper.data["feed"]["warmup_plans"])
    supervisor.run_once()
    assert paper.data["feed"]["warmup_plans"] == before
    source.omit.clear()
    supervisor.run_once()
    before = deepcopy(paper.data["feed"]["warmup_plans"])
    last = paper.feed_snapshot()["warmup"]["s0"]["next_open_ms"] // MINUTE - 1
    source.omit.add(last)
    supervisor.run_once()
    assert supervisor.status()["reason"] == "warmup_cursor_conflict_or_missing"
    assert paper.data["feed"]["warmup_plans"] == before


def test_storage_failure_rolls_back_chunk_cursor_and_its_feature_checkpoint():
    fail = [False]

    def persist(_state):
        if fail[0]:
            raise OSError("disk unavailable")

    paper = make(persist=persist)
    configure(paper)
    before = deepcopy(paper.data["feed"]["warmup_plans"])
    fail[0] = True
    PaperFeedSupervisor(paper, Source(), clock_ms=lambda: 6001 * MINUTE).run_once()
    assert paper.data["feed"]["warmup_plans"] == before
    assert paper.recording_health()["manual_resume_required"]


def test_old_chunk_is_rejected_after_pause_or_new_configuration_and_after_owner_retirement():
    paper = make()
    configure(paper)
    expected = paper.feed_snapshot()
    item = expected["warmup"]["s0"]
    rows = [
        bar(index)
        for index in range(item["start_ms"] // MINUTE, item["start_ms"] // MINUTE + CHUNK_MINUTES)
    ]
    paper.command("pause", {})
    with pytest.raises(ValidationError, match="plan changed"):
        paper.feed_warmup_chunk("s0", rows, expected=expected, now_ms=6001 * MINUTE)
    fresh = paper.feed_snapshot()
    paper.retire_feed_owner()
    with pytest.raises(ValidationError, match="retired"):
        paper.feed_warmup_chunk("s0", rows, expected=fresh, now_ms=6001 * MINUTE)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.update(next_open_ms=p["next_open_ms"] + 1),
        lambda p: p.update(next_open_ms=p["target_end_ms"] + MINUTE),
        lambda p: p.update(version=True),
        lambda p: p.update(state="complete"),
        lambda p: p.update(checkpoint=[]),
        lambda p: p.update(alignment_minutes=0),
    ],
)
def test_malformed_staged_progress_fails_before_runtime_reconstruction(mutate):
    paper = make()
    configure(paper)
    state = deepcopy(paper.data)
    mutate(state["feed"]["warmup_plans"]["s0"])
    with pytest.raises(ValidationError):
        make(state=state)


def test_chunk_commit_rechecks_all_positions_and_pending_orders_including_quarantine():
    paper = make()
    configure(paper)
    expected = paper.feed_snapshot()
    item = expected["warmup"]["s0"]
    rows = [
        bar(index)
        for index in range(item["start_ms"] // MINUTE, item["start_ms"] // MINUTE + CHUNK_MINUTES)
    ]
    paper.data["strategies"]["s0"].update(quantity="1", entry_price="100", status="quarantined")
    # Re-fetching a current plan cannot make an open position safe to prepare.
    current = paper.feed_snapshot()
    with pytest.raises(CommandRejected):
        paper.feed_warmup_chunk("s0", rows, expected=current, now_ms=6001 * MINUTE)
    assert paper.data["feed"]["warmup_plans"]["s0"]["next_open_ms"] == item["start_ms"]


def test_plan_horizon_is_bounded_after_alignment_and_before_epoch():
    for end, minutes, alignment in [(100, 2001, 1), (MAX_WARMUP + 1, MAX_WARMUP, 1440)]:
        with pytest.raises(ValidationError):
            aligned_window(end * MINUTE, minutes, alignment)


def test_unconfirmed_swing_stays_not_ready_without_promoting_or_repeating_history():
    paper = make(
        model="SPDDEFAULT",
        analysis={"swing": [{"interval_id": 0, "line_index": 0, "swing_range": 1.0}]},
    )
    configure(paper)
    active = paper.features["s0"].checkpoint()
    source = Source()
    supervisor = run_chunks(paper, source, limit=5)
    assert paper.data["feed"]["warmup_plans"]["s0"]["state"] == "not_ready"
    assert not paper.data["feed"]["warmed"]
    assert paper.features["s0"].checkpoint() == active
    before = len(source.calls)
    supervisor.run_once()
    assert len(source.calls) == before
    assert supervisor.status()["reason"] == "warmup_features_unavailable"
    with pytest.raises(CommandRejected):
        paper.command("resume", {})


def test_lower_progress_strategy_gets_the_next_chunk_instead_of_one_owner_monopolizing():
    paper = make(count=2)
    configure(paper)
    source = Source()
    supervisor = PaperFeedSupervisor(paper, source, clock_ms=lambda: 6001 * MINUTE)
    supervisor.run_once()
    supervisor.run_once()
    plans = paper.snapshot()["feed"]["warmup_plans"]
    assert plans["s0"]["processed_minutes"] == plans["s1"]["processed_minutes"] == CHUNK_MINUTES
    assert source.calls[0][0] != source.calls[1][0]


def test_completed_prepare_then_catchup_creates_no_historical_orders_and_keeps_user_pause():
    paper, source = make(), Source()
    configure(paper)
    supervisor = run_chunks(paper, source)
    assert not paper.feed_snapshot()["long_warmup_preparing"]
    supervisor.run_once()
    assert paper.data["candles"][INSTRUMENT.key][-1]["open_time_ms"] == 6000 * MINUTE
    assert paper.detail("s0")["last_candle"] == 6000 * MINUTE
    assert not paper.detail("s0")["pending"] and not paper.detail("s0")["fills"]
    assert paper.snapshot()["paused"]
    paper.command("resume", {})
    assert not paper.snapshot()["paused"]


def test_completed_long_checkpoint_recovery_keeps_warmed_state_and_no_new_preparation():
    paper = make()
    configure(paper)
    run_chunks(paper, Source())
    original = paper.features["s0"].checkpoint()
    restarted = make(state=json.loads(json.dumps(paper.data)))
    assert not restarted.data["feed"]["warmed"]
    restarted.command("paper.strategy.recover", {"instance": "s0"})
    assert restarted.data["feed"]["warmed"] == {"s0": True}
    assert restarted.features["s0"].checkpoint() == original


def test_repeated_chunk_cannot_advance_the_same_analysis_state_twice():
    paper = make()
    configure(paper)
    expected = paper.feed_snapshot()
    item = expected["warmup"]["s0"]
    rows = [
        bar(index)
        for index in range(item["start_ms"] // MINUTE, item["start_ms"] // MINUTE + CHUNK_MINUTES)
    ]
    paper.feed_warmup_chunk("s0", rows, expected=expected, now_ms=6001 * MINUTE)
    before = deepcopy(paper.data["feed"]["warmup_plans"])
    with pytest.raises(ValidationError, match="plan changed"):
        paper.feed_warmup_chunk("s0", rows, expected=expected, now_ms=6001 * MINUTE)
    assert paper.data["feed"]["warmup_plans"] == before


@pytest.mark.parametrize(
    "kind", ["missing", "duplicate", "wrong_instrument", "oversized", "future"]
)
def test_bad_chunk_never_partially_changes_graph_or_progress(kind):
    paper = make()
    configure(paper)
    expected = paper.feed_snapshot()
    item = expected["warmup"]["s0"]
    rows = [
        bar(index)
        for index in range(item["start_ms"] // MINUTE, item["start_ms"] // MINUTE + CHUNK_MINUTES)
    ]
    now = 6001 * MINUTE
    if kind == "missing":
        rows.pop(5)
    elif kind == "duplicate":
        rows[5] = rows[4]
    elif kind == "wrong_instrument":
        rows[5] = replace(rows[5], instrument=Instrument("binance", Market.USDM, "BTCUSDT"))
    elif kind == "oversized":
        rows.append(rows[-1])
    else:
        now = rows[5].open_time_ms
    before = deepcopy(paper.data["feed"]["warmup_plans"])
    active = paper.features["s0"].checkpoint()
    with pytest.raises(ValidationError):
        paper.feed_warmup_chunk("s0", rows, expected=expected, now_ms=now)
    assert paper.data["feed"]["warmup_plans"] == before
    assert paper.features["s0"].checkpoint() == active


@pytest.mark.parametrize("change", ["new_plan", "stop"])
def test_late_read_cannot_write_a_new_plan_or_a_retired_owner(change):
    paper = make()
    configure(paper)
    entered, release = threading.Event(), threading.Event()

    class BlockingSource(Source):
        def read(self, *args):
            entered.set()
            assert release.wait(3)
            return super().read(*args)

    supervisor = PaperFeedSupervisor(paper, BlockingSource(), clock_ms=lambda: 6300 * MINUTE)
    thread = threading.Thread(target=supervisor.run_once)
    thread.start()
    assert entered.wait(1)
    try:
        if change == "new_plan":
            configure(paper, end=6200)
        else:
            paper.retire_feed_owner()
            supervisor.stop.set()
        before = deepcopy(paper.data["feed"]["warmup_plans"])
    finally:
        release.set()
        thread.join(3)
    assert not thread.is_alive()
    assert paper.data["feed"]["warmup_plans"] == before
    assert all(plan["next_open_ms"] == plan["start_ms"] for plan in before.values())


def test_staged_checkpoint_checksum_mismatch_blocks_manual_recovery_and_preserves_pause():
    paper = make()
    configure(paper)
    PaperFeedSupervisor(paper, Source(), clock_ms=lambda: 6001 * MINUTE).run_once()
    state = deepcopy(paper.data)
    state["feed"]["warmup_plans"]["s0"]["checkpoint"]["sha256"] = "0" * 64
    restarted = make(state=state)
    with pytest.raises(ValidationError):
        restarted.command("paper.strategy.recover", {"instance": "s0"})
    assert restarted.detail("s0")["status"] == "recovery_required"
    assert restarted.snapshot()["paused"] and not restarted.data["feed"]["warmed"]


def test_local_clock_reversal_during_a_warmup_read_cannot_commit_a_chunk():
    paper, now = make(), [6001 * MINUTE]
    configure(paper)
    before = deepcopy(paper.data["feed"]["warmup_plans"])

    class ReversedClock(Source):
        def read(self, *args):
            rows = super().read(*args)
            now[0] -= 1
            return rows

    supervisor = PaperFeedSupervisor(paper, ReversedClock(), clock_ms=lambda: now[0])
    supervisor.run_once()
    assert supervisor.status()["reason"] == "clock_reversed_during_warmup_read"
    assert paper.data["feed"]["warmup_plans"] == before


def test_authenticated_core_commands_expose_progress_only_and_definitively_reject_early_resume(
    tmp_path,
):
    from uuid import uuid4

    from omega.runtime.service import CoreService

    paper = make()
    core = CoreService(
        tmp_path,
        paper_provider=paper,
        paper_feed=PaperFeedSupervisor(paper, Source(), clock_ms=lambda: 6001 * MINUTE),
    )
    try:
        response = core.request(
            "POST",
            "/api/commands",
            core.token,
            {
                "id": uuid4().hex,
                "kind": "paper.feed.configure",
                "payload": {
                    "enabled": True,
                    "start_ms": 6000 * MINUTE,
                    "warmup_minutes": 2001,
                },
            },
        )
        assert response["status"] == "completed"
        state = core.request("GET", "/api/state", core.token)
        assert state["feed"]["warmup_plans"]["s0"]["state"] == "warming"
        assert "checkpoint" not in json.dumps(state["feed"])
        control = state["core"]
        rejected = core.request(
            "POST",
            "/api/commands",
            core.token,
            {
                "id": uuid4().hex,
                "kind": "resume",
                "payload": {
                    "expected_epoch": control["epoch"],
                    "expected_revision": control["control_revision"],
                    "expires_at_ms": control["server_time_ms"] + 20_000,
                },
            },
        )
        assert rejected["status"] == "rejected"
        assert paper.snapshot()["paused"] and core.control.snapshot()["paused"]
    finally:
        core.close()
