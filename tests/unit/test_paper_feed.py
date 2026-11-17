"""Committed source fakes drive the real paper transaction owner; no network/DB."""

import threading
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService, candle_row
from omega.application.paper_state import validate_state
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.ports.modules import ModuleDescriptor
from omega.runtime.paper_feed import PaperFeedSupervisor
from omega.strategies.registry import StrategyRegistry
from omega.strategies.target_quantity.module import TargetQuantityModule

MINUTE = 60000
BTC = Instrument("binance", Market.SPOT, "BTCUSDT")
ETH = Instrument("binance", Market.SPOT, "ETHUSDT")


def bar(minute, instrument=BTC, price="100"):
    value = Decimal(price)
    return Candle(
        instrument,
        minute * MINUTE,
        (minute + 1) * MINUTE,
        value,
        value + 1,
        value - 1,
        value,
        Decimal(10),
    )


class Source:
    def __init__(self, rows=()):
        self.rows, self.calls, self.closed = list(rows), [], False
        self.fail = False

    def read(self, keys, start, end, *, limit=4096):
        self.calls.append((keys, start, end))
        if self.fail:
            raise RuntimeError("private connection details")
        return tuple(
            row
            for row in self.rows
            if row.instrument.key in keys and start <= row.open_time_ms < end
        )

    def close(self):
        self.closed = True


def portfolio(instruments=(BTC,), persist=lambda _state: None):
    paper = PaperTradingService(StrategyRegistry().create, create_features, persist=persist)
    paper.command("paper.account.create", {"account_id": "a", "balance": "10000"})
    for index, instrument in enumerate(instruments):
        add(paper, f"s{index}", instrument)
    return paper


def add(paper, name, instrument=BTC):
    paper.command(
        "paper.strategy.add",
        {
            "account_id": "a",
            "instance": name,
            "instrument": instrument.key,
            "module_id": "target_quantity",
            "configuration": {"quantity": "1"},
            "budget": "1000",
        },
    )


def enabled(paper, source, now, *, start=10, batch_frames=16):
    paper.feed_configure(True, start_ms=start * MINUTE, warmup_minutes=2)
    feed = PaperFeedSupervisor(paper, source, clock_ms=lambda: now[0], batch_frames=batch_frames)
    for _ in paper.data["strategies"]:
        if not paper.feed_snapshot()["warmup"]:
            break  # Empty-feature preparation is committed together without candle IO.
        feed.run_once()  # Empty-feature modules still get an explicit non-trading preparation.
    return feed


def test_disabled_feed_never_opens_source_and_stops_promptly():
    paper, source = portfolio(), Source()
    feed = PaperFeedSupervisor(paper, source)
    feed.run_once()
    assert not source.calls and feed.status()["state"] == "disabled"
    feed.start()
    feed.close()
    assert source.closed and not feed.thread.is_alive()


def test_continuous_committed_feed_next_bar_fill_and_user_pause_survive_gap_recovery():
    saved = []
    paper, source, now = (
        portfolio(persist=lambda state: saved.append(state)),
        Source([bar(10)]),
        [11 * MINUTE],
    )
    feed = enabled(paper, source, now)
    paper.command("resume", {})
    feed.run_once()
    assert paper.detail("s0")["pending"] is not None
    assert paper.detail("s0")["quantity"] == "0"
    paper.command("pause", {})
    now[0] = 12 * MINUTE
    feed.run_once()
    assert feed.status()["reason"] == "missing_minute"
    assert paper.data["paused"] is True
    source.rows.append(bar(11, price="110"))
    feed.run_once()
    assert paper.data["feed"]["ready"] is True and paper.data["paused"] is True
    assert paper.detail("s0")["quantity"] == "1"
    assert paper.detail("s0")["entry_price"] == "110"
    assert len(paper.detail("s0")["fills"]) == 1
    feed.run_once()
    assert len(paper.detail("s0")["fills"]) == 1
    assert saved[-1]["candles"][BTC.key][-1]["open_time_ms"] == 11 * MINUTE
    assert saved[-1]["feed"]["provenance"][BTC.key] == "database_committed"
    validate_state(saved[-1])


def test_timestamp_barrier_blocks_later_symbol_and_catches_up_without_historical_entries():
    paper, now = portfolio((BTC, ETH)), [13 * MINUTE]
    source = Source([bar(10), bar(11), bar(12), bar(10, ETH), bar(12, ETH)])
    feed = enabled(paper, source, now)
    paper.command("resume", {})
    feed.run_once()
    assert {key: rows[-1]["open_time_ms"] for key, rows in paper.data["candles"].items()} == {
        BTC.key: 10 * MINUTE,
        ETH.key: 10 * MINUTE,
    }
    assert all(row["pending"] is None for row in paper.data["strategies"].values())
    assert feed.status()["reason"] == "missing_minute"
    source.rows.append(bar(11, ETH))
    feed.run_once()
    assert all(rows[-1]["open_time_ms"] == 12 * MINUTE for rows in paper.data["candles"].values())
    assert all(row["pending"] for row in paper.data["strategies"].values())


def test_failed_frame_commit_rolls_back_every_symbol_cursor_and_account_effect():
    fail = [False]

    def persist(_state):
        if fail[0]:
            raise OSError("full")

    paper = portfolio((BTC, ETH), persist=persist)
    feed = enabled(paper, Source([bar(10), bar(10, ETH)]), [11 * MINUTE])
    paper.command("resume", {})
    before = deepcopy(paper.data)
    fail[0] = True
    feed.run_once()
    assert paper.data["candles"] == before["candles"] == {}
    assert paper.data["accounts"] == before["accounts"]
    assert all(row["pending"] is None for row in paper.data["strategies"].values())
    assert paper.data["feed"]["ready"] is False


def test_restart_requires_manual_recovery_and_does_not_skip_durable_cursor():
    paper, now, source = portfolio(), [11 * MINUTE], Source([bar(10), bar(11)])
    feed = enabled(paper, source, now)
    feed.run_once()
    restored = PaperTradingService(StrategyRegistry().create, create_features, state=paper.data)
    assert restored.feed_snapshot()["epoch"] != paper.feed_snapshot()["epoch"]
    new_feed = PaperFeedSupervisor(restored, source, clock_ms=lambda: 12 * MINUTE)
    count = len(source.calls)
    new_feed.run_once()
    assert len(source.calls) == count
    assert new_feed.status()["reason"] == "manual_recovery_required"
    assert restored.data["paused"] is True
    restored.command("paper.strategy.recover", {"instance": "s0"})
    new_feed.run_once()
    new_feed.run_once()
    assert restored.data["candles"][BTC.key][-1]["open_time_ms"] == 11 * MINUTE
    assert restored.data["paused"] is True


def test_http_control_revision_change_rejects_old_frame_without_unpausing():
    paper = portfolio()
    enabled(paper, Source(), [11 * MINUTE])
    expected = paper.feed_snapshot()
    paper.command("pause", {})
    with pytest.raises(ValidationError, match="plan changed"):
        paper.feed_apply(
            (bar(10),), expected=expected, entries_allowed=True, reason="ready", now_ms=11 * MINUTE
        )
    assert paper.data["paused"] is True and not paper.data["candles"]


def test_conflicting_committed_tail_blocks_new_minute_and_never_overwrites():
    paper, source, now = portfolio(), Source([bar(10)]), [11 * MINUTE]
    feed = enabled(paper, source, now)
    feed.run_once()
    source.rows[:] = [bar(10, price="200"), bar(11)]
    now[0] = 12 * MINUTE
    feed.run_once()
    assert feed.status()["reason"] == "committed_cursor_conflict_or_missing"
    assert paper.data["candles"][BTC.key] == [candle_row(bar(10))]


def test_source_failure_blocks_entries_without_setting_user_pause_or_leaking_details():
    paper, source, now = portfolio(), Source(), [11 * MINUTE]
    feed = enabled(paper, source, now)
    paper.command("resume", {})
    source.fail = True
    feed.run_once()
    assert paper.data["paused"] is False and paper.data["feed"]["ready"] is False
    assert feed.status()["reason"] == "RuntimeError"
    source.fail = False
    source.rows.append(bar(10))
    feed.run_once()
    assert paper.data["feed"]["ready"] is True


def test_manual_input_rejected_while_feed_owns_supply_and_analysis_snapshot_preserved():
    paper = portfolio()
    paper.feed_configure(True, start_ms=10 * MINUTE)
    with pytest.raises(ValidationError, match="manual candle"):
        paper.command("paper.step", {"instrument": BTC.key, "candle": candle_row(bar(10))})
    paper.feed_configure(False, start_ms=10 * MINUTE)
    paper.command("paper.step", {"instrument": BTC.key, "candle": candle_row(bar(10))})
    assert paper.data["feed"]["provenance"].get(BTC.key, "manual_paper") == "manual_paper"


def test_expected_recheck_serializes_source_read_against_http_pause():
    paper, source, now = portfolio(), Source([bar(10)]), [11 * MINUTE]
    feed = enabled(paper, source, now)
    entered, release = threading.Event(), threading.Event()
    original = source.read

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(2)
        return original(*args, **kwargs)

    source.read = blocked
    worker = threading.Thread(target=feed.run_once)
    worker.start()
    assert entered.wait(1)
    paper.command("pause", {})
    release.set()
    worker.join(2)
    assert not worker.is_alive() and not paper.data["candles"]
    assert paper.data["paused"] is True


class CountFeatures:
    def __init__(self):
        self.seen = []

    def update(self, candle):
        self.seen.append(candle.open_time_ms)
        return {"count": len(self.seen) if len(self.seen) >= 2 else None}


def analyzed_portfolio():
    def factory(_name, instrument, configuration):
        module = TargetQuantityModule(instrument, configuration)
        module.descriptor = ModuleDescriptor("test_analysis", "1", frozenset({"count"}), True)
        return module

    paper = PaperTradingService(factory, lambda *_: CountFeatures())
    paper.command("paper.account.create", {"account_id": "a"})
    add(paper, "s0")
    return paper


def test_warmup_updates_analysis_without_pending_fills_orders_or_delivery_cursors():
    paper = analyzed_portfolio()
    paper.feed_configure(True, start_ms=10 * MINUTE, warmup_minutes=2)
    source = Source([bar(8), bar(9), bar(10)])
    feed = PaperFeedSupervisor(paper, source, clock_ms=lambda: 11 * MINUTE)
    paper.command("resume", {})
    before = deepcopy(paper.data["accounts"])
    feed.run_once()
    assert not paper.data["candles"]
    assert paper.data["accounts"] == before
    assert paper.detail("s0")["pending"] is None and paper.detail("s0")["fills"] == []
    assert paper.features["s0"].seen == [8 * MINUTE, 9 * MINUTE]
    feed.run_once()
    assert paper.features["s0"].seen == [8 * MINUTE, 9 * MINUTE, 10 * MINUTE]
    assert paper.detail("s0")["pending"] is not None


def test_incomplete_warmup_blocks_until_actual_missing_observation_is_restored():
    paper = analyzed_portfolio()
    paper.feed_configure(True, start_ms=10 * MINUTE, warmup_minutes=2)
    source = Source([bar(9), bar(10)])
    feed = PaperFeedSupervisor(paper, source, clock_ms=lambda: 11 * MINUTE)
    feed.run_once()
    assert paper.data["feed"]["warmed"] == {} and not paper.data["candles"]
    source.rows.append(bar(8))
    feed.run_once()
    feed.run_once()
    assert paper.data["feed"]["ready"] is True and paper.data["paused"] is True


def test_new_strategy_gets_nontrading_warmup_even_if_portfolio_has_same_latest_candle():
    paper, source, now = analyzed_portfolio(), Source([bar(8), bar(9), bar(10)]), [11 * MINUTE]
    paper.feed_configure(True, start_ms=10 * MINUTE, warmup_minutes=2)
    feed = PaperFeedSupervisor(paper, source, clock_ms=lambda: now[0])
    feed.run_once()
    feed.run_once()
    paper.command("paper.account.create", {"account_id": "b"})
    paper.command(
        "paper.strategy.add",
        {
            "account_id": "b",
            "instance": "new",
            "instrument": BTC.key,
            "module_id": "target_quantity",
            "configuration": {"quantity": "1"},
            "budget": "1000",
            "analysis_snapshot": {"analysis_id": "test", "configuration": {}},
        },
    )
    before = deepcopy(paper.data["candles"])
    feed.run_once()
    assert paper.features["new"].seen == [9 * MINUTE, 10 * MINUTE]
    assert paper.data["candles"] == before
    assert paper.detail("new")["pending"] is None
    assert paper.detail("new")["analysis_snapshot"]["analysis_id"] == "test"
    validate_state(paper.data)


@pytest.mark.parametrize("rows", [[bar(11)], [bar(10), bar(10, price="200")]])
def test_unclosed_or_conflicting_duplicate_source_cannot_advance(rows):
    paper = portfolio()

    class InvalidSource(Source):
        def read(self, *args, **kwargs):
            return tuple(rows)

    feed = enabled(paper, InvalidSource(), [11 * MINUTE - 1])
    feed.run_once()
    assert not paper.data["candles"] and paper.data["feed"]["ready"] is False


def test_duplicate_complete_frame_is_harmless_and_manual_resume_remains_explicit():
    paper = portfolio()
    feed = enabled(paper, Source([bar(10)]), [11 * MINUTE])
    feed.run_once()
    before = deepcopy(paper.data)
    result = paper.feed_apply(
        (bar(10),),
        expected=paper.feed_snapshot(),
        entries_allowed=True,
        reason="ready",
        now_ms=11 * MINUTE,
    )
    assert result["duplicate"] and paper.data == before
    assert paper.data["paused"] is True


@pytest.mark.parametrize(
    "field,value",
    [
        ("enabled", "true"),
        ("ready", 1),
        ("revision", -1),
        ("warmup_minutes", 525601),
        ("start_ms", 1),
        ("warmed", {"unknown": True}),
        ("provenance", {BTC.key: "synthetic"}),
    ],
)
def test_corrupt_feed_control_cannot_be_restored(field, value):
    state = deepcopy(portfolio().data)
    state["feed"][field] = value
    with pytest.raises(ValidationError):
        validate_state(state)


def test_catching_up_preserves_owned_reduction_and_does_not_open_new_positions():
    paper, now = portfolio(), [11 * MINUTE]
    source = Source([bar(10)])
    feed = enabled(paper, source, now)
    paper.command("resume", {})
    feed.run_once()
    now[0] = 12 * MINUTE
    source.rows.append(bar(11))
    feed.run_once()  # Original order fills in its eligible minute.
    assert paper.detail("s0")["quantity"] == "1"
    paper.command("pause", {})
    paper.engines["s0"].module.target = Decimal(0)
    now[0] = 15 * MINUTE
    source.rows.extend([bar(12), bar(13)])  # latest minute14 absent: data gate remains blocked.
    feed.run_once()
    # The protection decision is made now (minute15), never retrospectively
    # filled in minute13 merely because we are catching up on older candles.
    assert paper.detail("s0")["quantity"] == "1"
    assert paper.detail("s0")["pending"]["reducing"]
    assert paper.detail("s0")["pending"]["eligible_at_ms"] == 15 * MINUTE
    assert len(paper.detail("s0")["fills"]) == 1
    assert feed.status()["reason"] == "missing_minute"
    assert paper.data["paused"] is True and not paper.data["feed"]["ready"]
    now[0] = 16 * MINUTE
    source.rows.extend([bar(14), bar(15)])
    feed.run_once()
    assert paper.detail("s0")["quantity"] == "0"
    assert len(paper.detail("s0")["fills"]) == 2
    assert paper.data["paused"] and paper.detail("s0")["pending"] is None


def test_quarantined_strategy_blocks_new_risk_but_does_not_prevent_pending_settlement():
    paper, source, now = portfolio(), Source([bar(10)]), [11 * MINUTE]
    feed = enabled(paper, source, now)
    paper.command("resume", {})
    feed.run_once()
    paper.data["strategies"]["s0"]["status"] = "quarantined"
    source.rows.append(bar(11))
    now[0] = 12 * MINUTE
    feed.run_once()
    assert paper.detail("s0")["quantity"] == "1"
    assert paper.data["feed"]["ready"] is False
    assert feed.status()["reason"] == "quarantined_strategy"


def test_running_supervisor_consumes_two_successive_fake_commits_and_closes():
    paper, source, now = portfolio(), Source([bar(10)]), [11 * MINUTE]
    paper.feed_configure(True, start_ms=10 * MINUTE, warmup_minutes=2)
    paper.command("resume", {})
    feed = PaperFeedSupervisor(paper, source, clock_ms=lambda: now[0], poll_seconds=0.05)
    feed.start()
    try:
        wait_for(lambda: feed.status()["frames"] == 1)
        source.rows.append(bar(11))
        now[0] = 12 * MINUTE
        wait_for(lambda: feed.status()["frames"] == 2)
        assert paper.detail("s0")["quantity"] == "1"
        assert len(paper.detail("s0")["fills"]) == 1
    finally:
        feed.close()
    assert not feed.status()["supervisor_alive"] and source.closed


def wait_for(predicate):
    import time

    deadline = time.monotonic() + 2
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("bounded background paper feed did not progress")
        threading.Event().wait(0.005)


def test_slow_database_read_cannot_label_an_old_minute_fresh_or_create_an_entry():
    paper, source, now = portfolio(), Source([bar(10)]), [11 * MINUTE]
    feed = enabled(paper, source, now)
    paper.command("resume", {})
    original = source.read

    def delayed(*args, **kwargs):
        result = original(*args, **kwargs)
        now[0] = 14 * MINUTE
        return result

    source.read = delayed
    feed.run_once()
    assert paper.detail("s0")["pending"] is None
    assert feed.status()["reason"] == "catching_up"
    assert paper.data["feed"]["ready"] is False


def test_later_symbol_plugin_failure_withdraws_only_unsubmitted_new_risk_in_same_frame():
    paper, source, now = portfolio((BTC, ETH)), Source([bar(10), bar(10, ETH)]), [11 * MINUTE]
    feed = enabled(paper, source, now)
    paper.command("resume", {})

    def broken(_context):
        raise RuntimeError("bad plugin")

    paper.engines["s1"].module.evaluate = broken
    feed.run_once()
    assert paper.detail("s1")["status"] == "quarantined"
    assert paper.detail("s0")["pending"] is None
    assert feed.status()["reason"] == "quarantined_strategy"
    assert all(rows[-1]["open_time_ms"] == 10 * MINUTE for rows in paper.data["candles"].values())


def test_enabling_after_manual_history_never_relabels_the_entire_cache_as_observed_database():
    paper = portfolio()
    paper.command("paper.step", {"instrument": BTC.key, "candle": candle_row(bar(9))})
    feed = enabled(paper, Source([bar(9), bar(10)]), [11 * MINUTE])
    feed.run_once()
    assert paper.data["feed"]["provenance"][BTC.key] == "mixed_manual_and_database"
    validate_state(paper.data)


class RecordBudgetModule(TargetQuantityModule):
    def evaluate(self, context):
        self.last_available = context.budget.available
        return super().evaluate(context)


def margined_portfolio(first, second, *, exit_second=False):
    paper = PaperTradingService(
        lambda _name, instrument, config: RecordBudgetModule(instrument, config),
        create_features,
    )
    paper.command("paper.account.create", {"account_id": "a", "balance": "300", "fee_rate": "0"})
    for name, instrument, budget, target in (
        ("first", first, "200", "1.75"),
        ("second", second, "100", "0" if exit_second else "1"),
    ):
        paper.command(
            "paper.strategy.add",
            {
                "account_id": "a",
                "instance": name,
                "instrument": instrument.key,
                "module_id": "target_quantity",
                "budget": budget,
                "configuration": {"quantity": target},
            },
        )
        paper.data["strategies"][name].update(quantity="1", entry_price="100")
    if exit_second:
        paper.data["strategies"]["second"]["pending"] = {
            "side": "SELL",
            "kind": "MARKET",
            "quantity": "1",
            "price": None,
            "reducing": True,
            "reason": "prior_exit",
            "eligible_at_ms": 10 * MINUTE,
            "reserved": "0",
        }
    return paper


@pytest.mark.parametrize("reverse_symbols", [False, True])
def test_all_close_decisions_share_current_frame_marks_regardless_of_symbol_order(reverse_symbols):
    first = Instrument("binance", Market.USDM, "ETHUSDT" if reverse_symbols else "BTCUSDT")
    second = Instrument("binance", Market.USDM, "BTCUSDT" if reverse_symbols else "ETHUSDT")
    paper = margined_portfolio(first, second)
    feed = enabled(paper, Source([bar(10, first), bar(10, second, price="50")]), [11 * MINUTE])
    paper.command("resume", {})
    feed.run_once()
    assert paper.engines["first"].module.last_available == 50
    assert paper.engines["second"].module.last_available == 50
    assert all(row["pending"] is None for row in paper.data["strategies"].values())
    assert "budget" in paper.detail("first")["last_reason"]


def test_all_preexisting_fills_settle_before_any_new_close_time_decision():
    first, second = (
        Instrument("binance", Market.USDM, "BTCUSDT"),
        Instrument("binance", Market.USDM, "ETHUSDT"),
    )
    paper = margined_portfolio(first, second, exit_second=True)
    feed = enabled(paper, Source([bar(10, first), bar(10, second, price="50")]), [11 * MINUTE])
    paper.command("resume", {})
    feed.run_once()
    assert paper.detail("second")["quantity"] == "0"
    assert paper.data["accounts"]["a"]["balance"] == "250"
    assert paper.engines["first"].module.last_available == 150
    assert paper.detail("first")["pending"]["reserved"] == "75.00"
    # Explicit serial reservation priority applies after common-time settlement/marks.
    assert paper.engines["second"].module.last_available == 75


@pytest.mark.parametrize("late_phase", ["source_read", "before_apply"])
def test_retired_owner_cannot_persist_after_timed_out_close_and_new_core_acquires_root(
    tmp_path, late_phase
):
    from omega.runtime.app import build_core

    entered, release = threading.Event(), threading.Event()
    core = build_core(tmp_path)
    old_paper, feed = core.paper, core.paper_feed
    old_writes = []
    original_persist = old_paper.persist

    def tracked_persist(state):
        old_writes.append(state)
        original_persist(state)

    old_paper.persist = tracked_persist
    old_paper.command("paper.account.create", {"account_id": "a"})
    old_paper.command(
        "paper.strategy.add",
        {
            "account_id": "a",
            "instance": "s",
            "instrument": "binance:usdm:BTCUSDT",
            "module_id": "tef",
            "configuration": {"model": "SPDDEFAULT"},
        },
    )
    old_paper.feed_configure(True, start_ms=10 * MINUTE, warmup_minutes=2)
    old_expected = old_paper.feed_snapshot()
    instrument = Instrument("binance", Market.USDM, "BTCUSDT")

    class BlockingSource(Source):
        def read(self, *args, **kwargs):
            if late_phase == "source_read":
                entered.set()
                assert release.wait(5)
            return (bar(8, instrument), bar(9, instrument))

    source = BlockingSource()
    feed.source, feed.clock_ms, feed.close_timeout_s = source, lambda: 11 * MINUTE, 0.01
    if late_phase == "before_apply":
        warmup = old_paper.feed_warmup

        def delayed_apply(*args, **kwargs):
            # This is AFTER the supervisor's stop check, before the paper lock.
            entered.set()
            assert release.wait(5)
            return warmup(*args, **kwargs)

        old_paper.feed_warmup = delayed_apply
    feed.start()
    new_core = None
    try:
        assert entered.wait(2)
        with pytest.raises(RuntimeError, match="still stopping"):
            core.close()
        count_after_retirement = len(old_writes)
        old_data_after_retirement = deepcopy(old_paper.data)
        path = tmp_path / "state/paper.json"
        retired_hash = sha256(path.read_bytes()).hexdigest()
        # Unknown child cleanup retains exclusive root ownership. Retirement
        # still fences writes while the original readonly query is blocked.
        assert not core.instance.released
        with pytest.raises(RuntimeError, match="already owns this root"):
            build_core(tmp_path)
        release.set()
        feed.thread.join(2)
        assert not feed.thread.is_alive()
        assert source.closed
        assert len(old_writes) == count_after_retirement
        assert old_paper.data == old_data_after_retirement
        assert sha256(path.read_bytes()).hexdigest() == retired_hash
        # Native child completion alone does not acknowledge the earlier
        # failed Core close. Only the same owner's explicit retry releases it.
        assert not core.instance.released
        with pytest.raises(RuntimeError, match="already owns this root"):
            build_core(tmp_path)
        core.close()
        assert core.instance.released
        assert len(old_writes) == count_after_retirement
        assert old_paper.data == old_data_after_retirement
        assert sha256(path.read_bytes()).hexdigest() == retired_hash
        new_core = build_core(tmp_path)
        new_core.paper.command("paper.account.create", {"account_id": "new_owner"})
        expected_hash = sha256(path.read_bytes()).hexdigest()
        core.close()
        assert len(old_writes) == count_after_retirement
        assert old_paper.data == old_data_after_retirement
        assert sha256(path.read_bytes()).hexdigest() == expected_hash
        assert "new_owner" in new_core.paper.data["accounts"]
        for action in (
            lambda: old_paper.command("pause", {}),
            lambda: old_paper.command("resume", {}),
            lambda: old_paper.data_gate(True, "ready"),
            lambda: old_paper.feed_configure(False, start_ms=10 * MINUTE),
            lambda: old_paper.feed_apply(
                (bar(10, instrument),),
                expected=old_expected,
                entries_allowed=True,
                reason="ready",
                now_ms=11 * MINUTE,
            ),
        ):
            with pytest.raises(ValidationError, match="owner is retired"):
                action()
        assert sha256(path.read_bytes()).hexdigest() == expected_hash
    finally:
        release.set()
        if feed.thread:
            feed.thread.join(2)
        if new_core is not None:
            new_core.close()
        else:
            core.close()
        feed.close()


def test_retirement_serializes_with_an_inflight_paper_commit():
    entered, release = threading.Event(), threading.Event()
    paper = portfolio()
    saves = []

    def blocking_persist(state):
        entered.set()
        assert release.wait(2)
        saves.append(state)

    paper.persist = blocking_persist
    command = threading.Thread(target=lambda: paper.command("pause", {}))
    command.start()
    assert entered.wait(1)
    retired = threading.Event()

    def retire():
        paper.retire_feed_owner()
        retired.set()

    retirement = threading.Thread(target=retire)
    retirement.start()
    assert not retired.wait(0.02)
    release.set()
    command.join(2)
    retirement.join(2)
    assert retired.is_set() and len(saves) == 1
    with pytest.raises(ValidationError, match="owner is retired"):
        paper.command("pause", {})
    assert len(saves) == 1


def resume_core(core, operation):
    control = core.control.snapshot()
    return core.request(
        "POST",
        "/api/commands",
        core.token,
        {
            "id": operation,
            "kind": "resume",
            "payload": {
                "expected_epoch": control["epoch"],
                "expected_revision": control["control_revision"],
                "expires_at_ms": control["server_time_ms"] + 30000,
            },
        },
    )


def configured_core(tmp_path):
    from omega.runtime.app import build_core

    core = build_core(tmp_path)
    core.paper.command("paper.account.create", {"account_id": "a"})
    add(core.paper, "s0")
    core.paper.feed_configure(True, start_ms=10 * MINUTE, warmup_minutes=2)
    core.paper_feed.source = Source([bar(10)])
    core.paper_feed.clock_ms = lambda: 11 * MINUTE
    core.paper_feed.run_once()
    assert resume_core(core, "resume-initial")["status"] == "completed"
    return core


def test_feed_recording_failure_emits_one_critical_and_exposes_effective_gate_until_manual_resume(
    tmp_path,
):
    core = configured_core(tmp_path)
    original = core.paper.persist
    try:

        def fail(_state):
            raise OSError("paper journal unavailable")

        core.paper.persist = fail
        core.paper_feed.run_once()
        sequence = core.control.snapshot()["sequence"]
        for _ in range(3):
            core.paper_feed.run_once()
        state = core.request("GET", "/api/state", core.token)
        health = state["core"]
        assert health["paused"] is health["control_paused"] is False
        assert health["paper_paused"] is True and health["paper_manual_resume_required"] is True
        assert health["control_recording"] is True and health["paper_recording"] is False
        assert health["data_gate"] is health["effective_entries_allowed"] is False
        incidents = [
            row for row in state["incidents"] if row["code"] == "PAPER_RECORDING_UNAVAILABLE"
        ]
        assert len(incidents) == 1 and incidents[0]["severity"] == "critical"
        assert (
            core.control.snapshot()["sequence"] == sequence
        )  # GET/repeated failures do not flood writes.
        core.paper.persist = original
        core.paper_feed.run_once()
        health = core.state()["core"]
        assert health["paper_recording"] and health["data_gate"]
        assert health["paper_paused"] and health["paper_manual_resume_required"]
        assert not health["effective_entries_allowed"]
        assert resume_core(core, "resume-after-recovery")["status"] == "completed"
        assert core.state()["core"]["effective_entries_allowed"]
        assert not core.paper.recording_health()["manual_resume_required"]
    finally:
        core.paper.persist = original
        core.close()


def test_source_only_failure_does_not_raise_storage_critical_or_clear_user_pause(tmp_path):
    from omega.domain.errors import StorageError

    core = configured_core(tmp_path)
    original = core.paper_feed.source.read
    try:

        def unavailable(*_args, **_kwargs):
            raise StorageError("database read unavailable")

        core.paper_feed.source.read = unavailable
        core.paper_feed.run_once()
        state = core.state()
        assert not state["core"]["control_paused"] and not state["core"]["paper_paused"]
        assert state["core"]["paper_recording"] and not state["core"]["data_gate"]
        assert not state["core"]["paper_manual_resume_required"]
        assert not [
            row for row in state["incidents"] if row["code"] == "PAPER_RECORDING_UNAVAILABLE"
        ]
        core.request(
            "POST",
            "/api/commands",
            core.token,
            {"id": "user-pause", "kind": "pause", "payload": {}},
        )
        core.paper_feed.source.read = original
        core.paper_feed.run_once()
        state = core.state()
        assert state["core"]["data_gate"]
        assert state["core"]["control_paused"] and state["core"]["paper_paused"]
        assert not state["core"]["effective_entries_allowed"]
    finally:
        core.close()


def test_one_failed_save_still_alerts_when_followup_gate_write_succeeds(tmp_path):
    core = configured_core(tmp_path)
    original, calls = core.paper.persist, []

    def fail_once(state):
        calls.append(True)
        if len(calls) == 1:
            raise OSError("single lost paper save")
        original(state)

    core.paper.persist = fail_once
    previous = core.control.snapshot()
    stale_resume = {
        "id": "resume-pre-fault",
        "kind": "resume",
        "payload": {
            "expected_epoch": previous["epoch"],
            "expected_revision": previous["control_revision"],
            "expires_at_ms": previous["server_time_ms"] + 30000,
        },
    }
    try:
        core.paper_feed.run_once()
        health = core.state()["core"]
        assert health["paper_recording"] is True  # The subsequent gate update was durable.
        assert health["paper_manual_resume_required"] is True
        assert not health["effective_entries_allowed"] and not core.paper.data["candles"]
        assert [row["code"] for row in core.control.snapshot()["incidents"]] == [
            "PAPER_RECORDING_UNAVAILABLE"
        ]
        with pytest.raises(ValidationError, match="revision is stale"):
            core.request("POST", "/api/commands", core.token, stale_resume)
        assert core.paper.recording_health()["manual_resume_required"]
    finally:
        core.paper.persist = original
        core.close()


def test_storage_callback_is_outside_paper_lock_and_does_not_deadlock_owner_close(tmp_path):
    core = configured_core(tmp_path)
    callback_entered = threading.Event()
    original = core.paper.persist

    def fail(_state):
        raise OSError("paper journal unavailable")

    core.paper.persist = fail
    callback = core.record_incident

    def notify(code, severity):
        acquired = threading.Event()
        observer = threading.Thread(target=lambda: (core.paper.snapshot(), acquired.set()))
        observer.start()
        assert acquired.wait(1)  # Callback holding paper.lock would block the observer.
        observer.join(1)
        callback_entered.set()
        callback(code, severity)

    core.paper_feed.on_incident = notify
    core.paper_feed.close_timeout_s = 0.5
    try:
        with core.owner_mutation_lock:
            core.paper_feed.start()
            assert callback_entered.wait(2)
            core.close()  # Joins the callback while holding the same owner lock.
        assert not core.paper_feed.thread.is_alive()
        assert core.instance.stream.closed
    finally:
        core.paper.persist = original
        if not core.instance.stream.closed:
            core.close()


def test_prefault_resume_is_rejected_while_critical_notification_waits_for_owner_lock(tmp_path):
    core = configured_core(tmp_path)
    original, calls = core.paper.persist, []
    entered, release = threading.Event(), threading.Event()
    core.request(
        "POST",
        "/api/commands",
        core.token,
        {"id": "user-pause-before-fault", "kind": "pause", "payload": {}},
    )
    previous = core.control.snapshot()
    request = {
        "id": "queued-prefault-resume",
        "kind": "resume",
        "payload": {
            "expected_epoch": previous["epoch"],
            "expected_revision": previous["control_revision"],
            "expires_at_ms": previous["server_time_ms"] + 30000,
        },
    }

    def fail_once(state):
        calls.append(True)
        if len(calls) == 1:
            raise OSError("paper save failed")
        original(state)

    callback = core.record_incident

    def delayed_incident(code, severity):
        entered.set()
        assert release.wait(3)
        callback(code, severity)

    core.paper.persist = fail_once
    core.paper_feed.on_incident = delayed_incident
    tick = threading.Thread(target=core.paper_feed.run_once)
    tick.start()
    try:
        assert entered.wait(2)
        assert core.paper.recording_health()["available"]
        # The control revision has not yet been advanced by the delayed callback.
        assert core.control.snapshot()["control_revision"] == previous["control_revision"]
        result = core.request("POST", "/api/commands", core.token, request)
        assert result["status"] == "rejected"
        assert core.control.snapshot()["paused"] is True
        assert core.paper.recording_health()["manual_resume_required"]
        assert core.paper.data["paused"]
        release.set()
        tick.join(2)
        assert core.paper.recording_health()["incident_announced"]
        assert core.control.snapshot()["control_revision"] > previous["control_revision"]
        assert resume_core(core, "fresh-resume-after-announcement")["status"] == "completed"
    finally:
        release.set()
        tick.join(2)
        core.paper.persist = original
        core.close()
