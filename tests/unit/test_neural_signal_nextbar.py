"""Real file journals and common virtual next-bar fills for the NN quantity demo."""

import json
from contextlib import ExitStack, contextmanager
from decimal import Decimal as D

import pytest

from omega.application.margin_account import MarginAccountOwner
from omega.application.margin_strategy import MarginStrategyAdapter
from omega.application.virtual_margin import VirtualMarginEngine
from omega.domain.margin import (
    MarginAccount,
    MarginBracket,
    MarginBracketSnapshot,
    MarginMode,
    MarginPosition,
    MarkSnapshot,
)
from omega.domain.models import Candle, Instrument, Market, Side
from omega.domain.trading import ExecutionConstraints
from omega.runtime.margin_store import FileMarginStateStore
from omega.runtime.margin_strategy_store import FileMarginStrategyStore
from omega.runtime.virtual_margin_journal import FileVirtualMarginJournal
from omega.strategies.neural_signal.module import NeuralSignalModule

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
RULES = ExecutionConstraints(leverage=D(10), can_short=True)


def bar(index, price="100"):
    value = D(price)
    return Candle(BTC, index * 60000, (index + 1) * 60000, value, value, value, value, D(10))


def brackets():
    return (
        MarginBracketSnapshot(
            BTC,
            (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
            0,
            0,
            "explicit_test_bracket",
            "a" * 64,
        ),
    )


@contextmanager
def durable(root, mode, *, create=False):
    initial = MarginAccount(
        "nn-demo",
        "USDT",
        D(1000),
        (
            MarginPosition(
                BTC,
                "nn-strategy",
                mode,
                10,
                D(100),
                isolated_wallet=D(100) if mode == MarginMode.ISOLATED else D(0),
            ),
        ),
    )
    with ExitStack() as resources:
        account_store = FileMarginStateStore(root, "nn-demo")
        resources.callback(account_store.close)
        journal = FileVirtualMarginJournal(root, "nn-demo")
        resources.callback(journal.close)
        strategy_store = FileMarginStrategyStore(root, "nn-strategy")
        resources.callback(strategy_store.close)
        owner = MarginAccountOwner(initial, account_store, create_if_missing=create)
        execution = VirtualMarginEngine(owner, journal, create_if_missing=create)
        adapter = MarginStrategyAdapter(
            "nn-strategy",
            BTC,
            NeuralSignalModule(BTC),
            execution,
            strategy_store,
            owner_id="nn-strategy",
            create_if_missing=create,
        )
        assert execution.snapshot().state == "ready", execution.snapshot()
        assert adapter.snapshot().state == "ready", adapter.snapshot()
        yield execution, adapter, strategy_store


def evaluate(adapter, index, *, signal="1", price="100", protection_only=False):
    candle = bar(index, price)
    return adapter.evaluate_closed(
        candle,
        {} if protection_only else {"NN.score": D(signal)},
        RULES,
        as_of_ms=candle.close_time_ms,
        marks=(
            MarkSnapshot(
                BTC, D(price), candle.close_time_ms, candle.close_time_ms, "synthetic_test_mark"
            ),
        ),
        brackets=brackets(),
        worst_execution_price=D(price),
        fee_rate=D(".001"),
        input_reference="b" * 64,
        protection_only=protection_only,
    )


def settle(execution, index, price="100"):
    candle = bar(index, price)
    for phase in ("open", "close"):
        plan = execution.prepare_batch(
            (candle,), as_of_ms=candle.close_time_ms, phase=phase, brackets=brackets()
        )
        if plan.facts or plan.rejections:
            execution.commit_batch(plan)
    return execution.snapshot()


@pytest.mark.parametrize("mode", [MarginMode.CROSS, MarginMode.ISOLATED])
def test_nn_demo_real_file_restart_close_before_reverse_and_nextbar_stop(mode, tmp_path):
    with durable(tmp_path, mode, create=True) as (execution, adapter, store):
        execution.manual_resume("explicit_test_start")
        entering = evaluate(adapter, 0)
        assert entering.orders[0].quantity == 1 and entering.orders[0].side == Side.BUY
        pending = execution.snapshot().pending[0]
        assert pending.eligible_at_ms == 60000
        assert execution.snapshot().account.positions[0].quantity == 0
        assert execution.snapshot().account.ledger == ()
        checkpoint = store.load("nn-strategy")
        assert json.loads(checkpoint.module_checkpoint)["payload"]["last_open_time_ms"] == 0
        assert not execution.prepare_batch(
            (bar(0),), as_of_ms=60000, phase="open", brackets=brackets()
        ).facts

    with durable(tmp_path, mode) as (execution, adapter, _):
        assert execution.snapshot().paused
        assert execution.snapshot().pending[0] == pending
        assert adapter.module.last_open_time_ms == 0
        execution.manual_resume("explicit_restart_resume")
        first_fill = settle(execution, 1)
        assert first_fill.account.positions[0].quantity == 1
        assert len(first_fill.account.ledger) == 1
        close_long = evaluate(adapter, 1, signal="-1")
        assert close_long.orders[0].reducing and close_long.orders[0].quantity == 1
        assert execution.snapshot().account.positions[0].quantity == 1
        assert settle(execution, 2).account.positions[0].quantity == 0
        short = evaluate(adapter, 2, signal="-1")
        assert short.orders[0].side == Side.SELL and not short.orders[0].reducing
        assert execution.snapshot().account.positions[0].quantity == 0
        assert settle(execution, 3).account.positions[0].quantity == -1
        # Advance the preceding closed minute without changing the achieved target.
        assert not evaluate(adapter, 3, signal="-1").orders
        stop = evaluate(adapter, 4, price="105", protection_only=True)
        assert stop.orders[0].reducing and stop.orders[0].side == Side.BUY
        assert execution.snapshot().account.positions[0].quantity == -1
        flat = settle(execution, 5, "105")
        assert flat.account.positions[0].quantity == 0 and len(flat.account.ledger) == 4
        assert not evaluate(adapter, 5, price="105").orders
        assert adapter.module.snapshot()["halted"]

    with durable(tmp_path, mode) as (execution, adapter, _):
        assert execution.snapshot().account.positions[0].quantity == 0
        assert len(execution.snapshot().account.ledger) == 4
        execution.manual_resume("execution_resume_preserves_local_stop_latch")
        assert not evaluate(adapter, 6, price="105").orders
        assert adapter.module.snapshot()["halted"] and adapter.module.last_open_time_ms == 360000
