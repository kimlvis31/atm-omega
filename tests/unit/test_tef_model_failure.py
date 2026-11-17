"""A failed TEF calculator must not disable independently known local protection."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
from types import MethodType

import pytest

from omega.application.trading import TradingEngine
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market, Side
from omega.domain.trading import BudgetView, ExecutionConstraints, PositionView, TradeContext
from omega.strategies.tef.module import TefModule

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def context(*, quantity="1", low="95", high="100", close="100", **controls):
    candle = Candle(INSTRUMENT, 60000, 120000, D(100), D(high), D(low), D(close), D(10))
    return TradeContext(
        INSTRUMENT,
        candle.close,
        candle.close_time_ms,
        PositionView(D(quantity), D(100) if D(quantity) else D(0), 1, 0),
        BudgetView(D(100), D(100)),
        ExecutionConstraints(can_short=True),
        candle,
        {"0_MMACD_MSDELTAABSMAREL": 0.5},
        **controls,
    )


def failing_module(monkeypatch, configuration=None):
    module = TefModule(INSTRUMENT, configuration or {"fullStopLossImmediate": "0.05"})
    first = Candle(INSTRUMENT, 0, 60000, D(100), D(100), D(100), D(100), D(10))
    module.model.evaluate(first, {"0_MMACD_MSDELTAABSMAREL": 0.5})
    before = module.model.checkpoint()

    def fail(*_args):
        raise ValidationError("synthetic calculator failure")

    monkeypatch.setattr(module.model, "evaluate", fail)
    return module, before


@pytest.mark.parametrize(
    "quantity,low,high,side",
    [
        ("1", "95", "100", Side.SELL),
        ("-1", "100", "105", Side.BUY),
    ],
)
def test_model_failure_still_reduces_known_stop_with_unchanged_model(
    monkeypatch, quantity, low, high, side
):
    module, before = failing_module(monkeypatch)
    engine = TradingEngine(module)
    decision = engine.evaluate(
        context(quantity=quantity, low=low, high=high, entries_allowed=False)
    )
    assert decision.reason == "FSLIMMED" and len(decision.orders) == 1
    assert decision.orders[0].reducing and decision.orders[0].side == side
    assert decision.orders[0].quantity == D(1)
    assert engine.module.model.checkpoint() == before
    assert engine.module.model.last_open_time_ms == 0
    assert engine.module.last_open_time_ms == 60000
    assert engine.module.snapshot()["model_failure"]["error_type"] == "ValidationError"
    assert not engine.module.history_priming_ready


def test_close_stop_remains_separate_from_failed_signal(monkeypatch):
    module, before = failing_module(monkeypatch, {"fullStopLossClose": "0.05"})
    result = module.evaluate(context(low="90", close="95"))
    assert result.reason == "FSLCLOSE" and result.orders[0].reducing
    assert module.model.checkpoint() == before


def test_failed_model_cancels_pending_entry_before_protection_without_duplicate(monkeypatch):
    module, before = failing_module(monkeypatch)
    engine = TradingEngine(module)
    pending = context(has_pending_order=True)
    cancel = engine.evaluate(pending)
    assert cancel.cancel_pending and not cancel.orders and cancel.reason == "FSLIMMED"
    reduction = engine.evaluate(replace(pending, has_pending_order=False))
    assert reduction.orders[0].reducing
    queued = engine.evaluate(replace(pending, position=replace(pending.position, revision=2)))
    assert not queued.orders and not queued.cancel_pending
    assert queued.reason == "await_pending_protection_order"
    assert engine.module.model.checkpoint() == before


@pytest.mark.parametrize("controls", [{"state_certain": False}, {"control_owned": False}])
def test_model_failure_does_not_override_financial_uncertainty(monkeypatch, controls):
    module, before = failing_module(monkeypatch)
    result = module.evaluate(context(**controls))
    assert not result.orders and not result.cancel_pending
    assert result.reason == "position_or_control_uncertain"
    assert module.model.checkpoint() == before


def test_model_error_cannot_become_a_signal_or_silent_warmup(monkeypatch):
    module, before = failing_module(monkeypatch)
    result = module.evaluate(context(quantity="0", low="100"))
    assert not result.orders and not result.cancel_pending
    assert result.reason == "model_failure_requires_recovery"
    assert module.model.checkpoint() == before
    assert module.snapshot()["model_failure"]["open_time_ms"] == 60000


def test_calculator_control_signals_are_not_swallowed(monkeypatch):
    module = TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"})

    def stop(*_args):
        raise KeyboardInterrupt

    monkeypatch.setattr(module.model, "evaluate", stop)
    with pytest.raises(KeyboardInterrupt):
        module.evaluate(context())


def test_fault_latch_survives_checkpoint_and_manual_resume_without_relabeling_model(monkeypatch):
    module, before = failing_module(monkeypatch)
    module.evaluate(context())
    checkpoint = json.loads(json.dumps(module.checkpoint()))
    restored = TefModule(INSTRUMENT, module.config)
    restored.restore(checkpoint)
    assert json.loads(json.dumps(restored.checkpoint())) == checkpoint
    assert restored.model.checkpoint() == before
    assert restored.model.last_open_time_ms == 0 and restored.last_open_time_ms == 60000
    restored.resume()  # Entry control is not a calculator rebuild.
    next_context = context(quantity="0", low="100")
    next_context = replace(
        next_context,
        closed_candle=replace(
            next_context.closed_candle, open_time_ms=120000, close_time_ms=180000
        ),
        observed_at_ms=180000,
    )
    assert restored.evaluate(next_context).reason == "model_failure_requires_recovery"
    assert restored.last_open_time_ms == 120000 and restored.model.last_open_time_ms == 0
    assert not restored.can_prime_history()


def test_calculator_partial_mutation_is_discarded_before_protection(monkeypatch):
    module = TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"})
    before = module.model.checkpoint()

    def fail(model, *_args):
        model._tracker["synthetic_partial"] = 100
        raise RuntimeError("synthetic partial calculation")

    monkeypatch.setattr(module.model, "evaluate", MethodType(fail, module.model))
    assert module.evaluate(context()).orders[0].reducing
    assert module.model.checkpoint() == before
    assert module.snapshot()["model_failure"]["error_type"] == "RuntimeError"


def test_failed_calculator_copy_also_preserves_local_protection(monkeypatch):
    import omega.strategies.tef.module as tef_module
    from omega.strategies.tef.strategy import TefModel

    module = TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"})
    before = module.model.checkpoint()
    original_copy = tef_module.deepcopy

    def fail_copy(value):
        if isinstance(value, TefModel):
            raise RuntimeError("synthetic model copy failure")
        return original_copy(value)

    monkeypatch.setattr(tef_module, "deepcopy", fail_copy)
    engine = TradingEngine(module)
    decision = engine.evaluate(context())
    assert decision.reason == "FSLIMMED" and decision.orders[0].reducing
    assert engine.module.model.checkpoint() == before
    assert engine.module.snapshot()["model_failure"]["error_type"] == "RuntimeError"


def test_actual_invalid_feature_is_blocked_without_disabling_a_known_stop():
    module = TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"})
    before = module.model.checkpoint()
    decision = module.evaluate(replace(context(), features={"0_MMACD_MSDELTAABSMAREL": True}))
    assert decision.reason == "FSLIMMED" and decision.orders[0].reducing
    assert module.model.checkpoint() == before
    assert module.snapshot()["model_failure"]["error_type"] == "ValidationError"


@pytest.mark.parametrize(
    "change",
    [
        {"model_failure": None},
        {"model_failure": {"error_type": "RuntimeError", "open_time_ms": True}},
        {"model_failure": {"error_type": "RuntimeError", "open_time_ms": 60001}},
        {"protection_open_time_ms": 0},
        {"protection_open_time_ms": True},
    ],
)
def test_corrupt_failed_model_checkpoint_cannot_change_fresh_module(monkeypatch, change):
    failed, _ = failing_module(monkeypatch)
    failed.evaluate(context())
    checkpoint = deepcopy(failed.checkpoint())
    checkpoint["payload"].update(change)
    checkpoint["sha256"] = sha256(
        json.dumps(checkpoint["payload"], sort_keys=True, allow_nan=False).encode()
    ).hexdigest()
    fresh = TefModule(INSTRUMENT, failed.config)
    before = fresh.checkpoint()
    with pytest.raises(ValidationError):
        fresh.restore(checkpoint)
    assert fresh.checkpoint() == before


def test_healthy_checkpoint_keeps_v1_field_set_and_exact_restore():
    module = TefModule(INSTRUMENT)
    module.evaluate(context(quantity="0", low="100"))
    state = module.checkpoint()
    assert state["version"] == 1
    assert set(state["payload"]) == {
        "configuration",
        "instrument",
        "model",
        "pending_stop",
        "sl_exited",
        "halted",
    }
    restored = TefModule(INSTRUMENT)
    restored.restore(state)
    assert restored.checkpoint() == state


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_failed_model_fsl_survives_actual_financial_files_restart_and_next_bar(
    tmp_path, monkeypatch, mode
):
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
    from omega.runtime.margin_store import FileMarginStateStore
    from omega.runtime.margin_strategy_store import FileMarginStrategyStore
    from omega.runtime.virtual_margin_journal import FileVirtualMarginJournal

    initial = MarginAccount(
        "a",
        "USDT",
        D(1000),
        (
            MarginPosition(
                INSTRUMENT,
                "s",
                MarginMode(mode),
                10,
                D(100),
                D(1),
                D(100),
                D(100) if mode == "isolated" else D(0),
            ),
        ),
    )
    brackets = (
        MarginBracketSnapshot(
            INSTRUMENT,
            (MarginBracket(D(0), D(1000000), D("0.01"), D(0), 20),),
            0,
            0,
            "synthetic_bracket",
            "a" * 64,
        ),
    )

    def open_owner(*, create):
        account_store = FileMarginStateStore(tmp_path, "a")
        journal = FileVirtualMarginJournal(tmp_path, "a")
        strategy_store = FileMarginStrategyStore(tmp_path, "s")
        try:
            owner = MarginAccountOwner(initial, account_store, create_if_missing=create)
            execution = VirtualMarginEngine(owner, journal, create_if_missing=create)
            module = TefModule(INSTRUMENT, {"fullStopLossImmediate": "0.05"})
            adapter = MarginStrategyAdapter(
                "s",
                INSTRUMENT,
                module,
                execution,
                strategy_store,
                owner_id="s",
                create_if_missing=create,
            )
        except BaseException:
            strategy_store.close()
            journal.close()
            account_store.close()
            raise
        return execution, adapter, (strategy_store, journal, account_store)

    def evaluate(adapter, index, *, low="100"):
        bar = Candle(
            INSTRUMENT, index * 60000, (index + 1) * 60000, D(100), D(100), D(low), D(100), D(10)
        )
        result = adapter.evaluate_closed(
            bar,
            {"0_MMACD_MSDELTAABSMAREL": 0.5},
            ExecutionConstraints(leverage=D(10), can_short=True),
            as_of_ms=bar.close_time_ms,
            marks=(
                MarkSnapshot(
                    INSTRUMENT, D(100), bar.close_time_ms, bar.close_time_ms, "synthetic_mark"
                ),
            ),
            brackets=brackets,
            worst_execution_price=D(100),
            fee_rate=D("0.001"),
            input_reference="b" * 64,
        )
        return bar, result

    execution, adapter, resources = open_owner(create=True)
    try:
        assert not evaluate(adapter, 0)[1].orders  # Paused entries; normal model progresses.
        original_model = adapter.module.model.checkpoint()

        def fail(*_args):
            raise ValidationError("synthetic calculator failure")

        monkeypatch.setattr(adapter.module.model, "evaluate", fail)
        _, decision = evaluate(adapter, 1, low="95")
        assert decision.reason == "FSLIMMED" and decision.orders[0].reducing
        pending = execution.snapshot().pending
        assert len(pending) == 1
        assert adapter.module.model.checkpoint() == original_model
        assert adapter.snapshot().checkpoint.last_open_time_ms == 60000
    finally:
        for resource in resources:
            resource.close()

    execution, adapter, resources = open_owner(create=False)
    try:
        assert execution.snapshot().pending == pending
        assert evaluate(adapter, 1, low="95")[1] == decision
        assert execution.snapshot().pending == pending
        assert adapter.module.model.checkpoint() == original_model
        bar = Candle(INSTRUMENT, 120000, 180000, D(100), D(100), D(100), D(100), D(10))
        plan = execution.prepare_batch((bar,), as_of_ms=180000, phase="open", brackets=brackets)
        execution.commit_batch(plan)
        final = execution.snapshot()
        assert not final.pending and final.account.positions[0].quantity == 0
        assert len(final.account.ledger) == 1
        assert final.account.ledger[0].fact.order_id == pending[0].order_id
        assert evaluate(adapter, 2)[1].reason == "model_failure_requires_recovery"
        assert adapter.module.model.last_open_time_ms == 0
        assert adapter.module.last_open_time_ms == 120000
    finally:
        for resource in resources:
            resource.close()

    execution, adapter, resources = open_owner(create=False)
    try:
        assert execution.snapshot().account == final.account
        assert execution.snapshot().pending == ()
        assert adapter.module.snapshot()["model_failure"] is not None
        assert not evaluate(adapter, 3)[1].orders
        assert len(execution.snapshot().account.ledger) == 1
    finally:
        for resource in resources:
            resource.close()
