"""Explicit strategy-local resume is separate from account entry control."""

import json
from copy import deepcopy
from dataclasses import replace
from threading import Thread

import pytest
from test_margin_session import bars, configuration, new, tables
from test_margin_session_features import Provider

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.trading import TradeDecision
from omega.ports.modules import ModuleDescriptor
from omega.runtime.margin_session import MarginTradingSession


def test_real_neural_stop_resume_keeps_pending_protection_and_other_ts_untouched(tmp_path):
    provider = Provider(tmp_path)
    account, strategies = configuration()
    strategies[0].update(
        module="neural_signal",
        configuration={"local_stop_rate": ".05"},
        external_features=[provider.binding.document()],
    )
    session = MarginTradingSession(
        tmp_path,
        account,
        strategies,
        0,
        create=True,
        external_feature_provider=provider,
        clock_ms=lambda: provider.clock,
    )
    try:
        session.resume("account-start")
        for index, price in ((0, "100"), (1, "100"), (2, "100"), (3, "90")):
            provider.clock = (index + 1) * 60000 + 10
            session.run_once(bars(index, price), as_of_ms=provider.clock, brackets=tables())
        target = session._adapters["btc-ts"]
        stopped = target.module.snapshot()
        assert stopped["halted"] and stopped["pending_stop"] is not None
        session.resume("account-resume-only")
        assert target.module.snapshot()["halted"]
        other = session._adapters["eth-ts"].snapshot().checkpoint
        original = session.execution.snapshot().pending
        metadata = session.path.read_bytes()
        result = session.resume_strategy("local-resume", "btc-ts")
        resumed = target.module.snapshot()
        assert not resumed["halted"]
        assert resumed["pending_stop"] == stopped["pending_stop"]
        assert resumed["last_open_time_ms"] == stopped["last_open_time_ms"]
        assert result["paused"] and session.execution.snapshot().pending == original
        assert session._adapters["eth-ts"].snapshot().checkpoint == other
        assert session.path.read_bytes() == metadata
    finally:
        session.close()


class ResumeCounter:
    calls = []
    descriptor = ModuleDescriptor("resume_counter", "1", frozenset(), True)

    def __init__(self, instrument):
        self.instrument, self.last_open_time_ms = instrument, None
        self.halted, self.resumes = True, 0

    def checkpoint(self):
        return {
            "version": 1,
            "cursor": self.last_open_time_ms,
            "halted": self.halted,
            "resumes": self.resumes,
        }

    def restore(self, document):
        self.last_open_time_ms, self.halted, self.resumes = (
            document["cursor"],
            document["halted"],
            document["resumes"],
        )

    def resume(self):
        self.calls.append(self.instrument.key)
        self.halted, self.resumes = False, self.resumes + 1

    def evaluate(self, context):
        self.last_open_time_ms = context.closed_candle.open_time_ms
        return TradeDecision(reason="counter_hold")


class Registry:
    def create(self, module, instrument, configuration):
        assert module == "resume_counter"
        return ResumeCounter(instrument)


def counter_session(root, *, create=True):
    account, strategies = configuration()
    for raw in strategies:
        raw.update(module="resume_counter", configuration={})
    return MarginTradingSession(root, account, strategies, 0, create=create, registry=Registry())


@pytest.mark.parametrize("after_commit", [False, True])
def test_checkpoint_ack_loss_reconciles_exact_candidate_without_resume_callback_replay(
    tmp_path, monkeypatch, after_commit
):
    ResumeCounter.calls.clear()
    session = counter_session(tmp_path)
    target = session._adapters["btc-ts"]
    original, fired = target.store.save, []

    def fail(state, **kwargs):
        if not fired and json.loads(state.module_checkpoint)["resumes"] == 1:
            fired.append(True)
            if after_commit:
                original(state, **kwargs)
            raise StorageError("synthetic module checkpoint ACK loss")
        original(state, **kwargs)

    monkeypatch.setattr(target.store, "save", fail)
    try:
        with pytest.raises(StorageError):
            session.resume_strategy("explicit-first", "btc-ts")
        assert len(ResumeCounter.calls) == 1 and session.snapshot()["paused"]
        with pytest.raises(TradingBlocked):
            session.resume_strategy("forbidden-unreconciled", "btc-ts")
        session.reconcile()
        assert len(ResumeCounter.calls) == 1 and target.module.resumes == 1
        session.resume_strategy("explicit-after-reconcile", "btc-ts")
        assert len(ResumeCounter.calls) == 2 and target.module.resumes == 2
        assert session.snapshot()["paused"]
    finally:
        session.close()


def test_parent_unknown_and_restart_do_not_reexecute_completed_module_callback(tmp_path):
    ResumeCounter.calls.clear()
    session = counter_session(tmp_path)
    session.resume_strategy("original-parent-command", "btc-ts")
    expected = session._adapters["btc-ts"].snapshot().checkpoint
    session.close()
    # The parent's reply can be lost after this exact checkpoint committed.
    restored = counter_session(tmp_path, create=False)
    try:
        assert len(ResumeCounter.calls) == 1
        assert restored._adapters["btc-ts"].snapshot().checkpoint == expected
        assert restored.snapshot()["paused"]
        restored.resume_strategy("new-user-command", "btc-ts")
        assert len(ResumeCounter.calls) == 2
        assert restored._adapters["btc-ts"].module.resumes == 2
    finally:
        restored.close()


def test_financial_pause_ack_loss_never_calls_strategy_resume_before_reconciliation(
    tmp_path, monkeypatch
):
    ResumeCounter.calls.clear()
    session = counter_session(tmp_path)
    journal = session.execution._journal
    original, fired = journal.append, []

    def lost_pause_ack(*args, **kwargs):
        acknowledgement = original(*args, **kwargs)
        if not fired:
            fired.append(True)
            raise StorageError("actual financial pause journal ACK loss")
        return acknowledgement

    monkeypatch.setattr(journal, "append", lost_pause_ack)
    try:
        with pytest.raises(StorageError, match="pause journal"):
            session.resume_strategy("pause-uncertain", "btc-ts")
        assert not ResumeCounter.calls
        with pytest.raises(TradingBlocked):
            session.resume_strategy("before-reconcile", "btc-ts")
        session.reconcile()
        assert not ResumeCounter.calls and session.snapshot()["paused"]
        session.resume_strategy("new-explicit-after-reconcile", "btc-ts")
        assert len(ResumeCounter.calls) == 1
        assert session._adapters["btc-ts"].module.resumes == 1
    finally:
        session.close()


@pytest.mark.parametrize(
    "failure", ["foreign", "bad_request", "pending_frame", "adapter", "unknown", "shutdown"]
)
def test_resume_preflight_rejects_without_any_callback_or_financial_pause(
    tmp_path, monkeypatch, failure
):
    ResumeCounter.calls.clear()
    session = counter_session(tmp_path)
    session.resume("account-start")
    before = deepcopy(session._meta)
    request, target = "request", "btc-ts"
    if failure == "foreign":
        target = "other-wallet-strategy"
    elif failure == "bad_request":
        request = "../invalid"
    elif failure == "pending_frame":
        pending = deepcopy(session._meta)
        pending["pending"] = session._frame(bars(0), 60000, tables(), False)
        session._persist(pending)
    elif failure == "adapter":
        adapter = session._adapters["eth-ts"]
        state = adapter.snapshot()
        monkeypatch.setattr(adapter, "snapshot", lambda: replace(state, state="unknown"))
    elif failure == "unknown":
        session._state = "unknown"
    else:
        monkeypatch.setattr(session, "shutdown_status", lambda: {"recovery_required": True})
    try:
        with pytest.raises((TradingBlocked, ValidationError)):
            session.resume_strategy(request, target)
        assert not ResumeCounter.calls
        assert not session.execution.snapshot().paused
        if failure not in {"pending_frame", "shutdown"}:
            assert session._meta == before
    finally:
        session.close()


def test_resume_strategy_rejects_wrong_thread_without_mutation(tmp_path):
    session = new(tmp_path)
    caught = []

    def other_thread():
        try:
            session.resume_strategy("explicit", "btc-ts")
        except Exception as error:
            caught.append(error)

    try:
        worker = Thread(target=other_thread)
        worker.start()
        worker.join()
        assert len(caught) == 1 and isinstance(caught[0], TradingBlocked)
        assert session.snapshot()["cursor_ms"] is None
    finally:
        session.close()
