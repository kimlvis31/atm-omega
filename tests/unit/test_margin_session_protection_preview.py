"""Read-only NN wait admission never delays known settlement or local protection."""

from copy import deepcopy
from dataclasses import replace

import pytest
from test_margin_session import BTC, ETH, bars, configuration, new, run, tables
from test_margin_session_features import NNRegistry, Provider, external_run, external_session

from omega.application.margin_execution import FrozenPendingOrder
from omega.application.margin_risk import approve_order
from omega.domain.margin import MarkSnapshot
from omega.domain.models import Candle, Side
from omega.domain.trading import OrderProposal
from omega.runtime.margin_session import MarginTradingSession
from omega.strategies.registry import StrategyRegistry


def files(root):
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*.json")}


def prepare_exposed(root):
    provider = Provider(root)
    session = external_session(root, provider)
    session.resume("start")
    for index in range(3):
        external_run(session, provider, index)
    assert session.execution.snapshot().account.positions[0].quantity == 1
    assert not session.execution.snapshot().pending
    return session, provider


def test_flat_account_can_wait_without_querying_or_changing_nn_state(tmp_path, monkeypatch):
    provider = Provider(tmp_path)
    session = external_session(tmp_path, provider)
    monkeypatch.setattr(
        session._adapters["btc-ts"],
        "preview_protection",
        lambda *a, **k: pytest.fail("flat account must not call a strategy"),
    )
    before = files(tmp_path)
    try:
        assert (
            session.requires_immediate_protection(
                (bars(0)[0],), as_of_ms=60010, brackets=(tables()[0],)
            )
            is False
        )
        assert provider.lookups == 0 and files(tmp_path) == before
        assert session.snapshot()["cursor_ms"] is None
    finally:
        session.close()


def test_exposed_safe_nn_waits_but_stop_is_immediate_and_preview_has_no_side_effect(tmp_path):
    session, provider = prepare_exposed(tmp_path)
    before = files(tmp_path)
    module = deepcopy(session._adapters["btc-ts"].module.checkpoint())
    metadata = deepcopy(session._meta)
    graphs = {key: deepcopy(graph.checkpoint()) for key, graph in session._graphs.items()}
    account = session.execution.snapshot().account
    try:
        assert (
            session.requires_immediate_protection(
                (bars(3)[0],), as_of_ms=240010, brackets=(tables()[0],)
            )
            is False
        )
        assert (
            session.requires_immediate_protection(
                (bars(3, "90")[0],), as_of_ms=240010, brackets=(tables()[0],)
            )
            is True
        )
        assert files(tmp_path) == before
        assert session._adapters["btc-ts"].module.checkpoint() == module
        assert session._meta == metadata
        assert {key: graph.checkpoint() for key, graph in session._graphs.items()} == graphs
        assert session.execution.snapshot().account == account
        provider.available = False
        external_run(session, provider, 3, "90")
        assert session.execution.snapshot().pending[0].original.proposal.reducing
    finally:
        session.close()


def test_existing_financial_pending_bypasses_wait_even_before_eligible_minute(
    tmp_path, monkeypatch
):
    provider = Provider(tmp_path)
    session = external_session(tmp_path, provider)
    session.resume("start")
    external_run(session, provider, 0)
    before = files(tmp_path)
    monkeypatch.setattr(
        session._adapters["btc-ts"],
        "preview_protection",
        lambda *a, **k: pytest.fail("settlement must bypass strategy preview"),
    )
    try:
        assert (
            session.requires_immediate_protection(
                (bars(1)[0],), as_of_ms=120010, brackets=(tables()[0],)
            )
            is True
        )
        assert files(tmp_path) == before
        external_run(session, provider, 1)
        assert not session.execution.snapshot().account.ledger
        external_run(session, provider, 2)
        assert len(session.execution.snapshot().account.ledger) == 1
    finally:
        session.close()


def test_supported_legacy_exposure_can_wait_but_missing_capability_cannot(tmp_path, monkeypatch):
    session = new(tmp_path)
    session.resume("start")
    run(session, 0)
    run(session, 1)
    before = files(tmp_path)
    try:
        assert (
            session.requires_immediate_protection(bars(2), as_of_ms=180000, brackets=tables())
            is False
        )
        assert files(tmp_path) == before
        monkeypatch.setattr(session._adapters["btc-ts"].module, "preview_protection", None)
        assert (
            session.requires_immediate_protection(bars(2), as_of_ms=180000, brackets=tables())
            is True
        )
    finally:
        session.close()


def test_same_account_tef_exposure_does_not_starve_nn_but_its_fsl_is_immediate(tmp_path):
    provider = Provider(tmp_path)
    account, strategies = configuration()
    strategies[0].update(
        module="nn_fixture", configuration={}, external_features=[provider.binding.document()]
    )
    strategies[1].update(module="tef", configuration={"fullStopLossClose": ".05"})

    class Mixed:
        def create(self, module, instrument, configuration):
            registry = NNRegistry() if module == "nn_fixture" else StrategyRegistry()
            return registry.create(module, instrument, configuration)

    session = MarginTradingSession(
        tmp_path,
        account,
        strategies,
        0,
        create=True,
        registry=Mixed(),
        external_feature_provider=provider,
        clock_ms=lambda: provider.clock,
    )
    try:
        session.resume("seed-account")
        marks = tuple(
            MarkSnapshot(instrument, bars(0)[0].close, 0, 0, "explicit_test_seed")
            for instrument in (BTC, ETH)
        )
        reservation = approve_order(
            session.execution.snapshot().account,
            "seed-eth",
            OrderProposal(Side.BUY, bars(0)[0].volume / 10),
            instrument=ETH,
            owner_id="eth-ts",
            as_of_ms=0,
            marks=marks,
            brackets=tables(),
            worst_execution_price=bars(0)[0].close,
            fee_rate=session.fee_rate,
        )
        session.execution.submit_intent(
            FrozenPendingOrder(reservation, 0), marks=marks, brackets=tables()
        )
        session.pause("pause-seeded-entry")
        session.run_once(bars(0), as_of_ms=60010, brackets=tables(), entries_allowed=False)
        assert not session.execution.snapshot().pending
        assert session.execution.snapshot().account.positions[1].quantity == 1
        before, checkpoint = files(tmp_path), session._adapters["eth-ts"].module.checkpoint()
        assert (
            session.requires_immediate_protection(bars(1), as_of_ms=120010, brackets=tables())
            is False
        )
        original = bars(1)[1]
        stopped = Candle(
            ETH,
            original.open_time_ms,
            original.close_time_ms,
            original.open,
            original.high,
            original.close - 5,
            original.close - 5,
            original.volume,
        )
        assert (
            session.requires_immediate_protection(
                (bars(1)[0], stopped), as_of_ms=120010, brackets=tables()
            )
            is True
        )
        assert (
            files(tmp_path) == before
            and session._adapters["eth-ts"].module.checkpoint() == checkpoint
        )
    finally:
        session.close()


@pytest.mark.parametrize(
    "failure",
    ["preview", "adapter", "financial", "shutdown", "pending_frame", "session", "wrong_bar"],
)
def test_uncertainty_or_preview_failure_cannot_defer_financial_work(tmp_path, monkeypatch, failure):
    session, provider = prepare_exposed(tmp_path)
    candle = bars(3)[0]
    if failure == "preview":

        def fail(*args, **kwargs):
            raise RuntimeError("independent protection unavailable")

        monkeypatch.setattr(session._adapters["btc-ts"], "preview_protection", fail)
    elif failure == "adapter":
        adapter = session._adapters["btc-ts"]
        state = adapter.snapshot()
        monkeypatch.setattr(adapter, "snapshot", lambda: replace(state, state="unknown"))
    elif failure == "financial":
        financial = session.execution.snapshot()
        monkeypatch.setattr(
            session.execution, "snapshot", lambda: replace(financial, state="unknown")
        )
    elif failure == "shutdown":
        shutdown = session.shutdown_status()
        monkeypatch.setattr(
            session, "shutdown_status", lambda: shutdown | {"recovery_required": True}
        )
    elif failure == "pending_frame":
        provider.clock = 240010
        candidate = deepcopy(session._meta)
        candidate["pending"] = session._frame((candle,), 240010, (tables()[0],), False)
        session._persist(candidate)
    elif failure == "session":
        session._state = "unknown"
    else:
        candle = bars(2)[0]
    before = files(tmp_path)
    try:
        assert (
            session.requires_immediate_protection(
                (candle,), as_of_ms=240010, brackets=(tables()[0],)
            )
            is True
        )
        assert files(tmp_path) == before
    finally:
        session.close()
