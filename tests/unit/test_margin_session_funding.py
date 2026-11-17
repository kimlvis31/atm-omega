"""Frozen session funding phases; real journals, explicit scenario rate inputs."""

from copy import deepcopy
from decimal import Decimal as D
from hashlib import sha256
from types import SimpleNamespace

import pytest
from test_margin_session import BTC, ETH, bars, configuration, tables
from test_margin_session_features import Provider, external_run, external_session

from omega.application.margin_funding import (
    FundingPoint,
    FundingSchedule,
    frozen_funding_document,
    restore_funding_batch,
)
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.margin import FillEvent, FundingCashflow, apply_fill
from omega.runtime import margin_session, margin_session_shutdown
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration
from omega.runtime.margin_session_funding import MarginSessionFunding


def schedule(*, first=0, rate=".001"):
    return FundingSchedule(
        tuple(
            FundingPoint(instrument, opened, opened + 10, opened, D(rate), D(100))
            for opened in (first, first + 120000)
            for instrument in (BTC, ETH)
        )
    ).document()


def create(root, *, create=True, funding=None, mode="cross"):
    account, strategies = configuration()
    for position in account["positions"]:
        position["margin_mode"] = mode
        if mode == "isolated":
            position["isolated_collateral"] = "100"
    return MarginTradingSession(
        root,
        account,
        strategies,
        0,
        create=create,
        funding=schedule() if funding is None else funding,
    )


def run(session, index, *, as_of_ms=None):
    return session.run_once(
        bars(index),
        as_of_ms=(index + 1) * 60000 if as_of_ms is None else as_of_ms,
        brackets=tables(),
    )


def facts(session):
    return tuple(
        entry.fact
        for entry in session.execution.snapshot().account.ledger
        if isinstance(entry.fact, FundingCashflow)
    )


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_zero_no_event_signed_settlement_and_original_nextbar_orders(tmp_path, mode):
    session = create(tmp_path, mode=mode)
    try:
        session.resume("start")
        first = run(session, 0)
        assert session._meta["schema_version"] == 3 and len(facts(session)) == 2
        assert all(fact.amount == 0 for fact in facts(session))
        assert len(first["pending_orders"]) == 2
        original = session.execution.snapshot().pending
        session.pause("pause-entries")
        run(session, 1)
        assert session._meta["last_frame"]["funding"]["plan"] is None
        assert len(facts(session)) == 2
        ledger = session.execution.snapshot().account.ledger
        assert {entry.fact.order_id for entry in ledger if hasattr(entry.fact, "order_id")} == {
            order.order_id for order in original
        }
        run(session, 2)
        assert [fact.amount for fact in facts(session)[-2:]] == [D("-.1"), D(".1")]
        assert all(fact.available_at_ms == 180000 for fact in facts(session)[-2:])
        assert all(fact.source_available_at_ms == 120010 for fact in facts(session)[-2:])
        account = session.execution.snapshot().account
        assert [position.quantity for position in account.positions] == [D(1), D(-1)]
        assert [position.realized_pnl for position in account.positions] == [0, 0]
        if mode == "cross":
            assert account.cross_wallet == D("999.8")
        else:
            assert account.cross_wallet == D(800)
            assert [position.isolated_wallet for position in account.positions] == [
                D("99.8"),
                D(100),
            ]
        assert session.snapshot()["funding"]["venue_account_payment"] is False
    finally:
        session.close()


def test_no_funding_external_schema2_bytes_match_pre_funding_record(tmp_path):
    provider = Provider(tmp_path)
    provider.available = False
    session = external_session(tmp_path, provider)
    try:
        session.resume("start")
        external_run(session, provider, 0)
        provider.available = True
        external_run(session, provider, 0)
        external_run(session, provider, 1)
        assert session._meta["schema_version"] == 2
        assert "funding" not in session._meta["last_frame"]
        # Captured from the earlier accepted lease-case real session file.
        assert (
            sha256(session.path.read_bytes()).hexdigest()
            == "8f4f658934a2a84ecd1c84883471abb0e3587aaa6b31b7d4dc7680f2454c8e88"
        )
    finally:
        session.close()


def test_funding_plus_external_features_share_one_frozen_frame_and_restart(tmp_path):
    provider = Provider(tmp_path)
    account, strategies = configuration()
    account["positions"] = account["positions"][:1]
    strategies = strategies[:1]
    strategies[0].update(
        module="nn_fixture", configuration={}, external_features=[provider.binding.document()]
    )
    from test_margin_session_features import NNRegistry

    funding = FundingSchedule(
        (FundingPoint(BTC, 120000, 120010, 120000, D(".001"), D(100)),)
    ).document()

    def session(create):
        return MarginTradingSession(
            tmp_path,
            account,
            strategies,
            0,
            create=create,
            funding=funding,
            registry=NNRegistry(),
            external_feature_provider=provider,
            clock_ms=lambda: provider.clock,
        )

    value = session(True)
    try:
        value.resume("start")
        for index in range(3):
            external_run(value, provider, index)
        # Funding at120000 precedes that minute's accepted entry fill: flat assessment.
        assert facts(value)[0].amount == 0 and facts(value)[0].signed_quantity == 0
        frame = value._meta["last_frame"]
        assert value._meta["schema_version"] == 3
        assert frame["external_features"]["btc-ts"][0]["pin"]["frame_ref"] == frame["digest"]
        saved = value.path.read_bytes()
    finally:
        value.close()
    restored = session(False)
    try:
        assert restored.path.read_bytes() == saved and len(facts(restored)) == 1
    finally:
        restored.close()


@pytest.mark.parametrize(
    "boundary",
    [
        "frozen",
        "source_ack",
        "first_prefix",
        "second_prefix",
        "phase_ack",
        "open_ack",
        "finish_ack",
    ],
)
def test_ack_loss_restarts_original_facts_cutoff_and_committed_lookup(
    tmp_path, monkeypatch, boundary
):
    session = create(tmp_path)
    session.resume("start")
    run(session, 0)
    run(session, 1)
    fired = []
    original_json = margin_session.atomic_json
    journal = session.execution._journal
    original_append = journal.append
    state_store = session.account_owner._store
    original_save = state_store.save

    def lose_meta(path, envelope):
        data = envelope.get("data", {})
        frame = data.get("pending")
        wanted = (
            frame is not None
            and frame["candles"][0]["open_time_ms"] == 120000
            and (
                boundary == "frozen"
                and frame["phase"] == "funding"
                or boundary == "phase_ack"
                and frame["phase"] == "open"
                or boundary == "open_ack"
                and frame["phase"] == "decisions"
            )
            or boundary == "finish_ack"
            and data.get("cursor_ms") == 120000
            and frame is None
        )
        original_json(path, envelope)
        if path == session.path and wanted and not fired:
            fired.append(True)
            raise OSError("original session frame ACK lost")

    def lose_source(record):
        receipt = original_append(record)
        if (
            boundary == "source_ack"
            and hasattr(record.payload, "settlement_time_ms")
            and record.payload.settlement_time_ms == 120000
            and not fired
        ):
            fired.append(True)
            raise StorageError("original funding source ACK lost")
        return receipt

    def lose_prefix(account, **kwargs):
        original_save(account, **kwargs)
        count = len(
            [
                entry
                for entry in account.ledger
                if isinstance(entry.fact, FundingCashflow) and entry.fact.event_time_ms == 120000
            ]
        )
        target = {"first_prefix": 1, "second_prefix": 2}.get(boundary)
        if target == count and not fired:
            fired.append(True)
            raise StorageError("original account funding prefix ACK lost")

    monkeypatch.setattr(margin_session, "atomic_json", lose_meta)
    monkeypatch.setattr(journal, "append", lose_source)
    monkeypatch.setattr(state_store, "save", lose_prefix)
    try:
        with pytest.raises(StorageError):
            run(session, 2)
        assert fired
    finally:
        session.close()
    monkeypatch.setattr(margin_session, "atomic_json", original_json)
    restored = create(tmp_path, create=False)
    try:
        if restored.snapshot()["pending_frame"]:
            restored.run_once((), as_of_ms=180999, entries_allowed=False)
        assert restored.snapshot()["cursor_ms"] == 120000
        assert len(facts(restored)) == 4
        assert len({fact.funding_id for fact in facts(restored)}) == 4
        assert [fact.available_at_ms for fact in facts(restored)[-2:]] == [180000, 180000]
        before = restored.path.read_bytes()
        run(restored, 2, as_of_ms=181000)
        assert restored.path.read_bytes() == before
    finally:
        restored.close()


@pytest.mark.parametrize("change", ["future", "other_instrument", "before_start", "duplicate"])
def test_unsupported_schedule_is_rejected_before_financial_files(tmp_path, change):
    selected = schedule()
    if change == "future":
        selected["points"][0]["source_available_at_ms"] = 60001
    elif change == "other_instrument":
        selected["points"][0]["instrument"] = "binance:usdm:SOLUSDT"
    elif change == "duplicate":
        selected["points"].insert(1, deepcopy(selected["points"][0]))
    account, strategies = configuration()
    with pytest.raises(ValidationError):
        prepare_margin_configuration(
            account, strategies, 60000 if change == "before_start" else 0, funding=selected
        )
    assert not (tmp_path / "state").exists()


def test_helper_committed_lookup_precedes_baseline_restoration_without_files():
    from test_margin_funding import account, point

    initial = account()
    helper = MarginSessionFunding(FundingSchedule((point(),)), initial, 0)
    candle = bars(1)[0]
    header = helper.freeze(
        (candle,), as_of_ms=120000, financial=SimpleNamespace(state="ready", account=initial)
    )

    class Execution:
        def __init__(self):
            self.committed, self.current, self.restores = (), initial, 0

        def reconcile(self):
            pass

        def committed_funding_batches(self, _):
            return self.committed

        def restore_prepared_funding(self, document):
            self.restores += 1
            return restore_funding_batch(document, self.current)

        def commit_funding(self, plan):
            self.committed, self.current = (plan,), plan.candidate_account

    execution = Execution()
    frame = {"funding": header}
    helper.settle(frame, execution)
    execution.current = apply_fill(
        execution.current,
        FillEvent("close", "close-order", BTC, "s", D(-2), D(100), D(0), "USDT", 180000, 180001),
    ).account
    helper.settle(frame, execution)
    assert (
        execution.restores == 1
        and frozen_funding_document(execution.committed[0]) == header["plan"]
    )


def test_mutated_frozen_scenario_is_not_recalculated_as_current_position(tmp_path):
    session = create(tmp_path)
    try:
        frame = session._frame(bars(0), 60000, tables(), False)
        frame["funding"]["plan"]["facts"][0]["amount"] = "1"
        frame["digest"] = session._frame_digest(frame)
        with pytest.raises((ProtocolError, ValidationError)):
            session._validate_frame(frame, 0)
        assert not session.execution.snapshot().account.ledger
    finally:
        session.close()


def drain_schedule():
    return FundingSchedule(
        tuple(
            FundingPoint(item, opening, opening + 10, opening, rate, D(100))
            for opening in (120000, 180000)
            for item, rate in ((BTC, D(".001")), (ETH, D(".002")))
        )
    ).document()


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_shutdown_funding_precedes_next_eligible_reduce_without_models(tmp_path, mode):
    from test_margin_session_shutdown import BrokenModel

    session = create(tmp_path, mode=mode, funding=drain_schedule())
    try:
        session.resume("start")
        run(session, 0)
        session.pause("pause")
        run(session, 1)
        original = session.path.read_bytes()
        session._adapters = {key: BrokenModel() for key in session._adapters}
        session._graphs = {key: BrokenModel() for key in session._graphs}
        status = session.shutdown_drain("exit", 120100, brackets=tables())
        assert status["needs_settlement"]
        accepted = session.execution.snapshot().pending
        session.shutdown_cancel("exit")
        assert session.execution.snapshot().pending == accepted
        first = session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
        assert first["needs_settlement"] and first["state"] == "canceled"
        assert [fact.amount for fact in facts(session)] == [D("-.1"), D(".2")]
        assert [p.quantity for p in session.execution.snapshot().account.positions] == [1, -1]
        last = session.shutdown_run_once(bars(3), as_of_ms=240000, brackets=tables())
        assert last["state"] == "complete" and not last["needs_settlement"]
        account = session.execution.snapshot().account
        assert [p.quantity for p in account.positions] == [0, 0]
        assert [fact.amount for fact in facts(session)] == [D("-.1"), D(".2")] * 2
        assert sum(p.isolated_wallet for p in account.positions) + account.cross_wallet == D(
            "999.8"
        )
        assert session.path.read_bytes() == original
        assert session._meta["cursor_ms"] == 60000
    finally:
        session.close()
    restored = create(tmp_path, create=False, mode=mode, funding=drain_schedule())
    try:
        assert restored.shutdown_status()["state"] == "complete"
        assert len(facts(restored)) == 4
    finally:
        restored.close()


@pytest.mark.parametrize("boundary", ["frozen", "source", "prefix", "open", "finish"])
def test_shutdown_funding_ack_loss_keeps_original_assessment_and_cursor(
    tmp_path, monkeypatch, boundary
):
    session = create(tmp_path, funding=drain_schedule())
    session.resume("start")
    run(session, 0)
    session.pause("pause")
    run(session, 1)
    session.shutdown_drain("exit", 120000, brackets=tables())
    fired = []
    original_json = margin_session_shutdown.atomic_json
    journal, store = session.execution._journal, session.account_owner._store
    original_append, original_save = journal.append, store.save

    def lose_meta(path, envelope):
        original_json(path, envelope)
        data = envelope["data"]
        if (
            path == session._shutdown.path
            and not fired
            and (
                boundary == "frozen"
                and data["pending_frame"] is not None
                or boundary == "finish"
                and data["cursor_ms"] == 120000
            )
        ):
            fired.append(True)
            raise OSError("shutdown funding metadata ACK lost")

    def lose_source(record):
        receipt = original_append(record)
        if not fired and (
            boundary == "source"
            and hasattr(record.payload, "settlement_time_ms")
            or boundary == "open"
            and getattr(record.payload, "phase", None) == "open"
        ):
            fired.append(True)
            raise StorageError("shutdown financial source ACK lost")
        return receipt

    def lose_prefix(account, **kwargs):
        original_save(account, **kwargs)
        if (
            boundary == "prefix"
            and not fired
            and any(isinstance(entry.fact, FundingCashflow) for entry in account.ledger)
        ):
            fired.append(True)
            raise StorageError("shutdown funding prefix ACK lost")

    monkeypatch.setattr(margin_session_shutdown, "atomic_json", lose_meta)
    monkeypatch.setattr(journal, "append", lose_source)
    monkeypatch.setattr(store, "save", lose_prefix)
    try:
        with pytest.raises(StorageError):
            session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
        assert fired
    finally:
        session.close()
    monkeypatch.setattr(margin_session_shutdown, "atomic_json", original_json)
    restored = create(tmp_path, create=False, funding=drain_schedule())
    try:
        if restored.shutdown_status()["state"] != "complete":
            restored.shutdown_run_once((), as_of_ms=180999, brackets=tables())
        assert restored.shutdown_status()["state"] == "complete"
        assert len(facts(restored)) == 2
        assert [fact.signed_quantity for fact in facts(restored)] == [1, -1]
        assert [fact.available_at_ms for fact in facts(restored)] == [180000, 180000]
        assert all(p.quantity == 0 for p in restored.execution.snapshot().account.positions)
        original = restored._shutdown.path.read_bytes()
        restored.shutdown_run_once(bars(2), as_of_ms=181000)
        assert restored._shutdown.path.read_bytes() == original
    finally:
        restored.close()


def test_shutdown_cancels_reserved_entry_without_synthetic_future_funding(tmp_path):
    session = create(tmp_path, funding=drain_schedule())
    try:
        session.resume("start")
        run(session, 0)
        assert len(session.execution.snapshot().pending) == 2
        assert session.shutdown_drain("exit", 60001, brackets=tables())["state"] == "complete"
        assert not session.execution.snapshot().pending and not facts(session)
        assert all(p.quantity == 0 for p in session.execution.snapshot().account.positions)
    finally:
        session.close()


def test_zero_assessment_restart_after_open_fill_never_uses_new_quantity(tmp_path, monkeypatch):
    funding = FundingSchedule(
        tuple(FundingPoint(item, 60000, 60010, 60000, D(".001"), D(100)) for item in (BTC, ETH))
    ).document()
    session = create(tmp_path, funding=funding)
    session.resume("start")
    run(session, 0)
    original_json = margin_session.atomic_json
    fired = []

    def lose_open_ack(path, envelope):
        original_json(path, envelope)
        pending = envelope["data"].get("pending")
        if path == session.path and pending is not None and pending["phase"] == "decisions":
            fired.append(True)
            raise OSError("open phase metadata ACK lost after original fills")

    monkeypatch.setattr(margin_session, "atomic_json", lose_open_ack)
    try:
        with pytest.raises(StorageError):
            run(session, 1)
        assert fired
    finally:
        session.close()
    monkeypatch.setattr(margin_session, "atomic_json", original_json)
    restored = create(tmp_path, create=False, funding=funding)
    try:
        assert [p.quantity for p in restored.execution.snapshot().account.positions] == [1, -1]
        restored.run_once((), as_of_ms=120999, entries_allowed=False)
        assert len(facts(restored)) == 2
        assert all(fact.amount == fact.signed_quantity == 0 for fact in facts(restored))
        assert all(fact.available_at_ms == 120000 for fact in facts(restored))
    finally:
        restored.close()


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_funding_debt_stays_recorded_without_implicit_liquidation_or_topup(tmp_path, mode):
    funding = FundingSchedule(
        (FundingPoint(BTC, 120000, 120010, 120000, D(1), D(2000)),)
    ).document()
    session = create(tmp_path, mode=mode, funding=funding)
    try:
        session.resume("start")
        run(session, 0)
        session.pause("pause")
        run(session, 1)
        run(session, 2)
        account = session.execution.snapshot().account
        assert [fact.amount for fact in facts(session)] == [D(-2000)]
        assert [p.quantity for p in account.positions] == [1, -1]
        assert [p.realized_pnl for p in account.positions] == [0, 0]
        assert not session.execution.snapshot().pending
        if mode == "cross":
            assert account.cross_wallet == D("-1000.2")
        else:
            assert account.cross_wallet == D(800)
            assert account.positions[0].isolated_wallet == D("-1900.1")
    finally:
        session.close()


def test_shutdown_funding_unknown_original_plan_is_explicitly_blocked(tmp_path):
    session = create(tmp_path, funding=drain_schedule())
    try:
        session.resume("start")
        run(session, 0)
        run(session, 1)
        session.shutdown_drain("exit", 120100, brackets=tables())
        session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
        before = session.execution.snapshot().account
        session._shutdown.data["last_frame"]["funding"]["plan"]["checksum"] = "a" * 64
        result = session.shutdown_preflight("shutdown-inspection", 180100, brackets=tables())
        assert not result["ready"] and "funding" in result["reason"]
        assert session.shutdown_status()["state"] == "unknown"
        assert session.execution.snapshot().account == before
    finally:
        session.close()
