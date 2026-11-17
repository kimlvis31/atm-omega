"""File-owned opt-in costs preserve frozen frames, funding and protective drains."""

from decimal import Decimal as D

import pytest
from test_margin_session import BTC, ETH, bars, configuration, tables
from test_margin_session_shutdown import BrokenModel
from test_margin_strategy_costs import policy

from omega.application.margin_cost import FrozenCostPendingOrder, MarginCostPolicy
from omega.application.margin_funding import FundingPoint, FundingSchedule
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.margin import FillEvent, FundingCashflow
from omega.runtime import margin_session, margin_session_shutdown
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration


def create(root, *, selected=None, mode="cross", funding=None, create=True, single=False):
    account, rows = configuration()
    if single:
        account["positions"], rows = account["positions"][:1], rows[:1]
    for position in account["positions"]:
        position["margin_mode"] = mode
        if mode == "isolated":
            position["isolated_collateral"] = "100"
    return MarginTradingSession(
        root,
        account,
        rows,
        0,
        create=create,
        cost_policy=policy(entry_delay_ms=0).document() if selected is None else selected,
        **({"funding": funding} if funding is not None else {}),
    )


def run(session, index, price="100", *, as_of_ms=None):
    return session.run_once(
        tuple(
            bar
            for bar in bars(index, price)
            if bar.instrument in {p.instrument for p in session.instruments}
        ),
        as_of_ms=(index + 1) * 60000 if as_of_ms is None else as_of_ms,
        brackets=tables(),
    )


def fills(session):
    return tuple(
        entry.fact
        for entry in session.execution.snapshot().account.ledger
        if isinstance(entry.fact, FillEvent)
    )


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_cost_session_favorable_gap_uses_original_buffer_and_exact_final_fee(tmp_path, mode):
    session = create(tmp_path, mode=mode, single=True)
    try:
        session.resume("start")
        run(session, 0, "101")
        original = session.execution.snapshot().pending[0]
        assert type(original) is FrozenCostPendingOrder
        assert original.original.envelope.anchor.price == 101
        assert session._meta["schema_version"] == 4
        run(session, 1, "100")
        assert len(fills(session)) == 1
        assert fills(session)[0].price == D("100.1")
        assert fills(session)[0].fee == D(".1001")
        assert session.execution.snapshot().account.positions[0].entry_price == D("100.1")
        assert (
            session._meta["last_frame"]["cost_policy"]["policy_sha256"]
            == policy(entry_delay_ms=0).sha256
        )
        assert margin_session._candle(session._meta["last_frame"]["candles"][0]).close == 100
    finally:
        session.close()


def test_delayed_entry_remains_pending_at_end_of_available_input(tmp_path):
    selected = policy(entry_delay_ms=1)
    session = create(tmp_path, selected=selected)
    try:
        session.resume("start")
        run(session, 0)
        original = session.execution.snapshot().pending
        assert len(original) == 2 and all(item.eligible_at_ms == 120000 for item in original)
        run(session, 1)
        assert not fills(session)
        assert session.execution.snapshot().pending == original
        assert all(
            position.quantity == 0 for position in session.execution.snapshot().account.positions
        )
    finally:
        session.close()
    restored = create(tmp_path, create=False, selected=selected)
    try:
        assert restored.execution.snapshot().pending == original
        run(restored, 2)
        assert [fact.price for fact in fills(restored)] == [D("100.1"), D("99.9")]
    finally:
        restored.close()


def test_raw_policy_is_preserved_separately_from_canonical_identity(tmp_path):
    raw = policy(entry_delay_ms=0).document()
    raw["slippage_rate"] = "0.0010"
    session = create(tmp_path, selected=raw)
    try:
        assert session._meta["configuration"]["cost_policy"] == raw
        assert (
            session.snapshot()["cost_policy"]["policy_sha256"] == MarginCostPolicy.parse(raw).sha256
        )
    finally:
        session.close()
    with pytest.raises(ProtocolError, match="configuration"):
        create(tmp_path, create=False, selected=policy(entry_delay_ms=0).document())


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_manual_and_module_resume_keep_accepted_cost_intent_and_delayed_eligibility(tmp_path, mode):
    session = create(tmp_path, selected=policy(entry_delay_ms=60001), mode=mode)
    try:
        session.resume("start")
        run(session, 0)
        original = session.execution.snapshot().pending
        assert len(original) == 2 and all(item.eligible_at_ms == 180000 for item in original)
        session.resume_strategy("module-resume", "btc-ts")
        assert session.execution.snapshot().paused
        assert session.execution.snapshot().pending == original
        session.resume("account-resume")
        assert session.execution.snapshot().pending == original
        assert not session.execution.snapshot().paused
    finally:
        session.close()


def test_frozen_cost_identity_cannot_be_relabelled_even_with_new_frame_digest(tmp_path):
    session = create(tmp_path)
    try:
        frame = session._frame(bars(0), 60000, tables(), False)
        frame["cost_policy"]["policy_sha256"] = "a" * 64
        frame["digest"] = session._frame_digest(frame)
        with pytest.raises(ProtocolError, match="cost policy"):
            session._validate_frame(frame, 0)
        assert not session.execution.snapshot().account.ledger
        assert not session.execution.snapshot().pending
    finally:
        session.close()


@pytest.mark.parametrize("fault", ["missing", "float_delay", "bool_delay", "nan", "unknown"])
def test_cost_configuration_rejected_before_any_financial_files(tmp_path, fault):
    raw = policy().document()
    if fault == "missing":
        raw.pop("entry_delay_ms")
    elif fault == "float_delay":
        raw["entry_delay_ms"] = 1.0
    elif fault == "bool_delay":
        raw["entry_delay_ms"] = True
    elif fault == "nan":
        raw["slippage_rate"] = "NaN"
    else:
        raw["unexpected"] = "private"
    account, rows = configuration()
    with pytest.raises(ValidationError):
        prepare_margin_configuration(account, rows, 0, cost_policy=raw)
    assert not (tmp_path / "state").exists()


def funding_schedule():
    return FundingSchedule(
        tuple(
            FundingPoint(item, opening, opening + 10, opening, D(".001"), D(100))
            for opening in (120000, 180000, 240000)
            for item in (BTC, ETH)
        )
    ).document()


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_costed_shutdown_keeps_delayed_reductions_and_funding_without_models(tmp_path, mode):
    selected, funding = policy(entry_delay_ms=0, reduction_delay_ms=60000), funding_schedule()
    session = create(tmp_path, selected=selected, funding=funding, mode=mode)
    try:
        session.resume("start")
        run(session, 0)
        session.pause("pause")
        run(session, 1)
        original_meta = session.path.read_bytes()
        session._adapters = {key: BrokenModel() for key in session._adapters}
        session._graphs = {key: BrokenModel() for key in session._graphs}
        session.shutdown_drain("first", 120100, brackets=tables())
        original = session.execution.snapshot().pending
        assert session._shutdown.data["schema_version"] == 2
        assert all(
            item.original.as_of_ms == 120100 and item.eligible_at_ms == 240000 for item in original
        )
        session.shutdown_cancel("first")
        session.shutdown_drain("second", 180000, brackets=tables())
        assert session.execution.snapshot().pending == original
        run_before = len(fills(session))
        for index in (2, 3):
            state = session.shutdown_run_once(
                bars(index), as_of_ms=(index + 1) * 60000, brackets=tables()
            )
            assert state["needs_settlement"] and len(fills(session)) == run_before
        state = session.shutdown_run_once(bars(4), as_of_ms=300000, brackets=tables())
        assert state["state"] == "complete"
        assert [fact.price for fact in fills(session)] == [
            D("100.1"),
            D("99.9"),
            D("99.9"),
            D("100.1"),
        ]
        final = session.execution.snapshot().account
        assert all(position.quantity == 0 for position in final.positions)
        assert final.cross_wallet + sum(
            position.isolated_wallet for position in final.positions
        ) == D("999.2")
        funding_facts = tuple(
            entry.fact for entry in final.ledger if isinstance(entry.fact, FundingCashflow)
        )
        assert (
            len(funding_facts) == 6
            and [fact.signed_quantity for fact in funding_facts] == [1, -1] * 3
        )
        assert session.path.read_bytes() == original_meta
        summary = session.cost_quote_summaries(limit=1)
        assert summary["state"] == "ready" and summary["total"] == 4
        assert summary["truncated"] is True and len(summary["rows"]) == 1
        summary["rows"].clear()
        assert len(session.cost_quote_summaries(limit=1)["rows"]) == 1
    finally:
        session.close()
    restored = create(tmp_path, create=False, selected=selected, funding=funding, mode=mode)
    try:
        assert restored.shutdown_status()["state"] == "complete"
        assert len(fills(restored)) == 4
    finally:
        restored.close()


@pytest.mark.parametrize("settled", [False, True])
def test_cost_shutdown_missing_metadata_detects_original_issue_not_eligibility(tmp_path, settled):
    selected = policy(entry_delay_ms=0, reduction_delay_ms=1)
    session = create(tmp_path, selected=selected)
    session.resume("start")
    run(session, 0)
    run(session, 1)
    session.shutdown_drain("exit", 120000, brackets=tables())
    if settled:
        for index in (2, 3):
            session.shutdown_run_once(bars(index), as_of_ms=(index + 1) * 60000, brackets=tables())
    path = session._shutdown.path
    session.close()
    path.unlink()
    with pytest.raises(StorageError, match="metadata is missing"):
        create(tmp_path, create=False, selected=selected)


@pytest.mark.parametrize("boundary", ["frame", "funding", "open", "prefix", "finish"])
def test_cost_frame_ack_loss_restarts_exact_original_prices_time_and_funding(
    tmp_path, monkeypatch, boundary
):
    selected = policy(entry_delay_ms=1)
    funding = funding_schedule()
    session = create(tmp_path, selected=selected, funding=funding)
    session.resume("start")
    run(session, 0)
    run(session, 1)
    fired = []
    original_json = margin_session.atomic_json
    original_append = session.execution._journal.append
    original_save = session.account_owner._store.save

    def lose_meta(path, envelope):
        original_json(path, envelope)
        data = envelope["data"]
        frame = data.get("pending")
        if (
            path == session.path
            and not fired
            and (
                boundary == "frame"
                and frame is not None
                and frame["phase"] == "funding"
                or boundary == "finish"
                and data["cursor_ms"] == 120000
                and frame is None
            )
        ):
            fired.append(True)
            raise OSError("cost frame metadata ACK lost")

    def lose_source(record):
        result = original_append(record)
        if not fired and (
            boundary == "funding"
            and hasattr(record.payload, "settlement_time_ms")
            or boundary == "open"
            and getattr(record.payload, "phase", None) == "open"
        ):
            fired.append(True)
            raise StorageError("cost source ACK lost")
        return result

    def lose_prefix(account, **kwargs):
        original_save(account, **kwargs)
        if (
            boundary == "prefix"
            and not fired
            and any(isinstance(row.fact, FillEvent) for row in account.ledger)
        ):
            fired.append(True)
            raise StorageError("cost account prefix ACK lost")

    monkeypatch.setattr(margin_session, "atomic_json", lose_meta)
    monkeypatch.setattr(session.execution._journal, "append", lose_source)
    monkeypatch.setattr(session.account_owner._store, "save", lose_prefix)
    try:
        with pytest.raises(StorageError):
            run(session, 2)
        assert fired
    finally:
        session.close()
    monkeypatch.setattr(margin_session, "atomic_json", original_json)
    restored = create(tmp_path, create=False, selected=selected, funding=funding)
    try:
        if restored.snapshot()["pending_frame"]:
            restored.run_once((), as_of_ms=181000, entries_allowed=False)
        assert [fact.price for fact in fills(restored)] == [D("100.1"), D("99.9")]
        assert [fact.available_at_ms for fact in fills(restored)] == [180000, 180000]
        source = [
            entry.fact
            for entry in restored.execution.snapshot().account.ledger
            if isinstance(entry.fact, FundingCashflow)
        ]
        assert len(source) == 2 and all(fact.signed_quantity == fact.amount == 0 for fact in source)
        original = restored.path.read_bytes()
        run(restored, 2, as_of_ms=182000)
        assert restored.path.read_bytes() == original
    finally:
        restored.close()


@pytest.mark.parametrize("boundary", ["metadata", "intent", "funding", "batch", "prefix"])
def test_cost_shutdown_ack_loss_preserves_submitted_delay_and_exact_quote(
    tmp_path, monkeypatch, boundary
):
    selected = policy(entry_delay_ms=0, reduction_delay_ms=1)
    funding = funding_schedule()
    session = create(tmp_path, selected=selected, funding=funding)
    session.resume("start")
    run(session, 0)
    session.pause("pause")
    run(session, 1)
    original_json = margin_session_shutdown.atomic_json
    original_append = session.execution._journal.append
    original_save = session.account_owner._store.save
    fired = []

    def lose_meta(path, envelope):
        original_json(path, envelope)
        if path == session._shutdown.path and boundary == "metadata" and not fired:
            fired.append(True)
            raise OSError("cost shutdown preparation ACK lost")

    def lose_source(record):
        result = original_append(record)
        if not fired and (
            boundary == "intent"
            and type(record.payload) is FrozenCostPendingOrder
            or boundary == "funding"
            and hasattr(record.payload, "settlement_time_ms")
            or boundary == "batch"
            and getattr(record.payload, "phase", None) == "open"
        ):
            fired.append(True)
            raise StorageError("original cost shutdown source ACK lost")
        return result

    def lose_prefix(account, **kwargs):
        original_save(account, **kwargs)
        if (
            boundary == "prefix"
            and not fired
            and len([row for row in account.ledger if isinstance(row.fact, FillEvent)]) > 2
        ):
            fired.append(True)
            raise StorageError("cost reduction account prefix ACK lost")

    monkeypatch.setattr(margin_session_shutdown, "atomic_json", lose_meta)
    monkeypatch.setattr(session.execution._journal, "append", lose_source)
    monkeypatch.setattr(session.account_owner._store, "save", lose_prefix)
    try:
        with pytest.raises(StorageError):
            session.shutdown_drain("exit", 120000, brackets=tables())
            for index in (2, 3):
                session.shutdown_run_once(
                    bars(index), as_of_ms=(index + 1) * 60000, brackets=tables()
                )
        assert fired
    finally:
        session.close()
    monkeypatch.setattr(margin_session_shutdown, "atomic_json", original_json)
    restored = create(tmp_path, create=False, selected=selected, funding=funding)
    try:
        restored.reconcile()
        restored.shutdown_drain("exit", 120000, brackets=tables())
        while restored.shutdown_status()["state"] != "complete":
            index = restored.shutdown_status()["next_open_ms"] // 60000
            assert index <= 3
            pending = restored._shutdown.data["pending_frame"]
            restored.shutdown_run_once(
                () if pending else bars(index),
                as_of_ms=max((index + 1) * 60000, 241000),
                brackets=tables(),
            )
        assert [fact.price for fact in fills(restored)] == [
            D("100.1"),
            D("99.9"),
            D("99.9"),
            D("100.1"),
        ]
        assert all(
            position.quantity == 0 for position in restored.execution.snapshot().account.positions
        )
        intents = [
            row.record.payload
            for row in restored.execution._history
            if type(row.record.payload) is FrozenCostPendingOrder
            and row.record.payload.original.proposal.reducing
        ]
        assert len(intents) == 2
        assert all(
            item.original.as_of_ms == 120000 and item.eligible_at_ms == 180000 for item in intents
        )
    finally:
        restored.close()


def test_cost_funding_nn_pins_use_one_schema4_frame_and_restore_without_provider_reads(tmp_path):
    from test_margin_session_features import (
        NNRegistry,
        Provider,
        external_configuration,
        external_run,
    )

    provider = Provider(tmp_path)
    account, rows = external_configuration(provider)
    selected = policy(entry_delay_ms=0)
    funding = FundingSchedule(
        (FundingPoint(BTC, 120000, 120010, 120000, D(".001"), D(100)),)
    ).document()

    def build(create, source):
        return MarginTradingSession(
            tmp_path,
            account,
            rows,
            0,
            create=create,
            registry=NNRegistry(),
            cost_policy=selected,
            funding=funding,
            external_feature_provider=source,
            clock_ms=lambda: provider.clock,
        )

    session = build(True, provider)
    try:
        session.resume("start")
        external_run(session, provider, 0)
        assert provider.guard_calls == 1
        provider.available = False
        external_run(session, provider, 1)
        external_run(session, provider, 2)
        assert len(fills(session)) == 1 and fills(session)[0].price == D("100.1")
        assert provider.guard_calls == 1
        frame = session._meta["last_frame"]
        assert session._meta["schema_version"] == 4
        assert {"funding", "external_features", "cost_policy"} <= frame.keys()
        original = session.path.read_bytes()
        lookups = provider.lookups
    finally:
        session.close()
    restored = build(False, None)
    try:
        assert restored.path.read_bytes() == original
        assert provider.lookups == lookups
        assert restored.snapshot()["new_risk_ready"] is False
    finally:
        restored.close()
