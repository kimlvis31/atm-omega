"""Real financial journals: TS-independent, causal and restart-safe margin drain."""

from decimal import Decimal as D
from threading import Thread

import pytest

from omega.application.virtual_margin import CancelIntent
from omega.domain.errors import StorageError, TradingBlocked
from omega.domain.margin import MarginBracket, MarginBracketSnapshot
from omega.domain.models import Candle, Instrument, Market
from omega.runtime import margin_session_shutdown as module
from omega.runtime.margin_session import MarginTradingSession

INSTRUMENTS = (
    Instrument("binance", Market.USDM, "BTCUSDT"),
    Instrument("binance", Market.USDM, "ETHUSDT"),
)


def configuration(mode="cross"):
    account = {
        "account_id": "shutdown-session",
        "balance": "1000",
        "positions": [
            {
                "instrument": item.key,
                "owner_id": str(i),
                "margin_mode": mode,
                "leverage": 10,
                "budget_margin": "100",
                **({"isolated_collateral": "100"} if mode == "isolated" else {}),
            }
            for i, item in enumerate(INSTRUMENTS)
        ],
    }
    strategies = [
        {
            "id": str(i),
            "instrument": item.key,
            "module": "target_quantity",
            "configuration": {"quantity": "1" if i == 0 else "-1"},
            "analysis": {},
            "constraints": {"leverage": "10", "can_short": True},
        }
        for i, item in enumerate(INSTRUMENTS)
    ]
    return account, strategies


def tables():
    return tuple(
        MarginBracketSnapshot(
            item,
            (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
            0,
            0,
            "synthetic_test_reference",
            "a" * 64,
        )
        for item in INSTRUMENTS
    )


def bars(index, price="100"):
    return tuple(
        Candle(item, index * 60000, (index + 1) * 60000, *([D(price)] * 4), D(10))
        for item in INSTRUMENTS
    )


def create(root, mode="cross", *, opened=True, create=True):
    account, strategies = configuration(mode)
    session = MarginTradingSession(root, account, strategies, 0, create=create)
    if create:
        session.resume("start")
        session.run_once(bars(0), as_of_ms=60000, brackets=tables())
        if opened:
            session.pause("pause")
            session.run_once(bars(1), as_of_ms=120000, brackets=tables())
    return session


def facts(session):
    return session.execution.snapshot().account.ledger


class BrokenModel:
    def __getattr__(self, name):
        raise AssertionError("financial-only shutdown touched a model")


@pytest.mark.parametrize("mode", ["cross", "isolated"])
def test_multisymbol_long_short_finance_only_waits_for_eligible_minute(tmp_path, mode):
    session = create(tmp_path, mode)
    try:
        metadata = session.path.read_bytes()
        session._adapters = {key: BrokenModel() for key in session._adapters}
        session._graphs = {key: BrokenModel() for key in session._graphs}
        assert session.shutdown_preflight("exit", 120100, brackets=tables())["ready"]
        before = len(facts(session))
        status = session.shutdown_drain("exit", 120100, brackets=tables())
        assert status["needs_settlement"] and status["next_open_ms"] == 120000
        assert len(facts(session)) == before
        pending = session.execution.snapshot().pending
        assert len(pending) == 2 and all(p.original.proposal.reducing for p in pending)
        assert all(p.eligible_at_ms == 120100 for p in pending)
        assert session.snapshot()["state"] == "shutdown_recovery_required"
        assert session.shutdown_preflight("shutdown-inspection", 999999)["ready"]
        session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
        assert len(facts(session)) == before and session.shutdown_status()["needs_settlement"]
        result = session.shutdown_run_once(bars(3, "90"), as_of_ms=240000, brackets=tables())
        assert result["state"] == "complete" and not result["needs_settlement"]
        financial = session.execution.snapshot()
        assert len(facts(session)) == before + 2
        assert all(position.quantity == 0 for position in financial.account.positions)
        assert not financial.pending and financial.paused
        cash = financial.account.cross_wallet + sum(
            position.isolated_wallet for position in financial.account.positions
        )
        assert cash == D("999.62")  # Opposite signed PnL cancels; only actual fees remain.
        assert session.path.read_bytes() == metadata
        assert session.snapshot()["cursor_ms"] == 60000
        with pytest.raises(TradingBlocked, match="preparation"):
            session.resume("bad-resume")
    finally:
        session.close()


def test_cancels_existing_entry_without_fabricating_fill(tmp_path):
    session = create(tmp_path, opened=False)
    try:
        original = session.execution.snapshot().pending
        assert len(original) == 2 and not facts(session)
        assert session.shutdown_drain("exit", 60001, brackets=tables())["state"] == "complete"
        assert not session.execution.snapshot().pending and not facts(session)
        journal = session.execution._history
        assert {
            receipt.record.payload.order_id
            for receipt in journal
            if isinstance(receipt.record.payload, CancelIntent)
        } == {p.order_id for p in original}
    finally:
        session.close()


def test_cancel_and_new_exit_adopt_original_orders_without_resubmission(tmp_path):
    session = create(tmp_path)
    try:
        session.shutdown_drain("first", 120100, brackets=tables())
        original = session.execution.snapshot().pending
        records = len(session.execution._history)
        assert session.shutdown_cancel("first")["needs_settlement"]
        assert session.execution.snapshot().pending == original
        assert session.shutdown_preflight("second", 500000, brackets=())["ready"]
        adopted = session.shutdown_drain("second", 500000, brackets=())
        assert adopted["operation_id"] == "second"
        assert len(session.execution._history) == records
        assert session.execution.snapshot().pending == original
        session.shutdown_run_once(bars(2), as_of_ms=500000, brackets=tables())
        session.shutdown_run_once(bars(3), as_of_ms=500000, brackets=tables())
        assert session.shutdown_status()["state"] == "complete"
    finally:
        session.close()


def test_cancel_preserves_reductions_and_continues_settlement(tmp_path):
    session = create(tmp_path)
    try:
        session.shutdown_drain("exit", 120000, brackets=tables())
        pending = session.execution.snapshot().pending
        assert session.shutdown_cancel("exit")["state"] == "canceled"
        assert session.execution.snapshot().pending == pending
        assert (
            session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())["state"]
            == "complete"
        )
        assert all(
            position.quantity == 0 for position in session.execution.snapshot().account.positions
        )
    finally:
        session.close()


@pytest.mark.parametrize("stage", ["intent", "settlement"])
def test_real_ack_loss_blocks_until_explicit_reconcile_then_no_duplicate(
    tmp_path, monkeypatch, stage
):
    session = create(tmp_path)
    original = session.execution._journal.append
    fired = []

    def append(record):
        receipt = original(record)
        payload = record.payload
        hit = hasattr(payload, "original") if stage == "intent" else hasattr(payload, "facts")
        if hit and not fired:
            fired.append(record)
            raise StorageError("synthetic durable journal ACK loss")
        return receipt

    try:
        if stage == "settlement":
            session.shutdown_drain("exit", 120000, brackets=tables())
        monkeypatch.setattr(session.execution._journal, "append", append)
        with pytest.raises(StorageError):
            if stage == "intent":
                session.shutdown_drain("exit", 120000, brackets=tables())
            else:
                session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
        assert session.shutdown_status()["state"] == "unknown"
        assert not session.shutdown_preflight("shutdown-inspection", 120001)["ready"]
        session.reconcile()
        if stage == "intent":
            session.shutdown_drain("exit", 120000, brackets=tables())
        session.shutdown_run_once(bars(2), as_of_ms=180001, brackets=tables())
        assert len(facts(session)) == 4 and not session.execution.snapshot().pending
    finally:
        session.close()


@pytest.mark.parametrize("commit_before_failure", [False, True])
def test_shutdown_metadata_ack_failure_never_replays_automatically(
    tmp_path, monkeypatch, commit_before_failure
):
    session = create(tmp_path)
    original = module.atomic_json
    fired = []

    def save(path, data):
        if not fired:
            fired.append(True)
            if commit_before_failure:
                original(path, data)
            raise OSError("synthetic fsync failure")
        return original(path, data)

    try:
        old = session.execution.snapshot()
        monkeypatch.setattr(module, "atomic_json", save)
        with pytest.raises(StorageError):
            session.shutdown_drain("exit", 120000, brackets=tables())
        assert session.execution.snapshot() == old
        assert session.shutdown_status()["recovery_required"]
        assert not session.shutdown_preflight("exit", 120000, brackets=tables())["ready"]
        session.reconcile()
        session.shutdown_drain("exit", 120000, brackets=tables())
        assert len(session.execution.snapshot().pending) == 2
    finally:
        session.close()


def test_restart_preserves_pending_and_requires_manual_strategy_preparation(tmp_path):
    first = create(tmp_path)
    first.shutdown_drain("exit", 120000, brackets=tables())
    pending = first.execution.snapshot().pending
    path = first._shutdown.path
    original = path.read_bytes()
    first.close()
    restored = create(tmp_path, create=False)
    try:
        assert restored.execution.snapshot().paused
        assert restored.execution.snapshot().pending == pending and path.read_bytes() == original
        assert restored.shutdown_status()["needs_settlement"]
        restored.shutdown_drain("exit", 120000, brackets=tables())
        assert restored.execution.snapshot().pending == pending
        restored.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
        assert len(facts(restored)) == 4
        with pytest.raises(TradingBlocked, match="preparation"):
            restored.run_once(bars(3), as_of_ms=240000, brackets=tables())
    finally:
        restored.close()


@pytest.mark.parametrize("issued", [0, 120000 + 120001])
def test_stale_or_future_marks_reject_without_any_write(tmp_path, issued):
    session = create(tmp_path)
    try:
        source = session.path.read_bytes()
        records = len(session.execution._history)
        assert not session.shutdown_preflight("exit", issued, brackets=tables())["ready"]
        with pytest.raises(TradingBlocked):
            session.shutdown_drain("exit", issued, brackets=tables())
        assert not session._shutdown.path.exists()
        assert session.path.read_bytes() == source and len(session.execution._history) == records
    finally:
        session.close()


def test_preflight_dust_rejection_and_pending_frame_leave_original_orders(tmp_path):
    from dataclasses import replace

    session = create(tmp_path)
    try:
        session._rules["1"] = replace(session._rules["1"], min_quantity=D(2))
        assert not session.shutdown_preflight("exit", 120000, brackets=tables())["ready"]
        assert not session._shutdown.path.exists()
        session._meta["pending"] = {"incomplete": True}
        assert "frame" in session.shutdown_preflight("exit", 120000, brackets=tables())["reason"]
    finally:
        session.close()


def test_owner_thread_and_corrupt_file_are_not_hidden(tmp_path):
    session = create(tmp_path)
    errors = []

    def run():
        try:
            session.shutdown_status()
        except TradingBlocked:
            errors.append(True)

    worker = Thread(target=run)
    worker.start()
    worker.join(2)
    assert errors == [True]
    session.shutdown_drain("exit", 120000, brackets=tables())
    path = session._shutdown.path
    session.close()
    damaged = b'{"data":{},"sha256":"bad"}'
    path.write_bytes(damaged)
    with pytest.raises(StorageError, match="checksum"):
        create(tmp_path, create=False)
    assert path.read_bytes() == damaged


@pytest.mark.parametrize("both_sources_fail", [False, True])
def test_primary_journal_failure_uses_actual_fallback_or_blocks(
    tmp_path, monkeypatch, both_sources_fail
):
    from omega.ports.journal import Durability

    session = create(tmp_path)
    journal = session.execution._journal
    original = journal._publish

    def publish(directory, record, payload):
        if directory == journal.primary or both_sources_fail:
            raise OSError("synthetic unavailable journal source")
        return original(directory, record, payload)

    try:
        monkeypatch.setattr(journal, "_publish", publish)
        if both_sources_fail:
            with pytest.raises(StorageError):
                session.shutdown_drain("exit", 120000, brackets=tables())
            assert session.shutdown_status()["state"] == "unknown"
            assert len(facts(session)) == 2 and not session.execution.snapshot().pending
        else:
            session.shutdown_drain("exit", 120000, brackets=tables())
            assert any(
                item.durability == Durability.FALLBACK for item in journal.read(session.account_id)
            )
            session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
            assert session.shutdown_status()["state"] == "complete"
            assert len(facts(session)) == 4 and session.execution.snapshot().paused
    finally:
        session.close()


def test_duplicate_calls_do_not_rewrite_or_cancel_after_completion(tmp_path):
    session = create(tmp_path)
    try:
        session.shutdown_drain("exit", 120000, brackets=tables())
        pending = session.execution.snapshot().pending
        original_file = session._shutdown.path.read_bytes()
        records = len(session.execution._history)
        session.shutdown_drain("exit", 120000, brackets=())
        assert session.execution.snapshot().pending == pending
        assert session._shutdown.path.read_bytes() == original_file
        assert len(session.execution._history) == records
        assert not session.shutdown_preflight("exit", 120001, brackets=tables())["ready"]
        session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
        final_file, records = session._shutdown.path.read_bytes(), len(session.execution._history)
        session.shutdown_cancel("exit")
        session.shutdown_drain("exit", 120000, brackets=tables())
        assert session._shutdown.path.read_bytes() == final_file
        assert len(session.execution._history) == records
    finally:
        session.close()


@pytest.mark.parametrize("lost_sequence", [3, 4])
def test_multisymbol_prefix_account_ack_loss_reconciles_exactly_once(
    tmp_path, monkeypatch, lost_sequence
):
    session = create(tmp_path)
    store = session.account_owner._store
    original = store.save
    fired = []

    def save(account, *, expected_sequence):
        original(account, expected_sequence=expected_sequence)
        if len(account.ledger) == lost_sequence and not fired:
            fired.append(True)
            raise OSError("synthetic prefix account ACK loss")

    try:
        session.shutdown_drain("exit", 120000, brackets=tables())
        monkeypatch.setattr(store, "save", save)
        with pytest.raises(Exception, match="commit outcome is unknown"):
            session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
        assert session.shutdown_status()["state"] == "unknown"
        session.reconcile()
        result = session.shutdown_run_once(bars(2), as_of_ms=180001, brackets=tables())
        assert result["state"] == "complete"
        assert len(facts(session)) == 4
        assert {entry.fact.available_at_ms for entry in facts(session)[2:]} == {180000}
        assert len({entry.fact.fill_id for entry in facts(session)}) == 4
    finally:
        session.close()


def test_final_shutdown_cursor_ack_loss_recovers_completed_fill_without_repricing(
    tmp_path, monkeypatch
):
    session = create(tmp_path)
    original = module.atomic_json
    fired = []

    def save(path, envelope):
        original(path, envelope)
        if envelope["data"]["state"] == "complete" and not fired:
            fired.append(True)
            raise OSError("synthetic shutdown cursor ACK loss")

    try:
        session.shutdown_drain("exit", 120000, brackets=tables())
        monkeypatch.setattr(module, "atomic_json", save)
        with pytest.raises(StorageError):
            session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
        assert len(facts(session)) == 4 and session.shutdown_status()["state"] == "unknown"
        session.reconcile()
        assert session.shutdown_status()["state"] == "complete"
        assert len(facts(session)) == 4
    finally:
        session.close()


def test_model_failure_diagnostic_blocks_new_risk_without_claiming_financial_unknown(
    tmp_path, monkeypatch
):
    import json
    from dataclasses import replace

    session = create(tmp_path, opened=False)
    try:
        assert session.snapshot()["new_risk_ready"]
        assert "model_errors" not in session.snapshot()
        adapter = session._adapters["0"]
        original = adapter.snapshot()
        document = {
            "payload": {"model_failure": {"error_type": "ArithmeticError", "open_time_ms": 0}}
        }
        changed = replace(
            original.checkpoint,
            module_checkpoint=json.dumps(document, sort_keys=True, separators=(",", ":")),
        )
        monkeypatch.setattr(adapter, "snapshot", lambda: replace(original, checkpoint=changed))
        view = session.snapshot()
        assert not view["new_risk_ready"]
        assert view["model_errors"] == {"0": document["payload"]["model_failure"]}
        assert view["state"] == "ready" and session.execution.snapshot().state == "ready"
        assert session.shutdown_preflight("exit", 60001, brackets=tables())["ready"]
    finally:
        session.close()


def test_cancellation_cannot_precede_its_original_financial_request(tmp_path):
    session = create(tmp_path, opened=False)
    try:
        before = session.execution.snapshot()
        result = session.shutdown_preflight("exit", 0, brackets=tables())
        assert not result["ready"] and "financial facts" in result["reason"]
        assert not session._shutdown.path.exists() and session.execution.snapshot() == before
    finally:
        session.close()


@pytest.mark.parametrize("settled", [False, True])
def test_missing_shutdown_file_never_restores_a_stale_model_cursor(tmp_path, settled):
    session = create(tmp_path)
    session.shutdown_drain("exit", 120000, brackets=tables())
    if settled:
        session.shutdown_run_once(bars(2), as_of_ms=180000, brackets=tables())
    path = session._shutdown.path
    session.close()
    path.unlink()
    with pytest.raises(StorageError, match="metadata is missing"):
        create(tmp_path, create=False)
