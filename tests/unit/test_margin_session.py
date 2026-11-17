"""Actual file journals/checkpoints + built-in TS, offline common-minute frames."""

import json
from copy import deepcopy
from decimal import Decimal as D
from threading import Thread

import pytest

from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.domain.margin import MarginBracket, MarginBracketSnapshot
from omega.domain.models import Candle, Instrument, Market
from omega.runtime import margin_session
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")


def configuration(*, tef=False):
    account = {
        "account_id": "session-test",
        "balance": "1000",
        "positions": [
            {
                "instrument": item.key,
                "owner_id": identity,
                "margin_mode": "cross",
                "leverage": 10,
                "budget_margin": "100",
            }
            for item, identity in ((BTC, "btc-ts"), (ETH, "eth-ts"))
        ],
    }
    rows = [
        {
            "id": identity,
            "instrument": item.key,
            "module": "target_quantity",
            "configuration": {"quantity": quantity},
            "analysis": {},
            "constraints": {"leverage": "10", "can_short": True},
        }
        for item, identity, quantity in ((BTC, "btc-ts", "1"), (ETH, "eth-ts", "-1"))
    ]
    if tef:
        rows[0].update(
            module="tef",
            configuration={"fullStopLossImmediate": ".05"},
            analysis={"ema_period": 2, "mmacd_periods": [2, 3], "signal_period": 2},
        )
    return account, rows


def tables():
    return tuple(
        MarginBracketSnapshot(
            item,
            (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
            0,
            0,
            "explicit_test_reference",
            "a" * 64,
        )
        for item in (BTC, ETH)
    )


def bars(index, price="100", *, opening=None, low=None):
    close = D(price)
    op = D(opening or price)
    return tuple(
        Candle(
            item,
            index * 60000,
            (index + 1) * 60000,
            op,
            max(op, close),
            D(low) if low else min(op, close),
            close,
            D(10),
        )
        for item in (BTC, ETH)
    )


def new(root, *, create=True, tef=False):
    account, strategies = configuration(tef=tef)
    return MarginTradingSession(root, account, strategies, 0, create=create)


def run(session, index, **changes):
    return session.run_once(bars(index), as_of_ms=(index + 1) * 60000, brackets=tables(), **changes)


def ledger(session):
    return session.execution.snapshot().account.ledger


def test_real_files_two_ts_nextbar_accounting_pause_and_restart_exactly_once(tmp_path):
    session = new(tmp_path)
    try:
        assert session.snapshot()["paused"] and session.snapshot()["next_open_ms"] == 0
        session.resume("start")
        first = run(session, 0)
        assert first["cursor_ms"] == 0 and len(first["pending_orders"]) == 2 and not ledger(session)
        session.pause("pause")
        second = run(session, 1)
        assert second["paused"] and len(ledger(session)) == 2 and not second["pending_orders"]
        assert session.execution.snapshot().account.cross_wallet == D("999.8")
        assert {fact.fact.delta_qty for fact in ledger(session)} == {D(1), D(-1)}
        source = session.path.read_bytes()
    finally:
        session.close()
    restored = new(tmp_path, create=False)
    try:
        assert restored.snapshot()["paused"] and restored.snapshot()["cursor_ms"] == 60000
        assert restored.run_once(bars(1), as_of_ms=120001, brackets=tables())["cursor_ms"] == 60000
        assert len(ledger(restored)) == 2 and restored.path.read_bytes() == source
        assert run(restored, 2)["next_open_ms"] == 180000
    finally:
        restored.close()


def test_missing_meta_and_wallet_do_not_create_on_ordinary_restart(tmp_path):
    with pytest.raises(StorageError, match="initialization"):
        new(tmp_path, create=False)
    assert not list(tmp_path.glob("state/margin-v2/*.json"))
    session = new(tmp_path)
    path = session.path
    session.close()
    path.unlink()
    with pytest.raises(StorageError, match="initialization"):
        new(tmp_path, create=False)
    assert list(tmp_path.glob("state/margin-v2/*.json"))


def test_initial_creation_ack_loss_retries_same_config_without_wallet_reset(tmp_path, monkeypatch):
    original = margin_session.atomic_json
    fired = []

    def lose_ack(path, document):
        original(path, document)
        if "margin-sessions" in str(path) and not fired:
            fired.append(True)
            raise OSError("initial creation ACK loss")

    monkeypatch.setattr(margin_session, "atomic_json", lose_ack)
    with pytest.raises(StorageError):
        new(tmp_path)
    monkeypatch.setattr(margin_session, "atomic_json", original)
    restored = new(tmp_path)
    try:
        assert restored.snapshot()["account"]["account"]["cross_wallet"] == "1000"
        assert restored.snapshot()["paused"] and not ledger(restored)
    finally:
        restored.close()


def test_empty_replay_only_pending_and_all_account_instrument_barrier(tmp_path):
    session = new(tmp_path)
    try:
        with pytest.raises(ValidationError, match="empty replay"):
            session.run_once((), as_of_ms=60000)
        with pytest.raises(ValidationError, match="every account"):
            session.run_once((bars(0)[0],), as_of_ms=60000, brackets=tables())
        with pytest.raises(ValidationError):
            session.run_once((bars(0)[0], bars(1)[1]), as_of_ms=120000, brackets=tables())
        assert session.snapshot()["cursor_ms"] is None and not ledger(session)
    finally:
        session.close()


@pytest.mark.parametrize(
    "where", ["frozen_frame", "after_open", "after_decision", "after_close", "final_ack"]
)
def test_crash_between_host_and_financial_commits_replays_frozen_frame_only(
    tmp_path, monkeypatch, where
):
    session = new(tmp_path)
    session.resume("start")
    run(session, 0)
    # Freeze the next frame; its open phase settles both previously accepted intents.
    original = margin_session.atomic_json
    fired = []

    def fail(path, envelope):
        data = envelope.get("data", {})
        pending = data.get("pending")
        match = (
            where == "frozen_frame"
            and pending
            and pending["phase"] == "open"
            or where == "after_open"
            and pending
            and pending["phase"] == "decisions"
            or where == "after_decision"
            and pending
            and pending["done"]
            or where == "after_close"
            and pending
            and pending["phase"] == "after"
            or where == "final_ack"
            and data.get("cursor_ms") == 60000
            and pending is None
        )
        if path == session.path and match and not fired:
            fired.append(True)
            if where == "final_ack":
                original(path, envelope)
            raise OSError("synthetic host crash/ACK loss")
        original(path, envelope)

    monkeypatch.setattr(margin_session, "atomic_json", fail)
    with pytest.raises(StorageError):
        run(session, 1)
    session.close()
    monkeypatch.setattr(margin_session, "atomic_json", original)
    restored = new(tmp_path, create=False)
    try:
        assert restored.snapshot()["paused"]
        if restored.snapshot()["pending_frame"]:
            frozen = deepcopy(restored._meta["pending"])
            with pytest.raises(ProtocolError, match="cannot be replaced"):
                restored.run_once(bars(1, "101"), as_of_ms=120001)
            restored.run_once((), as_of_ms=120001, brackets=(), entries_allowed=False)
            assert restored._meta["last_frame"]["candles"] == frozen["candles"]
            assert restored._meta["last_frame"]["as_of_ms"] == frozen["as_of_ms"]
        elif restored.snapshot()["cursor_ms"] == 0:
            run(restored, 1, entries_allowed=False)
        assert restored.snapshot()["cursor_ms"] == 60000
        assert len(
            ledger(restored)
        ) == 2 and restored.execution.snapshot().account.cross_wallet == D("999.8")
        assert len({entry.fact.fill_id for entry in ledger(restored)}) == 2
    finally:
        restored.close()


def test_delayed_catchup_has_no_new_entries_but_preserves_preexisting_orders(tmp_path):
    session = new(tmp_path)
    try:
        session.resume("start")
        session.run_once(bars(0), as_of_ms=300001, brackets=tables())
        assert not session.snapshot()["pending_orders"] and not ledger(session)
    finally:
        session.close()


def test_after_decision_crash_reuses_fixed_cycle_and_exact_strategy_checkpoints(
    tmp_path, monkeypatch
):
    snapshots = []
    for label in ("baseline", "interrupted"):
        root = tmp_path / label
        session = new(root)
        session.resume("start")
        original = margin_session.atomic_json
        fired = []

        def fail_after(
            path, envelope, *, label=label, session=session, fired=fired, original=original
        ):
            pending = envelope.get("data", {}).get("pending")
            if (
                label == "interrupted"
                and path == session.path
                and pending
                and pending["after_done"]
                and not fired
            ):
                fired.append(True)
                raise OSError("after decision committed; host progress not committed")
            original(path, envelope)

        monkeypatch.setattr(margin_session, "atomic_json", fail_after)
        try:
            if label == "interrupted":
                with pytest.raises(StorageError):
                    run(session, 0)
                session.close()
                monkeypatch.setattr(margin_session, "atomic_json", original)
                session = new(root, create=False)
                session.run_once((), as_of_ms=60001, entries_allowed=False)
            else:
                run(session, 0)
            snapshots.append(
                {key: value.snapshot().checkpoint for key, value in session._adapters.items()}
            )
        finally:
            session.close()
            monkeypatch.setattr(margin_session, "atomic_json", original)
    assert snapshots[0] == snapshots[1]


def test_two_accounts_can_reuse_ts_ids_without_sharing_strategy_checkpoints(tmp_path):
    account_a, rows = configuration()
    account_b = deepcopy(account_a)
    account_b["account_id"] = "second-account"
    first = MarginTradingSession(tmp_path, account_a, rows, 0, create=True)
    second = MarginTradingSession(tmp_path, account_b, rows, 0, create=True)
    try:
        first.resume("first-start")
        run(first, 0)
        assert first.strategy_root != second.strategy_root
        assert first.snapshot()["cursor_ms"] == 0 and second.snapshot()["cursor_ms"] is None
        assert second._adapters["btc-ts"].snapshot().checkpoint.account_id == "second-account"
        second.resume("second-start")
        run(second, 0)
        assert second.snapshot()["cursor_ms"] == 0
    finally:
        first.close()
        second.close()


@pytest.mark.parametrize("invalid", ["balance", "module", "analysis", "constraints"])
def test_preflight_rejects_invalid_config_before_any_financial_namespace(tmp_path, invalid):
    account, rows = configuration()
    if invalid == "balance":
        account["balance"] = "-1"
    elif invalid == "module":
        rows[0]["module"] = "does_not_exist"
    elif invalid == "analysis":
        rows[0]["analysis"] = {"unsupported": True}
    else:
        rows[0]["constraints"]["leverage"] = "2"
    with pytest.raises(ValidationError):
        prepare_margin_configuration(account, rows, 0)
    destination = tmp_path / "never-created"
    with pytest.raises(ValidationError):
        MarginTradingSession(destination, account, rows, 0, create=True)
    assert not destination.exists()


def test_preflight_valid_plan_and_missing_checkpoint_capability_have_no_file_owner(
    tmp_path, monkeypatch
):
    account, rows = configuration()
    monkeypatch.setattr(
        margin_session, "InstanceLock", lambda *_: pytest.fail("preflight must not create an owner")
    )
    initial, fee, prepared, modules, graphs, rules = prepare_margin_configuration(account, rows, 0)
    assert initial.account_id == account["account_id"] and fee == D(".001")
    assert {row["id"] for row in prepared} == set(modules) == set(graphs) == set(rules)
    assert not list(tmp_path.iterdir())

    class Registry:
        def create(self, module, instrument, configuration):
            from omega.strategies.target_quantity.module import TargetQuantityModule

            result = TargetQuantityModule(instrument, configuration)
            result.checkpoint = None
            return result

    with pytest.raises(ValidationError, match="checkpoint"):
        prepare_margin_configuration(account, rows, 0, registry=Registry())


def test_actual_tef_analysis_seed_and_local_stop_work_while_entries_paused(tmp_path, monkeypatch):
    session = new(tmp_path, tef=True)
    try:
        session.resume("start")
        for index in range(20):
            price = str(100 + index * index)
            opening = str(100 + max(0, index - 1) ** 2)
            session.run_once(
                bars(index, price, opening=opening), as_of_ms=(index + 1) * 60000, brackets=tables()
            )
            if session.execution.snapshot().account.positions[0].quantity > 0:
                break
        assert session._adapters["btc-ts"].module.last_open_time_ms == index * 60000
        held = session.execution.snapshot().account.positions[0].quantity
        assert held > 0 and ledger(session)
        session.pause("pause-for-stop")
        monkeypatch.setattr(
            type(session._graphs["btc-ts"]),
            "update",
            lambda *_: (_ for _ in ()).throw(RuntimeError("synthetic analysis failure")),
        )
        session.run_once(
            bars(index + 1, price, low="80"), as_of_ms=(index + 2) * 60000, brackets=tables()
        )
        owned = [
            item for item in session.execution.snapshot().pending if item.original.instrument == BTC
        ]
        assert owned and owned[0].original.proposal.reducing
        session.run_once(bars(index + 2, price), as_of_ms=(index + 3) * 60000, brackets=tables())
        assert session.execution.snapshot().account.positions[0].quantity == 0
        assert session.snapshot()["paused"]
        assert session.snapshot()["graph_errors"] == {"btc-ts": "RuntimeError"}
        assert not session.snapshot()["new_risk_ready"]
    finally:
        session.close()
    restored = new(tmp_path, create=False, tef=True)
    try:
        assert restored.snapshot()["graph_errors"] == {"btc-ts": "RuntimeError"}
        assert restored.execution.snapshot().account.positions[0].quantity == 0
    finally:
        restored.close()


def test_changed_configuration_and_corrupt_metadata_preserve_original_files(tmp_path):
    session = new(tmp_path)
    path = session.path
    session.close()
    original = path.read_bytes()
    account, rows = configuration()
    rows[0]["configuration"]["quantity"] = "2"
    with pytest.raises(ProtocolError, match="changed"):
        MarginTradingSession(tmp_path, account, rows, 0)
    assert path.read_bytes() == original
    damaged = b'{"broken":true}'
    path.write_bytes(damaged)
    with pytest.raises(StorageError, match="corrupt"):
        new(tmp_path, create=False)
    assert path.read_bytes() == damaged


def test_worker_ownership_close_fence_and_no_readonly_mutation(tmp_path):
    session = new(tmp_path)
    failures = []

    def foreign():
        try:
            session.snapshot()
        except TradingBlocked:
            failures.append(True)

    worker = Thread(target=foreign)
    worker.start()
    worker.join(2)
    assert failures == [True]
    before = session.path.read_bytes()
    assert not session.execution.committed_batches(0, "open")
    assert session.path.read_bytes() == before
    for value, phase in ((True, "open"), (1, "open"), (0, "other")):
        with pytest.raises(ValidationError):
            session.execution.committed_batches(value, phase)
    session.close()
    with pytest.raises(TradingBlocked):
        session.run_once(bars(0), as_of_ms=60000)


def test_auxiliary_dependency_is_explicitly_unsupported_before_financial_initialization(tmp_path):
    account, rows = configuration()

    # A complete configured producer may require depth/metrics. The first session
    # slice rejects the source contract rather than supplying made-up zero input.
    class Registry:
        def create(self, module, instrument, configuration):
            from omega.strategies.target_quantity.module import TargetQuantityModule

            result = TargetQuantityModule(instrument, configuration)
            if instrument == BTC:
                from omega.ports.modules import ModuleDescriptor

                result.descriptor = ModuleDescriptor(
                    "needs_aux", "1", frozenset({"0_NES_0_NES"}), False
                )
            return result

    with pytest.raises(ValidationError):
        MarginTradingSession(tmp_path, account, rows, 0, create=True, registry=Registry())
    assert not list(tmp_path.glob("state/margin-v2/*.json"))


def test_ack_unconfirmed_batch_lookup_is_readonly_and_fixed_facts_reconcile_once(
    tmp_path, monkeypatch
):
    session = new(tmp_path)
    try:
        session.resume("start")
        run(session, 0)
        journal = session.execution._journal
        original = journal.append

        def lose_ack(record):
            original(record)
            raise StorageError("synthetic durable batch ACK loss")

        monkeypatch.setattr(journal, "append", lose_ack)
        with pytest.raises(StorageError):
            run(session, 1)
        assert not ledger(session)
        before = session.path.read_bytes()
        found = session.execution.committed_batches(60000, "open")
        assert len(found) == 1 and len(found[0].facts) == 2
        assert all(fact.available_at_ms == 120000 for fact in found[0].facts)
        assert not ledger(session) and session.path.read_bytes() == before
        monkeypatch.setattr(journal, "append", original)
        session.reconcile()
        assert len(ledger(session)) == 2
        original_prepare = session.execution.prepare_batch
        prepared_phases = []

        def prepare(*args, **kwargs):
            prepared_phases.append(kwargs["phase"])
            return original_prepare(*args, **kwargs)

        monkeypatch.setattr(session.execution, "prepare_batch", prepare)
        session.run_once((), as_of_ms=120001, entries_allowed=False)
        assert prepared_phases == ["close"]
        assert tuple(entry.fact for entry in ledger(session)) == found[0].facts
        assert session.snapshot()["cursor_ms"] == 60000
    finally:
        session.close()


@pytest.mark.parametrize("tamper", ["cursor", "completed_phase", "feature", "graph"])
def test_valid_checksum_metadata_cannot_disagree_with_frozen_authority(tmp_path, tamper):
    session = new(tmp_path)
    try:
        session.resume("start")
        run(session, 0)
        path = session.path
    finally:
        session.close()
    envelope = json.loads(path.read_text())
    data = envelope["data"]
    if tamper == "cursor":
        data["cursor_ms"] = True
    elif tamper == "completed_phase":
        data["last_frame"]["phase"] = "open"
    elif tamper == "feature":
        data["last_frame"]["features"]["btc-ts"] = {"invented": 1}
    else:
        data["graphs"]["btc-ts"] = {"version": 1, "kind": "fake"}
    envelope["sha256"] = margin_session._digest(data)
    raw = json.dumps(envelope).encode()
    path.write_bytes(raw)
    with pytest.raises((ProtocolError, ValidationError)):
        new(tmp_path, create=False)
    assert path.read_bytes() == raw


def test_unknown_in_memory_host_commit_requires_explicit_same_candidate_reconcile(
    tmp_path, monkeypatch
):
    session = new(tmp_path)
    original = margin_session.atomic_json
    fired = []

    def lose_ack(path, envelope):
        original(path, envelope)
        if path == session.path and envelope["data"].get("pending") and not fired:
            fired.append(True)
            raise OSError("host frame ACK loss")

    monkeypatch.setattr(margin_session, "atomic_json", lose_ack)
    try:
        session.resume("start")
        with pytest.raises(StorageError):
            run(session, 0)
        assert session.snapshot()["state"] == "unknown" and not ledger(session)
        with pytest.raises(TradingBlocked, match="reconciliation"):
            session.run_once((), as_of_ms=60001)
        session.reconcile()
        frozen = deepcopy(session._meta["pending"])
        session.run_once((), as_of_ms=300001, entries_allowed=False)
        assert session._meta["last_frame"]["as_of_ms"] == frozen["as_of_ms"]
        assert not session.snapshot()["pending_orders"] and not ledger(session)
    finally:
        session.close()
