"""Real-file reconciliation distinguishes accepted intent from unsubmitted proposal."""

import json
import time
from copy import deepcopy
from decimal import Decimal

import pytest

from omega.application.margin_execution import FrozenPendingOrder
from omega.application.margin_strategy import MarginStrategyState
from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.domain.trading import TradeDecision, close_position
from omega.runtime import margin_strategy_store
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration
from omega.runtime.margin_strategy_store import FileMarginStrategyStore
from omega.runtime.margin_trading import MarginTradingService
from omega.runtime.virtual_margin_journal import decode_record
from omega.strategies.registry import StrategyRegistry
from omega.strategies.target_quantity.module import TargetQuantityModule

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")


def wait(read, check):
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline:
        value = read()
        if check(value):
            return value
        time.sleep(0.01)
    raise AssertionError(value)


def operation(service, identity, kind, parameters):
    service.submit(identity, kind, parameters)
    return wait(
        lambda: service.status(identity), lambda row: row["state"] not in {"queued", "running"}
    )


def parameters(protect=False):
    definitions = [(BTC, "b-btc")]
    if protect:
        definitions.append((ETH, "a-eth"))
    return {
        "account": {
            "account_id": "peer",
            "balance": "1000",
            "positions": [
                {
                    "instrument": instrument.key,
                    "owner_id": identity,
                    "margin_mode": "cross",
                    "leverage": 10,
                    "budget_margin": "100",
                }
                for instrument, identity in definitions
            ],
        },
        "strategies": [
            {
                "id": identity,
                "instrument": instrument.key,
                "module": "target_quantity",
                "configuration": {"quantity": "1"},
                "analysis": {},
                "constraints": {"leverage": "10", "can_short": True},
            }
            for instrument, identity in definitions
        ],
        "start_ms": 0,
        "fee_rate": "0.001",
        "brackets": [
            {
                "instrument": instrument.key,
                "observed_at_ms": 0,
                "available_at_ms": 0,
                "rows": [
                    {
                        "notional_floor": "0",
                        "notional_cap": "1000000",
                        "maintenance_rate": "0.01",
                        "maintenance_amount": "0",
                        "max_leverage": 20,
                    }
                ],
            }
            for instrument, _ in definitions
        ],
    }


def start(root, fault, *, protect=False):
    failures, reads, rejected_callbacks, clock = [], [], [], [60000]

    class ProtectedTarget(TargetQuantityModule):
        def evaluate(self, context):
            if context.has_pending_order:
                return TradeDecision(reason="fixture_await_pending")
            if context.position.quantity > 0 and context.closed_candle.low < Decimal(90):
                return close_position(context, "fixture_local_protection")
            return super().evaluate(context)

        def on_order_rejected(self, decision):
            rejected_callbacks.append(decision)

    class Registry(StrategyRegistry):
        def create(self, identity, instrument, configuration=None):
            return ProtectedTarget(instrument, configuration)

    class Factory:
        prepare_configuration = staticmethod(prepare_margin_configuration)

        def __call__(self, *args, **kwargs):
            session = MarginTradingSession(*args, **kwargs)
            original_submit = session.execution.submit_intent
            original_append = session.execution._journal.append

            def before_intent(pending, **values):
                if fault == "before" and pending.original.instrument == BTC and not failures:
                    failures.append(pending)
                    raise StorageError("synthetic before-intent failure")
                return original_submit(pending, **values)

            def after_append(record):
                result = original_append(record)
                payload = record.payload
                if (
                    fault == "ack"
                    and isinstance(payload, FrozenPendingOrder)
                    and payload.original.instrument == BTC
                    and not failures
                ):
                    failures.append(payload)
                    raise StorageError("synthetic durable intent ACK loss")
                return result

            session.execution.submit_intent = before_intent
            session.execution._journal.append = after_append
            return session

    class Source:
        def read(self, instruments, opening, end, **values):
            reads.append((instruments, opening, end, values))
            return tuple(
                Candle(
                    instrument,
                    opening,
                    opening + 60000,
                    *(
                        Decimal(value)
                        for value in (
                            100,
                            100,
                            80 if protect and opening == 60000 else 100,
                            100,
                            10,
                        )
                    ),
                )
                for instrument in ((BTC, ETH) if protect else (BTC,))
            )

        def close(self):
            pass

    service = MarginTradingService(
        root,
        Registry(),
        Source,
        core_epoch="peer-core",
        session_factory=Factory(),
        clock_ms=lambda: clock[0],
        entry_gate=lambda: True,
    )
    service.start()
    wait(service.snapshot, lambda row: row["state"] == "ready")
    assert operation(service, "configure", "configure", parameters(protect))["state"] == "completed"
    assert (
        operation(service, "resume-first", "resume", {"account_id": "peer"})["state"] == "completed"
    )
    wait(
        service.snapshot,
        lambda row: bool(row["accounts"] and row["accounts"][0].get("manual_recovery_required")),
    )
    assert "peer" in service.blocked_accounts and len(reads) == 1
    return service, clock, failures, rejected_callbacks


def checkpoint(root, strategy="b-btc"):
    for path in root.glob("state/margin-strategy-accounts/*/state/margin-strategies/*.json"):
        state = MarginStrategyState.parse(json.loads(path.read_text())["data"])
        if state.strategy_id == strategy:
            return state
    raise AssertionError("strategy checkpoint missing")


@pytest.mark.parametrize("fault", ["before", "ack"])
def test_real_file_reconcile_rejects_only_proven_unsubmitted_entry_and_keeps_accepted_original(
    tmp_path, fault
):
    service, clock, failures, callbacks = start(tmp_path, fault)
    try:
        original = checkpoint(tmp_path)
        assert original.pending is not None
        if fault == "before":
            assert (
                operation(service, "user-pause", "pause", {"account_id": "peer"})["state"]
                == "completed"
            )
        result = operation(service, "reconcile", "reconcile", {"account_id": "peer"})
        assert result["state"] == "completed", result
        assert "peer" not in service.blocked_accounts
        updated = checkpoint(tmp_path)
        assert updated.pending is None
        assert service.snapshot()["accounts"][0]["paused"]
        if fault == "before":
            assert updated.rejected_intents[0].action == original.pending
            assert updated.rejected_intents[0].reason == "paused_before_intent"
            assert len(callbacks) == 1
            assert updated.last_decision.reason == "blocked: paused_before_intent"
        else:
            assert updated.rejected_intents == () and callbacks == []
            wait(
                service.snapshot,
                lambda row: row["accounts"][0]["pending_orders"] == [failures[0].order_id],
            )
            clock[0] = 120000
            wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 60000)
            view = service.snapshot()["accounts"][0]
            assert view["account"]["summary"]["ledger_total"] == 1
            assert (
                view["account"]["account"]["ledger"][0]["fact"]["order_id"] == failures[0].order_id
            )
        resumed = operation(service, "resume-after-reconcile", "resume", {"account_id": "peer"})
        assert resumed["state"] == "completed", resumed
        records = [
            decode_record(path.read_bytes())
            for path in tmp_path.glob("state/margin-v2-journal/*/*.json")
        ]
        accepted = [
            record.payload for record in records if isinstance(record.payload, FrozenPendingOrder)
        ]
        assert accepted == ([] if fault == "before" else [failures[0]])
    finally:
        service.close()


def test_rejected_unsubmitted_entry_does_not_starve_other_strategy_pending_and_local_protection(
    tmp_path,
):
    service, clock, failures, callbacks = start(tmp_path, "before", protect=True)
    try:
        assert operation(service, "pause", "pause", {"account_id": "peer"})["state"] == "completed"
        assert (
            operation(service, "reconcile", "reconcile", {"account_id": "peer"})["state"]
            == "completed"
        )
        assert checkpoint(tmp_path).rejected_intents[0].action.order == failures[0]
        clock[0] = 120000
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 60000)
        view = service.snapshot()["accounts"][0]
        assert view["paused"] and view["account"]["summary"]["ledger_total"] == 1
        assert len(view["pending_orders"]) == 1
        clock[0] = 180000
        wait(service.snapshot, lambda row: row["accounts"][0]["cursor_ms"] == 120000)
        view = service.snapshot()["accounts"][0]
        assert view["paused"] and view["account"]["summary"]["ledger_total"] == 2
        assert all(
            Decimal(position["quantity"]) == 0
            for position in view["account"]["account"]["positions"]
        )
        assert len(callbacks) == 1 and not view["pending_orders"]
    finally:
        service.close()


@pytest.mark.parametrize("accepted", [False, True])
def test_actual_cp_final_ack_loss_reconciles_fixed_disposition_without_duplicate_callback(
    tmp_path, monkeypatch, accepted
):
    callbacks, faults = [], []

    class Target(TargetQuantityModule):
        def on_order_rejected(self, decision):
            callbacks.append(decision)

    class Registry(StrategyRegistry):
        def create(self, identity, instrument, configuration=None):
            return Target(instrument, configuration)

    config = parameters()
    session = MarginTradingSession(
        tmp_path, config["account"], config["strategies"], 0, create=True, registry=Registry()
    )
    from omega.runtime.margin_console import scenario_brackets

    tables = scenario_brackets(config["brackets"])
    bar = Candle(BTC, 0, 60000, *(Decimal(value) for value in (100, 100, 100, 100, 10)))
    original_submit = session.execution.submit_intent
    original_append = session.execution._journal.append

    def before(pending, **values):
        if not faults:
            faults.append(pending)
            raise StorageError("before intent")
        return original_submit(pending, **values)

    def after(record):
        result = original_append(record)
        if isinstance(record.payload, FrozenPendingOrder) and not faults:
            faults.append(record.payload)
            raise StorageError("intent ACK loss")
        return result

    if accepted:
        session.execution._journal.append = after
    else:
        session.execution.submit_intent = before
    original_atomic = margin_strategy_store.atomic_json
    failed = []

    def lose_final_ack(path, envelope):
        original_atomic(path, envelope)
        fields = envelope["data"]["state"]["fields"]
        if fields["last_open_time_ms"] == 0 and fields["pending"] is None and not failed:
            failed.append(True)
            raise OSError("final CP committed; ACK lost")

    try:
        session.resume("start")
        with pytest.raises(StorageError):
            session.run_once((bar,), as_of_ms=60000, brackets=tables)
        monkeypatch.setattr(margin_strategy_store, "atomic_json", lose_final_ack)
        with pytest.raises(OSError):
            session.reconcile()
        assert len(callbacks) == (0 if accepted else 1)
        session.reconcile()
        assert len(callbacks) == (0 if accepted else 1)
        state = checkpoint(tmp_path)
        assert state.pending is None
        assert len(state.rejected_intents) == (0 if accepted else 1)
        assert state.rejected_intents == () or state.rejected_intents[0].action.order == faults[0]
        assert session.snapshot()["paused"]
        session.run_once((), as_of_ms=60000, entries_allowed=False)
        assert session.execution.snapshot().pending == ((faults[0],) if accepted else ())
    finally:
        session.close()
    restored = MarginTradingSession(
        tmp_path, config["account"], config["strategies"], 0, registry=Registry()
    )
    try:
        assert restored.snapshot()["paused"]
        assert len(checkpoint(tmp_path).rejected_intents) == (0 if accepted else 1)
        assert len(callbacks) == (0 if accepted else 1)
    finally:
        restored.close()


def test_v2_disposition_schema_and_exact_v1_file_migration_preserve_cas(tmp_path):
    config = parameters()
    session = MarginTradingSession(
        tmp_path, config["account"], config["strategies"], 0, create=True
    )
    state = checkpoint(tmp_path)
    strategy_root = session.strategy_root
    session.close()
    document = state.document()
    assert document["schema_version"] == 2
    for change in ("missing", "unknown", "relabel"):
        damaged = deepcopy(document)
        if change == "missing":
            damaged["state"]["fields"].pop("rejected_intents")
        elif change == "unknown":
            damaged["state"]["fields"]["unknown"] = None
        else:
            damaged["schema_version"] = 1
        with pytest.raises(ValidationError):
            MarginStrategyState.parse(damaged)
    legacy = deepcopy(document)
    legacy["schema_version"] = 1
    legacy["state"]["fields"].pop("rejected_intents")
    assert MarginStrategyState.parse(legacy) == state
    path = next(strategy_root.glob("state/margin-strategies/*.json"))
    from hashlib import sha256

    raw = json.dumps(
        {
            "data": legacy,
            "sha256": sha256(
                json.dumps(legacy, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            ).hexdigest(),
        }
    ).encode()
    path.write_bytes(raw)
    store = FileMarginStrategyStore(strategy_root, state.strategy_id)
    try:
        assert store.load(state.strategy_id) == state and path.read_bytes() == raw
        store.save(state, expected_revision=state.revision)
        assert path.read_bytes() == raw  # Exact ACK retry does not rewrite a legacy source.
    finally:
        store.close()
