"""V1 shutdown uses real owner persistence and explicit synthetic market minutes."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.application.paper_state import validate_state
from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.execution_rules import DecimalRange, ExecutionRulesSnapshot
from omega.domain.models import Candle, Instrument, Market
from omega.research.artifacts import read_json, write_json
from omega.strategies.registry import StrategyRegistry

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def bar(index, price="100"):
    return {
        "open_time_ms": index * 60000,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": "10",
    }


def step(service, index, price="100"):
    return service.command(
        "paper.step", {"instrument": INSTRUMENT.key, "candle": bar(index, price)}
    )


def create(tmp_path, *, quantity="1", market=Market.USDM):
    path, now = tmp_path / "paper.json", [120100]
    instrument = replace(INSTRUMENT, market=market)
    service = PaperTradingService(
        StrategyRegistry().create,
        create_features,
        persist=lambda state: write_json(path, state),
        clock_ms=lambda: now[0],
    )
    service.command("paper.account.create", {"account_id": "a", "balance": "10000"})
    service.command(
        "paper.strategy.add",
        {
            "instance": "s",
            "account_id": "a",
            "instrument": instrument.key,
            "budget": "1000",
            "module_id": "target_quantity",
            "configuration": {"quantity": quantity},
        },
    )
    service.command("resume", {})
    for index in range(2):
        service.command("paper.step", {"instrument": instrument.key, "candle": bar(index)})
    return service, path, now


class BrokenPlugin:
    def __deepcopy__(self, memo):
        raise AssertionError("shutdown must not copy a plugin")

    def checkpoint(self):
        raise AssertionError("shutdown must not checkpoint a plugin")


@pytest.mark.parametrize(
    "quantity,market,exit_side",
    [
        ("1", Market.USDM, "SELL"),
        ("-1", Market.USDM, "BUY"),
        ("1", Market.SPOT, "SELL"),
    ],
)
def test_full_reduction_is_durable_causal_and_model_independent(
    tmp_path, quantity, market, exit_side
):
    service, path, now = create(tmp_path, quantity=quantity, market=market)
    old = deepcopy(service.data["strategies"]["s"])
    service.engines["s"] = service.features["s"] = BrokenPlugin()
    assert service.shutdown_snapshot()["positions"][0]["quantity"] == quantity
    result = service.shutdown_drain("exit-1", now[0])
    row = service.data["strategies"]["s"]
    assert not result["duplicate"] and row["quantity"] == quantity
    assert row["status"] == "recovery_required" and service.data["paused"]
    assert row["pending"]["side"] == exit_side and row["pending"]["quantity"] == "1"
    assert row["pending"]["reducing"] and row["pending"]["eligible_at_ms"] == now[0]
    assert row.get("runtime_checkpoint") == old.get("runtime_checkpoint")
    assert row["last_candle"] == old["last_candle"] and row["fills"] == old["fills"]
    assert read_json(path) == service.data and service.engines == service.features == {}
    assert not service.feed_snapshot()["recovery_required"]
    instrument = replace(INSTRUMENT, market=market)
    # Bar opening120000 predates actual issue120100: even its close cannot fill this request.
    service.command("paper.step", {"instrument": instrument.key, "candle": bar(2, "95")})
    assert row["quantity"] == quantity  # Previous committed object remains unchanged as well.
    assert service.data["strategies"]["s"]["pending"]
    service.command("paper.step", {"instrument": instrument.key, "candle": bar(3, "90")})
    final = service.data["strategies"]["s"]
    assert D(final["quantity"]) == 0 and final["pending"] is None
    assert final["fills"][-1]["timestamp_ms"] == 180000
    assert final["status"] == "recovery_required" and len(final["fills"]) == len(old["fills"]) + 1
    validate_state(read_json(path))


def test_same_operation_is_noop_and_changed_issue_conflicts_after_fill(tmp_path):
    service, path, now = create(tmp_path)
    service.shutdown_drain("exit-1", now[0])
    frozen = path.read_bytes()
    first = service.shutdown_snapshot()["pending_orders"][0]["order_id"]
    assert service.shutdown_drain("exit-1", now[0])["duplicate"]
    assert first == service.shutdown_snapshot()["pending_orders"][0]["order_id"]
    assert path.read_bytes() == frozen
    with pytest.raises(ValidationError, match="issue time conflicts"):
        service.shutdown_drain("exit-1", now[0] + 1)
    step(service, 2)
    step(service, 3)
    frozen = path.read_bytes()
    assert service.shutdown_drain("exit-1", now[0])["duplicate"]
    assert path.read_bytes() == frozen and service.shutdown_snapshot()["positions"] == []


def test_cancel_keeps_reduction_pause_and_original_checkpoint(tmp_path):
    service, path, now = create(tmp_path)
    service.shutdown_drain("exit-1", now[0])
    frozen = path.read_bytes()
    before = deepcopy(service.data)
    assert service.shutdown_cancel("exit-1")["reductions_retained"]
    assert service.data == before and path.read_bytes() == frozen


def test_flat_pending_only_is_canceled_without_inventing_a_price_or_fill(tmp_path):
    service, path, now = create(tmp_path)
    row = service.data["strategies"]["s"]
    row.update(
        quantity="0",
        entry_price="0",
        pending={
            "side": "BUY",
            "kind": "MARKET",
            "quantity": "1",
            "price": None,
            "reducing": False,
            "reason": "old-entry",
            "reserved": "100.1",
            "eligible_at_ms": 180000,
        },
    )
    service.data["candles"].clear()
    fills = deepcopy(row["fills"])
    service.shutdown_drain("pending-only", now[0])
    state = service.shutdown_snapshot()
    assert state["positions"] == state["pending_orders"] == [] and state["durable"]
    assert service.data["strategies"]["s"]["fills"] == fills
    assert read_json(path) == service.data


@pytest.mark.parametrize("fault", ["no_price", "stale", "future_issue", "dust", "missing_rules"])
def test_preflight_failure_preserves_all_old_protection_and_plugin(tmp_path, fault):
    service, path, now = create(tmp_path)
    # A pre-existing protective request must survive every rejected drain.
    row = service.data["strategies"]["s"]
    row["pending"] = {
        "side": "SELL",
        "kind": "LIMIT",
        "quantity": "1",
        "price": "110",
        "reducing": True,
        "reason": "old-protection",
        "reserved": "0",
        "eligible_at_ms": 120000,
    }
    if fault == "no_price":
        service.data["candles"].clear()
    if fault == "stale":
        now[0] += 60001
    if fault == "dust":
        row["quantity"], row["pending"]["quantity"] = "1.0001", "1.0001"
    if fault == "missing_rules":
        service.data["feed"]["enabled"] = True
        service.data["feed"]["provenance"][INSTRUMENT.key] = "database_committed"
    before, saved = deepcopy(service.data), path.read_bytes()
    engine = service.engines["s"]
    with pytest.raises((TradingBlocked, ValidationError)):
        service.shutdown_drain("exit-1", now[0] + (1 if fault == "future_issue" else 0))
    assert service.data == before and path.read_bytes() == saved
    assert service.engines["s"] is engine and service.shutdown_snapshot()["durable"]


def test_all_rows_preflight_before_any_cancel_and_quarantine_is_known(tmp_path):
    service, path, now = create(tmp_path)
    original = service.data["strategies"]["s"]
    original["status"] = "quarantined"
    assert service.shutdown_snapshot()["ready"]
    other = deepcopy(original)
    other["instrument"] = "binance:usdm:BTCUSDT"
    service.data["strategies"]["t"] = other
    before = deepcopy(service.data)
    with pytest.raises(TradingBlocked, match="no observed"):
        service.shutdown_drain("exit-1", now[0])
    assert service.data == before


def test_ack_loss_fences_every_stale_writer_and_restart_reads_actual_finance(tmp_path):
    service, path, now = create(tmp_path)
    original = deepcopy(service.data)

    def lost_ack(state):
        write_json(path, state)
        raise OSError("ACK lost after atomic publication")

    service.persist = lost_ack
    with pytest.raises(OSError, match="ACK lost"):
        service.shutdown_drain("exit-1", now[0])
    assert service.data == original
    saved = path.read_bytes()
    snapshot = service.shutdown_snapshot()
    assert not snapshot["ready"] and not snapshot["durable"]
    assert "paper_recording_unconfirmed" in snapshot["unknown"]
    assert service.command("pause", {}) == {"paused": True, "durable": False}
    for action in [
        lambda: service.command("resume", {}),
        lambda: step(service, 2),
        lambda: service.shutdown_drain("exit-1", now[0]),
    ]:
        with pytest.raises(TradingBlocked, match="unconfirmed"):
            action()
    assert path.read_bytes() == saved
    restored = PaperTradingService(
        lambda *args: pytest.fail("restart must not build a plugin"),
        create_features,
        state=read_json(path),
        persist=lambda state: write_json(path, state),
        clock_ms=lambda: now[0],
    )
    assert restored.shutdown_drain("exit-1", now[0])["duplicate"]
    assert restored.data["strategies"]["s"]["pending"]["reducing"]
    step(restored, 2)
    step(restored, 3)
    assert restored.shutdown_snapshot()["positions"] == []


def test_emergency_pause_preserves_confirmed_checkpoint_without_touching_plugin(tmp_path):
    service, path, _ = create(tmp_path)
    previous = deepcopy(service.data)
    service.engines["s"] = service.features["s"] = BrokenPlugin()
    assert service.command("pause", {}) == {"paused": True}
    assert service.data["strategies"] == previous["strategies"]
    assert read_json(path) == service.data and service.data["paused"]


def test_pause_ack_loss_remains_volatile_and_never_republishes_stale_pending(tmp_path):
    service, path, _ = create(tmp_path)

    def lose_ack(state):
        write_json(path, state)
        raise OSError("pause ACK lost")

    service.persist = lose_ack
    with pytest.raises(OSError, match="pause ACK lost"):
        service.command("pause", {})
    assert service.data["paused"]
    saved = path.read_bytes()
    service.persist = lambda state: pytest.fail("stale paused state must not be republished")
    assert service.command("pause", {}) == {"paused": True, "durable": False}
    assert path.read_bytes() == saved
    assert not service.shutdown_snapshot()["durable"]


def test_unknown_financial_state_is_not_hidden_as_flat_or_model_fault(tmp_path):
    service, _, _ = create(tmp_path)
    service.data["strategies"]["s"]["runtime_checkpoint"] = {"unrestorable_model": True}
    assert service.shutdown_snapshot()["ready"]
    service.data["strategies"]["s"]["quantity"] = "NaN"
    snapshot = service.shutdown_snapshot()
    assert not snapshot["ready"] and not snapshot["durable"] and snapshot["unknown"]


def test_database_feed_settles_detached_shutdown_with_pinned_rules(tmp_path):
    service, path, now = create(tmp_path)
    snapshot = ExecutionRulesSnapshot(
        INSTRUMENT,
        120100,
        "binance:usdm:exchangeInfo",
        "TRADING",
        DecimalRange(step=D(".001")),
        None,
        DecimalRange(step=D(".01")),
        reductions_allowed=True,
    )
    service.rules_provider = lambda key, *, max_age_ms: snapshot
    service.data["feed"].update(
        enabled=True,
        start_ms=120000,
        warmed={"s": True},
        provenance={INSTRUMENT.key: "database_committed"},
    )
    service.shutdown_drain("exit-1", now[0])
    pending = service.data["strategies"]["s"]["pending"]
    assert pending["execution"]["snapshot"] == snapshot.document()
    assert not pending["execution"]["retrospective"]
    validate_state(read_json(path))
    for index in (2, 3):
        candle = Candle(
            INSTRUMENT, index * 60000, (index + 1) * 60000, D(95), D(95), D(95), D(95), D(1)
        )
        service.feed_apply(
            (candle,),
            expected=service.feed_snapshot(),
            entries_allowed=False,
            reason="shutdown",
            now_ms=candle.close_time_ms,
        )
    assert service.shutdown_snapshot()["positions"] == []
    validate_state(read_json(path))


def test_missing_current_rules_use_known_reduction_rules_with_explicit_uncertainty(tmp_path):
    service, path, now = create(tmp_path)
    old = ExecutionRulesSnapshot(
        INSTRUMENT,
        0,
        "binance:usdm:exchangeInfo",
        "TRADING",
        DecimalRange(step=D(".001")),
        None,
        DecimalRange(step=D(".01")),
        reductions_allowed=True,
    )
    service.rules_provider = lambda key, *, max_age_ms: None
    service.data["strategies"]["s"]["execution_rules_snapshot"] = old.document()
    service.data["feed"].update(enabled=True, provenance={INSTRUMENT.key: "database_committed"})
    service.shutdown_drain("stale-known-reduce", now[0])
    pin = service.data["strategies"]["s"]["pending"]["execution"]
    assert pin["snapshot"] == old.document()
    assert "current_execution_rules_unavailable" in pin["unchecked"]
    validate_state(read_json(path))
