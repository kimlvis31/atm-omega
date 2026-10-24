import json
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.ledger import CashLedger, Reservations
from omega.domain.models import (
    Candle,
    Fill,
    Instrument,
    Market,
    OrderKind,
    OrderRequest,
    Side,
    decimal,
)
from omega.domain.safety import BlockReason, EntryGate

SPOT = Instrument("binance", Market.SPOT, "BTCSYNTH")


def fill(trade_id="1", side=Side.BUY, qty="1", price="100", fee="0.1"):
    return Fill("account", SPOT, trade_id, "order", side, D(qty), D(price), D(fee), "SYNTH", 0)


@pytest.mark.parametrize("value", [float("nan"), 1.1, True, "NaN", "Infinity", "bad"])
def test_money_rejects_float_and_nonfinite(value):
    with pytest.raises(ValidationError):
        decimal(value)


def test_cash_ledger_against_independent_fixture():
    cases = json.loads(Path("docs/design/fixtures/first-slice-cases.json").read_text())
    case = cases["cases"][0]
    ledger = CashLedger("account", SPOT, "SYNTH", D(case["initial"]["cash"]))
    for step in case["steps"]:
        event = fill(
            step["fill_id"], Side(step["side"]), step["quantity"], step["price"], step["fee"]
        )
        applied = ledger.apply(event)
        assert applied is not step["expected"].get("duplicate", False)
        for name in ["cash", "quantity", "book_cost", "realized_gross_pnl"]:
            if name in step["expected"]:
                assert getattr(ledger, name) == D(step["expected"][name])
    assert ledger.cash - D("2000") == D("-14.012")
    assert ledger.fees["SYNTH"] == D("2.012")


def test_conflicting_fill_identity_is_not_silently_deduplicated():
    ledger = CashLedger("account", SPOT, "SYNTH", D("1000"))
    ledger.apply(fill())
    with pytest.raises(ValidationError):
        ledger.apply(replace(fill(), quantity=D("2")))
    assert ledger.quantity == D("1")


def test_sell_exceeding_inventory_is_atomic():
    ledger = CashLedger("account", SPOT, "SYNTH", D("1000"))
    with pytest.raises(ValidationError):
        ledger.apply(fill(side=Side.SELL))
    assert ledger.cash == D("1000") and ledger.quantity == 0


@given(st.integers(1, 100_000), st.integers(1, 10_000))
def test_duplicate_fill_has_exactly_one_effect(qty, price):
    ledger = CashLedger("account", SPOT, "SYNTH", D(qty * price + 1))
    event = fill(qty=str(qty), price=str(price), fee="0")
    assert ledger.apply(event)
    assert not ledger.apply(event)
    assert ledger.cash == 1 and ledger.quantity == qty


def test_shared_reservation_partial_and_confirmed_cancel():
    reservations = Reservations(D("1000"))
    reservations.reserve("A", D("700"))
    with pytest.raises(ValidationError):
        reservations.reserve("B", D("400"))
    reservations.consume("A", D("200"))
    assert reservations.cash == 800 and reservations.available == 300
    # Sending a cancel request has no release method; only terminal confirmation does.
    assert reservations.reserved == 500
    reservations.terminal("A")
    assert reservations.available == 800


def test_data_recovery_preserves_manual_pause_and_restart():
    gate = EntryGate()
    gate.block(BlockReason.PAUSE)
    gate.block(BlockReason.DATA)
    gate.recovered(BlockReason.DATA)
    assert gate.reasons == {BlockReason.RESTART, BlockReason.PAUSE}
    with pytest.raises(TradingBlocked):
        gate.resume(reconciled=True, durable=True)
    gate.recovered(BlockReason.RESTART)
    gate.resume(reconciled=True, durable=True)
    assert gate.allows_new_risk


@pytest.mark.parametrize("reason", [BlockReason.STORAGE, BlockReason.MODEL, BlockReason.EXECUTION])
def test_technical_recovery_requires_manual_resume(reason):
    gate = EntryGate()
    gate.recovered(BlockReason.RESTART)
    gate.resume(reconciled=True, durable=True)
    gate.block(reason)
    gate.recovered(reason)
    assert not gate.allows_new_risk
    with pytest.raises(TradingBlocked):
        gate.resume(reconciled=True, durable=False)
    gate.resume(reconciled=True, durable=True)
    assert gate.allows_new_risk


def test_external_intervention_needs_explicit_reclaim():
    gate = EntryGate()
    gate.block(BlockReason.EXTERNAL)
    with pytest.raises(TradingBlocked):
        gate.recovered(BlockReason.EXTERNAL)
    with pytest.raises(TradingBlocked):
        gate.resume(reconciled=True, durable=True)
    gate.reclaim(reconciled=True, durable=True, strategy_ready=True)
    assert BlockReason.EXTERNAL not in gate.reasons
    assert BlockReason.PAUSE in gate.reasons


@pytest.mark.parametrize("qty,price", [("0", "100"), ("-1", "100"), ("1", "NaN")])
def test_invalid_orders_fail_before_a_gateway(qty, price):
    with pytest.raises(ValidationError):
        OrderRequest(SPOT, "operation", Side.BUY, OrderKind.LIMIT, D(qty), D(price))


def test_candle_ohlc_and_minute_boundary():
    candle = Candle(SPOT, 0, 60_000, D(100), D(110), D(90), D(105), D(1))
    with pytest.raises(ValidationError):
        replace(candle, high=D(95))
    with pytest.raises(ValidationError):
        replace(candle, close_time_ms=59_999)


def test_base_asset_commission_changes_actual_inventory():
    ledger = CashLedger("account", SPOT, "SYNTH", D("1000"), base_asset="BTC")
    ledger.apply(replace(fill(qty="2", fee="0.01"), fee_asset="BTC"))
    assert ledger.quantity == D("1.99") and ledger.cash == D("800")
    assert ledger.book_cost == D("199") and ledger.base_fee_cost_in_quote == D("1")
    ledger.apply(replace(fill("2", Side.SELL, qty="1", price="110", fee="0.01"), fee_asset="BTC"))
    assert ledger.quantity == D("0.98") and ledger.cash == D("910")
    assert ledger.book_cost == D("98") and ledger.base_fee_cost_in_quote == D("2")


def test_third_asset_commission_is_not_mixed_with_quote_money():
    ledger = CashLedger("account", SPOT, "SYNTH", D("1000"), base_asset="BTC")
    ledger.apply(replace(fill(fee="0.03"), fee_asset="BNB"))
    assert ledger.cash == D("900") and ledger.quantity == 1
    assert ledger.fees == {"BNB": D("0.03")}


def test_string_quantity_is_rejected_at_typed_domain_boundary():
    with pytest.raises(ValidationError):
        OrderRequest(SPOT, "operation", Side.BUY, OrderKind.MARKET, "1")
