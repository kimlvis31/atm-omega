"""Actual Fill records and JSONL page recovery with a memory-only read gateway."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.application.execution_fills import FACT, PAGE, ExecutionFillRecovery
from omega.domain.errors import JournalError, ProtocolError, TradingBlocked, ValidationError
from omega.domain.ledger import CashLedger
from omega.domain.models import (
    Fill,
    Instrument,
    Market,
    OrderKind,
    OrderObservation,
    OrderRequest,
    OrderStatus,
    Side,
)
from omega.domain.safety import BlockReason, EntryGate
from omega.infrastructure.journal import FailoverJournal, JsonlJournal
from omega.ports.journal import Durability

BTC = Instrument("binance", Market.SPOT, "BTCUSDT")


class Broker:
    account_id = "A"

    def __init__(self, pages):
        self.pages, self.calls = list(pages), []

    def trades(self, instrument, *, from_id):
        self.calls.append((instrument, from_id))
        assert self.pages, "retry must not refetch its frozen pending page"
        result = self.pages.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class Sink:
    def __init__(self):
        self.events, self.calls = [], []
        self.fail_at, self.durability = None, Durability.PRIMARY

    def records(self):
        return tuple(deepcopy(self.events))

    def record(self, event):
        self.calls.append(deepcopy(event))
        self.events.append(deepcopy(event))
        if len(self.calls) == self.fail_at:
            raise OSError("synthetic primary ACK lost after append")
        return self.durability


def owned(*, instrument=BTC, quantity="10000", executed="5000", side=Side.BUY, order="7"):
    request = OrderRequest(instrument, "owned-" + order, side, OrderKind.MARKET, D(quantity))
    status = OrderStatus.FILLED if D(quantity) == D(executed) else OrderStatus.PARTIALLY_FILLED
    observation = OrderObservation(
        instrument, request.client_order_id, order, status, D(executed), D(executed) * 100
    )
    return request, observation


def fill(identity="1", **changes):
    original = Fill("A", BTC, identity, "7", Side.BUY, D(1), D(100), D(".1"), "USDT", 60000)
    return replace(original, **changes)


def owner(pages=(), *, sink=None, pairs=None, initial=0):
    sink = sink if sink is not None else Sink()
    broker, gate = Broker(pages), EntryGate()
    gate.recovered(BlockReason.RESTART)
    gate.resume(reconciled=True, durable=True)
    recovery = ExecutionFillRecovery(
        "A", BTC, broker, sink, gate, initial_from_id=initial, owned=pairs or (owned(),)
    )
    return recovery, broker, sink, gate


def test_full_facts_precede_cursor_and_inclusive_queries_do_not_skip_or_duplicate():
    recovery, broker, sink, gate = owner(((fill(), fill("2")), (fill("2"), fill("3"))))
    first = recovery.recover_page()
    assert [event["kind"] for event in sink.events] == [FACT, FACT, PAGE]
    assert first.from_id == 0 and first.through_id == 2 and first.revision == 1
    assert first.new_facts == (fill(), fill("2"))
    second = recovery.recover_page()
    assert broker.calls == [(BTC, 0), (BTC, 2)]
    assert second.new_facts == (fill("3"),) and second.through_id == 3
    assert second.primary_acknowledged and second.recovery_required
    assert not second.history_complete and BlockReason.EXECUTION in gate.reasons
    assert sink.events[-1]["previous_sha256"] == first.page_sha256
    assert recovery.history.orphan_trade_ids == ()


def test_identical_duplicate_inside_one_page_has_one_fact_and_one_quantity():
    recovery, _broker, sink, _gate = owner(
        ((fill(), fill()),), pairs=(owned(quantity="1", executed="1"),)
    )
    receipt = recovery.recover_page()
    assert receipt.new_facts == receipt.facts == (fill(),)
    assert [event["kind"] for event in sink.events] == [FACT, PAGE]
    assert sink.events[-1]["trade_ids"] == ["1"] and sink.events[-1]["returned_count"] == 2


@pytest.mark.parametrize("fail_at", [1, 2, 3])
def test_same_owner_ack_loss_retries_exact_page_without_another_query(fail_at):
    recovery, broker, sink, _gate = owner(((fill(), fill("2")),))
    sink.fail_at = fail_at
    with pytest.raises(TradingBlocked, match="acknowledgement"):
        recovery.recover_page()
    assert recovery.history.cursor == 0 and not recovery.history.facts
    attempted = deepcopy(sink.calls)
    sink.fail_at = None
    receipt = recovery.recover_page()
    assert broker.calls == [(BTC, 0)] and receipt.new_facts == (fill(), fill("2"))
    assert sink.calls[fail_at:][:fail_at] == attempted


@pytest.mark.parametrize("fault", ["pending_fact", "missing_original"])
def test_pending_retry_revalidates_exact_original_and_partial_history_without_refetch(fault):
    recovery, broker, sink, gate = owner(((fill(),), (fill(), fill("2"))))
    recovery.recover_page()
    sink.fail_at = len(sink.calls) + 2  # Original repeat + second fill; second ACK lost.
    with pytest.raises(TradingBlocked):
        recovery.recover_page()
    assert BlockReason.STORAGE in gate.reasons
    if fault == "pending_fact":
        sink.events[-1] = recovery._fact_event(fill("2", price=D(101)))
    else:
        sink.events = [
            event
            for event in sink.events
            if not (event["kind"] == FACT and event["fill"]["trade_id"] == "1")
        ]
    before = len(sink.calls)
    sink.fail_at = None
    with pytest.raises((JournalError, ProtocolError)):
        recovery.recover_page()
    assert len(sink.calls) == before and broker.calls == [(BTC, 0), (BTC, 1)]
    assert recovery.history.cursor == 1 and recovery.history.facts == (fill(),)
    assert BlockReason.EXECUTION in gate.reasons


def test_restart_preserves_orphan_facts_and_requeries_original_inclusive_cursor():
    recovery, _broker, sink, _gate = owner(((fill(), fill("2")),))
    sink.fail_at = 1
    with pytest.raises(TradingBlocked):
        recovery.recover_page()
    restarted, broker, sink, gate = owner(((fill(), fill("2")),), sink=sink)
    history = restarted.history
    assert history.recovery_required and history.cursor == 0 and history.orphan_trade_ids == ("1",)
    assert history.facts == (fill(),) and len(sink.calls) == 1
    sink.fail_at = None
    receipt = restarted.recover_page()
    assert broker.calls == [(BTC, 0)] and receipt.new_facts == (fill(), fill("2"))
    assert BlockReason.EXECUTION in gate.reasons


def test_restart_after_page_ack_loss_allows_later_growth_but_never_claims_old_primary_ack():
    recovery, _broker, sink, _gate = owner(((fill(),),))
    sink.fail_at = 2
    with pytest.raises(TradingBlocked):
        recovery.recover_page()
    restarted, broker, sink, _gate = owner(((fill(), fill("2")),), sink=sink)
    assert restarted.history.recovery_required and restarted.history.cursor == 1
    assert not hasattr(restarted.history, "primary_acknowledged")
    sink.fail_at = None
    receipt = restarted.recover_page()
    assert broker.calls == [(BTC, 1)] and receipt.new_facts == (fill("2"),)
    assert receipt.revision == 2 and receipt.primary_acknowledged


@pytest.mark.parametrize(
    "durability", [Durability.FALLBACK, Durability.VOLATILE, "PRIMARY_COMMITTED"]
)
def test_nonprimary_or_untyped_fact_ack_never_advances_the_exposed_cursor(durability):
    recovery, broker, sink, _gate = owner(((fill(),),))
    sink.durability = durability
    with pytest.raises(TradingBlocked):
        recovery.recover_page()
    assert recovery.history.cursor == 0
    sink.durability = Durability.PRIMARY
    assert recovery.recover_page().through_id == 1 and len(broker.calls) == 1


def test_primary_facts_without_primary_page_ack_do_not_publish_cursor(monkeypatch):
    recovery, broker, sink, _gate = owner(((fill(),),))
    original_record = sink.record

    def fallback_page(event):
        original_record(event)
        return Durability.FALLBACK if event["kind"] == PAGE else Durability.PRIMARY

    monkeypatch.setattr(sink, "record", fallback_page)
    with pytest.raises(TradingBlocked):
        recovery.recover_page()
    assert recovery.history.cursor == 0 and recovery.history.facts == ()
    monkeypatch.setattr(sink, "record", original_record)
    assert recovery.recover_page().through_id == 1
    assert broker.calls == [(BTC, 0)]


def test_full_page_hints_more_but_empty_short_pages_never_claim_universal_completeness():
    full = tuple(fill(str(index)) for index in range(1000))
    recovery, broker, _sink, _gate = owner((full, (fill("999"),), ()))
    assert recovery.recover_page().needs_more is True
    short = recovery.recover_page()
    empty = recovery.recover_page()
    assert not short.needs_more and not short.new_facts
    assert not empty.needs_more and not empty.history_complete and empty.through_id == 999
    assert broker.calls == [(BTC, 0), (BTC, 999), (BTC, 999)]


@pytest.mark.parametrize(
    "damage", ["foreign_account", "foreign_instrument", "unknown_order", "side"]
)
def test_unowned_page_blocks_external_control_before_any_write(damage):
    values = {
        "foreign_account": {"account_id": "B"},
        "foreign_instrument": {"instrument": Instrument("binance", Market.SPOT, "ETHUSDT")},
        "unknown_order": {"provider_order_id": "foreign"},
        "side": {"side": Side.SELL},
    }
    recovery, _broker, sink, gate = owner(((fill(**values[damage]),),))
    with pytest.raises((TradingBlocked, ProtocolError)):
        recovery.recover_page()
    assert not sink.calls and recovery.history.cursor == 0
    assert BlockReason.EXTERNAL in gate.reasons


def test_exact_cumulative_quantity_distinguishes_original_excess_from_stale_observation():
    pages = ((fill(), fill("2")),)
    recovery, _broker, sink, gate = owner(pages, pairs=(owned(quantity="1", executed="1"),))
    with pytest.raises(ProtocolError, match="original"):
        recovery.recover_page()
    assert not sink.calls and BlockReason.EXTERNAL in gate.reasons
    recovery, _broker, sink, gate = owner(pages, pairs=(owned(quantity="2", executed="1"),))
    with pytest.raises(TradingBlocked, match="refresh"):
        recovery.recover_page()
    assert not sink.calls and BlockReason.EXECUTION in gate.reasons
    assert BlockReason.EXTERNAL not in gate.reasons


def test_changed_duplicate_fill_is_not_discarded_or_rewritten():
    recovery, _broker, sink, _gate = owner(((fill(),), (fill(price=D(101)),)))
    recovery.recover_page()
    before = deepcopy(sink.events)
    with pytest.raises(ProtocolError, match="conflicting"):
        recovery.recover_page()
    assert sink.events == before and recovery.history.cursor == 1


def test_cumulative_comparison_is_exact_beyond_default_decimal_context_precision():
    recovery, _broker, sink, gate = owner(
        ((fill(quantity=D("1e50")), fill("2")),),
        pairs=(owned(quantity="1e50", executed="1e50"),),
    )
    with pytest.raises(ProtocolError, match="original"):
        recovery.recover_page()
    assert not sink.calls and BlockReason.EXTERNAL in gate.reasons


def test_equivalent_decimal_spellings_remain_one_fact_across_pages_and_reopen():
    first = fill(quantity=D("1.00"), price=D("1E2"), fee=D(".100"))
    second = fill(quantity=D("1"), price=D("100.00"), fee=D(".1"))
    recovery, _broker, sink, _gate = owner(((first,), (second,)))
    recovery.recover_page()
    assert not recovery.recover_page().new_facts
    restarted, _broker, _sink, _gate = owner((), sink=sink)
    assert restarted.history.facts == (first,) and restarted.history.revision == 2


def test_signed_zero_commission_remains_zero_and_rebate_remains_negative():
    original = fill(fee=D("-0.000"))
    recovery, _broker, sink, _gate = owner(
        ((original,), (fill(fee=D("0")), fill("2", fee=D("-.01"))))
    )
    recovery.recover_page()
    assert recovery.recover_page().new_facts == (fill("2", fee=D("-.01")),)
    assert sink.events[0]["fill"]["fee"] == "0"
    restarted, _broker, _sink, _gate = owner((), sink=sink)
    assert [value.fee for value in restarted.history.facts] == [D(0), D("-.01")]


@pytest.mark.parametrize("damage", ["fact", "page", "chain", "missing_fact"])
def test_history_conflicts_across_primary_fallback_order_are_detected(damage):
    recovery, _broker, sink, _gate = owner(((fill(),),))
    recovery.recover_page()
    if damage == "fact":
        sink.events.append(deepcopy(sink.events[0]))
        sink.events[-1]["fill"]["price"] = "101"
    elif damage == "page":
        sink.events.append(deepcopy(sink.events[-1]))
        sink.events[-1]["through_id"] = "2"
    elif damage == "chain":
        sink.events[-1]["previous_sha256"] = "0" * 64
    else:
        sink.events.pop(0)
    with pytest.raises((JournalError, ProtocolError)):
        owner((), sink=sink)


def test_reordered_identical_primary_fallback_records_keep_original_chain():
    recovery, _broker, sink, _gate = owner(((fill(),), (fill(), fill("2"))))
    recovery.recover_page()
    recovery.recover_page()
    sink.events = list(reversed(sink.events)) + deepcopy(sink.events)
    reopened, _broker, _sink, _gate = owner((), sink=sink)
    assert reopened.history.cursor == 2 and reopened.history.facts == (fill(), fill("2"))


def test_synthetic_spot_ledger_replay_is_explicit_and_preserves_all_commission_assets(tmp_path):
    facts = (
        fill(),
        fill("2", fee=D(".001"), fee_asset="BTC"),
        fill("3", fee=D(".01"), fee_asset="BNB"),
        fill("4", provider_order_id="8", side=Side.SELL, price=D(110), fee=D("-.02")),
    )
    sink = FailoverJournal(
        JsonlJournal(tmp_path / "primary.jsonl"),
        JsonlJournal(tmp_path / "fallback.jsonl"),
        lambda _: None,
    )
    pairs = (
        owned(quantity="3", executed="3"),
        owned(quantity="1", executed="1", side=Side.SELL, order="8"),
    )
    recovery, _broker, _sink, _gate = owner((facts,), sink=sink, pairs=pairs)
    recovery.recover_page()
    reopened, _broker, _sink, _gate = owner((), sink=sink, pairs=pairs)
    assert reopened.history.recovery_required
    ledger = CashLedger("A", BTC, "USDT", D(1000), base_asset="BTC")
    for fact in reopened.history.facts:
        assert ledger.apply(fact) is True
        assert ledger.apply(fact) is False
    assert ledger.cash == D("809.92") and ledger.quantity == D("1.999")
    assert ledger.fees == {"USDT": D(".08"), "BTC": D(".001"), "BNB": D(".01")}
    assert reopened.history.facts[-1].fee == D("-.02")


@pytest.mark.parametrize(
    "fault", ["oversized", "not_tuple", "numeric_id", "native_time", "before_cursor"]
)
def test_invalid_page_is_fully_rejected_before_any_fact_write(fault):
    if fault == "oversized":
        page = tuple(fill(str(index)) for index in range(1001))
    elif fault == "not_tuple":
        page = [fill()]
    elif fault == "before_cursor":
        page = (fill("0"),)
    else:
        forged = fill()
        object.__setattr__(
            forged,
            "trade_id" if fault == "numeric_id" else "timestamp_ms",
            "01" if fault == "numeric_id" else True,
        )
        page = (forged,)
    recovery, _broker, sink, _gate = owner((page,), initial=1)
    with pytest.raises((ValidationError, ProtocolError)):
        recovery.recover_page()
    assert not sink.calls and recovery.history.cursor == 1


def test_omitted_recorded_orphan_cannot_be_skipped_by_cursor_advance():
    recovery, _broker, sink, _gate = owner(((fill("2"),),))
    sink.fail_at = 1
    with pytest.raises(TradingBlocked):
        recovery.recover_page()
    restarted, _broker, _sink, _gate = owner(((fill("3"),),), sink=sink)
    before = len(sink.calls)
    with pytest.raises(ProtocolError, match="orphan"):
        restarted.recover_page()
    assert len(sink.calls) == before and restarted.history.orphan_trade_ids == ("2",)


def test_constructor_history_storage_failure_preserves_both_storage_and_execution_gates():
    class FailedRead(Sink):
        def records(self):
            raise JournalError("original history unreadable")

    broker, sink, gate = Broker(()), FailedRead(), EntryGate()
    with pytest.raises(JournalError):
        ExecutionFillRecovery("A", BTC, broker, sink, gate, initial_from_id=0, owned=(owned(),))
    assert BlockReason.STORAGE in gate.reasons and BlockReason.EXECUTION in gate.reasons
    assert not broker.calls and not sink.calls
