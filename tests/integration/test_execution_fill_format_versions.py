"""Real JSONL: frozen v1 bytes survive explicit separate v2 reported-fact scans."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
from pathlib import Path

import pytest

from omega.application.execution_fills import ExecutionFillRecovery
from omega.domain.errors import JournalError, TradingBlocked, ValidationError
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

FIXTURE = json.loads(
    (Path(__file__).resolve().parents[1] / "fixtures/execution-fill-v1.json").read_text(
        encoding="utf-8"
    )
)
ACCOUNT = FIXTURE["expected"]["account_id"]


class Broker:
    account_id = ACCOUNT

    def __init__(self, *pages):
        self.pages, self.calls = list(pages), []

    def trades(self, instrument, *, from_id):
        self.calls.append(from_id)
        assert self.pages, "uncertain page write must retry without another query"
        return self.pages.pop(0)


def fixture_sink(root):
    primary = JsonlJournal(root / "primary.jsonl")
    sink = FailoverJournal(primary, JsonlJournal(root / "fallback.jsonl"), lambda _: None)
    return sink, primary


def owned(market=Market.SPOT):
    instrument = Instrument("binance", market, "BTCUSDT")
    request = OrderRequest(instrument, "legacy-format-fixture", Side.BUY, OrderKind.MARKET, D(2))
    observation = OrderObservation(
        instrument, request.client_order_id, "7", OrderStatus.FILLED, D(2), D(200)
    )
    return request, observation


def fact(identity="1", market=Market.SPOT, *, reported=True):
    return Fill(
        ACCOUNT,
        owned(market)[0].instrument,
        identity,
        "7",
        Side.BUY,
        D(1),
        D(100),
        D("0.1"),
        "USDT",
        60000 + int(identity) - 1,
        quote_quantity=D("100.0000") if reported else None,
        realized_pnl=D(0) if market == Market.USDM and reported else None,
        realized_pnl_asset="USDT" if market == Market.USDM and reported else None,
    )


def owner(broker, sink, *, market=Market.SPOT, version=1):
    request, observation = owned(market)
    gate = EntryGate()
    return ExecutionFillRecovery(
        ACCOUNT,
        request.instrument,
        broker,
        sink,
        gate,
        initial_from_id=0,
        owned=((request, observation),),
        format_version=version,
    ), gate


def test_frozen_original_v1_records_and_hashes_replay_without_new_metadata(tmp_path):
    assert (
        sha256(json.dumps(FIXTURE, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        == "b42a230e6f76fc77f130b88bfadc06ae5f8beecb0d0b671f65e3f0643bfe807f"
    )
    sink, primary = fixture_sink(tmp_path)
    for event in FIXTURE["events"]:
        primary.append(deepcopy(event))
    before = primary.path.read_bytes()
    recovery, gate = owner(Broker(), sink)
    history = recovery.history
    assert history.format_version == 1 and history.recovery_required
    assert history.cursor == 2 and len(history.facts) == 2
    assert history.page_sha256 == FIXTURE["expected"]["last_page_sha256"]
    assert all(
        fill.quote_quantity is fill.realized_pnl is fill.realized_pnl_asset is None
        for fill in history.facts
    )
    assert sink.records() == tuple(FIXTURE["events"]) and primary.path.read_bytes() == before
    assert BlockReason.EXECUTION in gate.reasons


def test_default_v1_refuses_to_drop_reported_metadata_before_any_record(tmp_path):
    sink, _primary = fixture_sink(tmp_path)
    broker = Broker((fact(),))
    recovery, gate = owner(broker, sink)
    with pytest.raises(ValidationError, match="format 2"):
        recovery.recover_page()
    assert broker.calls == [0] and sink.records() == ()
    assert recovery.history.cursor == recovery.history.revision == 0
    assert recovery.history.facts == () and BlockReason.EXECUTION in gate.reasons


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_v2_complete_reported_facts_ack_loss_and_inclusive_restart(tmp_path, market):
    base_sink, primary = fixture_sink(tmp_path)

    class LostAck:
        calls = 0

        def records(self):
            return base_sink.records()

        def record(self, event):
            self.calls += 1
            result = base_sink.record(event)
            if self.calls == 2:
                raise OSError("synthetic page primary ACK lost after fsync")
            return result

    sink = LostAck()
    first = fact(market=market)
    broker = Broker((first,))
    recovery, gate = owner(broker, sink, market=market, version=2)
    with pytest.raises(TradingBlocked):
        recovery.recover_page()
    assert recovery.history.revision == 0 and broker.calls == [0]
    receipt = recovery.recover_page()
    assert receipt.format_version == 2 and receipt.primary_acknowledged
    assert receipt.recovery_required and not receipt.history_complete
    assert receipt.facts == receipt.new_facts == (first,) and broker.calls == [0]
    assert BlockReason.STORAGE in gate.reasons
    events = sink.records()
    assert all(event["schema_version"] == event["scan"]["format_version"] == 2 for event in events)
    body = events[0]["fill"]
    assert body["quote_quantity"] == "100"
    assert body["realized_pnl"] == ("0" if market == Market.USDM else None)
    assert body["realized_pnl_asset"] == ("USDT" if market == Market.USDM else None)

    second = fact("2", market)
    if market == Market.USDM:
        second = replace(second, realized_pnl=D("-2.50"))
    reopened_sink, _ = fixture_sink(tmp_path)
    next_broker = Broker((first, second))
    reopened, _gate = owner(next_broker, reopened_sink, market=market, version=2)
    assert reopened.history.format_version == 2 and reopened.history.cursor == 1
    next_page = reopened.recover_page()
    assert next_broker.calls == [1] and next_page.new_facts == (second,)
    assert next_page.through_id == 2 and next_page.revision == 2
    assert reopened.history.facts == (first, second)
    assert primary.path.read_bytes()  # Actual fsynced JSONL, no in-memory-only replay.


def test_v2_recapture_is_separate_and_never_enriches_or_reapplies_v1(tmp_path):
    sink, primary = fixture_sink(tmp_path)
    for event in FIXTURE["events"]:
        primary.append(deepcopy(event))
    original_prefix = primary.path.read_bytes()
    modern, _ = owner(Broker((fact(), fact("2"))), sink, version=2)
    assert modern.history.facts == () and modern.history.cursor == modern.history.revision == 0
    assert modern.recover_page().from_id == 0
    assert primary.path.read_bytes().startswith(original_prefix)
    assert sink.records()[: len(FIXTURE["events"])] == tuple(FIXTURE["events"])
    legacy, _ = owner(Broker(), sink)
    assert legacy.history.page_sha256 == FIXTURE["expected"]["last_page_sha256"]
    assert all(item.quote_quantity is None for item in legacy.history.facts)

    ledger = CashLedger(ACCOUNT, owned()[0].instrument, "USDT", D(1000), base_asset="BTC")
    assert ledger.apply(legacy.history.facts[0])
    before = deepcopy(ledger)
    with pytest.raises(ValidationError, match="conflicting"):
        ledger.apply(modern.history.facts[0])
    assert ledger == before
    fresh = CashLedger(ACCOUNT, owned()[0].instrument, "USDT", D(1000), base_asset="BTC")
    for item in modern.history.facts:
        assert fresh.apply(item) and not fresh.apply(item)
    assert fresh.cash == D("799.8") and fresh.quantity == 2


@pytest.mark.parametrize("version", [True, False, 0, 3, "2", None])
def test_invalid_format_selector_rejects_before_history_or_gateway(version):
    class Untouched:
        def records(self):
            pytest.fail("invalid fact format must not read history")

    broker = Broker()
    with pytest.raises(ValidationError, match="format"):
        owner(broker, Untouched(), version=version)
    assert not broker.calls


def test_v2_nullable_quote_is_not_invented_from_price_and_zero_pnl_stays_observed(tmp_path):
    sink, _ = fixture_sink(tmp_path)
    source = replace(fact(market=Market.USDM), quote_quantity=None)
    recovery, _ = owner(Broker((source,)), sink, market=Market.USDM, version=2)
    recovery.recover_page()
    reopened, _ = owner(Broker(), sink, market=Market.USDM, version=2)
    (recorded,) = reopened.history.facts
    assert recorded.quote_quantity is None and recorded.realized_pnl == D(0)
    assert recorded.realized_pnl_asset == "USDT"


def test_v2_corrupt_amount_types_do_not_replay_as_reported_facts(tmp_path):
    source_sink, _ = fixture_sink(tmp_path / "source")
    recovery, _ = owner(Broker((fact(),)), source_sink, version=2)
    recovery.recover_page()
    events = list(deepcopy(source_sink.records()))
    events[0]["fill"]["quote_quantity"] = 100.0
    bad_sink, primary = fixture_sink(tmp_path / "bad")
    for event in events:
        primary.append(event)
    broker = Broker()
    with pytest.raises(JournalError, match="nullable decimal text"):
        owner(broker, bad_sink, version=2)
    assert not broker.calls


@pytest.mark.parametrize("row_index", [0, 1], ids=["fact", "page"])
def test_equal_numeric_float_cannot_impersonate_native_v2_scan(tmp_path, row_index):
    source_sink, _ = fixture_sink(tmp_path / "source")
    recovery, _ = owner(Broker((fact(),)), source_sink, version=2)
    recovery.recover_page()
    events = list(deepcopy(source_sink.records()))
    # Keep the original inner SHA. Python dictionary equality alone would accept
    # 2.0 as 2, even though the recorded canonical namespace bytes have changed.
    events[row_index]["scan"]["format_version"] = 2.0
    bad_sink, primary = fixture_sink(tmp_path / "bad")
    for event in events:
        primary.append(event)  # Produces a valid outer JSONL integrity checksum.
    broker = Broker()
    with pytest.raises(JournalError, match="native namespace"):
        owner(broker, bad_sink, version=2)
    assert not broker.calls
