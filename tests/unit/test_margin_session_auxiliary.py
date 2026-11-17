"""Real margin files and NES arithmetic with committed typed observations; no DB or NN."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256

import pytest
from test_margin_session import BTC, bars, configuration, new, tables

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Side
from omega.domain.trading import OrderProposal, TradeDecision
from omega.ports.analysis_inputs import PreparedMarketFrame
from omega.ports.auxiliary import StoredAuxiliary
from omega.ports.modules import ModuleDescriptor
from omega.runtime import margin_session
from omega.runtime.margin_session import MarginTradingSession
from omega.runtime.margin_session_inputs import MAX_INPUT_BYTES, unavailable_frames

DESTINATION = "d" * 64
FIELD = "0_NES_0_NES"


class NESModule:
    descriptor = ModuleDescriptor("nes_fixture", "1", frozenset({FIELD}), True)

    def __init__(self, instrument):
        self.instrument, self.last_open_time_ms, self.protected = instrument, None, 0

    def checkpoint(self):
        return {"cursor": self.last_open_time_ms, "protected": self.protected}

    def restore(self, document):
        self.last_open_time_ms, self.protected = document["cursor"], document["protected"]

    def resume(self):
        pass

    def evaluate_protection(self, context):
        self.last_open_time_ms = context.closed_candle.open_time_ms
        self.protected += 1
        if context.position.quantity and context.closed_candle.low <= D(95):
            return TradeDecision(
                (
                    OrderProposal(
                        Side.SELL,
                        abs(context.position.quantity),
                        reducing=True,
                    ),
                ),
                "local_stop_without_auxiliary",
            )
        return TradeDecision(reason="protection_without_auxiliary")

    def evaluate(self, context):
        self.last_open_time_ms = context.closed_candle.open_time_ms
        # A known input can still be warming up. This normal signal path does
        # not protect positions without a signal; the unavailable-input path
        # must call evaluate_protection independently to retain the local stop.
        if context.features[FIELD] is None:
            return TradeDecision(reason="nes_warmup")
        if (
            context.features[FIELD] > 0
            and not context.position.quantity
            and context.entries_allowed
        ):
            return TradeDecision((OrderProposal(Side.BUY, D(1)),), "positive_nes_entry")
        return self.evaluate_protection(context)


class Registry:
    def create(self, _module, instrument, _configuration):
        return NESModule(instrument)


def setup(root, *, create=True):
    account, strategies = configuration()
    account["account_id"] = "nes-session"
    account["positions"] = account["positions"][:1]
    strategies = strategies[:1]
    strategies[0].update(
        module="nes_fixture",
        configuration={},
        analysis={"order_flow": [{"interval_id": 0, "analysis_code": "NES", "nSamples": 2}]},
    )
    return MarginTradingSession(root, account, strategies, 0, create=create, registry=Registry())


def prepared(
    index,
    *,
    buy="2000",
    sell="500",
    quality="observed",
    coverage=D(1),
    revision=1,
    previous_checksum=None,
    cutoff=None,
    low=None,
):
    candle = bars(index, low=low)[0]
    cutoff = candle.close_time_ms if cutoff is None else cutoff
    observation = AuxiliaryInput(
        "trade_flow",
        (("AGGTRADE_NOTIONALBUY", D(buy)), ("AGGTRADE_NOTIONALSELL", D(sell))),
        candle.open_time_ms,
        candle.close_time_ms,
        candle.close_time_ms,
        candle.close_time_ms,
        quality,
        coverage,
        BTC.key + ":aggTrade",
    )
    checksum = observation_checksum(encode_observation(BTC, observation))
    pin = StoredAuxiliary(BTC, observation, revision, checksum, previous_checksum)
    return PreparedMarketFrame(MarketFeatureFrame(candle, cutoff, (observation,)), (pin,))


def step(session, index, value=None, *, destination=DESTINATION, low=None):
    value = prepared(index, low=low) if value is None else value
    return session.run_once(
        (bars(index, low=low)[0],),
        as_of_ms=(index + 1) * 60000,
        brackets=tables()[:1],
        prepared_frames=(value,),
        auxiliary_destination=destination,
    )


def test_nonzero_nes_consumes_original_frame_then_missing_data_keeps_fills_and_local_stop(tmp_path):
    session = setup(tmp_path)
    try:
        assert session.analysis_input_channels == {BTC.key: ("trade_flow",)}
        session.resume("start")
        for index in range(2):
            step(session, index)
        last = session._meta["last_frame"]
        assert last["features"]["btc-ts"][FIELD] == 1500
        assert last["graph_ready"]["btc-ts"] is True
        assert session.snapshot()["pending_orders"]
        entry_id = session.snapshot()["pending_orders"][0]
        assert not session.execution.snapshot().account.ledger
        missing = unavailable_frames(
            (bars(2, low="90")[0],), session.analysis_input_channels, as_of_ms=180000
        )
        result = step(session, 2, missing[0], low="90")
        assert result["market_inputs"]["state"] == "unavailable"
        assert session._meta["last_frame"]["features"]["btc-ts"][FIELD] is None
        assert session._meta["last_frame"]["gates"]["btc-ts"] is False
        assert len(session.execution.snapshot().account.ledger) == 1  # Accepted entry still filled.
        assert result["pending_orders"]  # Independent local reduction was accepted.
        entry = session.execution.snapshot().account.ledger[0].fact
        assert entry.order_id == entry_id and entry.delta_qty == 1
        assert entry.event_time_ms == 120000 and entry.available_at_ms == 180000
        missing = unavailable_frames(
            (bars(3)[0],), session.analysis_input_channels, as_of_ms=240000
        )
        step(session, 3, missing[0])
        financial = session.execution.snapshot().account
        assert len(financial.ledger) == 2 and financial.positions[0].quantity == 0
        assert not session.snapshot()["pending_orders"]
    finally:
        session.close()


@pytest.mark.parametrize("quality", ["partial", "missing", "unsupported"])
def test_unavailable_quality_preserves_the_fill_but_never_reuses_held_nes_for_entry(
    tmp_path, quality
):
    session = setup(tmp_path)
    try:
        session.resume("start")
        for index in range(2):
            step(session, index)
        assert session.snapshot()["pending_orders"]
        assert not session.execution.snapshot().account.ledger
        if quality == "partial":
            value = prepared(2, quality="partial", coverage=D(".5"))
        else:
            value = unavailable_frames(
                (bars(2)[0],), session.analysis_input_channels, as_of_ms=180000
            )[0]
            if quality == "unsupported":
                observation = replace(value.frame.inputs[0], quality="unsupported")
                value = replace(value, frame=replace(value.frame, inputs=(observation,)))
        step(session, 2, value)
        last = session._meta["last_frame"]
        assert last["features"]["btc-ts"][FIELD] is None
        assert last["graph_ready"]["btc-ts"] is False and last["gates"]["btc-ts"] is False
        assert len(session.execution.snapshot().account.ledger) == 1
        assert not session.snapshot()["pending_orders"]
        assert session._adapters["btc-ts"].module.protected > 0
    finally:
        session.close()


def test_observed_zero_is_a_ready_measurement_and_not_missing_data(tmp_path):
    session = setup(tmp_path)
    try:
        session.resume("start")
        for index in range(3):
            step(session, index, prepared(index, buy="0", sell="0"))
        last = session._meta["last_frame"]
        assert last["features"]["btc-ts"][FIELD] == 0
        assert last["graph_ready"]["btc-ts"] is True
        assert session.snapshot()["market_inputs"]["state"] == "ready"
        assert not session.snapshot()["pending_orders"]
    finally:
        session.close()


def test_observed_input_recovers_readiness_after_a_missing_minute_without_graph_failure(tmp_path):
    session = setup(tmp_path)
    try:
        session.resume("start")
        missing = unavailable_frames((bars(0)[0],), session.analysis_input_channels, as_of_ms=60000)
        step(session, 0, missing[0])
        assert session._meta["last_frame"]["graph_ready"]["btc-ts"] is False
        for index in range(1, 3):
            step(session, index)
        assert session._meta["last_frame"]["graph_ready"]["btc-ts"] is True
        assert session._meta["graph_errors"] == {}
        assert session.snapshot()["market_inputs"]["state"] == "ready"
        assert session.snapshot()["pending_orders"]
        assert not session.execution.snapshot().account.ledger
    finally:
        session.close()


@pytest.mark.parametrize("fault", ["missing_field", "negative", "wrong_type", "carried"])
def test_channel_specific_invalid_notionals_reject_before_meta_or_cursor_change(tmp_path, fault):
    session = setup(tmp_path)
    try:
        original = prepared(0)
        observation = original.frame.inputs[0]
        if fault == "carried":
            observation = replace(observation, quality="carried")
        else:
            values = dict(observation.values)
            name = "AGGTRADE_NOTIONALBUY"
            if fault == "missing_field":
                del values[name]
            else:
                values[name] = D(-1) if fault == "negative" else 1
            observation = replace(observation, values=tuple(values.items()))
        pin = StoredAuxiliary(
            BTC, observation, 1, observation_checksum(encode_observation(BTC, observation)), None
        )
        value = PreparedMarketFrame(replace(original.frame, inputs=(observation,)), (pin,))
        before = session.path.read_bytes()
        with pytest.raises(ProtocolError):
            step(session, 0, value)
        assert session.path.read_bytes() == before
        assert session.snapshot()["cursor_ms"] is None
        assert not session.execution.snapshot().account.ledger
    finally:
        session.close()


@pytest.mark.parametrize(
    "fault", ["retro", "cutoff", "foreign", "checksum", "source", "pin_missing"]
)
def test_invalid_prepared_inputs_reject_before_any_financial_or_session_mutation(tmp_path, fault):
    session = setup(tmp_path)
    try:
        original = prepared(0)
        value = original
        if fault == "retro":
            value = replace(original, frame=replace(original.frame, reference_time_ms=60000))
        elif fault == "cutoff":
            value = replace(original, frame=replace(original.frame, as_of_ms=60001))
        elif fault == "foreign":
            value = replace(original, frame=replace(original.frame, candle=bars(0)[1]))
        elif fault == "checksum":
            value = replace(original, pins=(replace(original.pins[0], checksum="0" * 64),))
        elif fault == "source":
            observation = replace(original.frame.inputs[0], source=BTC.key + ":untrusted")
            value = replace(original, frame=replace(original.frame, inputs=(observation,)))
        else:
            value = replace(original, pins=())
        before = session.path.read_bytes()
        with pytest.raises((ProtocolError, ValidationError)):
            step(session, 0, value)
        assert session.path.read_bytes() == before
        assert session.snapshot()["cursor_ms"] is None
        assert not session.execution.snapshot().account.ledger
    finally:
        session.close()


def test_destination_swap_and_explicit_replacement_of_consumed_inputs_are_rejected(tmp_path):
    session = setup(tmp_path)
    try:
        step(session, 0)
        before = session.path.read_bytes()
        with pytest.raises(ProtocolError, match="destination"):
            step(session, 1, destination="e" * 64)
        original = prepared(0)
        changed = prepared(0, buy="3000", revision=2, previous_checksum=original.pins[0].checksum)
        with pytest.raises(ProtocolError, match="replaced"):
            step(session, 0, changed)
        assert session.path.read_bytes() == before
    finally:
        session.close()
    restored = setup(tmp_path, create=False)
    try:
        with pytest.raises(ProtocolError, match="destination"):
            step(restored, 1, destination="e" * 64)
        assert restored.path.read_bytes() == before
    finally:
        restored.close()


def test_pending_ack_loss_replay_and_restart_use_original_native_pins(tmp_path, monkeypatch):
    session = setup(tmp_path)
    original_write = margin_session.atomic_json

    def lost_ack(path, envelope):
        original_write(path, envelope)
        if envelope["data"].get("pending") is not None:
            raise OSError("session original input ACK lost")

    try:
        monkeypatch.setattr(margin_session, "atomic_json", lost_ack)
        with pytest.raises(StorageError):
            step(session, 0)
        monkeypatch.setattr(margin_session, "atomic_json", original_write)
        # The original staged candidate must equal its durable JSON form exactly;
        # hash equivalence alone must never bypass the ACK-loss reconciliation CAS.
        assert session._uncertain[1]["configuration"]["market_inputs"]["channels"] == {
            BTC.key: ["trade_flow"]
        }
        assert session._read() == session._uncertain[1]
        session.reconcile()
        frozen = deepcopy(session._meta["pending"]["market_inputs"])
        original = prepared(0)
        changed = prepared(0, buy="3000", revision=2, previous_checksum=original.pins[0].checksum)
        before = session.path.read_bytes()
        with pytest.raises(ProtocolError, match="replaced"):
            step(session, 0, changed)
        assert session.path.read_bytes() == before
        session.run_once((), as_of_ms=60001)
        assert session._meta["last_frame"]["market_inputs"] == frozen
    finally:
        monkeypatch.setattr(margin_session, "atomic_json", original_write)
        session.close()
    restored = setup(tmp_path, create=False)
    try:
        assert restored._meta["last_frame"]["market_inputs"] == frozen
        assert restored._graphs["btc-ts"].last_input_frame == original.frame
    finally:
        restored.close()


def test_modified_frozen_payload_cannot_diverge_from_saved_graph_checkpoint(tmp_path):
    session = setup(tmp_path)
    try:
        step(session, 0)
        path = session.path
        envelope = json.loads(path.read_text())
        frame = envelope["data"]["last_frame"]
        changed, _ = session._inputs.freeze(
            (prepared(0, buy="3000"),), (bars(0)[0],), as_of_ms=60000, destination=DESTINATION
        )
        frame["market_inputs"] = changed
        frame["digest"] = session._frame_digest(frame)
        envelope["sha256"] = margin_session._digest(envelope["data"])
    finally:
        session.close()
    path.write_text(json.dumps(envelope))
    with pytest.raises(ProtocolError, match="checkpoint differs"):
        setup(tmp_path, create=False)


def test_ordinary_ohlcv_session_bytes_and_no_auxiliary_keys_are_preserved(tmp_path):
    session = new(tmp_path)
    try:
        assert sha256(session.path.read_bytes()).hexdigest() == (
            "726aa861770e1dc4d259075ffe9e13c1dc89f1fc230886376b5e20ff9950663c"
        )
        assert session.analysis_input_channels == {}
        before = session.path.read_bytes()
        with pytest.raises(ValidationError, match="OHLCV-only"):
            session.run_once(bars(0), as_of_ms=60000, prepared_frames=())
        assert session.path.read_bytes() == before and "market_inputs" not in session.snapshot()
    finally:
        session.close()


def test_account_input_budget_is_checked_before_persist(tmp_path, monkeypatch):
    from omega.runtime import margin_session_inputs

    session = setup(tmp_path)
    try:
        assert MAX_INPUT_BYTES == 2 * 1024 * 1024
        monkeypatch.setattr(margin_session_inputs, "MAX_INPUT_BYTES", 128)
        before = session.path.read_bytes()
        with pytest.raises(ProtocolError, match="byte budget"):
            step(session, 0)
        assert session.path.read_bytes() == before
    finally:
        session.close()
