"""One opt-in real isolated PG → pinned trade flow → NES → financial session case.

Every candle, notional, price and clock is explicitly synthetic. No exchange or
unit-test helper is imported; collecting this integration file alone is supported.
"""

import os
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import ProtocolError, StorageError
from omega.domain.margin import MarginBracket, MarginBracketSnapshot
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market, Side
from omega.domain.trading import OrderProposal, TradeDecision
from omega.infrastructure.analysis_frames import CommittedAnalysisFrameSource
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import database_destination_id, load_database_settings
from omega.ports.collection import MinuteObservation
from omega.ports.modules import ModuleDescriptor
from omega.runtime import margin_session as session_module
from omega.runtime.margin_session import MarginTradingSession

pytestmark = pytest.mark.database
START = 1_700_000_040_000
FIELD = "0_NES_0_NES"


class SyntheticNESStrategy:
    descriptor = ModuleDescriptor("pg_nes_fixture", "1", frozenset({FIELD}), True)

    def __init__(self, instrument, namespace):
        self.instrument, self.namespace = instrument, namespace
        self.last_open_time_ms, self.protected = None, 0

    def checkpoint(self):
        return {
            "namespace": self.namespace,
            "cursor": self.last_open_time_ms,
            "protected": self.protected,
        }

    def restore(self, document):
        assert set(document) == {"namespace", "cursor", "protected"}
        assert document["namespace"] == self.namespace
        self.last_open_time_ms, self.protected = document["cursor"], document["protected"]

    def resume(self):
        pass

    def evaluate_protection(self, context):
        self.last_open_time_ms = context.closed_candle.open_time_ms
        self.protected += 1
        if context.position.quantity and context.closed_candle.low <= D(95):
            return TradeDecision(
                (OrderProposal(Side.SELL, abs(context.position.quantity), reducing=True),),
                "synthetic_local_stop_without_flow",
            )
        return TradeDecision(reason="synthetic_local_protection")

    def evaluate(self, context):
        assert context.instrument == self.instrument
        self.last_open_time_ms = context.closed_candle.open_time_ms
        value = context.features[FIELD]
        if value is None:
            return TradeDecision(reason="synthetic_nes_warmup")
        if value > 0 and not context.position.quantity and context.entries_allowed:
            return TradeDecision((OrderProposal(Side.BUY, D(1)),), "synthetic_nes_entry")
        return self.evaluate_protection(context)


class SyntheticRegistry:
    def __init__(self, namespace):
        self.namespace = namespace

    def create(self, module, instrument, configuration):
        assert module == "pg_nes_fixture" and configuration == {"namespace": self.namespace}
        return SyntheticNESStrategy(instrument, self.namespace)


def _brackets(bar, namespace):
    return (
        MarginBracketSnapshot(
            bar.instrument,
            (MarginBracket(D(0), D(1_000_000), D(".01"), D(0), 20),),
            bar.open_time_ms,
            bar.open_time_ms,
            "synthetic_pg_nes_reference",
            sha256((namespace + str(bar.open_time_ms)).encode()).hexdigest(),
        ),
    )


def test_real_pg_nes_keeps_original_revision_across_pending_restart_and_missing_flow_protection(
    tmp_path, request, monkeypatch
):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit --database-tests and isolated database")
    configured = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not configured:
        pytest.fail("explicit isolated OMEGA_TEST_DB_CONFIG is required")
    config_path = Path(configured).resolve()
    settings = load_database_settings(config_path)
    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    )
    namespace = uuid4().hex
    root = tmp_path / namespace
    root.mkdir()
    (root / "database.config.toml").write_bytes(config_path.read_bytes())
    instrument = Instrument("binance", Market.USDM, "TEST_PGNES_" + namespace[:24].upper() + "USDT")
    dataset = instrument.key + ":aggTrade"
    identity, account_id = "nes-" + namespace, "pg-nes-" + namespace
    owned = False
    session = source = candles = auxiliary = None
    now = [START + 60_000]
    bars = tuple(
        Candle(
            instrument,
            START + index * 60000,
            START + (index + 1) * 60000,
            D(100),
            D(102),
            D(90) if index == 2 else D(99),
            D(100),
            D(10),
        )
        for index in range(4)
    )
    account = {
        "account_id": account_id,
        "balance": "1000",
        "positions": [
            {
                "instrument": instrument.key,
                "owner_id": identity,
                "margin_mode": "cross",
                "leverage": 10,
                "budget_margin": "100",
            }
        ],
    }
    strategies = [
        {
            "id": identity,
            "instrument": instrument.key,
            "module": "pg_nes_fixture",
            "configuration": {"namespace": namespace},
            "analysis": {"order_flow": [{"interval_id": 0, "analysis_code": "NES", "nSamples": 2}]},
            "constraints": {"leverage": "10", "can_short": True},
        }
    ]

    def open_session(*, create):
        return MarginTradingSession(
            root,
            account,
            strategies,
            START,
            create=create,
            registry=SyntheticRegistry(namespace),
            clock_ms=lambda: now[0],
        )

    def read_frame(index, cutoff):
        bar = bars[index]
        return source.read_frames(
            instrument,
            bar.open_time_ms,
            bar.close_time_ms,
            as_of_ms=cutoff,
            channels=("trade_flow",),
            maximum_metric_age_ms=0,
        )

    def step(index):
        bar = bars[index]
        now[0] = bar.close_time_ms
        prepared = read_frame(index, now[0])
        return session.run_once(
            (bar,),
            as_of_ms=now[0],
            brackets=_brackets(bar, namespace),
            prepared_frames=prepared,
            auxiliary_destination=source.destination_id,
        )

    try:
        with connect_database(settings) as connection:
            assert connection.execute("SELECT current_database(),current_user").fetchone() == (
                "omega_test",
                "omega_app",
            )
            assert connection.execute(
                "SELECT count(*) FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
            ).fetchone() == (0,)
            assert connection.execute(
                "SELECT count(*) FROM market.auxiliary_minutes WHERE instrument=%s",
                (instrument.key,),
            ).fetchone() == (0,)
        owned = True
        candles, auxiliary = PostgresCandleStore(settings), PostgresAuxiliaryStore(settings)
        source = CommittedAnalysisFrameSource(root)
        for bar in bars:
            candles.put(MinuteObservation(bar, "rest", bar.close_time_ms))
        flows, receipts = [], []
        for bar in bars[:2]:
            observation = AuxiliaryInput(
                "trade_flow",
                (("AGGTRADE_NOTIONALBUY", D(2000)), ("AGGTRADE_NOTIONALSELL", D(500))),
                bar.open_time_ms,
                bar.close_time_ms,
                bar.close_time_ms,
                bar.close_time_ms,
                source=dataset,
            )
            flows.append(observation)
            receipts.append(auxiliary.put(instrument, observation))
        original = read_frame(0, now[0])
        assert len(original) == 1 and len(original[0].pins) == 1
        pin = original[0].pins[0]
        payload = encode_observation(instrument, flows[0]).decode("utf-8")
        assert pin.revision == 1 and pin.checksum == receipts[0].checksum
        assert pin.checksum == observation_checksum(payload.encode("utf-8"))
        assert pin.observation == flows[0] and pin.observation.source == dataset
        assert source.destination_id == database_destination_id(settings)

        session = open_session(create=True)
        assert session.analysis_input_channels == {instrument.key: ("trade_flow",)}
        session.resume("initial-" + namespace)
        write = session_module.atomic_json
        fired = []

        def lost_original_ack(path, envelope):
            write(path, envelope)
            if path == session.path and envelope["data"].get("pending") is not None and not fired:
                fired.append(True)
                raise OSError("synthetic loss after original frame publication")

        with monkeypatch.context() as patch:
            patch.setattr(session_module, "atomic_json", lost_original_ack)
            with pytest.raises(StorageError):
                session.run_once(
                    (bars[0],),
                    as_of_ms=now[0],
                    brackets=_brackets(bars[0], namespace),
                    prepared_frames=original,
                    auxiliary_destination=source.destination_id,
                )
        assert fired == [True]
        session.reconcile()
        frozen = deepcopy(session._meta["pending"])
        native = frozen["market_inputs"]["frames"][0]
        assert frozen["market_inputs"]["destination"] == source.destination_id
        assert native["as_of_ms"] == now[0] and native["inputs"] == [payload]
        assert native["pins"] == [
            {"revision": 1, "checksum": pin.checksum, "previous_checksum": None, "payload": payload}
        ]

        correction = replace(
            flows[0],
            values=(("AGGTRADE_NOTIONALBUY", D(9000)), ("AGGTRADE_NOTIONALSELL", D(0))),
            available_at_ms=flows[0].available_at_ms + 100,
        )
        revised = auxiliary.put(instrument, correction, expected_previous=pin.checksum)
        now[0] = correction.available_at_ms + 1
        changed = read_frame(0, now[0])
        assert revised.revision == changed[0].pins[0].revision == 2
        assert changed[0].pins[0].previous_checksum == pin.checksum
        assert changed[0].pins[0].checksum == revised.checksum != pin.checksum
        assert (
            source.get_auxiliary(
                instrument, "trade_flow", dataset, START, as_of_ms=original[0].frame.as_of_ms
            )
            == pin
        )
        before = session.path.read_bytes()
        with pytest.raises(ProtocolError):
            session.run_once(
                (bars[0],),
                as_of_ms=now[0],
                brackets=_brackets(bars[0], namespace),
                prepared_frames=changed,
                auxiliary_destination=source.destination_id,
            )
        assert session.path.read_bytes() == before and session._meta["pending"] == frozen
        session.close()
        session = None
        session = open_session(create=False)
        assert session.snapshot()["paused"] and session._meta["pending"] == frozen
        session.run_once((), as_of_ms=now[0])
        assert session._meta["last_frame"]["market_inputs"] == frozen["market_inputs"]
        assert session._meta["last_frame"]["digest"] == frozen["digest"]
        assert session._graphs[identity].last_input_frame == original[0].frame
        consumed = session.path.read_bytes()
        session.run_once((bars[0],), as_of_ms=now[0])
        assert session.path.read_bytes() == consumed

        session.resume("after-restart-" + namespace)
        step(1)
        last = session._meta["last_frame"]
        assert last["features"][identity][FIELD] == 1500
        assert last["graph_ready"][identity] is True
        pending = session.execution.snapshot().pending
        assert len(pending) == 1 and not pending[0].original.proposal.reducing
        original_order = pending[0]
        assert original_order.eligible_at_ms == bars[2].open_time_ms
        missing = read_frame(2, bars[2].close_time_ms)
        assert missing[0].frame.input("trade_flow").quality == "missing"
        assert missing[0].pins == ()
        result = step(2)
        last = session._meta["last_frame"]
        assert result["market_inputs"]["state"] == "unavailable"
        assert last["features"][identity][FIELD] is None and last["gates"][identity] is False
        financial = session.execution.snapshot()
        assert len(financial.account.ledger) == 1
        assert financial.account.ledger[0].fact.order_id == original_order.order_id
        assert session.execution.original_intent(original_order.order_id) == original_order
        assert len(financial.pending) == 1 and financial.pending[0].original.proposal.reducing
        assert session._adapters[identity].module.protected > 0
        step(3)
        final = session.execution.snapshot()
        assert len(final.account.ledger) == 2 and final.account.positions[0].quantity == 0
        assert final.pending == () and session.snapshot()["pending_orders"] == []
    finally:
        try:
            if session is not None:
                session.close()
        finally:
            try:
                if source is not None:
                    source.close()
            finally:
                try:
                    if auxiliary is not None:
                        auxiliary.close()
                finally:
                    try:
                        if candles is not None:
                            candles.close()
                    finally:
                        if owned:
                            with connect_database(settings) as connection:
                                connection.execute(
                                    "DELETE FROM market.candles_1m WHERE instrument=%s",
                                    (instrument.key,),
                                )
                                connection.execute(
                                    "DELETE FROM market.auxiliary_minutes "
                                    "WHERE instrument=%s AND dataset=%s",
                                    (instrument.key, dataset),
                                )
