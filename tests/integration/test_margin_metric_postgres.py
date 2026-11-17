"""Opt-in isolated PG → original sparse metrics/flow → real MST → virtual finance.

All samples/bars/clocks are explicitly synthetic. The declared test TS consumes
the real MST graph; this is not complete PATHFINDER trading or exchange evidence.
This file collects alone without importing private or unit-test fixtures.
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

from omega.application.auxiliary_codec import (
    decode_observation,
    encode_observation,
    observation_checksum,
)
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
from omega.runtime.margin_session import MarginTradingSession

pytestmark = pytest.mark.database
START, MINUTE, AGE = 1_700_000_100_000, 60_000, 180_000
FIELD = "0_MST_0_MST"


class SyntheticMSTStrategy:
    descriptor = ModuleDescriptor("pg_mst_fixture", "1", frozenset({FIELD}), True)

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
                "synthetic_mst_local_stop",
            )
        return TradeDecision(reason="synthetic_mst_hold")

    def evaluate(self, context):
        assert context.instrument == self.instrument
        self.last_open_time_ms = context.closed_candle.open_time_ms
        value = context.features[FIELD]
        if (
            value is not None
            and value > 0
            and not context.position.quantity
            and context.entries_allowed
        ):
            return TradeDecision((OrderProposal(Side.BUY, D(1)),), "synthetic_calculated_mst_entry")
        return self.evaluate_protection(context)


class SyntheticRegistry:
    def __init__(self, namespace):
        self.namespace = namespace

    def create(self, module, instrument, configuration):
        assert module == "pg_mst_fixture" and configuration == {"namespace": self.namespace}
        return SyntheticMSTStrategy(instrument, self.namespace)


def native_metric(instrument, channel, event, number):
    suffix, field = (
        ("open_interest_history", "METRIC_OPENINTEREST")
        if channel == "open_interest"
        else ("long_short_ratio", "METRIC_LONGSHORTRATIO")
    )
    return AuxiliaryInput(
        channel,
        ((field, number), ("SOURCE_PERIOD_MS", 300000)),
        event,
        event + MINUTE,
        event,
        event,
        "observed",
        D(1),
        instrument.key + ":" + suffix,
    )


def brackets(bar, namespace):
    return (
        MarginBracketSnapshot(
            bar.instrument,
            (MarginBracket(D(0), D(1_000_000), D(".01"), D(0), 20),),
            bar.open_time_ms,
            bar.open_time_ms,
            "synthetic_pg_mst_reference",
            sha256((namespace + str(bar.open_time_ms)).encode()).hexdigest(),
        ),
    )


def test_real_isolated_pg_sparse_metric_mst_restart_stale_fill_protection_and_recovery(
    tmp_path, request
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
    instrument = Instrument("binance", Market.USDM, "TEST_PGMST_" + namespace[:20].upper() + "USDT")
    identity, account_id = "mst-" + namespace, "pg-mst-" + namespace
    session = source = candles = auxiliary = None
    owned = False
    now = [START + MINUTE]
    bars = tuple(
        Candle(
            instrument,
            START + index * MINUTE,
            START + (index + 1) * MINUTE,
            D(102) if index in {3, 4} else D(100),
            D(110),
            D(90) if index == 3 else D(99),
            D(100 + index),
            D(10),
        )
        for index in range(6)
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
            "module": "pg_mst_fixture",
            "configuration": {"namespace": namespace},
            "analysis": {
                "market_sentiment": [
                    {"interval_id": 0, "nSamples": 2, "line_index": 0, "maximum_metric_age_ms": AGE}
                ]
            },
            "constraints": {"leverage": "10", "can_short": True},
        }
    ]

    def open_session(create):
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
        rows = source.read_frames(
            instrument,
            bar.open_time_ms,
            bar.close_time_ms,
            as_of_ms=cutoff,
            channels=("metric", "trade_flow"),
            maximum_metric_age_ms=AGE,
        )
        assert len(rows) == 1 and rows[0].frame.candle == bar
        return rows

    def step(index):
        bar = bars[index]
        now[0] = bar.close_time_ms
        prepared = read_frame(index, now[0])
        result = session.run_once(
            (bar,),
            as_of_ms=now[0],
            brackets=brackets(bar, namespace),
            prepared_frames=prepared,
            auxiliary_destination=source.destination_id,
        )
        assert result["graph_errors"] == {}
        return prepared[0], result

    try:
        with connect_database(settings) as connection:
            assert connection.execute("SELECT current_database(),current_user").fetchone() == (
                "omega_test",
                "omega_app",
            )
            for table in ("market.candles_1m", "market.auxiliary_minutes"):
                assert connection.execute(
                    "SELECT count(*) FROM " + table + " WHERE instrument=%s", (instrument.key,)
                ).fetchone() == (0,)
        owned = True
        candles, auxiliary = PostgresCandleStore(settings), PostgresAuxiliaryStore(settings)
        source = CommittedAnalysisFrameSource(root)
        for bar in bars:
            candles.put(MinuteObservation(bar, "rest", bar.close_time_ms))
            auxiliary.put(
                instrument,
                AuxiliaryInput(
                    "trade_flow",
                    (("AGGTRADE_NOTIONALBUY", D(1600)), ("AGGTRADE_NOTIONALSELL", D(500))),
                    bar.open_time_ms,
                    bar.close_time_ms,
                    bar.close_time_ms,
                    bar.close_time_ms,
                    "observed",
                    D(1),
                    instrument.key + ":aggTrade",
                ),
            )
        samples, receipts = {}, {}
        for index, oi, ls in ((0, D(1000), D("1.5")), (5, D(1100), D("1.4"))):
            for channel, number in (("open_interest", oi), ("long_short_ratio", ls)):
                value = native_metric(instrument, channel, START + index * MINUTE, number)
                samples[index, channel] = value
                receipts[index, channel] = auxiliary.put(instrument, value)
        session = open_session(True)
        assert session.analysis_input_channels == {instrument.key: ("metric", "trade_flow")}
        policy = session.analysis_metric_policies[instrument.key]
        assert policy == {
            "version": 1,
            "maximum_metric_age_ms": AGE,
            "open_interest_source_suffix": "open_interest_history",
            "long_short_source_suffix": "long_short_ratio",
        }
        session.resume("initial-" + namespace)
        for index in range(3):
            prepared, view = step(index)
            assert view["market_inputs"]["state"] == "ready"
        assert source.destination_id == database_destination_id(settings)
        metric = prepared.frame.input("metric")
        assert (
            metric.quality == "carried" and metric.event_time_ms == metric.available_at_ms == START
        )
        assert metric.mapping()["SOURCE_OPENINTEREST_TIME_MS"] == START
        assert metric.mapping()["SOURCE_LONGSHORTRATIO_AVAILABLE_MS"] == START
        assert session._meta["last_frame"]["features"][identity][FIELD] > 0
        assert session._meta["last_frame"]["graph_ready"][identity] is True
        financial = session.execution.snapshot()
        assert financial.account.account_id == account_id and financial.account.ledger == ()
        assert len(financial.pending) == 1
        entry = financial.pending[0]
        assert not entry.original.proposal.reducing and entry.eligible_at_ms == bars[3].open_time_ms
        assert (
            entry.original.mark_price == entry.original.adverse_fill_price == bars[3].open == D(102)
        )
        frozen = deepcopy(session._meta["last_frame"])
        native = frozen["market_inputs"]["frames"][0]
        assert frozen["market_inputs"]["destination"] == source.destination_id
        assert native["as_of_ms"] == bars[2].close_time_ms and len(native["pins"]) == 3
        documents = {
            decode_observation(row["payload"].encode())[1].channel: row for row in native["pins"]
        }
        assert set(documents) == {"open_interest", "long_short_ratio", "trade_flow"}
        for channel in ("open_interest", "long_short_ratio"):
            original = samples[0, channel]
            payload = encode_observation(instrument, original)
            assert documents[channel] == {
                "revision": 1,
                "checksum": receipts[0, channel].checksum,
                "previous_checksum": None,
                "payload": payload.decode(),
            }
            assert observation_checksum(payload) == receipts[0, channel].checksum
            pin = next(row for row in prepared.pins if row.observation.channel == channel)
            assert pin.observation == original and pin.revision == 1

        # Later genuine revision of the same native key cannot rewrite a frozen frame.
        original = samples[0, "open_interest"]
        correction = replace(
            original,
            values=(("METRIC_OPENINTEREST", D(9000)), ("SOURCE_PERIOD_MS", 300000)),
            available_at_ms=bars[2].close_time_ms + 1,
        )
        revised = auxiliary.put(
            instrument, correction, expected_previous=receipts[0, "open_interest"].checksum
        )
        assert revised.revision == 2
        selected = auxiliary.get(
            instrument, "open_interest", original.source, START, as_of_ms=correction.available_at_ms
        )
        assert (
            selected.revision == 2
            and selected.previous_checksum == receipts[0, "open_interest"].checksum
        )
        assert selected.observation == correction
        assert read_frame(2, bars[2].close_time_ms)[0] == prepared
        now[0] = correction.available_at_ms
        session.close()
        session = open_session(False)
        assert session.snapshot()["paused"] is True
        assert session._meta["last_frame"] == frozen
        assert session._graphs[identity].last_input_frame == prepared.frame
        assert session.execution.snapshot().pending == (entry,)
        session.resume("after-restart-" + namespace)
        protected_before = session._adapters[identity].module.protected

        stale, view = step(3)
        assert stale.frame.input("metric").quality == "missing"
        assert view["market_inputs"]["state"] == "unavailable"
        assert session._meta["last_frame"]["features"][identity][FIELD] is None
        assert session._meta["last_frame"]["gates"][identity] is False
        financial = session.execution.snapshot()
        assert len(financial.account.ledger) == len(financial.pending) == 1
        assert financial.account.ledger[0].fact.order_id == entry.order_id
        assert financial.account.ledger[0].fact.event_time_ms == bars[3].open_time_ms
        assert session.execution.original_intent(entry.order_id) == entry
        assert financial.pending[0].original.proposal.reducing
        assert session._adapters[identity].module.protected > protected_before
        still_stale, _ = step(4)
        assert still_stale.frame.input("metric").quality == "missing"
        financial = session.execution.snapshot()
        assert [row.fact.delta_qty for row in financial.account.ledger] == [D(1), D(-1)]
        assert [row.fact.event_time_ms for row in financial.account.ledger] == [
            bars[3].open_time_ms,
            bars[4].open_time_ms,
        ]
        assert all(
            row.fact.owner_id == identity
            and row.fact.instrument == instrument
            and row.fact.price == D(102)
            for row in financial.account.ledger
        )
        assert financial.account.cross_wallet == D("999.796")
        assert financial.account.positions[0].quantity == 0 and financial.pending == ()
        healthy, view = step(5)
        assert healthy.frame.input("metric").quality == "observed"
        assert view["market_inputs"]["state"] == "ready"
        assert session._meta["last_frame"]["features"][identity][FIELD] is not None
        for channel in ("open_interest", "long_short_ratio"):
            selected = next(pin for pin in healthy.pins if pin.observation.channel == channel)
            assert selected.observation == samples[5, channel] and selected.revision == 1
            assert (
                selected.observation.event_time_ms
                == selected.observation.available_at_ms
                == bars[5].open_time_ms
            )
    finally:
        failures = []
        for resource in (session, source, auxiliary, candles):
            if resource is not None:
                try:
                    resource.close()
                except Exception as error:
                    failures.append(error)
        if owned and not failures:
            with connect_database(settings) as connection:
                assert connection.execute("SELECT current_database(),current_user").fetchone() == (
                    "omega_test",
                    "omega_app",
                )
                for table in ("market.auxiliary_minutes", "market.candles_1m"):
                    connection.execute(
                        "DELETE FROM " + table + " WHERE instrument=%s", (instrument.key,)
                    )
                    assert connection.execute(
                        "SELECT count(*) FROM " + table + " WHERE instrument=%s", (instrument.key,)
                    ).fetchone() == (0,)
        if failures:
            raise ExceptionGroup(
                "Own metric test handles did not close; UUID rows retained", failures
            )
