"""Isolated real PostgreSQL selection/CP/manifest recovery, synthetic paper account."""

import os
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.collection import MinuteObservation
from omega.ports.modules import ModuleDescriptor
from omega.runtime.analysis_manifest import InputPolicy
from omega.runtime.analysis_replay import replay
from omega.runtime.paper_feed import PaperFeedSupervisor
from omega.runtime.paper_inputs import PaperAnalysisSource
from omega.runtime.settings import atomic_json
from omega.strategies.target_quantity.module import TargetQuantityModule

pytestmark = pytest.mark.database
MINUTE = 60000
START = 1_700_000_040_000


class Module(TargetQuantityModule):
    descriptor = ModuleDescriptor("nes_target", "1", frozenset({"0_NES_0_NES"}), True)


def factory(_name, instrument, configuration):
    return Module(instrument, configuration)


def test_actual_db_missing_cutoff_remains_pinned_and_restart_requires_manual_resume(tmp_path):
    config = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not config:
        pytest.fail("explicit isolated OMEGA_TEST_DB_CONFIG is required")
    settings = load_database_settings(Path(config))
    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    )
    (tmp_path / "database.config.toml").write_bytes(Path(config).read_bytes())
    instrument = Instrument("binance", Market.USDM, "TESTPIN" + uuid4().hex[:16].upper() + "USDT")
    candles, auxiliary = PostgresCandleStore(settings), PostgresAuxiliaryStore(settings)
    now = [START + 3 * MINUTE + 100]
    state_path = tmp_path / "paper-state.json"
    paper = PaperTradingService(
        factory, create_features, persist=lambda state: atomic_json(state_path, state)
    )
    paper.command("paper.account.create", {"account_id": "a", "balance": "10000"})
    paper.command(
        "paper.strategy.add",
        {
            "instance": "s",
            "account_id": "a",
            "instrument": instrument.key,
            "module_id": "nes_target",
            "configuration": {"quantity": "1"},
            "analysis": {"order_flow": [{"interval_id": 0, "analysis_code": "NES", "nSamples": 2}]},
            "budget": "1000",
        },
    )
    paper.feed_configure(True, start_ms=START + 2 * MINUTE, warmup_minutes=2)
    source = PaperAnalysisSource(tmp_path, clock_ms=lambda: now[0])
    feed = PaperFeedSupervisor(paper, source, clock_ms=lambda: now[0])
    try:
        for index in range(4):
            start, end = START + index * MINUTE, START + (index + 1) * MINUTE
            candle = Candle(instrument, start, end, *(Decimal(v) for v in (100, 101, 99, 100, 10)))
            candles.put(MinuteObservation(candle, "rest", end + 50, raw='{"fixture":"paper-pins"}'))
            observation = AuxiliaryInput(
                "trade_flow",
                (("AGGTRADE_NOTIONALBUY", Decimal(12)), ("AGGTRADE_NOTIONALSELL", Decimal(2))),
                start,
                end,
                end,
                end + (500 if index == 2 else 50),
                source=instrument.key + ":aggTrade",
            )
            auxiliary.put(instrument, observation)
        feed.run_once()
        assert paper.data["feed"]["warmed"] == {"s": True}, feed.status()
        paper.command("resume", {})
        feed.run_once()
        row = paper.data["strategies"]["s"]
        assert row["pending"] is None and not row["analysis_input_health"]["ready"]
        assert not row["analysis_input_health"]["manual_recovery_required"]
        old = deepcopy(row["analysis_input"])
        policy = InputPolicy.from_document(old["policy"])
        selected = replay(
            source.manifest,
            old["head"],
            source.source,
            expected_analysis_id=old["binding"][:32],
            expected_policy=policy,
            expected_kind="paper_input",
        )
        assert selected[-1].pins == ()
        assert selected[-1].frame.input("trade_flow").quality == "missing"
        before = deepcopy(paper.data)
        feed.close()
        paper = PaperTradingService(
            factory,
            create_features,
            state=before,
            persist=lambda state: atomic_json(state_path, state),
        )
        paper.command("paper.strategy.recover", {"instance": "s"})
        assert paper.data["paused"]
        now[0] = START + 4 * MINUTE + 100
        source = PaperAnalysisSource(tmp_path, clock_ms=lambda: now[0])
        feed = PaperFeedSupervisor(paper, source, clock_ms=lambda: now[0])
        feed.run_once()
        row = paper.data["strategies"]["s"]
        assert row["analysis_input_health"]["ready"] and row["pending"] is None
        assert paper.data["paused"] and row["last_candle"] == START + 3 * MINUTE
        assert source.manifest.read(row["analysis_input"]["head"])["previous"] == old["head"]
        # The now-visible late fact never changes the previously consumed minute.
        replayed = replay(
            source.manifest,
            old["head"],
            source.source,
            expected_analysis_id=old["binding"][:32],
            expected_policy=policy,
            expected_kind="paper_input",
        )
        assert replayed == selected and replayed[-1].pins == ()
    finally:
        feed.close()
        candles.close()
        auxiliary.close()
        with connect_database(settings) as connection:
            connection.execute(
                "DELETE FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
            )
            connection.execute(
                "DELETE FROM market.auxiliary_minutes WHERE instrument=%s", (instrument.key,)
            )
