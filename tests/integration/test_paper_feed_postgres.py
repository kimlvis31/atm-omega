"""Explicit opt-in Omega test DB → committed feed → real paper engine smoke."""

import json
import os
import time
import tomllib
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.analysis.catalog import create_features
from omega.application.paper import PaperTradingService
from omega.domain.errors import ProtocolError
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.candle_feed import CommittedCandleSource
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import migrate
from omega.ports.collection import MinuteObservation
from omega.ports.modules import ModuleDescriptor
from omega.runtime.app import build_core
from omega.runtime.paper_feed import PaperFeedSupervisor
from omega.strategies.registry import StrategyRegistry
from omega.strategies.target_quantity.module import TargetQuantityModule

pytestmark = pytest.mark.database
MINUTE = 60000
START = 1_700_000_040_000


@pytest.fixture(scope="module")
def feed_database(request):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated database opt-in")
    path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not path:
        pytest.fail("OMEGA_TEST_DB_CONFIG must select the isolated Omega test DB")
    path = Path(path).resolve()
    settings = load_database_settings(path)
    if (settings.host, settings.port, settings.dbname, settings.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing paper-feed tests outside 127.0.0.1:55433/omega_test/omega_app")
    with connect_database(settings) as connection:
        assert connection.execute("SELECT current_database()").fetchone() == ("omega_test",)
        migrate(connection)
    return path, settings


@pytest.fixture
def committed(feed_database, tmp_path):
    config_path, settings = feed_database
    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    secret = Path(raw["password_file"])
    if not secret.is_absolute():
        secret = config_path.parent / secret
    configuration = {
        "host": settings.host,
        "port": settings.port,
        "dbname": settings.dbname,
        "user": settings.user,
        "password_file": secret.resolve().as_posix(),
    }
    (tmp_path / "database.config.toml").write_text(
        "\n".join(f"{key} = {json.dumps(value)}" for key, value in configuration.items()),
        encoding="utf-8",
    )
    instrument = Instrument("binance", Market.SPOT, "FEED" + uuid4().hex[:24].upper() + "USDT")
    store = PostgresCandleStore(settings)
    try:
        yield instrument, store, CommittedCandleSource(tmp_path)
    finally:
        store.close()


def observed(instrument, minute, price="100"):
    value, opened = Decimal(price), START + minute * MINUTE
    return MinuteObservation(
        Candle(
            instrument, opened, opened + MINUTE, value, value + 1, value - 1, value, Decimal(10)
        ),
        "rest",
        opened + MINUTE + 1,
    )


def paper_for(instrument, *, state=None, analyzed=False):
    def factory(name, instrument, configuration):
        if not analyzed:
            return StrategyRegistry().create(name, instrument, configuration)
        module = TargetQuantityModule(instrument, configuration)
        module.descriptor = ModuleDescriptor("feed_probe", "1", frozenset({"CLOSED_0"}), True)
        return module

    paper = PaperTradingService(factory, create_features, state=state)
    if state is None:
        paper.command("paper.account.create", {"account_id": "test", "balance": "10000"})
        paper.command(
            "paper.strategy.add",
            {
                "account_id": "test",
                "instance": "probe",
                "instrument": instrument.key,
                "module_id": "target_quantity",
                "budget": "1000",
                "configuration": {"quantity": "1"},
            },
        )
    return paper


def wait_for(predicate):
    deadline = time.monotonic() + 5
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("isolated committed paper feed did not progress")
        time.sleep(0.01)


def test_committed_two_minute_background_feed_next_bar_fill_and_readonly_query(committed):
    instrument, store, source = committed
    paper, now = paper_for(instrument), [START + MINUTE]
    paper.feed_configure(True, start_ms=START, warmup_minutes=2)
    paper.command("resume", {})
    feed = PaperFeedSupervisor(paper, source, clock_ms=lambda: now[0], poll_seconds=0.05)
    assert store.put(observed(instrument, 0))
    connection = source._connect()
    try:
        assert connection.execute("SHOW default_transaction_read_only").fetchone() == ("on",)
        assert connection.execute("SHOW transaction_read_only").fetchone() == ("on",)
    finally:
        connection.close()
    feed.start()
    try:
        wait_for(lambda: feed.status()["frames"] == 1)
        assert paper.detail("probe")["quantity"] == "0"
        assert store.put(observed(instrument, 1, "110"))
        assert not store.put(observed(instrument, 1, "110"))
        now[0] = START + 2 * MINUTE
        wait_for(lambda: feed.status()["frames"] == 2)
        assert paper.detail("probe")["quantity"] == "1"
        assert paper.detail("probe")["entry_price"] == "110"
        assert len(paper.detail("probe")["fills"]) == 1
        assert paper.snapshot()["feed"]["provenance"][instrument.key] == "database_committed"
    finally:
        feed.close()
    assert not feed.status()["supervisor_alive"]


def test_real_committed_gap_blocks_then_repairs_and_restart_remains_paused(committed):
    instrument, store, source = committed
    core, now = build_core(source.root), [START + MINUTE]
    try:
        paper, feed = core.paper, core.paper_feed
        feed.clock_ms = lambda: now[0]
        paper.command("paper.account.create", {"account_id": "test", "balance": "10000"})
        paper.command(
            "paper.strategy.add",
            {
                "account_id": "test",
                "instance": "probe",
                "instrument": instrument.key,
                "module_id": "target_quantity",
                "budget": "1000",
                "configuration": {"quantity": "1"},
            },
        )
        store.put(observed(instrument, 0))
        paper.feed_configure(True, start_ms=START, warmup_minutes=2)
        feed.run_once()
        feed.run_once()
        # User stayed paused; demonstrate data recovery without generating entry orders.
        store.put(observed(instrument, 2))
        now[0] = START + 3 * MINUTE
        feed.run_once()
        assert feed.status()["reason"] == "missing_minute"
        assert paper.data["candles"][instrument.key][-1]["open_time_ms"] == START
        store.put(observed(instrument, 1))
        with pytest.raises(ProtocolError):
            store.put(observed(instrument, 1, "999"))
        feed.run_once()
        assert paper.data["candles"][instrument.key][-1]["open_time_ms"] == START + 2 * MINUTE
        assert paper.data["paused"] is True and paper.snapshot()["feed"]["ready"] is True
        epoch = paper.feed_snapshot()["epoch"]
    finally:
        core.close()
    # Actual composition/owner lock is reopened from its atomic persisted paper.json.
    restarted = build_core(source.root)
    try:
        restored, reopened = restarted.paper, restarted.paper_feed
        reopened.clock_ms = lambda: START + 4 * MINUTE
        reopened.run_once()
        assert reopened.status()["reason"] == "manual_recovery_required"
        assert restored.feed_snapshot()["epoch"] != epoch and restored.data["paused"] is True
        restored.command("paper.strategy.recover", {"instance": "probe"})
        store.put(observed(instrument, 3))
        reopened.run_once()
        reopened.run_once()
        assert restored.data["candles"][instrument.key][-1]["open_time_ms"] == START + 3 * MINUTE
        assert restored.data["paused"] is True and restored.detail("probe")["fills"] == []
    finally:
        restarted.close()


def test_real_analysis_warmup_reads_older_commits_without_replaying_trades(committed):
    instrument, store, source = committed
    for minute in range(3):
        store.put(observed(instrument, minute))
    paper = paper_for(instrument, analyzed=True)
    paper.feed_configure(True, start_ms=START + 2 * MINUTE, warmup_minutes=2)
    paper.command("resume", {})
    feed = PaperFeedSupervisor(paper, source, clock_ms=lambda: START + 3 * MINUTE)
    feed.run_once()
    assert not paper.data["candles"] and not paper.detail("probe")["fills"]
    assert paper.detail("probe")["pending"] is None
    assert paper.feed_snapshot()["warmup"] == {}
    feed.run_once()
    assert paper.detail("probe")["pending"] is not None
    assert paper.detail("probe")["quantity"] == "0"
    assert len(paper.data["candles"][instrument.key]) == 1
    feed.close()
