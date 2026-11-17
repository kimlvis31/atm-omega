"""Real committed source -> independent CA, recovery and frozen checkpoint fences."""

import os
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.infrastructure.candle_feed import CommittedCandleSource
from omega.infrastructure.candles import EPOCH, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.runtime.analysis_execution import AnalysisExecution
from omega.runtime.analysis_library import AnalysisLibrary

pytestmark = pytest.mark.database
START = 1_700_000_040_000
END = START + 180000


@pytest.fixture
def case(tmp_path):
    path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not path:
        pytest.fail("explicit isolated OMEGA_TEST_DB_CONFIG is required")
    settings = load_database_settings(Path(path))
    if (settings.host, settings.port, settings.dbname, settings.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing CA test outside isolated Omega database")
    (tmp_path / "database.config.toml").write_bytes(Path(path).read_bytes())
    key = "binance:usdm:TEST_CA_" + uuid4().hex[:24].upper()
    library = AnalysisLibrary(tmp_path / "library.json", references_provider=lambda: [])
    template = library.command(
        "analysis.template.create",
        {
            "expected_revision": 0,
            "name": "CAC",
            "configuration": {
                "moving_averages": [{"interval_id": 0, "ema_period": 2, "mmacd_periods": []}]
            },
        },
        operation_id="template",
    )["item"]
    ca = library.command(
        "analysis.instance.create",
        {"expected_revision": 1, "name": "CA", "template_id": template["id"], "instrument": key},
        operation_id="instance",
    )["item"]
    frozen = library.snapshot_for_execution(ca["id"], 1)

    def write(index, close=11):
        opening = START + index * 60000
        with connect_database(settings) as connection:
            connection.execute(
                "INSERT INTO market.candles_1m "
                "(instrument,open_time,close_time,o,h,l,c,v,source,received_ms) "
                "VALUES (%s,%s,%s,10,12,9,%s,2,'rest',%s)",
                (
                    key,
                    EPOCH + timedelta(milliseconds=opening),
                    EPOCH + timedelta(milliseconds=opening + 60000),
                    close,
                    END,
                ),
            )

    engine = AnalysisExecution(
        tmp_path, lambda: CommittedCandleSource(tmp_path), clock_ms=lambda: END, background=False
    )
    yield tmp_path, settings, key, frozen, write, engine
    engine.close()
    with connect_database(settings) as connection:
        connection.execute("DELETE FROM market.candles_1m WHERE instrument=%s", (key,))


def test_committed_db_gap_blocks_then_repair_resumes_without_advancing_failed_checkpoint(case):
    root, _, _, frozen, write, engine = case
    write(0)
    write(2)
    engine.start(frozen, warmup_minutes=3)
    path = root / "state/analysis-executions" / (frozen["analysis_id"] + ".json")
    before = path.read_bytes()
    assert not engine.run_once()
    row = engine.snapshot()["instances"][0]
    assert row["state"] == "blocked" and not row["ready"]
    assert row["warmup"]["processed_minutes"] == 0 and path.read_bytes() == before
    write(1)
    engine.notify()
    assert engine.run_once()
    row = engine.snapshot()["instances"][0]
    assert row["ready"] and row["outputs"]["0_EMA_0_EMA"] == 11.0


def test_real_source_restart_requires_manual_start_and_revalidates_checkpoint_anchor(case):
    root, settings, key, frozen, write, engine = case
    for index in range(3):
        write(index)
    engine.start(frozen, warmup_minutes=3)
    assert engine.run_once()
    expected = engine.snapshot()["instances"][0]["outputs"]
    engine.close()
    restarted = AnalysisExecution(
        root, lambda: CommittedCandleSource(root), clock_ms=lambda: END, background=False
    )
    try:
        assert restarted.snapshot()["instances"][0]["state"] == "stopped"
        assert not restarted.run_once()
        restarted.start(frozen, warmup_minutes=3)
        assert restarted.run_once()
        assert restarted.snapshot()["instances"][0]["outputs"] == expected
        assert not restarted.run_once()  # Current minute already processed: no DB polling.
        row = restarted.snapshot()["instances"][0]
        restarted.stop(frozen["analysis_id"], expected_execution_id=row["execution_id"])
        with connect_database(settings) as connection:
            connection.execute(
                "UPDATE market.candles_1m SET c=10.5 WHERE instrument=%s AND open_time=%s",
                (key, EPOCH + timedelta(milliseconds=END - 60000)),
            )
        restarted.start(frozen)
        assert not restarted.run_once()
        row = restarted.snapshot()["instances"][0]
        assert row["state"] == "blocked" and not row["ready"]
        assert row["outputs"] == expected
    finally:
        restarted.close()
