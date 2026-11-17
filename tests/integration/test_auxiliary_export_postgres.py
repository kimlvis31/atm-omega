"""Explicit isolated PG -> spawned export -> independent portable observation bundle."""

import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.collection import MinuteObservation
from omega.research.auxiliary_datasets import (
    load_auxiliary_dataset,
    monte_carlo_auxiliary,
    snapshot_auxiliary_dataset,
)
from omega.research.datasets import export_dataset, load_dataset
from omega.research.service import ResearchService
from omega.runtime.auxiliary_research import run_auxiliary_export_job
from omega.runtime.export_cli import export_observations

pytestmark = pytest.mark.database
MINUTE = 60000
START = 1_700_000_040_000 // 3_600_000 * 3_600_000
END = START + 20 * MINUTE
RECEIVED = END + 1000


@pytest.fixture(scope="module")
def settings(request):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated database opt-in")
    configured = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not configured:
        pytest.fail("OMEGA_TEST_DB_CONFIG must name the isolated test database")
    path = Path(configured).resolve()
    value = load_database_settings(path)
    if (value.host, value.port, value.dbname, value.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing export tests outside 127.0.0.1:55433/omega_test/omega_app")
    with connect_database(value) as connection:
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
    return value, path


@pytest.fixture
def saved(settings, tmp_path):
    value, config = settings
    instrument = Instrument(
        "binance", Market.USDM, "TEST_AUXEXPORT_" + uuid4().hex[:12].upper() + "USDT"
    )
    auxiliary = PostgresAuxiliaryStore(value)
    candles = PostgresCandleStore(value)
    source = instrument.key + ":open_interest_history"
    funding_source = instrument.key + ":funding_rate"
    try:
        for minute in range(20):
            opening = START + minute * MINUTE
            candle = Candle(
                instrument,
                opening,
                opening + MINUTE,
                Decimal(100),
                Decimal(105),
                Decimal(95),
                Decimal(101),
                Decimal("3.25"),
            )
            assert candles.put(
                MinuteObservation(candle, "rest", RECEIVED, raw='{"fixture":"aux-export"}')
            )
        first = AuxiliaryInput(
            "open_interest",
            (("METRIC_OPENINTEREST", Decimal("7.123")),),
            START,
            START + MINUTE,
            START + 10,
            RECEIVED,
            source=source,
        )
        receipt = auxiliary.put(instrument, first)
        second = replace(
            first,
            values=(("METRIC_OPENINTEREST", Decimal("9.456")),),
            available_at_ms=RECEIVED + 5000,
        )
        auxiliary.put(instrument, second, expected_previous=receipt.checksum)
        sparse = replace(
            first,
            window_start_ms=START + 6 * MINUTE,
            window_end_ms=START + 7 * MINUTE,
            event_time_ms=START + 6 * MINUTE + 10,
            available_at_ms=RECEIVED - 1,
        )
        auxiliary.put(instrument, sparse)
        future = replace(
            first,
            window_start_ms=START + 8 * MINUTE,
            window_end_ms=START + 9 * MINUTE,
            event_time_ms=START + 8 * MINUTE + 10,
            available_at_ms=RECEIVED + 10000,
        )
        auxiliary.put(instrument, future)
        auxiliary.put(instrument, replace(first, source=instrument.key + ":not_selected"))
        funding = AuxiliaryInput(
            "funding_rate",
            (("FUNDING_RATE", Decimal("-.000125")),),
            START + 2 * MINUTE,
            START + 3 * MINUTE,
            START + 2 * MINUTE + 10,
            RECEIVED - 1,
            source=funding_source,
        )
        auxiliary.put(instrument, funding)
        csv = tmp_path / "committed.csv"
        report = export_observations(candles, instrument, csv, limit=20)
        assert report["rows"] == 20
        dataset = tmp_path / "candles"
        export_dataset(csv, dataset, {})
        selections = [
            {"instrument": instrument.key, "channel": "open_interest", "source": source},
            {"instrument": instrument.key, "channel": "funding_rate", "source": funding_source},
            {
                "instrument": instrument.key,
                "channel": "long_short_ratio",
                "source": instrument.key + ":absent_selection",
            },
        ]
        yield {
            "settings": value,
            "config": config,
            "instrument": instrument,
            "store": auxiliary,
            "csv": csv,
            "dataset": dataset,
            "selections": selections,
            "first": first,
            "second": second,
            "first_checksum": receipt.checksum,
        }
    finally:
        auxiliary.close()
        candles.close()
        with connect_database(value) as connection:
            for table in ("auxiliary_minutes", "candles_1m"):
                connection.execute(
                    f"DELETE FROM market.{table} WHERE instrument=%s", (instrument.key,)
                )
                assert (
                    connection.execute(
                        f"SELECT count(*) FROM market.{table} WHERE instrument=%s",
                        (instrument.key,),
                    ).fetchone()[0]
                    == 0
                )


def finished(service, job):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        status = service.status(job["id"])
        if status["state"] in {"complete", "failed", "interrupted", "cancelled"}:
            return status
        time.sleep(0.05)
    raise AssertionError("isolated export worker did not reach a terminal state")


def expected(saved, cutoff):
    result = []
    for selection in saved["selections"]:
        for minute in range(20):
            value = saved["store"].get(
                saved["instrument"],
                selection["channel"],
                selection["source"],
                START + minute * MINUTE,
                as_of_ms=cutoff,
            )
            if value is not None:
                result.append(value)
    return sorted(
        result, key=lambda row: (row.observation.window_start_ms, row.observation.channel)
    )


def assert_provenance(manifest, selections, cutoff):
    entry = manifest["export_provenance"]
    assert entry["data"] == {
        "schema_version": 1,
        "scope": "retrospective_training_only",
        "origin": "committed_postgresql",
        "as_of_ms": cutoff,
        "selections": selections,
    }
    canonical = json.dumps(
        entry["data"], sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    assert entry["sha256"] == sha256(canonical).hexdigest()
    assert "long_short_ratio" not in manifest["channels"]
    assert any(
        selection["source"].endswith(":absent_selection")
        for selection in entry["data"]["selections"]
    )


def retire_inside(source, target, root):
    assert source.resolve().is_relative_to(root.resolve())
    assert target.resolve().is_relative_to(root.resolve())
    source.rename(target)


@pytest.mark.parametrize("cutoff", [RECEIVED - 1, RECEIVED, RECEIVED + 5000])
def test_actual_spawn_export_keeps_exact_cutoff_revisions_and_self_contained_inputs(
    saved, tmp_path, cutoff
):
    root = tmp_path / "research"
    root.mkdir()
    shutil.copyfile(saved["config"], root / "database.config.toml")
    service = ResearchService(root, handlers={"export_auxiliary": run_auxiliary_export_job})
    try:
        prior = saved["store"].get(
            saved["instrument"], "open_interest", saved["first"].source, START, as_of_ms=RECEIVED
        )
        result = finished(
            service,
            service.submit(
                "export_auxiliary",
                {
                    "candle_dataset": str(saved["dataset"]),
                    "selections": saved["selections"],
                    "as_of_ms": cutoff,
                },
            ),
        )
        assert result["state"] == "complete", result
        bundle = Path(result["result_file"]).parent
        manifest, rows, observations = load_auxiliary_dataset(bundle)
        assert len(rows) == 20 and manifest["state"] == "complete"
        assert sorted(
            observations, key=lambda row: (row.observation.window_start_ms, row.observation.channel)
        ) == expected(saved, cutoff)
        assert len(observations) == (2 if cutoff < RECEIVED else 3)
        assert all(row.observation.available_at_ms <= cutoff for row in observations)
        assert not any(
            row.observation.window_start_ms == START + 8 * MINUTE for row in observations
        )
        assert result["result"]["requested_minute_slots"] == 60
        assert result["result"]["as_of_ms"] == cutoff
        assert result["result"]["scope"] == "retrospective_training_only"
        assert manifest["raw_payloads"] == []
        assert_provenance(manifest, saved["selections"], cutoff)
        selection_report = json.loads((Path(result["artifact"]) / "selection.json").read_text())
        assert selection_report == result["result"]
        assert (
            saved["store"].get(
                saved["instrument"],
                "open_interest",
                saved["first"].source,
                START,
                as_of_ms=RECEIVED,
            )
            == prior
        )
        assert prior.revision == 1 and prior.checksum == saved["first_checksum"]
        # Remove original locations inside this test's temporary root only.
        retire_inside(saved["csv"], tmp_path / "unavailable-source.csv", tmp_path)
        retire_inside(saved["dataset"], tmp_path / "unavailable-candle-dataset", tmp_path)
        assert load_auxiliary_dataset(bundle) == (manifest, rows, observations)
        assert_provenance(load_auxiliary_dataset(bundle)[0], saved["selections"], cutoff)
        generated = tmp_path / "joint-mc"
        monte_carlo_auxiliary(bundle, generated, {"seed": 7, "block_bars": 2, "splits": ["train"]})
        generated_manifest, _, _ = load_auxiliary_dataset(generated)
        assert generated_manifest["export_provenance"] == manifest["export_provenance"]
        snapshot = tmp_path / "mc-snapshot"
        snapshot_auxiliary_dataset(generated, snapshot)
        retire_inside(generated, tmp_path / "unavailable-mc-source", tmp_path)
        assert (
            load_auxiliary_dataset(snapshot)[0]["export_provenance"]
            == manifest["export_provenance"]
        )
        inspected = finished(service, service.submit("inspect_dataset", {"dataset": str(bundle)}))
        assert inspected["state"] == "complete", inspected
        assert (
            inspected["result"]["rows"] == 20
            and inspected["result"]["auxiliary"]["scope"] == "retrospective_training_only"
        )
        assert inspected["result"]["time_range"] == [START, END]
        assert len(load_dataset(bundle / "candles")[1]) == 20
    finally:
        service.close()


def test_actual_cli_auxiliary_branch_creates_independent_bundle_and_preserves_existing_target(
    saved, tmp_path
):
    destination = tmp_path / "cli-bundle"
    command = [
        sys.executable,
        "-I",
        "-m",
        "omega.runtime.export_cli",
        "--instrument",
        saved["instrument"].key,
        "--db-config",
        str(saved["config"]),
        "--candle-dataset",
        str(saved["dataset"]),
        "--output",
        str(destination),
        "--as-of-ms",
        str(RECEIVED),
    ]
    for selection in saved["selections"]:
        command.extend(["--auxiliary-source", selection["channel"] + "=" + selection["source"]])
    result = subprocess.run(command, capture_output=True, text=True, timeout=20, check=False)
    assert result.returncode == 0, result.stderr + result.stdout
    summary = json.loads(result.stdout)
    assert summary["observations"] == 3 and summary["as_of_ms"] == RECEIVED
    original = (destination / "dataset.json").read_bytes()
    repeated = subprocess.run(command, capture_output=True, text=True, timeout=20, check=False)
    assert repeated.returncode == 1
    assert (destination / "dataset.json").read_bytes() == original
    retire_inside(saved["csv"], tmp_path / "unavailable-cli-source.csv", tmp_path)
    retire_inside(saved["dataset"], tmp_path / "unavailable-cli-dataset", tmp_path)
    manifest, rows, observations = load_auxiliary_dataset(destination)
    assert_provenance(manifest, saved["selections"], RECEIVED)
    assert len(rows) == 20
    assert sorted(
        observations, key=lambda row: (row.observation.window_start_ms, row.observation.channel)
    ) == expected(saved, RECEIVED)
