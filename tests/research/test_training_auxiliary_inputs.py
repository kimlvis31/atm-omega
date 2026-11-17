"""Real-file auxiliary joins and external demo-neuron training, without market IO."""

import hashlib
import json
import shutil
import time
from decimal import Decimal as D
from pathlib import Path

import pytest

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.ports.auxiliary import StoredAuxiliary
from omega.research import demo_instance, neural, training_inputs
from omega.research.auxiliary_datasets import (
    export_auxiliary_dataset,
    load_auxiliary_dataset,
    monte_carlo_auxiliary,
)
from omega.research.datasets import FIELDS, MarketRow, export_dataset, load_dataset
from omega.research.interpolation import interpolate_dataset
from omega.research.neural import read_training_run, train
from omega.research.service import ResearchService


def _observations(dataset, quantity=D(7)):
    result = []
    for row in load_dataset(dataset)[1]:
        if row.open_time_ms % 300000:
            continue
        exchange, market, symbol = row.instrument.split(":")
        instrument = Instrument(exchange, Market(market), symbol)
        observation = AuxiliaryInput(
            "open_interest",
            (("METRIC_OPENINTEREST", quantity), ("SOURCE_PERIOD_MS", 300000)),
            row.open_time_ms,
            row.open_time_ms + 60000,
            row.open_time_ms + 100,
            row.open_time_ms + 9_000_000,
            "observed",
            D(1),
            "offline-test:actual-availability",
        )
        checksum = observation_checksum(encode_observation(instrument, observation))
        result.append(StoredAuxiliary(instrument, observation, 2, checksum, "a" * 64))
    return result


@pytest.fixture
def observations(dataset, tmp_path):
    raw = tmp_path / "raw-received.jsonl"
    raw.write_bytes(b'{"synthetic_fixture_observation":true}\n')
    root = tmp_path / "observations"
    export_auxiliary_dataset(
        dataset, root, _observations(dataset), raw_files={"received.jsonl": raw}
    )
    return root


def _rename_inside_tmp(source, target, tmp_path):
    # Verify both absolute Windows targets before moving only this test's files.
    assert source.resolve().is_relative_to(tmp_path.resolve())
    assert target.resolve().is_relative_to(tmp_path.resolve())
    source.rename(target)


def _auxiliary_plugin(tmp_path):
    source = Path(demo_instance.__file__).read_text(encoding="utf-8")
    source = source.replace(
        "    def prepare(self, rows, context):\n",
        "    def prepare(self, rows, context):\n"
        "        assert max(row.open_time_ms for row in rows) < 42 * 60000\n"
        "        selected = [value for row in rows for value in row.auxiliary]\n"
        "        context.emit_event('auxiliary_prepare', samples=len(rows), "
        "observations=len(selected), available=[value.observation.available_at_ms "
        "for value in selected], revisions=[value.revision for value in selected])\n",
    )
    source = source.replace(
        "    def transform(self, rows, purpose, context):\n",
        "    def transform(self, rows, purpose, context):\n"
        "        context.emit_event('auxiliary_transform', purpose=purpose, "
        "samples=len(rows), observations=sum(len(row.auxiliary) for row in rows))\n",
    )
    source = source.replace(
        "float(row.volume) / self.scale,",
        "float(row.volume) / self.scale + "
        "sum(float(dict(value.observation.values)['METRIC_OPENINTEREST']) "
        "for value in row.auxiliary) / 10,",
    )
    source = source.replace(
        "        for epoch in range(self.completed_epochs, self.epochs):",
        "        assert len(train) == 84 and len(validation) == 18\n"
        "        context.emit_event('auxiliary_training_loop')\n"
        "        for epoch in range(self.completed_epochs, self.epochs):",
    )
    plugin = tmp_path / "auxiliary_neuron.py"
    plugin.write_text(source, encoding="utf-8")
    return plugin


def _completed(service, job):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        state = service.status(job["id"])
        if state["state"] in {"complete", "failed", "cancelled", "interrupted"}:
            assert state["state"] == "complete", state
            return state
        time.sleep(0.05)
    pytest.fail(f"auxiliary research job did not finish: {state}")


def test_legacy_training_keeps_exact_ohlcv_contract_and_bytes(dataset, tmp_path):
    original = {path.name: path.read_bytes() for path in dataset.iterdir() if path.is_file()}
    manifest, rows = load_dataset(dataset)
    loaded, inputs, identity = training_inputs.load_training_inputs(dataset)
    assert loaded == manifest and inputs == rows and identity is None
    assert all(type(row) is MarketRow and not hasattr(row, "auxiliary") for row in inputs)
    root = tmp_path / "legacy-training"
    run = train(dataset, Path(demo_instance.__file__), root, {"config": {"epochs": 1}})
    assert "auxiliary" not in run["dataset"]
    assert not (root / "input-observations").exists()
    assert (root / "input.rows.csv").read_bytes() == original["rows.csv"]
    assert json.loads((root / "input.dataset.json").read_text()) == manifest
    assert {
        path.name: path.read_bytes() for path in dataset.iterdir() if path.is_file()
    } == original


def test_sparse_join_retains_actual_future_availability_revision_and_native_values(observations):
    auxiliary_manifest, plain, selected = load_auxiliary_dataset(observations)
    manifest, rows, identity = training_inputs.load_training_inputs(observations / "dataset.json")
    assert manifest == load_dataset(observations / "candles")[0]
    assert [[getattr(row, name) for name in FIELDS] for row in rows] == [
        [getattr(row, name) for name in FIELDS] for row in plain
    ]
    assert all(isinstance(row, MarketRow) for row in rows)
    by_key = {
        (value.instrument.key, value.observation.window_start_ms): value for value in selected
    }
    for row in rows:
        expected = by_key.get((row.instrument, row.open_time_ms))
        assert row.auxiliary == (() if expected is None else (expected,))
    assert sum(len(row.auxiliary) for row in rows) == 24
    assert all(value.observation.available_at_ms > 60 * 60000 for value in selected)
    assert identity["channels"] == auxiliary_manifest["channels"]
    assert identity["quality"] == auxiliary_manifest["quality"]
    assert identity["observations_sha256"] == auxiliary_manifest["observations"]["sha256"]
    assert identity["scope"] == "retrospective_training_only"
    assert "Actual availability" in identity["availability_policy"]
    canonical = json.dumps(auxiliary_manifest, sort_keys=True, separators=(",", ":"))
    assert identity["manifest_sha256"] == hashlib.sha256(canonical.encode()).hexdigest()


def test_external_preparation_consumes_auxiliary_in_actual_training_loop(observations, tmp_path):
    plugin = _auxiliary_plugin(tmp_path)
    root = tmp_path / "actual-training"
    run = train(observations, plugin, root, {"config": {"epochs": 2}})
    view = read_training_run(root)
    events = view["events"]
    prepared = next(event["data"] for event in events if event["kind"] == "auxiliary_prepare")
    assert prepared["samples"] == 84 and prepared["observations"] == 18
    assert set(prepared["revisions"]) == {2}
    assert min(prepared["available"]) == 9_000_000
    transforms = [event["data"] for event in events if event["kind"] == "auxiliary_transform"]
    assert [(event["purpose"], event["samples"]) for event in transforms] == [
        ("train", 84),
        ("validation", 18),
        ("test", 18),
    ]
    loop = next(
        index for index, event in enumerate(events) if event["kind"] == "auxiliary_training_loop"
    )
    test = next(
        index
        for index, event in enumerate(events)
        if event["kind"] == "auxiliary_transform" and event["data"]["purpose"] == "test"
    )
    assert loop < test
    assert run["state"] == "complete" and run["fit_scope"] == "train_only"
    assert len(view["evaluations"]) == 18 and len(view["metrics"]) == 5
    model = json.loads((root / "model.json").read_text())
    assert model["completed_epochs"] == 2 and model["weight"] != 0
    assert (root / "instance_module.py").read_bytes() == plugin.read_bytes()
    baseline = tmp_path / "plain-preprocessing"
    train(observations, Path(demo_instance.__file__), baseline, {"config": {"epochs": 2}})
    assert json.loads((baseline / "model.json").read_text())["weight"] != model["weight"]
    assert run["dataset"]["auxiliary"] == training_inputs.load_training_inputs(observations)[2]


@pytest.mark.parametrize("generated", [False, True], ids=["original", "monte-carlo"])
def test_training_snapshot_is_self_contained_after_source_rename(observations, tmp_path, generated):
    source = observations
    job_training = None
    if generated:
        source = tmp_path / "generated"
        monte_carlo_auxiliary(observations, source, {"seed": 7, "block_bars": 2})
        service = ResearchService(tmp_path / "research-service")
        try:
            generated_job = _completed(
                service,
                service.submit(
                    "monte_carlo", {"dataset": str(observations), "seed": 7, "block_bars": 2}
                ),
            )
            assert generated_job["result"]["rows"] == 120
            assert generated_job["result"]["schema_id"] == "omega.market-observations.1m.v1"
            trained_job = _completed(
                service,
                service.submit(
                    "train",
                    {
                        "dataset": str(observations),
                        "demo": True,
                        "config": {"epochs": 1},
                        "monte_carlo": {"enabled": True, "seed": 7, "block_bars": 2},
                    },
                ),
            )
            job_training = Path(trained_job["result_file"]).parent
            assert (
                read_training_run(job_training)["run"]["dataset"]["auxiliary"]["transform"]["scope"]
                == "synthetic_research_only"
            )
        finally:
            service.close()
    expected = training_inputs.load_training_inputs(source)
    root = tmp_path / "training"
    run = train(source, Path(demo_instance.__file__), root, {"config": {"epochs": 1}})
    _rename_inside_tmp(source, tmp_path / "retired-source", tmp_path)
    if generated:
        _rename_inside_tmp(observations, tmp_path / "retired-original", tmp_path)
    snapshot = root / "input-observations"
    assert training_inputs.load_training_inputs(snapshot) == expected
    assert read_training_run(root)["run"]["dataset"]["auxiliary"] == expected[2]
    assert run["state"] == "complete"
    raw = snapshot / ("source/raw/received.jsonl" if generated else "raw/received.jsonl")
    assert raw.read_bytes() == b'{"synthetic_fixture_observation":true}\n'
    if generated:
        parent = load_auxiliary_dataset(snapshot / "source")
        assert parent[0]["transform"] is None
        assert expected[2]["transform"]["scope"] == "synthetic_research_only"
        assert training_inputs.load_training_inputs(job_training / "input-observations") == expected


def test_repaired_source_masks_are_retained_in_training_snapshot(gapped_source, tmp_path):
    base, repaired, joined = (tmp_path / name for name in ("base", "repaired", "joined"))
    export_dataset(gapped_source, base, {})
    interpolate_dataset(base, repaired, {"method": "previous_close_flat", "max_ratio": 1.0})
    export_auxiliary_dataset(repaired, joined, _observations(repaired))
    evidence = {
        name: (repaired / name).read_bytes()
        for name in ("quality-mask.jsonl", "source.rows.csv", "source.dataset.json")
    }
    root = tmp_path / "training"
    train(joined, Path(demo_instance.__file__), root, {"config": {"epochs": 1}})
    _rename_inside_tmp(joined, tmp_path / "retired-joined", tmp_path)
    _rename_inside_tmp(repaired, tmp_path / "retired-repaired", tmp_path)
    _rename_inside_tmp(base, tmp_path / "retired-base", tmp_path)
    snapshot = root / "input-observations"
    assert load_auxiliary_dataset(snapshot)[0]["candle_evidence"]
    assert all(
        (snapshot / "candles" / name).read_bytes() == value for name, value in evidence.items()
    )


def test_auxiliary_change_alone_changes_identity_and_rejects_resume(
    dataset, observations, tmp_path
):
    changed = tmp_path / "changed-observations"
    export_auxiliary_dataset(dataset, changed, _observations(dataset, D(8)))
    before = training_inputs.load_training_inputs(observations)
    after = training_inputs.load_training_inputs(changed)
    assert before[0] == after[0]
    assert before[2]["observations_sha256"] != after[2]["observations_sha256"]
    assert before[2]["manifest_sha256"] != after[2]["manifest_sha256"]
    first = tmp_path / "first"
    module = Path(demo_instance.__file__)
    train(observations, module, first, {"config": {"epochs": 1}})
    with pytest.raises(ValueError, match="dataset/split/provenance mismatch"):
        train(
            changed,
            module,
            tmp_path / "denied",
            {"config": {"epochs": 2}, "resume": {"run": str(first), "checkpoint": "model.json"}},
        )


def test_unchanged_auxiliary_resume_matches_uninterrupted_model(observations, tmp_path):
    plugin = _auxiliary_plugin(tmp_path)
    first, resumed, complete = (tmp_path / name for name in ("first", "resumed", "complete"))
    train(observations, plugin, first, {"config": {"epochs": 1}})
    run = train(
        observations,
        plugin,
        resumed,
        {"config": {"epochs": 3}, "resume": {"run": str(first), "checkpoint": "model.json"}},
    )
    train(observations, plugin, complete, {"config": {"epochs": 3}})
    assert json.loads((resumed / "model.json").read_text()) == json.loads(
        (complete / "model.json").read_text()
    )
    assert run["resume"]["scope"] == "same_dataset_and_module_epoch_boundary"
    assert not any(
        event["kind"] == "auxiliary_prepare" for event in read_training_run(resumed)["events"]
    )


def test_source_change_during_snapshot_is_detected(dataset, observations, tmp_path, monkeypatch):
    changed = tmp_path / "replacement"
    export_auxiliary_dataset(
        dataset,
        changed,
        _observations(dataset, D(99)),
        raw_files={"received.jsonl": observations / "raw/received.jsonl"},
    )
    original = training_inputs.snapshot_auxiliary_dataset

    def swap_then_snapshot(source, destination, cancel=None):
        # Fault injection swaps real, independently valid files at the copy boundary.
        for name in ("dataset.json", "observations.jsonl"):
            shutil.copyfile(changed / name, Path(source) / name)
        return original(source, destination, cancel)

    monkeypatch.setattr(training_inputs, "snapshot_auxiliary_dataset", swap_then_snapshot)
    with pytest.raises(ValidationError, match="changed while snapshotting"):
        training_inputs.snapshot_training_inputs(observations, tmp_path / "training")


def test_source_change_between_initial_train_load_and_snapshot_is_detected(
    dataset, observations, tmp_path, monkeypatch
):
    changed = tmp_path / "replacement"
    export_auxiliary_dataset(
        dataset,
        changed,
        _observations(dataset, D(99)),
        raw_files={"received.jsonl": observations / "raw/received.jsonl"},
    )
    original = neural.snapshot_training_inputs

    def swap_then_snapshot(source, destination, cancel=None):
        for name in ("dataset.json", "observations.jsonl"):
            shutil.copyfile(changed / name, Path(source) / name)
        return original(source, destination, cancel)

    monkeypatch.setattr(neural, "snapshot_training_inputs", swap_then_snapshot)
    with pytest.raises((ValidationError, ValueError), match="changed"):
        train(
            observations,
            Path(demo_instance.__file__),
            tmp_path / "training",
            {"config": {"epochs": 1}},
        )
