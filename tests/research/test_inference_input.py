"""Raw inference is retrospective research; all market rows here are synthetic."""

import ast
import copy
import csv
import json
import math
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from omega.research import demo_instance
from omega.research.artifacts import content_hash, write_json
from omega.research.datasets import FIELDS, export_dataset
from omega.research.deployment import (
    ShadowDeployment,
    ShadowModelContract,
    prepare_candidate,
    verify_fixture,
)
from omega.research.inference_input import (
    MAX_BYTES,
    identity,
    load_window,
    manifest_identity,
    parse_parameters,
    parse_prepared,
    parse_request,
    read_prepared,
)
from omega.research.neural import inspect_module, train
from omega.research.prediction import predict_run, prepare_run
from omega.research.service import ResearchService


def completed(service, job, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = service.status(job["id"])
        if row["state"] in {"complete", "failed", "cancelled", "interrupted"}:
            assert row["state"] == "complete", row
            return row
        time.sleep(0.02)
    raise AssertionError("research child did not finish")


def parameters(source, dataset, *, contract=None, candidate_id="a" * 64, opening=600000):
    run = json.loads((source / "run.json").read_text())
    artifact = next(item for item in run["artifacts"] if item["file"] == run["model"]["file"])
    return {
        "training_run": str(source),
        "dataset": str(dataset),
        "trusted_module": True,
        "request": {
            "candidate_id": candidate_id,
            "module_sha256": run["module_source_sha256"],
            "checkpoint_sha256": artifact["sha256"],
            "dataset_sha256": manifest_identity(dataset),
            "contract": (
                contract or ShadowModelContract("omega.ohlcv.1m.v1", ("volume",), 1, (None,))
            ).document(),
            "instrument": "binance:usdm:BTCUSDT",
            "open_time_ms": opening,
            "stream": "raw-demo",
            "sequence": 1,
            "state_hash": identity(None),
            "core_epoch": "core-a",
            "deployment_epoch": "b" * 32,
            "stream_epoch": "c" * 32,
            "deadline_ms": int(time.time() * 1000) + 60000,
        },
    }


@pytest.fixture
def raw_case(dataset, tmp_path):
    source = tmp_path / "training"
    train(dataset, Path(demo_instance.__file__), source, {"config": {"epochs": 1}})
    params = parameters(source, dataset)
    destination = tmp_path / "preparation"
    result = prepare_run(source, destination, params)
    return source, dataset, params, destination, result


def test_restored_demo_preparation_freezes_raw_and_preserves_training(raw_case, tmp_path):
    source, dataset, params, destination, result = raw_case
    before = {p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()}
    model = json.loads((source / "model.json").read_text())
    assert result["batch"] == [20 / model["volume_scale"]]
    assert result["used_rows"] == [0]
    assert result["window"][0]["volume"] == "20"
    assert result["reference_time_ms"] == 660000
    assert result["as_of_ms"] == result["snapshot_available_at_ms"] > result["reference_time_ms"]
    assert read_prepared(destination / "prepared.json", params["request"]) == result
    predicted = predict_run(
        source, tmp_path / "prediction", {"batch": result["batch"], "trusted_module": True}
    )
    assert predicted["prediction"]["values"] == [
        model["weight"] * result["batch"][0] + model["bias"]
    ]
    assert result["dataset_sha256"] == content_hash(dataset / "dataset.json")
    assert result["rows_sha256"] == content_hash(dataset / "rows.csv")
    assert result["dataset_sha256"] != result["rows_sha256"]
    assert all((source / name).read_bytes() == value for name, value in before.items())


@pytest.mark.parametrize(
    "mutation",
    [
        "nan",
        "shape",
        "used",
        "bool",
        "future",
        "availability",
        "hash",
        "candidate",
        "unknown",
        "module",
        "large",
    ],
)
def test_prepared_codec_rejects_invalid_or_cross_bound_results(raw_case, mutation):
    _, _, params, _, result = raw_case
    changed = copy.deepcopy(result)
    if mutation == "nan":
        changed["batch"] = [float("nan")]
    elif mutation == "shape":
        changed["batch"] = [[1]]
    elif mutation == "used":
        changed["used_rows"] = []
    elif mutation == "bool":
        changed["used_rows"] = [False]
    elif mutation == "future":
        changed["window"][0]["open_time_ms"] += 60000
    elif mutation == "availability":
        changed["as_of_ms"] = changed["reference_time_ms"] - 1
    elif mutation == "hash":
        changed["rows_sha256"] = "not-a-sha"
    elif mutation == "candidate":
        changed["request"]["candidate_id"] = "e" * 64
    elif mutation == "unknown":
        changed["extra"] = 0
    elif mutation == "module":
        changed["module"]["capabilities"]["prepare_inference"] = False
    elif mutation == "large":
        changed["lineage"]["synthetic"] = "x" * MAX_BYTES
    if mutation not in {"nan", "large"}:
        changed["batch_sha256"] = identity(changed["batch"])
        changed["window_sha256"] = identity(changed["window"])
        changed["prepared_sha256"] = identity(
            {k: v for k, v in changed.items() if k != "prepared_sha256"}
        )
    with pytest.raises(ValueError):
        parse_prepared(changed, params["request"])


def test_manifest_bytes_change_duplicate_json_and_missing_inputs_rejected(raw_case, tmp_path):
    _, dataset, params, destination, result = raw_case
    altered = params["request"] | {"dataset_sha256": "0" * 64}
    with pytest.raises(ValueError, match="manifest bytes"):
        load_window(dataset, altered)
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text(
        '{"schema_version":1,' + (destination / "prepared.json").read_text().lstrip()[1:]
    )
    with pytest.raises(ValueError, match="duplicate"):
        read_prepared(duplicate, params["request"])
    with pytest.raises(ValueError):
        parse_prepared(result | {"lineage": None}, params["request"])
    assert parse_parameters(params | {"seed": 0, "settings_revision": 2})["seed"] == 0
    for change in ({"seed": True}, {"settings_revision": False}, {"timeout_seconds": 121}):
        with pytest.raises(ValueError):
            parse_parameters(params | change)


@pytest.mark.parametrize("fault", ["gap", "duplicate", "mc", "repair", "future", "source"])
def test_invalid_raw_window_rejected_before_instance_creation(
    raw_case, tmp_path, monkeypatch, fault
):
    source, dataset, params, _, _ = raw_case
    manifest = json.loads((dataset / "dataset.json").read_text())
    lines = (dataset / "rows.csv").read_text().splitlines()
    if fault == "gap":
        lines = [line for line in lines if not line.startswith("binance:usdm:BTCUSDT,600000,")]
        manifest["rows"]["count"] -= 1
    elif fault == "duplicate":
        lines.append(
            next(line for line in lines if line.startswith("binance:usdm:BTCUSDT,600000,"))
        )
        manifest["rows"]["count"] += 1
    elif fault == "mc":
        manifest["dataset_kind"] = "generated_market"
    elif fault == "repair":
        manifest["repair"] = {"causal": False}
    elif fault == "source":
        (dataset / manifest["source"]["file"]).write_text("changed")
    (dataset / "rows.csv").write_text("\n".join(lines) + "\n")
    manifest["rows"]["sha256"] = content_hash(dataset / "rows.csv")
    write_json(dataset / "dataset.json", manifest)
    params["request"]["dataset_sha256"] = manifest_identity(dataset)
    monkeypatch.setattr(
        "omega.research.prediction.PredictionSession",
        lambda *a, **k: pytest.fail("invalid raw window reached plugin"),
    )
    with pytest.raises(ValueError):
        prepare_run(
            source,
            tmp_path / "denied",
            params,
            clock_ms=(lambda: 600000) if fault == "future" else None,
        )
    assert not (tmp_path / "denied").exists()


def test_old_frozen_module_retains_numeric_predict_but_raw_is_explicitly_unsupported(
    raw_case, tmp_path
):
    source, dataset, _, _, _ = raw_case
    module = source / "instance_module.py"
    module.write_text(
        module.read_text().replace('"prepare_inference": True,', '"prepare_inference": False,')
    )
    run = json.loads((source / "run.json").read_text())
    run["module"]["capabilities"]["prepare_inference"] = False
    run["module_source_sha256"] = content_hash(module)
    write_json(source / "run.json", run)
    params = parameters(source, dataset)
    with pytest.raises(ValueError, match="capability"):
        prepare_run(source, tmp_path / "unsupported", params)
    assert predict_run(source, tmp_path / "numeric", {"batch": [0.1], "trusted_module": True})[
        "prediction"
    ]["values"]


def test_shared_deadline_and_clock_regression_never_publish(raw_case, tmp_path):
    source, _, params, _, _ = raw_case
    now = int(time.time() * 1000)
    params["request"]["deadline_ms"] = now + 10
    ticks = iter([now, now + 1, now + 2, now + 11])
    with pytest.raises(ValueError, match="deadline"):
        prepare_run(source, tmp_path / "expired", params, clock_ms=lambda: next(ticks))
    assert not (tmp_path / "expired/prepared.json").exists()
    ticks = iter([now + 2, now + 3, now + 1])
    with pytest.raises(ValueError, match="clock"):
        prepare_run(source, tmp_path / "regressed", params, clock_ms=lambda: next(ticks))
    assert not (tmp_path / "regressed/prepared.json").exists()


def test_raw_preparation_failure_during_cleanup_never_publishes(raw_case, tmp_path, monkeypatch):
    from omega.research.prediction import PredictionSession

    source, _, params, _, _ = raw_case
    original = PredictionSession.close

    def failing_close(self):
        original(self)
        raise RuntimeError("fixture close failed")

    monkeypatch.setattr(PredictionSession, "close", failing_close)
    with pytest.raises(RuntimeError, match="close failed"):
        prepare_run(source, tmp_path / "cleanup", params)
    assert not (tmp_path / "cleanup/prepared.json").exists()


def test_raw_module_cannot_mutate_the_frozen_request(raw_case, tmp_path, monkeypatch):
    from omega.research.prediction import PredictionSession

    source, _, params, _, _ = raw_case
    original = PredictionSession.__init__

    def replace_hook(self, *args, **kwargs):
        original(self, *args, **kwargs)
        hook = self.instance.prepare_inference

        def modifying(rows, request, context):
            result = hook(rows, request, context)
            request["sequence"] = 999
            return result

        self.instance.prepare_inference = modifying

    monkeypatch.setattr(PredictionSession, "__init__", replace_hook)
    result = prepare_run(source, tmp_path / "copied-request", params)
    assert result["request"] == params["request"] and result["request"]["sequence"] == 1


def test_prepared_reader_rejects_parent_symlink(raw_case, tmp_path):
    _, dataset, _, _, _ = raw_case
    link = tmp_path / "linked"
    try:
        link.symlink_to(dataset, target_is_directory=True)
    except OSError:
        pytest.skip("Windows symlink creation unavailable")
    with pytest.raises(ValueError, match="symlinks"):
        manifest_identity(link / "dataset.json")


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO validation")
def test_prepared_reader_fifo_does_not_block(raw_case, tmp_path):
    _, _, params, _, _ = raw_case
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match="regular"):
        read_prepared(fifo, params["request"])


def test_actual_demo_children_raw_prepare_then_existing_cpu_shadow(dataset, tmp_path, monkeypatch):
    parent_torch = "torch" in sys.modules
    service = ResearchService(tmp_path / "research")
    try:
        trained = completed(
            service,
            service.submit(
                "train", {"dataset": str(dataset), "demo": True, "config": {"epochs": 1}}
            ),
        )
        source = Path(trained["result_file"]).parent
        contract = ShadowModelContract("omega.ohlcv.1m.v1", ("volume",), 1, (None,))
        candidate = prepare_candidate(
            source, tmp_path / "candidate", contract, fixture_batch=[0], trusted_module=True
        )
        fixture = completed(service, service.submit("nn_predict", candidate.parameters()))
        verify_fixture(candidate, fixture, research_root=service.root)
        controller = ShadowDeployment("core-raw")
        controller.activate(candidate, expected_revision=0)
        controller.reset_stream(
            "raw-demo", "binance:usdm:BTCUSDT", expected_revision=controller.revision, reason="test"
        )
        params = parameters(source, dataset, candidate_id=candidate.candidate_id)
        stream = controller.snapshot()["document"]["streams"]["raw-demo|binance:usdm:BTCUSDT"]
        params["request"].update(
            core_epoch=controller.core_epoch,
            deployment_epoch=controller.epoch,
            stream_epoch=stream["epoch"],
            state_hash=stream["state_hash"],
        )
        monkeypatch.setattr(
            "omega.research.neural.load_module",
            lambda *a, **k: pytest.fail("parent imported plugin"),
        )
        monkeypatch.setattr(
            "omega.research.prediction.load_module",
            lambda *a, **k: pytest.fail("parent imported plugin"),
        )
        prepared_job = completed(service, service.submit("nn_prepare", params))
        prepared = read_prepared(Path(prepared_job["result_file"]), params["request"])
        request = controller.request(
            "raw-demo",
            params["request"]["instrument"],
            sequence=1,
            open_time_ms=600000,
            as_of_ms=prepared["as_of_ms"],
            now_ms=int(time.time() * 1000),
            deadline_ms=params["request"]["deadline_ms"],
            input_digest=prepared["prepared_sha256"],
            batch=prepared["batch"],
            schema_id=contract.schema_id,
            features=contract.features,
            lookback=1,
        )
        job = completed(service, service.submit("nn_predict", request["parameters"]))
        controller.bind_job(request["id"], job, research_root=service.root)
        accepted = controller.accept(request["id"], job, now_ms=int(time.time() * 1000))
        assert accepted["scope"] == "paper_shadow_only" and accepted["sequence"] == 1
        assert accepted["input_digest"] == prepared["prepared_sha256"]
        assert ("torch" in sys.modules) == parent_torch
    finally:
        service.close()


def test_actual_pytorch_mlp_child_uses_restored_features_without_future_target(dataset, tmp_path):
    parent_torch = "torch" in sys.modules
    service = ResearchService(tmp_path / "research")
    try:
        trained = completed(
            service,
            service.submit(
                "train",
                {
                    "dataset": str(dataset),
                    "builtin": "pytorch_return",
                    "device": "cpu",
                    "config": {"epochs": 1, "lookback": 3, "hidden_size": 3, "batch_size": 32},
                },
            ),
            timeout=60,
        )
        source = Path(trained["result_file"]).parent
        contract = ShadowModelContract(
            "omega.ohlcv.1m.v1",
            ("close/open-1", "high/open-1", "low/open-1", "log_volume"),
            3,
            (None, 3, 4),
        )
        params = parameters(source, dataset, contract=contract)
        prepared_job = completed(service, service.submit("nn_prepare", params), timeout=60)
        prepared = read_prepared(Path(prepared_job["result_file"]), params["request"])
        scale = max(math.log1p(index + 10) for index in range(42))
        expected = [
            [
                1 / (100 + index),
                3 / (100 + index),
                -2 / (100 + index),
                math.log1p(index + 10) / scale,
            ]
            for index in (8, 9, 10)
        ]
        for actual_row, expected_row in zip(prepared["batch"][0], expected, strict=True):
            assert actual_row == expected_row
        assert [r["open_time_ms"] for r in prepared["window"]] == [480000, 540000, 600000]
        predicted = completed(
            service,
            service.submit(
                "nn_predict",
                {"training_run": str(source), "batch": prepared["batch"], "trusted_module": True},
            ),
        )
        assert predicted["result"]["count"] == 1
        assert ("torch" in sys.modules) == parent_torch
    finally:
        service.close()


@pytest.fixture
def raw_transform_case(tmp_path):
    """Real external Python fixture owns aggregation; no host aggregation formula."""
    source = tmp_path / "synthetic-long.csv"
    with source.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        for index in range(360):
            writer.writerow(
                {
                    "instrument": "binance:usdm:BTCUSDT",
                    "open_time_ms": index * 60000,
                    "open": 100 + index,
                    "high": 103 + index,
                    "low": 98 + index,
                    "close": 101 + index,
                    "volume": 10 + index,
                }
            )
    dataset = tmp_path / "long-dataset"
    manifest = export_dataset(source, dataset, {})
    write_json(dataset / "dataset.json", manifest | {"synthetic": True})

    def make(mode):
        features = ["volume"] if mode == "aggregate" else [f"volume_{i:02}" for i in range(60)]
        lookback = 12 if mode == "aggregate" else 1
        shape = (None, 12, 1) if mode == "aggregate" else (None, 60)
        text = Path(demo_instance.__file__).read_text()
        text = text.replace(
            '"module_id": "omega.demo.linear_neuron"', f'"module_id": "fixture.raw_{mode}"'
        )
        text = text.replace('"module_version": "1.1"', '"module_version": "1.2-fixture"')
        text = text.replace(
            '"fit_scope": "train_only",',
            f'"fit_scope": "train_only", "raw_lookback": 60, "features": {features!r},',
        )
        tree = ast.parse(text)
        cls = next(node for node in tree.body if isinstance(node, ast.ClassDef))
        lines = text.splitlines(keepends=True)
        methods = [
            node
            for node in cls.body
            if isinstance(node, ast.FunctionDef) and node.name in {"predict", "prepare_inference"}
        ]
        for method in sorted(methods, key=lambda node: node.lineno, reverse=True):
            if method.name == "predict":
                replacement = """    def predict(self, batch, state=None):
        def pooled(value):
            return sum(pooled(v) for v in value) if isinstance(value, list) else value
        return {"values": [self.weight * pooled(v) + self.bias for v in batch], "state": state}
"""
            else:
                # These formulas are in the frozen external fixture, never Omega's core.
                batch = (
                    "[[[sum(values[i:i+5])] for i in range(0, 60, 5)]]"
                    if mode == "aggregate"
                    else "[values]"
                )
                replacement = f"""    def prepare_inference(self, rows, request, context):
        if len(rows) != 60:
            raise ValueError("external module requires exactly 60 raw minutes")
        values = [float(row.volume) / self.scale for row in rows]
        return {{"batch": {batch}, "used_rows": list(range(60))}}
"""
            lines[method.lineno - 1 : method.end_lineno] = [replacement]
        plugin = tmp_path / f"external_{mode}.py"
        plugin.write_text("".join(lines))
        training = tmp_path / f"training-{mode}"
        train(dataset, plugin, training, {"config": {"epochs": 1}})
        contract = ShadowModelContract("omega.ohlcv.1m.v1", tuple(features), lookback, shape)
        params = parameters(training, dataset, contract=contract, opening=119 * 60000)
        params["request"]["raw_lookback"] = 60
        return training, params

    return dataset, make


@pytest.mark.parametrize("mode", ["aggregate", "flatten"])
def test_module_owned_raw60_to_prepared12_or_flat_vector(raw_transform_case, tmp_path, mode):
    _, make = raw_transform_case
    source, params = make(mode)
    before = {p.relative_to(source): p.read_bytes() for p in source.rglob("*") if p.is_file()}
    result = prepare_run(source, tmp_path / "prepared-transform", params)
    model = json.loads((source / "model.json").read_text())
    values = [float(10 + i) / model["volume_scale"] for i in range(60, 120)]
    expected = (
        [[[sum(values[i : i + 5])] for i in range(0, 60, 5)]] if mode == "aggregate" else [values]
    )
    assert result["batch"] == expected
    assert len(result["window"]) == 60 and result["used_rows"] == list(range(60))
    assert result["window"][0]["open_time_ms"] == 60 * 60000
    assert result["window"][-1]["open_time_ms"] == 119 * 60000
    assert result["request"]["contract"]["lookback"] == (12 if mode == "aggregate" else 1)
    assert result["request"]["raw_lookback"] == 60 and result["lineage"]["synthetic"] is True
    prediction = predict_run(
        source,
        tmp_path / "transformed-prediction",
        {"batch": result["batch"], "trusted_module": True},
    )
    assert len(prediction["prediction"]["values"]) == 1
    assert read_prepared(tmp_path / "prepared-transform/prepared.json", params["request"]) == result
    assert all((source / name).read_bytes() == value for name, value in before.items())


@pytest.mark.parametrize("value", [None, 59, True, 0, 4097])
def test_declared_raw_window_cannot_be_changed_by_direct_client(
    raw_transform_case, tmp_path, monkeypatch, value
):
    _, make = raw_transform_case
    source, params = make("aggregate")
    if value is None:
        params["request"].pop("raw_lookback")
    else:
        params["request"]["raw_lookback"] = value
    monkeypatch.setattr(
        "omega.research.prediction.PredictionSession",
        lambda *a, **k: pytest.fail("wrong rawN reached module hook"),
    )
    with pytest.raises(ValueError, match="raw_lookback"):
        prepare_run(source, tmp_path / "wrong-window", params)
    assert not (tmp_path / "wrong-window").exists()


def test_raw_window_split_and_gap_use_raw_minutes_not_prepared_axis(raw_transform_case):
    dataset, make = raw_transform_case
    _, params = make("aggregate")
    crossing = params["request"] | {"open_time_ms": 270 * 60000}
    with pytest.raises(ValueError, match="one frozen dataset split"):
        load_window(dataset, crossing)
    lines = (dataset / "rows.csv").read_text().splitlines()
    lines = [line for line in lines if not line.startswith("binance:usdm:BTCUSDT,4200000,")]
    (dataset / "rows.csv").write_text("\n".join(lines) + "\n")
    manifest = json.loads((dataset / "dataset.json").read_text())
    manifest["rows"].update(count=359, sha256=content_hash(dataset / "rows.csv"))
    write_json(dataset / "dataset.json", manifest)
    params["request"]["dataset_sha256"] = manifest_identity(dataset)
    with pytest.raises(ValueError, match="gapped"):
        load_window(dataset, params["request"])


def test_legacy_no_raw_declaration_keeps_request_artifact_bytes_and_hash(raw_case):
    _, _, params, destination, result = raw_case
    original_bytes = (destination / "prepared.json").read_bytes()
    before = identity(params["request"]), identity(result), result["prepared_sha256"]
    parsed_request = parse_request(params["request"])
    parsed = read_prepared(destination / "prepared.json", parsed_request)
    assert "raw_lookback" not in parsed_request and "raw_lookback" not in parsed["request"]
    assert (identity(parsed_request), identity(parsed), parsed["prepared_sha256"]) == before
    assert (destination / "prepared.json").read_bytes() == original_bytes


@pytest.mark.parametrize("value", [True, 0, 4097, 60.0, None])
def test_descriptor_raw_window_is_native_bounded_integer(value):
    descriptor = demo_instance.describe()
    descriptor["input"]["raw_lookback"] = value
    module = SimpleNamespace(describe=lambda: descriptor, create=lambda *a: None)
    with pytest.raises(ValueError, match="raw_lookback"):
        inspect_module(module)
