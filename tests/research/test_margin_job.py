"""Composed replay handlers execute in real isolated research child processes."""

import json
import shutil
import time
from pathlib import Path

import pytest

from omega.domain.errors import ValidationError
from omega.research.artifacts import content_hash, write_json
from omega.research.datasets import export_dataset, load_dataset
from omega.research.result_summary import summarize_simulation
from omega.research.service import ResearchService
from omega.runtime import margin_research
from omega.runtime.margin_research import run_margin_simulation_job


def completed(service, job):
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        value = service.status(job["id"])
        if value["state"] in {"complete", "failed", "cancelled", "interrupted", "unknown"}:
            return value
        time.sleep(0.02)
    raise AssertionError(f"composed research child did not finish: {value}")


def parameters(dataset):
    instruments = ["binance:usdm:BTCUSDT", "binance:usdm:ETHUSDT"]
    return {
        "dataset": str(dataset),
        "start_ms": 0,
        "spec": {"fee_rate": "0.001"},
        "account": {
            "account_id": "job-parity",
            "balance": "1000",
            "positions": [
                {
                    "instrument": instrument,
                    "owner_id": str(index),
                    "margin_mode": mode,
                    "leverage": 10,
                    "budget_margin": "100",
                    "isolated_collateral": collateral,
                }
                for index, (instrument, mode, collateral) in enumerate(
                    zip(instruments, ("cross", "isolated"), ("0", "100"), strict=True)
                )
            ],
        },
        "strategies": [
            {
                "id": str(index),
                "instrument": instrument,
                "module": "target_quantity",
                "configuration": {"quantity": quantity},
                "analysis": {},
                "constraints": {"leverage": "10", "can_short": True},
            }
            for index, (instrument, quantity) in enumerate(
                zip(instruments, ("1", "-1"), strict=True)
            )
        ],
        "brackets": [
            {
                "instrument": instrument,
                "observed_at_ms": 0,
                "available_at_ms": 0,
                "rows": [
                    {
                        "notional_floor": "0",
                        "notional_cap": "1000000",
                        "maintenance_rate": ".01",
                        "maintenance_amount": "0",
                        "max_leverage": 20,
                    }
                ],
            }
            for instrument in instruments
        ],
    }


def hostile_handler(partial, final, parameters, cancel):
    write_json(partial / "value.json", {"state": "complete"})
    mode = parameters["mode"]
    if mode == "unknown_envelope":
        return {"result_file": "value.json", "summary": {}, "state": "unknown"}
    relative = {
        "outside": "../value.json",
        "absolute_inside": str(partial / "value.json"),
        "missing": "missing.json",
        "good": "value.json",
    }[mode]
    return {"result_file": relative, "summary": {"mode": mode}}


def test_actual_spawn_margin_job_publishes_financial_and_source_artifacts(dataset, tmp_path):
    service = ResearchService(
        tmp_path / "research", handlers={"simulate_margin": run_margin_simulation_job}
    )
    try:
        result = completed(service, service.submit("simulate_margin", parameters(dataset)))
        assert result["state"] == "complete", result
        assert result["result"]["scope"] == "virtual_margin_v2_ohlcv_proxy"
        artifact = Path(result["artifact"])
        replay = json.loads(Path(result["result_file"]).read_text())
        index = json.loads((artifact / "artifact.json").read_text())
        assert replay["state"] == "complete" and replay["scope"] == result["result"]["scope"]
        assert replay["positions"] == {"binance:usdm:BTCUSDT": "1", "binance:usdm:ETHUSDT": "-1"}
        assert len(replay["fills"]) == replay["input_manifest"]["rows"]["count"] // 60 == 2
        assert replay["input_manifest"]["rows"]["sha256"] == content_hash(
            artifact / "input.rows.csv"
        )
        assert replay["lineage"]["frames"] == len(replay["equity"]) == 60
        copied_manifest, copied_rows = load_dataset(artifact / replay["input_dataset_file"])
        assert copied_manifest == replay["input_manifest"] and len(copied_rows) == 120
        copied_root = (artifact / replay["input_dataset_file"]).parent
        assert (
            content_hash(copied_root / copied_manifest["source"]["file"])
            == copied_manifest["source"]["sha256"]
        )
        implementation = replay["implementation"]
        assert implementation["scope"] == "all_installed_builtin_ts_and_execution_host"
        assert implementation["selected_modules"] == ["target_quantity"]
        assert "target_quantity" in implementation["builtin_modules"]
        assert implementation["python_version"] and implementation["omega_version"]
        implementation_names = {item["file"] for item in implementation["files"]}
        assert "code-snapshot/strategies/target_quantity/module.py" in implementation_names
        assert "code-snapshot/domain/margin.py" in implementation_names
        assert "code-snapshot/runtime/margin_session_features.py" in implementation_names
        assert "code-snapshot/strategies/registry.py" in implementation_names
        assert all(
            content_hash(artifact / item["file"]) == item["sha256"]
            for item in implementation["files"]
        )
        summary = summarize_simulation(replay)
        assert summary["reconciliation"]["matches"] is True
        assert summary["metrics"]["initial_wallet"] == "1000"
        assert replay["noncausal_input"] is False
        assert (artifact / "margin-session/margin-replay.json").is_file()
        assert any("margin-sessions" in item["file"] for item in index["files"])
        assert all(
            content_hash(artifact / item["file"]) == item["sha256"] for item in index["files"]
        )
        assert not list((tmp_path / "research/artifacts").glob(".partial-*"))
    finally:
        service.close()


@pytest.mark.parametrize(
    "change",
    [
        "session_helper",
        "ledger_helper",
        "cleanup_helper",
        "lease_helper",
        "settings_helper",
        "analysis_helper",
        "new_analysis_source",
        "unselected_family",
        "new_source",
    ],
)
def test_actual_copied_direct_dependency_change_cannot_escape_implementation_guard(
    tmp_path, monkeypatch, change
):
    original_root = Path(margin_research.__file__).resolve().parents[1]
    package = tmp_path / "isolated-source-package"
    for name in ("strategies", "analysis", "domain", "ports"):
        shutil.copytree(
            original_root / name,
            package / name,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
    for name in margin_research._HOST_SOURCES:
        copied = package / name
        copied.parent.mkdir(parents=True, exist_ok=True)
        copied.write_bytes((original_root / name).read_bytes())
    monkeypatch.setattr(margin_research, "__file__", str(package / "runtime/margin_research.py"))
    partial = tmp_path / "artifact"
    partial.mkdir()
    # Select TEF: target_quantity remains a frozen builtin without a core family branch.
    implementation = margin_research._snapshot_implementation(partial, [{"module": "tef"}])
    names = {row["file"] for row in implementation["files"]}
    assert implementation["selected_modules"] == ["tef"]
    assert "code-snapshot/strategies/neural_signal/module.py" in names
    assert "code-snapshot/strategies/target_quantity/module.py" in names
    assert "code-snapshot/runtime/margin_session_features.py" in names
    assert "code-snapshot/runtime/margin_ledger.py" in names
    assert "code-snapshot/runtime/margin_cleanup.py" in names
    assert "code-snapshot/runtime/control.py" in names
    assert "code-snapshot/analysis/temporal_probability.py" in names
    assert "code-snapshot/domain/external_features.py" in names
    margin_research._check_implementation(partial, implementation)
    target = (
        package
        / {
            "session_helper": "runtime/margin_session_features.py",
            "ledger_helper": "runtime/margin_ledger.py",
            "cleanup_helper": "runtime/margin_cleanup.py",
            "lease_helper": "runtime/control.py",
            "settings_helper": "runtime/settings.py",
            "analysis_helper": "analysis/temporal_probability.py",
            "new_analysis_source": "analysis/new_direct_helper.py",
            "unselected_family": "strategies/target_quantity/module.py",
            "new_source": "strategies/new_direct_helper.py",
        }[change]
    )
    previous = target.read_bytes() if target.exists() else b""
    target.write_bytes(previous + b"\n# isolated source replacement after snapshot\n")
    with pytest.raises(ValidationError, match="changed"):
        margin_research._check_implementation(partial, implementation)
    assert all(
        content_hash(partial / row["file"]) == row["sha256"] for row in implementation["files"]
    )


@pytest.mark.parametrize("kind", ["future_bracket", "unsupported_policy"])
def test_unavailable_reference_or_policy_never_publishes_success(dataset, tmp_path, kind):
    service = ResearchService(
        tmp_path / "research", handlers={"simulate_margin": run_margin_simulation_job}
    )
    try:
        request = parameters(dataset)
        if kind == "future_bracket":
            request["brackets"][0]["available_at_ms"] = 60001
            request["brackets"][0]["observed_at_ms"] = 60001
        else:
            request["spec"]["slippage_rate"] = "0"
        result = completed(service, service.submit("simulate_margin", request))
        assert result["state"] == "failed", result
        assert "artifact" not in result and "result_file" not in result
        assert not (tmp_path / "research/artifacts" / result["id"]).exists()
    finally:
        service.close()


@pytest.mark.parametrize("mode", ["outside", "absolute_inside", "missing", "unknown_envelope"])
def test_composed_handler_cannot_publish_bad_result_reference_or_unknown_envelope(tmp_path, mode):
    service = ResearchService(tmp_path / "research", handlers={"guard_probe": hostile_handler})
    try:
        result = completed(service, service.submit("guard_probe", {"mode": mode}))
        assert result["state"] == "failed", result
        assert "artifact" not in result
        assert not (tmp_path / "research/artifacts" / result["id"]).exists()
        following = completed(service, service.submit("guard_probe", {"mode": "good"}))
        assert following["state"] == "complete", following
        assert Path(following["result_file"]).is_file()
    finally:
        service.close()


def test_noncausal_repair_needs_consent_and_keeps_visible_warning(gapped_source, tmp_path):
    export_dataset(gapped_source, tmp_path / "repaired", {"interpolation": {"enabled": True}})
    original_source = gapped_source.read_bytes()
    request = parameters(tmp_path / "repaired/interpolated")
    service = ResearchService(
        tmp_path / "research", handlers={"simulate_margin": run_margin_simulation_job}
    )
    try:
        refused = completed(service, service.submit("simulate_margin", request))
        assert refused["state"] == "failed" and "noncausal" in refused["error"]
        request["allow_noncausal_research_input"] = True
        accepted = completed(service, service.submit("simulate_margin", request))
        assert accepted["state"] == "complete", accepted
        result = json.loads(Path(accepted["result_file"]).read_text())
        assert result["input_manifest"]["repair"]["causal"] is False
        assert result["input_manifest"]["quality"]["interpolated_rows"] == 1
        assert result["noncausal_input"] is True
        assert any("noncausal" in value.casefold() for value in result["assumptions"])
        copied_manifest, copied_rows = load_dataset(
            Path(accepted["artifact"]) / result["input_dataset_file"]
        )
        assert copied_manifest == result["input_manifest"] and len(copied_rows) == 120
        assert gapped_source.read_bytes() == original_source
    finally:
        service.close()
