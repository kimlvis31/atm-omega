"""Offline job composition for the shared virtual-margin replay engine."""

import json
import sys
from hashlib import sha256
from importlib import metadata
from pathlib import Path

from omega.application.margin_funding import POLICY_SHA256, FundingSchedule
from omega.application.native_metric_inputs import MetricInputPolicy
from omega.domain.errors import ValidationError
from omega.research.artifacts import (
    bounded_artifact_hash,
    child_path,
    copy_bounded_artifact,
    read_status_json,
    write_json,
)
from omega.research.auxiliary_datasets import (
    SCHEMA_ID,
    SCHEMA_ID_V2,
    load_auxiliary_dataset,
    snapshot_auxiliary_dataset,
)
from omega.research.datasets import load_dataset, write_rows
from omega.research.margin_observation_inputs import NativeObservationReplay
from omega.research.margin_simulation import MarginSimulationSpec, simulate_margin
from omega.runtime.margin_console import scenario_brackets
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration
from omega.strategies.registry import StrategyRegistry

_HOST_SOURCES = (
    "domain/margin.py",
    "domain/trading.py",
    "domain/protection.py",
    "domain/external_features.py",
    "domain/inference.py",
    "application/trading.py",
    "application/margin_account.py",
    "application/margin_risk.py",
    "application/margin_execution.py",
    "application/margin_cost.py",
    "application/margin_funding.py",
    "application/virtual_margin.py",
    "application/margin_strategy.py",
    "application/margin_strategy_costs.py",
    "runtime/margin_session.py",
    "runtime/margin_session_features.py",
    "runtime/margin_session_costs.py",
    "runtime/margin_session_funding.py",
    "runtime/margin_session_shutdown.py",
    "runtime/margin_store.py",
    "runtime/margin_ledger.py",
    "runtime/margin_cleanup.py",
    "runtime/margin_retirement.py",
    "runtime/control.py",
    "runtime/settings.py",
    "runtime/margin_strategy_store.py",
    "runtime/virtual_margin_journal.py",
    "runtime/margin_research.py",
    "research/margin_simulation.py",
    "research/result_summary.py",
    "research/artifacts.py",
    "research/datasets.py",
    "research/auxiliary_datasets.py",
    "research/margin_observation_inputs.py",
    "application/market_frame_builder.py",
    "application/native_metric_inputs.py",
    "application/auxiliary_codec.py",
    "runtime/margin_session_inputs.py",
    "runtime/margin_metric_inputs.py",
    "research/auxiliary_bundle_v2.py",
)
_PURE_PACKAGES = ("strategies", "analysis", "domain", "ports")


def _builtin_sources(root):
    # Freeze calculation providers and their data contracts too: a strategy may
    # reuse any installed analysis family without a hand-maintained import list.
    return {
        path
        for package in _PURE_PACKAGES
        for path in (root / package).rglob("*")
        if path.is_file() and path.suffix in {".py", ".json"}
    }


def _snapshot_inputs(source, destination):
    manifest_path = source / "dataset.json" if source.is_dir() else source
    manifest = read_status_json(manifest_path)
    entries = [manifest["rows"], manifest["source"]]
    if manifest.get("repair"):
        entries.append(manifest["repair"]["mask"])
    destination.mkdir(exist_ok=False)
    names = set()
    for entry in entries:
        name = entry["file"]
        if name in names:
            continue
        names.add(name)
        target = child_path(destination, name)
        target.parent.mkdir(parents=True, exist_ok=True)
        actual = copy_bounded_artifact(
            child_path(manifest_path.parent, name), target, max_bytes=32 * 1024 * 1024
        )
        if actual != entry["sha256"]:
            raise ValidationError("research input changed while taking its snapshot")
    write_json(destination / "dataset.json", manifest)
    return load_dataset(destination)


def _snapshot_funding(value, destination):
    """Preserve received JSON values, not whitespace bytes of an external user file."""
    raw = json.loads(json.dumps(value, allow_nan=False))
    schedule = FundingSchedule.parse(raw)
    destination.mkdir(exist_ok=False)
    source, normalized = destination / "source.json", destination / "schedule.json"
    write_json(source, raw)
    write_json(normalized, schedule.document())
    restored = read_status_json(source)
    if restored != raw or FundingSchedule.parse(read_status_json(normalized)) != schedule:
        raise ValidationError("funding request changed while snapshotting")
    return restored, {
        "scope": "request_json_snapshot",
        "source_file": "input-funding/source.json",
        "source_file_sha256": bounded_artifact_hash(source, max_bytes=2_097_152),
        "normalized_file": "input-funding/schedule.json",
        "normalized_file_sha256": bounded_artifact_hash(normalized, max_bytes=2_097_152),
        "schedule_sha256": schedule.sha256,
        "policy_sha256": POLICY_SHA256,
    }


def _check_funding_snapshot(partial, source):
    for file_key, hash_key in (
        ("source_file", "source_file_sha256"),
        ("normalized_file", "normalized_file_sha256"),
    ):
        if (
            bounded_artifact_hash(child_path(partial, source[file_key]), max_bytes=2_097_152)
            != source[hash_key]
        ):
            raise ValidationError("funding request snapshot changed during replay")


def _snapshot_cost_policy(value, destination):
    """Freeze received policy JSON separately from its canonical identity."""
    from omega.application.margin_cost import MarginCostPolicy

    raw = json.loads(json.dumps(value, allow_nan=False))
    policy = MarginCostPolicy.parse(raw)
    destination.mkdir(exist_ok=False)
    source, normalized = destination / "source.json", destination / "policy.json"
    write_json(source, raw)
    write_json(normalized, policy.document())
    restored = read_status_json(source)
    if restored != raw or MarginCostPolicy.parse(read_status_json(normalized)) != policy:
        raise ValidationError("cost policy request changed while snapshotting")
    return restored, {
        "scope": "request_json_snapshot",
        "source_file": "input-cost-policy/source.json",
        "source_file_sha256": bounded_artifact_hash(source, max_bytes=2_097_152),
        "normalized_file": "input-cost-policy/policy.json",
        "normalized_file_sha256": bounded_artifact_hash(normalized, max_bytes=2_097_152),
        "request_sha256": sha256(
            json.dumps(raw, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "policy_sha256": policy.sha256,
    }


def _check_cost_policy_snapshot(partial, source):
    for file_key, hash_key in (
        ("source_file", "source_file_sha256"),
        ("normalized_file", "normalized_file_sha256"),
    ):
        if (
            bounded_artifact_hash(child_path(partial, source[file_key]), max_bytes=2_097_152)
            != source[hash_key]
        ):
            raise ValidationError("cost policy request snapshot changed during replay")


def _snapshot_implementation(partial, strategies):
    if type(strategies) is not list or not 1 <= len(strategies) <= 100:
        raise ValidationError("shared replay needs a bounded strategy list")
    selected = {row["module"] for row in strategies}
    available = {
        row["id"] for row in StrategyRegistry().list_modules() if row["origin"] == "builtin"
    }
    if not selected <= available:
        raise ValidationError("shared replay currently accepts installed builtin TS modules only")
    root = Path(__file__).resolve().parents[1]
    sources = _builtin_sources(root) | {root / name for name in _HOST_SOURCES}
    records = []
    for source in sorted(sources):
        relative = source.relative_to(root).as_posix()
        destination = partial / "code-snapshot" / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        checksum = copy_bounded_artifact(source, destination, max_bytes=1024 * 1024)
        records.append({"file": "code-snapshot/" + relative, "sha256": checksum})
    return {
        "scope": "all_installed_builtin_ts_and_execution_host",
        "selected_modules": sorted(selected),
        "builtin_modules": sorted(available),
        "files": records,
        "python_version": sys.version.split()[0],
        "omega_version": metadata.version("atm-omega"),
        "limitations": (
            "All installed builtin strategy-package Python/JSON and listed execution-host sources; "
            "not external strategies, other Omega sources, "
            "or the entire Python dependency environment"
        ),
    }


def _check_implementation(partial, implementation):
    root = Path(__file__).resolve().parents[1]
    expected = {
        record["file"].removeprefix("code-snapshot/")
        for record in implementation["files"]
        if record["file"].startswith(
            tuple("code-snapshot/" + name + "/" for name in _PURE_PACKAGES)
        )
    }
    if expected != {path.relative_to(root).as_posix() for path in _builtin_sources(root)}:
        raise ValidationError("installed builtin calculation sources changed during replay")
    for record in implementation["files"]:
        relative = record["file"].removeprefix("code-snapshot/")
        if bounded_artifact_hash(root / relative, max_bytes=1024 * 1024) != record["sha256"]:
            raise ValidationError("shared replay implementation changed during its run")


def run_margin_simulation_job(partial, final, parameters, cancel):
    required = {"dataset", "account", "strategies", "start_ms", "brackets"}
    common_metadata = {"seed", "settings_revision", "timeout_seconds"}
    allowed = (
        required
        | {"spec", "funding", "cost_policy", "allow_noncausal_research_input", "auxiliary_replay"}
        | common_metadata
    )
    if (
        type(parameters) is not dict
        or not required <= parameters.keys()
        or parameters.keys() - allowed
    ):
        raise ValidationError(
            "shared margin replay needs dataset, account, strategies, start and brackets"
        )
    spec = MarginSimulationSpec.parse(parameters.get("spec", {}))
    for field in ("seed", "settings_revision"):
        if field in parameters and (
            type(parameters[field]) is not int or not 0 <= parameters[field] < 2**63
        ):
            raise ValidationError("invalid shared replay job metadata: " + field)
    allow_noncausal = parameters.get("allow_noncausal_research_input", False)
    if type(allow_noncausal) is not bool:
        raise ValidationError("noncausal research consent must be boolean")
    source = Path(parameters["dataset"]).resolve()
    source_manifest_path = source / "dataset.json" if source.is_dir() else source
    source_manifest = read_status_json(source_manifest_path)
    observation_bundle = source_manifest.get("schema_id") in {SCHEMA_ID, SCHEMA_ID_V2}
    auxiliary_policy = parameters.get("auxiliary_replay")
    if "auxiliary_replay" in parameters and (
        not observation_bundle
        or type(auxiliary_policy) is not dict
        or set(auxiliary_policy) != {"mode", "decision_latency_ms"}
        or type(auxiliary_policy["mode"]) is not str
        or auxiliary_policy["mode"] != "native"
        or type(auxiliary_policy["decision_latency_ms"]) is not int
        or not 0 <= auxiliary_policy["decision_latency_ms"] <= 60000
    ):
        raise ValidationError(
            "native auxiliary replay requires an original observation bundle "
            "and bounded native policy"
        )
    if observation_bundle and (
        source_manifest.get("transform") is not None
        or source_manifest.get("dataset_kind")
        != (
            "retained_native_observation_revisions"
            if source_manifest.get("schema_id") == SCHEMA_ID_V2
            else "selected_normalized_observations"
        )
    ):
        raise ValidationError(
            "native margin replay cannot use generated/retrospective observations"
        )
    funding = funding_source = None
    if "funding" in parameters:
        funding, funding_source = _snapshot_funding(
            parameters["funding"], partial / "input-funding"
        )
    cost_policy = cost_source = None
    if "cost_policy" in parameters:
        cost_policy, cost_source = _snapshot_cost_policy(
            parameters["cost_policy"], partial / "input-cost-policy"
        )
    auxiliary_replay = None
    if observation_bundle:
        snapshot_auxiliary_dataset(source, partial / "input-dataset", cancel)
        manifest, rows, observations = load_auxiliary_dataset(partial / "input-dataset", cancel)
        auxiliary_replay = NativeObservationReplay(
            manifest,
            observations,
            decision_latency_ms=0
            if auxiliary_policy is None
            else auxiliary_policy["decision_latency_ms"],
        )
        observation_manifest_sha256 = bounded_artifact_hash(partial / "input-dataset/dataset.json")
        candle_manifest = read_status_json(partial / "input-dataset/candles/dataset.json")
    else:
        manifest, rows = _snapshot_inputs(source, partial / "input-dataset")
        candle_manifest = manifest
    implementation = _snapshot_implementation(partial, parameters["strategies"])
    noncausal = bool(
        candle_manifest.get("repair")
        and not candle_manifest["repair"]["causal"]
        and candle_manifest["quality"]["interpolated_rows"] > 0
    )
    if (
        candle_manifest.get("repair")
        and not candle_manifest["repair"]["causal"]
        and candle_manifest["quality"]["interpolated_rows"] > 0
        and not allow_noncausal
    ):
        raise ValidationError("noncausal repaired input requires explicit offline research consent")
    result = simulate_margin(
        rows,
        spec,
        root=partial / "margin-session",
        account=parameters["account"],
        strategies=parameters["strategies"],
        start_ms=parameters["start_ms"],
        brackets=scenario_brackets(parameters["brackets"]),
        session_factory=MarginTradingSession,
        cancel=cancel,
        **({"funding": funding} if funding_source is not None else {}),
        **({"cost_policy": cost_policy} if cost_source is not None else {}),
        **({"auxiliary_replay": auxiliary_replay} if auxiliary_replay is not None else {}),
        **(
            {"analysis_preflight": _native_analysis_preflight}
            if auxiliary_replay is not None and auxiliary_replay.version == 2
            else {}
        ),
    )
    result["input_manifest"] = manifest
    result["input_dataset_file"] = "input-dataset/dataset.json"
    result["implementation"] = implementation
    if funding_source is not None:
        result["funding"]["source_snapshot"] = funding_source
    if cost_source is not None:
        result["cost_policy"]["source_snapshot"] = cost_source
    result["noncausal_input"] = noncausal
    if noncausal:
        result["assumptions"].append(
            "NONCAUSAL repaired input: offline stress research only; "
            "not historical/live availability evidence"
        )
    _check_implementation(partial, implementation)
    if auxiliary_replay is not None:
        restored_manifest, restored_rows, restored_observations = load_auxiliary_dataset(
            partial / "input-dataset", cancel
        )
        if (
            bounded_artifact_hash(partial / "input-dataset/dataset.json")
            != observation_manifest_sha256
            or restored_manifest != manifest
            or restored_rows != rows
            or restored_observations != observations
        ):
            raise ValidationError("native observation snapshot changed during replay")
    if funding_source is not None:
        _check_funding_snapshot(partial, funding_source)
    if cost_source is not None:
        _check_cost_policy_snapshot(partial, cost_source)
    result["configuration"] = {
        key: parameters[key]
        for key in sorted(parameters)
        if key not in {"dataset"} | common_metadata
    }
    if funding_source is not None:
        result["configuration"]["funding"] = funding
    if cost_source is not None:
        result["configuration"]["cost_policy"] = cost_policy
    result["job_metadata"] = {key: parameters[key] for key in common_metadata if key in parameters}
    result["dataset_file"] = str(Path(parameters["dataset"]).resolve())
    write_rows(partial / "input.rows.csv", rows, cancel)
    write_json(partial / "simulation.json", result)
    response = {
        "result_file": "simulation.json",
        "summary": {
            "fills": len(result["fills"]),
            "wallet": result["wallet"],
            "backend": result["backend"],
            "scope": result["scope"],
            "pending_orders": result["pending_orders"],
        },
    }
    if funding_source is not None:
        response["summary"].update(
            funding_net=result["funding"]["funding_net"],
            funding_settlements=result["funding"]["settlements"],
            funding_schedule_sha256=result["funding"]["schedule_sha256"],
        )
    if cost_source is not None:
        response["summary"].update(cost_policy_sha256=result["cost_policy"]["policy_sha256"])
    if auxiliary_replay is not None:
        response["summary"].update(
            input_scope="native_observation_replay", auxiliary_replay=result["auxiliary_replay"]
        )
    return response


def _native_analysis_preflight(
    account, strategies, start_ms, *, fee_rate, funding=None, cost_policy=None
):
    """Derive the actual public graph policy without creating a financial owner."""
    _initial, _fee, rows, _modules, graphs, _constraints = prepare_margin_configuration(
        account,
        strategies,
        start_ms,
        fee_rate=fee_rate,
        **({"funding": funding} if funding is not None else {}),
        **({"cost_policy": cost_policy} if cost_policy is not None else {}),
    )
    required, policies = {}, {}
    for row in rows:
        graph = graphs[row["id"]]
        channels = tuple(sorted(set(graph.descriptor.required_fields) - {"OHLCV"}))
        if channels:
            required[row["instrument"].key] = channels
        if "metric" in channels:
            policies[row["instrument"].key] = MetricInputPolicy(
                graph.maximum_metric_age_ms
            ).document()
    return {"required_channels": required, "metric_policies": policies}


run_margin_simulation_job.omega_feature_scopes = {
    "analysis_execution": "required_fields_v1",
    "funding": "minute_boundary_scenario_rate_v1",
    "cost_policy": "fixed_adverse_full_fill_delay_v1",
    "auxiliary_replay": "native_trade_flow_available_cutoff_v1",
    "auxiliary_replay_v2": "native_auxiliary_available_cutoff_v2",
}
