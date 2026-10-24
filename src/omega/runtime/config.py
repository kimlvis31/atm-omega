"""Read-only bootstrap configuration. Loading never creates directories or starts services."""

import math
import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from omega.domain.errors import ValidationError

SAFETY_POLICY = {
    "on_live_anomaly": "block_new_risk_keep_protection_and_recovery",
    "after_core_restart": "reconcile_then_manual_resume",
    "after_storage_recovery": "reconcile_then_manual_resume",
    "after_model_recovery": "restore_validate_then_manual_resume",
    "after_execution_recovery": "reconcile_then_manual_resume",
    "on_all_recording_failure": "block_new_risk_manage_existing_emit_critical",
}
PATH_DEFAULTS = {
    "data_dir": "./data",
    "artifact_dir": "./artifacts",
    "runtime_settings_store": "./state/settings.json",
    "bootstrap_log_dir": "./logs/bootstrap",
}
TABLE_KEYS = {
    "gui": {"autostart", "single_instance"},
    "runtime": {"start_method", "ready_timeout_s", "shutdown_timeout_s", "health_interval_s"},
    "resources": {"profile", "lazy_batch_workers", "lazy_gpu_workers"},
    "safety": set(SAFETY_POLICY),
    "paths": set(PATH_DEFAULTS),
}


@dataclass(frozen=True)
class MasterConfig:
    instance_id: str
    gui_autostart: bool
    single_gui_instance: bool
    ready_timeout_s: float
    shutdown_timeout_s: float
    health_interval_s: float
    resource_profile: str
    lazy_batch_workers: bool
    lazy_gpu_workers: bool
    paths: Mapping[str, Path]


def load_master(root: Path, path: Path | None = None) -> MasterConfig:
    root = Path(root).resolve()
    selected = Path(path) if path is not None else root / "master.config.toml"
    if not selected.is_absolute():
        selected = root / selected
    try:
        if path is None and not selected.exists():
            data = {}
        else:
            with selected.open("rb") as stream:
                raw = stream.read(65_537)
            if len(raw) > 65_536:
                raise ValidationError("master configuration exceeds size limit")
            data = tomllib.loads(raw.decode("utf-8"))
    except (OSError, ValueError, UnicodeError):
        raise ValidationError("master configuration cannot be read or parsed") from None
    if set(data) - {"schema_version", "instance_id", *TABLE_KEYS}:
        raise ValidationError("unknown bootstrap configuration key")
    if type(data.get("schema_version", 1)) is not int or data.get("schema_version", 1) != 1:
        raise ValidationError("unsupported bootstrap schema version")
    for name, allowed in TABLE_KEYS.items():
        table = data.get(name, {})
        if not isinstance(table, dict) or set(table) - allowed:
            raise ValidationError(f"invalid bootstrap table: {name}")
    instance = data.get("instance_id", "omega-main")
    if not isinstance(instance, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", instance):
        raise ValidationError("invalid instance identity")
    gui, runtime, resources = (data.get(name, {}) for name in ("gui", "runtime", "resources"))
    if runtime.get("start_method", "spawn") != "spawn":
        raise ValidationError("spawn is the supported process start method")
    for key, expected in SAFETY_POLICY.items():
        if data.get("safety", {}).get(key, expected) != expected:
            raise ValidationError("unsupported safety policy override")
    profile = resources.get("profile", "auto")
    if profile not in {"auto", "lightweight"}:
        raise ValidationError("unsupported resource profile")
    paths = {}
    for key, default in PATH_DEFAULTS.items():
        value = data.get("paths", {}).get(key, default)
        if not isinstance(value, str) or not value:
            raise ValidationError("invalid configured path")
        candidate = Path(value)
        paths[key] = (candidate if candidate.is_absolute() else root / candidate).resolve()
    return MasterConfig(
        instance,
        _boolean(gui, "autostart", False),
        _boolean(gui, "single_instance", True),
        _seconds(runtime, "ready_timeout_s", 30),
        _seconds(runtime, "shutdown_timeout_s", 30),
        _seconds(runtime, "health_interval_s", 5),
        profile,
        _boolean(resources, "lazy_batch_workers", True),
        _boolean(resources, "lazy_gpu_workers", True),
        MappingProxyType(paths),
    )


def _boolean(table: dict, key: str, default: bool) -> bool:
    value = table.get(key, default)
    if type(value) is not bool:
        raise ValidationError(f"{key} must be a boolean")
    return value


def _seconds(table: dict, key: str, default: float) -> float:
    value = table.get(key, default)
    if type(value) not in {int, float} or not math.isfinite(value) or not 0 < value <= 3600:
        raise ValidationError(f"{key} must be a bounded positive duration")
    return float(value)
