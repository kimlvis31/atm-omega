"""Validated runtime preferences with one atomic file authority."""

# Imports
import copy
import json
import math
import os
import tempfile
from pathlib import Path
from threading import RLock

from omega.domain.errors import ValidationError
from omega.infrastructure.atomic_files import replace_atomic
from omega.runtime.mobile_origin import canonical_mobile_origin

DEFAULT_SETTINGS = {
    "schema_version": 1,
    "revision": 0,
    "logging": {"level": "INFO", "retention_days": 30, "max_file_bytes": 5_000_000},
    "notifications": {
        "sound": True,
        "repeat_critical_s": 30,
        "speech": False,
        "speech_language": "auto",
        "speech_voice": "",
        "speech_rate": 0.0,
        "speech_volume": 0.8,
    },
    "training": {"recording": "standard", "optional_recording_failure": "continue"},
    "simulation": {"backend": "auto", "intrabar": "interpolation", "seed": 0},
    "execution": {"mode": "static", "max_age_ms": 60000, "apply_to_manual": False},
    "analysis": {"input_storage_mb": 1024},
    "mobile": {"https_origin": ""},
}


def atomic_json(path: Path, data: dict) -> None:
    """Replace only after fsync; failed writes never truncate the previous state."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, allow_nan=False, sort_keys=True).encode("utf-8")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        replace_atomic(temporary, path)
        if os.name != "nt":
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


class SettingsStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.lock = RLock()
        self.data = copy.deepcopy(DEFAULT_SETTINGS)
        if self.path.exists():
            if self.path.stat().st_size > 65_536:
                raise ValidationError("settings file exceeds limit")
            candidate = json.loads(self.path.read_text(encoding="utf-8"))
            # Preserve both exact historic execution/analysis migrations, with
            # or without the newer independent mobile category. Partial pairs
            # and unknown/malformed categories still fail validation below.
            previous_categories = set(DEFAULT_SETTINGS) - {"mobile"}
            without_execution = set(DEFAULT_SETTINGS) - {"execution", "analysis"}
            if isinstance(candidate, dict) and set(candidate) in (
                without_execution,
                without_execution - {"mobile"},
            ):
                for category in ("execution", "analysis"):
                    candidate[category] = copy.deepcopy(DEFAULT_SETTINGS[category])
            if isinstance(candidate, dict) and set(candidate) == previous_categories:
                candidate["mobile"] = copy.deepcopy(DEFAULT_SETTINGS["mobile"])
            # Earlier v1 files had exactly these three notification fields. Add only
            # the newly introduced preferences, never repair a malformed old schema.
            notify = candidate.get("notifications") if isinstance(candidate, dict) else None
            if isinstance(notify, dict) and set(notify) == {"sound", "speech", "repeat_critical_s"}:
                candidate["notifications"] = {**DEFAULT_SETTINGS["notifications"], **notify}
            self._validate(candidate)
            self.data = candidate

    def snapshot(self) -> dict:
        with self.lock:
            return copy.deepcopy(self.data)

    def update(self, changes: dict, *, revision: int) -> dict:
        with self.lock:
            if type(revision) is not int or revision != self.data["revision"]:
                raise ValidationError("settings revision conflict")
            if not isinstance(changes, dict) or set(changes) - (
                set(DEFAULT_SETTINGS) - {"schema_version", "revision"}
            ):
                raise ValidationError("unknown settings category")
            candidate = copy.deepcopy(self.data)
            for category, values in changes.items():
                if not isinstance(values, dict) or set(values) - set(candidate[category]):
                    raise ValidationError("unknown settings parameter")
                candidate[category].update(values)
            candidate["revision"] += 1
            self._validate(candidate)
            atomic_json(self.path, candidate)
            self.data = candidate
            return self.snapshot()

    @staticmethod
    def _validate(data: dict) -> None:
        if not isinstance(data, dict) or set(data) != set(DEFAULT_SETTINGS):
            raise ValidationError("invalid settings schema")
        if (
            type(data["schema_version"]) is not int
            or data["schema_version"] != 1
            or type(data["revision"]) is not int
            or data["revision"] < 0
        ):
            raise ValidationError("invalid settings version")
        for category in (
            "logging",
            "notifications",
            "training",
            "simulation",
            "execution",
            "analysis",
            "mobile",
        ):
            if not isinstance(data[category], dict) or set(data[category]) != set(
                DEFAULT_SETTINGS[category]
            ):
                raise ValidationError("invalid settings category")
        canonical_mobile_origin(data["mobile"]["https_origin"])
        log, notify, train, simulation = (
            data[key] for key in ("logging", "notifications", "training", "simulation")
        )
        if log["level"] not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValidationError("invalid log level")
        for value, lower, upper in (
            (log["retention_days"], 1, 3650),
            (log["max_file_bytes"], 4096, 100_000_000),
            (notify["repeat_critical_s"], 5, 3600),
        ):
            if type(value) is not int or not lower <= value <= upper:
                raise ValidationError("setting outside supported range")
        if type(notify["sound"]) is not bool or type(notify["speech"]) is not bool:
            raise ValidationError("invalid notification preference")
        if notify["speech_language"] not in {"auto", "ko_KR", "en_US"}:
            raise ValidationError("unsupported speech language")
        if (
            not isinstance(notify["speech_voice"], str)
            or len(notify["speech_voice"]) > 256
            or any(ord(c) < 32 for c in notify["speech_voice"])
        ):
            raise ValidationError("invalid speech voice identity")
        for key, minimum, maximum in (("speech_rate", -1, 1), ("speech_volume", 0, 1)):
            value = notify[key]
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or not minimum <= value <= maximum
            ):
                raise ValidationError("speech setting outside supported range")
        if train["recording"] not in {"light", "standard", "detailed"} or train[
            "optional_recording_failure"
        ] not in {"continue", "stop"}:
            raise ValidationError("invalid training recording policy")
        if simulation["backend"] not in {"auto", "cpu", "gpu"} or simulation["intrabar"] not in {
            "conservative",
            "interpolation",
            "random",
        }:
            raise ValidationError("invalid simulation preference")
        if type(simulation["seed"]) is not int or not 0 <= simulation["seed"] < 2**63:
            raise ValidationError("invalid simulation seed")
        execution, analysis = data["execution"], data["analysis"]
        if (
            type(execution["mode"]) is not str
            or execution["mode"] not in {"static", "strict"}
            or type(execution["max_age_ms"]) is not int
            or not 1 <= execution["max_age_ms"] <= 86400000
            or type(execution["apply_to_manual"]) is not bool
        ):
            raise ValidationError("invalid execution rules policy")
        if (
            type(analysis["input_storage_mb"]) is not int
            or not 16 <= analysis["input_storage_mb"] <= 16384
        ):
            raise ValidationError("invalid analysis input storage quota")
