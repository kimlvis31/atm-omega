"""Read-only display models shared by Qt views and offline artifact inspection."""

# Imports
import csv
import io
import json
import math
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

# Constants
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_CANDLES = 50_000


@dataclass(frozen=True, slots=True)
class Candle:
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    @classmethod
    def from_mapping(cls, value: dict) -> "Candle":
        timestamp = int(value["open_time_ms"])
        numbers = [float(value[key]) for key in ("open", "high", "low", "close", "volume")]
        if not 0 <= timestamp <= 253_402_300_799_000 or not all(
            math.isfinite(item) for item in numbers
        ):
            raise ValueError("candle contains a non-finite value or unsupported UTC timestamp")
        opening, high, low, close, volume = numbers
        if low > min(opening, close) or high < max(opening, close) or volume < 0:
            raise ValueError("candle OHLC or volume is inconsistent")
        return cls(timestamp, opening, high, low, close, volume)


def parse_candles(values: list[dict]) -> tuple[Candle, ...]:
    if len(values) > MAX_CANDLES:
        raise ValueError(f"display response exceeds {MAX_CANDLES} candles")
    candles = tuple(Candle.from_mapping(value) for value in values)
    if any(a.open_time_ms >= b.open_time_ms for a, b in zip(candles, candles[1:], strict=False)):
        raise ValueError("candles must be sorted with unique timestamps")
    return candles


def reduce_candles(candles: tuple[Candle, ...], nSamples: int) -> tuple[Candle, ...]:
    """Preserve OHLC extremes while limiting rendered geometry; never trading input."""
    if nSamples < 1:
        raise ValueError("nSamples must be positive")
    if len(candles) <= nSamples:
        return candles
    result = []
    for index in range(nSamples):
        start = index * len(candles) // nSamples
        stop = (index + 1) * len(candles) // nSamples
        group = candles[start:stop]
        result.append(
            Candle(
                group[0].open_time_ms,
                group[0].open,
                max(item.high for item in group),
                min(item.low for item in group),
                group[-1].close,
                sum(item.volume for item in group),
            )
        )
    return tuple(result)


def validate_endpoint(url: str) -> str:
    """Desktop credentials are only sent to an explicitly local core descriptor."""
    parsed = urlsplit(url)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"127.0.0.1", "::1", "localhost"}
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("desktop core endpoint must be a loopback HTTP origin")
    if parsed.port is None or not 1 <= parsed.port <= 65535:
        raise ValueError("core endpoint requires an explicit port")
    return url.rstrip("/")


def read_connection(root: Path) -> tuple[str, str]:
    """Descriptor selects an address; it cannot redirect the owner secret file."""
    descriptor = root / "state" / "core.json"
    if descriptor.stat().st_size > 64 * 1024:
        raise ValueError("core descriptor is too large")
    value = json.loads(descriptor.read_text(encoding="utf-8"))
    url = value.get("url") or value.get("base_url")
    if not url:
        host = value.get("host", "127.0.0.1")
        url = f"http://{host}:{value['port']}"
    token_path = root / "secrets" / "core_token"
    if token_path.stat().st_size > 4096:
        raise ValueError("core credential file is too large")
    token = token_path.read_text(encoding="utf-8").strip()
    if not token or not token.isascii() or any(character.isspace() for character in token):
        raise ValueError("core credential is empty or malformed")
    return validate_endpoint(url), token


def validate_state(value: dict) -> None:
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise ValueError("unsupported core state schema")
    for key in ("core", "market", "settings"):
        if not isinstance(value.get(key), dict):
            raise ValueError(f"core state {key} must be an object")
    for key, maximum in (
        ("accounts", 10),
        ("strategies", 1000),
        ("jobs", 1000),
        ("incidents", 1000),
    ):
        rows = value.get(key)
        if (
            not isinstance(rows, list)
            or len(rows) > maximum
            or any(not isinstance(row, dict) for row in rows)
        ):
            raise ValueError(f"core state {key} must be a bounded object list")
    core = value["core"]
    if (
        type(core.get("paused")) is not bool
        or not isinstance(core.get("epoch"), str)
        or not core["epoch"]
        or type(core.get("control_revision")) is not int
        or type(core.get("server_time_ms")) is not int
    ):
        raise ValueError("core state is missing control freshness metadata")


def read_artifact(path: Path) -> tuple[object, str]:
    """Only bounded inert data is opened. No model deserialization or Python import."""
    if path.suffix.lower() not in {".json", ".jsonl", ".csv"}:
        raise ValueError("viewer supports JSON, JSONL and CSV data only")
    with path.open("rb") as stream:
        raw = stream.read(MAX_ARTIFACT_BYTES + 1)
    if len(raw) > MAX_ARTIFACT_BYTES:
        raise ValueError("artifact exceeds the 16 MiB viewer limit; select a smaller summary")
    content = raw.decode("utf-8-sig")
    if path.suffix.lower() == ".jsonl":
        data = [json.loads(line) for line in content.splitlines() if line.strip()]
    elif path.suffix.lower() == ".csv":
        data = list(csv.DictReader(io.StringIO(content)))
    else:
        data = json.loads(content)
    return data, content


class IncidentTracker:
    """Acknowledgement silences a revision, without pretending that it is resolved."""

    def __init__(self):
        self.seen: set[tuple[str, str, str]] = set()
        self.acknowledged: set[tuple[str, str, str]] = set()
        self.active: dict[tuple[str, str, str], dict] = {}

    @staticmethod
    def key(incident: dict) -> tuple[str, str, str]:
        return (
            str(incident.get("id", incident.get("incident_id", "unknown"))),
            str(incident.get("revision", 1)),
            str(incident.get("severity", "warning")).lower(),
        )

    def update(self, incidents: list[dict]) -> list[dict]:
        active = {
            self.key(incident): incident
            for incident in incidents
            if not incident.get("resolved")
            and incident.get("status", "active") not in {"resolved", "closed"}
        }
        fresh = [
            incident
            for key, incident in active.items()
            if key not in self.seen
            and key not in self.acknowledged
            and not incident.get("acknowledged", False)
        ]
        self.seen.update(active)
        self.active = active
        # Bound client memory independently of the core incident retention policy.
        if len(self.seen) > 4096:
            self.seen = set(active)
            self.acknowledged.intersection_update(active)
        return fresh

    def acknowledge(self, incident: dict) -> None:
        self.acknowledged.add(self.key(incident))

    def critical(self) -> list[dict]:
        return [
            incident
            for key, incident in self.active.items()
            if key[2] in {"critical", "fatal"}
            and key not in self.acknowledged
            and not incident.get("acknowledged", False)
        ]


def graph_layers(structure: object, limit: int = 16) -> list[tuple[str, int]]:
    """Bounded layer summary for an illustrative 3D projection, not a tensor viewer."""
    if not isinstance(structure, dict):
        return []
    layers = structure.get("layers", structure.get("nodes", []))
    if not isinstance(layers, list):
        return []
    result = []
    for index, layer in enumerate(layers[:limit]):
        if not isinstance(layer, dict):
            continue
        label = str(layer.get("name", layer.get("id", layer.get("type", f"Layer {index + 1}"))))[
            :64
        ]
        shape = layer.get("shape", [4])
        fallback = shape[-1] if isinstance(shape, list) and shape else 4
        size = layer.get("units", layer.get("out_features", layer.get("size", fallback)))
        try:
            size = max(1, min(int(size), 24))
        except (TypeError, ValueError, OverflowError):
            size = 4
        result.append((label, size))
    return result
