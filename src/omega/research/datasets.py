"""Immutable one-minute OHLCV exports and joint block Monte Carlo datasets.

The source file is preserved byte for byte. Generated prices are research
scenarios, never substituted for an exchange history or a live market feed.
"""

# Imports
import csv
import json
import random
import shutil
from collections import defaultdict
from dataclasses import dataclass, replace
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any

from omega.research.artifacts import check_cancel, content_hash, read_json, write_json

# Constants
INTERVAL_MS = 60_000
SCHEMA_ID = "omega.ohlcv.1m.v1"
FIELDS = ("instrument", "open_time_ms", "open", "high", "low", "close", "volume")
SPLITS = ("train", "validation", "test")
MAX_ROWS = 1_000_000


def decimal_value(value: Any, field: str, *, positive: bool = False) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a decimal") from exc
    if not result.is_finite() or result < 0 or (positive and result == 0):
        raise ValueError(f"{field} must be finite and {'positive' if positive else 'nonnegative'}")
    return result


@dataclass(frozen=True)
class MarketRow:
    instrument: str
    open_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    def __post_init__(self) -> None:
        if not self.instrument or self.open_time_ms < 0 or self.open_time_ms % INTERVAL_MS:
            raise ValueError("instrument and UTC-aligned one-minute timestamp are required")
        for name in ("open", "high", "low", "close", "volume"):
            value = getattr(self, name)
            if not value.is_finite() or value < 0 or (name != "volume" and value == 0):
                raise ValueError(f"invalid {name}")
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError("invalid OHLC ordering")

    @classmethod
    def parse(cls, raw: dict) -> "MarketRow":
        if set(raw) != set(FIELDS):
            raise ValueError(f"OHLCV schema requires exactly {FIELDS}; other channels unsupported")
        timestamp = raw["open_time_ms"]
        if isinstance(timestamp, bool) or str(int(timestamp)) != str(timestamp):
            raise ValueError("open_time_ms must be an integer UTC millisecond timestamp")
        return cls(
            str(raw["instrument"]),
            int(timestamp),
            *(decimal_value(raw[name], name, positive=name != "volume") for name in FIELDS[2:]),
        )

    def as_dict(self) -> dict:
        return {"instrument": self.instrument, "open_time_ms": self.open_time_ms} | {
            name: str(getattr(self, name)) for name in FIELDS[2:]
        }


def read_rows(path: Path, cancel: Any = None) -> list[MarketRow]:
    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        if path.suffix.lower() == ".csv":
            records = csv.DictReader(stream)
        elif path.suffix.lower() in {".jsonl", ".ndjson"}:
            records = (json.loads(line) for line in stream if line.strip())
        else:
            raise ValueError("dataset input must be CSV or JSONL")
        for index, raw in enumerate(records):
            if index >= MAX_ROWS:
                raise ValueError(f"initial dataset limit is {MAX_ROWS} rows")
            check_cancel(cancel)
            rows.append(MarketRow.parse(raw))
    if not rows:
        raise ValueError("empty dataset")
    rows.sort(key=lambda row: (row.open_time_ms, row.instrument))
    keys = [(row.open_time_ms, row.instrument) for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate instrument/candle key")
    return rows


def write_rows(path: Path, rows: list[MarketRow], cancel: Any = None) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            check_cancel(cancel)
            writer.writerow(row.as_dict())


def gap_counts(rows: list[MarketRow]) -> dict[str, int]:
    by_instrument = defaultdict(list)
    for row in rows:
        by_instrument[row.instrument].append(row.open_time_ms)
    return {
        instrument: sum((b - a) // INTERVAL_MS - 1 for a, b in zip(times, times[1:], strict=False))
        for instrument, times in by_instrument.items()
    }


def split_ranges(
    rows: list[MarketRow], ratios: list[float] | None = None, purge_bars: int = 0
) -> dict[str, tuple[int, int]]:
    """Freeze source timeline first; purge both sides of an internal boundary."""
    ratios = ratios or [0.7, 0.15, 0.15]
    if len(ratios) != 3 or any(not 0 < n < 1 for n in ratios) or abs(sum(ratios) - 1) > 1e-9:
        raise ValueError("split_ratios must be three positive fractions summing to one")
    if type(purge_bars) is not int or purge_bars < 0:
        raise ValueError("purge_bars must be a nonnegative integer")
    times = sorted({row.open_time_ms for row in rows})
    boundaries = [0, int(len(times) * ratios[0]), int(len(times) * sum(ratios[:2])), len(times)]
    ranges = {}
    for index, split in enumerate(SPLITS):
        begin = boundaries[index] + (purge_bars if index else 0)
        end = boundaries[index + 1] - (purge_bars if index < 2 else 0)
        if begin >= end:
            raise ValueError(f"not enough timestamps for {split} after split/purge")
        ranges[split] = (times[begin], times[end - 1] + INTERVAL_MS)
    return ranges


def select_split(rows: list[MarketRow], ranges: dict, split: str) -> list[MarketRow]:
    if split not in SPLITS:
        raise ValueError("split must be train, validation or test")
    begin, end = ranges[split]
    return [row for row in rows if begin <= row.open_time_ms < end]


def export_dataset(source: Path, destination: Path, parameters: dict, cancel: Any = None) -> dict:
    if not source.is_file():
        raise ValueError("source dataset file does not exist")
    destination.mkdir(parents=True, exist_ok=False)
    original = destination / f"source.original{source.suffix.lower()}"
    # [1]: Freeze bytes before normalization; changing inputs never produce a mixed snapshot.
    before = content_hash(source)
    shutil.copyfile(source, original)
    if before != content_hash(source) or before != content_hash(original):
        raise ValueError("source changed while snapshotting")
    rows = read_rows(original, cancel)
    purge = parameters.get("purge_bars", 0)
    ranges = split_ranges(rows, parameters.get("split_ratios"), purge)
    output = destination / "rows.csv"
    write_rows(output, rows, cancel)
    instruments = sorted({row.instrument for row in rows})
    gaps = gap_counts(rows)
    manifest = {
        "schema_version": 1,
        "schema_id": SCHEMA_ID,
        "dataset_kind": "normalized_source",
        "state": "complete",
        "source": {"file": original.name, "sha256": before, "representation": "input_file_bytes"},
        "rows": {"file": "rows.csv", "sha256": content_hash(output), "count": len(rows)},
        "instruments": instruments,
        "interval_ms": INTERVAL_MS,
        "fields": list(FIELDS),
        "splits": ranges,
        "purge_bars": purge,
        "quality": {
            "missing_minutes": gaps,
            "interpolated_rows": 0,
            "finality": "closed_bars_asserted_by_input_schema",
        },
        "transform": None,
    }
    write_json(destination / "dataset.json", manifest)
    interpolation = parameters.get("interpolation", {})
    if not isinstance(interpolation, dict) or type(interpolation.get("enabled", False)) is not bool:
        raise ValueError("interpolation must be an object with boolean enabled")
    if interpolation.get("enabled", False):
        from omega.research.interpolation import interpolate_dataset

        repaired = interpolate_dataset(
            destination, destination / "interpolated", interpolation, cancel
        )
        manifest["variants"] = {
            "interpolated": {
                "manifest": "interpolated/dataset.json",
                "rows_sha256": repaired["rows"]["sha256"],
                "interpolated_rows": repaired["quality"]["interpolated_rows"],
            }
        }
        write_json(destination / "dataset.json", manifest)
    return manifest


def load_dataset(path: Path, cancel: Any = None) -> tuple[dict, list[MarketRow]]:
    manifest_path = path / "dataset.json" if path.is_dir() else path
    manifest = read_json(manifest_path)
    if manifest.get("schema_id") != SCHEMA_ID or manifest.get("state") != "complete":
        raise ValueError("unsupported or incomplete dataset manifest")
    root = manifest_path.parent.resolve()
    from omega.research.artifacts import child_path

    rows_path = child_path(root, manifest["rows"]["file"])
    source_path = child_path(root, manifest["source"]["file"])
    if content_hash(rows_path) != manifest["rows"]["sha256"]:
        raise ValueError("dataset content hash mismatch")
    if content_hash(source_path) != manifest["source"]["sha256"]:
        raise ValueError("preserved source hash mismatch")
    rows = read_rows(rows_path, cancel)
    if len(rows) != manifest["rows"]["count"]:
        raise ValueError("dataset row count mismatch")
    if manifest.get("fields") != list(FIELDS) or manifest.get("interval_ms") != INTERVAL_MS:
        raise ValueError("dataset field/interval schema mismatch")
    if sorted({row.instrument for row in rows}) != manifest.get("instruments"):
        raise ValueError("dataset instrument manifest mismatch")
    ranges = manifest.get("splits", {})
    if set(ranges) != set(SPLITS):
        raise ValueError("dataset requires frozen train/validation/test splits")
    previous_end = -1
    for split in SPLITS:
        bounds = ranges[split]
        if (
            not isinstance(bounds, (list, tuple))
            or len(bounds) != 2
            or any(type(value) is not int or value % INTERVAL_MS for value in bounds)
            or bounds[0] < previous_end
            or bounds[0] >= bounds[1]
            or bounds[0] < rows[0].open_time_ms
            or bounds[1] > rows[-1].open_time_ms + INTERVAL_MS
            or not select_split(rows, ranges, split)
        ):
            raise ValueError("invalid or overlapping dataset splits")
        previous_end = bounds[1]
    if manifest.get("repair"):
        from omega.research.interpolation import read_mask

        mask = read_mask(root, manifest)
        if (
            mask.keys() - {(row.instrument, row.open_time_ms) for row in rows}
            or len(mask) != manifest["quality"]["interpolated_rows"]
        ):
            raise ValueError("dataset interpolation mask/row mismatch")
    return manifest, rows


def monte_carlo(source: Path, destination: Path, parameters: dict, cancel: Any = None) -> dict:
    path = source / "dataset.json" if source.is_dir() else source
    if read_json(path).get("schema_id") == "omega.market-observations.1m.v1":
        from omega.research.auxiliary_datasets import monte_carlo_auxiliary

        return monte_carlo_auxiliary(source, destination, parameters, cancel)
    # Caller decimal context must not silently alter a scenario's precision.
    with localcontext() as context:
        context.prec = 34
        context.rounding = ROUND_HALF_EVEN
        return _monte_carlo(source, destination, parameters, cancel)


def _monte_carlo(source: Path, destination: Path, parameters: dict, cancel: Any = None) -> dict:
    manifest, rows = load_dataset(source, cancel)
    if manifest.get("transform"):
        raise ValueError("resample an original dataset, not an already generated scenario")
    seed = parameters.get("seed", 0)
    block_bars = parameters.get("block_bars", 60)
    scenario_index = parameters.get("scenario_index", 0)
    if type(seed) is not int or type(scenario_index) is not int or scenario_index < 0:
        raise ValueError("seed and scenario_index must be integers; scenario_index nonnegative")
    if type(block_bars) is not int or block_bars < 1:
        raise ValueError("block_bars must be positive")
    generated_splits = parameters.get("splits", ["train"])
    if not generated_splits or any(split not in SPLITS for split in generated_splits):
        raise ValueError("invalid Monte Carlo splits")
    rng = random.Random(f"joint-relative-block-v1:{seed}:{scenario_index}")
    result = []
    provenance = {}
    source_manifest = source / "dataset.json" if source.is_dir() else source
    source_mask = {}
    generated_mask = []
    if manifest.get("repair"):
        from omega.research.interpolation import read_mask

        source_mask = read_mask(source_manifest.parent, manifest)
    # [2]: Sample aligned blocks jointly across instruments, independently inside each frozen split.
    for split in SPLITS:
        selected = select_split(rows, manifest["splits"], split)
        if split not in generated_splits:
            result.extend(selected)
            generated_mask.extend(
                source_mask[row.instrument, row.open_time_ms]
                for row in selected
                if (row.instrument, row.open_time_ms) in source_mask
            )
            continue
        by_time: dict[int, dict[str, MarketRow]] = defaultdict(dict)
        for row in selected:
            by_time[row.open_time_ms][row.instrument] = row
        times = sorted(by_time)
        instruments = manifest["instruments"]
        if any(set(frame) != set(instruments) for frame in by_time.values()):
            raise ValueError("joint MC requires aligned instruments; missing data is not invented")
        if any(b - a != INTERVAL_MS for a, b in zip(times, times[1:], strict=False)):
            raise ValueError("joint MC requires contiguous one-minute data within each split")
        if len(times) < 2 or block_bars > len(times) - 1:
            raise ValueError(f"block_bars exceeds usable source returns in {split}")
        indexes = []
        while len(indexes) < len(times):
            check_cancel(cancel)
            start = rng.randrange(1, len(times) - block_bars + 1)
            indexes.extend(range(start, start + block_bars))
        indexes = indexes[: len(times)]
        previous = {instrument: by_time[times[0]][instrument].open for instrument in instruments}
        repair_dependency = {
            instrument: (instrument, times[0]) in source_mask for instrument in instruments
        }
        for timestamp, source_index in zip(times, indexes, strict=True):
            check_cancel(cancel)
            for instrument in instruments:
                candle = by_time[times[source_index]][instrument]
                anchor = by_time[times[source_index - 1]][instrument].close
                scale = previous[instrument] / anchor
                generated = replace(
                    candle,
                    open_time_ms=timestamp,
                    open=candle.open * scale,
                    high=candle.high * scale,
                    low=candle.low * scale,
                    close=candle.close * scale,
                )
                result.append(generated)
                previous[instrument] = generated.close
                candle_key = instrument, candle.open_time_ms
                anchor_key = instrument, times[source_index - 1]
                if (
                    candle_key in source_mask
                    or anchor_key in source_mask
                    or repair_dependency[instrument]
                ):
                    repair_dependency[instrument] = True
                    generated_mask.append(
                        {
                            "instrument": instrument,
                            "open_time_ms": timestamp,
                            "split": split,
                            "synthetic": True,
                            "method": "mc_from_repaired_source",
                            "source_timestamp_ms": candle.open_time_ms,
                            "source_anchor_timestamp_ms": times[source_index - 1],
                            "cumulative_repair_dependency": True,
                        }
                    )
        provenance[split] = {"source_timestamps": [times[index] for index in indexes]}
    result.sort(key=lambda row: (row.open_time_ms, row.instrument))
    destination.mkdir(parents=True, exist_ok=False)
    source_root = source_manifest.parent
    original = destination / manifest["source"]["file"]
    shutil.copyfile(source_root / manifest["source"]["file"], original)
    shutil.copyfile(source_root / "rows.csv", destination / "source.rows.csv")
    write_json(destination / "source.dataset.json", manifest)
    write_rows(destination / "rows.csv", result, cancel)
    generated_manifest = manifest | {
        "dataset_kind": "generated_market",
        "rows": {
            "file": "rows.csv",
            "sha256": content_hash(destination / "rows.csv"),
            "count": len(result),
        },
        "parent": {
            "manifest_sha256": content_hash(source_manifest),
            "rows_sha256": manifest["rows"]["sha256"],
        },
        "quality": manifest["quality"] | {"missing_minutes": gap_counts(result)},
        "transform": {
            "id": "joint-relative-block",
            "version": 1,
            "rng": "python.random.MT19937",
            "decimal_precision": 34,
            "decimal_rounding": "ROUND_HALF_EVEN",
            "seed": seed,
            "scenario_index": scenario_index,
            "block_bars": block_bars,
            "splits": generated_splits,
            "joint_instruments": True,
            "volume_policy": "joint_resampled_base_volume",
            "derived_features": "must_recompute",
            "unsupported": ["depth", "trades", "open_interest", "funding", "quote_volume"],
            "provenance": provenance,
        },
    }
    generated_manifest.pop("variants", None)
    if manifest.get("repair"):
        from omega.research.interpolation import write_mask

        shutil.copyfile(
            source_root / manifest["repair"]["mask"]["file"],
            destination / "source.quality-mask.jsonl",
        )
        generated_mask.sort(key=lambda row: (row["open_time_ms"], row["instrument"]))
        generated_manifest["repair"] = {
            "id": "mc_quality_lineage",
            "version": 1,
            "causal": manifest["repair"]["causal"],
            "parent_repair": manifest["repair"],
            "source_mask": {
                "file": "source.quality-mask.jsonl",
                "sha256": content_hash(destination / "source.quality-mask.jsonl"),
            },
            "mask": write_mask(destination / "quality-mask.jsonl", generated_mask),
        }
        generated_manifest["quality"] |= {
            "interpolated_rows": len(generated_mask),
            "interpolation_mask_meaning": (
                "generated rows depending on repaired source candle or return anchor"
            ),
            "split_quality": "see source.dataset.json; MC changes source row sampling",
        }
    write_json(destination / "dataset.json", generated_manifest)
    return generated_manifest
