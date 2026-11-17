"""Explicit research-only gap repair with frozen split-local provenance.

Flat OHLC and zero volume are a synthetic convention, not a claim that no trades
occurred. Bidirectional interpolation cannot be consumed as causal market data.
"""

# Imports
import json
import shutil
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from pathlib import Path
from typing import Any

from omega.research.artifacts import check_cancel, child_path, content_hash, write_json
from omega.research.datasets import (
    INTERVAL_MS,
    MAX_ROWS,
    SPLITS,
    MarketRow,
    gap_counts,
    load_dataset,
    write_rows,
)


def read_mask(root: Path, manifest: dict) -> dict[tuple[str, int], dict]:
    repair = manifest.get("repair")
    if not repair:
        return {}
    mask = child_path(root, repair["mask"]["file"])
    if content_hash(mask) != repair["mask"]["sha256"]:
        raise ValueError("dataset repair mask hash mismatch")
    if repair.get("source_mask"):
        ref = repair["source_mask"]
        if content_hash(child_path(root, ref["file"])) != ref["sha256"]:
            raise ValueError("dataset source repair mask hash mismatch")
    records = [json.loads(line) for line in mask.read_text(encoding="utf-8").splitlines()]
    result = {(row["instrument"], row["open_time_ms"]): row for row in records}
    if len(records) != len(result):
        raise ValueError("duplicate repair mask key")
    return result


def write_mask(path: Path, records: list[dict]) -> dict:
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for record in records:
            stream.write(
                json.dumps(record, ensure_ascii=False, allow_nan=False, sort_keys=True) + "\n"
            )
    return {
        "file": path.name,
        "sha256": content_hash(path),
        "synthetic_rows": len(records),
        "representation": "sparse; omitted existing row keys are observed",
    }


def interpolate_dataset(
    source: Path, destination: Path, parameters: dict, cancel: Any = None
) -> dict:
    manifest, rows = load_dataset(source, cancel)
    if manifest.get("transform") or manifest.get("repair"):
        raise ValueError("repair an original normalized dataset; nested repair/MC is unsupported")
    method = parameters.get("method", "linear_close_flat")
    max_gap = parameters.get("max_gap_bars", 5)
    ratio = parameters.get("max_ratio", 0.05)
    if method not in {"linear_close_flat", "previous_close_flat"}:
        raise ValueError("unsupported interpolation method")
    if type(max_gap) is not int or not 1 <= max_gap <= 1440:
        raise ValueError("max_gap_bars must be within [1, 1440]")
    if isinstance(ratio, bool) or not isinstance(ratio, (int, float)) or not 0 <= ratio <= 1:
        raise ValueError("max_ratio must be a fraction within [0, 1]")
    ratio_decimal = Decimal(str(ratio))
    result = list(rows)
    mask = []
    summaries = {}
    # Freeze bounds before observing either anchor. No fit/repair crosses a split boundary.
    with localcontext() as context:
        context.prec = 34
        context.rounding = ROUND_HALF_EVEN
        for split in SPLITS:
            begin, end = manifest["splits"][split]
            summaries[split] = {}
            for instrument in manifest["instruments"]:
                check_cancel(cancel)
                selected = [
                    row
                    for row in rows
                    if row.instrument == instrument and begin <= row.open_time_ms < end
                ]
                expected = (end - begin) // INTERVAL_MS
                inserted = 0
                unresolved = []
                if selected and begin < selected[0].open_time_ms:
                    unresolved.append(
                        {
                            "start_ms": begin,
                            "end_ms": selected[0].open_time_ms,
                            "reason": "missing_left_split_anchor",
                        }
                    )
                for left, right in zip(selected, selected[1:], strict=False):
                    nMissing = (right.open_time_ms - left.open_time_ms) // INTERVAL_MS - 1
                    if nMissing == 0:
                        continue
                    check_cancel(cancel)
                    reason = None
                    if nMissing > max_gap:
                        reason = "max_gap_bars_exceeded"
                    elif Decimal(inserted + nMissing) / expected > ratio_decimal:
                        reason = "max_ratio_exceeded"
                    if reason:
                        unresolved.append(
                            {
                                "start_ms": left.open_time_ms + INTERVAL_MS,
                                "end_ms": right.open_time_ms,
                                "reason": reason,
                            }
                        )
                        continue
                    if len(result) + nMissing > MAX_ROWS:
                        raise ValueError("interpolation exceeds dataset row budget")
                    for step in range(1, nMissing + 1):
                        check_cancel(cancel)
                        timestamp = left.open_time_ms + step * INTERVAL_MS
                        value = left.close
                        available_at = left.open_time_ms + INTERVAL_MS
                        if method == "linear_close_flat":
                            value += (right.close - left.close) * step / (nMissing + 1)
                            available_at = right.open_time_ms + INTERVAL_MS
                        result.append(
                            MarketRow(instrument, timestamp, value, value, value, value, Decimal(0))
                        )
                        mask.append(
                            {
                                "instrument": instrument,
                                "open_time_ms": timestamp,
                                "split": split,
                                "synthetic": True,
                                "method": method,
                                "left_anchor_ms": left.open_time_ms,
                                "right_anchor_ms": right.open_time_ms
                                if method == "linear_close_flat"
                                else None,
                                "available_at_ms": available_at,
                            }
                        )
                    inserted += nMissing
                last_end = selected[-1].open_time_ms + INTERVAL_MS if selected else begin
                if last_end < end:
                    unresolved.append(
                        {
                            "start_ms": last_end,
                            "end_ms": end,
                            "reason": "missing_right_split_anchor"
                            if selected
                            else "no_split_anchors",
                        }
                    )
                summaries[split][instrument] = {
                    "expected_rows": expected,
                    "observed_rows": len(selected),
                    "interpolated_rows": inserted,
                    "interpolation_rate": str(Decimal(inserted) / expected),
                    "unresolved_rows": expected - len(selected) - inserted,
                    "unresolved_ranges": unresolved,
                }
    result.sort(key=lambda row: (row.open_time_ms, row.instrument))
    mask.sort(key=lambda row: (row["open_time_ms"], row["instrument"]))
    destination.mkdir(parents=True, exist_ok=False)
    source_manifest = source / "dataset.json" if source.is_dir() else source
    root = source_manifest.parent
    shutil.copyfile(
        child_path(root, manifest["source"]["file"]), destination / manifest["source"]["file"]
    )
    shutil.copyfile(child_path(root, manifest["rows"]["file"]), destination / "source.rows.csv")
    write_json(destination / "source.dataset.json", manifest)
    write_rows(destination / "rows.csv", result, cancel)
    mask_ref = write_mask(destination / "quality-mask.jsonl", mask)
    gaps = gap_counts(result)
    repaired = manifest | {
        "dataset_kind": "repaired_market",
        "rows": {
            "file": "rows.csv",
            "sha256": content_hash(destination / "rows.csv"),
            "count": len(result),
        },
        "parent": {"rows_sha256": manifest["rows"]["sha256"]},
        "quality": manifest["quality"]
        | {"missing_minutes": gaps, "interpolated_rows": len(mask), "split_quality": summaries},
        "repair": {
            "id": method,
            "version": 1,
            "causal": method == "previous_close_flat",
            "max_gap_bars": max_gap,
            "max_ratio": str(ratio_decimal),
            "limit_scope": "per_split_per_instrument; whole gap skipped if over budget",
            "convention": (
                "synthetic flat OHLC at estimated close; "
                "volume=0 placeholder, not observed zero trading"
            ),
            "decimal_precision": 34,
            "decimal_rounding": "ROUND_HALF_EVEN",
            "split_boundary_policy": "anchors and repaired rows stay inside same frozen split",
            "mask": mask_ref,
        },
    }
    repaired.pop("variants", None)
    write_json(destination / "dataset.json", repaired)
    return repaired
