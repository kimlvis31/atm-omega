import csv
from decimal import Decimal

import pytest

from omega.research.datasets import FIELDS, export_dataset, load_dataset, monte_carlo
from omega.research.interpolation import read_mask


def remove_minutes(source_file, minutes):
    with source_file.open(newline="") as stream:
        records = [
            row
            for row in csv.DictReader(stream)
            if not (
                row["instrument"] == "binance:usdm:BTCUSDT"
                and int(row["open_time_ms"]) // 60000 in minutes
            )
        ]
    with source_file.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(records)


def repaired(source_file, destination, **options):
    manifest = export_dataset(
        source_file, destination, {"interpolation": {"enabled": True, **options}}
    )
    root = destination / "interpolated"
    loaded, rows = load_dataset(root)
    return manifest, root, loaded, rows


def test_linear_variant_preserves_source_and_records_sparse_mask(source_file, tmp_path):
    remove_minutes(source_file, {10})
    original = source_file.read_bytes()
    raw, root, manifest, rows = repaired(source_file, tmp_path / "export")
    assert raw["rows"]["count"] == 119
    assert manifest["rows"]["count"] == 120
    assert (root / "source.original.csv").read_bytes() == original
    assert (root.parent / "source.original.csv").read_bytes() == original
    generated = next(
        row
        for row in rows
        if row.instrument == "binance:usdm:BTCUSDT" and row.open_time_ms == 600000
    )
    assert generated.open == generated.high == generated.low == generated.close == 111
    assert generated.volume == 0
    mask = read_mask(root, manifest)
    entry = mask["binance:usdm:BTCUSDT", 600000]
    assert entry["split"] == "train"
    assert entry["left_anchor_ms"] == 9 * 60000
    assert entry["right_anchor_ms"] == 11 * 60000
    assert entry["available_at_ms"] == 12 * 60000
    assert not manifest["repair"]["causal"]
    quality = manifest["quality"]["split_quality"]["train"]["binance:usdm:BTCUSDT"]
    assert abs(Decimal(quality["interpolation_rate"]) - Decimal(1) / 42) < Decimal("1e-27")
    assert quality["unresolved_rows"] == 0


def test_previous_close_variant_has_causal_availability(source_file, tmp_path):
    remove_minutes(source_file, {10})
    _, root, manifest, rows = repaired(
        source_file, tmp_path / "export", method="previous_close_flat"
    )
    generated = next(
        row
        for row in rows
        if row.instrument == "binance:usdm:BTCUSDT" and row.open_time_ms == 600000
    )
    assert generated.close == 110
    assert manifest["repair"]["causal"]
    entry = read_mask(root, manifest)["binance:usdm:BTCUSDT", 600000]
    assert entry["right_anchor_ms"] is None
    assert entry["available_at_ms"] <= generated.open_time_ms


def test_limits_skip_ineligible_gap_without_discarding_dataset(source_file, tmp_path):
    remove_minutes(source_file, {10, 20, 21, 22})
    _, _, manifest, rows = repaired(
        source_file, tmp_path / "export", max_gap_bars=2, max_ratio=0.05
    )
    quality = manifest["quality"]["split_quality"]["train"]["binance:usdm:BTCUSDT"]
    assert quality["interpolated_rows"] == 1
    assert quality["unresolved_rows"] == 3
    assert quality["unresolved_ranges"][0]["reason"] == "max_gap_bars_exceeded"
    assert len(rows) == 117
    _, _, limited, _ = repaired(source_file, tmp_path / "limited", max_ratio=0.01)
    assert limited["quality"]["interpolated_rows"] == 0
    assert (
        limited["quality"]["split_quality"]["train"]["binance:usdm:BTCUSDT"]["unresolved_rows"] == 4
    )


def test_repair_never_uses_anchors_across_train_validation_boundary(source_file, tmp_path):
    remove_minutes(source_file, {41, 42})
    _, root, manifest, _ = repaired(source_file, tmp_path / "export", max_ratio=1)
    assert manifest["quality"]["interpolated_rows"] == 0
    assert not read_mask(root, manifest)
    train = manifest["quality"]["split_quality"]["train"]["binance:usdm:BTCUSDT"]
    validation = manifest["quality"]["split_quality"]["validation"]["binance:usdm:BTCUSDT"]
    assert train["unresolved_ranges"][-1]["reason"] == "missing_right_split_anchor"
    assert validation["unresolved_ranges"][0]["reason"] == "missing_left_split_anchor"


def test_limit_is_per_split_not_subsidized_by_other_data(source_file, tmp_path):
    remove_minutes(source_file, {10, 45})
    _, _, manifest, _ = repaired(source_file, tmp_path / "export", max_ratio=0.05)
    assert (
        manifest["quality"]["split_quality"]["train"]["binance:usdm:BTCUSDT"]["interpolated_rows"]
        == 1
    )
    validation = manifest["quality"]["split_quality"]["validation"]["binance:usdm:BTCUSDT"]
    assert validation["interpolated_rows"] == 0
    assert validation["unresolved_ranges"][0]["reason"] == "max_ratio_exceeded"


def test_mc_retains_and_propagates_repair_lineage(source_file, tmp_path):
    remove_minutes(source_file, {10})
    _, root, manifest, _ = repaired(source_file, tmp_path / "export")
    generated = tmp_path / "mc"
    monte_carlo(root, generated, {"seed": 42, "block_bars": 2})
    output, rows = load_dataset(generated)
    assert output["repair"]["parent_repair"]["id"] == "linear_close_flat"
    assert (generated / "source.quality-mask.jsonl").read_bytes() == (
        root / "quality-mask.jsonl"
    ).read_bytes()
    assert len(read_mask(generated, output)) == output["quality"]["interpolated_rows"]
    assert output["source"]["sha256"] == manifest["source"]["sha256"]
    assert len(rows) == 120
    assert not output["repair"]["causal"]


def test_mask_tamper_is_not_accepted_as_observed_data(source_file, tmp_path):
    remove_minutes(source_file, {10})
    _, root, _, _ = repaired(source_file, tmp_path / "export")
    (root / "quality-mask.jsonl").write_text("")
    with pytest.raises(ValueError, match="mask hash mismatch"):
        load_dataset(root)


@pytest.mark.parametrize(
    "options",
    [{"max_gap_bars": 0}, {"max_ratio": -1}, {"max_ratio": True}, {"method": "fake_trades"}],
)
def test_invalid_repair_policy(source_file, tmp_path, options):
    with pytest.raises(ValueError):
        repaired(source_file, tmp_path / "export", **options)
