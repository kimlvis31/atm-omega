import csv
from decimal import Decimal, localcontext

import pytest

from omega.research.artifacts import Cancelled, content_hash, read_json, write_json
from omega.research.datasets import (
    FIELDS,
    MarketRow,
    export_dataset,
    load_dataset,
    monte_carlo,
    read_rows,
    select_split,
    split_ranges,
)


def test_export_preserves_bytes_and_reports_gap(source_file, tmp_path):
    result = export_dataset(source_file, tmp_path / "export", {"purge_bars": 1})
    assert result["source"]["sha256"] == content_hash(source_file)
    assert (tmp_path / "export" / "source.original.csv").read_bytes() == source_file.read_bytes()
    assert result["rows"]["count"] == 120
    assert result["splits"]["train"][1] < result["splits"]["validation"][0]
    assert result["quality"]["missing_minutes"] == {
        "binance:usdm:BTCUSDT": 0,
        "binance:usdm:ETHUSDT": 0,
    }


def test_mc_reproducible_joint_and_split_safe(dataset, tmp_path):
    original, original_rows = load_dataset(dataset)
    first = monte_carlo(dataset, tmp_path / "mc1", {"seed": 42, "block_bars": 3})
    monte_carlo(dataset, tmp_path / "mc2", {"seed": 42, "block_bars": 3})
    assert (tmp_path / "mc1/rows.csv").read_bytes() == (tmp_path / "mc2/rows.csv").read_bytes()
    manifest, rows = load_dataset(tmp_path / "mc1")
    assert manifest["source"]["sha256"] == original["source"]["sha256"]
    for split in ("validation", "test"):
        assert select_split(rows, manifest["splits"], split) == select_split(
            original_rows, original["splits"], split
        )
    begin, end = original["splits"]["train"]
    sampled = first["transform"]["provenance"]["train"]["source_timestamps"]
    assert all(begin <= timestamp < end for timestamp in sampled)
    for first_row, second_row in zip(rows[::2], rows[1::2], strict=True):
        assert abs(second_row.close - 2 * first_row.close) < Decimal("1e-23")
        assert second_row.volume == first_row.volume
    assert content_hash(tmp_path / "mc1/source.rows.csv") == original["rows"]["sha256"]


def test_mc_different_seed_and_no_nested_generation(dataset, tmp_path):
    monte_carlo(dataset, tmp_path / "mc1", {"seed": 1, "block_bars": 2})
    monte_carlo(dataset, tmp_path / "mc2", {"seed": 2, "block_bars": 2})
    assert (tmp_path / "mc1/rows.csv").read_bytes() != (tmp_path / "mc2/rows.csv").read_bytes()
    with pytest.raises(ValueError, match="already generated"):
        monte_carlo(tmp_path / "mc1", tmp_path / "nested", {})


def test_hash_tamper_is_rejected(dataset):
    with (dataset / "rows.csv").open("a") as stream:
        stream.write("tamper")
    with pytest.raises(ValueError, match="hash mismatch"):
        load_dataset(dataset)


@pytest.mark.parametrize(
    "field,value",
    [
        ("open", "NaN"),
        ("low", "999"),
        ("volume", "-1"),
        ("close", "0"),
        ("open_time_ms", "1"),
        ("open_time_ms", "60000.0"),
    ],
)
def test_invalid_ohlcv(field, value):
    raw = dict(zip(FIELDS, ("X", "60000", "100", "110", "90", "105", "5"), strict=True))
    raw[field] = value
    with pytest.raises(ValueError):
        MarketRow.parse(raw)


def test_unsupported_channels_and_duplicate_key(source_file, tmp_path):
    raw = {
        "instrument": "X",
        "open_time_ms": 0,
        "open": "1",
        "high": "1",
        "low": "1",
        "close": "1",
        "volume": "0",
        "open_interest": "99",
    }
    with pytest.raises(ValueError, match="other channels unsupported"):
        MarketRow.parse(raw)
    lines = source_file.read_text().splitlines()
    with source_file.open("a") as stream:
        stream.write(lines[1] + "\n")
    with pytest.raises(ValueError, match="duplicate"):
        export_dataset(source_file, tmp_path / "duplicate", {})


def test_gap_not_silently_interpolated_or_resampled(source_file, tmp_path):
    rows = list(csv.DictReader(source_file.open()))
    rows.pop(10)
    with source_file.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    export_dataset(source_file, tmp_path / "gap", {})
    manifest = read_json(tmp_path / "gap/dataset.json")
    assert manifest["quality"]["missing_minutes"]["binance:usdm:BTCUSDT"] == 1
    with pytest.raises(ValueError, match="aligned"):
        monte_carlo(tmp_path / "gap", tmp_path / "mc", {"block_bars": 1})


def test_split_validation_and_cancel(source_file):
    rows = read_rows(source_file)
    with pytest.raises(ValueError, match="not enough"):
        split_ranges(rows, purge_bars=30)
    with pytest.raises(ValueError, match="fractions"):
        split_ranges(rows, [0.8, 0.2, 0.2])

    class Cancel:
        def is_set(self):
            return True

    with pytest.raises(Cancelled):
        read_rows(source_file, Cancel())


def test_marketrow_cannot_bypass_invariants():
    with pytest.raises(ValueError):
        MarketRow("X", 0, Decimal(1), Decimal(0), Decimal(1), Decimal(1), Decimal(1))


def test_tampered_split_cannot_leak_test_into_train(dataset):
    manifest = read_json(dataset / "dataset.json")
    manifest["splits"]["train"][1] = manifest["splits"]["test"][1]
    write_json(dataset / "dataset.json", manifest)
    with pytest.raises(ValueError, match="overlapping"):
        load_dataset(dataset)


def test_mc_is_independent_of_callers_decimal_precision(dataset, tmp_path):
    with localcontext() as context:
        context.prec = 7
        monte_carlo(dataset, tmp_path / "precision7", {"seed": 99, "block_bars": 2})
    with localcontext() as context:
        context.prec = 50
        monte_carlo(dataset, tmp_path / "precision50", {"seed": 99, "block_bars": 2})
    assert (tmp_path / "precision7/rows.csv").read_bytes() == (
        tmp_path / "precision50/rows.csv"
    ).read_bytes()
