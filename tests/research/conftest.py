"""Synthetic offline datasets; no exchange history or account data."""

import csv
from decimal import Decimal

import pytest

from omega.research.datasets import FIELDS, export_dataset


@pytest.fixture
def source_file(tmp_path):
    path = tmp_path / "synthetic.csv"
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        for index in range(60):
            for instrument, factor in (("binance:usdm:BTCUSDT", 1), ("binance:usdm:ETHUSDT", 2)):
                price = Decimal(100 + index) * factor
                writer.writerow(
                    {
                        "instrument": instrument,
                        "open_time_ms": index * 60000,
                        "open": str(price),
                        "high": str(price + factor * 3),
                        "low": str(price - factor * 2),
                        "close": str(price + factor),
                        "volume": str(index + 10),
                    }
                )
    return path


@pytest.fixture
def dataset(source_file, tmp_path):
    path = tmp_path / "dataset"
    export_dataset(source_file, path, {})
    return path


@pytest.fixture
def gapped_source(source_file):
    lines = source_file.read_text().splitlines()
    source_file.write_text(
        "\n".join(line for line in lines if not line.startswith("binance:usdm:BTCUSDT,600000,"))
        + "\n"
    )
    return source_file
