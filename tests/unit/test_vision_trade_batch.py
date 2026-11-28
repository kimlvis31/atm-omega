"""Parity and conservative fallback checks for the Polars Vision trade fast path."""

import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from itertools import groupby
from types import SimpleNamespace

import pytest

pytest.importorskip("polars")

from omega.application.trade_flow import trade_flow_values
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance import vision_trade_batch as batch_module
from omega.infrastructure.binance.vision import VisionArchive, VisionFile
from omega.runtime.collector_aux_history import trade_provenance

FUTURES = Instrument("binance", Market.USDM, "ETHUSDT")
SPOT = Instrument("binance", Market.SPOT, "BNBUSDT")
DAY = int(datetime(2025, 6, 15, tzinfo=UTC).timestamp() * 1000)
NATIVE_FLOW_KEYS = (
    "AGGTRADE_QUANTITYBUY",
    "AGGTRADE_QUANTITYSELL",
    "AGGTRADE_NOTIONALBUY",
    "AGGTRADE_NOTIONALSELL",
    "AGGTRADE_NTRADESBUY",
    "AGGTRADE_NTRADESSELL",
    "AGGTRADE_EVENTCOUNT",
)


def csv_row(aggregate_id, price, quantity, first, last, timestamp, maker, extra=None):
    fields = [aggregate_id, price, quantity, first, last, timestamp, maker]
    if extra is not None:
        fields.append(extra)
    return ",".join(map(str, fields))


def reference(rows):
    result = []
    for opening, group in groupby(rows, key=lambda row: row.timestamp_ms // 60_000 * 60_000):
        events = [trade_provenance(row) for row in group]
        continuous = all(
            right.aggregate_id == left.aggregate_id + 1
            for left, right in zip(events, events[1:], strict=False)
        )
        result.append((opening, trade_flow_values(events), continuous))
    return result


def test_exact_native_fields_and_split_minute_boundaries():
    # ID 0 falls before the requested window, ID 4 exactly at its exclusive end.
    raw = "\n".join(
        (
            csv_row(0, "999", "9", 0, 0, DAY + 59_999, "false"),
            csv_row(1, "12.3400", "0.2500", 10, 12, DAY + 60_000, "false"),
            csv_row(3, "10.125", "1.500", 13, 14, DAY + 60_500, "true"),
            csv_row(4, "2.00", "0.5", 15, 15, DAY + 120_000, "false"),
            csv_row(5, "1", "1", 16, 16, DAY + 120_001, "false"),
        )
    ).encode()

    rows = batch_module.aggregate_trade_csv(raw, FUTURES, DAY, DAY + 60_000, DAY + 120_000)

    assert [row[0] for row in rows] == [DAY + 60_000]
    opening, fields, continuous = rows[0]
    assert type(opening) is int and type(continuous) is bool and continuous is False
    assert tuple(name for name, _ in fields) == NATIVE_FLOW_KEYS
    values = dict(fields)
    assert values == {
        "AGGTRADE_QUANTITYBUY": Decimal("0.2500"),
        "AGGTRADE_QUANTITYSELL": Decimal("1.500"),
        "AGGTRADE_NOTIONALBUY": Decimal("3.08500000"),
        "AGGTRADE_NOTIONALSELL": Decimal("15.187500"),
        "AGGTRADE_NTRADESBUY": 3,
        "AGGTRADE_NTRADESSELL": 2,
        "AGGTRADE_EVENTCOUNT": 2,
    }
    assert all(
        type(value) is Decimal for key, value in fields if "QUANTITY" in key or "NOTIONAL" in key
    )
    assert all(
        type(value) is int
        for key, value in fields
        if "NTRADES" in key or key.endswith("EVENTCOUNT")
    )


@pytest.mark.parametrize("columns", [7, 8])
def test_futures_seven_and_eight_column_rows_are_equivalent(columns):
    rows = [
        csv_row(
            20,
            "100.00000001",
            "0.00000003",
            30,
            30,
            DAY + 1,
            "false",
            "0.00000002" if columns == 8 else None,
        ),
        csv_row(
            21,
            "100.00000001",
            "0.00000007",
            31,
            32,
            DAY + 2,
            "true",
            "0.00000006" if columns == 8 else None,
        ),
    ]
    raw = ("\r\n".join(rows) + "\r\n").encode()

    result = batch_module.aggregate_trade_csv(raw, FUTURES, DAY, DAY, DAY + 60_000)

    assert len(result) == 1
    assert result[0][1] == (
        ("AGGTRADE_QUANTITYBUY", Decimal("0.00000003")),
        ("AGGTRADE_QUANTITYSELL", Decimal("0.00000007")),
        ("AGGTRADE_NOTIONALBUY", Decimal("0.0000030000000003")),
        ("AGGTRADE_NOTIONALSELL", Decimal("0.0000070000000007")),
        ("AGGTRADE_NTRADESBUY", 1),
        ("AGGTRADE_NTRADESSELL", 2),
        ("AGGTRADE_EVENTCOUNT", 2),
    )
    assert result[0][2] is True


def test_spot_2025_microsecond_timestamps_are_normalized_to_milliseconds():
    stamp_us = (DAY + 1234) * 1000
    raw = csv_row(1, "2.50", "0.4", 5, 6, stamp_us, "false", "0.4").encode()

    result = batch_module.aggregate_trade_csv(raw, SPOT, DAY, DAY, DAY + 60_000)

    assert len(result) == 1
    assert result[0][0] == DAY
    assert dict(result[0][1])["AGGTRADE_NOTIONALBUY"] == Decimal("1.000")
    assert result[0][1][-1] == ("AGGTRADE_EVENTCOUNT", 1)


@pytest.mark.parametrize(
    "raw",
    [
        None,
        b"1,2,3,4,5,not-a-timestamp,false\n",
        b"1,2,3,4,5,1749945600001,maybe\n",
        b"1,2,3,4,5,1749945600001,false,extra,unexpected\n",
        b"1,2,0,4,5,1749945600001,false\n",
    ],
)
def test_missing_or_invalid_raw_rows_select_reference_fallback(raw):
    assert batch_module.aggregate_trade_csv(raw, FUTURES, DAY, DAY, DAY + 60_000) is None


def test_empty_csv_selects_reference_fallback():
    assert batch_module.aggregate_trade_csv(b"", FUTURES, DAY, DAY, DAY + 60_000) is None


def test_oversized_raw_csv_selects_reference_fallback(monkeypatch):
    monkeypatch.setattr(batch_module, "TRADE_BATCH_CSV_BYTES", 1)
    raw = csv_row(1, "2", "1", 1, 1, DAY + 1, "false").encode()

    assert batch_module.aggregate_trade_csv(raw, FUTURES, DAY, DAY, DAY + 60_000) is None


@pytest.mark.parametrize(
    "raw",
    [
        (
            csv_row(1, "2", "1", 1, 1, DAY + 1, "false")
            + "\n"
            + csv_row(1, "2", "1", 1, 1, DAY + 1, "false")
        ).encode(),
        (
            csv_row(1, "2", "1", 1, 1, DAY + 1, "false")
            + "\n"
            + csv_row(1, "3", "1", 1, 1, DAY + 1, "false")
        ).encode(),
    ],
)
def test_duplicate_or_conflicting_aggregate_ids_select_reference_fallback(raw):
    assert batch_module.aggregate_trade_csv(raw, FUTURES, DAY, DAY, DAY + 60_000) is None


def test_vision_zip_caller_matches_rows_to_trade_provenance_reference(tmp_path, monkeypatch):
    pytest.importorskip("polars")
    day_start = DAY
    descriptor = VisionFile("BNBUSDT-aggTrades-2025-06-15.zip", day_start, 0)
    csv_data = "\n".join(
        (
            csv_row(41, "1.2300", "0.010", 100, 101, (day_start + 10) * 1000, "false", "0.010"),
            csv_row(42, "1.2400", "0.020", 102, 102, (day_start + 60_010) * 1000, "true", "0.020"),
        )
    )
    archive_path = tmp_path / descriptor.object_key
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("BNBUSDT-aggTrades-2025-06-15.csv", csv_data)

    vision = VisionArchive(tmp_path / "vision-store", clock_ms=lambda: day_start + 90_000)
    receipt = SimpleNamespace(archive_path=archive_path, received_at_ms=day_start + 120_000)
    monkeypatch.setattr(vision, "files", lambda *_args: [descriptor])
    monkeypatch.setattr(vision, "_archive", lambda *_args: receipt)

    archive_rows = list(vision._file_rows(SPOT, "agg_trade", descriptor))
    expected = reference(archive_rows)
    actual = vision.trade_minute_batch(SPOT, day_start, day_start + 120_000)

    assert actual == expected
    assert dict(actual[0][1])["AGGTRADE_NOTIONALBUY"] == Decimal("0.01230000")
    assert dict(actual[1][1])["AGGTRADE_NOTIONALSELL"] == Decimal("0.02480000")
