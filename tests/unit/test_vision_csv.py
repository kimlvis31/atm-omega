"""Exact CSV field parity and bounded streaming checks for Vision archive parsing."""

import csv
import io
import zipfile
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

pytest.importorskip("polars")

from omega.domain.errors import ProtocolError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.vision import VisionArchive, VisionFile
from omega.infrastructure.binance.vision_csv import iter_csv_fields

DAY = int(datetime(2025, 6, 15, tzinfo=UTC).timestamp() * 1000)
INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


CSV_SAMPLES = (
    (
        "12-column candle",
        "open_time,open,high,low,close,volume,close_time,quote_asset_volume,number_of_trades,taker_buy_base_asset_volume,taker_buy_quote_asset_volume,ignore\n"
        f"{DAY},1.2300,1.2500,1.2000,1.2400,2.500,{DAY + 59999},3.075,17,1.250,1.5375,0\n",
        12,
    ),
    (
        "4-column depth",
        "timestamp,percentage,depth,notional\n2025-06-15 00:00:00,-0.2,1.0000,123.4500\n",
        4,
    ),
    (
        "8-column metrics",
        "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio\n"
        "2025-06-15 00:00:00,ETHUSDT,12.3400,150.125,1.20,1.30,0.80,0.90\n",
        8,
    ),
    (
        "7-column futures trades",
        "aggregate_trade_id,price,quantity,first_trade_id,last_trade_id,timestamp,is_buyer_maker\n"
        f"1,123.4500,0.00200,20,21,{DAY + 10},false\n",
        7,
    ),
    (
        "8-column futures trades",
        "agg_trade_id,price,quantity,first_trade_id,last_trade_id,timestamp,is_buyer_maker,normal_quantity\n"
        f"1,123.4500,0.00200,20,21,{DAY + 10},true,0.00150\n",
        8,
    ),
)


def fast_fields(text, *, batch_rows=4096, check_stop=lambda: None):
    return list(
        iter_csv_fields(
            text.splitlines(keepends=True), batch_rows=batch_rows, check_stop=check_stop
        )
    )


@pytest.mark.parametrize("label,text,width", CSV_SAMPLES)
def test_normal_vision_csv_matches_strict_csv_reader_without_casting(label, text, width):
    expected = list(csv.reader(io.StringIO(text, newline=""), strict=True))

    actual = fast_fields(text)

    assert actual == expected, label
    assert all(len(row) == width for row in actual)
    assert any("." in value and value[-1] == "0" for row in actual for value in row)


@pytest.mark.parametrize(
    "text",
    [
        '1,2,"multi\nline",4\n\n5,6\n',
        'a,b,c\n"a,b",2,3\n4,5\n',
        '1,2,"embedded ""quote""",4\r\n',
    ],
)
def test_quoted_multiline_blank_and_ragged_rows_use_csv_fallback(text):
    expected = list(csv.reader(io.StringIO(text, newline=""), strict=True))

    assert fast_fields(text) == expected


@pytest.mark.parametrize("text", ['a,b\n1,"unterminated', 'a,b\n1,2\n"bad"tail,3\n'])
def test_malformed_strict_csv_preserves_csv_reader_error(text):
    with pytest.raises(csv.Error):
        fast_fields(text)


def test_batch_boundary_preserves_bom_and_empty_fields():
    text = "first,valid\n\ufeffsecond,\nthird,data\n"
    expected = list(csv.reader(io.StringIO(text, newline=""), strict=True))

    assert fast_fields(text, batch_rows=1) == expected


def test_first_row_probe_uses_one_row_batch_without_reading_later_input():
    first = "1,2.5000,0.0100,3,3,1749945600010,false\n"
    yielded = []

    def source():
        yielded.append(1)
        yield first
        raise AssertionError("first-row probe read ahead into later Vision bytes")

    stop_calls = []
    rows = iter_csv_fields(source(), batch_rows=1, check_stop=lambda: stop_calls.append(True))

    assert next(rows) == next(csv.reader([first], strict=True))
    assert yielded == [1]
    assert stop_calls


@pytest.mark.parametrize(
    "channel,key,member,header,row,expected_columns",
    [
        (
            "kline",
            "ETHUSDT-1m-2025-06-15.zip",
            "ETHUSDT-1m-2025-06-15.csv",
            CSV_SAMPLES[0][1].splitlines()[0],
            CSV_SAMPLES[0][1].splitlines()[1],
            12,
        ),
        (
            "depth",
            "ETHUSDT-bookDepth-2025-06-15.zip",
            "ETHUSDT-bookDepth-2025-06-15.csv",
            CSV_SAMPLES[1][1].splitlines()[0],
            CSV_SAMPLES[1][1].splitlines()[1],
            4,
        ),
        (
            "metrics",
            "ETHUSDT-metrics-2025-06-15.zip",
            "ETHUSDT-metrics-2025-06-15.csv",
            CSV_SAMPLES[2][1].splitlines()[0],
            CSV_SAMPLES[2][1].splitlines()[1],
            8,
        ),
        (
            "agg_trade",
            "ETHUSDT-aggTrades-2025-06-15.zip",
            "ETHUSDT-aggTrades-2025-06-15.csv",
            CSV_SAMPLES[3][1].splitlines()[0],
            CSV_SAMPLES[3][1].splitlines()[1],
            7,
        ),
    ],
)
def test_archive_reader_preserves_csv_fields_for_each_native_channel(
    tmp_path, monkeypatch, channel, key, member, header, row, expected_columns
):
    descriptor = VisionFile(key, DAY, 0)
    archive_path = tmp_path / key
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, header + "\n" + row + "\n")
    vision = VisionArchive(tmp_path / "vision", clock_ms=lambda: DAY + 120_000)
    receipt = SimpleNamespace(archive_path=archive_path, received_at_ms=DAY + 120_000)
    monkeypatch.setattr(vision, "_archive", lambda *_args: receipt)

    rows = list(vision._file_rows(INSTRUMENT, channel, descriptor))

    assert len(rows) == 1
    assert rows[0].channel == channel
    assert len(rows[0].values) == expected_columns
    assert rows[0].values == tuple(next(csv.reader([row], strict=True)))
    assert rows[0].row_number == 2


def test_archive_reader_rejects_out_of_order_trade_rows_without_sorting(tmp_path, monkeypatch):
    key = "ETHUSDT-aggTrades-2025-06-15.zip"
    member = "ETHUSDT-aggTrades-2025-06-15.csv"
    descriptor = VisionFile(key, DAY, 0)
    archive_path = tmp_path / key
    text = (
        CSV_SAMPLES[3][1].splitlines()[0]
        + "\n"
        + f"1,10,0.5,1,1,{DAY + 100},false\n"
        + f"2,11,0.5,2,2,{DAY + 50},false\n"
    )
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, text)
    vision = VisionArchive(tmp_path / "vision", clock_ms=lambda: DAY + 120_000)
    receipt = SimpleNamespace(archive_path=archive_path, received_at_ms=DAY + 120_000)
    monkeypatch.setattr(vision, "_archive", lambda *_args: receipt)

    with pytest.raises(ProtocolError, match="order moved backward"):
        list(vision._file_rows(INSTRUMENT, "agg_trade", descriptor))
