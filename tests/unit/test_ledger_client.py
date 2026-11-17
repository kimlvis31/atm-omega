"""An exported ledger must be one bounded, exact, immutable observation."""

import csv
import hashlib
import io
import json
import zipfile
from copy import deepcopy
from decimal import Decimal

import pytest

from omega.domain.errors import ValidationError
from omega.domain.margin import CollateralTransfer, FillEvent, FundingCashflow, LedgerEntry
from omega.domain.models import Instrument, Market
from omega.runtime.console import ControlConsole
from omega.runtime.ledger_client import export_margin_ledger, ledger_url, validate_ledger_page
from omega.runtime.margin_store import ledger_entry_document

EPOCH = "a" * 32
INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def rows():
    fill = FillEvent(
        "f", "o", INSTRUMENT, "ts", Decimal(1), Decimal(100), Decimal("0.10"), "USDT", 0, 0
    )
    transfer = CollateralTransfer("t", INSTRUMENT, "ts", Decimal("2.00"), 60000, 60000)
    return [
        ledger_entry_document(
            LedgerEntry(1, fill, Decimal(0), Decimal("0.10"), Decimal("-0.10"), Decimal(0))
        ),
        ledger_entry_document(
            LedgerEntry(2, transfer, Decimal(0), Decimal(0), Decimal("-2.00"), Decimal("2.00"))
        ),
    ]


def page(offset=0, limit=1):
    data = rows()
    return {
        "schema_version": 1,
        "scope": "virtual_margin_v2_ledger",
        "state": "ready",
        "account_id": "a:b",
        "snapshot": "opaque",
        "ledger_total": 2,
        "ledger_capacity": 10000,
        "total": 2,
        "offset": offset,
        "limit": limit,
        "next_offset": offset + limit if offset + limit < 2 else None,
        "rows": data[offset : offset + limit],
        "reason_code": None,
        "core_epoch": EPOCH,
    }


def test_exact_zip_signed_amounts_manifest_and_transport_contract(tmp_path):
    calls = []

    def read(identity, query):
        calls.append((identity, deepcopy(query)))
        return page(query["offset"], query["limit"])

    target = tmp_path / "history.zip"
    result = export_margin_ledger(read, target, account_id="a:b", page_size=1)
    assert len(calls) == 2 and calls[1][1]["snapshot"] == "opaque"
    with zipfile.ZipFile(target) as archive:
        assert set(archive.namelist()) == {"ledger.csv", "manifest.json"}
        raw = archive.read("ledger.csv")
        manifest = json.loads(archive.read("manifest.json"))
    records = list(csv.DictReader(io.StringIO(raw.decode())))
    assert records[0]["fee"] == "0.10" and records[1]["cross_delta"] == "-2.00"
    assert json.loads(records[1]["fact_json"]) == rows()[1]["fact"]
    assert manifest["csv_sha256"] == hashlib.sha256(raw).hexdigest() == result["csv_sha256"]
    assert manifest["csv_bytes"] == len(raw) and manifest["rows"] == 2
    assert not list(tmp_path.glob(".omega-ledger-*"))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda p: p.update(core_epoch="b" * 32),
        lambda p: p.update(snapshot="changed"),
        lambda p: p.update(total=1),
        lambda p: p.update(next_offset=1),
        lambda p: p.update(rows=[rows()[0]]),
        lambda p: p["rows"][0].update(cross_delta="9"),
    ],
)
def test_changed_later_page_never_replaces_prior_export(tmp_path, mutation):
    destination = tmp_path / "history.zip"
    destination.write_bytes(b"prior valid export")

    def read(_identity, query):
        result = page(query["offset"], query["limit"])
        if query["offset"]:
            mutation(result)
        return result

    with pytest.raises(ValidationError):
        export_margin_ledger(read, destination, account_id="a:b", page_size=1)
    assert destination.read_bytes() == b"prior valid export"
    assert not list(tmp_path.glob(".omega-ledger-*"))


def test_read_cancellation_preserves_previous_output(tmp_path):
    destination = tmp_path / "history.zip"
    destination.write_bytes(b"old")

    def read(_identity, query):
        if query["offset"]:
            raise TimeoutError("disconnected")
        return page()

    with pytest.raises(TimeoutError):
        export_margin_ledger(read, destination, account_id="a:b", page_size=1)
    assert destination.read_bytes() == b"old"
    assert not list(tmp_path.glob(".omega-ledger-*"))


@pytest.mark.parametrize(
    "query",
    [
        {"offset": True},
        {"limit": 0},
        {"limit": 1001},
        {"start_ms": -1},
        {"start_ms": 2, "end_ms": 2},
        {"unknown": 1},
        {"snapshot": "x/y"},
    ],
)
def test_invalid_query_never_becomes_url(query):
    with pytest.raises(ValidationError):
        ledger_url("a:b", query)


@pytest.mark.parametrize("identity", ["../x", "a/b", "%2f", "", "한글"])
def test_identity_is_one_ascii_segment(identity):
    with pytest.raises(ValidationError):
        ledger_url(identity, {})


def test_url_encoding_and_console_windows_path(tmp_path, monkeypatch):
    assert "/a%3Ab/ledger?" in ledger_url("a:b", {})
    console = ControlConsole(tmp_path)
    calls = []

    def request(method, path, body=None):
        calls.append((method, path))
        return page(0, 128)

    monkeypatch.setattr(console, "request", request)
    assert console.execute("ledger a:b")["total"] == 2
    destination = tmp_path / "space name.zip"
    result = console.execute(f'ledger-export a:b "{destination}"')
    assert result["rows"] == 2
    assert all(method == "GET" for method, _ in calls)


def test_filtered_selection_may_have_sequence_gaps_and_empty_result():
    result = page(0, 128)
    result.update(rows=[rows()[1]], total=1)
    checked = validate_ledger_page(result, account_id="a:b", offset=0, limit=128, start_ms=60000)
    assert checked["rows"][0]["sequence"] == 2
    result.update(rows=[], total=0)
    assert (
        validate_ledger_page(result, account_id="a:b", offset=0, limit=128, end_ms=0)["rows"] == []
    )


@pytest.mark.parametrize("rate,amount", [("0.01", "-1.00000000"), ("-0.01", "1.00000000")])
def test_signed_funding_fact_is_exported_without_rounding_or_second_debit(tmp_path, rate, amount):
    fact = FundingCashflow(
        funding_id="fund",
        instrument=INSTRUMENT,
        owner_id="ts",
        amount=Decimal(amount),
        rate=Decimal(rate),
        reference_price=Decimal(100),
        signed_quantity=Decimal(1),
        event_time_ms=60000,
        source_available_at_ms=60000,
        available_at_ms=120000,
        source_kind="scenario_rate",
        source_sha256="a" * 64,
        policy_sha256="b" * 64,
    )
    row = ledger_entry_document(
        LedgerEntry(1, fact, Decimal(0), Decimal(0), Decimal(amount), Decimal(0))
    )
    observation = {**page(0, 128), "rows": [row], "ledger_total": 1, "total": 1}
    target = tmp_path / "funding.zip"
    export_margin_ledger(lambda *_: observation, target, account_id="a:b")
    with zipfile.ZipFile(target) as archive:
        records = list(csv.DictReader(io.StringIO(archive.read("ledger.csv").decode())))
    assert records[0]["cross_delta"] == amount
    assert json.loads(records[0]["fact_json"]) == row["fact"]
    assert observation["rows"] == [row]


@pytest.mark.parametrize(
    "change",
    [
        lambda p: p["rows"][0]["fact"].update(instrument="not:an:instrument:extra"),
        lambda p: p.update(total=1, rows=[rows()[0]], next_offset=None),
        lambda p: p.update(rows=[rows()[1], rows()[0]]),
    ],
)
def test_corrupted_unfiltered_observation_is_rejected(change):
    observation = page(0, 128)
    change(observation)
    with pytest.raises(ValidationError):
        validate_ledger_page(observation, account_id="a:b", offset=0, limit=128)
