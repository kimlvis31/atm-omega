"""Transport-independent, bounded owner history reads and atomic financial ZIP exports."""

# Imports
import csv
import hashlib
import json
import os
import re
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import quote, urlencode

from omega.domain.errors import ValidationError
from omega.domain.margin import MAX_EVENTS, MAX_TIME

SCOPE = "virtual_margin_v2_ledger"
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_QUERY = {"offset", "limit", "snapshot", "start_ms", "end_ms"}
_FIELDS = {
    "schema_version",
    "scope",
    "state",
    "account_id",
    "snapshot",
    "ledger_total",
    "ledger_capacity",
    "total",
    "offset",
    "limit",
    "next_offset",
    "rows",
    "reason_code",
    "core_epoch",
}


def _identity(value):
    if type(value) is not str or _ID.fullmatch(value) is None:
        raise ValidationError("invalid ledger account identity")


def _integer(value, lower, upper):
    if type(value) is not int or not lower <= value <= upper:
        raise ValidationError("invalid bounded ledger integer")


def _token(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,256}", value) is None:
        raise ValidationError("invalid opaque ledger snapshot")


def _arguments(query):
    if type(query) is not dict or set(query) - _QUERY:
        raise ValidationError("unknown ledger query field")
    result = {"offset": 0, "limit": 128, **query}
    _integer(result["offset"], 0, MAX_EVENTS)
    _integer(result["limit"], 1, 1000)
    for key in ("start_ms", "end_ms"):
        if result.get(key) is not None:
            _integer(result[key], 0, MAX_TIME)
    if (
        result.get("start_ms") is not None
        and result.get("end_ms") is not None
        and result["start_ms"] >= result["end_ms"]
    ):
        raise ValidationError("ledger start must precede exclusive end")
    if result.get("snapshot") is not None:
        _token(result["snapshot"])
    return {key: value for key, value in result.items() if value is not None}


def ledger_url(account_id, query):
    """Encode one identity segment; there is no arbitrary URL or credential input."""
    _identity(account_id)
    return (
        "/api/margin-trading/accounts/"
        + quote(account_id, safe="")
        + "/ledger?"
        + urlencode(_arguments(query))
    )


def validate_ledger_page(
    page, *, account_id, offset, limit, snapshot=None, start_ms=None, end_ms=None, core_epoch=None
):
    """Reject unknown/inconsistent observations; preserve exact server amount strings."""
    _identity(account_id)
    _arguments(
        dict(offset=offset, limit=limit, snapshot=snapshot, start_ms=start_ms, end_ms=end_ms)
    )
    if (
        type(page) is not dict
        or set(page) != _FIELDS
        or type(page["schema_version"]) is not int
        or page["schema_version"] != 1
        or page["scope"] != SCOPE
        or page["account_id"] != account_id
        or page["offset"] != offset
        or type(page["offset"]) is not int
        or page["limit"] != limit
        or type(page["limit"]) is not int
        or type(page["core_epoch"]) is not str
        or re.fullmatch(r"[a-f0-9]{32}", page["core_epoch"]) is None
        or core_epoch is not None
        and page["core_epoch"] != core_epoch
    ):
        raise ValidationError("ledger page identity, epoch or shape changed")
    if page["state"] == "unavailable":
        if (
            any(
                page[key] is not None
                for key in ("snapshot", "ledger_total", "ledger_capacity", "total", "next_offset")
            )
            or page["rows"] != []
            or type(page["rows"]) is not list
            or type(page["reason_code"]) is not str
            or not 1 <= len(page["reason_code"]) <= 128
        ):
            raise ValidationError("invalid unavailable ledger observation")
        return dict(page)
    if page["state"] != "ready" or page["reason_code"] is not None:
        raise ValidationError("ledger page is not a confirmed observation")
    _token(page["snapshot"])
    if snapshot is not None and page["snapshot"] != snapshot:
        raise ValidationError("ledger snapshot changed")
    _integer(page["ledger_capacity"], 1, MAX_EVENTS)
    _integer(page["ledger_total"], 0, page["ledger_capacity"])
    _integer(page["total"], 0, page["ledger_total"])
    total = page["total"]
    unfiltered = start_ms is None and end_ms is None
    if unfiltered and total != page["ledger_total"]:
        raise ValidationError("unfiltered ledger count changed")
    if (
        offset > total
        or type(page["rows"]) is not list
        or len(page["rows"]) != min(limit, total - offset)
    ):
        raise ValidationError("ledger page count or offset changed")
    ending = offset + len(page["rows"])
    expected_next = ending if ending < total else None
    if page["next_offset"] != expected_next or (
        expected_next is not None and type(page["next_offset"]) is not int
    ):
        raise ValidationError("ledger page does not advance exactly")
    from omega.runtime.margin_store import ledger_entry_from_document

    previous = 0
    rows = []
    for index, row in enumerate(page["rows"]):
        try:
            entry = ledger_entry_from_document(row)
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ValidationError("invalid typed ledger row") from None
        if (
            not previous < entry.sequence <= page["ledger_total"]
            or unfiltered
            and entry.sequence != offset + index + 1
            or start_ms is not None
            and entry.fact.event_time_ms < start_ms
            or end_ms is not None
            and entry.fact.event_time_ms >= end_ms
        ):
            raise ValidationError("ledger sequence or time filter changed")
        previous = entry.sequence
        # Take an independent JSON-only copy, without normalizing Decimal text.
        rows.append(json.loads(json.dumps(row, allow_nan=False)))
    return {**page, "rows": rows}


class _CSVWriter:
    def __init__(self, stream):
        self.stream, self.digest, self.size = stream, hashlib.sha256(), 0

    def write(self, text):
        raw = text.encode("utf-8")
        self.stream.write(raw)
        self.digest.update(raw)
        self.size += len(raw)
        return len(text)


def export_margin_ledger(
    read_page, destination, *, account_id, start_ms=None, end_ms=None, page_size=128
):
    """Publish one ZIP only after every page belongs to the same frozen prefix/epoch.

    read_page(account_id, query) is supplied by the authenticated GUI or console.
    It may raise on cancellation/disconnect. Existing output is preserved on failure.
    No source files, ledger capacity, pins, or accounts are modified.
    """
    query = _arguments(dict(limit=page_size, start_ms=start_ms, end_ms=end_ms))
    _identity(account_id)
    path = Path(destination)
    if path.suffix.lower() != ".zip":
        raise ValidationError("ledger export destination must end in .zip")
    first = validate_ledger_page(
        read_page(account_id, query),
        account_id=account_id,
        offset=0,
        limit=page_size,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    if first["state"] != "ready":
        raise ValidationError("ledger is unavailable; existing output preserved")
    snapshot, epoch = first["snapshot"], first["core_epoch"]
    stable = tuple(first[key] for key in ("ledger_total", "ledger_capacity", "total"))
    temporary = None
    try:
        fd, temporary = tempfile.mkstemp(prefix=".omega-ledger-", suffix=".zip", dir=path.parent)
        with os.fdopen(fd, "w+b") as output:
            with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                with archive.open("ledger.csv", "w") as raw:
                    hashed = _CSVWriter(raw)
                    writer = csv.writer(hashed, lineterminator="\n")
                    writer.writerow(
                        (
                            "sequence",
                            "kind",
                            "event_time_ms",
                            "available_at_ms",
                            "instrument",
                            "owner_id",
                            "realized_pnl",
                            "fee",
                            "cross_delta",
                            "isolated_delta",
                            "fact_json",
                        )
                    )
                    page, count, previous = first, 0, 0
                    while True:
                        if (
                            page["state"] != "ready"
                            or tuple(
                                page[key] for key in ("ledger_total", "ledger_capacity", "total")
                            )
                            != stable
                        ):
                            raise ValidationError("ledger snapshot accounting changed")
                        for row in page["rows"]:
                            if row["sequence"] <= previous:
                                raise ValidationError("ledger page overlaps previous sequence")
                            previous = row["sequence"]
                            fact = row["fact"]
                            writer.writerow(
                                (
                                    row["sequence"],
                                    fact["kind"],
                                    fact["event_time_ms"],
                                    fact["available_at_ms"],
                                    fact["instrument"],
                                    fact["owner_id"],
                                    row["realized_pnl"],
                                    row["fee"],
                                    row["cross_delta"],
                                    row["isolated_delta"],
                                    json.dumps(fact, sort_keys=True, separators=(",", ":")),
                                )
                            )
                            count += 1
                        if page["next_offset"] is None:
                            break
                        query = {**query, "offset": page["next_offset"], "snapshot": snapshot}
                        page = validate_ledger_page(
                            read_page(account_id, query),
                            account_id=account_id,
                            offset=query["offset"],
                            limit=page_size,
                            snapshot=snapshot,
                            start_ms=start_ms,
                            end_ms=end_ms,
                            core_epoch=epoch,
                        )
                if count != stable[2]:
                    raise ValidationError("ledger export is incomplete")
                manifest = {
                    "schema_version": 1,
                    "scope": SCOPE,
                    "account_id": account_id,
                    "core_epoch": epoch,
                    "snapshot": snapshot,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "ledger_total": stable[0],
                    "ledger_capacity": stable[1],
                    "rows": count,
                    "csv_sha256": hashed.digest.hexdigest(),
                    "csv_bytes": hashed.size,
                    "contents": "financial_ledger_only; not a source or recovery archive",
                }
                archive.writestr("manifest.json", json.dumps(manifest, sort_keys=True, indent=2))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        temporary = None
        return {**manifest, "path": str(path)}
    finally:
        if temporary is not None:
            Path(temporary).unlink(missing_ok=True)
