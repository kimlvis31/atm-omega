"""Bounded retrospective raw-window preparation records; never imports a plugin.

Dataset CSV has no received-time evidence. The actual frozen snapshot time is
the knowledge cutoff; reference_time_ms is a separate historical bar boundary.
"""

# Imports
import csv
import io
import json
import math
import os
import re
import stat
from functools import wraps
from hashlib import sha256
from pathlib import Path

from omega.domain.models import Instrument, Market
from omega.research.artifacts import _status_descriptor, check_cancel, child_path
from omega.research.datasets import FIELDS, SCHEMA_ID, MarketRow
from omega.research.deployment import ShadowModelContract

# Constants
MAX_BYTES = 256 * 1024
MAX_DATASET_BYTES = 32 * 1024 * 1024
MAX_DATASET_ROWS = 100_000
MAX_TIME = 253_402_300_799_000
SCOPE = "offline_retrospective_shadow_only"
REQUEST_KEYS = frozenset(
    {
        "candidate_id",
        "module_sha256",
        "checkpoint_sha256",
        "dataset_sha256",
        "contract",
        "instrument",
        "open_time_ms",
        "stream",
        "sequence",
        "state_hash",
        "core_epoch",
        "deployment_epoch",
        "stream_epoch",
        "deadline_ms",
    }
)
PREPARED_KEYS = frozenset(
    {
        "schema_version",
        "scope",
        "request",
        "module",
        "module_sha256",
        "checkpoint_sha256",
        "dataset_sha256",
        "rows_sha256",
        "window_sha256",
        "window",
        "snapshot_available_at_ms",
        "prepared_at_ms",
        "reference_time_ms",
        "as_of_ms",
        "batch",
        "used_rows",
        "batch_sha256",
        "prepared_sha256",
        "lineage",
    }
)


def encoded(value):
    raw = json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()
    if len(raw) > MAX_BYTES:
        raise ValueError("raw inference record exceeds 256 KiB capacity")
    return raw


def identity(value):
    return sha256(encoded(value)).hexdigest()


def _hex(value, size=64):
    return type(value) is str and re.fullmatch(f"[a-f0-9]{{{size}}}", value) is not None


def _name(value):
    return type(value) is str and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is not None


def _time(value):
    return type(value) is int and 0 <= value <= MAX_TIME


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate raw inference JSON field")
        result[key] = value
    return result


def _guard(function):
    @wraps(function)
    def guarded(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (TypeError, KeyError, AttributeError, OverflowError, RecursionError) as exc:
            raise ValueError("malformed raw inference record") from exc

    return guarded


def _safe_path(path):
    path = Path(os.path.abspath(path))
    for current in (path, *path.parents):
        details = current.lstat()
        if stat.S_ISLNK(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
            raise ValueError("raw inference paths cannot contain symlinks/reparse points")
    return path


def _read_bytes(path, limit):
    path = _safe_path(path)
    descriptor = _status_descriptor(path)
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
            raise ValueError("raw inference input must be a regular non-reparse file")
        if details.st_size > limit:
            raise ValueError("raw inference file exceeds capacity")
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = None
            raw = stream.read(limit + 1)
        if len(raw) > limit:
            raise ValueError("raw inference file exceeds capacity")
        _safe_path(path)
        return raw
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _decode(raw):
    def invalid(value):
        raise ValueError(f"non-finite JSON constant {value}")

    return json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=invalid)


def raw_window_size(request, descriptor=None):
    """Raw minute count is independent of the prepared model's temporal axis.

    Absence is a read-time legacy fallback, never a serialized default injection.
    """
    fallback = request["contract"]["lookback"]
    count = request.get("raw_lookback", fallback)
    if type(count) is not int or not 1 <= count <= 4096:
        raise ValueError("raw_lookback must be an integer within [1,4096]")
    if descriptor is not None:
        declared = descriptor.get("input", {}).get("raw_lookback", fallback)
        if type(declared) is not int or not 1 <= declared <= 4096 or declared != count:
            raise ValueError("raw_lookback differs from the frozen module declaration")
    return count


@_guard
def parse_request(value):
    value = _decode(encoded(value))
    if (
        type(value) is not dict
        or not REQUEST_KEYS <= value.keys()
        or value.keys() - REQUEST_KEYS - {"raw_lookback"}
    ):
        raise ValueError("raw inference request schema mismatch")
    for field in (
        "candidate_id",
        "module_sha256",
        "checkpoint_sha256",
        "dataset_sha256",
        "state_hash",
    ):
        if not _hex(value[field]):
            raise ValueError("raw inference request hash mismatch")
    if (
        not all(_name(value[k]) for k in ("stream", "core_epoch"))
        or not all(_hex(value[k], 32) for k in ("deployment_epoch", "stream_epoch"))
        or type(value["sequence"]) is not int
        or not 1 <= value["sequence"] <= 2**63 - 1
        or not _time(value["open_time_ms"])
        or value["open_time_ms"] % 60000
        or not _time(value["deadline_ms"])
        or value["deadline_ms"] <= value["open_time_ms"] + 60000
    ):
        raise ValueError("raw inference request stream/clock mismatch")
    try:
        provider, market, symbol = value["instrument"].split(":")
        if Instrument(provider, Market(market), symbol).key != value["instrument"]:
            raise ValueError("noncanonical instrument")
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("raw inference instrument mismatch") from exc
    contract = ShadowModelContract.parse(value["contract"])
    if (
        contract.schema_id != SCHEMA_ID
        or value["open_time_ms"] < (raw_window_size(value) - 1) * 60000
    ):
        raise ValueError("raw inference schema/window mismatch")
    return value


@_guard
def parse_parameters(value):
    if (
        type(value) is not dict
        or not {"training_run", "dataset", "request", "trusted_module"} <= value.keys()
        or value.keys()
        - {
            "training_run",
            "dataset",
            "request",
            "trusted_module",
            "checkpoint",
            "timeout_seconds",
            "seed",
            "settings_revision",
        }
    ):
        raise ValueError("nn_prepare parameters mismatch")
    if value["trusted_module"] is not True or any(
        type(value[k]) is not str or not value[k] for k in ("training_run", "dataset")
    ):
        raise ValueError("nn_prepare requires trusted_module and explicit source paths")
    if "checkpoint" in value and (type(value["checkpoint"]) is not str or not value["checkpoint"]):
        raise ValueError("invalid nn_prepare checkpoint")
    timeout = value.get("timeout_seconds", 15)
    if type(timeout) not in {int, float} or not math.isfinite(timeout) or not 0 < timeout <= 120:
        raise ValueError("nn_prepare timeout must be within (0,120]")
    if "seed" in value and type(value["seed"]) is not int:
        raise ValueError("nn_prepare seed metadata must be an integer")
    if "settings_revision" in value and (
        type(value["settings_revision"]) is not int or value["settings_revision"] < 0
    ):
        raise ValueError("nn_prepare settings revision metadata must be a nonnegative integer")
    return {**value, "request": parse_request(value["request"])}


def _manifest_path(dataset):
    path = _safe_path(dataset)
    return path / "dataset.json" if path.is_dir() else path


def manifest_identity(path):
    """SHA256 of actual bounded manifest bytes, never the rows/source hash."""
    return sha256(_read_bytes(_manifest_path(path), MAX_BYTES)).hexdigest()


@_guard
def load_window(dataset, request, cancel=None):
    """Read bounded source bytes before plugin execution and select one exact window."""
    request = parse_request(request)
    path = _manifest_path(dataset)
    raw_manifest = _read_bytes(path, MAX_BYTES)
    if sha256(raw_manifest).hexdigest() != request["dataset_sha256"]:
        raise ValueError("raw dataset manifest bytes hash mismatch")
    manifest = _decode(raw_manifest)
    if (
        manifest.get("schema_id") != SCHEMA_ID
        or manifest.get("state") != "complete"
        or manifest.get("dataset_kind") != "normalized_source"
        or manifest.get("fields") != list(FIELDS)
        or manifest.get("interval_ms") != 60000
        or manifest.get("transform") is not None
        or manifest.get("repair") is not None
        or manifest.get("quality", {}).get("interpolated_rows") != 0
    ):
        raise ValueError("raw preparation requires unrepaired normalized OHLCV; MC unsupported")
    root = path.parent.resolve()
    _safe_path(root / manifest["rows"]["file"])
    _safe_path(root / manifest["source"]["file"])
    rows_raw = _read_bytes(child_path(root, manifest["rows"]["file"]), MAX_DATASET_BYTES)
    source_raw = _read_bytes(child_path(root, manifest["source"]["file"]), MAX_DATASET_BYTES)
    if (
        sha256(rows_raw).hexdigest() != manifest["rows"]["sha256"]
        or sha256(source_raw).hexdigest() != manifest["source"]["sha256"]
    ):
        raise ValueError("raw dataset rows/preserved source hash mismatch")
    start = request["open_time_ms"] - (raw_window_size(request) - 1) * 60000
    end = request["open_time_ms"] + 60000
    splits = manifest.get("splits", {})
    previous_end = -1
    if type(splits) is not dict or set(splits) != {"train", "validation", "test"}:
        raise ValueError("raw dataset requires frozen split ranges")
    for split in ("train", "validation", "test"):
        bounds = splits[split]
        if (
            type(bounds) not in {list, tuple}
            or len(bounds) != 2
            or any(type(v) is not int or not 0 <= v <= MAX_TIME or v % 60000 for v in bounds)
            or not previous_end <= bounds[0] < bounds[1]
        ):
            raise ValueError("invalid or overlapping raw dataset split ranges")
        previous_end = bounds[1]
    if set(splits) != {"train", "validation", "test"} or not any(
        type(bounds) in {list, tuple}
        and len(bounds) == 2
        and all(type(v) is int for v in bounds)
        and bounds[0] <= start < end <= bounds[1]
        for bounds in splits.values()
    ):
        raise ValueError("raw window must stay inside one frozen dataset split")
    selected, seen, instruments, count = [], set(), set(), 0
    for count, raw in enumerate(csv.DictReader(io.StringIO(rows_raw.decode("utf-8-sig"))), 1):
        check_cancel(cancel)
        if count > MAX_DATASET_ROWS:
            raise ValueError("raw inference dataset exceeds 100000 rows")
        row = MarketRow.parse(raw)
        key = row.instrument, row.open_time_ms
        if key in seen:
            raise ValueError("duplicate raw dataset row")
        seen.add(key)
        instruments.add(row.instrument)
        if row.instrument == request["instrument"] and start <= row.open_time_ms < end:
            selected.append(row)
    if (
        type(manifest["rows"]["count"]) is not int
        or count != manifest["rows"]["count"]
        or sorted(instruments) != manifest.get("instruments")
    ):
        raise ValueError("raw dataset count/instruments mismatch")
    selected.sort(key=lambda row: row.open_time_ms)
    window = [row.as_dict() for row in selected]
    if [row.open_time_ms for row in selected] != list(range(start, end, 60000)):
        raise ValueError("raw inference window missing or gapped")
    encoded(window)
    lineage = {
        "dataset_kind": manifest["dataset_kind"],
        "source": manifest["source"],
        "quality": manifest["quality"],
        "synthetic": manifest.get("synthetic"),
        "availability": "actual_snapshot_only; historical received times unavailable",
    }
    return tuple(selected), manifest["rows"]["sha256"], lineage


def _validate_batch(batch, shape):
    if not shape:
        if type(batch) not in {int, float} or not math.isfinite(batch):
            raise ValueError("prepared batch must contain finite numeric values")
        return
    length = 1 if shape[0] is None else shape[0]
    if type(batch) is not list or len(batch) != length:
        raise ValueError("prepared batch shape mismatch")
    for value in batch:
        _validate_batch(value, shape[1:])


@_guard
def parse_prepared(document, expected_request):
    document = _decode(encoded(document))
    request = parse_request(expected_request)
    if (
        type(document) is not dict
        or set(document) != PREPARED_KEYS
        or type(document["schema_version"]) is not int
        or document["schema_version"] != 1
        or document["scope"] != SCOPE
        or document["request"] != request
    ):
        raise ValueError("prepared inference schema/request mismatch")
    for field in ("module_sha256", "checkpoint_sha256", "dataset_sha256"):
        if document[field] != request[field]:
            raise ValueError("prepared inference source identity mismatch")
    for field in ("rows_sha256", "window_sha256", "batch_sha256", "prepared_sha256"):
        if not _hex(document[field]):
            raise ValueError("prepared inference hash mismatch")
    contract = ShadowModelContract.parse(request["contract"])
    raw_count = raw_window_size(request, document["module"])
    window = document["window"]
    if type(window) is not list or len(window) != raw_count:
        raise ValueError("prepared raw window shape mismatch")
    rows = [MarketRow.parse(row) for row in window]
    start = request["open_time_ms"] - (raw_count - 1) * 60000
    if any(
        row.instrument != request["instrument"] or row.open_time_ms != start + i * 60000
        for i, row in enumerate(rows)
    ):
        raise ValueError("prepared raw window identity mismatch")
    if (
        type(document["used_rows"]) is not list
        or any(type(i) is not int for i in document["used_rows"])
        or document["used_rows"] != list(range(len(rows)))
    ):
        raise ValueError("prepared used_rows must identify the exact frozen window")
    reference = request["open_time_ms"] + 60000
    if (
        not all(
            _time(document[k])
            for k in ("snapshot_available_at_ms", "prepared_at_ms", "as_of_ms", "reference_time_ms")
        )
        or not reference
        == document["reference_time_ms"]
        <= document["snapshot_available_at_ms"]
        == document["as_of_ms"]
        <= document["prepared_at_ms"]
        < request["deadline_ms"]
    ):
        raise ValueError("prepared inference actual/reference clock mismatch")
    module = document["module"]
    if (
        type(module) is not dict
        or module.get("input", {}).get("schema_id") != contract.schema_id
        or module.get("capabilities", {}).get("prepare_inference") is not True
    ):
        raise ValueError("prepared module capability/schema mismatch")
    lineage = document["lineage"]
    if (
        type(lineage) is not dict
        or set(lineage) != {"dataset_kind", "source", "quality", "synthetic", "availability"}
        or lineage["dataset_kind"] != "normalized_source"
        or lineage["quality"].get("interpolated_rows") != 0
        or lineage["availability"] != "actual_snapshot_only; historical received times unavailable"
    ):
        raise ValueError("prepared dataset lineage mismatch")
    _validate_batch(document["batch"], contract.input_shape)
    if (
        identity(window) != document["window_sha256"]
        or identity(document["batch"]) != document["batch_sha256"]
        or identity({k: v for k, v in document.items() if k != "prepared_sha256"})
        != document["prepared_sha256"]
    ):
        raise ValueError("prepared inference content checksum mismatch")
    return document


def read_prepared(path, expected_request):
    return parse_prepared(_decode(_read_bytes(path, MAX_BYTES)), expected_request)


def build_prepared(
    request, module, rows, rows_sha256, lineage, result, *, snapshot_available_at_ms, prepared_at_ms
):
    if type(result) is not dict or set(result) != {"batch", "used_rows"}:
        raise ValueError("prepare_inference must return batch and used_rows only")
    window = [row.as_dict() for row in rows]
    document = {
        "schema_version": 1,
        "scope": SCOPE,
        "request": request,
        "module": module,
        **{key: request[key] for key in ("module_sha256", "checkpoint_sha256", "dataset_sha256")},
        "rows_sha256": rows_sha256,
        "window": window,
        "window_sha256": identity(window),
        "snapshot_available_at_ms": snapshot_available_at_ms,
        "prepared_at_ms": prepared_at_ms,
        "reference_time_ms": request["open_time_ms"] + 60000,
        "as_of_ms": snapshot_available_at_ms,
        "batch": result["batch"],
        "used_rows": result["used_rows"],
        "batch_sha256": identity(result["batch"]),
        "lineage": lineage,
    }
    document["prepared_sha256"] = identity(document)
    return parse_prepared(document, request)
