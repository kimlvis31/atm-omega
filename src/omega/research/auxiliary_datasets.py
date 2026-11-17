"""Self-contained minute observations and joint, explicitly synthetic research scenarios.

OHLCV v1 stays unchanged. A separate manifest joins selected stored observations
without inventing missing samples or replacing their original availability proof.
"""

# Imports
import json
import os
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import replace
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from hashlib import sha256
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from omega.application.auxiliary_codec import (
    decode_observation,
    encode_observation,
    observation_checksum,
)
from omega.domain.errors import ValidationError
from omega.domain.models import Instrument
from omega.ports.auxiliary import AuxiliaryReadRange, StoredAuxiliary
from omega.research import auxiliary_bundle_v2 as native_v2
from omega.research.artifacts import (
    _bounded_artifact_stream,
    bounded_artifact_hash,
    check_cancel,
    copy_bounded_artifact,
    read_status_json,
    validate_artifact_path,
    write_json,
)
from omega.research.datasets import SPLITS, MarketRow, load_dataset, monte_carlo, select_split

# Constants
SCHEMA_ID = "omega.market-observations.1m.v1"
SCHEMA_ID_V2 = native_v2.SCHEMA_ID
MAX_FILE_BYTES = 32 * 1024 * 1024
MAX_OBSERVATIONS = 100_000
MAX_RAW_FILES = 32
MAX_RAW_TOTAL_BYTES = 128 * 1024 * 1024
_MAX_RECORD_BYTES = 512 * 1024
_FIELDS = {
    "AGGTRADE_QUANTITYBUY": "base_quantity",
    "AGGTRADE_QUANTITYSELL": "base_quantity",
    "AGGTRADE_NOTIONALBUY": "quote_notional",
    "AGGTRADE_NOTIONALSELL": "quote_notional",
    "AGGTRADE_NTRADESBUY": "count",
    "AGGTRADE_NTRADESSELL": "count",
    "METRIC_OPENINTEREST": "contract_quantity",
    "METRIC_OPENINTERESTVALUE": "quote_notional",
    "METRIC_LONGSHORTRATIO": "ratio",
    "METRIC_LONGACCOUNT": "account_fraction",
    "METRIC_SHORTACCOUNT": "account_fraction",
    "FUNDING_RATE": "signed_rate",
    "KLINE_VOLQUOTE": "quote_notional",
    "KLINE_VOLBASETAKERBUY": "base_quantity",
    "KLINE_VOLQUOTETAKERBUY": "quote_notional",
    "KLINE_NTRADES": "count",
    "SOURCE_PERIOD_MS": "milliseconds",
}
_CHANNEL_FIELDS = {
    "trade_flow": {key for key in _FIELDS if key.startswith("AGGTRADE_")},
    "open_interest": {"METRIC_OPENINTEREST", "METRIC_OPENINTERESTVALUE", "SOURCE_PERIOD_MS"},
    "long_short_ratio": {
        "METRIC_LONGSHORTRATIO",
        "METRIC_LONGACCOUNT",
        "METRIC_SHORTACCOUNT",
        "SOURCE_PERIOD_MS",
    },
    "funding_rate": {"FUNDING_RATE", "SOURCE_PERIOD_MS"},
    "candle_statistics": {key for key in _FIELDS if key.startswith("KLINE_")},
    "native_statistics": set(_FIELDS),
}
_RECORD_KEYS = {"revision", "checksum", "previous_checksum", "payload"}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _relative(root, name):
    if (
        type(name) is not str
        or not 1 <= len(name) <= 256
        or not name.isprintable()
        or ":" in name
        or "\\" in name
        or PurePosixPath(name).is_absolute()
        or PureWindowsPath(name).drive
        or ".." in PurePosixPath(name).parts
        or PurePosixPath(name).as_posix() != name
    ):
        raise ValidationError("dataset references require canonical relative paths")
    return validate_artifact_path(Path(root).absolute() / name, allow_missing=True)


def _manifest_path(source):
    source = Path(source).absolute()
    return validate_artifact_path(source / "dataset.json" if source.is_dir() else source)


def _write_manifest(path, manifest):
    if len(_json(manifest)) > 2_097_152:
        raise ValidationError("observation manifest exceeds two MiB")
    write_json(path, manifest)


def _retained_candle_entries(manifest, root):
    parent = manifest.get("parent")
    if not parent:
        return []
    if type(parent) is not dict or "rows_sha256" not in parent:
        raise ValidationError("invalid candle parent lineage")
    path = _relative(root, "source.dataset.json")
    checksum = parent.get("manifest_sha256") or bounded_artifact_hash(path, max_bytes=2_097_152)
    return [
        {"file": "source.rows.csv", "sha256": parent["rows_sha256"]},
        {"file": "source.dataset.json", "sha256": checksum},
    ]


def _candle_evidence(root):
    manifest = read_status_json(root / "dataset.json")
    return [
        entry | {"file": "candles/" + entry["file"]}
        for entry in _retained_candle_entries(manifest, root)
    ]


def _snapshot_candles(source, destination, cancel=None):
    path = _manifest_path(source)
    manifest = read_status_json(path)
    entries = [manifest["rows"], manifest["source"]]
    if manifest.get("repair"):
        entries.append(manifest["repair"]["mask"])
        if manifest["repair"].get("source_mask"):
            entries.append(manifest["repair"]["source_mask"])
    entries.extend(_retained_candle_entries(manifest, path.parent))
    destination.mkdir(parents=True, exist_ok=False)
    seen = {}
    for entry in entries:
        check_cancel(cancel)
        name, expected = entry["file"], entry["sha256"]
        if name in seen:
            if seen[name] != expected:
                raise ValidationError("conflicting dataset references")
            continue
        seen[name] = expected
        target = _relative(destination, name)
        target.parent.mkdir(parents=True, exist_ok=True)
        if (
            copy_bounded_artifact(_relative(path.parent, name), target, max_bytes=MAX_FILE_BYTES)
            != expected
        ):
            raise ValidationError("candle dataset changed while snapshotting")
    _write_manifest(destination / "dataset.json", manifest)
    return load_dataset(destination, cancel)


def _verify_candles(path, cancel=None):
    manifest = read_status_json(path)
    entries = [manifest["rows"], manifest["source"]]
    if manifest.get("repair"):
        entries.append(manifest["repair"]["mask"])
        if manifest["repair"].get("source_mask"):
            entries.append(manifest["repair"]["source_mask"])
    entries.extend(_retained_candle_entries(manifest, path.parent))
    for entry in entries:
        check_cancel(cancel)
        if (
            bounded_artifact_hash(_relative(path.parent, entry["file"]), max_bytes=MAX_FILE_BYTES)
            != entry["sha256"]
        ):
            raise ValidationError("candle snapshot reference hash mismatch")
    return load_dataset(path, cancel)


def _units(observation, *, generating=False):
    if observation.channel == "depth" or observation.channel.startswith("depth_"):
        raise ValidationError("depth observations require a separately validated dataset contract")
    known = _CHANNEL_FIELDS.get(observation.channel, set())
    result = {}
    for name, value in observation.values:
        unit = _FIELDS.get(name) if name in known else None
        if generating and unit is None:
            raise ValidationError("Monte Carlo has no declared unit policy for this channel/field")
        result[name] = unit or "unspecified"
        if not generating:
            continue
        if (
            type(value) is int
            and value.bit_length() > 256
            or type(value) is Decimal
            and (len(value.as_tuple().digits) > 64 or abs(value.as_tuple().exponent) > 128)
        ):
            raise ValidationError("auxiliary scenario arithmetic exceeds its numeric bound")
        if unit in {"count", "milliseconds"}:
            if type(value) is not int or value < (1 if unit == "milliseconds" else 0):
                raise ValidationError("count/period observations require valid native integers")
        elif unit == "account_fraction" and not 0 <= value <= 1:
            raise ValidationError("account fractions must be within zero and one")
        elif unit and unit != "signed_rate" and value < 0:
            raise ValidationError("unsigned auxiliary amounts cannot be negative")
    if not generating:
        return result
    values = dict(observation.values)
    for side in ("BUY", "SELL"):
        base = values.get("AGGTRADE_QUANTITY" + side)
        quote = values.get("AGGTRADE_NOTIONAL" + side)
        count = values.get("AGGTRADE_NTRADES" + side)
        if base == 0 and quote not in {None, 0} or count == 0 and base not in {None, 0}:
            raise ValidationError("trade-flow quantity/count/notional are inconsistent")
        if quote == 0 and base not in {None, 0} or base == 0 and count not in {None, 0}:
            raise ValidationError("positive trade counts/quantity require positive source notional")
    if values.get("METRIC_OPENINTEREST") == 0 and values.get("METRIC_OPENINTERESTVALUE") not in {
        None,
        0,
    }:
        raise ValidationError("zero open-interest quantity cannot have a positive quote value")
    return result


def _quality(rows, records, *, generating=False):
    candles = {(row.instrument, row.open_time_ms): row for row in rows}
    compared = differences = zero_volume = invalid_units = price_outside = partial = 0
    maximum = Decimal(0)
    with localcontext() as context:
        context.prec, context.rounding = 34, ROUND_HALF_EVEN
        for stored in records:
            observation = stored.observation
            try:
                _units(observation, generating=True)
            except ValidationError:
                invalid_units += 1
                if generating:
                    raise
                continue
            if observation.channel not in {"trade_flow", "candle_statistics"}:
                continue
            if observation.quality != "observed" or observation.coverage != 1:
                partial += 1
                continue
            candle = candles[stored.instrument.key, observation.window_start_ms]
            fields = dict(observation.values)
            if observation.channel == "trade_flow":
                buy = fields.get("AGGTRADE_QUANTITYBUY")
                sell = fields.get("AGGTRADE_QUANTITYSELL")
                if buy is not None and sell is not None and buy >= 0 and sell >= 0:
                    compared += 1
                    amount = buy + sell
                    if amount != candle.volume:
                        differences += 1
                        if candle.volume:
                            maximum = max(maximum, abs(amount - candle.volume) / candle.volume)
                        else:
                            zero_volume += 1
                pairs = [
                    (fields.get("AGGTRADE_QUANTITY" + side), fields.get("AGGTRADE_NOTIONAL" + side))
                    for side in ("BUY", "SELL")
                ]
            else:
                pairs = [
                    (fields.get("KLINE_VOLBASETAKERBUY"), fields.get("KLINE_VOLQUOTETAKERBUY"))
                ]
            for base, quote in pairs:
                if base is None or quote is None or base <= 0 or quote < 0:
                    continue
                price = quote / base
                tolerance = max(candle.high, Decimal(1)) * Decimal("1E-24")
                if price < candle.low - tolerance or price > candle.high + tolerance:
                    price_outside += 1
        if generating and price_outside:
            raise ValidationError(
                "complete observed quote/base implied price is outside its same-minute OHLC range"
            )
    return {
        "unit_policy_unverified_rows": invalid_units,
        "complete_flow_volume_rows_compared": compared,
        "flow_volume_difference_rows": differences,
        "flow_volume_zero_candle_rows": zero_volume,
        "flow_volume_max_relative_difference": str(maximum),
        "known_implied_price_outside_ohlc": price_outside,
        "partial_flow_or_candle_statistics_rows": partial,
        "price_relative_tolerance": "1E-24",
        "volume_policy": (
            "difference reported, not forced equal: provider coverage/RPI definitions may differ"
        ),
        "consistency_scope": (
            "known units and source relationships only; not proof of provider-wide completeness"
        ),
    }


def _record(stored, origin=None):
    if not isinstance(stored, StoredAuxiliary):
        raise ValidationError("selected observations must be typed StoredAuxiliary records")
    payload = encode_observation(stored.instrument, stored.observation)
    if observation_checksum(payload) != stored.checksum:
        raise ValidationError("stored auxiliary checksum differs from its actual observation")
    _units(stored.observation)
    result = {
        "revision": stored.revision,
        "checksum": stored.checksum,
        "previous_checksum": stored.previous_checksum,
        "payload": json.loads(payload),
    }
    if origin is not None:
        result["origin"] = origin
    return result


def _decode(record):
    if type(record) is not dict or set(record) not in (_RECORD_KEYS, _RECORD_KEYS | {"origin"}):
        raise ValidationError("invalid observation record schema")
    payload = _json(record["payload"])
    instrument, observation = decode_observation(payload)
    value = StoredAuxiliary(
        instrument, observation, record["revision"], record["checksum"], record["previous_checksum"]
    )
    _record(value)
    return value


def _key(stored):
    row = stored.observation
    return stored.instrument.key, row.window_start_ms, row.channel, row.source


def _inventory(records):
    result = {}
    for stored in records:
        row = stored.observation
        entry = result.setdefault(row.channel, {"fields": {}, "sources": []})
        entry["fields"].update(_units(row))
        if row.source not in entry["sources"]:
            entry["sources"].append(row.source)
    for entry in result.values():
        entry["sources"].sort()
    return result


def _write_records(path, records, cancel=None):
    count, size = 0, 0
    with path.open("xb") as stream:
        for document in records:
            check_cancel(cancel)
            raw = _json(document) + b"\n"
            size += len(raw)
            if len(raw) > _MAX_RECORD_BYTES or size > MAX_FILE_BYTES or count >= MAX_OBSERVATIONS:
                raise ValidationError("bounded observation snapshot capacity exceeded")
            stream.write(raw)
            count += 1
        stream.flush()
        os.fsync(stream.fileno())
    return {
        "file": path.name,
        "count": count,
        "sha256": bounded_artifact_hash(path, max_bytes=MAX_FILE_BYTES),
    }


def _read_records(path, cancel=None):
    records, count = [], 0
    with _bounded_artifact_stream(path, MAX_FILE_BYTES) as stream:
        while raw := stream.readline(_MAX_RECORD_BYTES + 1):
            check_cancel(cancel)
            if len(raw) > _MAX_RECORD_BYTES or count >= MAX_OBSERVATIONS:
                raise ValidationError("bounded observation record capacity exceeded")
            document = json.loads(raw, object_pairs_hook=_unique, parse_constant=_invalid_constant)
            records.append((document, _decode(document)))
            count += 1
    return records


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValidationError("duplicate observation JSON field")
        value[key] = item
    return value


def _invalid_constant(_value):
    raise ValidationError("non-finite observation JSON value")


def _validate_rows(rows, records):
    candles = {(row.instrument, row.open_time_ms) for row in rows}
    seen = set()
    for stored in records:
        key = _key(stored)
        if key in seen or (key[0], key[1]) not in candles:
            raise ValidationError(
                "observations require one selected revision per existing candle/source"
            )
        seen.add(key)


def _raw_snapshot(raw_files, destination, cancel=None):
    if raw_files is None:
        return []
    if (
        type(raw_files) is not dict
        or len(raw_files) > MAX_RAW_FILES
        or any(type(name) is not str for name in raw_files)
    ):
        raise ValidationError("raw payload references exceed capacity")
    result, total = [], 0
    for name, source in sorted(raw_files.items()):
        check_cancel(cancel)
        target = _relative(destination, "raw/" + name)
        target.parent.mkdir(parents=True, exist_ok=True)
        checksum = copy_bounded_artifact(Path(source), target, max_bytes=MAX_FILE_BYTES)
        size = target.stat().st_size
        total += size
        if total > MAX_RAW_TOTAL_BYTES:
            raise ValidationError("raw payload snapshot total exceeds capacity")
        result.append({"id": name, "file": "raw/" + name, "sha256": checksum, "bytes": size})
    return result


def export_auxiliary_dataset(
    candle_dataset: Path,
    destination: Path,
    observations: Iterable[StoredAuxiliary],
    *,
    raw_files: dict[str, Path] | None = None,
    export_provenance: dict | None = None,
    cancel: Any = None,
    format_version: int = 1,
    source_ranges: tuple[AuxiliaryReadRange, ...] | None = None,
    metric_lookback_ms: int | None = None,
    as_of_ms: int | None = None,
) -> dict:
    """Freeze selected actual revisions and optional received bytes beside OHLCV v1.

    ``raw_files`` is an explicit archive catalog, not a claim that every selected
    normalized observation has a corresponding original exchange payload.
    """
    if type(format_version) is not int or format_version not in {1, 2}:
        raise ValidationError("unsupported native observation export format")
    if format_version == 2:
        return _export_native_v2(
            candle_dataset,
            destination,
            observations,
            raw_files=raw_files,
            export_provenance=export_provenance,
            cancel=cancel,
            source_ranges=source_ranges,
            metric_lookback_ms=metric_lookback_ms,
            as_of_ms=as_of_ms,
        )
    if source_ranges is not None or metric_lookback_ms is not None or as_of_ms is not None:
        raise ValidationError("V2 selection parameters require explicit format_version2")
    destination = validate_artifact_path(Path(destination).absolute(), allow_missing=True)
    destination.mkdir(parents=True, exist_ok=False)
    candles, rows = _snapshot_candles(candle_dataset, destination / "candles", cancel)
    selected = []
    for stored in observations:
        check_cancel(cancel)
        if len(selected) >= MAX_OBSERVATIONS:
            raise ValidationError("selected observation capacity exceeded")
        _record(stored)
        selected.append(stored)
    selected.sort(key=_key)
    _validate_rows(rows, selected)
    entry = _write_records(destination / "observations.jsonl", map(_record, selected), cancel)
    manifest = {
        "schema_version": 1,
        "schema_id": SCHEMA_ID,
        "state": "complete",
        "scope": "offline_market_observations",
        "dataset_kind": "selected_normalized_observations",
        "candles": {
            "manifest": "candles/dataset.json",
            "sha256": bounded_artifact_hash(destination / "candles/dataset.json"),
            "rows_sha256": candles["rows"]["sha256"],
        },
        "observations": entry,
        "candle_evidence": _candle_evidence(destination / "candles"),
        "channels": _inventory(selected),
        "quality": _quality(rows, selected),
        "raw_payloads": _raw_snapshot(raw_files, destination, cancel),
        "transform": None,
        "limitations": [
            "selected revisions, not the complete storage revision history",
            "raw payloads are supplied archive files; "
            "no coverage or observation-to-wire correspondence is invented",
            "sparse observations stay sparse; missing minutes are not zero-filled",
            "units refer to provider quantities; implied OI valuation need not equal candle close",
        ],
    }
    if export_provenance is not None:
        raw = _json(export_provenance)
        if len(raw) > 16384:
            raise ValidationError("observation export provenance exceeds capacity")
        manifest["export_provenance"] = {"data": json.loads(raw), "sha256": sha256(raw).hexdigest()}
        _validate_export_provenance(manifest, selected)
    _write_manifest(destination / "dataset.json", manifest)
    return manifest


def load_auxiliary_dataset(
    source: Path, cancel: Any = None
) -> tuple[dict, list[MarketRow], tuple[StoredAuxiliary, ...]]:
    """Verify actual snapshot files before exposing typed observations to a plugin."""
    manifest, rows, records = _load_bundle(source, cancel)
    return manifest, rows, tuple(stored for _, stored in records)


def freeze_selected_observation_snapshot(manifest, observations):
    """Copy and verify an original selected bundle's in-memory record identity.

    Call load_auxiliary_dataset/snapshot_auxiliary_dataset first to verify actual
    files. This pure boundary never opens references or relabels availability.
    Returned records are independent typed copies sorted as in the export.
    """
    if type(manifest) is dict and manifest.get("schema_id") == SCHEMA_ID_V2:
        return native_v2.freeze_snapshot(manifest, observations)
    if type(manifest) is not dict:
        raise ValidationError("selected observations require a bounded verified manifest")
    try:
        raw_manifest = _json(manifest)
        if len(raw_manifest) > 2_097_152:
            raise ValidationError("selected observation manifest exceeds two MiB")
        header = json.loads(raw_manifest)
    except (TypeError, ValueError, RecursionError):
        raise ValidationError(
            "selected observation manifest requires bounded JSON values"
        ) from None
    if (
        header.get("schema_id") != SCHEMA_ID
        or type(header.get("schema_version")) is not int
        or header["schema_version"] != 1
        or header.get("state") != "complete"
        or header.get("scope") != "offline_market_observations"
        or header.get("dataset_kind") != "selected_normalized_observations"
        or "transform" not in header
        or header["transform"] is not None
    ):
        raise ValidationError("selected snapshot accepts original bundles only, not MC/proxy")
    if type(observations) is not tuple or len(observations) > MAX_OBSERVATIONS:
        raise ValidationError("selected observations require a bounded immutable tuple")
    selected, seen = [], set()
    for stored in observations:
        if type(stored) is not StoredAuxiliary or type(stored.instrument) is not Instrument:
            raise ValidationError("selected observations require original typed source records")
        instrument, observation = decode_observation(
            encode_observation(stored.instrument, stored.observation)
        )
        copied = StoredAuxiliary(
            instrument, observation, stored.revision, stored.checksum, stored.previous_checksum
        )
        _record(copied)
        key = _key(copied)
        if key in seen:
            raise ValidationError("selected snapshot needs one revision per source minute")
        seen.add(key)
        selected.append(copied)
    selected.sort(key=_key)
    digest, byte_count = sha256(), 0
    for stored in selected:
        raw = _json(_record(stored)) + b"\n"
        digest.update(raw)
        byte_count += len(raw)
    try:
        reference = header["observations"]
        identities = (
            reference["sha256"],
            header["candles"]["sha256"],
            header["candles"]["rows_sha256"],
        )
        if any(
            type(value) is not str
            or len(value) != 64
            or any(letter not in "0123456789abcdef" for letter in value)
            for value in identities
        ):
            raise ValidationError("selected snapshot requires canonical SHA256 identities")
        if (
            type(reference["count"]) is not int
            or reference["count"] != len(selected)
            or reference["sha256"] != digest.hexdigest()
            or byte_count > MAX_FILE_BYTES
            or header["channels"] != _inventory(selected)
        ):
            raise ValidationError("selected observations differ from their manifest")
        _validate_export_provenance(header, selected)
    except (KeyError, TypeError):
        raise ValidationError("selected manifest lacks original record identities") from None
    return header, tuple(selected)


def _validate_export_provenance(manifest, selected):
    entry = manifest.get("export_provenance")
    if entry is None:
        return
    if type(entry) is not dict or set(entry) != {"data", "sha256"}:
        raise ValidationError("invalid observation export provenance")
    data = entry["data"]
    raw = _json(data)
    if (
        len(raw) > 16384
        or sha256(raw).hexdigest() != entry["sha256"]
        or type(data) is not dict
        or set(data) != {"schema_version", "scope", "origin", "as_of_ms", "selections"}
        or type(data["schema_version"]) is not int
        or data["schema_version"] != 1
        or data["scope"] != "retrospective_training_only"
        or data["origin"] != "committed_postgresql"
        or type(data["as_of_ms"]) is not int
        or not 0 <= data["as_of_ms"] <= 253402300799000
        or type(data["selections"]) is not list
        or not 1 <= len(data["selections"]) <= 8
    ):
        raise ValidationError("observation export provenance hash/schema mismatch")
    keys = set()
    for selection in data["selections"]:
        if type(selection) is not dict or set(selection) != {"instrument", "channel", "source"}:
            raise ValidationError("invalid retained observation selection")
        if any(
            type(value) is not str or not 1 <= len(value) <= 128 for value in selection.values()
        ):
            raise ValidationError("invalid retained observation source")
        key = tuple(selection[field] for field in ("instrument", "channel", "source"))
        if key in keys:
            raise ValidationError("duplicate retained observation selection")
        keys.add(key)
    if manifest["transform"] is None and any(
        (item.instrument.key, item.observation.channel, item.observation.source) not in keys
        or item.observation.available_at_ms > data["as_of_ms"]
        for item in selected
    ):
        raise ValidationError("retained export cutoff/source differs from actual observations")


def _load_bundle(source, cancel=None, *, parent=False):
    path = _manifest_path(source)
    manifest = read_status_json(path)
    if type(manifest) is dict and manifest.get("schema_id") == SCHEMA_ID_V2:
        if parent:
            raise ValidationError("native V2 cannot be a generated/MC parent")
        return _load_native_v2(path, manifest, cancel)
    if (
        type(manifest) is not dict
        or manifest.get("schema_id") != SCHEMA_ID
        or type(manifest.get("schema_version")) is not int
        or manifest["schema_version"] != 1
        or manifest.get("state") != "complete"
        or manifest.get("scope") != "offline_market_observations"
    ):
        raise ValidationError("unsupported/incomplete market observation dataset")
    expected_kind = (
        "selected_normalized_observations"
        if manifest.get("transform") is None
        else "generated_market_observations"
    )
    if manifest.get("dataset_kind") != expected_kind:
        raise ValidationError("observation dataset kind conflicts with its generation policy")
    root, candles = path.parent, manifest["candles"]
    evidence = manifest.get("candle_evidence")
    if type(evidence) is not list or len(evidence) > 4:
        raise ValidationError("invalid retained candle evidence catalog")
    for entry in evidence:
        if type(entry) is not dict or set(entry) != {"file", "sha256"}:
            raise ValidationError("invalid retained candle evidence reference")
        if (
            bounded_artifact_hash(_relative(root, entry["file"]), max_bytes=MAX_FILE_BYTES)
            != entry["sha256"]
        ):
            raise ValidationError("retained candle evidence hash mismatch")
    candle_path = _relative(root, candles["manifest"])
    if bounded_artifact_hash(candle_path) != candles["sha256"]:
        raise ValidationError("candle manifest hash mismatch")
    candle_manifest, rows = _verify_candles(candle_path, cancel)
    if _candle_evidence(candle_path.parent) != evidence:
        raise ValidationError("retained candle evidence catalog differs from its actual parent")
    if candle_manifest["rows"]["sha256"] != candles["rows_sha256"]:
        raise ValidationError("candle rows identity mismatch")
    entry = manifest["observations"]
    record_path = _relative(root, entry["file"])
    if bounded_artifact_hash(record_path, max_bytes=MAX_FILE_BYTES) != entry["sha256"]:
        raise ValidationError("auxiliary snapshot hash mismatch")
    records = _read_records(record_path, cancel)
    if type(entry["count"]) is not int or len(records) != entry["count"]:
        raise ValidationError("auxiliary snapshot row count mismatch")
    selected = [value for _, value in records]
    _validate_rows(rows, selected)
    _validate_export_provenance(manifest, selected)
    if _inventory(selected) != manifest["channels"]:
        raise ValidationError("auxiliary channel/unit catalog mismatch")
    if _json(_quality(rows, selected)) != _json(manifest.get("quality")):
        raise ValidationError("auxiliary quality diagnostics differ from actual rows")
    raw = manifest["raw_payloads"]
    if type(raw) is not list or len(raw) > MAX_RAW_FILES:
        raise ValidationError("raw payload catalog exceeds capacity")
    total, names = 0, set()
    for item in raw:
        check_cancel(cancel)
        if (
            type(item) is not dict
            or set(item) != {"id", "file", "sha256", "bytes"}
            or type(item["id"]) is not str
            or not 1 <= len(item["id"]) <= 256
            or type(item["bytes"]) is not int
            or not 0 <= item["bytes"] <= MAX_FILE_BYTES
            or item["id"] in names
        ):
            raise ValidationError("invalid raw payload identity")
        names.add(item["id"])
        actual = _relative(root, item["file"])
        if (
            bounded_artifact_hash(actual, max_bytes=MAX_FILE_BYTES) != item["sha256"]
            or actual.stat().st_size != item["bytes"]
        ):
            raise ValidationError("raw payload hash/size mismatch")
        total += item["bytes"]
    if total > MAX_RAW_TOTAL_BYTES:
        raise ValidationError("raw payload total exceeds capacity")
    if manifest["transform"] is not None:
        if parent:
            raise ValidationError("resampling an already generated bundle is unsupported")
        transform = manifest["transform"]
        if type(transform) is not dict or transform.get("id") != "joint-relative-block-auxiliary":
            raise ValidationError("unknown auxiliary generation policy")
        if (
            type(transform.get("version")) is not int
            or transform["version"] != 1
            or transform.get("scope") != "synthetic_research_only"
            or any(
                type(transform.get(key)) is not int
                for key in ("seed", "scenario_index", "block_bars")
            )
            or any(
                transform.get(key) != candle_manifest["transform"].get(key)
                for key in ("seed", "scenario_index", "block_bars", "splits")
            )
        ):
            raise ValidationError("auxiliary and candle Monte Carlo policies differ")
        parent_path = _relative(root, transform["parent_manifest"])
        if bounded_artifact_hash(parent_path) != transform["parent_sha256"]:
            raise ValidationError("parent observation manifest hash mismatch")
        original, source_rows, source_records = _load_bundle(parent_path, cancel, parent=True)
        if manifest.get("export_provenance") != original.get("export_provenance"):
            raise ValidationError("generated observation export provenance differs from parent")
        source_candles = read_status_json(
            _relative(parent_path.parent, original["candles"]["manifest"])
        )
        if (
            source_candles["splits"] != candle_manifest["splits"]
            or candle_manifest["parent"]["rows_sha256"] != original["candles"]["rows_sha256"]
            or candle_manifest["parent"]["manifest_sha256"] != original["candles"]["sha256"]
        ):
            raise ValidationError("generated candle lineage differs from its actual parent dataset")
        _validate_generated(records, rows, source_rows, source_records, candle_manifest)
    elif any("origin" in document for document, _ in records):
        raise ValidationError("original observations cannot claim a generated origin")
    return manifest, rows, records


def _source_mapping(source_rows, generated_rows, candle_manifest):
    source = {(row.instrument, row.open_time_ms): row for row in source_rows}
    targets = {}
    policy = candle_manifest["transform"]
    if type(policy) is not dict or policy.get("id") != "joint-relative-block":
        raise ValidationError("joint auxiliary generation needs the declared OHLCV block mapping")
    for split in SPLITS:
        selected = select_split(generated_rows, candle_manifest["splits"], split)
        times = sorted({row.open_time_ms for row in selected})
        mapped = policy["provenance"].get(split, {}).get("source_timestamps", times)
        if len(mapped) != len(times):
            raise ValidationError("joint auxiliary block mapping length mismatch")
        for target, original in zip(times, mapped, strict=True):
            begin, end = candle_manifest["splits"][split]
            if type(original) is not int or original % 60000 or not begin <= original < end:
                raise ValidationError("joint source mapping crossed its frozen split")
            targets[target] = original, split, split in policy["splits"]
    result = []
    with localcontext() as context:
        context.prec, context.rounding = 34, ROUND_HALF_EVEN
        for row in generated_rows:
            original, split, synthetic = targets[row.open_time_ms]
            before = source[row.instrument, original]
            result.append((row, original, split, synthetic, row.close / before.close))
    return result


def _generated_record(stored, target, split, synthetic, scale):
    observation = stored.observation
    units = _units(observation, generating=True)
    if (
        observation.quality == "carried"
        or not observation.window_start_ms <= observation.event_time_ms <= observation.window_end_ms
    ):
        raise ValidationError("joint generation does not infer carried or out-of-window samples")
    origin = {
        "source_checksum": stored.checksum,
        "source_revision": stored.revision,
        "window_start_ms": observation.window_start_ms,
        "event_time_ms": observation.event_time_ms,
        "available_at_ms": observation.available_at_ms,
        "price_scale": str(scale),
        "split": split,
        "synthetic": synthetic,
    }
    if not synthetic:
        return _record(stored, origin)
    with localcontext() as context:
        context.prec, context.rounding = 34, ROUND_HALF_EVEN
        values = tuple(
            (name, value * scale if units[name] == "quote_notional" else value)
            for name, value in observation.values
        )
    offset = target.open_time_ms - observation.window_start_ms
    generated = replace(
        observation,
        values=values,
        window_start_ms=target.open_time_ms,
        window_end_ms=target.open_time_ms + 60000,
        event_time_ms=observation.event_time_ms + offset,
        available_at_ms=observation.available_at_ms + offset,
        source="research_mc:" + sha256(observation.source.encode()).hexdigest()[:32],
    )
    checksum = observation_checksum(encode_observation(stored.instrument, generated))
    return _record(StoredAuxiliary(stored.instrument, generated, 1, checksum, None), origin)


def _generated_documents(source_rows, generated_rows, records, candle_manifest, cancel=None):
    by_time = defaultdict(list)
    for _, stored in records:
        by_time[stored.instrument.key, stored.observation.window_start_ms].append(stored)
    result = []
    for row, source_time, split, synthetic, scale in _source_mapping(
        source_rows, generated_rows, candle_manifest
    ):
        check_cancel(cancel)
        for stored in by_time[row.instrument, source_time]:
            result.append(_generated_record(stored, row, split, synthetic, scale))
    result.sort(key=lambda record: _key(_decode(record)))
    return result


def _validate_generated(records, rows, source_rows, source_records, candle_manifest):
    expected = _generated_documents(source_rows, rows, source_records, candle_manifest)
    if len(expected) != len(records) or any(
        _json(before) != _json(after[0]) for before, after in zip(expected, records, strict=True)
    ):
        raise ValidationError("generated observations differ from their joint source/unit lineage")


def _snapshot_bundle(source, destination, cancel=None, *, allow_existing=False):
    path = _manifest_path(source)
    manifest, _, _ = _load_bundle(path, cancel)
    destination.mkdir(parents=True, exist_ok=allow_existing)
    _snapshot_candles(
        _relative(path.parent, manifest["candles"]["manifest"]), destination / "candles", cancel
    )
    references = [manifest["observations"], *manifest["raw_payloads"]]
    for entry in references:
        check_cancel(cancel)
        target = _relative(destination, entry["file"])
        target.parent.mkdir(parents=True, exist_ok=True)
        if allow_existing and target.exists():
            if bounded_artifact_hash(target, max_bytes=MAX_FILE_BYTES) != entry["sha256"]:
                raise ValidationError("conflicting observation snapshot references")
            continue
        if (
            copy_bounded_artifact(
                _relative(path.parent, entry["file"]), target, max_bytes=MAX_FILE_BYTES
            )
            != entry["sha256"]
        ):
            raise ValidationError("observation bundle changed while snapshotting")
    # The copied candle manifest has identical canonical bytes to its source.
    manifest = manifest | {
        "candles": manifest["candles"]
        | {"sha256": bounded_artifact_hash(destination / "candles/dataset.json")}
    }
    _write_manifest(destination / "dataset.json", manifest)
    return manifest


def snapshot_auxiliary_dataset(source, destination, cancel=None):
    """Copy verified selected files, including a generated scenario's original parent."""
    path = _manifest_path(source)
    destination = validate_artifact_path(Path(destination).absolute(), allow_missing=True)
    manifest, _, _ = _load_bundle(path, cancel)
    if manifest.get("transform"):
        destination.mkdir(parents=True, exist_ok=False)
        parent = manifest["transform"]["parent_manifest"]
        parent_source = _relative(path.parent, parent)
        parent_target = _relative(destination, parent)
        _snapshot_bundle(parent_source, parent_target.parent, cancel)
        if bounded_artifact_hash(parent_target) != manifest["transform"]["parent_sha256"]:
            raise ValidationError("generated observation parent changed while snapshotting")
        manifest = _snapshot_bundle(path, destination, cancel, allow_existing=True)
    else:
        manifest = _snapshot_bundle(path, destination, cancel)
    load_auxiliary_dataset(destination, cancel)
    return manifest


def monte_carlo_auxiliary(
    source: Path, destination: Path, parameters: dict, cancel: Any = None
) -> dict:
    """Apply one frozen OHLCV block mapping to all supplied auxiliary channels.

    Base/contract quantities, counts, ratios and funding rates are inherited.
    Quote notionals scale with price, preserving their source implied valuation
    relative to the sampled candle, not asserting equality with its close/mark.
    Actual source times remain in each origin and the complete parent bundle.
    """
    manifest, source_rows, records = _load_bundle(source, cancel)
    if manifest.get("schema_id") == SCHEMA_ID_V2:
        raise ValidationError("native V2 metric/depth Monte Carlo needs a separate policy")
    if manifest["transform"] is not None:
        raise ValidationError("resample an original market observation bundle, not a generated one")
    for _, stored in records:
        _units(stored.observation, generating=True)
        if stored.observation.quality == "carried":
            raise ValidationError(
                "carried auxiliary observations require a separate resampling policy"
            )
    _quality(source_rows, [stored for _, stored in records], generating=True)
    destination = validate_artifact_path(Path(destination).absolute(), allow_missing=True)
    destination.mkdir(parents=True, exist_ok=False)
    original = _snapshot_bundle(source, destination / "source", cancel)
    candles = monte_carlo(
        destination / "source/candles", destination / "candles", parameters, cancel
    )
    _, generated_rows = _verify_candles(destination / "candles/dataset.json", cancel)
    documents = _generated_documents(source_rows, generated_rows, records, candles, cancel)
    selected = [_decode(document) for document in documents]
    quality = _quality(generated_rows, selected, generating=True)
    entry = _write_records(destination / "observations.jsonl", documents, cancel)
    generated = {
        **manifest,
        "dataset_kind": "generated_market_observations",
        "candles": {
            "manifest": "candles/dataset.json",
            "sha256": bounded_artifact_hash(destination / "candles/dataset.json"),
            "rows_sha256": candles["rows"]["sha256"],
        },
        "observations": entry,
        "candle_evidence": _candle_evidence(destination / "candles"),
        "channels": _inventory(selected),
        "quality": quality,
        "raw_payloads": [
            row | {"file": "source/" + row["file"]} for row in original["raw_payloads"]
        ],
        "transform": {
            "id": "joint-relative-block-auxiliary",
            "version": 1,
            "parent_manifest": "source/dataset.json",
            "parent_sha256": bounded_artifact_hash(destination / "source/dataset.json"),
            "seed": candles["transform"]["seed"],
            "scenario_index": candles["transform"]["scenario_index"],
            "block_bars": candles["transform"]["block_bars"],
            "splits": candles["transform"]["splits"],
            "scope": "synthetic_research_only",
            "availability": (
                "source offsets remapped; original actual timestamps retained in every origin"
            ),
            "price_policy": (
                "quote notionals follow price scale; source implied valuation/close ratio "
                "preserved, not exchange mark equality"
            ),
            "volume_policy": (
                "same source block; base/contract quantities, counts, ratios "
                "and funding rates unchanged"
            ),
            "funding_policy": (
                "resampled rate observations only; not a generated funding settlement "
                "calendar or cash-flow schedule"
            ),
            "source_period_policy": (
                "provider period metadata belongs to the original sample, "
                "not the cadence of generated samples"
            ),
            "derived_features": "must_recompute",
            "unsupported": ["depth", "unknown channels/fields", "carried samples"],
        },
    }
    _write_manifest(destination / "dataset.json", generated)
    load_auxiliary_dataset(destination, cancel)
    return generated


def _native_v2_quality(selected):
    counts = defaultdict(int)
    for stored in selected:
        counts[stored.observation.quality] += 1
    return {
        "scope": "normalized_retained_revision_quality",
        "revision_quality_counts": dict(sorted(counts.items())),
        "history": "bounded retained records; not provider/raw completeness",
        "outside_candle_keys": "range evidence only; never additional tradable candles",
    }


def _native_v2_ranges(selection):
    return tuple(
        AuxiliaryReadRange(
            native_v2.instrument_key(row["instrument"]),
            row["channel"],
            row["source"],
            row["start_ms"],
            row["end_ms"],
        )
        for row in selection["ranges"]
    )


def _export_native_v2(
    candle_dataset,
    destination,
    observations,
    *,
    raw_files,
    export_provenance,
    cancel,
    source_ranges,
    metric_lookback_ms,
    as_of_ms,
):
    destination = validate_artifact_path(Path(destination).absolute(), allow_missing=True)
    destination.mkdir(parents=True, exist_ok=False)
    candles, rows = _snapshot_candles(candle_dataset, destination / "candles", cancel)
    if candles.get("transform") is not None:
        raise ValidationError("native V2 requires original candles, not MC/proxy")
    selection = native_v2.selection_document(
        source_ranges, rows, as_of_ms=as_of_ms, metric_lookback_ms=metric_lookback_ms
    )
    selected = []
    total_bytes = 0
    for stored in observations:
        check_cancel(cancel)
        if len(selected) >= native_v2.MAX_RECORDS:
            raise ValidationError("V2 retained revision capacity exceeded")
        total_bytes += len(native_v2.canonical_json(native_v2.record(stored))) + 1
        if total_bytes > native_v2.MAX_BYTES:
            raise ValidationError("V2 retained revision bytes exceed32MiB")
        selected.append(stored)
    selected = native_v2.validate_records(selection, tuple(selected))
    entry = _write_records(
        destination / "observations.jsonl", map(native_v2.record, selected), cancel
    )
    manifest = {
        "schema_version": 2,
        "schema_id": SCHEMA_ID_V2,
        "state": "complete",
        "scope": "offline_market_observations",
        "dataset_kind": "retained_native_observation_revisions",
        "candles": {
            "manifest": "candles/dataset.json",
            "sha256": bounded_artifact_hash(destination / "candles/dataset.json"),
            "rows_sha256": candles["rows"]["sha256"],
        },
        "observations": entry,
        "candle_evidence": _candle_evidence(destination / "candles"),
        "selection": selection,
        "channels": native_v2.inventory(selection, selected),
        "quality": _native_v2_quality(selected),
        "raw_payloads": _raw_snapshot(raw_files, destination, cancel),
        "transform": None,
        "limitations": [
            "retained source-range revisions known by export cutoff; "
            "no provider completeness proof",
            "range observations at absent OHLCV minutes are nontradable evidence only",
            "actual native availability is never moved to historical close",
            "normalized depth profile retained; "
            "original producer implementation/raw links were not stored",
            "raw_payloads is only an optional supplied archive catalog; "
            "not observation-to-wire correspondence",
            "metric/depth MC and retrospective proxy scenarios are unsupported",
        ],
    }
    if export_provenance is not None:
        raw = _json(export_provenance)
        if len(raw) > 16384:
            raise ValidationError("observation export provenance exceeds capacity")
        manifest["export_provenance"] = {"data": json.loads(raw), "sha256": sha256(raw).hexdigest()}
        _validate_export_provenance(manifest, selected)
        if export_provenance.get("as_of_ms") != as_of_ms:
            raise ValidationError("V2 provenance cutoff differs from retained selection")
    native_v2.freeze_snapshot(manifest, selected)
    _write_manifest(destination / "dataset.json", manifest)
    return manifest


def _read_native_v2_records(path, cancel):
    records = []
    with _bounded_artifact_stream(path, MAX_FILE_BYTES) as stream:
        while raw := stream.readline(_MAX_RECORD_BYTES + 1):
            check_cancel(cancel)
            if len(raw) > _MAX_RECORD_BYTES or len(records) >= native_v2.MAX_RECORDS:
                raise ValidationError("V2 retained record capacity exceeded")
            document = json.loads(raw, object_pairs_hook=_unique, parse_constant=_invalid_constant)
            records.append((document, native_v2.decode_record(document)))
    return records


def _load_native_v2(path, manifest, cancel):
    root = path.parent
    candles = manifest["candles"]
    candle_path = _relative(root, candles["manifest"])
    if bounded_artifact_hash(candle_path) != candles["sha256"]:
        raise ValidationError("V2 candle manifest hash mismatch")
    candle_manifest, rows = _verify_candles(candle_path, cancel)
    if (
        candle_manifest.get("transform") is not None
        or candle_manifest["rows"]["sha256"] != candles["rows_sha256"]
        or _candle_evidence(candle_path.parent) != manifest.get("candle_evidence")
    ):
        raise ValidationError("V2 candle evidence differs from its original snapshot")
    for entry in manifest["candle_evidence"]:
        if (
            type(entry) is not dict
            or set(entry) != {"file", "sha256"}
            or bounded_artifact_hash(_relative(root, entry["file"]), max_bytes=MAX_FILE_BYTES)
            != entry["sha256"]
        ):
            raise ValidationError("V2 retained candle evidence hash mismatch")
    entry = manifest["observations"]
    record_path = _relative(root, entry["file"])
    if bounded_artifact_hash(record_path, max_bytes=MAX_FILE_BYTES) != entry["sha256"]:
        raise ValidationError("V2 retained record file hash mismatch")
    records = _read_native_v2_records(record_path, cancel)
    header, selected = native_v2.freeze_snapshot(manifest, tuple(value for _, value in records))
    expected = native_v2.selection_document(
        _native_v2_ranges(header["selection"]),
        rows,
        as_of_ms=header["selection"]["as_of_ms"],
        metric_lookback_ms=header["selection"]["metric_lookback_ms"],
    )
    if expected != header["selection"] or _native_v2_quality(selected) != header.get("quality"):
        raise ValidationError("V2 declared ranges/quality differ from actual candle/record content")
    _validate_export_provenance(header, selected)
    provenance = header.get("export_provenance")
    if provenance is not None and provenance["data"]["as_of_ms"] != header["selection"]["as_of_ms"]:
        raise ValidationError("V2 export provenance and selection cutoffs differ")
    raw = header["raw_payloads"]
    if type(raw) is not list or len(raw) > MAX_RAW_FILES:
        raise ValidationError("V2 raw archive catalog exceeds capacity")
    total, names = 0, set()
    for item in raw:
        check_cancel(cancel)
        if (
            type(item) is not dict
            or set(item) != {"id", "file", "sha256", "bytes"}
            or type(item["id"]) is not str
            or not 1 <= len(item["id"]) <= 256
            or item["id"] in names
            or type(item["bytes"]) is not int
            or not 0 <= item["bytes"] <= MAX_FILE_BYTES
        ):
            raise ValidationError("invalid V2 raw archive identity")
        actual = _relative(root, item["file"])
        if (
            bounded_artifact_hash(actual, max_bytes=MAX_FILE_BYTES) != item["sha256"]
            or actual.stat().st_size != item["bytes"]
        ):
            raise ValidationError("V2 raw archive hash/size differs")
        names.add(item["id"])
        total += item["bytes"]
    if total > MAX_RAW_TOTAL_BYTES:
        raise ValidationError("V2 raw archive total exceeds capacity")
    return header, rows, records
