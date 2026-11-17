"""Bounded native V2 selection/record contract; no file, database or replay execution."""

import json
from collections import defaultdict
from decimal import Decimal
from hashlib import sha256

from omega.application.auxiliary_codec import decode_observation, encode_observation
from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market
from omega.ports.auxiliary import AuxiliaryReadRange, StoredAuxiliary

SCHEMA_ID = "omega.market-observations.1m.v2"
MAX_RECORDS = 10000
MAX_BYTES = 32 * 1024 * 1024
MAX_TIME = 253402300799000
METRIC_CHANNELS = frozenset({"open_interest", "long_short_ratio"})
DEPTH_BINS = tuple(f"DEPTH_{side}{index}" for side in ("BIDS", "ASKS") for index in range(6))
SOURCES = {
    "trade_flow": "aggTrade",
    "open_interest": "open_interest_history",
    "long_short_ratio": "long_short_ratio",
    "depth": "binance_depth_ws_v1",
}
UNITS = {
    **{
        f"AGGTRADE_{kind}{side}": unit
        for kind, unit in (
            ("QUANTITY", "base_quantity"),
            ("NOTIONAL", "quote_notional"),
            ("NTRADES", "count"),
        )
        for side in ("BUY", "SELL")
    },
    "METRIC_OPENINTEREST": "contract_quantity",
    "METRIC_OPENINTERESTVALUE": "quote_notional",
    "METRIC_LONGSHORTRATIO": "ratio",
    "METRIC_LONGACCOUNT": "account_fraction",
    "METRIC_SHORTACCOUNT": "account_fraction",
    "SOURCE_PERIOD_MS": "milliseconds",
    **dict.fromkeys(DEPTH_BINS, "quote_notional"),
    "DEPTH_CENTER_PRICE": "quote_per_base",
    "DEPTH_UPDATE_ID": "count",
    "DEPTH_GENERATION": "generation",
    "DEPTH_RECEIVED_AT_MS": "timestamp_ms",
    "DEPTH_PROFILE_VERSION": "profile_version",
    "DEPTH_EVENT_CLOCK": "event_clock",
}
CHANNEL_FIELDS = {
    "trade_flow": {key for key in UNITS if key.startswith("AGGTRADE_")},
    "open_interest": {"METRIC_OPENINTEREST", "METRIC_OPENINTERESTVALUE", "SOURCE_PERIOD_MS"},
    "long_short_ratio": {
        "METRIC_LONGSHORTRATIO",
        "METRIC_LONGACCOUNT",
        "METRIC_SHORTACCOUNT",
        "SOURCE_PERIOD_MS",
    },
    "depth": {key for key in UNITS if key.startswith("DEPTH_")},
}


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def depth_profile():
    contract = {
        "id": "omega.binance.depth-normalized.v1",
        "version": 1,
        "source_suffix": "binance_depth_ws_v1",
        "radii": [".002", ".01", ".02", ".03", ".04", ".05"],
        "aggregation": "latest_snapshot",
        "rounding": "ROUND_HALF_EVEN",
        "unknown_bins": "omitted_not_zero",
    }
    return {
        **contract,
        "contract_sha256": sha256(canonical_json(contract)).hexdigest(),
        "producer_implementation": "not_retained_by_normalized_store",
    }


def instrument_key(value):
    try:
        if type(value) is not str:
            raise ValueError
        provider, market, symbol = value.split(":")
        result = Instrument(provider, Market(market), symbol)
    except (TypeError, ValueError):
        raise ValidationError("native V2 needs canonical instrument identities") from None
    if (
        result.key != value
        or provider != "binance"
        or result.market not in {Market.SPOT, Market.USDM}
    ):
        raise ValidationError("native V2 supports public Binance spot/USDM only")
    return result


def _time(value):
    return type(value) is int and 0 <= value <= MAX_TIME


def selection_document(ranges, candle_rows, *, as_of_ms, metric_lookback_ms):
    if (
        type(ranges) is not tuple
        or not 1 <= len(ranges) <= 8
        or any(type(row) is not AuxiliaryReadRange for row in ranges)
        or not _time(as_of_ms)
    ):
        raise ValidationError("V2 needs1..8 bounded native source ranges and export cutoff")
    has_metric = any(row.channel in METRIC_CHANNELS for row in ranges)
    if (
        has_metric
        and (type(metric_lookback_ms) is not int or not 0 <= metric_lookback_ms <= 86400000)
        or not has_metric
        and metric_lookback_ms is not None
    ):
        raise ValidationError("V2 metric lookback must be explicit within0..86400000")
    groups = defaultdict(list)
    for row in candle_rows:
        groups[row.instrument].append(row.open_time_ms)
    candle_ranges = [
        {
            "instrument": key,
            "start_ms": min(values),
            "end_ms": max(values) + 60000,
            "count": len(values),
        }
        for key, values in sorted(groups.items())
    ]
    document = {
        "version": 1,
        "revision_coverage": "all_retained_revisions_known_by_export_cutoff",
        "as_of_ms": as_of_ms,
        "metric_lookback_ms": metric_lookback_ms,
        "candle_ranges": candle_ranges,
        "ranges": [
            {
                "instrument": row.instrument.key,
                "channel": row.channel,
                "source": row.source,
                "start_ms": row.start_ms,
                "end_ms": row.end_ms,
            }
            for row in sorted(
                ranges,
                key=lambda row: (
                    row.instrument.key,
                    row.channel,
                    row.source,
                    row.start_ms,
                    row.end_ms,
                ),
            )
        ],
        "depth_profile": depth_profile() if any(row.channel == "depth" for row in ranges) else None,
    }
    return validate_selection(document)


def validate_selection(value):
    if (
        type(value) is not dict
        or set(value)
        != {
            "version",
            "revision_coverage",
            "as_of_ms",
            "metric_lookback_ms",
            "candle_ranges",
            "ranges",
            "depth_profile",
        }
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["revision_coverage"] != "all_retained_revisions_known_by_export_cutoff"
        or not _time(value["as_of_ms"])
    ):
        raise ValidationError("invalid V2 retained selection policy")
    if (
        type(value["candle_ranges"]) is not list
        or not 1 <= len(value["candle_ranges"]) <= 100
        or type(value["ranges"]) is not list
        or not 1 <= len(value["ranges"]) <= 8
    ):
        raise ValidationError("V2 range catalog exceeds bounds")
    candles = {}
    for row in value["candle_ranges"]:
        if type(row) is not dict or set(row) != {"instrument", "start_ms", "end_ms", "count"}:
            raise ValidationError("invalid V2 candle range")
        instrument_key(row["instrument"])
        if (
            row["instrument"] in candles
            or not _time(row["start_ms"])
            or not _time(row["end_ms"])
            or not row["start_ms"] < row["end_ms"] <= value["as_of_ms"]
            or row["start_ms"] % 60000
            or row["end_ms"] % 60000
            or type(row["count"]) is not int
            or not 1 <= row["count"] <= (row["end_ms"] - row["start_ms"]) // 60000
        ):
            raise ValidationError("V2 candle range differs from bounded closed minutes")
        candles[row["instrument"]] = row
    seen, slots, has_metric, has_depth = set(), 0, False, False
    for row in value["ranges"]:
        if (
            type(row) is not dict
            or set(row) != {"instrument", "channel", "source", "start_ms", "end_ms"}
            or type(row["instrument"]) is not str
            or row["instrument"] not in candles
        ):
            raise ValidationError("invalid V2 native range identity")
        instrument = instrument_key(row["instrument"])
        channel = row["channel"]
        if (
            type(channel) is not str
            or channel not in SOURCES
            or row["source"] != instrument.key + ":" + SOURCES[channel]
            or channel in METRIC_CHANNELS
            and instrument.market is not Market.USDM
        ):
            raise ValidationError("V2 source/channel is not native for this venue")
        AuxiliaryReadRange(instrument, channel, row["source"], row["start_ms"], row["end_ms"])
        identity = (instrument.key, channel, row["source"])
        if identity in seen:
            raise ValidationError("V2 source ranges repeat or overlap")
        seen.add(identity)
        metric = channel in METRIC_CHANNELS
        has_metric |= metric
        has_depth |= channel == "depth"
        lookback = value["metric_lookback_ms"]
        if metric and (type(lookback) is not int or not 0 <= lookback <= 86400000):
            raise ValidationError("V2 needs an explicit metric lookback")
        expected_start = (
            max(0, candles[instrument.key]["start_ms"] - (lookback if metric else 0))
            // 60000
            * 60000
        )
        if row["start_ms"] != expected_start or row["end_ms"] != candles[instrument.key]["end_ms"]:
            raise ValidationError("V2 native range differs from its declared candle/seed coverage")
        slots += (row["end_ms"] - row["start_ms"]) // 60000
    if (
        slots > 10000
        or not has_metric
        and value["metric_lookback_ms"] is not None
        or canonical_json(value["depth_profile"])
        != canonical_json(depth_profile() if has_depth else None)
    ):
        raise ValidationError("V2 coverage/profile is unsupported or exceeds10000 slots")
    if value["ranges"] != sorted(
        value["ranges"],
        key=lambda row: tuple(
            row[key] for key in ("instrument", "channel", "source", "start_ms", "end_ms")
        ),
    ) or value["candle_ranges"] != sorted(
        value["candle_ranges"], key=lambda row: row["instrument"]
    ):
        raise ValidationError("V2 range policy is not canonical")
    return json.loads(canonical_json(value))


def observation_units(stored):
    row = stored.observation
    instrument = stored.instrument
    channel = row.channel
    instrument_key(instrument.key)
    if (
        channel not in SOURCES
        or row.source != instrument.key + ":" + SOURCES[channel]
        or channel in METRIC_CHANNELS
        and instrument.market is not Market.USDM
        or row.quality == "carried"
    ):
        raise ValidationError("V2 normalized record is not a supported native source")
    values = row.mapping()
    if values.keys() - CHANNEL_FIELDS[channel]:
        raise ValidationError("V2 has no declared units for unknown fields")
    for name, number in values.items():
        unit = UNITS[name]
        if unit in {
            "count",
            "milliseconds",
            "generation",
            "timestamp_ms",
            "profile_version",
            "event_clock",
        }:
            if (
                type(number) is not int
                or not 0 <= number <= (MAX_TIME if unit == "timestamp_ms" else 2**63 - 1)
                or unit in {"milliseconds", "generation"}
                and number == 0
            ):
                raise ValidationError("V2 metadata requires native bounded nonnegative integers")
        elif (
            type(number) is not Decimal
            or not number.is_finite()
            or number < 0
            or unit == "account_fraction"
            and number > 1
        ):
            raise ValidationError(
                "V2 normalized amounts require finite nonnegative native Decimals"
            )
    if channel in METRIC_CHANNELS:
        required = "METRIC_OPENINTEREST" if channel == "open_interest" else "METRIC_LONGSHORTRATIO"
        if (
            row.quality != "observed"
            or required not in values
            or row.window_start_ms != row.event_time_ms // 60000 * 60000
        ):
            raise ValidationError("V2 native point lacks its actual source minute/value")
    elif channel == "trade_flow":
        if (
            row.event_time_ms != row.window_end_ms
            or row.quality in {"observed", "partial"}
            and not {"AGGTRADE_NOTIONALBUY", "AGGTRADE_NOTIONALSELL"} <= values.keys()
        ):
            raise ValidationError("V2 flow differs from a finalized native minute")
    else:
        if (
            not (
                row.window_start_ms <= row.event_time_ms < row.window_end_ms
                or row.quality in {"missing", "unsupported"}
                and row.event_time_ms == row.window_end_ms
            )
            or row.available_at_ms < row.window_end_ms
        ):
            raise ValidationError("V2 depth differs from an actually finalized native snapshot")
        if row.quality in {"observed", "partial"}:
            metadata = {
                "DEPTH_CENTER_PRICE",
                "DEPTH_UPDATE_ID",
                "DEPTH_GENERATION",
                "DEPTH_RECEIVED_AT_MS",
                "DEPTH_PROFILE_VERSION",
                "DEPTH_EVENT_CLOCK",
            }
            if (
                not metadata <= values.keys()
                or values["DEPTH_CENTER_PRICE"] <= 0
                or values["DEPTH_PROFILE_VERSION"] != 1
                or values["DEPTH_EVENT_CLOCK"] != (1 if instrument.market is Market.USDM else 0)
                or values["DEPTH_RECEIVED_AT_MS"] > row.available_at_ms
                or row.quality == "observed"
                and (not set(DEPTH_BINS) <= values.keys() or row.coverage != 1)
            ):
                raise ValidationError("V2 depth profile/metadata/bin coverage differs")
    return {name: UNITS[name] for name in values}


def record(stored):
    if type(stored) is not StoredAuxiliary or type(stored.instrument) is not Instrument:
        raise ValidationError("V2 needs typed retained native records")
    payload = encode_observation(stored.instrument, stored.observation)
    instrument, observation = decode_observation(payload)
    native = StoredAuxiliary(
        instrument, observation, stored.revision, stored.checksum, stored.previous_checksum
    )
    if sha256(payload).hexdigest() != native.checksum:
        raise ValidationError("V2 native payload checksum differs")
    observation_units(native)
    return {
        "revision": native.revision,
        "checksum": native.checksum,
        "previous_checksum": native.previous_checksum,
        "payload": json.loads(payload),
    }


def decode_record(document):
    if type(document) is not dict or set(document) != {
        "revision",
        "checksum",
        "previous_checksum",
        "payload",
    }:
        raise ValidationError("invalid V2 retained native record")
    instrument, observation = decode_observation(canonical_json(document["payload"]))
    stored = StoredAuxiliary(
        instrument,
        observation,
        document["revision"],
        document["checksum"],
        document["previous_checksum"],
    )
    if record(stored) != document:
        raise ValidationError("V2 record is not canonical")
    return stored


def record_key(stored):
    row = stored.observation
    return stored.instrument.key, row.window_start_ms, row.channel, row.source, stored.revision


def validate_records(selection, observations):
    policy = validate_selection(selection)
    if type(observations) is not tuple or len(observations) > MAX_RECORDS:
        raise ValidationError("V2 retained revision count exceeds10000")
    spans = {(row["instrument"], row["channel"], row["source"]): row for row in policy["ranges"]}
    selected = []
    groups = defaultdict(list)
    total_bytes = 0
    for stored in observations:
        document = record(stored)
        total_bytes += len(canonical_json(document)) + 1
        if total_bytes > MAX_BYTES:
            raise ValidationError("V2 retained revision bytes exceed32MiB")
        copied = decode_record(document)
        row = copied.observation
        span = spans.get((copied.instrument.key, row.channel, row.source))
        if (
            span is None
            or not span["start_ms"] <= row.window_start_ms < span["end_ms"]
            or row.available_at_ms > policy["as_of_ms"]
        ):
            raise ValidationError("V2 record lies outside the declared source/cutoff coverage")
        selected.append(copied)
        groups[record_key(copied)[:-1]].append(copied)
    selected.sort(key=record_key)
    for group in groups.values():
        prior = None
        for expected, stored in enumerate(sorted(group, key=lambda item: item.revision), 1):
            if (
                stored.revision != expected
                or prior is not None
                and (
                    stored.previous_checksum != prior.checksum
                    or stored.observation.available_at_ms <= prior.observation.available_at_ms
                )
            ):
                raise ValidationError("V2 retained source revision chain is missing/conflicting")
            prior = stored
    return tuple(selected)


def inventory(selection, observations):
    result = {}
    for row in selection["ranges"]:
        entry = result.setdefault(row["channel"], {"fields": {}, "sources": []})
        entry["sources"].append(row["source"])
    for stored in observations:
        result[stored.observation.channel]["fields"].update(observation_units(stored))
    for entry in result.values():
        entry["sources"] = sorted(set(entry["sources"]))
    return result


def freeze_snapshot(manifest, observations):
    if type(manifest) is not dict:
        raise ValidationError("V2 needs a verified native bundle header")
    raw = canonical_json(manifest)
    if len(raw) > 2097152:
        raise ValidationError("V2 manifest exceeds two MiB")
    header = json.loads(raw)
    if (
        header.get("schema_id") != SCHEMA_ID
        or type(header.get("schema_version")) is not int
        or header["schema_version"] != 2
        or header.get("state") != "complete"
        or header.get("scope") != "offline_market_observations"
        or header.get("dataset_kind") != "retained_native_observation_revisions"
        or "transform" not in header
        or header["transform"] is not None
    ):
        raise ValidationError("V2 accepts only original retained native bundles, not MC/proxy")
    if (
        not {
            "candles",
            "observations",
            "selection",
            "channels",
            "raw_payloads",
            "quality",
            "candle_evidence",
        }
        <= header.keys()
        or type(header["candles"]) is not dict
        or set(header["candles"]) != {"manifest", "sha256", "rows_sha256"}
        or type(header["observations"]) is not dict
        or set(header["observations"]) != {"file", "sha256", "count"}
    ):
        raise ValidationError("V2 manifest lacks original file/selection identities")
    selected = validate_records(header["selection"], observations)
    digest = sha256()
    size = 0
    for stored in selected:
        raw = canonical_json(record(stored)) + b"\n"
        size += len(raw)
        digest.update(raw)
    entry = header["observations"]
    hashes = (entry["sha256"], header["candles"]["sha256"], header["candles"]["rows_sha256"])
    if (
        any(
            type(value) is not str
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
            for value in hashes
        )
        or type(entry["count"]) is not int
        or entry["count"] != len(selected)
        or entry["sha256"] != digest.hexdigest()
        or size > MAX_BYTES
        or header["channels"] != inventory(header["selection"], selected)
    ):
        raise ValidationError("V2 retained records differ from verified header identities")
    return header, selected


def inspection_identity(manifest):
    """Summary identity AFTER public file verification; no NN-join/delivery authority."""
    if type(manifest) is not dict:
        raise ValidationError("V2 inspection needs a verified header")
    raw = canonical_json(manifest)
    if len(raw) > 2097152:
        raise ValidationError("V2 inspection header exceeds bounds")
    header = json.loads(raw)
    if (
        header.get("schema_id") != SCHEMA_ID
        or type(header.get("schema_version")) is not int
        or header["schema_version"] != 2
        or header.get("transform", True) is not None
    ):
        raise ValidationError("V2 inspection accepts only native recorded bundles")
    return {
        "schema_id": SCHEMA_ID,
        "scope": "recorded_native_inputs_v2",
        "manifest_sha256": sha256(raw).hexdigest(),
        "observations_sha256": header["observations"]["sha256"],
        "observations_count": header["observations"]["count"],
        "channels": header["channels"],
        "quality": header["quality"],
        "selection": header["selection"],
        "transform": None,
        "limitations": header["limitations"],
    }
