"""Compose committed observation reads with portable offline dataset exports."""

import time
from pathlib import Path

from omega.application.paper import parse_instrument
from omega.domain.errors import ValidationError
from omega.ports.auxiliary import AuxiliaryReadRange
from omega.research.artifacts import check_cancel, write_json
from omega.research.auxiliary_bundle_v2 import METRIC_CHANNELS, selection_document
from omega.research.auxiliary_datasets import export_auxiliary_dataset
from omega.research.datasets import load_dataset

MAX_SELECTIONS = 8
MAX_MINUTES = 10000


def _selections(value):
    if type(value) is not list or not 1 <= len(value) <= MAX_SELECTIONS:
        raise ValidationError("select 1..8 explicit observation sources")
    result = []
    for entry in value:
        if type(entry) is not dict or set(entry) != {"instrument", "channel", "source"}:
            raise ValidationError("observation selection needs instrument, channel and source")
        instrument = parse_instrument(entry["instrument"])
        # Reuse the port's source/channel validation before opening a connection.
        probe = AuxiliaryReadRange(instrument, entry["channel"], entry["source"], 0, 60000)
        key = (instrument.key, probe.channel, probe.source)
        if key in result:
            raise ValidationError("duplicate observation source selection")
        result.append(key)
    return tuple(result)


def _ranges(rows, selections):
    times = {}
    for row in rows:
        times.setdefault(row.instrument, []).append(row.open_time_ms)
    result = []
    for key, channel, source in selections:
        if key not in times:
            raise ValidationError("selected observation instrument is absent from candle dataset")
        values = times[key]
        result.append(
            AuxiliaryReadRange(
                parse_instrument(key), channel, source, min(values), max(values) + 60000
            )
        )
    if sum((row.end_ms - row.start_ms) // 60000 for row in result) > MAX_MINUTES:
        raise ValidationError("observation export exceeds 10000 requested minute slots")
    return tuple(result)


def export_stored_auxiliary(
    candle_dataset,
    destination,
    selections,
    *,
    store_factory,
    as_of_ms=None,
    cancel=None,
    clock=None,
    format_version=1,
    metric_lookback_ms=None,
):
    """Freeze candle inputs first; read selected revisions in one database snapshot.

    Retrospective export only. Candle files and observation storage are distinct
    sources, so this does not claim an atomic historical snapshot across both.
    """
    if type(format_version) is not int or format_version not in {1, 2}:
        raise ValidationError("unsupported native observation export format")
    if format_version == 2:
        return _export_stored_native_v2(
            candle_dataset,
            destination,
            selections,
            store_factory=store_factory,
            as_of_ms=as_of_ms,
            cancel=cancel,
            clock=clock,
            metric_lookback_ms=metric_lookback_ms,
        )
    if metric_lookback_ms is not None:
        raise ValidationError("metric seed lookback requires explicit format_version2")
    selected = _selections(selections)
    now = (clock or (lambda: time.time_ns() // 1_000_000))()
    cutoff = now if as_of_ms is None else as_of_ms
    if type(now) is not int or type(cutoff) is not int or not 0 <= cutoff <= now < 2**63:
        raise ValidationError("observation export cutoff must be an actual non-future timestamp")
    destination = Path(destination)
    details = {}
    provenance = {
        "schema_version": 1,
        "scope": "retrospective_training_only",
        "origin": "committed_postgresql",
        "as_of_ms": cutoff,
        "selections": [
            dict(zip(("instrument", "channel", "source"), row, strict=True)) for row in selected
        ],
    }

    def observations():
        # export_auxiliary_dataset creates/verifies its own candle snapshot before
        # consuming this iterator. Range selection therefore uses those exact rows.
        candle_manifest, rows = load_dataset(destination / "candles", cancel)
        if candle_manifest.get("transform") is not None:
            raise ValidationError("join original observations before generating Monte Carlo data")
        ranges = _ranges(rows, selected)
        keys = {(row.instrument, row.open_time_ms) for row in rows}
        check_cancel(cancel)
        store = store_factory()
        try:
            received = store.read_ranges(ranges, as_of_ms=cutoff, limit=MAX_MINUTES)
            details.update(
                rows=len(rows),
                requested_minute_slots=sum((row.end_ms - row.start_ms) // 60000 for row in ranges),
            )
            for stored in received:
                check_cancel(cancel)
                if (
                    stored.instrument.key,
                    stored.observation.channel,
                    stored.observation.source,
                ) not in selected or stored.observation.available_at_ms > cutoff:
                    raise ValidationError("observation store returned an unselected/future row")
                if (stored.instrument.key, stored.observation.window_start_ms) in keys:
                    yield stored
        finally:
            store.close()

    iterator = observations()
    try:
        manifest = export_auxiliary_dataset(
            Path(candle_dataset), destination, iterator, export_provenance=provenance, cancel=cancel
        )
    finally:
        iterator.close()
    return {
        **details,
        "observations": manifest["observations"]["count"],
        "channels": manifest["channels"],
        "schema_id": manifest["schema_id"],
        "as_of_ms": cutoff,
        "selections": [
            dict(zip(("instrument", "channel", "source"), row, strict=True)) for row in selected
        ],
        "scope": "retrospective_training_only",
        "raw_payloads": "not included; normalized committed observations only",
    }


def run_auxiliary_export_job(partial, final, parameters, cancel):
    required = {"candle_dataset", "selections"}
    allowed = required | {
        "as_of_ms",
        "seed",
        "settings_revision",
        "timeout_seconds",
        "format_version",
        "metric_lookback_ms",
    }
    if (
        type(parameters) is not dict
        or not required <= parameters.keys()
        or parameters.keys() - allowed
    ):
        raise ValidationError("observation export needs candle_dataset and selections")
    for field in ("seed", "settings_revision"):
        if field in parameters and (
            type(parameters[field]) is not int or not 0 <= parameters[field] < 2**63
        ):
            raise ValidationError("invalid observation export job metadata")

    def store_factory():
        from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
        from omega.infrastructure.postgres.config import load_database_settings

        root = partial.parents[1]
        return PostgresAuxiliaryStore(load_database_settings(root / "database.config.toml"))

    report = export_stored_auxiliary(
        parameters["candle_dataset"],
        partial / "dataset",
        parameters["selections"],
        store_factory=store_factory,
        as_of_ms=parameters.get("as_of_ms"),
        cancel=cancel,
        **(
            {
                "format_version": parameters["format_version"],
                "metric_lookback_ms": parameters.get("metric_lookback_ms"),
            }
            if "format_version" in parameters
            else {"metric_lookback_ms": parameters["metric_lookback_ms"]}
            if "metric_lookback_ms" in parameters
            else {}
        ),
    )
    write_json(partial / "selection.json", report)
    return {"result_file": "dataset/dataset.json", "summary": report}


def _export_stored_native_v2(
    candle_dataset,
    destination,
    selections,
    *,
    store_factory,
    as_of_ms,
    cancel,
    clock,
    metric_lookback_ms,
):
    selected = _selections(selections)
    now = (clock or (lambda: time.time_ns() // 1000000))()
    cutoff = now if as_of_ms is None else as_of_ms
    if type(now) is not int or type(cutoff) is not int or not 0 <= cutoff <= now <= 253402300799000:
        raise ValidationError("V2 export cutoff must be an actual non-future timestamp")
    _manifest, rows = load_dataset(Path(candle_dataset), cancel)
    has_metric = any(channel in METRIC_CHANNELS for _key, channel, _source in selected)
    if (
        has_metric
        and (type(metric_lookback_ms) is not int or not 0 <= metric_lookback_ms <= 86400000)
        or not has_metric
        and metric_lookback_ms is not None
    ):
        raise ValidationError("V2 metrics require explicit bounded metric_lookback_ms")
    times = {}
    for row in rows:
        times.setdefault(row.instrument, []).append(row.open_time_ms)
    ranges = []
    for key, channel, source in selected:
        if key not in times:
            raise ValidationError("V2 source instrument is absent from candles")
        start = min(times[key])
        if channel in METRIC_CHANNELS:
            start = max(0, start - metric_lookback_ms) // 60000 * 60000
        ranges.append(
            AuxiliaryReadRange(
                parse_instrument(key), channel, source, start, max(times[key]) + 60000
            )
        )
    ranges = tuple(ranges)
    selection = selection_document(
        ranges, rows, as_of_ms=cutoff, metric_lookback_ms=metric_lookback_ms
    )
    provenance = {
        "schema_version": 1,
        "scope": "retrospective_training_only",
        "origin": "committed_postgresql",
        "as_of_ms": cutoff,
        "selections": [
            dict(zip(("instrument", "channel", "source"), row, strict=True)) for row in selected
        ],
    }

    def observations():
        check_cancel(cancel)
        store = store_factory()
        try:
            received = store.read_revisions(ranges, as_of_ms=cutoff, limit=MAX_MINUTES)
            for stored in received:
                check_cancel(cancel)
                yield stored
        finally:
            store.close()

    iterator = observations()
    try:
        manifest = export_auxiliary_dataset(
            Path(candle_dataset),
            destination,
            iterator,
            export_provenance=provenance,
            cancel=cancel,
            format_version=2,
            source_ranges=ranges,
            metric_lookback_ms=metric_lookback_ms,
            as_of_ms=cutoff,
        )
    finally:
        iterator.close()
    if manifest["selection"] != selection:
        raise ValidationError("V2 candle range changed during snapshot")
    return {
        "rows": len(rows),
        "requested_minute_slots": sum((row.end_ms - row.start_ms) // 60000 for row in ranges),
        "observations": manifest["observations"]["count"],
        "channels": manifest["channels"],
        "schema_id": manifest["schema_id"],
        "as_of_ms": cutoff,
        "selections": provenance["selections"],
        "selection": manifest["selection"],
        "scope": "retrospective_training_only",
        "raw_payloads": "not included; normalized committed observations only",
    }


run_auxiliary_export_job.omega_feature_scopes = {"native_bundle_v2": "recorded_native_inputs_v2"}
