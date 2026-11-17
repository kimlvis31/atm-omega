"""Immutable native input selection, without imports of trading owners or brokers."""

import csv
import json
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256

import pytest

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.ports.auxiliary import StoredAuxiliary
from omega.research.auxiliary_datasets import (
    export_auxiliary_dataset,
    freeze_selected_observation_snapshot,
    load_auxiliary_dataset,
)
from omega.research.datasets import FIELDS, export_dataset
from omega.research.margin_observation_inputs import NativeObservationReplay

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")
MAX_TIME = 253_402_300_799_000


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def stored(
    *,
    opening=0,
    instrument=BTC,
    available=None,
    quality="observed",
    buy=D(1500),
    sell=D(0),
    source=None,
    revision=1,
):
    observation = AuxiliaryInput(
        "trade_flow",
        ()
        if quality in {"missing", "unsupported"}
        else (("AGGTRADE_NOTIONALBUY", buy), ("AGGTRADE_NOTIONALSELL", sell)),
        opening,
        opening + 60000,
        opening + 60000,
        opening + 60500 if available is None else available,
        quality,
        D(0)
        if quality in {"missing", "unsupported"}
        else D(".5")
        if quality == "partial"
        else D(1),
        source or instrument.key + ":aggTrade",
    )
    return StoredAuxiliary(
        instrument,
        observation,
        revision,
        observation_checksum(encode_observation(instrument, observation)),
        None if revision == 1 else "c" * 64,
    )


def manifest(records, *, selections=None):
    rows, fields, sources = [], {}, []
    for item in sorted(
        records,
        key=lambda item: (
            item.instrument.key,
            item.observation.window_start_ms,
            item.observation.channel,
            item.observation.source,
        ),
    ):
        rows.append(
            {
                "revision": item.revision,
                "checksum": item.checksum,
                "previous_checksum": item.previous_checksum,
                "payload": json.loads(encode_observation(item.instrument, item.observation)),
            }
        )
        fields.update({name: "quote_notional" for name, _ in item.observation.values})
        sources.append(item.observation.source)
    result = {
        "schema_version": 1,
        "schema_id": "omega.market-observations.1m.v1",
        "state": "complete",
        "scope": "offline_market_observations",
        "dataset_kind": "selected_normalized_observations",
        "transform": None,
        "candles": {
            "manifest": "candles/dataset.json",
            "sha256": "a" * 64,
            "rows_sha256": "b" * 64,
        },
        "observations": {
            "file": "observations.jsonl",
            "count": len(records),
            "sha256": sha256(b"".join(encoded(row) + b"\n" for row in rows)).hexdigest(),
        },
        "channels": {"trade_flow": {"fields": fields, "sources": sorted(set(sources))}}
        if records
        else {},
    }
    if selections is not None:
        provenance = {
            "schema_version": 1,
            "scope": "retrospective_training_only",
            "origin": "committed_postgresql",
            "as_of_ms": 200000,
            "selections": selections,
        }
        result["export_provenance"] = {
            "data": provenance,
            "sha256": sha256(encoded(provenance)).hexdigest(),
        }
    return result


def candle(instrument=BTC, opening=0):
    return Candle(instrument, opening, opening + 60000, D(100), D(110), D(90), D(105), D(1))


def prepare(replay, bars=None, requirements=None):
    bars = (candle(),) if bars is None else bars
    return replay.prepare(
        bars,
        as_of_ms=replay.as_of_ms(bars),
        requirements={BTC.key: ["trade_flow"]} if requirements is None else requirements,
    )


def test_exact_available_boundary_returns_original_native_pin_and_fresh_lineage():
    original = stored()
    header = manifest((original,))
    replay = NativeObservationReplay(header, (original,), decision_latency_ms=500)
    assert replay.as_of_ms((candle(),)) == 60500
    assert replay.validate_requirements({BTC.key: ("trade_flow",)}) is None
    (prepared,) = prepare(replay)
    assert prepared.pins == (original,) and prepared.pins[0] is not original
    assert prepared.frame.inputs == (original.observation,) and not prepared.retrospective
    assert prepared.frame.as_of_ms == original.observation.available_at_ms
    identity = replay.destination_id
    lineage = replay.lineage
    lineage["decision_latency_ms"] = 0
    header["observations"]["sha256"] = "d" * 64
    assert replay.lineage["decision_latency_ms"] == 500
    assert replay.destination_id == identity and len(identity) == 64


def test_late_selected_revision_never_substitutes_an_absent_older_revision():
    latest = stored(revision=2, available=61000)
    replay = NativeObservationReplay(manifest((latest,)), (latest,), decision_latency_ms=500)
    (prepared,) = prepare(replay)
    assert prepared.pins == () and prepared.frame.inputs[0].values == ()
    assert prepared.frame.inputs[0].quality == "missing"
    assert prepared.frame.inputs[0].available_at_ms == 60500
    assert latest.revision == 2 and latest.observation.available_at_ms == 61000


@pytest.mark.parametrize("quality", ["partial", "missing", "unsupported"])
def test_declared_quality_is_retained_without_promoting_it_to_observed(quality):
    original = stored(quality=quality)
    replay = NativeObservationReplay(manifest((original,)), (original,), decision_latency_ms=500)
    (prepared,) = prepare(replay)
    assert prepared.frame.inputs[0].quality == quality
    assert not prepared.frame.inputs[0].usable_at(prepared.frame.as_of_ms)
    assert prepared.pins == (original,)


def test_declared_source_with_no_minute_records_is_missing_not_an_invented_zero():
    selections = [{"instrument": BTC.key, "channel": "trade_flow", "source": BTC.key + ":aggTrade"}]
    replay = NativeObservationReplay(manifest((), selections=selections), ())
    (prepared,) = prepare(replay)
    assert prepared.frame.inputs[0].quality == "missing" and prepared.pins == ()
    assert prepared.frame.inputs[0].values == ()
    undeclared = NativeObservationReplay(manifest(()), ())
    with pytest.raises(ValidationError, match="absent"):
        undeclared.validate_requirements({BTC.key: ["trade_flow"]})


def test_frames_cover_exactly_requested_instruments_and_do_not_alias_returned_pins():
    original = stored()
    replay = NativeObservationReplay(manifest((original,)), (original,), decision_latency_ms=500)
    (prepared,) = prepare(replay, bars=(candle(ETH), candle()))
    assert prepared.frame.candle.instrument == BTC
    object.__setattr__(prepared.pins[0], "checksum", "e" * 64)
    object.__setattr__(original.observation, "source", "changed-after-construction")
    (again,) = prepare(replay)
    assert again.pins[0].checksum != "e" * 64
    assert again.frame.inputs[0].source == BTC.key + ":aggTrade"
    assert replay.prepare((candle(),), as_of_ms=60500, requirements={}) == ()


@pytest.mark.parametrize("latency", [True, False, 0.5, -1, 60001, "500"])
def test_invalid_latency_is_rejected(latency):
    original = stored()
    with pytest.raises(ValidationError, match="latency"):
        NativeObservationReplay(manifest((original,)), (original,), decision_latency_ms=latency)


@pytest.mark.parametrize(
    "requirements",
    [
        {BTC.key: ["metric"]},
        {BTC.key: ["depth"]},
        {BTC.key: []},
        {BTC.key: ["trade_flow", "trade_flow"]},
        {ETH.key: ["trade_flow"]},
        {BTC.key: "trade_flow"},
        {"BINANCE:usdm:BTCUSDT": ["trade_flow"]},
        [],
    ],
)
def test_unsupported_undeclared_or_foreign_requirements_are_rejected(requirements):
    original = stored()
    replay = NativeObservationReplay(manifest((original,)), (original,))
    with pytest.raises(ValidationError):
        replay.validate_requirements(requirements)


@pytest.mark.parametrize(
    "damage", ["record_hash", "file_hash", "count", "catalog", "generated", "source", "duplicate"]
)
def test_bad_native_identity_or_generated_inputs_fail_before_any_preparation(damage):
    original = stored()
    records = (original,)
    header = manifest(records)
    if damage == "record_hash":
        records = (replace(original, checksum="f" * 64),)
    elif damage == "file_hash":
        header["observations"]["sha256"] = "f" * 64
    elif damage == "count":
        header["observations"]["count"] = True
    elif damage == "catalog":
        header["channels"]["trade_flow"]["sources"] = [ETH.key + ":aggTrade"]
    elif damage == "generated":
        header["transform"] = {"id": "joint-relative-block-auxiliary"}
    elif damage == "source":
        records = (stored(source="research_mc:original-source"),)
        header = manifest(records)
    else:
        records = (original, original)
        header = manifest(records)
    with pytest.raises(ValidationError):
        NativeObservationReplay(header, records)


def test_native_zero_notionals_are_valid_but_missing_or_negative_required_fields_are_not():
    zero = stored(buy=D(0), available=60000)
    assert prepare(NativeObservationReplay(manifest((zero,)), (zero,)))[0].pins == (zero,)
    bad = stored(buy=D(-1))
    with pytest.raises(ValidationError):
        NativeObservationReplay(manifest((bad,)), (bad,))


def test_missing_required_notional_is_not_admitted_as_an_observed_native_frame():
    original = stored()
    observation = replace(original.observation, values=(("AGGTRADE_NOTIONALBUY", D(1500)),))
    damaged = replace(
        original,
        observation=observation,
        checksum=observation_checksum(encode_observation(original.instrument, observation)),
    )
    with pytest.raises(ValidationError, match="notionals"):
        NativeObservationReplay(manifest((damaged,)), (damaged,))


def test_cutoff_and_common_frame_guards_do_not_allow_future_override():
    original = stored()
    replay = NativeObservationReplay(manifest((original,)), (original,), decision_latency_ms=500)
    for cutoff in (True, 60500.0, 60000, 61000):
        with pytest.raises(ValidationError, match="cutoff"):
            replay.prepare((candle(),), as_of_ms=cutoff, requirements={BTC.key: ["trade_flow"]})
    for bars in ([], (), (candle(), candle()), (candle(), candle(ETH, 60000))):
        with pytest.raises(ValidationError):
            replay.as_of_ms(bars)
    late_open = ((MAX_TIME // 60000) - 1) * 60000
    late_replay = NativeObservationReplay(
        manifest((original,)), (original,), decision_latency_ms=60000
    )
    with pytest.raises(ValidationError, match="overflows"):
        late_replay.as_of_ms((candle(opening=late_open),))


def test_actual_public_export_loader_matches_canonical_record_identity(tmp_path):
    source = tmp_path / "rows.csv"
    with source.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        for index in range(5):
            writer.writerow(
                dict(
                    zip(
                        FIELDS,
                        (BTC.key, index * 60000, "100", "110", "90", "105", "1"),
                        strict=True,
                    )
                )
            )
    candles, bundle = tmp_path / "candles", tmp_path / "bundle"
    export_dataset(source, candles, {})
    originals = tuple(stored(opening=index * 60000) for index in range(5))
    export_auxiliary_dataset(candles, bundle, originals)
    before = {str(path): path.read_bytes() for path in bundle.rglob("*") if path.is_file()}
    header, _rows, observations = load_auxiliary_dataset(bundle)
    frozen_header, frozen_records = freeze_selected_observation_snapshot(header, observations)
    assert frozen_header == header and frozen_header is not header
    assert frozen_records == observations and frozen_records[0] is not observations[0]
    replay = NativeObservationReplay(header, observations, decision_latency_ms=500)
    assert prepare(replay)[0].pins == (originals[0],)
    assert before == {str(path): path.read_bytes() for path in bundle.rglob("*") if path.is_file()}
