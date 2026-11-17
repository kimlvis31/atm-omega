"""Native V2 files keep sparse depth, metric seeds and original revision histories."""

import json
from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256

import pytest

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.depth_book import BookView
from omega.infrastructure.binance.depth_sampling import DepthMinuteSampler
from omega.ports.auxiliary import AuxiliaryReadRange, StoredAuxiliary
from omega.research import auxiliary_bundle_v2 as v2
from omega.research.auxiliary_datasets import (
    export_auxiliary_dataset,
    freeze_selected_observation_snapshot,
    load_auxiliary_dataset,
    monte_carlo_auxiliary,
    snapshot_auxiliary_dataset,
)
from omega.research.datasets import MarketRow, export_dataset, write_rows
from omega.research.training_inputs import load_training_inputs
from omega.runtime.auxiliary_research import export_stored_auxiliary, run_auxiliary_export_job

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
START, END = 600000, 1200000


def pin(observation, revision=1, previous=None):
    return StoredAuxiliary(
        BTC,
        observation,
        revision,
        observation_checksum(encode_observation(BTC, observation)),
        previous,
    )


def point(channel, *, number=D(1000), available=300001):
    field = "METRIC_OPENINTEREST" if channel == "open_interest" else "METRIC_LONGSHORTRATIO"
    return AuxiliaryInput(
        channel,
        ((field, number), ("SOURCE_PERIOD_MS", 300000)),
        300000,
        360000,
        300000,
        available,
        source=BTC.key + ":" + v2.SOURCES[channel],
    )


def depth(index, kind):
    opened = START + index * 60000
    sampler = DepthMinuteSampler(BTC, price_precision=2, quote_precision=8)
    if kind != "missing":
        sampler.begin(1, started_at_ms=opened)
        bid, ask = ("99.999", "100.001") if kind == "zero" else ("99.9", "100.1")
        floor, ceiling = {
            "full": ("95", "105"),
            "partial": ("99", "101"),
            "zero": ("99.99", "100.01"),
        }[kind]
        view = BookView(17, ((D(bid), D(2)),), ((D(ask), D(1)),), D(floor), D(ceiling))
        assert sampler.observe(
            view,
            event_time_ms=opened + 59999,
            received_at_ms=opened + 60001,
            accepted_at_ms=opened + 60002,
            generation=1,
            raw_refs=("a" * 64,),
        )
    return pin(sampler.finalize(opened, as_of_ms=opened + 60003).observation)


@pytest.fixture
def source(tmp_path):
    rows = [
        MarketRow(BTC.key, START + i * 60000, D(100), D(110), D(90), D(101), D(2))
        for i in range(10)
    ]
    csv = tmp_path / "native-source.csv"
    write_rows(csv, rows)
    destination = tmp_path / "candles"
    export_dataset(csv, destination, {})
    return destination, rows


def records():
    oi = pin(point("open_interest"))
    correction = pin(point("open_interest", number=D(2000), available=660001), 2, oi.checksum)
    ls = pin(point("long_short_ratio", number=D("1.2")))
    flow = pin(
        AuxiliaryInput(
            "trade_flow",
            (("AGGTRADE_NOTIONALBUY", D(1500)), ("AGGTRADE_NOTIONALSELL", D(500))),
            START,
            START + 60000,
            START + 60000,
            START + 60000,
            source=BTC.key + ":aggTrade",
        )
    )
    return (
        oi,
        correction,
        ls,
        flow,
        *(depth(i, kind) for i, kind in enumerate(("full", "partial", "zero", "missing"))),
    )


def ranges():
    return tuple(
        AuxiliaryReadRange(
            BTC,
            channel,
            BTC.key + ":" + suffix,
            300000 if channel in v2.METRIC_CHANNELS else START,
            END,
        )
        for channel, suffix in v2.SOURCES.items()
    )


def export(source, destination, values=None, **options):
    return export_auxiliary_dataset(
        source,
        destination,
        records() if values is None else values,
        format_version=2,
        source_ranges=ranges(),
        metric_lookback_ms=300000,
        as_of_ms=END,
        **options,
    )


def test_actual_sampler_all_four_quality_cases_seed_and_revisions_roundtrip(source, tmp_path):
    candles, _ = source
    destination = tmp_path / "native2"
    header = export(candles, destination)
    loaded, rows, selected = load_auxiliary_dataset(destination)
    assert loaded == header and len(rows) == 10 and len(selected) == 8
    assert selected == tuple(sorted(records(), key=v2.record_key))
    assert header["schema_id"] == v2.SCHEMA_ID and header["schema_version"] == 2
    assert header["selection"]["candle_ranges"] == [
        {"instrument": BTC.key, "start_ms": START, "end_ms": END, "count": 10}
    ]
    assert header["selection"]["metric_lookback_ms"] == 300000
    assert (
        header["selection"]["depth_profile"]["producer_implementation"]
        == "not_retained_by_normalized_store"
    )
    depths = [row.observation for row in selected if row.observation.channel == "depth"]
    assert [row.quality for row in depths] == ["observed", "partial", "partial", "missing"]
    assert [len(set(row.mapping()) & set(v2.DEPTH_BINS)) for row in depths] == [12, 4, 0, 0]
    assert depths[2].values and depths[2].coverage == 0 and depths[3].values == ()
    assert depths[1].mapping()["DEPTH_RECEIVED_AT_MS"] == 720001
    assert "DEPTH_BIDS5" not in depths[1].mapping()
    seeds = [row for row in selected if row.observation.channel == "open_interest"]
    assert [row.revision for row in seeds] == [1, 2] and seeds[1].previous_checksum == seeds[
        0
    ].checksum
    assert seeds[0].observation.window_start_ms < rows[0].open_time_ms
    frozen, copied = freeze_selected_observation_snapshot(header, selected)
    assert frozen == header and copied == selected
    target = tmp_path / "snapshot"
    assert snapshot_auxiliary_dataset(destination, target) == header
    assert load_auxiliary_dataset(target) == (header, rows, selected)
    identity = v2.inspection_identity(header)
    assert identity["scope"] == "recorded_native_inputs_v2" and identity["observations_count"] == 8
    assert identity["manifest_sha256"] == sha256(v2.canonical_json(header)).hexdigest()


def test_default_and_explicit_v1_export_remain_identical(source, tmp_path):
    candles, _ = source
    flow = (records()[3],)
    default = export_auxiliary_dataset(candles, tmp_path / "old-default", flow)
    explicit = export_auxiliary_dataset(candles, tmp_path / "old-explicit", flow, format_version=1)
    assert default == explicit and default["schema_id"] == "omega.market-observations.1m.v1"
    assert (tmp_path / "old-default/observations.jsonl").read_bytes() == (
        tmp_path / "old-explicit/observations.jsonl"
    ).read_bytes()


@pytest.mark.parametrize(
    "fault",
    [
        "missing_predecessor",
        "changed_predecessor",
        "duplicate",
        "future",
        "wrongsource",
        "profile",
        "unknownfield",
    ],
)
def test_bad_native_chain_or_profile_cannot_export_as_v2(source, tmp_path, fault):
    candles, _ = source
    values = list(records())
    if fault == "missing_predecessor":
        values.pop(0)
    elif fault == "changed_predecessor":
        values[1] = replace(values[1], previous_checksum="0" * 64)
    elif fault == "duplicate":
        values.append(values[0])
    elif fault == "future":
        values[1] = pin(
            replace(values[1].observation, available_at_ms=END + 1), 2, values[0].checksum
        )
    elif fault == "wrongsource":
        values[0] = pin(replace(values[0].observation, source=BTC.key + ":open_interest"))
    else:
        row = values[4].observation
        fields = row.mapping()
        fields["DEPTH_PROFILE_VERSION" if fault == "profile" else "UNKNOWN_FIELD"] = 2
        values[4] = pin(replace(row, values=tuple(fields.items())))
    with pytest.raises(ValidationError):
        export(candles, tmp_path / fault, tuple(values))


@pytest.mark.parametrize("fault", ["units", "profile_bool", "count", "revision_sha"])
def test_header_or_record_tampering_rejects_even_with_independent_file_hash(
    source, tmp_path, fault
):
    candles, _ = source
    destination = tmp_path / fault
    header = export(candles, destination)
    if fault == "units":
        header["channels"]["depth"]["fields"]["DEPTH_CENTER_PRICE"] = "base_quantity"
    elif fault == "profile_bool":
        header["selection"]["depth_profile"]["version"] = True
    elif fault == "count":
        header["observations"]["count"] += 1
    else:
        path = destination / "observations.jsonl"
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        rows[0]["checksum"] = "0" * 64
        raw = b"".join(v2.canonical_json(row) + b"\n" for row in rows)
        path.write_bytes(raw)
        header["observations"]["sha256"] = sha256(raw).hexdigest()
    (destination / "dataset.json").write_text(json.dumps(header), encoding="utf-8")
    with pytest.raises(ValidationError):
        load_auxiliary_dataset(destination)


def test_runtime_v2_uses_all_revision_snapshot_explicit_seed_and_closes_owner(source, tmp_path):
    candles, _ = source
    calls = []

    class Store:
        closed = False

        def read_ranges(self, *args, **kwargs):
            raise AssertionError("V2 must not select latest-only rows")

        def read_revisions(self, specs, **kwargs):
            calls.append((specs, kwargs))
            return records()

        def close(self):
            self.closed = True

    store = Store()
    selections = [
        {"instrument": BTC.key, "channel": channel, "source": BTC.key + ":" + suffix}
        for channel, suffix in v2.SOURCES.items()
    ]
    report = export_stored_auxiliary(
        candles,
        tmp_path / "runtime",
        selections,
        store_factory=lambda: store,
        as_of_ms=END,
        clock=lambda: END,
        format_version=2,
        metric_lookback_ms=300000,
    )
    assert store.closed and len(calls) == 1 and calls[0][1] == {"as_of_ms": END, "limit": 10000}
    assert report["observations"] == 8 and report["selection"]["metric_lookback_ms"] == 300000
    assert (
        run_auxiliary_export_job.omega_feature_scopes["native_bundle_v2"]
        == "recorded_native_inputs_v2"
    )


def test_v2_mc_and_v1_training_join_explicitly_reject_before_output(source, tmp_path):
    candles, _ = source
    destination = tmp_path / "native"
    export(candles, destination)
    with pytest.raises(ValidationError, match="Monte Carlo"):
        monte_carlo_auxiliary(destination, tmp_path / "mc", {})
    assert not (tmp_path / "mc").exists()
    with pytest.raises((ValidationError, ValueError)):
        load_training_inputs(destination)


def test_explicit_zero_lookback_never_silently_uses_default_or_pre_range_seed(source, tmp_path):
    candles, rows = source
    metric_ranges = tuple(
        AuxiliaryReadRange(BTC, c, BTC.key + ":" + v2.SOURCES[c], START, END)
        for c in ("open_interest", "long_short_ratio")
    )
    document = v2.selection_document(metric_ranges, rows, as_of_ms=END, metric_lookback_ms=0)
    assert document["metric_lookback_ms"] == 0
    with pytest.raises(ValidationError, match="coverage"):
        v2.validate_records(document, records()[:3])
    with pytest.raises(ValidationError):
        export_auxiliary_dataset(
            candles,
            tmp_path / "implicit",
            (),
            format_version=2,
            source_ranges=metric_ranges,
            as_of_ms=END,
        )


def test_native_gap_minute_evidence_never_creates_an_extra_candle(source, tmp_path):
    _, rows = source
    hole = START + 60000
    retained = [row for row in rows if row.open_time_ms != hole]
    csv = tmp_path / "gapped.csv"
    write_rows(csv, retained)
    candles = tmp_path / "gapped-candles"
    export_dataset(csv, candles, {})
    flow = records()[3].observation
    gap_flow = pin(
        replace(
            flow,
            window_start_ms=hole,
            window_end_ms=hole + 60000,
            event_time_ms=hole + 60000,
            available_at_ms=hole + 60000,
        )
    )
    destination = tmp_path / "gapped-native"
    export(candles, destination, (*records(), gap_flow))
    header, loaded, values = load_auxiliary_dataset(destination)
    assert not any(row.open_time_ms == hole for row in loaded)
    assert any(
        row.observation.channel == "trade_flow" and row.observation.window_start_ms == hole
        for row in values
    )
    assert header["selection"]["candle_ranges"][0]["count"] == 9
    assert header["quality"]["outside_candle_keys"] == (
        "range evidence only; never additional tradable candles"
    )


def test_v2_count_and_byte_limits_reject_before_retaining_unbounded_input(
    source, tmp_path, monkeypatch
):
    _, rows = source
    selection = v2.selection_document(ranges(), rows, as_of_ms=END, metric_lookback_ms=300000)
    with pytest.raises(ValidationError, match="10000"):
        v2.validate_records(selection, (records()[0],) * 10001)
    monkeypatch.setattr(v2, "MAX_BYTES", 10)
    with pytest.raises(ValidationError, match="bytes"):
        v2.validate_records(selection, records())
