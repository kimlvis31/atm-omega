"""Actual immutable multichannel files; no exchange, accounts or market DB."""

import json
import os
from dataclasses import replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.ports.auxiliary import StoredAuxiliary
from omega.research import auxiliary_datasets as module
from omega.research.artifacts import Cancelled, content_hash, write_json
from omega.research.datasets import export_dataset, load_dataset, monte_carlo
from omega.research.interpolation import interpolate_dataset


def selected(dataset):
    _, rows = load_dataset(dataset)
    result = []
    for row in rows:
        _, market, symbol = row.instrument.split(":")
        instrument = Instrument("binance", Market(market), symbol)
        base = row.volume
        fields = {
            "AGGTRADE_QUANTITYBUY": base * D(".4"),
            "AGGTRADE_QUANTITYSELL": base * D(".6"),
            "AGGTRADE_NOTIONALBUY": base * D(".4") * row.close,
            "AGGTRADE_NOTIONALSELL": base * D(".6") * row.close,
            "AGGTRADE_NTRADESBUY": 2,
            "AGGTRADE_NTRADESSELL": 3,
        }
        result.append(stored(instrument, "trade_flow", row.open_time_ms, fields))
        if row.open_time_ms % 300000 == 0:
            quantity = D(10 + row.open_time_ms // 60000)
            result.append(
                stored(
                    instrument,
                    "open_interest",
                    row.open_time_ms,
                    {
                        "METRIC_OPENINTEREST": quantity,
                        "METRIC_OPENINTERESTVALUE": quantity * row.close,
                        "SOURCE_PERIOD_MS": 300000,
                    },
                )
            )
            result.append(
                stored(
                    instrument,
                    "long_short_ratio",
                    row.open_time_ms,
                    {
                        "METRIC_LONGSHORTRATIO": D("1.5"),
                        "METRIC_LONGACCOUNT": D(".6"),
                        "METRIC_SHORTACCOUNT": D(".4"),
                        "SOURCE_PERIOD_MS": 300000,
                    },
                )
            )
        if row.open_time_ms % 900000 == 0:
            result.append(
                stored(
                    instrument,
                    "funding_rate",
                    row.open_time_ms,
                    {
                        "FUNDING_RATE": D("-.0001"),
                        "SOURCE_PERIOD_MS": 900000,
                    },
                )
            )
    return result


def stored(instrument, channel, opening, values, *, quality="observed", source=None):
    observation = AuxiliaryInput(
        channel,
        tuple(values.items()),
        opening,
        opening + 60000,
        opening + (60000 if channel == "trade_flow" else 100),
        opening + 60500,
        quality,
        D(1),
        source or f"{instrument.key}:{channel}",
    )
    return StoredAuxiliary(
        instrument,
        observation,
        1,
        observation_checksum(encode_observation(instrument, observation)),
        None,
    )


@pytest.fixture
def bundle(dataset, tmp_path):
    raw = tmp_path / "received.jsonl"
    raw.write_bytes(b'{"actual_received_test_payload":true}\n')
    destination = tmp_path / "observations"
    module.export_auxiliary_dataset(
        dataset, destination, selected(dataset), raw_files={"batch.jsonl": raw}
    )
    return destination


def read_documents(bundle):
    return [json.loads(line) for line in (bundle / "observations.jsonl").read_text().splitlines()]


def rewrite_documents(bundle, documents):
    path = bundle / "observations.jsonl"
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in documents))
    manifest = json.loads((bundle / "dataset.json").read_text())
    manifest["observations"]["sha256"] = content_hash(path)
    write_json(bundle / "dataset.json", manifest)


def test_export_freezes_candles_normalized_revision_units_and_received_bytes(dataset, bundle):
    before = {path.name: path.read_bytes() for path in dataset.iterdir() if path.is_file()}
    manifest, rows, values = module.load_auxiliary_dataset(bundle)
    original = sorted(selected(dataset), key=module._key)
    assert values == tuple(original)
    assert rows == load_dataset(dataset)[1]
    assert manifest["schema_id"] == "omega.market-observations.1m.v1"
    assert manifest["transform"] is None
    assert manifest["channels"]["trade_flow"]["fields"]["AGGTRADE_NOTIONALBUY"] == "quote_notional"
    assert (
        manifest["channels"]["open_interest"]["fields"]["METRIC_OPENINTEREST"]
        == "contract_quantity"
    )
    assert (bundle / "raw/batch.jsonl").read_bytes() == b'{"actual_received_test_payload":true}\n'
    assert {path.name: path.read_bytes() for path in dataset.iterdir() if path.is_file()} == before
    with pytest.raises(FileExistsError):
        module.export_auxiliary_dataset(dataset, bundle, original)


def test_sparse_statistics_are_not_carried_or_zero_filled(bundle):
    _, rows, values = module.load_auxiliary_dataset(bundle)
    assert len(rows) == 120
    oi = [row for row in values if row.observation.channel == "open_interest"]
    assert len(oi) == 24
    assert all(row.observation.window_start_ms % 300000 == 0 for row in oi)
    assert len([row for row in values if row.observation.channel == "funding_rate"]) == 8


def test_selected_revision_and_predecessor_are_preserved(dataset, tmp_path):
    original = selected(dataset)[0]
    revised_observation = replace(original.observation, available_at_ms=120000)
    revised = StoredAuxiliary(
        original.instrument,
        revised_observation,
        2,
        observation_checksum(encode_observation(original.instrument, revised_observation)),
        original.checksum,
    )
    module.export_auxiliary_dataset(dataset, tmp_path / "revision", [revised])
    assert module.load_auxiliary_dataset(tmp_path / "revision")[2] == (revised,)
    with pytest.raises(ValidationError, match="selected revision"):
        module.export_auxiliary_dataset(dataset, tmp_path / "duplicates", [original, revised])


def test_mc_uses_identical_candle_mapping_and_preserves_native_units(bundle, tmp_path):
    settings = {"seed": 42, "block_bars": 2, "splits": ["train", "validation", "test"]}
    direct = tmp_path / "direct"
    monte_carlo(bundle / "candles", direct, settings)
    generated = tmp_path / "generated"
    manifest = module.monte_carlo_auxiliary(bundle, generated, settings)
    _, rows, values = module.load_auxiliary_dataset(generated)
    assert (generated / "candles/rows.csv").read_bytes() == (direct / "rows.csv").read_bytes()
    assert manifest["transform"]["scope"] == "synthetic_research_only"
    assert "not a generated funding settlement" in manifest["transform"]["funding_policy"]
    assert "not the cadence" in manifest["transform"]["source_period_policy"]
    assert (generated / "source/raw/batch.jsonl").read_bytes() == (
        bundle / "raw/batch.jsonl"
    ).read_bytes()
    original = {row.checksum: row for row in module.load_auxiliary_dataset(bundle)[2]}
    by_candle = {(row.instrument, row.open_time_ms): row for row in rows}
    documents = read_documents(generated)
    assert len(documents) == len(values)
    with localcontext() as context:
        context.prec = 34
        for document, value in zip(documents, values, strict=True):
            origin = document["origin"]
            source = original[origin["source_checksum"]]
            observed, prior = value.observation, source.observation
            assert origin["available_at_ms"] == prior.available_at_ms
            assert origin["event_time_ms"] == prior.event_time_ms
            assert (
                observed.available_at_ms - observed.window_start_ms
                == prior.available_at_ms - prior.window_start_ms
            )
            assert (
                observed.event_time_ms - observed.window_start_ms
                == prior.event_time_ms - prior.window_start_ms
            )
            units = module._units(observed, generating=True)
            prior_values = dict(prior.values)
            for key, number in observed.values:
                expected = (
                    prior_values[key] * D(origin["price_scale"])
                    if units[key] == "quote_notional"
                    else prior_values[key]
                )
                assert number == expected
            if observed.channel == "trade_flow":
                fields = dict(observed.values)
                candle = by_candle[value.instrument.key, observed.window_start_ms]
                assert (
                    fields["AGGTRADE_QUANTITYBUY"] + fields["AGGTRADE_QUANTITYSELL"]
                    == candle.volume
                )


def test_mc_seed_is_reproducible_and_unselected_splits_keep_actual_observations(bundle, tmp_path):
    first, second, third = (tmp_path / name for name in ("first", "second", "third"))
    module.monte_carlo_auxiliary(bundle, first, {"seed": 3, "block_bars": 2})
    module.monte_carlo_auxiliary(bundle, second, {"seed": 3, "block_bars": 2})
    module.monte_carlo_auxiliary(bundle, third, {"seed": 4, "block_bars": 2})
    assert (first / "observations.jsonl").read_bytes() == (
        second / "observations.jsonl"
    ).read_bytes()
    assert (first / "observations.jsonl").read_bytes() != (
        third / "observations.jsonl"
    ).read_bytes()
    _, _, original = module.load_auxiliary_dataset(bundle)
    original_ids = {row.checksum for row in original}
    for document in read_documents(first):
        if document["origin"]["split"] != "train":
            assert not document["origin"]["synthetic"]
            assert document["checksum"] in original_ids
    with pytest.raises(ValidationError, match="not a generated"):
        module.monte_carlo_auxiliary(first, tmp_path / "twice", {"seed": 3})


@pytest.mark.parametrize(
    "channel,fields,quality",
    [
        ("custom", {"UNDECLARED": D(1)}, "observed"),
        ("trade_flow", {"UNDECLARED": D(1)}, "observed"),
        ("open_interest", {"METRIC_OPENINTEREST": D(1)}, "carried"),
    ],
)
def test_mc_rejects_unknown_units_and_carried_samples_without_losing_original(
    dataset, tmp_path, channel, fields, quality
):
    original = selected(dataset)[0]
    row = stored(original.instrument, channel, 0, fields, quality=quality)
    root = tmp_path / "unsupported"
    module.export_auxiliary_dataset(dataset, root, [row])
    assert module.load_auxiliary_dataset(root)[2] == (row,)
    before = (root / "observations.jsonl").read_bytes()
    with pytest.raises(ValidationError):
        module.monte_carlo_auxiliary(root, tmp_path / "denied", {"block_bars": 2})
    assert (root / "observations.jsonl").read_bytes() == before


def test_depth_and_observation_without_a_matching_candle_are_explicitly_rejected(dataset, tmp_path):
    original = selected(dataset)[0]
    with pytest.raises(ValidationError, match="depth"):
        module.export_auxiliary_dataset(
            dataset, tmp_path / "depth", [stored(original.instrument, "depth", 0, {"BID": D(1)})]
        )
    with pytest.raises(ValidationError, match="existing candle"):
        module.export_auxiliary_dataset(
            dataset,
            tmp_path / "future",
            [stored(original.instrument, "funding_rate", 6000000, {"FUNDING_RATE": D(0)})],
        )


def test_actual_checksum_and_unit_invariants_are_validated(dataset, tmp_path):
    original = selected(dataset)[0]
    with pytest.raises(ValidationError, match="checksum"):
        module.export_auxiliary_dataset(
            dataset, tmp_path / "bad-checksum", [replace(original, checksum="0" * 64)]
        )
    impossible = stored(
        original.instrument,
        "trade_flow",
        0,
        {"AGGTRADE_QUANTITYBUY": D(0), "AGGTRADE_NOTIONALBUY": D(1)},
    )
    destination = tmp_path / "impossible"
    exported = module.export_auxiliary_dataset(dataset, destination, [impossible])
    assert module.load_auxiliary_dataset(destination)[2] == (impossible,)
    assert exported["quality"]["unit_policy_unverified_rows"] == 1
    with pytest.raises(ValidationError, match="inconsistent"):
        module.monte_carlo_auxiliary(destination, tmp_path / "impossible-mc", {"block_bars": 2})


def test_complete_implied_trade_price_outside_candle_is_preserved_but_not_generated(
    dataset, tmp_path
):
    original = selected(dataset)[0]
    fields = dict(original.observation.values)
    fields["AGGTRADE_NOTIONALBUY"] = fields["AGGTRADE_QUANTITYBUY"] * D(10000)
    actual = stored(original.instrument, "trade_flow", 0, fields)
    source = tmp_path / "outside-price"
    manifest = module.export_auxiliary_dataset(dataset, source, [actual])
    assert manifest["quality"]["known_implied_price_outside_ohlc"] == 1
    assert module.load_auxiliary_dataset(source)[2] == (actual,)
    with pytest.raises(ValidationError, match="implied price"):
        module.monte_carlo_auxiliary(source, tmp_path / "outside-mc", {"block_bars": 2})


def test_provider_flow_volume_difference_is_visible_without_forcing_false_equality(
    dataset, tmp_path
):
    original = selected(dataset)[0]
    fields = dict(original.observation.values)
    fields["AGGTRADE_QUANTITYBUY"] *= 2
    fields["AGGTRADE_NOTIONALBUY"] *= 2
    actual = stored(original.instrument, "trade_flow", 0, fields)
    source = tmp_path / "volume-difference"
    manifest = module.export_auxiliary_dataset(dataset, source, [actual])
    assert manifest["quality"]["flow_volume_difference_rows"] == 1
    assert D(manifest["quality"]["flow_volume_max_relative_difference"]) == D(".4")
    assert "RPI" in manifest["quality"]["volume_policy"]
    module.monte_carlo_auxiliary(source, tmp_path / "volume-mc", {"block_bars": 2})


def test_small_decimal_implied_price_rounding_is_not_a_hard_contradiction(dataset, tmp_path):
    original = selected(dataset)[0]
    fields = dict(original.observation.values)
    fields["AGGTRADE_NOTIONALBUY"] = fields["AGGTRADE_QUANTITYBUY"] * (D(103) + D("1E-24"))
    actual = stored(original.instrument, "trade_flow", 0, fields)
    source = tmp_path / "rounding"
    manifest = module.export_auxiliary_dataset(dataset, source, [actual])
    assert manifest["quality"]["known_implied_price_outside_ohlc"] == 0
    module.monte_carlo_auxiliary(source, tmp_path / "rounding-mc", {"block_bars": 2})


@pytest.mark.parametrize("target", ["raw", "normalized", "candle"])
def test_changed_actual_files_never_load_as_complete(bundle, target):
    if target == "raw":
        (bundle / "raw/batch.jsonl").write_bytes(b"changed")
    elif target == "normalized":
        (bundle / "observations.jsonl").write_bytes(b"changed")
    else:
        (bundle / "candles/rows.csv").write_bytes(b"changed")
    with pytest.raises((ValidationError, ValueError), match="hash"):
        module.load_auxiliary_dataset(bundle)


def test_tampered_generated_values_or_origins_are_rejected_even_after_file_hash_refresh(
    bundle, tmp_path
):
    root = tmp_path / "generated"
    module.monte_carlo_auxiliary(bundle, root, {"seed": 9, "block_bars": 2})
    documents = read_documents(root)
    documents[0]["origin"]["available_at_ms"] += 1
    rewrite_documents(root, documents)
    with pytest.raises(ValidationError, match="lineage"):
        module.load_auxiliary_dataset(root)


def test_mc_metadata_cannot_claim_a_different_seed(bundle, tmp_path):
    root = tmp_path / "generated"
    module.monte_carlo_auxiliary(bundle, root, {"seed": 9, "block_bars": 2})
    manifest = json.loads((root / "dataset.json").read_text())
    manifest["transform"]["seed"] = 10
    write_json(root / "dataset.json", manifest)
    with pytest.raises(ValidationError, match="policies differ"):
        module.load_auxiliary_dataset(root)


def test_generated_bundle_remains_readable_when_original_files_change(bundle, tmp_path):
    target = tmp_path / "independent"
    module.monte_carlo_auxiliary(bundle, target, {"seed": 3, "block_bars": 2})
    before = module.load_auxiliary_dataset(target)
    (bundle / "observations.jsonl").write_bytes(b"corrupted original after snapshot")
    (bundle / "candles/source.original.csv").write_bytes(b"changed original market file")
    (bundle / "raw/batch.jsonl").write_bytes(b"changed received bytes")
    assert module.load_auxiliary_dataset(target) == before


def test_repaired_candle_masks_and_direct_parent_evidence_are_preserved(gapped_source, tmp_path):
    base, repaired, target = (tmp_path / name for name in ("base", "repaired", "joined"))
    export_dataset(gapped_source, base, {})
    interpolate_dataset(base, repaired, {"method": "previous_close_flat", "max_ratio": 1.0})
    manifest = module.export_auxiliary_dataset(repaired, target, [])
    assert manifest["candle_evidence"]
    assert (target / "candles/quality-mask.jsonl").read_bytes() == (
        repaired / "quality-mask.jsonl"
    ).read_bytes()
    assert (target / "candles/source.rows.csv").read_bytes() == (
        repaired / "source.rows.csv"
    ).read_bytes()
    assert (target / "candles/source.dataset.json").read_bytes() == (
        repaired / "source.dataset.json"
    ).read_bytes()
    assert module.load_auxiliary_dataset(target)[1] == load_dataset(repaired)[1]


def test_candle_parent_evidence_cannot_be_hidden_by_an_empty_catalog(bundle, tmp_path):
    target = tmp_path / "generated"
    module.monte_carlo_auxiliary(bundle, target, {"seed": 3, "block_bars": 2})
    manifest = json.loads((target / "dataset.json").read_text())
    manifest["candle_evidence"] = []
    write_json(target / "dataset.json", manifest)
    with pytest.raises(ValidationError, match="evidence catalog"):
        module.load_auxiliary_dataset(target)


def test_joint_mapping_cannot_cross_its_source_split(bundle, tmp_path):
    target = tmp_path / "generated"
    module.monte_carlo_auxiliary(bundle, target, {"seed": 3, "block_bars": 2})
    candles_path = target / "candles/dataset.json"
    candles = json.loads(candles_path.read_text())
    candles["transform"]["provenance"]["train"]["source_timestamps"][0] = candles["splits"]["test"][
        0
    ]
    write_json(candles_path, candles)
    manifest = json.loads((target / "dataset.json").read_text())
    manifest["candles"]["sha256"] = content_hash(candles_path)
    write_json(target / "dataset.json", manifest)
    with pytest.raises(ValidationError, match="crossed its frozen split"):
        module.load_auxiliary_dataset(target)


@pytest.mark.parametrize(
    "name", ["../escape", "/absolute", "C:/absolute", "a/../escape", "a\\escape"]
)
def test_unsafe_raw_reference_names_are_rejected(dataset, tmp_path, name):
    source = tmp_path / "raw.json"
    source.write_bytes(b"{}")
    with pytest.raises((ValidationError, ValueError)):
        module.export_auxiliary_dataset(
            dataset, tmp_path / "bad-path", [], raw_files={name: source}
        )


def test_cancel_does_not_create_a_complete_manifest(dataset, tmp_path):
    import threading

    stopped = threading.Event()
    stopped.set()
    root = tmp_path / "cancelled"
    with pytest.raises(Cancelled):
        module.export_auxiliary_dataset(dataset, root, [], cancel=stopped)
    assert not (root / "dataset.json").exists()


def test_fifo_or_symlink_auxiliary_file_is_not_opened_as_data(bundle, tmp_path):
    target = bundle / "observations.jsonl"
    target.unlink()
    if hasattr(os, "mkfifo"):
        os.mkfifo(target)
    else:
        destination = tmp_path / "other.jsonl"
        destination.write_bytes(b"{}")
        try:
            target.symlink_to(destination)
        except OSError:
            pytest.skip("local symlink creation unavailable; Linux FIFO validation required")
    with pytest.raises(ValueError):
        module.load_auxiliary_dataset(bundle)
