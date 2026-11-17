"""Stored export composition uses frozen candle files and bounded revision reads."""

import json
import time
from decimal import Decimal
from pathlib import Path

import pytest

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.ports.auxiliary import StoredAuxiliary
from omega.research.auxiliary_datasets import load_auxiliary_dataset
from omega.research.datasets import monte_carlo
from omega.research.service import ResearchService
from omega.runtime.auxiliary_research import export_stored_auxiliary

KEY = "binance:usdm:BTCUSDT"
SOURCE = KEY + ":open_interest_history"
SELECTION = {"instrument": KEY, "channel": "open_interest", "source": SOURCE}


def stored(*, available=60001, source=SOURCE):
    instrument = Instrument("binance", Market.USDM, "BTCUSDT")
    observation = AuxiliaryInput(
        "open_interest",
        (("METRIC_OPENINTEREST", Decimal("7.123")),),
        0,
        60000,
        100,
        available,
        "observed",
        Decimal(1),
        source,
    )
    checksum = observation_checksum(encode_observation(instrument, observation))
    return StoredAuxiliary(instrument, observation, 1, checksum, None)


class Store:
    def __init__(self, values=(), fail=None, close_error=False):
        self.values, self.fail, self.close_error = values, fail, close_error
        self.calls, self.closed = [], False

    def read_ranges(self, ranges, **kwargs):
        self.calls.append((ranges, kwargs))
        if self.fail:
            raise self.fail
        return self.values

    def close(self):
        self.closed = True
        if self.close_error:
            raise StorageError("unknown connection cleanup")


def export(dataset, target, store, selections=None, **kwargs):
    return export_stored_auxiliary(
        dataset,
        target,
        [SELECTION] if selections is None else selections,
        store_factory=lambda: store,
        clock=lambda: 10_000_000,
        **kwargs,
    )


def test_export_uses_exact_snapshot_before_read_and_retains_sparse_revision(dataset, tmp_path):
    store, target = Store([stored()]), tmp_path / "selected"
    report = export(dataset, target, store, as_of_ms=70000)
    manifest, rows, observations = load_auxiliary_dataset(target)
    ranges, args = store.calls[0]
    assert (ranges[0].start_ms, ranges[0].end_ms) == (0, 3600000)
    assert args == {"as_of_ms": 70000, "limit": 10000}
    assert len(rows) == report["rows"] == 120
    assert observations == (stored(),)
    assert report["requested_minute_slots"] == 60
    assert report["scope"] == "retrospective_training_only"
    assert manifest["raw_payloads"] == []
    assert manifest["export_provenance"]["data"]["as_of_ms"] == 70000
    assert store.closed
    # Original files are no longer needed by the completed portable bundle.
    (dataset / "dataset.json").write_text("{}")
    assert load_auxiliary_dataset(target)[2] == observations


@pytest.mark.parametrize("selections", [[], [SELECTION] * 2, [{}], [SELECTION | {"extra": 1}]])
def test_invalid_selection_opens_no_store_or_dataset(dataset, tmp_path, selections):
    store, target = Store(), tmp_path / "denied"
    with pytest.raises(ValidationError):
        export(dataset, target, store, selections)
    assert not target.exists() and not store.calls


@pytest.mark.parametrize("cutoff", [True, -1, 10_000_001, "100"])
def test_future_or_invalid_cutoff_rejected_before_io(dataset, tmp_path, cutoff):
    store, target = Store(), tmp_path / "denied"
    with pytest.raises(ValidationError):
        export(dataset, target, store, as_of_ms=cutoff)
    assert not target.exists() and not store.calls


@pytest.mark.parametrize("failure", ["read", "close", "future", "unselected"])
def test_failed_read_never_writes_complete_dataset(dataset, tmp_path, failure):
    store = Store([stored()])
    if failure == "read":
        store.fail = StorageError("read failed")
    elif failure == "close":
        store.close_error = True
    elif failure == "future":
        store.values = [stored(available=8_000_000)]
    else:
        store.values = [stored(source="not-selected")]
    target = tmp_path / "denied"
    with pytest.raises((ValidationError, StorageError)):
        export(dataset, target, store, as_of_ms=70000)
    assert store.closed
    assert not (target / "dataset.json").exists()


def test_missing_instrument_opens_no_database(dataset, tmp_path):
    store = Store()
    with pytest.raises(ValidationError, match="absent"):
        export(
            dataset,
            tmp_path / "denied",
            store,
            [SELECTION | {"instrument": "binance:spot:BTCUSDT"}],
        )
    assert not store.calls and not store.closed


def test_original_selection_cutoff_survives_zero_observations_and_tampering_fails(
    dataset, tmp_path
):
    target = tmp_path / "empty"
    export(dataset, target, Store())
    manifest, _, values = load_auxiliary_dataset(target)
    assert not values
    assert manifest["export_provenance"]["data"]["selections"] == [SELECTION]
    manifest["export_provenance"]["data"]["as_of_ms"] -= 1
    (target / "dataset.json").write_text(json.dumps(manifest))
    with pytest.raises(ValidationError, match="provenance"):
        load_auxiliary_dataset(target)


def test_original_observations_cannot_be_joined_to_generated_prices(dataset, tmp_path):
    generated = tmp_path / "generated"
    monte_carlo(dataset, generated, {"seed": 42, "block_bars": 2})
    store = Store([stored()])
    with pytest.raises(ValidationError, match="before generating"):
        export(generated, tmp_path / "denied", store)
    assert not store.calls


def test_actual_spawn_dataset_inspection_supports_auxiliary_and_legacy(dataset, tmp_path):
    selected = tmp_path / "selected"
    export(dataset, selected, Store([stored()]))
    service = ResearchService(tmp_path / "research")
    try:
        for path, expected in ((selected, True), (dataset, False)):
            job = service.submit("inspect_dataset", {"dataset": str(path)})
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                result = service.status(job["id"])
                if result["state"] in {"complete", "failed", "interrupted", "cancelled"}:
                    break
                time.sleep(0.05)
            assert result["state"] == "complete", result
            summary = json.loads(Path(result["result_file"]).read_text())
            assert summary == result["result"]
            assert summary["rows"] == 120 and summary["time_range"] == [0, 3600000]
            assert bool(summary["auxiliary"]) == expected
    finally:
        service.close()
