"""Actual offline inspection validates retained V2 revisions without a training join."""

import json
import time
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.ports.auxiliary import AuxiliaryReadRange, StoredAuxiliary
from omega.research.auxiliary_datasets import export_auxiliary_dataset, load_auxiliary_dataset
from omega.research.service import ResearchService
from omega.research.training_inputs import load_training_inputs


def test_actual_worker_inspection_preserves_all_v2_revisions_and_rejects_training_join(
    dataset, tmp_path
):
    instrument = Instrument("binance", Market.USDM, "BTCUSDT")
    source = instrument.key + ":open_interest_history"
    first = AuxiliaryInput(
        "open_interest",
        (("METRIC_OPENINTEREST", Decimal(1000)), ("SOURCE_PERIOD_MS", 300000)),
        0,
        60000,
        0,
        60001,
        "observed",
        Decimal(1),
        source,
    )
    first_sha = observation_checksum(encode_observation(instrument, first))
    revised = replace(
        first,
        available_at_ms=60002,
        values=(
            ("METRIC_OPENINTEREST", Decimal(1100)),
            ("SOURCE_PERIOD_MS", 300000),
        ),
    )
    records = (
        StoredAuxiliary(instrument, first, 1, first_sha, None),
        StoredAuxiliary(
            instrument,
            revised,
            2,
            observation_checksum(encode_observation(instrument, revised)),
            first_sha,
        ),
    )
    target = tmp_path / "native-v2"
    manifest = export_auxiliary_dataset(
        dataset,
        target,
        records,
        format_version=2,
        source_ranges=(AuxiliaryReadRange(instrument, "open_interest", source, 0, 3600000),),
        metric_lookback_ms=0,
        as_of_ms=10000000,
    )
    assert load_auxiliary_dataset(target)[2] == records
    with pytest.raises(ValueError, match="unsupported or incomplete dataset manifest"):
        load_training_inputs(target)  # no seed/revision join into V1 NN rows
    service = ResearchService(tmp_path / "research")
    try:
        job = service.submit("inspect_dataset", {"dataset": str(target)})
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            result = service.status(job["id"])
            if result["state"] in {"complete", "failed", "interrupted", "cancelled"}:
                break
            time.sleep(0.05)
        assert result["state"] == "complete", result
        summary = result["result"]
        assert json.loads(Path(result["result_file"]).read_text()) == summary
        assert summary["rows"] == 120 and summary["time_range"] == [0, 3600000]
        identity = summary["auxiliary"]
        assert summary["schema_id"] == identity["schema_id"] == manifest["schema_id"]
        assert identity["scope"] == "recorded_native_inputs_v2"
        assert identity["observations_count"] == 2
        assert identity["observations_sha256"] == manifest["observations"]["sha256"]
        assert identity["selection"] == manifest["selection"]
        assert identity["channels"] == manifest["channels"]
        assert identity["quality"] == manifest["quality"]
        assert identity["transform"] is None
        assert "payload" not in identity and "raw_payloads" not in identity
        assert load_auxiliary_dataset(target)[2] == records
    finally:
        service.close()
