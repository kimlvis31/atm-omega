"""Training views enrich only explicitly selected observation datasets."""

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from omega.domain.errors import ValidationError
from omega.ports.auxiliary import StoredAuxiliary
from omega.research.artifacts import child_path, read_status_json
from omega.research.auxiliary_datasets import (
    SCHEMA_ID as OBSERVATION_SCHEMA,
)
from omega.research.auxiliary_datasets import (
    load_auxiliary_dataset,
    snapshot_auxiliary_dataset,
)
from omega.research.datasets import FIELDS, MarketRow, load_dataset


@dataclass(frozen=True)
class EnrichedMarketRow(MarketRow):
    auxiliary: tuple[StoredAuxiliary, ...] = ()

    def __post_init__(self):
        super().__post_init__()
        if (
            type(self.auxiliary) is not tuple
            or len(self.auxiliary) > 64
            or any(
                not isinstance(row, StoredAuxiliary)
                or row.instrument.key != self.instrument
                or row.observation.window_start_ms != self.open_time_ms
                for row in self.auxiliary
            )
        ):
            raise ValidationError("invalid one-minute training observation join")


def load_training_inputs(source, cancel=None):
    source = Path(source)
    path = source / "dataset.json" if source.is_dir() else source
    header = read_status_json(path)
    if header.get("schema_id") != OBSERVATION_SCHEMA:
        manifest, rows = load_dataset(source, cancel)
        return manifest, rows, None
    auxiliary_manifest, rows, observations = load_auxiliary_dataset(source, cancel)
    manifest, _ = load_dataset(
        child_path(path.parent, auxiliary_manifest["candles"]["manifest"]), cancel
    )
    groups = defaultdict(list)
    for stored in observations:
        groups[stored.instrument.key, stored.observation.window_start_ms].append(stored)
    enriched = [
        EnrichedMarketRow(
            *(getattr(row, name) for name in FIELDS),
            tuple(
                sorted(
                    groups[row.instrument, row.open_time_ms],
                    key=lambda item: (
                        item.observation.channel,
                        item.observation.source,
                        item.revision,
                    ),
                )
            ),
        )
        for row in rows
    ]
    identity = {
        "schema_id": OBSERVATION_SCHEMA,
        "manifest_sha256": hashlib.sha256(
            json.dumps(
                auxiliary_manifest, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode()
        ).hexdigest(),
        "observations_sha256": auxiliary_manifest["observations"]["sha256"],
        "channels": auxiliary_manifest["channels"],
        "quality": auxiliary_manifest["quality"],
        "transform": auxiliary_manifest.get("transform"),
        "scope": "retrospective_training_only",
        "availability_policy": (
            "Actual availability is retained; module owns preprocessing, no causal claim"
        ),
    }
    return manifest, enriched, identity


def snapshot_training_inputs(source, destination, cancel=None):
    manifest, rows, identity = load_training_inputs(source, cancel)
    if identity is None:
        return manifest, rows, None
    target = Path(destination) / "input-observations"
    snapshot_auxiliary_dataset(source, target, cancel)
    copied_manifest, copied_rows, copied_identity = load_training_inputs(target, cancel)
    if copied_identity != identity or copied_manifest != manifest or copied_rows != rows:
        raise ValidationError("training observation dataset changed while snapshotting")
    return copied_manifest, copied_rows, copied_identity
