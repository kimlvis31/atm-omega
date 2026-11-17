"""Small data-only training metadata for deployment forms; no model import/hash.

This is a preview, not candidate verification. The deployment worker still
verifies checkpoint bytes and executes the explicitly trusted fixture in a child.
"""

import json
import re
from copy import deepcopy
from pathlib import Path

from omega import __version__
from omega.research.artifacts import child_path, read_status_json
from omega.research.deployment import ShadowModelContract


def training_view(directory, training_job_id):
    if type(training_job_id) is not str or re.fullmatch("[a-f0-9]{32}", training_job_id) is None:
        raise ValueError("invalid completed training job identity")
    directory = Path(directory)
    run = read_status_json(directory / "run.json")
    if len(json.dumps(run, allow_nan=False).encode()) > 1_048_576:
        raise ValueError("training metadata exceeds preview capacity")
    descriptor, config = run.get("module"), run.get("config")
    if (
        run.get("state") != "complete"
        or run.get("cleanup_completed") is not True
        or type(descriptor) is not dict
        or type(config) is not dict
        or descriptor.get("core_version") != __version__
        or type(descriptor.get("api_version")) is not int
        or descriptor["api_version"] != 1
    ):
        raise ValueError("completed compatible training metadata is required")
    selected = (run.get("model") or {}).get("file")
    artifacts = [
        row
        for row in run.get("artifacts", [])
        if type(row) is dict and row.get("kind") == "checkpoint" and row.get("file") == selected
    ]
    if len(artifacts) != 1 or selected in {"run.json", "instance_module.py"}:
        raise ValueError("one registered selected checkpoint is required")
    child_path(directory, selected)
    checkpoint = {"file": selected, "sha256": artifacts[0].get("sha256")}
    if (
        type(checkpoint["sha256"]) is not str
        or re.fullmatch("[a-f0-9]{64}", checkpoint["sha256"]) is None
    ):
        raise ValueError("invalid registered checkpoint checksum")

    # Known examples have stable documented tensor contracts. All other modules
    # must declare a machine-readable contract or receive explicit user shapes.
    defaults = None
    inputs = descriptor.get("input") or {}
    explicit = (descriptor.get("inference") or {}).get("contract")
    if explicit is not None:
        defaults = ShadowModelContract.parse(explicit).document()
    elif descriptor.get("module_id") == "omega.demo.linear_neuron":
        defaults = ShadowModelContract(
            inputs["schema_id"], ("normalized_volume",), 1, (None,)
        ).document()
    elif (
        descriptor.get("module_id") == "omega.example.pytorch_return"
        and config.get("architecture", "mlp") == "mlp"
    ):
        lookback = config.get("lookback", 4)
        features = tuple(inputs.get("features", ()))
        defaults = ShadowModelContract(
            inputs["schema_id"], features, lookback, (None, lookback, len(features))
        ).document()
    if defaults is not None and (
        defaults["schema_id"] != inputs.get("schema_id")
        or inputs.get("features", defaults["features"]) != defaults["features"]
        or config.get("lookback", defaults["lookback"]) != defaults["lookback"]
    ):
        raise ValueError("declared contract differs from actual training schema")
    return deepcopy(
        {
            "schema_version": 1,
            "training_job_id": training_job_id,
            "module": descriptor,
            "config": config,
            "device": run.get("device", {}),
            "checkpoint": checkpoint,
            "contract_defaults": defaults,
            "shape_confirmation_required": defaults is None,
            "verification": "metadata_only; candidate and fixture verification required",
        }
    )
