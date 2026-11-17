"""Pure tensor contracts shared by research, runtime and desktop controls.

The historic class name remains compatible. No artifacts, plugins, framework or
worker lifecycle are imported by this value/validation boundary.
"""

import math
import re
from dataclasses import dataclass


def _name(value):
    return type(value) is str and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is not None


def _shape(shape, *, dynamic):
    return (
        type(shape) is tuple
        and 1 <= len(shape) <= 5
        and all(
            value is None and dynamic and index == 0 or type(value) is int and 1 <= value <= 4096
            for index, value in enumerate(shape)
        )
        and math.prod(value or 1 for value in shape) <= 100_000
    )


def _tensor(value, shape, *, batch=None, nullable=False):
    if not shape:
        if value is None and nullable:
            return
        if type(value) not in {int, float} or not math.isfinite(value):
            raise ValueError("shadow tensor must contain finite numeric values")
        return
    if type(value) is not list:
        raise ValueError("shadow tensor shape mismatch")
    count = batch if shape[0] is None else shape[0]
    if count is None:
        count = len(value)
        if not 1 <= count <= 256:
            raise ValueError("shadow batch size exceeds capacity")
    if len(value) != count:
        raise ValueError("shadow tensor shape mismatch")
    for row in value:
        _tensor(row, shape[1:], nullable=nullable)


@dataclass(frozen=True, slots=True)
class ShadowModelContract:
    schema_id: str
    features: tuple[str, ...]
    lookback: int
    input_shape: tuple[int | None, ...]
    output_shape: tuple[int | None, ...] = (None,)
    state_shape: tuple[int, ...] | None = None
    state_version: str | None = None
    allow_no_signal: bool = False

    def __post_init__(self):
        if (
            not _name(self.schema_id)
            or type(self.features) is not tuple
            or not 1 <= len(self.features) <= 256
            or any(type(v) is not str or not 1 <= len(v) <= 128 for v in self.features)
            or len(set(self.features)) != len(self.features)
            or type(self.lookback) is not int
            or not 1 <= self.lookback <= 4096
            or not _shape(self.input_shape, dynamic=True)
            or not _shape(self.output_shape, dynamic=True)
            or self.input_shape[0] is not None
            or self.output_shape[0] is not None
            or type(self.allow_no_signal) is not bool
            or (self.state_shape is None) != (self.state_version is None)
            or self.state_shape is not None
            and (not _shape(self.state_shape, dynamic=False) or not _name(self.state_version))
        ):
            raise ValueError("invalid explicit shadow model contract")
        if (
            len(self.input_shape) > 3
            or len(self.input_shape) == 1
            and (self.lookback != 1 or len(self.features) != 1)
            or len(self.input_shape) == 2
            and (self.lookback != 1 or self.input_shape[-1] != len(self.features))
            or len(self.input_shape) == 3
            and (
                self.input_shape[-2] != self.lookback or self.input_shape[-1] != len(self.features)
            )
        ):
            raise ValueError("shadow feature order/width or lookback shape mismatch")

    def document(self):
        return {
            "schema_id": self.schema_id,
            "features": list(self.features),
            "lookback": self.lookback,
            "input_shape": list(self.input_shape),
            "output_shape": list(self.output_shape),
            "state_shape": None if self.state_shape is None else list(self.state_shape),
            "state_version": self.state_version,
            "allow_no_signal": self.allow_no_signal,
        }

    @classmethod
    def parse(cls, value):
        keys = {
            "schema_id",
            "features",
            "lookback",
            "input_shape",
            "output_shape",
            "state_shape",
            "state_version",
            "allow_no_signal",
        }
        if (
            type(value) is not dict
            or set(value) != keys
            or any(type(value[k]) is not list for k in ("features", "input_shape", "output_shape"))
            or value["state_shape"] is not None
            and type(value["state_shape"]) is not list
        ):
            raise ValueError("invalid serialized shadow contract")
        return cls(
            value["schema_id"],
            tuple(value["features"]),
            value["lookback"],
            tuple(value["input_shape"]),
            tuple(value["output_shape"]),
            None if value["state_shape"] is None else tuple(value["state_shape"]),
            value["state_version"],
            value["allow_no_signal"],
        )

    def validate_prediction(self, prediction, batch_size):
        if (
            type(prediction) is not dict
            or set(prediction) - {"values", "state"}
            or "values" not in prediction
        ):
            raise ValueError("shadow prediction schema mismatch")
        _tensor(
            prediction["values"], self.output_shape, batch=batch_size, nullable=self.allow_no_signal
        )
        leaves = []

        def collect(value):
            if type(value) is list:
                for item in value:
                    collect(item)
            else:
                leaves.append(value)

        collect(prediction["values"])
        if any(v is None for v in leaves) and not all(v is None for v in leaves):
            raise ValueError("partial null output has no declared shadow meaning")
        state = prediction.get("state")
        if self.state_shape is None:
            if state is not None:
                raise ValueError("stateless shadow model returned recurrent state")
        else:
            _tensor(state, self.state_shape)
        return bool(leaves and leaves[0] is None)
