"""Quality-aware DAG for immutable OHLCV and auxiliary minute observations.

The existing candle-only graph/checkpoint is unchanged. Using this graph does
not start a collector or claim that an auxiliary channel exists in storage.
"""

from copy import deepcopy

from omega.analysis.aggregation import validate_minute
from omega.analysis.graph import FeatureGraph, FeatureNode, evaluate_nodes
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Instrument
from omega.ports.modules import ModuleDescriptor


class MarketFeatureGraph(FeatureGraph):
    def __init__(self, instrument, nodes):
        if (
            not isinstance(instrument, Instrument)
            or not isinstance(nodes, (tuple, list))
            or not 1 <= len(nodes) <= 64
        ):
            raise ValidationError("a market graph requires an instrument and bounded nodes")
        for node in nodes:
            if (
                not isinstance(node, FeatureNode)
                or not isinstance(node.name, str)
                or not 1 <= len(node.name) <= 80
                or not isinstance(node.dependencies, tuple)
                or any(not isinstance(name, str) for name in node.dependencies)
                or not callable(
                    getattr(node.feature, "update_with" if node.dependencies else "update", None)
                )
            ):
                raise ValidationError("market graph nodes require a name and calculation interface")
            descriptor = getattr(node.feature, "descriptor", self.descriptor)
            if (
                not isinstance(descriptor, ModuleDescriptor)
                or not isinstance(descriptor.required_fields, frozenset)
                or any(
                    not isinstance(field, str) or not field for field in descriptor.required_fields
                )
            ):
                raise ValidationError("market graph node descriptor must declare named inputs")
        super().__init__(nodes)
        self.instrument = instrument
        self._last_frame = None
        self._node_outputs = {}
        inputs = frozenset({"OHLCV"}).union(
            *(
                getattr(node.feature, "descriptor", self.descriptor).required_fields
                for node in nodes
            )
        )
        self.descriptor = ModuleDescriptor("market_feature_graph", "1", inputs, True)

    @property
    def maximum_metric_age_ms(self):
        """Public configured metric horizon, independent of current readiness or IO."""
        ages = []
        for node in self._nodes:
            descriptor = getattr(node.feature, "descriptor", None)
            if descriptor is None or "metric" not in descriptor.required_fields:
                continue
            age = getattr(node.feature, "maximum_metric_age_ms", None)
            if type(age) is not int or not 0 <= age <= 86400000:
                raise ValidationError("metric feature lacks a valid configured native age policy")
            ages.append(age)
        return max(ages) if ages else None

    @property
    def last_input_frame(self):
        """Immutable selected input, for checkpoint-to-manifest consistency checks."""
        return self._last_frame

    @property
    def input_quality(self):
        result = {}
        for node in self._nodes:
            result[node.name] = {
                "ready": bool(getattr(node.feature, "input_ready", True))
                and all(result[name]["ready"] for name in node.dependencies),
                "details": deepcopy(getattr(node.feature, "input_quality", {})),
            }
        return result

    @property
    def input_ready(self):
        return self._last_frame is not None and all(
            row["ready"] for row in self.input_quality.values()
        )

    def update(self, candle):
        return self.update_frame(MarketFeatureFrame(candle, candle.close_time_ms))

    def update_frame(self, frame):
        if not isinstance(frame, MarketFeatureFrame):
            raise ValidationError("an immutable market frame is required")
        candle = frame.candle
        if (
            type(candle.open_time_ms) is not int
            or type(candle.close_time_ms) is not int
            or candle.open_time_ms % 60000
            or candle.close_time_ms - candle.open_time_ms != 60000
        ):
            raise ValidationError("market graph requires aligned minute candles")
        validate_minute(self.instrument, self._last, candle)
        previous = self._last_frame
        if previous is not None:
            if frame.as_of_ms < previous.as_of_ms:
                raise ValidationError("market frame availability cannot go backwards")
            if candle == previous.candle:
                if (
                    frame.inputs != previous.inputs
                    or frame.reference_time_ms != previous.reference_time_ms
                ):
                    raise ValidationError("market graph needs immutable auxiliary observations")
                if frame.as_of_ms == previous.as_of_ms:
                    return dict(self._output)
        cached = self._node_outputs if previous is not None and candle == previous.candle else None
        nodes, output, raw = evaluate_nodes(self._nodes, candle, frame, cached)
        self._nodes, self._last, self._output = nodes, candle, output
        self._node_outputs = raw
        self._last_frame = frame
        return dict(output)

    def validate_checkpoint(self):
        for node in self._nodes:
            feature = node.feature
            cursor = getattr(feature, "last_open_time_ms", None)
            if cursor is None:
                last = getattr(feature, "_last", None)
                last_frame = getattr(feature, "_last_frame", None)
                cursor = (
                    last.open_time_ms
                    if last is not None
                    else (last_frame.candle.open_time_ms if last_frame is not None else None)
                )
            expected_cursor = self._last.open_time_ms if self._last is not None else None
            if cursor != expected_cursor:
                raise ValidationError("market graph node checkpoint cursor mismatch")
        if self._last_frame is None:
            if self._last is not None or self._output or self._node_outputs:
                raise ValidationError("inconsistent empty market graph checkpoint")
            return
        if (
            not isinstance(self._last_frame, MarketFeatureFrame)
            or self._last_frame.candle != self._last
            or self._last.instrument != self.instrument
        ):
            raise ValidationError("market graph checkpoint cursor mismatch")
        if set(self._node_outputs) != {node.name for node in self._nodes}:
            raise ValidationError("market graph node output checkpoint mismatch")
        _, expected, raw = evaluate_nodes(self._nodes, self._last, self._last_frame)
        if expected != self._output or raw != self._node_outputs:
            raise ValidationError("market graph checkpoint output mismatch")
