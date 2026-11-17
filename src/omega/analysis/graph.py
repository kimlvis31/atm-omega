"""Shared, transactional feature dependency graph."""

from copy import deepcopy
from dataclasses import dataclass
from math import lcm

from omega.domain.errors import ValidationError
from omega.domain.models import Candle
from omega.ports.modules import ModuleDescriptor

MAX_NULLABLE_FIELDS = 1024
MAX_FIELD_NAME = 128


def _nullable_fields(nodes):
    result = set()
    for node in nodes:
        fields = getattr(node.feature, "nullable_fields", frozenset())
        if (
            type(fields) is not frozenset
            or len(fields) > MAX_NULLABLE_FIELDS
            or any(
                type(field) is not str
                or not 1 <= len(field) <= MAX_FIELD_NAME
                or not field.isprintable()
                or field.strip() != field
                for field in fields
            )
        ):
            raise ValidationError("feature nullable fields require bounded frozen names")
        result.update(
            fields if node.public_fields is None else fields.intersection(node.public_fields)
        )
        if len(result) > MAX_NULLABLE_FIELDS:
            raise ValidationError("graph nullable field budget exceeded")
    return frozenset(result)


def _calculation_ready(feature):
    value = getattr(feature, "calculation_ready", True)
    if type(value) is not bool:
        raise ValidationError("feature calculation readiness must be boolean")
    return value


@dataclass(frozen=True)
class FeatureNode:
    name: str
    feature: object
    dependencies: tuple[str, ...] = ()
    public_fields: tuple[str, ...] | None = None


def evaluate_nodes(nodes, candle, frame=None, cached_outputs=None):
    """One transactional DAG pass; frame-aware nodes remain optional adapters."""
    candidate, outputs, flat, readiness, raw_outputs = deepcopy(nodes), {}, {}, {}, {}
    for node in candidate:
        dependencies = {name: dict(outputs[name]) for name in node.dependencies}
        if node.dependencies:
            operation = getattr(node.feature, "update_frame_with", None) if frame else None
            if callable(operation):
                value = operation(frame, dependencies)
            elif cached_outputs is not None:
                value = deepcopy(cached_outputs[node.name])
            else:
                value = node.feature.update_with(candle, dependencies)
        else:
            operation = getattr(node.feature, "update_frame", None) if frame else None
            if callable(operation):
                value = operation(frame)
            elif cached_outputs is not None:
                value = deepcopy(cached_outputs[node.name])
            else:
                value = node.feature.update(candle)
        if not isinstance(value, dict):
            raise ValidationError("feature output must be a mapping")
        _nullable_fields((node,))
        _calculation_ready(node.feature)
        if node.public_fields is not None and set(node.public_fields) - value.keys():
            raise ValidationError("feature publication references missing fields")
        if frame is not None:
            raw_outputs[node.name] = deepcopy(value)
        ready = bool(getattr(node.feature, "input_ready", True)) and all(
            readiness[name] for name in node.dependencies
        )
        readiness[node.name] = ready
        if frame is not None and not ready:
            # Keep held legacy numbers inside the provider for diagnostics only.
            # Downstream nodes and consumers must not receive them as usable data.
            value = {key: False if key.startswith("CLOSED_") else None for key in value}
        published = (
            value if node.public_fields is None else {key: value[key] for key in node.public_fields}
        )
        if flat.keys() & published.keys():
            raise ValidationError("feature output names collide")
        outputs[node.name] = value
        flat.update(published)
    _nullable_fields(candidate)
    return candidate, flat, raw_outputs


class FeatureGraph:
    """Explicit DAG. Each node is evaluated once per immutable closed minute.

    Nodes implement update(candle) or update_with(candle, dependency_results).
    Node state must be small, copyable calculation state; never sockets or DBs.
    """

    descriptor = ModuleDescriptor("feature_graph", "1", frozenset({"OHLCV"}), True)

    def __init__(self, nodes: tuple[FeatureNode, ...]):
        by_name = {n.name: n for n in nodes}
        if not nodes or len(by_name) != len(nodes):
            raise ValidationError("unique feature nodes required")
        ordered = []
        while by_name:
            available = {n.name for n in ordered}
            ready = [n for n in by_name.values() if set(n.dependencies) <= available]
            if not ready:
                raise ValidationError("cyclic or missing feature dependency")
            for node in ready:
                ordered.append(node)
                del by_name[node.name]
        _nullable_fields(ordered)
        for node in ordered:
            _calculation_ready(node.feature)
        self._nodes = ordered
        self._last: Candle | None = None
        self._output: dict = {}

    def update(self, candle: Candle) -> dict:
        if candle == self._last:
            return dict(self._output)
        candidate, flat, _ = evaluate_nodes(self._nodes, candle)
        self._nodes, self._last, self._output = candidate, candle, flat
        return dict(flat)

    def checkpoint(self):
        from omega.analysis.checkpoint import checkpoint

        return checkpoint(self)

    @property
    def last_open_time_ms(self):
        return self._last.open_time_ms if self._last else None

    @property
    def last_candle(self):
        return self._last

    @property
    def output_values(self):
        return deepcopy(self._output)

    @property
    def nullable_fields(self):
        return _nullable_fields(self._nodes)

    @property
    def calculation_ready(self):
        states = tuple(_calculation_ready(node.feature) for node in self._nodes)
        input_ready = getattr(self, "input_ready", True)
        if type(input_ready) is not bool:
            raise ValidationError("graph input readiness must be boolean")
        return self._last is not None and all(states) and input_ready

    @property
    def warmup_alignment_minutes(self):
        return lcm(*(getattr(node.feature, "warmup_alignment_minutes", 1) for node in self._nodes))

    def restored(self, state, instrument):
        from omega.analysis.checkpoint import restore

        return restore(self, state, instrument)
