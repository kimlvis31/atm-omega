"""Shared, transactional feature dependency graph."""

from copy import deepcopy
from dataclasses import dataclass

from omega.domain.errors import ValidationError
from omega.domain.models import Candle
from omega.ports.modules import ModuleDescriptor


@dataclass(frozen=True)
class FeatureNode:
    name: str
    feature: object
    dependencies: tuple[str, ...] = ()


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
        self._nodes = ordered
        self._last: Candle | None = None
        self._output: dict = {}

    def update(self, candle: Candle) -> dict:
        if candle == self._last:
            return dict(self._output)
        candidate, outputs, flat = deepcopy(self._nodes), {}, {}
        for node in candidate:
            if node.dependencies:
                value = node.feature.update_with(
                    candle, {name: dict(outputs[name]) for name in node.dependencies}
                )
            else:
                value = node.feature.update(candle)
            if flat.keys() & value.keys():
                raise ValidationError("feature output names collide")
            outputs[node.name] = value
            flat.update(value)
        self._nodes, self._last, self._output = candidate, candle, flat
        return dict(flat)
