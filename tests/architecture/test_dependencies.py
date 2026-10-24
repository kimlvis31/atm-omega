"""Enforce inward dependencies, rather than merely documenting layers."""

import ast
from pathlib import Path


def test_dependency_direction_and_no_legacy_gui_framework_imports():
    forbidden = {
        "domain": {"omega.application", "omega.infrastructure", "omega.runtime", "omega.ports"},
        "ports": {"omega.application", "omega.infrastructure", "omega.runtime"},
        "application": {"omega.infrastructure", "omega.runtime"},
        "analysis": {
            "omega.infrastructure",
            "omega.runtime",
            "omega.application",
            "omega.strategies",
        },
        "strategies": {
            "omega.infrastructure",
            "omega.runtime",
            "omega.application",
            "omega.analysis",
        },
    }
    for path in Path("src/omega").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        layer = path.relative_to("src/omega").parts[0]
        for node in ast.walk(tree):
            imports = (
                [a.name for a in node.names]
                if isinstance(node, ast.Import)
                else [node.module or ""]
                if isinstance(node, ast.ImportFrom)
                else []
            )
            for name in imports:
                assert not any(
                    name == f or name.startswith(f + ".") for f in forbidden.get(layer, set())
                ), (path, name)
                assert not name.startswith(("pyglet", "PySide6", "torch", "AutoTradeMachine")), (
                    path,
                    name,
                )
