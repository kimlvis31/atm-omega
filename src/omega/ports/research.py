"""Composition-supplied offline research jobs; runtime implementations stay outside research."""

from pathlib import Path
from typing import Protocol


class ResearchJobHandler(Protocol):
    def __call__(self, partial: Path, final: Path, parameters: dict, cancel) -> dict:
        """Write inside partial and return {result_file: relative path, summary: dict}."""
        ...
