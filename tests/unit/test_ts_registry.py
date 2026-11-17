"""TS discovery is non-executable; trusted local factories obey the public contract."""

import json

import pytest

from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market
from omega.strategies.registry import StrategyRegistry


def instrument():
    return Instrument("binance", Market.USDM, "BTCUSDT")


def external(tmp_path, *, identity="custom", factory="module.py:create"):
    folder = tmp_path / identity
    folder.mkdir()
    manifest = {
        "schema_version": 1,
        "api_version": "1",
        "module_id": identity,
        "label": "Custom TS",
        "factory": factory,
    }
    (folder / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return folder


def test_builtin_families_are_separate_and_create_typed_instances():
    registry = StrategyRegistry()
    assert {item["id"] for item in registry.list_modules()} == {
        "tef",
        "target_quantity",
        "neural_signal",
    }
    for identity in ("tef", "target_quantity", "neural_signal"):
        instance = registry.create(identity, instrument())
        assert instance.descriptor.module_id == identity
        assert callable(instance.evaluate)
    assert len(next(item for item in registry.list_modules() if item["id"] == "tef")["models"]) == 6


def test_discovery_never_imports_external_code_and_requires_explicit_trust(tmp_path):
    folder = external(tmp_path)
    marker = tmp_path / "executed"
    (folder / "module.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('executed')\n"
        "raise RuntimeError('loaded')\n",
        encoding="utf-8",
    )
    registry = StrategyRegistry(tmp_path)
    assert (
        next(item for item in registry.list_modules() if item["id"] == "custom")["status"]
        == "trust_required"
    )
    assert not marker.exists()
    with pytest.raises(PermissionError):
        registry.create("custom", instrument())
    assert not marker.exists()
    with pytest.raises(RuntimeError):
        StrategyRegistry(tmp_path, trust_external=True).create("custom", instrument())
    assert marker.exists()


def test_selected_external_factory_supports_relative_helpers(tmp_path):
    folder = external(tmp_path)
    (folder / "helpers.py").write_text("QUANTITY = '2'\n", encoding="utf-8")
    (folder / "module.py").write_text(
        "from .helpers import QUANTITY\n"
        "from omega.strategies.target_quantity.module import TargetQuantityModule\n"
        "from omega.ports.modules import ModuleDescriptor\n"
        "def create(instrument, configuration):\n"
        "    module = TargetQuantityModule(instrument, {'quantity': QUANTITY})\n"
        "    module.descriptor = ModuleDescriptor('custom', '1', frozenset(), True)\n"
        "    return module\n",
        encoding="utf-8",
    )
    instance = StrategyRegistry(tmp_path, trust_external=True).create("custom", instrument())
    assert instance.snapshot()["quantity"] == "2"


@pytest.mark.parametrize("factory", ["../outside.py:create", "/outside.py:create", "module:create"])
def test_external_factory_cannot_escape_its_own_directory(tmp_path, factory):
    external(tmp_path, factory=factory)
    with pytest.raises(ValidationError):
        StrategyRegistry(tmp_path, trust_external=True).create("custom", instrument())


def test_invalid_manifest_reported_and_cannot_override_builtin(tmp_path):
    folder = external(tmp_path)
    (folder / "manifest.json").write_text("{}", encoding="utf-8")
    registry = StrategyRegistry(tmp_path, trust_external=True)
    assert (
        next(item for item in registry.list_modules() if item["id"] == "custom")["status"]
        == "invalid"
    )
    with pytest.raises(ValidationError):
        registry.create("custom", instrument())
    builtin = external(tmp_path, identity="tef")
    (builtin / "module.py").write_text("raise RuntimeError('must not load')", encoding="utf-8")
    assert registry.create("tef", instrument()).descriptor.module_id == "tef"


def test_invalid_instance_descriptor_is_rejected(tmp_path):
    folder = external(tmp_path)
    (folder / "module.py").write_text(
        "from omega.strategies.target_quantity.module import create\n", encoding="utf-8"
    )
    with pytest.raises(ValidationError, match="descriptor"):
        StrategyRegistry(tmp_path, trust_external=True).create("custom", instrument())


@pytest.mark.parametrize("configuration", [[], {"x": float("nan")}, {"x": "x" * 65_537}])
def test_configuration_is_bounded_finite_json_before_factory_load(configuration):
    with pytest.raises(ValidationError):
        StrategyRegistry().create("target_quantity", instrument(), configuration)
