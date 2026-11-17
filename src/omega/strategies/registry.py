"""Manifest discovery is data-only. Selecting a trusted external module executes Python."""

# Imports
import hashlib
import importlib
import importlib.util
import json
import re
import sys
from copy import deepcopy
from importlib.resources import files
from pathlib import Path
from types import ModuleType

from omega.domain.errors import AuthorizationDenied, ValidationError
from omega.domain.models import Instrument
from omega.ports.modules import ModuleDescriptor


class StrategyRegistry:
    def __init__(self, external_dir: Path | None = None, *, trust_external: bool = False):
        self.external_dir = Path(external_dir).resolve() if external_dir else None
        self.trust_external = trust_external

    @staticmethod
    def _manifest(raw: str, expected: str):
        if len(raw.encode("utf-8")) > 65_536:
            raise ValidationError("TS manifest exceeds limit")
        data = json.loads(raw)
        required = {"schema_version", "api_version", "module_id", "label", "factory"}
        allowed = required | {
            "validation_entry",
            "description",
            "models",
            "default_configuration",
            "configuration_schema",
        }
        if not isinstance(data, dict) or not required <= data.keys() or data.keys() - allowed:
            raise ValidationError("invalid TS manifest fields")
        if (
            type(data["schema_version"]) is not int
            or data["schema_version"] != 1
            or str(data["api_version"]) != "1"
        ):
            raise ValidationError("unsupported TS manifest version")
        if data["module_id"] != expected or not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", expected):
            raise ValidationError("TS module identity must match its folder")
        for name in ("factory", "validation_entry"):
            if name in data and (
                not isinstance(data[name], str)
                or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_./]*:[A-Za-z_][A-Za-z0-9_]*", data[name])
            ):
                raise ValidationError("invalid TS Python entrypoint")
        if not isinstance(data["label"], str) or not 1 <= len(data["label"]) <= 128:
            raise ValidationError("invalid TS label")
        if "default_configuration" in data and not isinstance(data["default_configuration"], dict):
            raise ValidationError("default TS configuration must be an object")
        if "configuration_schema" in data and not isinstance(data["configuration_schema"], dict):
            raise ValidationError("TS configuration schema must be an object")
        return data

    def _discover(self):
        discovered = {}
        for directory in sorted(files("omega.strategies").iterdir(), key=lambda item: item.name):
            if not directory.is_dir():
                continue
            identity = directory.name
            resource = directory.joinpath("manifest.json")
            if not resource.is_file():
                continue
            manifest = self._manifest(resource.read_text(encoding="utf-8"), identity)
            discovered[identity] = (manifest, None)
        if self.external_dir and self.external_dir.is_dir():
            for directory in sorted(self.external_dir.iterdir()):
                if (
                    not directory.is_dir()
                    or directory.is_symlink()
                    or directory.is_junction()
                    or directory.name in discovered
                ):
                    continue
                path = directory / "manifest.json"
                if not path.is_file() or path.is_symlink():
                    continue
                try:
                    if path.stat().st_size > 65_536:
                        raise ValidationError("TS manifest exceeds limit")
                    manifest = self._manifest(path.read_text(encoding="utf-8"), directory.name)
                    discovered[directory.name] = (manifest, directory)
                except (OSError, ValueError, TypeError, ValidationError):
                    discovered[directory.name] = (
                        {
                            "module_id": directory.name,
                            "label": directory.name,
                            "status": "invalid",
                            "error": "INVALID_MANIFEST",
                        },
                        directory,
                    )
        return discovered

    def list_modules(self):
        result = []
        for manifest, directory in self._discover().values():
            item = deepcopy(manifest)
            item.update(
                {
                    "id": manifest["module_id"],
                    "origin": "external" if directory else "builtin",
                    "trusted": directory is None or self.trust_external,
                }
            )
            item.setdefault("status", "available" if item["trusted"] else "trust_required")
            result.append(item)
        return result

    def _selected(self, identity):
        selected = self._discover().get(identity)
        if selected is None or selected[0].get("status") == "invalid":
            raise ValidationError("unknown or invalid TS module")
        manifest, directory = selected
        if directory is not None and not self.trust_external:
            raise AuthorizationDenied(
                "external TS modules execute arbitrary Python; explicit trust required"
            )
        return manifest, directory

    @staticmethod
    def _entry(entry, directory):
        module_name, attribute = entry.split(":")
        if directory is None:
            module = importlib.import_module(module_name)
        else:
            relative = Path(module_name)
            path = (directory / relative).resolve()
            if (
                relative.is_absolute()
                or relative.suffix != ".py"
                or not path.is_relative_to(directory.resolve())
                or not path.is_file()
            ):
                raise ValidationError("external TS entry must name a local Python file")
            package_name = "_omega_ts_" + hashlib.sha256(str(directory).encode()).hexdigest()[:16]
            if package_name not in sys.modules:
                package = ModuleType(package_name)
                package.__path__ = [str(directory)]
                sys.modules[package_name] = package
            name = package_name + "." + path.stem
            if name not in sys.modules:
                spec = importlib.util.spec_from_file_location(name, path)
                if spec is None or spec.loader is None:
                    raise ValidationError("external TS module cannot be loaded")
                module = importlib.util.module_from_spec(spec)
                sys.modules[name] = module
                try:
                    spec.loader.exec_module(module)
                except Exception:
                    sys.modules.pop(name, None)
                    raise
            module = sys.modules[name]
        function = getattr(module, attribute, None)
        if not callable(function):
            raise ValidationError("TS entrypoint is not callable")
        return function

    def create(self, identity: str, instrument: Instrument, configuration: dict | None = None):
        manifest, directory = self._selected(identity)
        if configuration is not None:
            if not isinstance(configuration, dict):
                raise ValidationError("TS configuration must be an object")
            try:
                if len(json.dumps(configuration, allow_nan=False).encode("utf-8")) > 65_536:
                    raise ValidationError("TS configuration exceeds limit")
            except (TypeError, ValueError):
                raise ValidationError("TS configuration must be finite JSON data") from None
        factory = self._entry(manifest["factory"], directory)
        module = factory(
            instrument,
            deepcopy(
                configuration
                if configuration is not None
                else manifest.get("default_configuration", {})
            ),
        )
        descriptor = getattr(module, "descriptor", None)
        if (
            not isinstance(descriptor, ModuleDescriptor)
            or descriptor.module_id != identity
            or str(descriptor.api_version) != "1"
        ):
            raise ValidationError("TS instance descriptor does not match manifest")
        if (
            not isinstance(descriptor.required_fields, frozenset)
            or not all(
                isinstance(key, str) and 1 <= len(key) <= 128 for key in descriptor.required_fields
            )
            or type(descriptor.stateful) is not bool
        ):
            raise ValidationError("invalid TS instance descriptor fields")
        if not all(
            callable(getattr(module, name, None)) for name in ("evaluate", "snapshot", "resume")
        ):
            raise ValidationError("TS instance is missing a required interface")
        return module

    def validator(self, identity):
        manifest, directory = self._selected(identity)
        entry = manifest.get("validation_entry")
        return self._entry(entry, directory) if entry else None


def list_modules():
    return StrategyRegistry().list_modules()


def create(identity: str, instrument: Instrument, configuration: dict | None = None):
    return StrategyRegistry().create(identity, instrument, configuration)
