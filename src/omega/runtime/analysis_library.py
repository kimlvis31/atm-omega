"""Durable CAC templates and instrument-bound CA snapshots, owned by the core.

Configured CA records are not running analyzers. A strategy receives a detached
configuration snapshot; template edits never mutate an existing CA or strategy.
"""

# Imports
import hashlib
import json
import re
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from threading import RLock
from uuid import uuid4

from omega.analysis.aggregation import INTERVAL_MINUTES
from omega.analysis.catalog import FEATURE_CODES, validate_configuration
from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market
from omega.runtime.settings import atomic_json

# Constants
SCHEMA_VERSION = 1
MAX_BYTES = 4_000_000
MAX_TEMPLATES, MAX_INSTANCES, MAX_OPERATIONS = 128, 512, 4096
_ID = re.compile(r"[a-f0-9]{32}\Z")


def _encoded(value) -> bytes:
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise ValidationError("analysis library requires finite JSON values") from exc


def _name(value) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not 1 <= len(value) <= 80
        or not all(character.isprintable() for character in value)
    ):
        raise ValidationError("analysis name must contain 1–80 printable characters")
    return value


def _identifier(value) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ValidationError("invalid analysis library ID")
    return value


def _instrument(value) -> str:
    if not isinstance(value, str) or len(value) > 100:
        raise ValidationError("invalid CA instrument")
    parts = value.split(":")
    if len(parts) != 3 or parts[0] != "binance" or parts[1] not in {"spot", "usdm"}:
        raise ValidationError("CA currently supports Binance spot and USDM only")
    return Instrument(parts[0], Market(parts[1]), parts[2]).key


def _digest(configuration: dict) -> str:
    return hashlib.sha256(_encoded(configuration)).hexdigest()


class AnalysisLibrary:
    """One core writer; optimistic revisions and durable operation receipts.

    ``references_provider`` returns the *current* bound strategy/position records
    as ``[{analysis_id, strategy_id}, ...]``. Missing or malformed authority blocks
    CA deletion. References count instances, never only accounts.
    """

    def __init__(self, path: Path, *, references_provider: Callable | None = None):
        self.path = Path(path)
        self.references_provider = references_provider
        self.lock = RLock()
        self.degraded = False
        self.data = {
            "schema_version": SCHEMA_VERSION,
            "revision": 0,
            "templates": {},
            "instances": {},
            "operations": {},
        }
        if self.path.exists():
            if self.path.stat().st_size > MAX_BYTES:
                raise ValidationError("analysis library exceeds file limit")
            candidate = json.loads(self.path.read_text(encoding="utf-8"))
            _encoded(candidate)
            self._validate(candidate)
            self.data = candidate

    @staticmethod
    def capabilities() -> dict:
        return {
            "features": list(FEATURE_CODES),
            "intervals": [
                {"interval_id": iid, "minutes": minutes}
                for iid, minutes in INTERVAL_MINUTES.items()
            ],
            "max_definitions": 64,
            "max_templates": MAX_TEMPLATES,
            "max_instances": MAX_INSTANCES,
            "execution_subscription": False,
            "snapshot_binding": True,
        }

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "schema_version": SCHEMA_VERSION,
                "revision": self.data["revision"],
                "templates": deepcopy(list(self.data["templates"].values())),
                "instances": deepcopy(list(self.data["instances"].values())),
                "capabilities": self.capabilities(),
                "persistence": "degraded" if self.degraded else "ready",
            }

    def snapshot_for_strategy(
        self, analysis_id: str, instrument: str, expected_revision: int | None = None
    ) -> dict:
        with self.lock:
            row = self._get(self.data, "instances", analysis_id)
            if _instrument(instrument) != row["instrument"]:
                raise ValidationError("strategy and CA instruments differ")
            if expected_revision is not None and (
                type(expected_revision) is not int or expected_revision != row["revision"]
            ):
                raise ValidationError("CA revision conflict")
            return deepcopy(
                {
                    "analysis_id": row["id"],
                    "analysis_revision": row["revision"],
                    "template_id": row["template_id"],
                    "template_revision": row["template_revision"],
                    "instrument": row["instrument"],
                    "configuration": row["configuration"],
                    "configuration_hash": _digest(row["configuration"]),
                }
            )

    def snapshot_for_execution(self, analysis_id: str, expected_revision: int) -> dict:
        """Freeze a saved CA atomically, without trusting a client's instrument."""
        with self.lock:
            row = self._get(self.data, "instances", analysis_id)
            if type(expected_revision) is not int:
                raise ValidationError("a saved CA revision is required")
            return self.snapshot_for_strategy(
                analysis_id, row["instrument"], expected_revision=expected_revision
            )

    def command(self, kind: str, payload: dict, *, operation_id: str) -> dict:
        """Commit state and the receipt together; retries never repeat a mutation."""
        if not isinstance(kind, str) or not isinstance(payload, dict):
            raise ValidationError("invalid analysis operation")
        if not isinstance(operation_id, str) or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,128}", operation_id
        ):
            raise ValidationError("invalid analysis operation ID")
        fingerprint = _digest({"kind": kind, "payload": payload})
        with self.lock:
            if self.degraded:
                raise OSError("analysis library persistence unavailable; restart and reconcile")
            previous = self.data["operations"].get(operation_id)
            if previous is not None:
                if previous["fingerprint"] != fingerprint:
                    raise ValidationError("analysis operation ID reused with different content")
                return deepcopy(previous["result"])
            if (
                type(payload.get("expected_revision")) is not int
                or payload["expected_revision"] != self.data["revision"]
            ):
                raise ValidationError("analysis library revision conflict; refresh first")
            if len(self.data["operations"]) >= MAX_OPERATIONS:
                raise ValidationError(
                    "analysis operation receipt capacity reached; maintenance required"
                )
            candidate = deepcopy(self.data)
            item = self._apply(candidate, kind, payload)
            candidate["revision"] += 1
            result = {
                "schema_version": SCHEMA_VERSION,
                "revision": candidate["revision"],
                "item": deepcopy(item),
            }
            candidate["operations"][operation_id] = {"fingerprint": fingerprint, "result": result}
            self._validate(candidate)
            if len(_encoded(candidate)) > MAX_BYTES:
                raise ValidationError("analysis library exceeds file limit")
            try:
                atomic_json(self.path, candidate)
            except OSError:
                self.degraded = True
                raise
            self.data = candidate
            return deepcopy(result)

    @staticmethod
    def _get(data: dict, group: str, identity: str) -> dict:
        identity = _identifier(identity)
        if identity not in data[group]:
            raise ValidationError("analysis library item not found")
        return data[group][identity]

    @staticmethod
    def _unique_name(data: dict, group: str, name: str, except_id="") -> str:
        name = _name(name)
        if any(
            row["id"] != except_id and row["name"].casefold() == name.casefold()
            for row in data[group].values()
        ):
            raise ValidationError("analysis name already exists")
        return name

    def _references(self, analysis_id: str) -> list[str]:
        if self.references_provider is None:
            raise ValidationError("strategy reference authority unavailable; CA deletion blocked")
        references = self.references_provider()
        if (
            not isinstance(references, (list, tuple))
            or len(references) > 10000
            or any(
                not isinstance(row, dict)
                or not isinstance(row.get("analysis_id"), str)
                or not isinstance(row.get("strategy_id"), str)
                for row in references
            )
        ):
            raise ValidationError("invalid strategy reference authority; CA deletion blocked")
        return [row["strategy_id"] for row in references if row["analysis_id"] == analysis_id]

    def _apply(self, data: dict, kind: str, payload: dict) -> dict:
        parts = kind.split(".")
        if len(parts) != 3 or parts[0] != "analysis" or parts[1] not in {"template", "instance"}:
            raise ValidationError("unknown analysis library operation")
        group = "templates" if parts[1] == "template" else "instances"
        action = parts[2]
        fields = {
            "create": {"name", "configuration"}
            if group == "templates"
            else {"name", "instrument", "template_id"},
            "update": {"id", "name", "configuration"},
            "clone": {"id", "name"},
            "rename": {"id", "name"},
            "delete": {"id"},
            "rebase": {"id", "template_id"},
        }
        if action not in fields or set(payload) - (fields[action] | {"expected_revision"}):
            raise ValidationError("unknown analysis library parameter")
        required = fields[action] - ({"name"} if action == "update" else set())
        if required - payload.keys():
            raise ValidationError("missing analysis library parameter")
        if (
            group == "instances"
            and action in {"update", "clone"}
            or group == "templates"
            and action == "rebase"
        ):
            raise ValidationError("unsupported analysis library operation")
        if action == "create":
            identity = uuid4().hex
            limit = MAX_TEMPLATES if group == "templates" else MAX_INSTANCES
            if len(data[group]) >= limit:
                raise ValidationError("analysis library item limit reached")
            row = {
                "id": identity,
                "name": self._unique_name(data, group, payload.get("name")),
                "revision": 1,
            }
            if group == "templates":
                row["configuration"] = validate_configuration(payload.get("configuration"))
            else:
                template = self._get(data, "templates", payload.get("template_id"))
                row.update(
                    instrument=_instrument(payload.get("instrument")),
                    template_id=template["id"],
                    template_revision=template["revision"],
                    configuration=deepcopy(template["configuration"]),
                    status="configured",
                )
            data[group][identity] = row
            return row
        row = self._get(data, group, payload.get("id"))
        if action == "delete":
            if group == "templates" and any(
                ca["template_id"] == row["id"] for ca in data["instances"].values()
            ):
                raise ValidationError("CAC is referenced by a CA; remove that CA first")
            if group == "instances" and (references := self._references(row["id"])):
                raise ValidationError(
                    "CA is referenced by strategy instances: " + ", ".join(references)[:500]
                )
            del data[group][row["id"]]
            return {"id": row["id"], "deleted": True}
        if action == "clone":
            if len(data[group]) >= MAX_TEMPLATES:
                raise ValidationError("analysis template limit reached")
            clone = deepcopy(row)
            clone.update(
                id=uuid4().hex, revision=1, name=self._unique_name(data, group, payload.get("name"))
            )
            data[group][clone["id"]] = clone
            return clone
        if action in {"rename", "update"}:
            row["name"] = self._unique_name(
                data, group, payload.get("name", row["name"]), row["id"]
            )
        if action == "update":
            row["configuration"] = validate_configuration(payload.get("configuration"))
        if action == "rebase":
            template = self._get(data, "templates", payload.get("template_id"))
            row.update(
                template_id=template["id"],
                template_revision=template["revision"],
                configuration=deepcopy(template["configuration"]),
            )
        row["revision"] += 1
        return row

    @staticmethod
    def _validate(data: dict) -> None:
        if (
            not isinstance(data, dict)
            or set(data) != {"schema_version", "revision", "templates", "instances", "operations"}
            or type(data["schema_version"]) is not int
            or data["schema_version"] != SCHEMA_VERSION
            or type(data["revision"]) is not int
            or data["revision"] < 0
        ):
            raise ValidationError("invalid analysis library schema/version")
        for group, limit in (("templates", MAX_TEMPLATES), ("instances", MAX_INSTANCES)):
            rows = data[group]
            if not isinstance(rows, dict) or len(rows) > limit:
                raise ValidationError("invalid analysis library collection")
            names = set()
            for identity, row in rows.items():
                if not isinstance(row, dict):
                    raise ValidationError("invalid analysis library record")
                expected = {"id", "name", "revision", "configuration"}
                if group == "instances":
                    expected |= {"instrument", "template_id", "template_revision", "status"}
                if set(row) != expected or _identifier(identity) != row["id"]:
                    raise ValidationError("invalid analysis library record fields")
                name = _name(row["name"]).casefold()
                if name in names or type(row["revision"]) is not int or row["revision"] < 1:
                    raise ValidationError("duplicate name or invalid analysis revision")
                names.add(name)
                if validate_configuration(row["configuration"]) != row["configuration"]:
                    raise ValidationError("analysis configuration is not canonical")
                if group == "instances":
                    _instrument(row["instrument"])
                    _identifier(row["template_id"])
                    if (
                        row["template_id"] not in data["templates"]
                        or row["status"] != "configured"
                        or type(row["template_revision"]) is not int
                        or row["template_revision"] < 1
                        or row["template_revision"]
                        > data["templates"][row["template_id"]]["revision"]
                    ):
                        raise ValidationError("invalid CA template reference")
        operations = data["operations"]
        if not isinstance(operations, dict) or len(operations) > MAX_OPERATIONS:
            raise ValidationError("invalid analysis operation receipts")
        for identity, receipt in operations.items():
            if (
                not isinstance(identity, str)
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", identity)
                or not isinstance(receipt, dict)
                or set(receipt) != {"fingerprint", "result"}
                or not isinstance(receipt["fingerprint"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", receipt["fingerprint"])
                or not isinstance(receipt["result"], dict)
                or set(receipt["result"]) != {"schema_version", "revision", "item"}
                or receipt["result"]["schema_version"] != SCHEMA_VERSION
                or type(receipt["result"]["revision"]) is not int
                or not 1 <= receipt["result"]["revision"] <= data["revision"]
                or not isinstance(receipt["result"]["item"], dict)
            ):
                raise ValidationError("invalid analysis operation receipt")
