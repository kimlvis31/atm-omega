"""Explicit flat-account retirement authority, never financial reconciliation.

The actor persists its account fence before calling prepare. Original financial
files remain untouched. Native cleanup proof is process-local until this owner
records and verifies the exact closed authority transition.
"""

import json
import os
import re
import stat
import threading
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from omega.application.margin_strategy import MarginStrategyState
from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.runtime.control import InstanceLock
from omega.runtime.margin_cleanup import MarginCleanup, confirmed_cleanup
from omega.runtime.margin_configuration import configuration_parameters
from omega.runtime.margin_ledger import LedgerPages, LedgerQuery
from omega.runtime.margin_store import account_from_document
from omega.runtime.settings import atomic_json
from omega.runtime.virtual_margin_journal import decode_record

MAX_BYTES = 16_000_000
MAX_FILES = 40_102
MAX_SOURCE_BYTES = 128_000_000
ARCHIVE_SCOPE = "virtual_margin_retirement_archive"
SCOPE = "virtual_margin_retirement"
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_SHA = re.compile(r"[a-f0-9]{64}\Z")
PLAN_SCOPE = "virtual_margin_retirement_plan"
MAX_PLAN_BYTES = 1_000_000


@dataclass(frozen=True)
class RetirementPlan:
    document: dict
    archive: dict


def registered_session_configuration(parameters, model):
    """Pure original Session configuration projection; no module validation IO."""
    fee = parameters.get("fee_rate", "0.001")
    if type(fee) not in {str, int}:
        raise ValidationError("registered session fee needs its original decimal representation")
    value = Decimal(fee)
    if not value.is_finite():
        raise ValidationError("registered session fee must be finite")
    configuration = {
        "account": deepcopy(parameters["account"]),
        "strategies": deepcopy(parameters["strategies"]),
        "start_ms": parameters["start_ms"],
        "fee_rate": str(value),
        "model": model,
    }
    for key in ("funding", "cost_policy"):
        if parameters.get(key) is not None:
            configuration[key] = deepcopy(parameters[key])
    return configuration


def _json(value):
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError, RecursionError) as error:
        raise ValidationError("invalid retirement JSON") from error
    if len(raw) > MAX_BYTES:
        raise ValidationError("retirement document exceeds16MB capacity")
    return raw


def retirement_sha256(value):
    return sha256(_json(value)).hexdigest()


def _seal(value):
    return {**deepcopy(value), "sha256": retirement_sha256(value)}


def _checked(value, scope):
    if type(value) is not dict:
        raise ProtocolError("invalid retirement envelope")
    body = {key: member for key, member in value.items() if key != "sha256"}
    if (
        value.get("scope") != scope
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("sha256") != retirement_sha256(body)
    ):
        raise ProtocolError("retirement envelope checksum or scope differs")
    return value


def _identity(value):
    if type(value) is not str or _ID.fullmatch(value) is None:
        raise ValidationError("invalid retirement identity")
    return value


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ProtocolError("duplicate retirement JSON field")
        result[key] = value
    return result


class MarginRetirement:
    """One worker-owned retirement; the actor bounds total active transactions."""

    def __init__(self, root, account_id):
        self.root, self.account_id = Path(root).absolute(), _identity(account_id)
        self._thread = threading.get_ident()
        self.name = sha256(account_id.encode()).hexdigest()
        directory = self.root / "state/margin-retired"
        self.path = directory / (self.name + ".json")
        self.archive_path = directory / (self.name + ".archive.json")
        self._session = None
        self._recovery_cleanup = None
        self._close_failed = False
        self._idle_cleanup = MarginCleanup()
        self._pending_closed = None

    def cleanup_status(self):
        """Process-local handle observation, never a retired receipt."""
        if self._session is not None:
            return self._session.cleanup_status()
        return (self._recovery_cleanup or self._idle_cleanup).status()

    def close_resources(self):
        """Owner shutdown cleanup only; it cannot finalize account retirement."""
        self._owner()
        if self._session is not None:
            self._session.close()
        else:
            (self._recovery_cleanup or self._idle_cleanup).close()

    def _owner(self):
        if threading.get_ident() != self._thread:
            raise TradingBlocked("retirement belongs to its original worker")
        self._guard(self.path)

    def _guard(self, path):
        if not path.is_relative_to(self.root):
            raise StorageError("retirement path escaped its namespace")
        for candidate in (path, *path.parents):
            if candidate.is_symlink() or candidate.is_junction():
                raise StorageError("retirement cannot use linked paths")

    def _bytes(self, path, *, missing=False):
        self._guard(path)
        descriptor = None
        try:
            descriptor = os.open(
                path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
            )
            details = os.fstat(descriptor)
            if (
                not stat.S_ISREG(details.st_mode)
                or details.st_size > MAX_BYTES
                or getattr(details, "st_file_attributes", 0) & 0x400
            ):
                raise StorageError("retirement input must be a bounded regular file")
            with os.fdopen(descriptor, "rb") as stream:
                descriptor = None
                raw = stream.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise StorageError("retirement input exceeds capacity")
            return raw
        except FileNotFoundError:
            if missing:
                return None
            raise StorageError("retirement evidence is missing") from None
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _read(self, path, scope):
        raw = self._bytes(path, missing=True)
        if raw is None:
            return None
        try:
            return _checked(json.loads(raw, object_pairs_hook=_unique), scope)
        except (ValueError, TypeError, RecursionError) as error:
            raise ProtocolError("retirement evidence is malformed") from error

    def read(self):
        self._owner()
        document = self._read(self.path, SCOPE)
        if document is not None:
            required = {
                "schema_version",
                "scope",
                "account_id",
                "state",
                "request",
                "registered_parameters_sha256",
                "session_config_hash",
                "observation_sha256",
                "archive_sha256",
                "cleanup_generation",
                "release_evidence",
                "sha256",
            }
            if (
                set(document) != required
                or document["account_id"] != self.account_id
                or document["state"] not in {"prepared", "closed", "retired"}
            ):
                raise ProtocolError("retirement authority identity or fields differ")
            if (document["release_evidence"] is None) != (document["state"] == "prepared"):
                raise ProtocolError("retirement release phase differs")
            if (
                any(
                    type(document[key]) is not str or _SHA.fullmatch(document[key]) is None
                    for key in (
                        "registered_parameters_sha256",
                        "session_config_hash",
                        "observation_sha256",
                        "archive_sha256",
                    )
                )
                or type(document["cleanup_generation"]) is not str
                or re.fullmatch(r"[a-f0-9]{32}", document["cleanup_generation"]) is None
            ):
                raise ProtocolError("retirement authority hash identity differs")
            request = document["request"]
            if (
                type(request) is not dict
                or set(request) != {"operation_id", "core_epoch", "issued_at_ms"}
                or type(request["issued_at_ms"]) is not int
                or not 0 <= request["issued_at_ms"] <= 253402300799000
            ):
                raise ProtocolError("retirement authority original request differs")
            _identity(request["operation_id"])
            _identity(request["core_epoch"])
            evidence = document["release_evidence"]
            if evidence is not None:
                if (
                    type(evidence) is not dict
                    or set(evidence) != {"method", "generation", "resources"}
                    or evidence["method"]
                    not in {"owned_native_cleanup", "recovery_exclusive_reacquire"}
                ):
                    raise ProtocolError("invalid retirement native release evidence")
                status = {
                    "schema_version": 1,
                    "scope": "process_local_margin_cleanup",
                    "generation": evidence["generation"],
                    "logical_closed": True,
                    "state": "complete",
                    "resources": evidence["resources"],
                }
                if (
                    not confirmed_cleanup(status)
                    or len(evidence["resources"]) < 3
                    or evidence["method"] == "owned_native_cleanup"
                    and evidence["generation"] != document["cleanup_generation"]
                ):
                    raise ProtocolError("retirement native release evidence is unconfirmed")
        return document

    def archive(self):
        self._owner()
        authority = self.read()
        document = self._read(self.archive_path, ARCHIVE_SCOPE)
        if (
            authority is None
            or document is None
            or document["sha256"] != authority["archive_sha256"]
        ):
            raise ProtocolError("retirement archive does not match its authority")
        self._validate_archive(document)
        if (
            any(
                authority[key] != document[key]
                for key in ("account_id", "registered_parameters_sha256", "session_config_hash")
            )
            or authority["observation_sha256"] != retirement_sha256(document["observation"])
            or authority["cleanup_generation"] != document["observation"]["cleanup_generation"]
        ):
            raise ProtocolError("retirement archive observation differs from authority")
        if (
            authority["release_evidence"] is not None
            and sorted(row["name"] for row in authority["release_evidence"]["resources"])
            != document["observation"]["cleanup_resource_names"]
        ):
            raise ProtocolError("retirement did not release every original resource")
        return document

    def _cas(self, path, scope, previous, candidate):
        current = self._read(path, scope)
        if current == candidate:
            return deepcopy(candidate)
        if current != previous:
            raise ProtocolError("retirement compare-and-swap evidence changed")
        self._guard(path)
        _json(candidate)
        atomic_json(path, candidate)
        self._guard(path)
        if self._read(path, scope) != candidate:
            raise StorageError("retirement acknowledgement remains unconfirmed")
        return deepcopy(candidate)

    def _source_paths(self, strategy_ids):
        files = [
            self.root / "state/margin-sessions" / (self.name + ".json"),
            self.root / "state/margin-v2" / (self.name + ".json"),
        ]
        shutdown = self.root / "state/margin-shutdown" / (self.name + ".json")
        self._guard(shutdown)
        if shutdown.exists():
            files.append(shutdown)
        strategy_root = (
            self.root / "state/margin-strategy-accounts" / self.name / "state/margin-strategies"
        )
        files.extend(
            strategy_root / (sha256(identity.encode()).hexdigest() + ".json")
            for identity in strategy_ids
        )
        for kind in ("margin-v2-journal", "margin-v2-fallback"):
            directory = self.root / "state" / kind / self.name
            self._guard(directory)
            if not directory.exists():
                continue
            for index, path in enumerate(directory.iterdir()):
                if index >= 20000:
                    raise StorageError("retirement source inventory exceeds capacity")
                if not (
                    path.name.startswith(".stage-")
                    or path.suffix == ".json"
                    and _SHA.fullmatch(path.stem)
                ):
                    raise ProtocolError("unexpected retirement source file")
                files.append(path)
        if len(files) > MAX_FILES:
            raise StorageError("retirement source inventory exceeds capacity")
        return sorted(files)

    def _inventory(self, strategy_ids):
        rows, total = [], 0
        for path in self._source_paths(strategy_ids):
            raw = self._bytes(path)
            total += len(raw)
            if total > MAX_SOURCE_BYTES:
                raise StorageError("retirement source bytes exceed bounded verification budget")
            rows.append(
                {
                    "path": path.relative_to(self.root).as_posix(),
                    "size": len(raw),
                    "sha256": sha256(raw).hexdigest(),
                }
            )
        return rows

    def _validate_archive(self, document):
        required = {
            "schema_version",
            "scope",
            "account_id",
            "registered_parameters",
            "registered_parameters_sha256",
            "session_config_hash",
            "ledger_namespace",
            "observation",
            "account",
            "source_files",
            "source_inventory_sha256",
            "sha256",
        }
        if set(document) != required or document["account_id"] != self.account_id:
            raise ProtocolError("retirement archive identity or fields differ")
        _, digest = configuration_parameters(
            document["registered_parameters"], account_id=self.account_id
        )
        observation = document["observation"]
        observed_fields = {
            "schema_version",
            "scope",
            "account_id",
            "registered_parameters_sha256",
            "session_config_hash",
            "ledger_namespace",
            "account_sha256",
            "source",
            "ledger_snapshot",
            "ledger_total",
            "session_metadata_sha256",
            "shutdown_sha256",
            "shutdown_metadata_sha256",
            "strategies",
            "paused",
            "orders",
            "staged",
            "cleanup_generation",
            "cleanup_resource_names",
        }
        if (
            type(observation) is not dict
            or set(observation) != observed_fields
            or type(observation["schema_version"]) is not int
            or observation["schema_version"] != 1
            or observation.get("scope") != "virtual_margin_retirement_observation"
            or observation.get("account_id") != self.account_id
            or observation.get("registered_parameters_sha256") != digest
            or document["registered_parameters_sha256"] != digest
            or observation.get("session_config_hash") != document["session_config_hash"]
            or observation.get("ledger_namespace") != document["ledger_namespace"]
            or observation.get("paused") is not True
            or type(observation.get("orders")) is not int
            or observation["orders"] != 0
            or type(observation.get("staged")) is not int
            or observation["staged"] != 0
            or "account" in observation
            or retirement_sha256(document.get("account")) != observation.get("account_sha256")
            or retirement_sha256(document["source_files"]) != document["source_inventory_sha256"]
        ):
            raise ProtocolError("retirement archive is not the confirmed flat observation")
        account = account_from_document(document["account"])
        if (
            account.account_id != self.account_id
            or any(row.quantity != 0 for row in account.positions)
            or len(account.ledger) != observation.get("ledger_total")
        ):
            raise ProtocolError("retirement archive account or ledger differs")
        strategies = observation.get("strategies")
        expected = {row["id"] for row in document["registered_parameters"]["strategies"]}
        if (
            type(strategies) is not list
            or len(strategies) != len(expected)
            or {row.get("id") for row in strategies} != expected
            or any(row.get("pending") is not False for row in strategies)
        ):
            raise ProtocolError("retirement strategy observations differ")
        if any(
            set(row) != {"id", "revision", "bootstrap_sha256", "checkpoint_sha256", "pending"}
            or type(row["revision"]) is not int
            or not 0 <= row["revision"] <= 1000000
            or any(
                type(row[key]) is not str or _SHA.fullmatch(row[key]) is None
                for key in ("bootstrap_sha256", "checkpoint_sha256")
            )
            for row in strategies
        ):
            raise ProtocolError("retirement strategy checkpoint identities are invalid")
        names = sorted(
            [
                "session_lease",
                "journal",
                "account_store",
                *("strategy_store:" + identity for identity in expected),
            ]
        )
        if (
            observation.get("cleanup_resource_names") != names
            or type(observation.get("cleanup_generation")) is not str
            or re.fullmatch(r"[a-f0-9]{32}", observation["cleanup_generation"]) is None
        ):
            raise ProtocolError("retirement resource identity differs")
        source = observation.get("source")
        if (
            type(source) is not dict
            or set(source) != {"anchor_sha256", "tip_sha256", "count", "chain_sha256"}
            or type(source["count"]) is not int
            or not 1 <= source["count"] <= 20000
            or any(
                type(source[key]) is not str or _SHA.fullmatch(source[key]) is None
                for key in ("anchor_sha256", "tip_sha256", "chain_sha256")
            )
        ):
            raise ProtocolError("retirement source identity is invalid")
        expected_namespace = retirement_sha256(
            (os.path.normcase(str(self.root.resolve())), document["session_config_hash"])
        )
        if document["ledger_namespace"] != expected_namespace:
            raise ProtocolError("retirement archive belongs to another namespace")
        page = LedgerPages(document["ledger_namespace"]).page(
            source["anchor_sha256"],
            account,
            LedgerQuery(limit=1, snapshot=observation["ledger_snapshot"]),
        )
        if page["ledger_total"] != len(account.ledger):
            raise ProtocolError("retirement ledger snapshot is not the complete prefix")

    def _envelope_data(self, path):
        envelope = json.loads(self._bytes(path), object_pairs_hook=_unique)
        if (
            type(envelope) is not dict
            or set(envelope) != {"data", "sha256"}
            or retirement_sha256(envelope["data"]) != envelope["sha256"]
        ):
            raise ProtocolError("retirement original data envelope differs")
        return envelope["data"]

    def _verify_observed_sources(self, archive):
        """Pure codecs bind frozen cache observations to their original files."""
        observation = archive["observation"]
        account_path = self.root / "state/margin-v2" / (self.name + ".json")
        if self._envelope_data(account_path) != archive["account"]:
            raise ProtocolError("retirement account file differs from confirmed observation")
        metadata = self._envelope_data(self.root / "state/margin-sessions" / (self.name + ".json"))
        if (
            retirement_sha256(metadata) != observation["session_metadata_sha256"]
            or metadata.get("pending") is not None
            or metadata.get("config_hash") != archive["session_config_hash"]
        ):
            raise ProtocolError("retirement session metadata differs from confirmed observation")
        configuration = metadata.get("configuration")
        if (
            type(configuration) is not dict
            or registered_session_configuration(
                archive["registered_parameters"], configuration.get("model")
            )
            != configuration
            or retirement_sha256(configuration) != archive["session_config_hash"]
        ):
            raise ProtocolError(
                "retirement registered parameters differ from original session configuration"
            )
        shutdown_path = self.root / "state/margin-shutdown" / (self.name + ".json")
        self._guard(shutdown_path)
        shutdown = self._envelope_data(shutdown_path) if shutdown_path.exists() else None
        if retirement_sha256(shutdown) != observation["shutdown_metadata_sha256"]:
            raise ProtocolError("retirement shutdown metadata differs from confirmed observation")
        base = self.root / "state/margin-strategy-accounts" / self.name / "state/margin-strategies"
        for row in observation["strategies"]:
            document = self._envelope_data(
                base / (sha256(row["id"].encode()).hexdigest() + ".json")
            )
            checkpoint = MarginStrategyState.parse(document)
            if (
                checkpoint.account_id != self.account_id
                or checkpoint.strategy_id != row["id"]
                or checkpoint.pending is not None
                or checkpoint.revision != row["revision"]
                or checkpoint.bootstrap_digest != row["bootstrap_sha256"]
                or retirement_sha256(checkpoint.document()) != row["checkpoint_sha256"]
            ):
                raise ProtocolError("retirement strategy file differs from confirmed observation")
        records = {}
        for item in archive["source_files"]:
            relative = Path(item["path"])
            if (
                len(relative.parts) != 4
                or relative.parts[1] not in {"margin-v2-journal", "margin-v2-fallback"}
                or relative.name.startswith(".stage-")
            ):
                continue
            record = decode_record(self._bytes(self.root / relative))
            if (
                record.account_id != self.account_id
                or record.record_id != relative.stem
                or record.sequence in records
                and records[record.sequence] != record
            ):
                raise ProtocolError("retirement journal source identity differs")
            records[record.sequence] = record
        ordered = [records[index] for index in sorted(records)]
        if (
            not ordered
            or [record.sequence for record in ordered] != list(range(1, len(ordered) + 1))
            or any(
                record.previous_checksum != previous.checksum
                for previous, record in zip(ordered, ordered[1:], strict=False)
            )
        ):
            raise ProtocolError("retirement source chain is incomplete")
        source = {
            "anchor_sha256": ordered[0].checksum,
            "tip_sha256": ordered[-1].checksum,
            "count": len(ordered),
            "chain_sha256": retirement_sha256(
                tuple((record.sequence, record.record_id, record.checksum) for record in ordered)
            ),
        }
        if source != observation["source"]:
            raise ProtocolError("retirement journal files differ from confirmed source tip")

    def make_plan(self, request, registered_parameters, observation):
        self._owner()
        if type(request) is not dict or set(request) != {
            "operation_id",
            "core_epoch",
            "issued_at_ms",
        }:
            raise ValidationError("retirement needs its frozen original request")
        _identity(request["operation_id"])
        _identity(request["core_epoch"])
        if (
            type(request["issued_at_ms"]) is not int
            or not 0 <= request["issued_at_ms"] <= 253402300799000
        ):
            raise ValidationError("invalid retirement issue time")
        parameters, parameters_sha = configuration_parameters(
            registered_parameters, account_id=self.account_id
        )
        inventory = self._inventory(sorted(row["id"] for row in parameters["strategies"]))
        summary = {key: deepcopy(value) for key, value in observation.items() if key != "account"}
        archive = _seal(
            {
                "schema_version": 1,
                "scope": ARCHIVE_SCOPE,
                "account_id": self.account_id,
                "registered_parameters": parameters,
                "registered_parameters_sha256": parameters_sha,
                "session_config_hash": observation["session_config_hash"],
                "ledger_namespace": observation["ledger_namespace"],
                "observation": summary,
                "account": deepcopy(observation["account"]),
                "source_files": inventory,
                "source_inventory_sha256": retirement_sha256(inventory),
            }
        )
        self._validate_archive(archive)
        self._verify_observed_sources(archive)
        document = _seal(
            {
                "schema_version": 1,
                "scope": PLAN_SCOPE,
                "account_id": self.account_id,
                "request": deepcopy(request),
                "registered_parameters_sha256": parameters_sha,
                "session_config_hash": archive["session_config_hash"],
                "ledger_namespace": archive["ledger_namespace"],
                "observation": summary,
                "source_files": inventory,
                "source_inventory_sha256": archive["source_inventory_sha256"],
                "archive_sha256": archive["sha256"],
            }
        )
        if len(_json(document)) > MAX_PLAN_BYTES:
            raise ValidationError("retirement plan exceeds1MB registry capacity")
        return RetirementPlan(document, archive)

    def _validate_plan(self, document):
        _checked(document, PLAN_SCOPE)
        fields = {
            "schema_version",
            "scope",
            "account_id",
            "request",
            "registered_parameters_sha256",
            "session_config_hash",
            "ledger_namespace",
            "observation",
            "source_files",
            "source_inventory_sha256",
            "archive_sha256",
            "sha256",
        }
        if (
            set(document) != fields
            or document["account_id"] != self.account_id
            or len(_json(document)) > MAX_PLAN_BYTES
        ):
            raise ProtocolError("retirement fence plan differs")
        if retirement_sha256(document["source_files"]) != document["source_inventory_sha256"]:
            raise ProtocolError("retirement plan inventory differs")
        return document

    def prepare(self, plan):
        self._owner()
        if type(plan) is not RetirementPlan:
            raise ValidationError("retirement requires its original typed plan")
        document = self._validate_plan(plan.document)
        archive = _checked(plan.archive, ARCHIVE_SCOPE)
        self._validate_archive(archive)
        if archive["sha256"] != document["archive_sha256"] or any(
            archive[key] != document[key]
            for key in (
                "account_id",
                "registered_parameters_sha256",
                "session_config_hash",
                "ledger_namespace",
                "observation",
                "source_files",
                "source_inventory_sha256",
            )
        ):
            raise ProtocolError("retirement archive differs from its frozen fence plan")
        original = self.read()
        if original is not None:
            if (
                original["request"] != document["request"]
                or original["archive_sha256"] != archive["sha256"]
            ):
                raise ProtocolError("retirement request conflicts with its original")
            self.archive()
            return original
        self._check_sources(archive)
        self._cas(self.archive_path, ARCHIVE_SCOPE, None, archive)
        authority = _seal(
            {
                "schema_version": 1,
                "scope": SCOPE,
                "account_id": self.account_id,
                "state": "prepared",
                "request": deepcopy(document["request"]),
                "registered_parameters_sha256": document["registered_parameters_sha256"],
                "session_config_hash": document["session_config_hash"],
                "observation_sha256": retirement_sha256(document["observation"]),
                "archive_sha256": archive["sha256"],
                "cleanup_generation": document["observation"]["cleanup_generation"],
                "release_evidence": None,
            }
        )
        return self._cas(self.path, SCOPE, None, authority)

    def _check_sources(self, archive):
        strategies = sorted(row["id"] for row in archive["registered_parameters"]["strategies"])
        if self._inventory(strategies) != archive["source_files"]:
            raise ProtocolError("retirement source evidence changed after its frozen observation")
        self._verify_observed_sources(archive)

    def _closed(self, current, status, method):
        if not confirmed_cleanup(status):
            raise StorageError("retirement native resource release remains unconfirmed")
        if (
            sorted(row["name"] for row in status["resources"])
            != self.archive()["observation"]["cleanup_resource_names"]
        ):
            raise ProtocolError("retirement resource release set differs")
        body = {key: value for key, value in current.items() if key != "sha256"}
        body.update(
            state="closed",
            release_evidence={
                "method": method,
                "generation": status["generation"],
                "resources": deepcopy(status["resources"]),
            },
        )
        candidate = _seal(body)
        self._pending_closed = (deepcopy(current), deepcopy(candidate))
        return self._cas(self.path, SCOPE, current, candidate)

    def close(self, session):
        self._owner()
        current = self.read()
        if current is None:
            raise TradingBlocked("retirement must be prepared before resource close")
        if current["state"] != "prepared":
            return current
        archive = self.archive()
        self._check_sources(archive)
        if self._session is not None and self._session is not session:
            raise ProtocolError("retirement cannot replace its original closing session")
        if self._pending_closed is not None:
            original, candidate = self._pending_closed
            if current != original:
                raise ProtocolError("retirement pending closed transition changed")
            return self._cas(self.path, SCOPE, original, candidate)
        if getattr(session, "account_id", None) != self.account_id:
            raise ProtocolError("retirement session belongs to another account")
        self._session = session
        status = session.cleanup_status()
        if status.get("generation") != current["cleanup_generation"]:
            raise ProtocolError("retirement cleanup generation changed")
        try:
            session.close()
        except Exception:
            self._close_failed = True
            raise
        status = session.cleanup_status()
        if not confirmed_cleanup(status, generation=current["cleanup_generation"]):
            raise StorageError("retirement original native cleanup remains unconfirmed")
        self._check_sources(archive)
        return self._closed(current, status, "owned_native_cleanup")

    def finish(self):
        self._owner()
        current = self.read()
        if current is None or current["state"] == "prepared":
            raise TradingBlocked("retirement requires confirmed closed evidence")
        self._check_sources(self.archive())
        if current["state"] == "retired":
            return current
        body = {key: value for key, value in current.items() if key != "sha256"}
        body["state"] = "retired"
        return self._cas(self.path, SCOPE, current, _seal(body))

    def recover(self, fence_document, registered_parameters):
        """Explicit fenced recovery without constructing a financial Session."""
        self._owner()
        plan_document = self._validate_plan(fence_document)
        parameters, parameters_sha = configuration_parameters(
            registered_parameters, account_id=self.account_id
        )
        if parameters_sha != plan_document["registered_parameters_sha256"]:
            raise ProtocolError("retirement recovery registered parameters changed")
        current = self.read()
        if current is not None and (
            current["request"] != plan_document["request"]
            or current["archive_sha256"] != plan_document["archive_sha256"]
        ):
            raise ProtocolError("retirement recovery belongs to another original fence")
        if current is not None and current["state"] != "prepared":
            return self.finish()
        if self._pending_closed is not None:
            original, candidate = self._pending_closed
            self._cas(self.path, SCOPE, original, candidate)
            return self.finish()
        if self._session is not None:
            self.close(self._session)
            return self.finish()
        if self._recovery_cleanup is None:
            cleanup = self._recovery_cleanup = MarginCleanup()
            paths = [
                (
                    "session_lease",
                    self.root / "state/margin-sessions" / (self.name + ".lock"),
                    True,
                ),
                (
                    "journal",
                    self.root / "state/locks" / ("margin-v2-journal-" + self.name + ".lock"),
                    False,
                ),
                ("account_store", self.root / "state/margin-v2" / (self.name + ".lock"), False),
            ]
            base = (
                self.root / "state/margin-strategy-accounts" / self.name / "state/margin-strategies"
            )
            paths.extend(
                (
                    "strategy_store:" + row["id"],
                    base / (sha256(row["id"].encode()).hexdigest() + ".lock"),
                    False,
                )
                for row in plan_document["observation"]["strategies"]
            )
            try:
                for name, path, final in paths:
                    self._guard(path)
                    lease = InstanceLock(path)
                    cleanup.add(
                        name, lease, final=final, released=lambda lease=lease: lease.released
                    )
            except Exception:
                # Preserve ownership and mark partial acquisition unusable;
                # another explicit recovery must not treat this as a full set.
                self._recovery_acquisition_failed = True
                raise
        if getattr(self, "_recovery_acquisition_failed", False):
            raise StorageError(
                "retirement recovery lease acquisition is incomplete; process recovery required"
            )
        if (
            self._inventory(sorted(row["id"] for row in parameters["strategies"]))
            != plan_document["source_files"]
        ):
            raise ProtocolError("retirement recovery source evidence changed")
        account_path = self.root / "state/margin-v2" / (self.name + ".json")
        envelope = json.loads(self._bytes(account_path), object_pairs_hook=_unique)
        if (
            type(envelope) is not dict
            or set(envelope) != {"data", "sha256"}
            or retirement_sha256(envelope["data"]) != envelope["sha256"]
        ):
            raise ProtocolError("retirement recovery account envelope differs")
        # Existing strict codec checks funding variants and conservation without
        # a financial owner, journal write, graph or plugin constructor.
        account_from_document(envelope["data"])
        archive = _seal(
            {
                "schema_version": 1,
                "scope": ARCHIVE_SCOPE,
                "account_id": self.account_id,
                "registered_parameters": parameters,
                "registered_parameters_sha256": parameters_sha,
                "session_config_hash": plan_document["session_config_hash"],
                "ledger_namespace": plan_document["ledger_namespace"],
                "observation": deepcopy(plan_document["observation"]),
                "account": envelope["data"],
                "source_files": deepcopy(plan_document["source_files"]),
                "source_inventory_sha256": plan_document["source_inventory_sha256"],
            }
        )
        self.prepare(RetirementPlan(deepcopy(plan_document), archive))
        current = self.read()
        self._check_sources(archive)
        self._recovery_cleanup.close()
        self._check_sources(archive)
        self._closed(current, self._recovery_cleanup.status(), "recovery_exclusive_reacquire")
        return self.finish()
