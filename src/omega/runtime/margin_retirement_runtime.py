"""Owner-thread retirement orchestration, separate from financial scheduling."""

import json
import math
import re
import threading
import time
from collections import deque
from copy import deepcopy
from dataclasses import dataclass, field

from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.runtime.margin_cleanup import confirmed_cleanup
from omega.runtime.margin_configuration import configuration_parameters
from omega.runtime.margin_retired_history import MarginRetiredHistory
from omega.runtime.margin_retirement import (
    MAX_PLAN_BYTES,
    PLAN_SCOPE,
    MarginRetirement,
    retirement_sha256,
)
from omega.runtime.margin_retirement_client import retirement_response
from omega.runtime.margin_store import account_from_document

MAX_REGISTRY_BYTES = 4_194_304
MAX_RETIRED = 128
MAX_OBSERVATIONS = 8
OBSERVATION_TTL = 60
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_SHA = re.compile(r"[a-f0-9]{64}\Z")
_META = {
    "account_id",
    "state",
    "operation_id",
    "receipt_sha256",
    "registered_parameters_sha256",
    "session_config_hash",
    "ledger_namespace",
    "archive_sha256",
    "ledger_total",
    "reason_code",
}
_INDEX = _META | {"observation_sha256", "plan_sha256", "request", "feed_enabled_before"}


def _identity(value):
    if type(value) is not str or _ID.fullmatch(value) is None:
        raise ValidationError("invalid retirement runtime identity")
    return value


def _checksum(value):
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise ValidationError("invalid retirement runtime checksum")
    return value


def _encoded(value):
    try:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError, RecursionError, OverflowError):
        raise ValidationError("invalid retirement registry JSON") from None
    if len(raw) > MAX_REGISTRY_BYTES:
        raise ValidationError("retirement registry exceeds byte capacity")
    return raw


def _parameters(value):
    if type(value) is not dict or set(value) != {"account_id", "expected_observation_sha256"}:
        raise ValidationError("retirement needs its exact account and observation")
    return {
        "account_id": _identity(value["account_id"]),
        "expected_observation_sha256": _checksum(value["expected_observation_sha256"]),
    }


def _request(value, operation_id):
    if type(value) is not dict or set(value) != {"operation_id", "core_epoch", "issued_at_ms"}:
        raise ValidationError("invalid original retirement request")
    if value["operation_id"] != operation_id:
        raise ValidationError("retirement operation binding differs")
    _identity(value["operation_id"])
    _identity(value["core_epoch"])
    if type(value["issued_at_ms"]) is not int or not 0 <= value["issued_at_ms"] <= 253402300799000:
        raise ValidationError("invalid original retirement issue time")


def _descriptor(value, account_id, operation_id, parameters, configuration):
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
        type(value) is not dict
        or set(value) != fields
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or value["scope"] != PLAN_SCOPE
        or value["account_id"] != account_id
        or len(_encoded(value)) > MAX_PLAN_BYTES
        or value["sha256"] != retirement_sha256({k: v for k, v in value.items() if k != "sha256"})
    ):
        raise ValidationError("invalid sealed retirement descriptor")
    _request(value["request"], operation_id)
    _, digest = configuration_parameters(configuration, account_id=account_id)
    observation = value["observation"]
    if (
        digest != value["registered_parameters_sha256"]
        or type(observation) is not dict
        or "account" in observation
        or observation.get("account_id") != account_id
        or retirement_sha256(observation) != parameters["expected_observation_sha256"]
        or value["source_inventory_sha256"] != retirement_sha256(value["source_files"])
    ):
        raise ValidationError("retirement descriptor observation binding differs")
    if (
        type(observation.get("schema_version")) is not int
        or observation["schema_version"] != 1
        or observation.get("scope") != "virtual_margin_retirement_observation"
        or observation.get("paused") is not True
        or type(observation.get("orders")) is not int
        or observation["orders"] != 0
        or type(observation.get("staged")) is not int
        or observation["staged"] != 0
        or type(observation.get("ledger_total")) is not int
        or not 0 <= observation["ledger_total"] <= 10000
        or type(value["source_files"]) is not list
    ):
        raise ValidationError("retirement descriptor lacks bounded certain observation")
    for name in (
        "registered_parameters_sha256",
        "session_config_hash",
        "ledger_namespace",
        "source_inventory_sha256",
        "archive_sha256",
        "sha256",
    ):
        _checksum(value[name])


def validate_registry(data):
    """Pure bounded schema validation; schema1 has no implicit lifecycle upgrade."""
    if type(data) is not dict or type(data.get("schema_version")) is not int:
        raise ValidationError("invalid retirement registry schema")
    version = data["schema_version"]
    required = {"schema_version", "accounts", "operations"}
    if version == 2:
        required.add("retired")
    if version not in {1, 2} or set(data) != required:
        raise ValidationError("invalid retirement registry fields")
    accounts, operations, retired = data["accounts"], data["operations"], data.get("retired", {})
    if (
        type(accounts) is not dict
        or len(accounts) > 10
        or type(operations) is not dict
        or len(operations) > 128
        or type(retired) is not dict
        or len(retired) > MAX_RETIRED
        or set(accounts) & set(retired)
    ):
        raise ValidationError("retirement registry capacity or account lineage differs")
    if version == 1:
        # The pre-retirement actor also supports injected Session factories
        # with their own configuration contract. Do not retrospectively apply
        # production Session configuration validation to that legacy registry.
        try:
            strategies = sum(len(slot["parameters"]["strategies"]) for slot in accounts.values())
        except (KeyError, TypeError):
            raise ValidationError("invalid legacy margin account registry") from None
        if strategies > 100 or any("retirement_control" in slot for slot in accounts.values()):
            raise ValidationError("invalid legacy margin retirement registry")
        _encoded(data)
        return data
    strategies, reservations = 0, 0
    for account_id, slot in accounts.items():
        _identity(account_id)
        if (
            type(slot) is not dict
            or not {"parameters", "creation_state", "feed_enabled"} <= set(slot)
            or set(slot)
            - {
                "parameters",
                "creation_state",
                "feed_enabled",
                "shutdown_control",
                "retirement_control",
                "retirement_feed_enabled_before",
            }
            or slot["creation_state"] not in {"creating", "created"}
            or type(slot["feed_enabled"]) is not bool
        ):
            raise ValidationError("invalid retirement account registry slot")
        configuration_parameters(slot["parameters"], account_id=account_id)
        strategies += len(slot["parameters"]["strategies"])
        control = slot.get("retirement_control")
        if control is not None:
            if (
                version != 2
                or type(control) is not dict
                or set(control) != {"operation_id", "parameters", "plan"}
                or slot["creation_state"] != "created"
                or slot["feed_enabled"] is not False
                or type(slot.get("retirement_feed_enabled_before")) is not bool
            ):
                raise ValidationError("invalid durable retirement fence")
            identity = _identity(control["operation_id"])
            parameters = _parameters(control["parameters"])
            if parameters["account_id"] != account_id:
                raise ValidationError("retirement fence belongs to another account")
            _descriptor(control["plan"], account_id, identity, parameters, slot["parameters"])
            operation = operations.get(identity)
            if (
                type(operation) is not dict
                or operation.get("kind") != "retire"
                or operation.get("state") not in {"queued", "running", "unknown"}
                or operation.get("request_digest") != retirement_sha256(("retire", parameters))
                or operation.get("core_epoch") != control["plan"]["request"]["core_epoch"]
            ):
                raise ValidationError("retirement fence operation differs")
            reservations += 1
        elif "retirement_feed_enabled_before" in slot:
            raise ValidationError("retirement feed provenance has no original fence")
    if strategies > 100 or reservations > 1 or len(retired) + reservations > MAX_RETIRED:
        raise ValidationError("retirement active strategy or reserved archive capacity exceeded")
    for identity, row in operations.items():
        _identity(identity)
        if (
            type(row) is not dict
            or set(row)
            != {"id", "kind", "request_digest", "core_epoch", "state", "result", "error"}
            or row["id"] != identity
            or row["kind"]
            not in {
                "configure",
                "resume",
                "pause",
                "reconcile",
                "update_brackets",
                "resume_strategy",
                "retire",
            }
            or row["state"] not in {"queued", "running", "completed", "failed", "unknown"}
        ):
            raise ValidationError("invalid retirement operation registry row")
        _identity(row["core_epoch"])
        _checksum(row["request_digest"])
        if row["error"] is not None and (type(row["error"]) is not str or len(row["error"]) > 128):
            raise ValidationError("invalid bounded retirement operation error")
    for account_id, row in retired.items():
        _identity(account_id)
        if (
            type(row) is not dict
            or set(row) != _INDEX
            or row["account_id"] != account_id
            or row["state"] != "retired"
            or row["reason_code"] is not None
            or type(row["feed_enabled_before"]) is not bool
            or type(row["ledger_total"]) is not int
            or not 0 <= row["ledger_total"] <= 10000
        ):
            raise ValidationError("invalid retired account tombstone")
        identity = _identity(row["operation_id"])
        _request(row["request"], identity)
        for name in (
            "receipt_sha256",
            "registered_parameters_sha256",
            "session_config_hash",
            "ledger_namespace",
            "archive_sha256",
            "observation_sha256",
            "plan_sha256",
        ):
            _checksum(row[name])
        operation = operations.get(identity)
        if (
            type(operation) is not dict
            or operation["kind"] != "retire"
            or operation["state"] != "completed"
            or operation["core_epoch"] != row["request"]["core_epoch"]
            or operation["request_digest"]
            != retirement_sha256(
                (
                    "retire",
                    {
                        "account_id": account_id,
                        "expected_observation_sha256": row["observation_sha256"],
                    },
                )
            )
            or operation["result"]
            != {"account_id": account_id, "retired": True, "receipt_sha256": row["receipt_sha256"]}
        ):
            raise ValidationError("retired tombstone operation differs")
    _encoded(data)
    return data


@dataclass(eq=False, slots=True)
class _Inspection:
    account_id: str
    deadline: float
    event: threading.Event = field(default_factory=threading.Event)
    result: dict | None = None
    expired: bool = False


class _Resources:
    def __init__(self, helper):
        self.helper = helper

    def close(self):
        self.helper.close_resources()

    def cleanup_status(self):
        return self.helper.cleanup_status()


@dataclass(slots=True)
class _Context:
    account_id: str
    identity: str
    parameters: dict
    descriptor: dict
    helper: MarginRetirement
    resource: _Resources
    row: dict
    session: object = None
    plan: object = None
    fenced: bool = False
    recovering: bool = False
    phase: str = "fenced"
    reason: str | None = None
    receipt: dict | None = None
    feed_enabled_before: bool | None = None


class RetirementRuntime:
    def __init__(self, owner):
        self.owner = owner
        self.queue, self.waiters = deque(), set()
        self.history = MarginRetiredHistory(owner.root)
        self.registry_uncertain = False
        self.helpers, self._contexts, self._fences = {}, {}, set()
        self._confirmed, self._invalid_retired = set(), set()
        self._observation = None

    def _owned(self):
        if threading.current_thread() is not self.owner.thread:
            raise TradingBlocked("retirement belongs to its financial owner")
        if self.owner.closed or self.registry_uncertain:
            raise StorageError("retirement owner is unavailable")

    def _unavailable(self, account_id, reason):
        return retirement_response(account_id, self.owner.core_epoch, reason_code=reason)

    def gated(self, account_id):
        with self.owner.condition:
            slot = (self.owner.data or {}).get("accounts", {}).get(account_id, {})
            return (
                account_id in self._fences
                or bool(slot.get("retirement_control"))
                or account_id in self._invalid_retired
            )

    def retired(self, account_id):
        with self.owner.condition:
            row = (self.owner.data or {}).get("retired", {}).get(account_id)
            return deepcopy(row) if row is not None and account_id in self._confirmed else None

    def metadata(self):
        with self.owner.condition:
            result = []
            data = self.owner.data or {}
            for account_id, row in sorted(data.get("retired", {}).items()):
                item = {key: row[key] for key in _META}
                if account_id not in self._confirmed:
                    item.update(state="retiring", reason_code="retired_receipt_unconfirmed")
                result.append(item)
            accounts = data.get("accounts", {})
            identities = set(self._contexts) | {
                key for key, slot in accounts.items() if slot.get("retirement_control")
            }
            identities.difference_update(data.get("retired", {}))
            for account_id in sorted(identities):
                context = self._contexts.get(account_id)
                control = accounts.get(account_id, {}).get("retirement_control")
                plan = context.descriptor if context else control["plan"]
                result.append(
                    {
                        "account_id": account_id,
                        "state": "retiring",
                        "operation_id": plan["request"]["operation_id"],
                        "receipt_sha256": context.receipt["sha256"]
                        if context and context.receipt
                        else None,
                        "registered_parameters_sha256": plan["registered_parameters_sha256"],
                        "session_config_hash": plan["session_config_hash"],
                        "ledger_namespace": plan["ledger_namespace"],
                        "archive_sha256": plan["archive_sha256"],
                        "ledger_total": plan["observation"].get("ledger_total"),
                        "reason_code": context.reason if context else "retirement_pending",
                    }
                )
            return deepcopy(result)

    def _cached_response(self, account_id):
        item = next((row for row in self.metadata() if row["account_id"] == account_id), None)
        if item is None:
            return None
        context = self._contexts.get(account_id)
        data = self.owner.data or {}
        control = data.get("accounts", {}).get(account_id, {}).get("retirement_control")
        descriptor = context.descriptor if context else control["plan"] if control else None
        observation_sha = (
            retirement_sha256(descriptor["observation"])
            if descriptor is not None
            else data.get("retired", {}).get(account_id, {}).get("observation_sha256")
        )
        return retirement_response(
            account_id,
            self.owner.core_epoch,
            state=item["state"],
            phase="retired"
            if item["state"] == "retired"
            else "unknown"
            if item["reason_code"]
            else context.phase
            if context
            else "fenced",
            reason_code=item["reason_code"],
            operation_id=item["operation_id"],
            receipt_sha256=item["receipt_sha256"],
            observation_sha256=observation_sha,
            registered_parameters_sha256=item["registered_parameters_sha256"],
            session_config_hash=item["session_config_hash"],
            ledger_namespace=item["ledger_namespace"],
            ledger_total=item["ledger_total"],
        )

    def inspect(self, account_id, timeout=5):
        account_id = _identity(account_id)
        if type(timeout) not in {int, float} or not math.isfinite(timeout) or not 0 < timeout <= 5:
            raise ValidationError("retirement inspection timeout must be within(0,5]seconds")
        owner = self.owner
        ticket = _Inspection(account_id, time.monotonic() + timeout)
        with owner.condition:
            if (
                owner.closed
                or not owner.ready
                or owner.error
                or self.registry_uncertain
                or owner.thread is None
                or not owner.thread.is_alive()
            ):
                return self._unavailable(account_id, "owner_unavailable")
            if threading.current_thread() is owner.thread:
                return self._unavailable(account_id, "owner_reentrant_read")
            cached = self._cached_response(account_id)
            if cached is not None:
                return cached
            if len(self.waiters) >= MAX_OBSERVATIONS:
                return self._unavailable(account_id, "read_queue_full")
            self.waiters.add(ticket)
            self.queue.append(ticket)
            owner.condition.notify_all()
        ticket.event.wait(timeout)
        with owner.condition:
            if ticket.result is None:
                ticket.expired = True
                self.waiters.discard(ticket)
                try:
                    self.queue.remove(ticket)
                except ValueError:
                    pass
                return self._unavailable(account_id, "read_timeout")
            return deepcopy(ticket.result)

    def fail(self, reason):
        with self.owner.condition:
            for ticket in self.waiters:
                ticket.result = self._unavailable(ticket.account_id, reason)
                ticket.event.set()
            self.waiters.clear()
            self.queue.clear()
            self._observation = None

    def _busy(self, account_id, exclude=None):
        owner = self.owner
        for identity, kind, parameters in owner.queue:
            target = (
                parameters.get("account_id")
                if kind != "configure"
                else parameters.get("account", {}).get("account_id")
            )
            if identity != exclude and target == account_id:
                return True
        for identity, row in owner.operations.items():
            if identity != exclude and row["state"] == "running":
                target = (row.get("result") or {}).get("account_id")
                if target is None or target == account_id:
                    return True
        bridge = getattr(owner, "shutdown_bridge", None)
        return bool(bridge is not None and bridge.queue)

    def _observe(self, account_id, exclude=None):
        owner = self.owner
        if owner.closed or not owner.ready or owner.error or self.registry_uncertain:
            raise TradingBlocked("retirement owner unavailable")
        if self._contexts or any(
            slot.get("retirement_control") for slot in owner.data["accounts"].values()
        ):
            raise TradingBlocked("another retirement is in progress")
        slot, session = owner.data["accounts"].get(account_id), owner.sessions.get(account_id)
        if (
            slot is None
            or slot["creation_state"] != "created"
            or session is None
            or account_id in owner.blocked_accounts
            or self._busy(account_id, exclude)
        ):
            raise TradingBlocked("retirement account observation unavailable")
        observation = session.retirement_snapshot(slot["parameters"])
        summary = {key: value for key, value in observation.items() if key != "account"}
        _, registered_sha = configuration_parameters(slot["parameters"], account_id=account_id)
        if (
            observation.get("account_id") != account_id
            or observation.get("paused") is not True
            or type(observation.get("orders")) is not int
            or observation["orders"] != 0
            or type(observation.get("staged")) is not int
            or observation["staged"] != 0
            or observation.get("registered_parameters_sha256") != registered_sha
            or observation.get("schema_version") != 1
            or observation.get("scope") != "virtual_margin_retirement_observation"
        ):
            raise ProtocolError("retirement observation identity or certainty differs")
        account = account_from_document(observation["account"])
        cleanup = session.cleanup_status()
        if (
            account.account_id != account_id
            or any(position.quantity != 0 for position in account.positions)
            or len(account.ledger) != observation.get("ledger_total")
            or retirement_sha256(observation["account"]) != observation.get("account_sha256")
            or type(cleanup) is not dict
            or cleanup.get("scope") != "process_local_margin_cleanup"
            or cleanup.get("logical_closed") is not False
            or cleanup.get("state") != "not_started"
            or cleanup.get("generation") != observation.get("cleanup_generation")
            or sorted(row["name"] for row in cleanup["resources"])
            != observation.get("cleanup_resource_names")
            or any(row["error_type"] is not None for row in cleanup["resources"])
        ):
            raise ProtocolError("retirement financial or native observation differs")
        digest = retirement_sha256(summary)
        response = retirement_response(
            account_id,
            owner.core_epoch,
            state="ready",
            observation_sha256=digest,
            registered_parameters_sha256=registered_sha,
            session_config_hash=observation["session_config_hash"],
            ledger_namespace=observation["ledger_namespace"],
            paused=True,
            open_positions=0,
            pending_orders=0,
            staged=0,
            source_pending=False,
            strategy_count=len(observation["strategies"]),
            source_record_count=observation["source"]["count"],
            ledger_total=observation["ledger_total"],
        )
        self._observation = (account_id, digest, time.monotonic() + OBSERVATION_TTL, observation)
        return observation, response

    def serve_one(self):
        self._owned()
        owner, ticket = self.owner, None
        with owner.condition:
            while self.queue:
                candidate = self.queue.popleft()
                if not candidate.expired and candidate.deadline > time.monotonic():
                    ticket = candidate
                    break
                self.waiters.discard(candidate)
        if ticket is None:
            return
        try:
            cached = self._cached_response(ticket.account_id)
            result = cached if cached is not None else self._observe(ticket.account_id)[1]
        except Exception:
            result = self._unavailable(ticket.account_id, "retirement_observation_unavailable")
        with owner.condition:
            self.waiters.discard(ticket)
            if not ticket.expired and ticket.deadline > time.monotonic():
                ticket.result = (
                    result
                    if not owner.closed
                    else self._unavailable(ticket.account_id, "owner_closed")
                )
                ticket.event.set()

    def retryable(self, identity, parameters):
        parameters = _parameters(parameters)
        with self.owner.condition:
            context = self._contexts.get(parameters["account_id"])
            row = self.owner.operations.get(identity)
            return bool(
                not self.registry_uncertain
                and context is not None
                and context.identity == identity
                and context.parameters == parameters
                and row is not None
                and row["state"] == "unknown"
                and row["request_digest"] == retirement_sha256(("retire", parameters))
            )

    def mark_unknown(self, identity, error):
        with self.owner.condition:
            row = self.owner.operations.get(identity)
            if row is not None:
                row.update(state="unknown", error="retirement_unconfirmed")
            for context in self._contexts.values():
                if context.identity == identity:
                    context.reason = (
                        "registry_unconfirmed"
                        if self.registry_uncertain
                        else "retirement_unconfirmed"
                    )
            self.owner.condition.notify_all()

    def _commit(self, candidate):
        if self.registry_uncertain:
            raise StorageError("retirement registry outcome is unconfirmed")
        validate_registry(candidate)
        owner, original = self.owner, deepcopy(self.owner.data)
        try:
            owner._write(deepcopy(candidate))
        except Exception:
            pass
        try:
            actual = owner._read()
        except Exception:
            actual = None
        if actual == candidate:
            with owner.condition:
                owner.data = deepcopy(candidate)
            return True
        if actual == original:
            with owner.condition:
                owner.data = original
            return False
        self.registry_uncertain = True
        self.fail("registry_unconfirmed")
        raise StorageError("retirement registry outcome is unconfirmed")

    def _fence_candidate(self, context):
        candidate = deepcopy(self.owner.data)
        slot = candidate["accounts"][context.account_id]
        existing = slot.get("retirement_control")
        control = {
            "operation_id": context.identity,
            "parameters": deepcopy(context.parameters),
            "plan": deepcopy(context.descriptor),
        }
        if existing is not None and existing != control:
            raise ProtocolError("retirement cannot replace another durable fence")
        if (
            type(context.feed_enabled_before) is not bool
            or existing is not None
            and slot.get("retirement_feed_enabled_before") is not context.feed_enabled_before
        ):
            raise ProtocolError("retirement original feed provenance changed")
        if (
            configuration_parameters(slot["parameters"], account_id=context.account_id)[1]
            != context.descriptor["registered_parameters_sha256"]
        ):
            raise ProtocolError("retirement registered parameters changed")
        candidate.update(schema_version=2)
        candidate.setdefault("retired", {})
        slot.update(
            retirement_control=control,
            feed_enabled=False,
            retirement_feed_enabled_before=context.feed_enabled_before,
        )
        row = deepcopy(context.row)
        row.update(state="running", error=None, result=None)
        candidate["operations"][context.identity] = row
        return candidate

    def _tombstone(self, context, receipt_sha):
        plan = context.descriptor
        return {
            "account_id": context.account_id,
            "state": "retired",
            "operation_id": context.identity,
            "receipt_sha256": receipt_sha,
            "registered_parameters_sha256": plan["registered_parameters_sha256"],
            "session_config_hash": plan["session_config_hash"],
            "ledger_namespace": plan["ledger_namespace"],
            "archive_sha256": plan["archive_sha256"],
            "ledger_total": plan["observation"]["ledger_total"],
            "reason_code": None,
            "observation_sha256": context.parameters["expected_observation_sha256"],
            "plan_sha256": plan["sha256"],
            "request": deepcopy(plan["request"]),
            "feed_enabled_before": context.feed_enabled_before,
        }

    def _index_candidate(self, context, base, receipt_sha):
        candidate = deepcopy(base)
        candidate["accounts"].pop(context.account_id)
        candidate["retired"][context.account_id] = self._tombstone(context, receipt_sha)
        row = deepcopy(context.row)
        row.update(
            state="completed",
            error=None,
            result={
                "account_id": context.account_id,
                "retired": True,
                "receipt_sha256": receipt_sha,
            },
        )
        candidate["operations"][context.identity] = row
        validate_registry(candidate)
        return candidate

    def _check_receipt(self, context, receipt):
        plan = context.descriptor
        if (
            receipt is None
            or receipt.get("account_id") != context.account_id
            or receipt.get("request") != plan["request"]
            or any(
                receipt.get(key) != plan[key]
                for key in ("registered_parameters_sha256", "session_config_hash", "archive_sha256")
            )
            or receipt.get("observation_sha256")
            != context.parameters["expected_observation_sha256"]
            or receipt.get("cleanup_generation") != plan["observation"]["cleanup_generation"]
        ):
            raise ProtocolError("retirement receipt differs from its original fence")

    def _advance(self, context):
        owner = self.owner
        if not context.fenced:
            candidate = self._fence_candidate(context)
            self._index_candidate(context, candidate, "0" * 64)  # Reserve final bytes before close.
            if not self._commit(candidate):
                raise StorageError("retirement fence was not acknowledged")
            context.fenced = True
            with owner.condition:
                owner.operations[context.identity] = deepcopy(
                    candidate["operations"][context.identity]
                )
        if context.recovering:
            slot = owner.data["accounts"][context.account_id]
            receipt = context.helper.recover(context.descriptor, slot["parameters"])
        else:
            receipt = context.helper.read()
            if receipt is None:
                if context.plan is None:
                    raise ProtocolError("retirement original archive workspace is unavailable")
                receipt = context.helper.prepare(context.plan)
            self._check_receipt(context, receipt)
            context.plan = None
            context.phase = "closed" if receipt["state"] == "retired" else receipt["state"]
            if receipt["state"] == "prepared":
                receipt = context.helper.close(context.session)
            # A durable closed receipt is insufficient to discard an original
            # live Session whose own native cleanup has not been confirmed.
            if (
                context.session is None
                or owner.sessions.get(context.account_id) is not context.session
                or not confirmed_cleanup(
                    context.session.cleanup_status(),
                    generation=context.descriptor["observation"]["cleanup_generation"],
                )
            ):
                raise StorageError("retirement original session cleanup remains unconfirmed")
            receipt = context.helper.finish()
        self._check_receipt(context, receipt)
        if receipt["state"] != "retired":
            raise ProtocolError("retirement has no final receipt")
        context.receipt, context.phase = receipt, "closed"
        candidate = self._index_candidate(context, owner.data, receipt["sha256"])
        if not self._commit(candidate):
            raise StorageError("retirement index was not acknowledged")
        with owner.condition:
            owner.operations[context.identity] = deepcopy(candidate["operations"][context.identity])
            owner.sessions.pop(context.account_id, None)
            owner.views.pop(context.account_id, None)
            owner.blocked_accounts.discard(context.account_id)
            self._confirmed.add(context.account_id)
            self._contexts.pop(context.account_id, None)
            self.helpers.pop(context.account_id, None)
            self._fences.discard(context.account_id)
            context.plan = context.session = None
            owner.condition.notify_all()

    def execute(self, identity, parameters):
        self._owned()
        owner = self.owner
        owner._command_started = False
        identity, parameters = _identity(identity), _parameters(parameters)
        account_id = parameters["account_id"]
        context = self._contexts.get(account_id)
        if context is not None:
            if context.identity != identity or context.parameters != parameters:
                raise ValidationError("retirement conflicts with its original request")
        else:
            if len(owner.operations) > 128 or len(owner.data.get("retired", {})) >= MAX_RETIRED:
                raise ValidationError("retirement receipt capacity reached")
            if (
                self._contexts
                or self.gated(account_id)
                or account_id in owner.data.get("retired", {})
            ):
                raise ValidationError("retirement account is already gated or retired")
            cached = self._observation
            if (
                cached is None
                or cached[0] != account_id
                or cached[1] != parameters["expected_observation_sha256"]
                or cached[2] <= time.monotonic()
            ):
                raise ValidationError("retirement observation is missing or expired")
            row = owner.operations.get(identity)
            if (
                row is None
                or row["kind"] != "retire"
                or row["core_epoch"] != owner.core_epoch
                or row["request_digest"] != retirement_sha256(("retire", parameters))
            ):
                raise ValidationError("retirement operation identity differs")
            observation, response = self._observe(account_id, exclude=identity)
            if response["observation_sha256"] != parameters["expected_observation_sha256"]:
                raise ValidationError("retirement observation changed before admission")
            helper = MarginRetirement(owner.root, account_id)
            request = {
                "operation_id": identity,
                "core_epoch": row["core_epoch"],
                "issued_at_ms": owner.clock_ms(),
            }
            plan = helper.make_plan(
                request, owner.data["accounts"][account_id]["parameters"], observation
            )
            context = _Context(
                account_id,
                identity,
                parameters,
                plan.document,
                helper,
                _Resources(helper),
                deepcopy(row),
                owner.sessions[account_id],
                plan,
                feed_enabled_before=owner.data["accounts"][account_id]["feed_enabled"],
            )
            candidate = self._fence_candidate(context)
            validate_registry(candidate)
            self._index_candidate(context, candidate, "0" * 64)
            self._observation = observation = cached = None
            with owner.condition:
                self._contexts[account_id], self.helpers[account_id] = context, helper
                self._fences.add(account_id)
        owner._command_started = True
        try:
            self._advance(context)
        except Exception as error:
            self.mark_unknown(identity, error)
            if self.registry_uncertain:
                raise StorageError("retirement registry outcome is unconfirmed") from None
        return deepcopy(owner.operations[identity])

    def _verify_index(self, account_id, row):
        helper = MarginRetirement(self.owner.root, account_id)
        receipt, archive = helper.read(), helper.archive()
        if (
            receipt is None
            or receipt["state"] != "retired"
            or receipt["sha256"] != row["receipt_sha256"]
            or receipt["request"] != row["request"]
            or any(
                receipt[key] != row[key]
                for key in (
                    "registered_parameters_sha256",
                    "session_config_hash",
                    "archive_sha256",
                    "observation_sha256",
                )
            )
            or archive["ledger_namespace"] != row["ledger_namespace"]
            or archive["observation"]["ledger_total"] != row["ledger_total"]
        ):
            raise ProtocolError("retirement index receipt binding differs")
        descriptor = {
            "schema_version": 1,
            "scope": PLAN_SCOPE,
            "account_id": account_id,
            "request": receipt["request"],
            "registered_parameters_sha256": archive["registered_parameters_sha256"],
            "session_config_hash": archive["session_config_hash"],
            "ledger_namespace": archive["ledger_namespace"],
            "observation": archive["observation"],
            "source_files": archive["source_files"],
            "source_inventory_sha256": archive["source_inventory_sha256"],
            "archive_sha256": archive["sha256"],
        }
        if retirement_sha256(descriptor) != row["plan_sha256"]:
            raise ProtocolError("retirement index plan binding differs")

    def recover(self):
        self._owned()
        owner = self.owner
        validate_registry(owner.data)
        if owner.data["schema_version"] == 1:
            return
        with owner.condition:
            fenced = [
                (account_id, slot)
                for account_id, slot in owner.data["accounts"].items()
                if slot.get("retirement_control")
            ]
            self._fences.update(account_id for account_id, _ in fenced)
        for account_id, row in sorted(owner.data.get("retired", {}).items()):
            try:
                self._verify_index(account_id, row)
            except Exception:
                self._invalid_retired.add(account_id)
            else:
                self._confirmed.add(account_id)
        for account_id, slot in fenced:
            if account_id in self._contexts:
                continue
            control = slot["retirement_control"]
            identity = control["operation_id"]
            helper = MarginRetirement(owner.root, account_id)
            context = _Context(
                account_id,
                identity,
                deepcopy(control["parameters"]),
                deepcopy(control["plan"]),
                helper,
                _Resources(helper),
                deepcopy(owner.data["operations"][identity]),
                fenced=True,
                recovering=True,
                feed_enabled_before=slot["retirement_feed_enabled_before"],
            )
            self._contexts[account_id], self.helpers[account_id] = context, helper
            try:
                if account_id in owner.sessions:
                    raise ProtocolError("retirement recovery cannot replace a live session")
                self._advance(context)
            except Exception as error:
                self.mark_unknown(identity, error)
                if self.registry_uncertain:
                    raise StorageError("retirement registry outcome is unconfirmed") from None

    def resources(self):
        with self.owner.condition:
            return [
                ("retirement:" + account_id, context.resource)
                for account_id, context in sorted(self._contexts.items())
            ] + [("retired_history", self.history)]
