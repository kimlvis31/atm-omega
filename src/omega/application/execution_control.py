"""Read-only known-scope reconciliation owner; always paused, never live authority.

The account caller serializes this owner with financial recovery/coordinator use.
Endpoint inventories provide no global transactional watermark. Even a matched,
PRIMARY-confirmed round cannot authorize new risk or clear another owner's gate.
"""

import json
import re
from copy import deepcopy
from dataclasses import asdict, replace
from decimal import Decimal
from hashlib import sha256
from threading import RLock

from omega.application.execution_account import (
    ConfirmedExecutionAccountInputs,
    ExecutionAccountRecovery,
)
from omega.domain.errors import JournalError, TradingBlocked, ValidationError
from omega.domain.execution_inventory import (
    MAX_INVENTORY_ROWS,
    ConditionalOrderInventory,
    ExternalConditionalOrder,
    InventoryProvenance,
    OpenOrder,
    OpenOrderInventory,
)
from omega.domain.execution_rules import MAX_TIME, bounded_decimal
from omega.domain.models import Instrument, Market, OrderObservation, OrderStatus, Side
from omega.domain.safety import BlockReason
from omega.ports.journal import Durability

KIND = "ExecutionControlObserved"
MAX_RECORD_BYTES = 61_440
MAX_RECORDS = 128
TERMINAL = frozenset(
    (
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
        OrderStatus.EXPIRED_IN_MATCH,
    )
)


def _encoded(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > MAX_RECORD_BYTES:
        raise ValidationError("execution control record exceeds capacity")
    return raw


def _hash(value):
    return sha256(_encoded(value)).hexdigest()


def _time(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("execution control requires native bounded milliseconds")
    return value


def _json_values(value):
    # These are already validated inventory DTOs, not a second venue parser.
    if type(value) is Decimal:
        bounded_decimal(value)
        text = format(value, "f")
        return (text.rstrip("0").rstrip(".") if "." in text else text) or "0"
    if isinstance(value, dict):
        return {name: _json_values(item) for name, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_values(item) for item in value]
    return value


def _read_inventory(document):
    """Validate saved normalized DTOs through their existing domain constructors."""

    def amount(value):
        if type(value) is not str or not 1 <= len(value) <= 160:
            raise ValidationError("inventory amount is not a bounded decimal string")
        result = Decimal(value)
        bounded_decimal(result)
        return result

    def instrument(value):
        return Instrument(value["provider"], Market(value["market"]), value["symbol"])

    try:
        if type(document) is not dict or set(document) != {"provenance", "orders"}:
            raise ValidationError("invalid normalized inventory fields")
        source = deepcopy(document["provenance"])
        source["market"] = Market(source["market"])
        source = InventoryProvenance(**source)
        rows = document["orders"]
        if type(rows) is not list or len(rows) > MAX_INVENTORY_ROWS:
            raise ValidationError("normalized inventory exceeds capacity")
        values = []
        for row in rows:
            native = deepcopy(row)
            if source.endpoint_kind == "ordinary":
                observation = native["observation"]
                observation["instrument"] = instrument(observation["instrument"])
                observation["status"] = OrderStatus(observation["status"])
                for name in ("executed_quantity", "cumulative_quote"):
                    observation[name] = amount(observation[name])
                native["observation"] = OrderObservation(**observation)
                native["side"] = Side(native["side"])
                for name in ("original_quantity", "price"):
                    native[name] = amount(native[name])
                values.append(OpenOrder(**native))
            else:
                native["instrument"] = instrument(native["instrument"])
                values.append(ExternalConditionalOrder(**native))
        inventory = (
            OpenOrderInventory if source.endpoint_kind == "ordinary" else ConditionalOrderInventory
        )(source, tuple(values))
        if _encoded(_json_values(asdict(inventory))) != _encoded(document):
            raise ValidationError("normalized inventory is noncanonical")
        return inventory
    except (KeyError, TypeError, ValueError, ArithmeticError, ValidationError):
        raise JournalError("saved execution inventory is malformed") from None


class ExecutionControlOwner:
    def __init__(
        self,
        recovery,
        *,
        ordinary_reader,
        conditional_reader=None,
        required_inventory_kinds=("ordinary", "conditional"),
        origin,
        clock_ms,
        max_age_ms=5000,
        max_round_ms=5000,
        on_incident=None,
    ):
        if (
            type(recovery) is not ExecutionAccountRecovery
            or type(required_inventory_kinds) is not tuple
            or required_inventory_kinds not in (("ordinary",), ("ordinary", "conditional"))
            or not callable(getattr(ordinary_reader, "open_orders", None))
            or "conditional" in required_inventory_kinds
            and not callable(getattr(conditional_reader, "open_conditional_orders", None))
            or type(origin) is not str
            or not 1 <= len(origin) <= 1024
            or not callable(clock_ms)
            or on_incident is not None
            and not callable(on_incident)
            or any(
                type(value) is not int or not 1 <= value <= 60_000
                for value in (max_age_ms, max_round_ms)
            )
        ):
            raise ValidationError("execution control requires original readers and bounded policy")
        self.recovery, self.coordinator, self.sink = recovery, recovery.coordinator, recovery.sink
        self.ordinary_reader, self.conditional_reader = ordinary_reader, conditional_reader
        self.account_id, self.instrument = recovery.account_id, recovery.opening.instrument
        self._binding = (
            recovery,
            self.coordinator,
            self.sink,
            self.coordinator.broker,
            self.coordinator.gate,
            recovery.account_reader,
            ordinary_reader,
            conditional_reader,
        )
        self._policy = {
            "provider": self.instrument.provider,
            "market": self.instrument.market.value,
            "origin": origin,
            "required_inventory_kinds": list(required_inventory_kinds),
            "max_age_ms": max_age_ms,
            "max_round_ms": max_round_ms,
        }
        self._clock, self._on_incident, self._lock = clock_ms, on_incident, RLock()
        self._records, self._pending, self._confirmed = [], None, None
        self._state, self._error = "held", None
        self._notification_failed = False
        self._incidents = []
        self.coordinator.gate.block(BlockReason.PAUSE)
        self.coordinator.gate.block(BlockReason.EXECUTION)
        self._records = self._read()
        if any(row["state"] == "external" for row in self._records):
            self.coordinator.gate.block(BlockReason.EXTERNAL)
        if self._records:
            self._pending = (deepcopy(self._records[-1]), None)
            self._state = "historical"

    def _check_binding(self):
        recovery, coordinator, sink, broker, gate, reader, ordinary, conditional = self._binding
        if (
            self.recovery is not recovery
            or self.coordinator is not coordinator
            or self.sink is not sink
            or recovery.coordinator is not coordinator
            or recovery.sink is not sink
            or recovery.account_reader is not reader
            or recovery.account_id != self.account_id
            or recovery.opening.instrument != self.instrument
            or coordinator.account_id != self.account_id
            or coordinator.sink is not sink
            or coordinator.broker is not broker
            or coordinator.gate is not gate
            or self.ordinary_reader is not ordinary
            or self.conditional_reader is not conditional
        ):
            raise TradingBlocked("execution control original composition changed")
        gate.block(BlockReason.PAUSE)
        gate.block(BlockReason.EXECUTION)

    def _read(self):
        selected, account_records = {}, set()
        rows = self.sink.records()
        for row in rows:
            if (
                row.get("kind") == "ExecutionAccountApplied"
                and row.get("account_id") == self.account_id
            ):
                account_records.add(_hash(row))
        previous = None
        for row in rows:
            if row.get("kind") != KIND or row.get("account_id") != self.account_id:
                continue
            revision = row.get("revision")
            if (
                type(revision) is not int
                or not 1 <= revision <= MAX_RECORDS
                or set(row)
                != {
                    "kind",
                    "schema_version",
                    "account_id",
                    "instrument",
                    "policy",
                    "revision",
                    "previous_sha256",
                    "account_sha256",
                    "started_at_ms",
                    "completed_at_ms",
                    "inventories",
                    "state",
                    "issues",
                    "command_id",
                    "expected_revision",
                }
                or type(row["schema_version"]) is not int
                or row["schema_version"] != 1
                or row["instrument"] != self.instrument.key
                or _encoded(row["policy"]) != _encoded(self._policy)
                or row["account_sha256"] not in account_records
                or row["state"]
                not in (
                    "observed_known_scope_paused",
                    "held",
                    "external",
                    "manual_resume_refused",
                )
                or type(row["issues"]) is not list
                or any(type(issue) is not str or len(issue) > 80 for issue in row["issues"])
                or type(row["inventories"]) is not list
            ):
                raise JournalError("execution control record has invalid source/schema")
            _time(row["started_at_ms"])
            _time(row["completed_at_ms"])
            if row["completed_at_ms"] < row["started_at_ms"]:
                raise JournalError("execution control record interval regressed")
            inventories = tuple(_read_inventory(item) for item in row["inventories"])
            if [item.provenance.endpoint_kind for item in inventories] != self._policy[
                "required_inventory_kinds"
            ]:
                raise JournalError("saved execution inventory coverage differs")
            for item in inventories:
                source = item.provenance
                if (
                    source.account_id != self.account_id
                    or source.provider != self.instrument.provider
                    or source.market is not self.instrument.market
                    or source.origin != self._policy["origin"]
                    or not row["started_at_ms"]
                    <= source.requested_at_ms
                    <= source.completed_at_ms
                    <= row["completed_at_ms"]
                ):
                    raise JournalError("saved execution inventory source/interval differs")
            if row["issues"] != sorted(set(row["issues"])):
                raise JournalError("saved execution control issues are noncanonical")
            if set(row["issues"]) - {
                "UNOWNED_CONDITIONAL_ORDER",
                "UNOWNED_OPEN_ORDER",
                "KNOWN_OPEN_ORDER_CONTRADICTION",
                "NONTERMINAL_ORIGINALS",
                "UNSUPPORTED_ACCOUNT_COVERAGE",
            }:
                raise JournalError("saved execution control issues are unsupported")
            if row["state"] == "manual_resume_refused":
                if (
                    type(row["command_id"]) is not str
                    or re.fullmatch(r"[A-Za-z0-9_-]{1,96}", row["command_id"]) is None
                    or type(row["expected_revision"]) is not int
                    or row["expected_revision"] != revision - 1
                    or row["issues"] != ["UNSUPPORTED_ACCOUNT_COVERAGE"]
                ):
                    raise JournalError("saved manual request is invalid")
            elif row["command_id"] is not None or row["expected_revision"] is not None:
                raise JournalError("non-command execution record has a command identity")
            elif row["state"] != (
                "external"
                if any(issue.startswith("UNOWNED_") for issue in row["issues"])
                else "held"
                if row["issues"]
                else "observed_known_scope_paused"
            ):
                raise JournalError("saved execution control decision differs from its issues")
            if revision in selected and _encoded(selected[revision]) != _encoded(row):
                raise JournalError("execution control revision conflicts")
            selected[revision] = deepcopy(row)
        result = []
        for expected, revision in enumerate(sorted(selected), 1):
            row = selected[revision]
            if revision != expected or row["previous_sha256"] != previous:
                raise JournalError("execution control chain is incomplete")
            if (
                result
                and row["state"] != "manual_resume_refused"
                and row["started_at_ms"] < result[-1]["completed_at_ms"]
            ):
                raise JournalError("execution control clock regressed")
            if row["state"] == "manual_resume_refused" and (
                not result
                or any(
                    row[key] != result[-1][key]
                    for key in ("account_sha256", "started_at_ms", "completed_at_ms", "inventories")
                )
            ):
                raise JournalError("manual refusal replaced original reconciliation evidence")
            previous = _hash(row)
            result.append(row)
        return result

    def _check_history(self):
        rows = self._read()
        allowed = self._records[:]
        if self._pending is not None and self._pending[1] is not None:
            allowed.append(self._pending[0])
        if (
            len(rows) < len(self._records)
            or len(rows) > len(allowed)
            or any(_encoded(row) != _encoded(allowed[index]) for index, row in enumerate(rows))
        ):
            raise JournalError("execution control original history changed")

    def _fresh(self, inputs, started, completed, inventories):
        _time(started)
        _time(completed)
        maximum_age, maximum_round = self._policy["max_age_ms"], self._policy["max_round_ms"]
        if (
            not started <= completed <= started + maximum_round
            or not completed - maximum_age <= inputs.account.observed_at_ms <= completed
            or not started <= inputs.observed_at_ms <= completed
        ):
            raise TradingBlocked("execution control account/round is stale or future")
        for inventory in inventories:
            provenance = inventory.provenance
            if not (
                started
                <= provenance.requested_at_ms
                <= provenance.received_at_ms
                <= provenance.completed_at_ms
                <= completed
                and provenance.requested_at_ms >= completed - maximum_age
            ):
                raise TradingBlocked("execution control inventory is stale or outside its round")

    def _inventories(self, inputs, started):
        result, issues = [], []
        originals = {
            request.client_order_id: (request, observation)
            for request, observation in inputs.originals
        }
        for kind in self._policy["required_inventory_kinds"]:
            inventory = (
                self.ordinary_reader.open_orders()
                if kind == "ordinary"
                else self.conditional_reader.open_conditional_orders()
            )
            expected = OpenOrderInventory if kind == "ordinary" else ConditionalOrderInventory
            if type(inventory) is not expected:
                raise ValidationError("execution control requires full typed endpoint inventories")
            replace(inventory)
            source = inventory.provenance
            if (
                source.account_id != self.account_id
                or source.provider != self.instrument.provider
                or source.market is not self.instrument.market
                or source.origin != self._policy["origin"]
                or source.endpoint_kind != kind
            ):
                raise TradingBlocked("execution control inventory source differs")
            self._fresh(inputs, started, _time(self._clock()), (inventory,))
            if self._records:
                old_sources = {
                    item["provenance"]["endpoint_kind"]: item["provenance"]["endpoint"]
                    for item in self._records[0]["inventories"]
                }
                if source.endpoint != old_sources[kind]:
                    raise TradingBlocked("execution control original endpoint changed")
            result.append(inventory)
            for row in inventory.orders:
                if kind == "conditional":
                    issues.append("UNOWNED_CONDITIONAL_ORDER")
                    continue  # Opaque native fields never become executable protection.
                pair = originals.get(row.observation.client_order_id)
                if pair is None or row.observation.instrument != self.instrument:
                    issues.append("UNOWNED_OPEN_ORDER")
                    continue
                request, observation = pair
                if row.observation.provider_order_id != observation.provider_order_id:
                    issues.append("UNOWNED_OPEN_ORDER")
                elif (
                    observation.status in TERMINAL
                    or row.observation != observation
                    or row.side is not request.side
                    or row.order_type != request.kind.value
                    or row.original_quantity != request.quantity
                    or row.price != (request.price or Decimal(0))
                    or (row.reduce_only is not None and row.reduce_only != request.reduce_only)
                    or row.reduce_only is None
                    and request.reduce_only
                    or row.position_side not in (None, "BOTH")
                    or row.close_position not in (None, False)
                ):
                    issues.append("KNOWN_OPEN_ORDER_CONTRADICTION")
            if any(issue.startswith("UNOWNED_") for issue in issues):
                if BlockReason.EXTERNAL not in self.coordinator.gate.reasons:
                    self._incident("EXECUTION_CONTROL_EXTERNAL_INVENTORY")
                self.coordinator.gate.block(BlockReason.EXTERNAL)
        return tuple(result), sorted(set(issues))

    def _incident(self, code):
        self._incidents.append(
            {
                "code": code,
                "severity": "critical",
                "account_id": self.account_id,
                "instrument": self.instrument.key,
            }
        )

    def _commit(self):
        event, inputs = self._pending
        _encoded(event)
        if inputs is not None and self.recovery.confirmed_inputs() != inputs:
            raise TradingBlocked("pending execution control financial input changed")
        try:
            durability = self.sink.record(deepcopy(event))
        except (JournalError, OSError, TimeoutError) as error:
            self.coordinator.gate.block(BlockReason.STORAGE)
            raise TradingBlocked("execution control PRIMARY acknowledgement unavailable") from error
        if durability is not Durability.PRIMARY:
            self.coordinator.gate.block(BlockReason.STORAGE)
            raise TradingBlocked("execution control requires fresh PRIMARY acknowledgement")
        self._check_history()
        historical = inputs is None
        if not historical:
            self._records.append(deepcopy(event))
        self._pending = None
        self._confirmed = None if historical else inputs
        self._state, self._error = ("historical" if historical else event["state"]), None
        if event["state"] == "external":
            self.coordinator.gate.block(BlockReason.EXTERNAL)
        if (
            not historical
            and event["state"] == "observed_known_scope_paused"
            and BlockReason.EXTERNAL not in self.coordinator.gate.reasons
        ):
            now = _time(self._clock())
            if (
                not event["completed_at_ms"]
                <= now
                <= event["started_at_ms"] + self._policy["max_age_ms"]
            ):
                self._state = "held_stale"
            else:
                self.recovery.adopt_confirmed_originals()
                self._state = "known_terminal_adopted_paused"
        return self.status()

    def _public(self, action):
        incidents = []
        try:
            with self._lock:
                try:
                    self._check_binding()
                    self._check_history()
                    return action()
                except Exception as error:
                    self._state = "pending" if self._pending is not None else "held"
                    self._error = type(error).__name__
                    if isinstance(error, (JournalError, OSError, TimeoutError)):
                        self.coordinator.gate.block(BlockReason.STORAGE)
                    self._incident("EXECUTION_CONTROL_HELD")
                    raise
                finally:
                    incidents, self._incidents = self._incidents, []
        finally:
            for event in incidents:
                if self._on_incident is not None:
                    try:
                        self._on_incident(event)
                    except Exception:
                        with self._lock:
                            self._notification_failed = True

    def reconcile_once(self):
        def action():
            if self._pending is not None:
                raise TradingBlocked("retry original pending control candidate first")
            if self.recovery.status()["pending"]:
                raise TradingBlocked("retry original financial candidate before a new round")
            started = _time(self._clock())
            if self._records and started < self._records[-1]["completed_at_ms"]:
                raise TradingBlocked("new execution control round predates prior completion")
            self.recovery.recover_once(observed_at_ms=started)
            inputs = self.recovery.confirmed_inputs()
            if type(inputs) is not ConfirmedExecutionAccountInputs:
                raise ValidationError("execution control requires original typed financial inputs")
            inventories, issues = self._inventories(inputs, started)
            completed = _time(self._clock())
            self._fresh(inputs, started, completed, inventories)
            if not all(observation.status in TERMINAL for _, observation in inputs.originals):
                issues.append("NONTERMINAL_ORIGINALS")
            external = any(issue.startswith("UNOWNED_") for issue in issues)
            if external:
                self.coordinator.gate.block(BlockReason.EXTERNAL)
            state = "external" if external else "held" if issues else "observed_known_scope_paused"
            self._prepare(inputs, started, completed, inventories, state, issues)
            return self._commit()

        return self._public(action)

    def _prepare(
        self,
        inputs,
        started,
        completed,
        inventories,
        state,
        issues,
        command_id=None,
        expected_revision=None,
    ):
        if len(self._records) >= MAX_RECORDS:
            raise ValidationError("execution control history capacity reached")
        event = {
            "kind": KIND,
            "schema_version": 1,
            "account_id": self.account_id,
            "instrument": self.instrument.key,
            "policy": deepcopy(self._policy),
            "revision": len(self._records) + 1,
            "previous_sha256": None if not self._records else _hash(self._records[-1]),
            "account_sha256": inputs.receipt.record_sha256,
            "started_at_ms": started,
            "completed_at_ms": completed,
            "inventories": [_json_values(asdict(inventory)) for inventory in inventories],
            "state": state,
            "issues": sorted(set(issues)),
            "command_id": command_id,
            "expected_revision": expected_revision,
        }
        _encoded(event)
        self._pending = (event, inputs)

    def retry_pending(self):
        def action():
            if self._pending is None:
                raise TradingBlocked("execution control has no pending candidate")
            return self._commit()

        return self._public(action)

    def request_manual_resume(self, command_id, expected_revision):
        def action():
            if (
                type(command_id) is not str
                or re.fullmatch(r"[A-Za-z0-9_-]{1,96}", command_id) is None
                or type(expected_revision) is not int
                or not 1 <= expected_revision <= MAX_RECORDS
            ):
                raise ValidationError("manual request requires original command identity/revision")
            if self._pending is not None:
                raise TradingBlocked("retry original pending control candidate first")
            matches = [row for row in self._records if row["command_id"] == command_id]
            if matches:
                if matches[0]["expected_revision"] != expected_revision:
                    raise TradingBlocked("manual request identity was used for another revision")
                return self.status()
            if expected_revision != len(self._records) or self._confirmed is None:
                raise TradingBlocked("manual request has no current original confirmed round")
            inputs = self.recovery.confirmed_inputs()
            if inputs != self._confirmed:
                raise TradingBlocked("manual request financial source changed")
            last = self._records[-1]
            # Refusal is durable; it does not refresh/relabel the old inventory.
            self._prepare(
                inputs,
                last["started_at_ms"],
                last["completed_at_ms"],
                (),
                "manual_resume_refused",
                ["UNSUPPORTED_ACCOUNT_COVERAGE"],
                command_id,
                expected_revision,
            )
            self._pending[0]["inventories"] = deepcopy(last["inventories"])
            return self._commit()

        return self._public(action)

    def status(self):
        with self._lock:
            return {
                "account_id": self.account_id,
                "instrument": self.instrument.key,
                "state": self._state,
                "revision": len(self._records),
                "record_sha256": None if not self._records else _hash(self._records[-1]),
                "pending": self._pending is not None,
                "paused": True,
                "recovery_required": True,
                "manual_resume_ready": False,
                "history_complete": False,
                "coverage": "endpoint_only",
                "error_type": self._error,
                "notification_failed": self._notification_failed,
            }
