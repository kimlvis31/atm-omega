"""Opt-in worker-owned margin-v2 TS sessions over committed minute data.

All financial mutations and their files belong to one worker. GET reads cached
data. Session configuration never converts v1 accounts or authorizes real orders.
The caller supplies the core mutation fence, source and entry-control provider.
"""

import json
import math
import os
import re
import stat
import threading
import time
from collections import deque
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.ports.shutdown import ShutdownSnapshot
from omega.runtime.margin_cleanup import MarginCleanup, confirmed_cleanup
from omega.runtime.margin_configuration import configuration_response
from omega.runtime.margin_console import scenario_brackets
from omega.runtime.margin_ledger import MarginLedgerReads
from omega.runtime.margin_metric_inputs import MetricInputPolicy
from omega.runtime.margin_session_inputs import destination_id, unavailable_frames
from omega.runtime.settings import atomic_json

MAX_BYTES = 4_194_304
MAX_SHUTDOWN_OBSERVATIONS = 8
DEFAULT_EXTERNAL_FEATURE_WAIT_MS = 15000


def external_feature_wait_ms(parameters):
    value = parameters.get("external_feature_wait_ms", DEFAULT_EXTERNAL_FEATURE_WAIT_MS)
    if type(value) is not int or not 0 <= value <= 30000:
        raise ValidationError("external feature wait must be an integer in [0,30000]ms")
    return value


@dataclass(eq=False, slots=True)
class _ShutdownObservation:
    event: threading.Event = field(default_factory=threading.Event)
    result: ShutdownSnapshot | None = None
    expired: bool = False


def _shutdown_unavailable(reason: str) -> ShutdownSnapshot:
    return {
        "owner": "margin_v2",
        "ready": False,
        "durable": False,
        "supports_flatten": False,
        "positions": [],
        "pending_orders": [],
        "unknown": [reason],
        "notes": [],
    }


def _json(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > MAX_BYTES:
        raise ValidationError("margin runtime document exceeds capacity")
    return raw


def _identity(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValidationError("invalid virtual margin runtime identity")
    return value


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate margin runtime field")
        result[key] = value
    return result


class MarginTradingService:
    def __init__(
        self,
        root,
        registry,
        source_factory,
        *,
        core_epoch,
        clock_ms=None,
        entry_gate=None,
        mutation_lock=None,
        on_incident=None,
        session_factory=None,
        external_feature_provider=None,
        protection_source=None,
    ):
        self.root, self.registry = Path(root).absolute(), registry
        self.path = self.root / "state/margin-runtime.json"
        self.core_epoch = _identity(core_epoch)
        self.source_factory = source_factory
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.entry_gate = entry_gate or (lambda: True)
        self.mutation_lock = mutation_lock or threading.RLock()
        self.on_incident, self.session_factory = on_incident, session_factory
        self.analysis_execution_scope = (
            "required_fields_v1"
            if session_factory is None
            else getattr(session_factory, "analysis_execution_scope", None)
        )
        if protection_source is not None and not callable(protection_source):
            raise ValidationError("partial protection source must be callable")
        self.protection_source = protection_source
        self.external_feature_provider = external_feature_provider
        self._external_wait = {}
        self.condition = threading.Condition(threading.RLock())
        self.queue, self.operations, self.sessions, self.views = deque(), {}, {}, {}
        self.thread, self.source = None, None
        self._rejected_sessions = []
        self.closed, self.ready, self.error = False, False, None
        self.data = None
        self.blocked_accounts, self.retry_at = set(), {}
        self._shutdown_observations, self._shutdown_waiters = deque(), set()
        self._ledger_reads = MarginLedgerReads(self)
        self._observation_turn = "ledger"
        self._resource_cleanup = MarginCleanup()
        self._cleanup_requested = self._cleanup_attempted = 0
        self._cleanup_complete = self._cleanup_attempt_error = False
        self._cleanup_prior_error = None
        self._cleanup_reported_serial = 0
        from omega.runtime.margin_shutdown_bridge import MarginShutdownBridge

        self.shutdown_bridge = MarginShutdownBridge(self)
        from omega.runtime.margin_retirement_runtime import RetirementRuntime

        self.retirement = RetirementRuntime(self)

    @contextmanager
    def _mutation(self):
        # Core shutdown holds the same fence while joining providers. Never
        # remain blocked behind that fence after this owner has been retired.
        while not self.closed:
            if self.mutation_lock.acquire(timeout=0.1):
                try:
                    self._guard()
                    yield
                finally:
                    self.mutation_lock.release()
                return
        raise StorageError("margin runtime owner is retired")

    def _guard(self):
        if self.closed:
            raise StorageError("margin runtime owner is retired")
        for path in (self.path, *self.path.parent.parents, self.path.parent):
            if path.is_symlink() or path.is_junction():
                raise StorageError("margin runtime metadata cannot use linked paths")

    def _read(self):
        self._guard()
        descriptor = None
        try:
            descriptor = os.open(
                self.path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
            )
            details = os.fstat(descriptor)
            if (
                not stat.S_ISREG(details.st_mode)
                or details.st_size > MAX_BYTES * 2
                or getattr(details, "st_file_attributes", 0) & 0x400
            ):
                raise StorageError("margin runtime metadata is not a bounded regular file")
            stream = os.fdopen(descriptor, "rb")
            descriptor = None
            with stream:
                raw = stream.read(MAX_BYTES * 2 + 1)
        except FileNotFoundError:
            return {"schema_version": 1, "accounts": {}, "operations": {}}
        finally:
            if descriptor is not None:
                os.close(descriptor)
        if len(raw) > MAX_BYTES * 2:
            raise StorageError("margin runtime metadata exceeds capacity")
        envelope = json.loads(raw, object_pairs_hook=_unique)
        if (
            type(envelope) is not dict
            or set(envelope) != {"data", "sha256"}
            or envelope["sha256"] != sha256(_json(envelope["data"])).hexdigest()
        ):
            raise StorageError("invalid margin runtime metadata; original preserved")
        data = envelope["data"]
        if (
            type(data) is not dict
            or set(data)
            != (
                {"schema_version", "accounts", "operations", "retired"}
                if data.get("schema_version") == 2
                else {"schema_version", "accounts", "operations"}
            )
            or type(data["schema_version"]) is not int
            or data["schema_version"] not in {1, 2}
            or type(data["accounts"]) is not dict
            or len(data["accounts"]) > 10
            or type(data["operations"]) is not dict
            or len(data["operations"]) > 128
        ):
            raise StorageError("invalid bounded margin runtime registry")
        if sum(len(row["parameters"]["strategies"]) for row in data["accounts"].values()) > 100:
            raise StorageError("margin runtime strategy capacity exceeded")
        if data["schema_version"] == 2:
            from omega.runtime.margin_retirement_runtime import validate_registry

            validate_registry(data)
        return data

    def _write(self, data):
        self._guard()
        atomic_json(self.path, {"data": data, "sha256": sha256(_json(data)).hexdigest()})
        self._guard()
        with self.condition:
            self.data = deepcopy(data)

    def start(self):
        with self.condition:
            self._guard()
            if self.thread is not None:
                raise ValidationError("margin runtime already started")
            self.thread = threading.Thread(
                target=self._loop, name="Omega-MarginTrading", daemon=True
            )
            self.thread.start()
        return self.snapshot()

    def submit(self, identity, kind, parameters):
        _identity(identity)
        if (
            type(kind) is not str
            or kind
            not in {
                "configure",
                "resume",
                "pause",
                "reconcile",
                "update_brackets",
                "resume_strategy",
                "retire",
            }
            or type(parameters) is not dict
            or len(_json(parameters)) > 131072
        ):
            raise ValidationError("invalid bounded margin runtime operation")
        digest = sha256(_json((kind, parameters))).hexdigest()
        with self.condition:
            self._guard()
            if not self.ready or self.error is not None:
                raise ValidationError("margin runtime is not ready")
            previous = self.operations.get(identity)
            if previous is not None:
                if previous["request_digest"] != digest:
                    raise ValidationError("margin operation ID conflicts with its original request")
                if (
                    kind == "retire"
                    and previous["state"] == "unknown"
                    and self.retirement.retryable(identity, parameters)
                ):
                    if len(self.queue) >= 8:
                        raise ValidationError("margin runtime queue capacity reached")
                    previous.update(state="queued", error=None)
                    self.queue.append((identity, kind, deepcopy(parameters)))
                    self.condition.notify_all()
                return deepcopy(previous)
            if len(self.queue) >= 8 or len(self.operations) >= 128:
                raise ValidationError("margin runtime queue/history capacity reached")
            row = {
                "id": identity,
                "kind": kind,
                "request_digest": digest,
                "core_epoch": self.core_epoch,
                "state": "queued",
                "result": None,
                "error": None,
            }
            self.operations[identity] = row
            self.queue.append((identity, kind, deepcopy(parameters)))
            self.condition.notify_all()
            return deepcopy(row)

    def status(self, identity):
        with self.condition:
            if identity not in self.operations:
                raise ValidationError("margin runtime operation not found")
            return deepcopy(self.operations[identity])

    def snapshot(self):
        with self.condition:
            funding_supported = self.session_factory is None or (
                getattr(self.session_factory, "supports_scenario_funding", None) is True
            )
            cost_supported = self.session_factory is None or (
                getattr(self.session_factory, "supports_cost_policy", None) is True
            )
            result = {
                "schema_version": 1,
                "scope": "virtual_margin_v2_ohlcv_proxy",
                "core_epoch": self.core_epoch,
                "state": "closed"
                if self.closed
                else "unavailable"
                if self.error
                else "ready"
                if self.ready
                else "starting",
                "error": self.error,
                "accounts": deepcopy(list(self.views.values())),
                "operations": deepcopy(list(self.operations.values())),
                "capabilities": {
                    "automatic_ts": True,
                    "retirement": self.session_factory is None
                    or callable(getattr(self.session_factory, "retirement_snapshot", None)),
                    "module_local_resume": self.session_factory is None
                    or callable(getattr(self.session_factory, "resume_strategy", None)),
                    "real_orders": False,
                    "funding": funding_supported,
                    "funding_scope": "minute_boundary_scenario_rate_v1"
                    if funding_supported
                    else None,
                    "liquidation": False,
                    "partial_fill_generator": False,
                    "slippage": cost_supported,
                    "cost_policy": cost_supported,
                    "cost_policy_scope": "fixed_adverse_full_fill_delay_v1"
                    if cost_supported
                    else None,
                },
                "worker_alive": bool(self.thread and self.thread.is_alive()),
            }
            metadata = self.retirement.metadata()
            if metadata:
                result["retired_accounts"] = metadata
            return result

    def read_configuration(self, account_id):
        """Copy only confirmed registered settings from the bounded cache.

        No session, financial fence, file, reconciliation or clock is involved.
        The copied configuration is provenance, not permission to resume/copy
        a model's state or replace an existing financial namespace.
        """
        _identity(account_id)
        with self.condition:
            reason = None
            slot = None
            if self.ready is not True or self.closed or self.error is not None:
                reason = "service_unavailable"
            else:
                retired = self.retirement.retired(account_id)
                if retired is not None:
                    return self.retirement.history.configuration(
                        account_id, retired["receipt_sha256"]
                    )
                accounts = self.data.get("accounts") if type(self.data) is dict else None
                slot = accounts.get(account_id) if type(accounts) is dict else None
                view = self.views.get(account_id)
                if type(slot) is not dict:
                    reason = "account_unavailable"
                elif (
                    type(slot.get("creation_state")) is not str
                    or slot["creation_state"] != "created"
                    or account_id in self.blocked_accounts
                    or type(view) is not dict
                    or type(view.get("state")) is not str
                    or view["state"] != "ready"
                    or view.get("error") is not None
                    or type(view.get("account_id")) is not str
                    or view["account_id"] != account_id
                ):
                    reason = "configuration_unconfirmed"
            if reason is not None:
                return configuration_response(account_id, reason_code=reason)
            try:
                return configuration_response(account_id, parameters=slot.get("parameters"))
            except ValidationError:
                return configuration_response(account_id, reason_code="configuration_invalid")

    def readonly_portfolio(self):
        """Bounded cached virtual observation; no owner, files, commands or clock."""
        from omega.runtime.mobile_margin_view import (
            mobile_margin_portfolio,
            select_cached_margin_view,
        )

        with self.condition:
            declared = self.data.get("accounts") if type(self.data) is dict else None
            selected = select_cached_margin_view(
                declared, self.views, blocked_accounts=self.blocked_accounts
            )
            ready = self.ready is True and not self.closed and self.error is None
        return mobile_margin_portfolio(selected, service_ready=ready)

    def inspect_shutdown(self, timeout=5) -> ShutdownSnapshot:
        """Fresh owner barrier, without core locks, financial IO or command history.

        Callers must not hold the core mutation lock while waiting. A timeout is
        unknown, never evidence of an empty account or completed shutdown.
        """
        if type(timeout) not in {int, float} or not math.isfinite(timeout) or not 0 < timeout <= 30:
            raise ValidationError("shutdown inspection timeout must be within (0,30]")
        ticket = _ShutdownObservation()
        with self.condition:
            if self.error == "cleanup_unconfirmed":
                return _shutdown_unavailable("owner_cleanup_unconfirmed")
            if self.closed:
                return _shutdown_unavailable("owner_closed")
            if not self.ready or self.error or self.thread is None or not self.thread.is_alive():
                return _shutdown_unavailable("owner_unavailable")
            if threading.current_thread() is self.thread:
                return _shutdown_unavailable("owner_reentrant_inspection")
            if len(self._shutdown_waiters) >= MAX_SHUTDOWN_OBSERVATIONS:
                return _shutdown_unavailable("inspection_queue_full")
            self._shutdown_waiters.add(ticket)
            self._shutdown_observations.append(ticket)
            self.condition.notify_all()
        ticket.event.wait(timeout)
        with self.condition:
            if ticket.result is None:
                ticket.expired = True
                self._shutdown_waiters.discard(ticket)
                try:
                    self._shutdown_observations.remove(ticket)
                except ValueError:
                    pass  # The owner may already be inspecting this ticket.
                return _shutdown_unavailable("inspection_timeout")
            return deepcopy(ticket.result)

    def read_ledger(
        self,
        account_id,
        *,
        offset=0,
        limit=128,
        snapshot=None,
        start_ms=None,
        end_ms=None,
        timeout=5,
    ):
        with self.condition:
            retired = (
                self.retirement.retired(account_id)
                if type(account_id) is str and self.ready and not self.closed and not self.error
                else None
            )
        if retired is not None:
            from omega.runtime.margin_ledger import LedgerQuery, unavailable_ledger

            if (
                type(timeout) not in {int, float}
                or not math.isfinite(timeout)
                or not 0 < timeout <= 5
            ):
                raise ValidationError("ledger timeout must be within(0,5]seconds")
            query = LedgerQuery(offset, limit, snapshot, start_ms, end_ms)
            try:
                return self.retirement.history.ledger_page(
                    account_id, retired["receipt_sha256"], **query.arguments()
                )
            except ProtocolError:
                return unavailable_ledger(account_id, query, "snapshot_conflict")
        return self._ledger_reads.request(
            account_id,
            offset=offset,
            limit=limit,
            snapshot=snapshot,
            start_ms=start_ms,
            end_ms=end_ms,
            timeout=timeout,
        )

    def inspect_retirement(self, account_id, *, timeout=5):
        return self.retirement.inspect(account_id, timeout=timeout)

    def load_retired_history(self, identity, account_id, receipt_sha256):
        with self.condition:
            self._guard()
            if not self.ready or self.error:
                raise ValidationError("margin runtime is not ready")
            retired = self.retirement.retired(account_id)
            if retired is None or retired["receipt_sha256"] != receipt_sha256:
                raise ValidationError("retired history receipt is not confirmed")
            return self.retirement.history.load(identity, account_id, receipt_sha256)

    def retired_history_status(self, identity):
        return self.retirement.history.status(identity)

    def shutdown_drain(self, operation_id, issued_at_ms, *, timeout=5):
        return self.shutdown_bridge.request("drain", operation_id, issued_at_ms, timeout=timeout)

    def shutdown_cancel(self, operation_id, *, timeout=5):
        return self.shutdown_bridge.request("cancel", operation_id, timeout=timeout)

    def _shutdown_snapshot(self) -> ShutdownSnapshot:
        from omega.application.virtual_margin import VirtualMarginSnapshot

        result = _shutdown_unavailable("owner_unavailable")
        if self.closed or not self.ready or self.error or self.data is None:
            return result
        result.update(ready=True, durable=True, unknown=[])
        supported = True
        accounts = self.data["accounts"]
        for account_id in sorted(set(accounts) | set(self.sessions) | self.blocked_accounts):
            slot, session = accounts.get(account_id), self.sessions.get(account_id)
            if self.retirement.gated(account_id):
                result["unknown"].append(f"account_retirement_unconfirmed:{account_id}")
                supported = False
                continue
            if slot is None:
                result["unknown"].append(f"account_registry_missing:{account_id}")
            elif slot["creation_state"] != "created":
                result["unknown"].append(f"account_creating:{account_id}")
            if account_id in self.blocked_accounts:
                result["unknown"].append(f"account_quarantined:{account_id}")
            if session is None:
                result["unknown"].append(f"account_session_unavailable:{account_id}")
                continue
            try:
                financial = session.execution.snapshot()
                if not isinstance(financial, VirtualMarginSnapshot):
                    raise ProtocolError("typed authoritative financial snapshot required")
                if financial.state != "ready" or financial.pending_record_id is not None:
                    result["unknown"].append(f"account_financial_{financial.state}:{account_id}")
                account = financial.account
                if account is None or account.account_id != account_id:
                    result["unknown"].append(f"account_state_unavailable:{account_id}")
                    continue
                for position in account.positions:
                    if position.quantity != 0:
                        result["positions"].append(
                            {
                                "account_id": account_id,
                                "instance": position.owner_id,
                                "instrument": position.instrument.key,
                                "quantity": str(position.quantity),
                            }
                        )
                if len(financial.pending) > 100:
                    raise ProtocolError("financial pending capacity exceeded")
                for pending in financial.pending:
                    original = pending.original
                    if original.account_id != account_id:
                        raise ProtocolError("pending financial account identity mismatch")
                    result["pending_orders"].append(
                        {
                            "account_id": account_id,
                            "instance": original.owner_id,
                            "instrument": original.instrument.key,
                            "quantity": str(original.proposal.quantity),
                            "order_id": pending.order_id,
                            "reducing": original.proposal.reducing,
                            "side": original.proposal.side.value,
                        }
                    )
                if (
                    any(position.quantity != 0 for position in account.positions)
                    or financial.pending
                ):
                    preflight = getattr(session, "shutdown_preflight", None)
                    if preflight is None:
                        supported = False
                        result["notes"].append(f"account_drain_unsupported:{account_id}")
                    else:
                        plan = preflight(
                            "shutdown-inspection",
                            self.clock_ms(),
                            brackets=scenario_brackets(slot["parameters"]["brackets"]),
                        )
                        if plan.get("ready") is not True:
                            supported = False
                            result["notes"].append(
                                f"account_drain_blocked:{account_id}:{plan.get('reason')}"[:256]
                            )
                # Model/graph readiness is distinct from confirmed financial facts.
                try:
                    model = session.snapshot()
                except Exception as error:
                    result["notes"].append(
                        f"account_model_unavailable:{account_id}:{type(error).__name__}"
                    )
                else:
                    if model.get("state") != "ready" or model.get("graph_errors"):
                        result["notes"].append(f"account_model_unready:{account_id}")
                    if model.get("pending_frame"):
                        result["notes"].append(f"account_frame_pending:{account_id}")
            except Exception as error:
                result["unknown"].append(
                    f"account_inspection_unavailable:{account_id}:{type(error).__name__}"
                )
        financial_known = not result["unknown"]
        with self.condition:
            operations = tuple(
                (identity, row["state"]) for identity, row in self.operations.items()
            )
        for identity, state in sorted(operations):
            if state in {"queued", "running"}:
                result["unknown"].append(f"operation_pending:{identity}")
            elif state == "unknown":
                if financial_known:
                    result["notes"].append(f"operation_outcome_unknown:{identity}")
                else:
                    result["unknown"].append(f"operation_unknown:{identity}")
        result["ready"] = result["durable"] = not result["unknown"]
        result["supports_flatten"] = result["ready"] and supported
        _json(result)
        return result

    def _serve_shutdown_observations(self):
        with self.condition:
            tickets = tuple(self._shutdown_observations)
            self._shutdown_observations.clear()
        if not tickets:
            return
        try:
            result = self._shutdown_snapshot()
        except Exception as error:
            result = _shutdown_unavailable(f"inspection_unavailable:{type(error).__name__}")
        with self.condition:
            for ticket in tickets:
                if ticket in self._shutdown_waiters and not ticket.expired:
                    ticket.result = (
                        deepcopy(result)
                        if not self.closed
                        else _shutdown_unavailable("owner_closed")
                    )
                    self._shutdown_waiters.discard(ticket)
                    ticket.event.set()

    def _fail_shutdown_observations(self, reason):
        # Called with condition held, including close while a ticket is in-flight.
        self.shutdown_bridge.fail()
        self._ledger_reads.fail(reason)
        self.retirement.fail(reason)
        for ticket in self._shutdown_waiters:
            ticket.result = _shutdown_unavailable(reason)
            ticket.event.set()
        self._shutdown_waiters.clear()
        self._shutdown_observations.clear()

    def _open(self, parameters, *, create):
        if (
            any(
                type(row) is dict and "analysis_execution" in row
                for row in parameters["strategies"]
            )
            and self.analysis_execution_scope != "required_fields_v1"
        ):
            raise ValidationError("session factory does not support required analysis execution")
        expected = None
        if "cost_policy" in parameters:
            from omega.application.margin_cost import MarginCostPolicy

            expected = MarginCostPolicy.parse(parameters["cost_policy"])
        if self.session_factory is None:
            from omega.runtime.margin_session import MarginTradingSession

            factory = MarginTradingSession
        else:
            factory = self.session_factory
        session = factory(
            self.root,
            parameters["account"],
            parameters["strategies"],
            parameters["start_ms"],
            registry=self.registry,
            create=create,
            fee_rate=parameters.get("fee_rate", "0.001"),
            **self._session_options(parameters),
        )
        if expected is not None:
            try:
                observed = session.snapshot().get("cost_policy")
                if (
                    type(observed) is not dict
                    or set(observed) != {"scope", "policy", "policy_sha256", "venue_execution"}
                    or type(observed["scope"]) is not str
                    or observed["scope"] != "fixed_adverse_full_fill_delay_v1"
                    or observed["policy_sha256"] != expected.sha256
                    or observed["venue_execution"] is not False
                    or MarginCostPolicy.parse(observed["policy"]).document() != expected.document()
                ):
                    raise ValidationError("session did not confirm its configured cost policy")
            except Exception:
                try:
                    session.close()
                except Exception as error:
                    # Preserve ownership until the worker's final cleanup retry.
                    self._rejected_sessions.append(session)
                    raise StorageError("rejected cost session cleanup is unconfirmed") from error
                raise
        return session

    def _session_options(self, parameters):
        options = self._feature_options(parameters)
        if "funding" in parameters:
            options["funding"] = parameters["funding"]
        if "cost_policy" in parameters:
            options["cost_policy"] = parameters["cost_policy"]
        return options

    def _feature_options(self, parameters):
        # Preserve the original factory call for all configurations without
        # external inputs, including injected legacy factories.
        if not any(
            type(row) is dict and row.get("external_features") for row in parameters["strategies"]
        ):
            return {}
        return {
            "external_feature_provider": self.external_feature_provider,
            "clock_ms": self.clock_ms,
        }

    def _waiting_external(self, account_id, parameters, candles, *, immediate_protection=False):
        """Offer once through a cached port; never wait inside the financial fence.

        Existing orders and module-owned urgent protection bypass this grace.
        With no immediate financial work, a configured bounded wait can cover
        actual inference/evidence latency. A late result never rewrites a frame.
        """
        provider = self.external_feature_provider
        if provider is None or not self._feature_options(parameters):
            return False
        from omega.domain.external_features import ExternalFeatureBinding

        opening = candles[0].open_time_ms
        pending, seen = False, set()
        try:
            by_instrument = {bar.instrument: bar for bar in candles}
            for row in parameters["strategies"]:
                for raw in row.get("external_features", []):
                    binding = ExternalFeatureBinding.parse(raw)
                    if binding.binding_sha256 in seen:
                        continue
                    seen.add(binding.binding_sha256)
                    result = provider.offer_committed(binding, by_instrument[binding.instrument])
                    pending |= result.disposition == "pending"
        except Exception:
            # This is not financial recording failure. Session lookup and the
            # dispatch authorizer separately fail closed for new exposure.
            pending = False
        if pending and not immediate_protection:
            now = time.monotonic()
            previous = self._external_wait.get(account_id)
            first = previous[1] if previous is not None and previous[0] == opening else now
            self._external_wait[account_id] = (opening, first)
            if now - first < external_feature_wait_ms(parameters) / 1000:
                return True
        self._external_wait.pop(account_id, None)
        return False

    def _refresh(self, account_id, **changes):
        session = self.sessions[account_id]
        try:
            view = session.snapshot()
        except Exception as error:
            shutdown = getattr(session, "shutdown_status", lambda: {})()
            if not shutdown.get("recovery_required"):
                raise
            from omega.runtime.margin_store import account_document

            financial = session.execution.snapshot()
            view = {
                "account_id": account_id,
                "state": "financial_only",
                "error": type(error).__name__,
                "paused": financial.paused,
                "new_risk_ready": False,
                "account": None
                if financial.account is None
                else account_document(financial.account, ledger_limit=128),
                "pending_orders": [row.order_id for row in financial.pending],
                "shutdown": shutdown,
                "instruments": [item.instrument.key for item in session.instruments],
            }
        with self.condition:
            # Feed metadata belongs to this source owner, not the financial
            # session. A control snapshot must not erase the last observation.
            previous = self.views.get(account_id, {})
            feed = {key: previous[key] for key in ("feed_state", "feed_reason") if key in previous}
            if not feed:
                slot = (self.data or {}).get("accounts", {}).get(account_id)
                enabled = slot is not None and slot.get("feed_enabled") is True
                feed = {
                    "feed_state": "unavailable"
                    if slot is None
                    else "waiting"
                    if enabled
                    else "paused",
                    "feed_reason": "feed_not_registered"
                    if slot is None
                    else "awaiting_closed_minute"
                    if enabled
                    else "manual_start_required",
                }
            self.views[account_id] = {**feed, **view, **changes}

    def _execute(self, identity, kind, parameters):
        self._command_started = False
        if kind == "retire":
            return self.retirement.execute(identity, parameters)
        account_id = (
            parameters.get("account", {}).get("account_id")
            if kind == "configure" and type(parameters.get("account")) is dict
            else parameters.get("account_id")
        )
        if type(account_id) is str:
            if self.retirement.gated(account_id):
                raise ValidationError("account retirement must finish before further control")
            if kind == "configure" and account_id in self.data.get("retired", {}):
                raise ValidationError("a retired account ID cannot be reused")
        data = deepcopy(self.data)
        row = deepcopy(self.operations[identity])
        row["state"] = "running"
        data["operations"][identity] = row
        self._write(data)  # Durable intent before session/financial side effects.
        if kind == "configure":
            if (
                not {"account", "strategies", "start_ms", "brackets"} <= parameters.keys()
                or parameters.keys()
                - {
                    "account",
                    "strategies",
                    "start_ms",
                    "brackets",
                    "fee_rate",
                    "external_feature_wait_ms",
                    "funding",
                    "cost_policy",
                }
                or type(parameters["account"]) is not dict
                or type(parameters["strategies"]) is not list
            ):
                raise ValidationError(
                    "margin configuration needs account, strategies, start and bracket assumptions"
                )
            external_feature_wait_ms(parameters)
            if "cost_policy" in parameters:
                from omega.application.margin_cost import MarginCostPolicy

                if self.session_factory is not None and (
                    getattr(self.session_factory, "supports_cost_policy", None) is not True
                ):
                    raise ValidationError("injected margin session does not support cost policy")
                MarginCostPolicy.parse(parameters["cost_policy"])
            if "funding" in parameters:
                from omega.application.margin_funding import FundingSchedule

                if self.session_factory is not None and (
                    getattr(self.session_factory, "supports_scenario_funding", None) is not True
                ):
                    raise ValidationError(
                        "injected margin session does not support scenario funding"
                    )
                FundingSchedule.parse(parameters["funding"])
            account_id = _identity(parameters["account"]["account_id"])
            scenario_brackets(parameters["brackets"])
            existing = data["accounts"].get(account_id)
            if existing is not None and existing["parameters"] != parameters:
                raise ValidationError("existing margin configuration cannot be replaced")
            if existing is None:
                if (
                    len(data["accounts"]) >= 10
                    or sum(
                        len(slot["parameters"]["strategies"]) for slot in data["accounts"].values()
                    )
                    + len(parameters["strategies"])
                    > 100
                ):
                    raise ValidationError("margin runtime account/strategy capacity reached")
                if self.session_factory is None:
                    from omega.runtime.margin_session import prepare_margin_configuration

                    validator = prepare_margin_configuration
                else:
                    validator = getattr(self.session_factory, "prepare_configuration", None)
                    if not callable(validator):
                        raise ValidationError(
                            "injected session factory needs a configuration validator"
                        )
                validator(
                    parameters["account"],
                    parameters["strategies"],
                    parameters["start_ms"],
                    registry=self.registry,
                    fee_rate=parameters.get("fee_rate", "0.001"),
                    **{
                        key: value
                        for key, value in self._session_options(parameters).items()
                        if key != "clock_ms"
                    },
                )
                self._command_started = True
                data["accounts"][account_id] = {
                    "parameters": parameters,
                    "creation_state": "creating",
                    "feed_enabled": False,
                }
                self._write(data)
                self.sessions[account_id] = self._open(parameters, create=True)
                data["accounts"][account_id]["creation_state"] = "created"
                self._write(data)
            elif account_id not in self.sessions:
                raise ValidationError("existing margin session needs explicit recovery")
        else:
            allowed = (
                {"account_id", "brackets"}
                if kind == "update_brackets"
                else {"account_id", "strategy_id"}
                if kind == "resume_strategy"
                else {"account_id"}
            )
            if set(parameters) != allowed:
                raise ValidationError("invalid margin account control fields")
            if kind == "resume_strategy":
                _identity(parameters["strategy_id"])
            account_id = _identity(parameters["account_id"])
            session = self.sessions[account_id]
            self._command_started = True
            if kind == "resume":
                if account_id in self.blocked_accounts:
                    raise ValidationError(
                        "recording conflict needs explicit reconciliation before resume"
                    )
                session.resume(identity)
                data["accounts"][account_id]["feed_enabled"] = True
            elif kind == "resume_strategy":
                session.resume_strategy(identity, parameters["strategy_id"])
            elif kind == "pause":
                session.pause(identity)
            elif kind == "reconcile":
                session.reconcile()
                session.pause(sha256((identity + ":reconciled:" + account_id).encode()).hexdigest())
                self.blocked_accounts.discard(account_id)
            else:
                scenario_brackets(parameters["brackets"])
                data["accounts"][account_id]["parameters"]["brackets"] = parameters["brackets"]
            self._write(data)
        if kind == "resume":
            self._refresh(account_id, feed_state="waiting", feed_reason="awaiting_closed_minute")
        else:
            self._refresh(account_id)
        result = {"account_id": account_id}
        if kind == "resume_strategy":
            if self.views[account_id].get("paused") is not True:
                raise ProtocolError("strategy-local resume lacks its confirmed financial pause")
            result.update(strategy_id=parameters["strategy_id"], account_paused=True)
        row.update(state="completed", result=result)
        data["operations"][identity] = row
        self._write(data)
        with self.condition:
            self.operations[identity] = row

    def _analysis_inputs(self, session, view, candles, *, as_of_ms):
        channels = getattr(session, "analysis_input_channels", {})
        if not channels:
            return {}, None
        if (
            type(channels) is not dict
            or not set(channels) <= {bar.instrument.key for bar in candles}
            or any(
                type(value) is not tuple
                or not 1 <= len(value) <= 3
                or any(type(channel) is not str for channel in value)
                or set(value) - {"trade_flow", "depth", "metric"}
                or value != tuple(sorted(set(value)))
                for value in channels.values()
            )
        ):
            raise ProtocolError("invalid margin auxiliary source selection")
        metric_keys = {key for key, value in channels.items() if "metric" in value}
        policies = getattr(session, "analysis_metric_policies", None) if metric_keys else {}
        if type(policies) is not dict or set(policies) != metric_keys:
            raise ProtocolError("margin source needs its exact frozen metric policy")
        ages = {
            key: MetricInputPolicy.from_document(value).maximum_metric_age_ms
            for key, value in policies.items()
        }
        destination = destination_id(getattr(self.source, "destination_id", None))
        previous = (view.get("market_inputs") or {}).get("destination")
        if previous is not None and previous != destination:
            raise ProtocolError("margin auxiliary source destination changed")
        prepared, reason = [], None
        for bar in candles:
            if bar.instrument.key not in channels:
                continue
            try:
                rows = self.source.read_frames(
                    bar.instrument,
                    bar.open_time_ms,
                    bar.close_time_ms,
                    as_of_ms=as_of_ms,
                    channels=channels[bar.instrument.key],
                    maximum_metric_age_ms=ages.get(bar.instrument.key, 0),
                    limit=1,
                )
            except (StorageError, OSError, TimeoutError) as error:
                # Only this optional read is converted to unavailability. Financial
                # commits and malformed source data remain errors at their own boundary.
                rows = unavailable_frames(
                    (bar,),
                    channels,
                    as_of_ms=as_of_ms,
                    **({"metric_policies": policies} if metric_keys else {}),
                )
                reason = "auxiliary_read_" + type(error).__name__
            if destination_id(getattr(self.source, "destination_id", None)) != destination:
                raise ProtocolError("margin auxiliary source destination changed during its read")
            if type(rows) is not tuple or len(rows) != 1:
                raise ProtocolError("margin auxiliary source must return the exact selected minute")
            prepared.extend(rows)
        return {"prepared_frames": tuple(prepared), "auxiliary_destination": destination}, reason

    def _observe_protection(self, session, parameters, instruments):
        if self.protection_source is None or not callable(
            getattr(session, "observe_protection", None)
        ):
            return
        brackets = scenario_brackets(parameters["brackets"])
        with self._mutation():
            self._guard()
            session.observe_protection((), as_of_ms=self.clock_ms(), brackets=brackets)
        # An uncertain/staged original is finished above without fetching a replacement.
        try:
            observations = self.protection_source(tuple(instruments), as_of_ms=self.clock_ms())
        except (OSError, StorageError, ProtocolError, ValidationError):
            if self.on_incident is not None and not self.closed:
                self.on_incident("protection_observation_unavailable", "source")
            return  # Still process committed candles/fills; source failure grants no protection.
        if observations:
            with self._mutation():
                self._guard()
                session.observe_protection(
                    observations, as_of_ms=self.clock_ms(), brackets=brackets
                )

    def _tick(self):
        for account_id, slot in list(self.data["accounts"].items()):
            if (
                self.retirement.gated(account_id)
                or not slot["feed_enabled"]
                or account_id not in self.sessions
                or account_id in self.blocked_accounts
                or time.monotonic() < self.retry_at.get(account_id, 0)
            ):
                continue
            session = self.sessions[account_id]
            shutdown = getattr(session, "shutdown_status", lambda: {})()
            financial_only = shutdown.get("recovery_required", False)
            if financial_only and not shutdown.get("needs_settlement"):
                candidate = deepcopy(self.data)
                candidate["accounts"][account_id]["feed_enabled"] = False
                self._write(candidate)
                self._refresh(
                    account_id, feed_state="paused", feed_reason="shutdown_manual_recovery"
                )
                continue
            if slot.get("shutdown_control") and not financial_only:
                self._refresh(
                    account_id, feed_state="blocked", feed_reason="shutdown_state_unconfirmed"
                )
                continue
            view = (
                {
                    "next_open_ms": shutdown["next_open_ms"],
                    "instruments": [item.instrument.key for item in session.instruments],
                    "pending_frame": False,
                }
                if financial_only
                else session.snapshot()
            )
            start = view["next_open_ms"]
            now = self.clock_ms()
            phase = "protection"
            try:
                if not financial_only and not view.get("pending_frame"):
                    self._observe_protection(session, slot["parameters"], view["instruments"])
                if start + 60000 > now:
                    continue
                phase = "source"
                if view.get("pending_frame"):
                    phase = "session"
                    with self._mutation():
                        self._guard()
                        session.run_once(
                            (),
                            as_of_ms=self.clock_ms(),
                            brackets=(),
                            entries_allowed=bool(self.entry_gate()),
                        )
                    self._refresh(account_id, feed_state="running", feed_reason=None)
                    continue
                if self.source is None:
                    self.source = self.source_factory()
                candles = tuple(
                    self.source.read(
                        tuple(view["instruments"]), start, start + 60000, limit=100, as_of_ms=now
                    )
                )
                if (
                    len(candles) != len(view["instruments"])
                    or {c.instrument.key for c in candles} != set(view["instruments"])
                    or any(c.open_time_ms != start for c in candles)
                ):
                    self._refresh(
                        account_id,
                        feed_state="waiting",
                        feed_reason="common_closed_minute_unavailable",
                    )
                    continue
                immediate = True
                if not financial_only and self._feature_options(slot["parameters"]):
                    try:
                        immediate = (
                            session.requires_immediate_protection(
                                candles,
                                as_of_ms=self.clock_ms(),
                                brackets=scenario_brackets(slot["parameters"]["brackets"]),
                            )
                            is not False
                        )
                    except Exception:
                        immediate = True
                if not financial_only and self._waiting_external(
                    account_id, slot["parameters"], candles, immediate_protection=immediate
                ):
                    self._refresh(
                        account_id,
                        feed_state="waiting",
                        feed_reason="bounded_external_inference_grace",
                    )
                    continue
                options, auxiliary_reason = {}, None
                if not financial_only:
                    phase = "auxiliary_source"
                    options, auxiliary_reason = self._analysis_inputs(
                        session, view, candles, as_of_ms=now
                    )
                with self._mutation():
                    self._guard()
                    phase = "session"
                    if financial_only:
                        session.shutdown_run_once(
                            candles,
                            as_of_ms=self.clock_ms(),
                            brackets=scenario_brackets(slot["parameters"]["brackets"]),
                        )
                    else:
                        session.run_once(
                            candles,
                            as_of_ms=self.clock_ms(),
                            brackets=scenario_brackets(slot["parameters"]["brackets"]),
                            entries_allowed=bool(self.entry_gate()),
                            **options,
                        )
                self._refresh(account_id, feed_state="running", feed_reason=auxiliary_reason)
            except Exception as error:
                financial_failure = phase in {"session", "protection"} and isinstance(
                    error, (OSError, StorageError, ProtocolError)
                )
                self._refresh(
                    account_id,
                    feed_state="blocked",
                    feed_reason=type(error).__name__,
                    manual_recovery_required=financial_failure,
                )
                if financial_failure:
                    self.blocked_accounts.add(account_id)
                    if self.on_incident is not None and not self.closed:
                        self.on_incident("margin_recording_unavailable", account_id)
                else:
                    self.retry_at[account_id] = time.monotonic() + 3

    def _loop(self):
        try:
            data = self._read()
            for row in data["operations"].values():
                if row["state"] in {"queued", "running"}:
                    row.update(state="unknown", error="core_restarted_manual_recovery")
            self._write(data)
            with self.condition:
                self.operations = deepcopy(data["operations"])
            self.retirement.recover()
            data = deepcopy(self.data)
            for account_id, slot in data["accounts"].items():
                if self.retirement.gated(account_id):
                    continue
                try:
                    self.sessions[account_id] = self._open(
                        slot["parameters"], create=slot["creation_state"] == "creating"
                    )
                    slot.update(creation_state="created", feed_enabled=False)
                    self._refresh(
                        account_id, feed_state="paused", feed_reason="core_restart_manual_resume"
                    )
                    view = self.views[account_id]
                    positions = (view.get("account") or {}).get("account", {}).get("positions", [])
                    if view.get("pending_orders") or any(
                        Decimal(row["quantity"]) != 0 for row in positions
                    ):
                        self._refresh(
                            account_id,
                            feed_state="paused",
                            feed_reason="core_restart_manual_resume",
                            manual_protection_required=True,
                        )
                        if self.on_incident is not None:
                            self.on_incident("margin_open_positions_on_restart", account_id)
                except Exception as error:
                    self.views[account_id] = {
                        "account_id": account_id,
                        "state": "unavailable",
                        "error": type(error).__name__,
                    }
            self._write(data)
            with self.condition:
                self.operations = deepcopy(data["operations"])
                self.ready = True
                self.condition.notify_all()
            next_tick = time.monotonic()
            while True:
                self.shutdown_bridge.serve()
                self._serve_shutdown_observations()
                read_pending = False
                with self.condition:
                    if self.closed:
                        break
                    item = self.queue.popleft() if self.queue else None
                    active = any(row["feed_enabled"] for row in self.data["accounts"].values())
                    if item is None and (not active or time.monotonic() < next_tick):
                        if self._shutdown_observations or self.shutdown_bridge.queue:
                            continue
                        if self.retirement.queue and (
                            self._observation_turn == "retirement" or not self._ledger_reads.queue
                        ):
                            read_pending = "retirement"
                        elif self._ledger_reads.queue:
                            read_pending = "ledger"
                        else:
                            self.condition.wait(
                                max(0, next_tick - time.monotonic()) if active else None
                            )
                            continue
                if read_pending:
                    if read_pending == "ledger":
                        self._ledger_reads.serve_one()
                        self._observation_turn = "retirement"
                    else:
                        self.retirement.serve_one()
                        self._observation_turn = "ledger"
                    continue
                if item is not None:
                    try:
                        with self._mutation():
                            self._guard()
                            self._execute(*item)
                    except Exception as error:
                        identity = item[0]
                        # A rejected preflight has no financial/namespace side
                        # effect. Later errors remain unknown and are not replayed.
                        with self.condition:
                            self.operations[identity].update(
                                state="unknown" if self._command_started else "failed",
                                error=type(error).__name__,
                            )
                        if not self._command_started:
                            rejected = deepcopy(self.data)
                            rejected["operations"][identity] = deepcopy(self.operations[identity])
                            self._write(rejected)
                        if item[1] == "retire" and self._command_started:
                            self.retirement.mark_unknown(identity, error)
                        if isinstance(error, (OSError, StorageError)) and (
                            item[1] != "retire" or self.retirement.registry_uncertain
                        ):
                            raise
                self._tick()
                next_tick = time.monotonic() + 1
        except Exception as error:
            with self.condition:
                self.error = type(error).__name__
                self.ready = False
                self._fail_shutdown_observations("owner_unavailable")
                self.condition.notify_all()
            if self.on_incident is not None:
                self.on_incident("margin_runtime_unavailable", type(error).__name__)
        finally:
            self._finish_resources()

    def cleanup_status(self):
        return self._resource_cleanup.status()

    def _finish_resources(self):
        resources = [
            ("session:" + identity, resource) for identity, resource in self.sessions.items()
        ]
        resources += [
            ("rejected_session:" + str(index), resource)
            for index, resource in enumerate(self._rejected_sessions)
        ]
        if self.source is not None:
            resources.append(("source", self.source))
        resources.extend(self.retirement.resources())
        seen = set()
        for name, resource in reversed(resources):
            if id(resource) in seen:
                continue
            seen.add(id(resource))
            status = getattr(resource, "cleanup_status", None)
            if callable(status):
                generation = [None]

                def released(status=status, generation=generation):
                    document = status()
                    if (
                        type(document) is dict
                        and type(document.get("generation")) is str
                        and re.fullmatch(r"[a-f0-9]{32}", document["generation"])
                    ):
                        if generation[0] is None:
                            generation[0] = document["generation"]
                    return confirmed_cleanup(document, generation=generation[0])
            elif hasattr(type(resource), "released"):

                def released(resource=resource):
                    return resource.released is True
            else:
                released = None
            self._resource_cleanup.add(
                name, resource, released=released, initial_ack=released is None
            )
        if self.error != "cleanup_unconfirmed":
            self._cleanup_prior_error = self.error
        while True:
            with self.condition:
                requested = self._cleanup_requested
            failure = False
            try:
                self._resource_cleanup.close()
            except Exception:
                failure = True
            complete = confirmed_cleanup(self.cleanup_status())
            with self.condition:
                self._cleanup_attempted = requested
                self._cleanup_complete, self._cleanup_attempt_error = complete, failure
                if complete:
                    if failure or (
                        self._cleanup_reported_serial > 0
                        and self._cleanup_reported_serial >= requested
                    ):
                        # Even confirmed release does not discard the first
                        # reported close error before an explicit observation.
                        self.error = "cleanup_unconfirmed"
                        self._fail_shutdown_observations("owner_cleanup_unconfirmed")
                    elif self.error == "cleanup_unconfirmed":
                        self.error = self._cleanup_prior_error
                else:
                    self.error = "cleanup_unconfirmed"
                    self._fail_shutdown_observations("owner_cleanup_unconfirmed")
                self.condition.notify_all()
                if complete:
                    return
                # Preserve owner-affine handles and wait for a new explicit close
                # request. Never return to financial callbacks or busy-retry IO.
                self.condition.wait_for(
                    lambda requested=requested: self._cleanup_requested > requested
                )

    def close(self):
        with self.mutation_lock:
            with self.condition:
                self.closed = True
                if self._cleanup_requested == 0 and self.error != "cleanup_unconfirmed":
                    self._cleanup_prior_error = self.error
                self._cleanup_requested += 1
                requested = self._cleanup_requested
                self._fail_shutdown_observations("owner_closed")
                self.condition.notify_all()
        if self.thread is None:
            if self.sessions or self._rejected_sessions or self.source is not None:
                self.error = "cleanup_unconfirmed"
                self._cleanup_reported_serial = requested
                raise StorageError("margin runtime owner cleanup is not confirmed")
            self.retirement.history.close()
            self._resource_cleanup.close()
            self._cleanup_complete = True
        elif isinstance(self.thread, threading.Thread):
            if self.thread is threading.current_thread():
                self._cleanup_reported_serial = requested
                raise StorageError("margin runtime cannot await cleanup on its own worker")
            if not self.thread.is_alive() and not self._cleanup_complete:
                self.error = "cleanup_unconfirmed"
                self._cleanup_reported_serial = requested
                raise StorageError("margin runtime owner cleanup is not confirmed")
            with self.condition:
                finished = self.condition.wait_for(
                    lambda: self._cleanup_complete or self._cleanup_attempted >= requested,
                    timeout=12,
                )
                if not finished or not self._cleanup_complete:
                    self.error = "cleanup_unconfirmed"
                    self._cleanup_reported_serial = max(self._cleanup_reported_serial, requested)
                    raise StorageError("margin runtime resource cleanup is not confirmed")
                reported_error = (
                    self._cleanup_attempt_error and self._cleanup_attempted == requested
                )
                if (
                    not reported_error
                    and self.error == "cleanup_unconfirmed"
                    and confirmed_cleanup(self.cleanup_status())
                ):
                    self.error = self._cleanup_prior_error
            self.thread.join(12)
            if self.thread.is_alive():
                self.error = "cleanup_unconfirmed"
                self._cleanup_reported_serial = max(self._cleanup_reported_serial, requested)
                raise StorageError("margin runtime worker shutdown is not confirmed")
            if reported_error:
                self._cleanup_reported_serial = max(self._cleanup_reported_serial, requested)
                raise StorageError("margin runtime resource cleanup reported its first error")
        else:
            self.thread.join(12)
            if self.thread.is_alive():
                self.error = "cleanup_unconfirmed"
                raise StorageError("margin runtime worker shutdown is not confirmed")
        if self.error == "cleanup_unconfirmed":
            raise StorageError("margin runtime resource cleanup is not confirmed")
        return self.snapshot()
