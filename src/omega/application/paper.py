"""Transactional paper account owner. No exchange execution or network dependency.

Version 1 models full fills, quote fees, spot cash and unleveraged linear collateral.
It deliberately rejects leveraged/cross/isolated liquidation claims. Orders created
after a closed minute first become eligible in the NEXT minute.
"""

import json
import re
import time
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from threading import RLock
from uuid import uuid4

from omega.analysis.warmup import (
    CHUNK_MINUTES,
    MAX_WARMUP,
    MINUTE,
    SHORT_WARMUP,
    aligned_window,
    summary,
)
from omega.application.execution_rules import sizing_constraints, validate_execution_order
from omega.application.paper_fill import resolve_bar_fill
from omega.application.paper_inputs import PaperInputBatch
from omega.application.paper_instruments import require_paper_settlement
from omega.application.paper_shutdown import inspect_finance, is_shutdown_row, shutdown_marker
from omega.application.paper_state import execution_policy, execution_snapshot, validate_state
from omega.application.trading import TradingEngine, validate_decision
from omega.domain.errors import CommandRejected, TradingBlocked, ValidationError
from omega.domain.execution_rules import MAX_TIME, ExecutionRulesSnapshot
from omega.domain.market_inputs import MarketFeatureFrame, market_frame_checksum
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side, decimal
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
)


def parse_instrument(value):
    try:
        provider, market, symbol = value.split(":")
        instrument = Instrument(provider, Market(market), symbol)
    except (ValueError, AttributeError, TypeError):
        raise ValidationError("expected provider:market:SYMBOL") from None
    if instrument.provider != "binance" or instrument.market not in {Market.SPOT, Market.USDM}:
        raise ValidationError("paper v1 supports Binance spot and unleveraged linear USDM")
    return instrument


def parse_candle(instrument, row):
    if (
        not isinstance(row, dict)
        or set(row) != {"open_time_ms", "open", "high", "low", "close", "volume"}
        or type(row["open_time_ms"]) is not int
    ):
        raise ValidationError("expected a closed 1m OHLCV object")
    return Candle(
        instrument,
        row["open_time_ms"],
        row["open_time_ms"] + 60000,
        *(decimal(row[k]) for k in ("open", "high", "low", "close", "volume")),
    )


def candle_row(candle):
    return {
        "open_time_ms": candle.open_time_ms,
        **{k: str(getattr(candle, k)) for k in ("open", "high", "low", "close", "volume")},
    }


def identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value):
        raise ValidationError("invalid account or strategy identity")
    return value


class PaperTradingService:
    history_limit = 10_000  # Portfolio-wide display cache; PostgreSQL owns market history.

    def __init__(
        self,
        module_factory,
        feature_factory,
        *,
        persist=lambda state: None,
        state=None,
        rules_provider=None,
        execution_policy_provider=None,
        clock_ms=None,
    ):
        if any(
            callback is not None and not callable(callback)
            for callback in (rules_provider, execution_policy_provider, clock_ms)
        ):
            raise ValidationError("paper execution providers must be memory-only callbacks")
        self.factory, self.feature_factory, self.persist = module_factory, feature_factory, persist
        self.rules_provider, self.execution_policy_provider = (
            rules_provider,
            execution_policy_provider,
        )
        self.execution_clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.lock = RLock()
        self.feed_epoch = uuid4().hex
        self._owner_retired = False
        self._recording_available = True
        self._recording_failure_revision = 0
        self._recording_manual_resume_required = False
        self._recording_incident_announced = False
        self._shutdown_uncertain = False
        self.data = (
            deepcopy(state)
            if state is not None
            else {
                "schema_version": 1,
                "paused": True,
                "accounts": {},
                "strategies": {},
                "candles": {},
            }
        )
        validate_state(self.data)
        self.data.setdefault("feed", self._empty_feed())
        self.data["paused"] = True
        self.data["feed"].update(ready=False, reason="restart", warmed={})
        self.engines, self.features = {}, {}
        for strategy in self.data["strategies"].values():
            # Arbitrary external plugin state cannot be reconstructed by guessing.
            strategy["status"] = "recovery_required"
        self._persist_state()

    def shutdown_snapshot(self):
        """Fresh financial inspection without copying or checkpointing a plugin."""
        with self.lock:
            unknown, notes = [], []
            try:
                positions, pending = inspect_finance(self.data)
            except (ValidationError, ValueError, TypeError, KeyError, ArithmeticError):
                positions, pending = [], []
                unknown.append("paper_financial_state_invalid")
            if self._owner_retired:
                unknown.append("paper_owner_retired")
            if (
                self._shutdown_uncertain
                or not self._recording_available
                or self._recording_manual_resume_required
            ):
                unknown.append("paper_recording_unconfirmed")
            if any(row.get("status") != "ready" for row in self.data["strategies"].values()):
                notes.append("model recovery is separate from known financial state")
            if any(is_shutdown_row(row) for row in self.data["strategies"].values()):
                notes.append(
                    "shutdown reductions settle normally; explicit model recovery required"
                )
            if not self.data["feed"]["enabled"]:
                notes.append("manual paper feed uses explicit synthetic constraints unless enabled")
            return {
                "owner": "paper_v1",
                "ready": not unknown,
                "durable": not unknown,
                "supports_flatten": True,
                "positions": positions,
                "pending_orders": pending,
                "unknown": unknown,
                "notes": notes,
            }

    def _persist_financial_state(self, candidate):
        """One data-only commit. Any missing ACK fences stale in-memory finance."""
        try:
            self.persist(deepcopy(candidate))
        except Exception:
            self._shutdown_uncertain = True
            self._recording_available = False
            self._recording_failure_revision += 1
            if not self._recording_manual_resume_required:
                self._recording_incident_announced = False
            self._recording_manual_resume_required = True
            raise
        self.data = candidate
        self._recording_available = True

    def shutdown_drain(self, operation_id, issued_at_ms):
        """Atomically cancel old requests and install full reductions, never fill now."""
        marker = shutdown_marker(operation_id, issued_at_ms)
        prefix = marker.rsplit(":", 1)[0] + ":"
        with self.lock:
            self._require_owner()
            state = self.shutdown_snapshot()
            if not state["ready"] or not state["durable"]:
                raise TradingBlocked("paper shutdown needs confirmed financial state")
            matching = [
                row.get("last_reason", "")
                for row in self.data["strategies"].values()
                if row.get("last_reason", "").startswith(prefix)
            ]
            if matching:
                if any(value != marker for value in matching):
                    raise ValidationError("paper shutdown operation issue time conflicts")
                if len(matching) != len(self.data["strategies"]):
                    raise TradingBlocked("paper shutdown marker coverage changed")
                return {"duplicate": True, "state": self.shutdown_snapshot()}
            now = self.execution_clock_ms()
            if (
                type(now) is not int
                or not issued_at_ms <= now <= MAX_TIME
                or now - issued_at_ms > 60000
            ):
                raise TradingBlocked("paper shutdown issue time is stale or future")
            candidate = deepcopy(self.data)
            candidate["paused"] = True
            for row in candidate["strategies"].values():
                quantity = decimal(row["quantity"])
                pending = None
                if quantity:
                    history = candidate["candles"].get(row["instrument"], [])
                    if not history:
                        raise TradingBlocked("paper shutdown has no observed close price")
                    candle = parse_candle(parse_instrument(row["instrument"]), history[-1])
                    source = candidate["feed"]["provenance"].get(row["instrument"])
                    if source is None and not candidate["feed"]["enabled"]:
                        source = "manual_paper"
                    if source not in {"manual_paper", "database_committed"}:
                        raise TradingBlocked("paper shutdown input provenance is unavailable")
                    setup = self._execution_setup(row, candle, source)
                    max_age = setup["policy"]["max_age_ms"]
                    decision_now = setup["now"] if setup["active"] else now
                    if (
                        not setup["policy_valid"]
                        or decision_now is None
                        or decision_now < now
                        or not 0 <= decision_now - candle.close_time_ms <= max_age
                        or row["last_changed_ms"] > decision_now
                    ):
                        raise TradingBlocked("paper shutdown requires a fresh known close price")
                    if source == "database_committed" and not setup["active"]:
                        raise TradingBlocked("paper shutdown venue rules are unavailable")
                    proposal = OrderProposal(
                        Side.SELL if quantity > 0 else Side.BUY,
                        abs(quantity),
                        OrderKind.MARKET,
                        None,
                        True,
                        marker,
                    )
                    context = TradeContext(
                        candle.instrument,
                        candle.close,
                        candle.close_time_ms,
                        PositionView(
                            quantity,
                            decimal(row["entry_price"]),
                            row["revision"],
                            row["last_changed_ms"],
                        ),
                        BudgetView(decimal(row["budget"]), Decimal(0)),
                        setup["constraints"],
                        candle,
                        {},
                        False,
                    )
                    validate_decision(context, TradeDecision(orders=(proposal,), reason=marker))
                    pin = (
                        self._execution_pin(row, candle, setup, proposal, retrospective=False)
                        if setup["active"]
                        else None
                    )
                    pending = {
                        "side": proposal.side.value,
                        "kind": "MARKET",
                        "quantity": str(abs(quantity)),
                        "price": None,
                        "reducing": True,
                        "reason": marker,
                        "eligible_at_ms": max(now, issued_at_ms, setup["now"] or 0),
                        "reserved": "0",
                        **({"execution": pin} if pin is not None else {}),
                    }
                row.update(pending=pending, status="recovery_required", last_reason=marker)
            candidate["feed"]["revision"] += 1
            inspect_finance(candidate)
            self._persist_financial_state(candidate)
            # Financial ACK precedes detachment. Checkpoint bytes/cursors stay unchanged.
            self.engines, self.features = {}, {}
            return {"duplicate": False, "state": self.shutdown_snapshot()}

    def shutdown_cancel(self, operation_id):
        """Keep already submitted reductions and entry pause; never restore old orders."""
        prefix = shutdown_marker(operation_id, 0).rsplit(":", 1)[0] + ":"
        with self.lock:
            self._require_owner()
            if any(
                row.get("last_reason", "").startswith(prefix)
                for row in self.data["strategies"].values()
            ):
                if not self.data["paused"]:
                    raise TradingBlocked("shutdown pause changed; manual inspection required")
            return {
                "entries_resumed": False,
                "reductions_retained": True,
                "state": self.shutdown_snapshot(),
            }

    def readonly_snapshot(self):
        """Cached financial fields only: a device read never invokes a TS plugin."""
        with self.lock:
            confirmed = (
                self._recording_available
                and not self._recording_manual_resume_required
                and not self._shutdown_uncertain
                and not self._owner_retired
            )
            accounts = [
                {
                    "account_id": name,
                    "positions": [
                        {key: row[key] for key in ("instrument", "quantity", "entry_price")}
                        for row in self.data["strategies"].values()
                        if row["account_id"] == name and decimal(row["quantity"])
                    ]
                    if confirmed
                    else None,
                }
                for name in self.data["accounts"]
            ]
            return {
                "accounts": accounts,
                "paused": self.data["paused"],
                "feed": {key: self.data["feed"][key] for key in ("enabled", "ready", "reason")},
                "recording": {
                    "available": self._recording_available,
                    "manual_resume_required": self._recording_manual_resume_required,
                    "owner_retired": self._owner_retired,
                },
            }

    def snapshot(self):
        with self.lock:
            accounts, strategies = [], []
            for name, account in self.data["accounts"].items():
                positions = []
                for row in self.data["strategies"].values():
                    if row["account_id"] == name and decimal(row["quantity"]):
                        positions.append(
                            {
                                "instrument": row["instrument"],
                                "quantity": row["quantity"],
                                "entry_price": row["entry_price"],
                                "control": row["status"],
                            }
                        )
                accounts.append(
                    {
                        "account_id": name,
                        "market": account.get("market") or "unassigned",
                        "balance": account["balance"],
                        "cash": account["balance"],
                        "quote_asset": "USDT",
                        "positions": positions,
                        "status": "paused" if self.data["paused"] else "enabled",
                    }
                )
            for name, row in self.data["strategies"].items():
                fields = (
                    "account_id",
                    "instrument",
                    "module_id",
                    "budget",
                    "quantity",
                    "entry_price",
                    "revision",
                    "last_changed_ms",
                    "status",
                    "last_candle",
                    "last_reason",
                    "history_preparation",
                    "execution_health",
                    "analysis_input_health",
                )
                strategies.append(
                    {
                        "instance": name,
                        **{k: row[k] for k in fields if k in row},
                        "has_pending_order": row["pending"] is not None,
                        "fill_count": len(row["fills"]),
                        "model_history_ready": self._model_history_status(name)[
                            "model_history_ready"
                        ],
                    }
                )
            return {
                "accounts": accounts,
                "strategies": strategies,
                "paused": self.data["paused"],
                "limitations": ["full_fill", "leverage_1_only", "no_liquidation_or_funding"],
                "model_version": "paper-linear-v1",
                "feed": self._public_feed(),
                "recording": self.recording_health(),
                "execution": self.execution_health(),
            }

    def execution_health(self):
        """Diagnostic values only. Incident delivery must happen outside paper.lock."""
        with self.lock:
            states, incidents = [], []
            for instance, row in self.data["strategies"].items():
                exposed = decimal(row["quantity"]) != 0
                health = row.get("execution_health")
                if health is None:
                    if row["status"] == "ready" or not exposed:
                        continue
                    # Older saved positions have no venue-rule metadata. Their
                    # missing strategy owner still prevents local protection.
                    health = {
                        "state": "synthetic",
                        "mode": "synthetic",
                        "reason": "execution_metadata_not_recorded",
                        "input_source": "not_recorded",
                        "snapshot_hash": None,
                        "observed_at_ms": None,
                        "decision_at_ms": None,
                        "unchecked": [],
                        "protection_blocked": False,
                    }
                health = deepcopy(health)
                if row["status"] != "ready" and (exposed or health["mode"] != "synthetic"):
                    health.update(
                        state="protection_unavailable" if exposed else "blocked",
                        reason="strategy_recovery_or_quarantine",
                        protection_blocked=exposed,
                    )
                states.append({"instance": instance, **deepcopy(health)})
                if health["state"] in {"blocked", "protection_unavailable"}:
                    incidents.append(
                        {
                            "account_id": row["account_id"],
                            "instance": instance,
                            "instrument": row["instrument"],
                            "code": "POSITION_PROTECTION_UNAVAILABLE"
                            if health["protection_blocked"]
                            else "EXECUTION_RULES_UNAVAILABLE",
                            "severity": "critical" if health["protection_blocked"] else "warning",
                        }
                    )
            return {
                "configured": self.rules_provider is not None,
                "states": states,
                "incidents": incidents,
                "owner_retired": self._owner_retired,
            }

    def _execution_setup(self, row, candle, input_source):
        policy = {"mode": "static", "max_age_ms": 60000, "apply_to_manual": False}
        policy_error = False
        try:
            if self.execution_policy_provider is not None:
                policy = execution_policy(self.execution_policy_provider())
        except Exception:
            policy_error = True
        active = self.rules_provider is not None and (
            input_source == "database_committed" or policy["apply_to_manual"] or policy_error
        )
        health = {
            "state": "synthetic",
            "mode": "synthetic",
            "reason": "synthetic_constraints",
            "input_source": input_source,
            "snapshot_hash": None,
            "observed_at_ms": None,
            "decision_at_ms": None,
            "unchecked": [],
            "protection_blocked": False,
        }
        setup = {
            "active": active,
            "policy": policy,
            "snapshot": None,
            "current_ready": False,
            "policy_valid": not policy_error,
            "constraints": ExecutionConstraints(can_short=candle.instrument.market == Market.USDM),
            "sizing_known": False,
            "health": health,
            "now": None,
        }
        if not active:
            row["execution_health"] = health
            return setup
        health.update(mode=policy["mode"], state="blocked", reason="rules_missing")
        try:
            now = self.execution_clock_ms()
            if type(now) is not int or not 0 <= now <= MAX_TIME:
                raise ValidationError("invalid actual execution clock")
            if now < candle.close_time_ms:
                raise ValidationError("execution clock precedes the supplied observation")
            setup["now"] = now
            health["decision_at_ms"] = now
        except Exception:
            health["reason"] = "execution_clock_unavailable"
            health["protection_blocked"] = decimal(row["quantity"]) != 0
            if health["protection_blocked"]:
                health["state"] = "protection_unavailable"
            row["execution_health"] = health
            return setup
        current = None
        try:
            provided = self.rules_provider(candle.instrument.key, max_age_ms=policy["max_age_ms"])
            if provided is not None:
                if not isinstance(provided, ExecutionRulesSnapshot):
                    raise ValidationError("invalid execution rules provider result")
                current = execution_snapshot(provided.document(), candle.instrument)
                if current.observed_at_ms > now:
                    current = None
                    health["reason"] = "rules_future"
        except Exception:
            current = None
            health["reason"] = "rules_provider_unavailable"
        known = []
        for document in (
            row.get("execution_rules_snapshot"),
            (row["pending"] or {}).get("execution", {}).get("snapshot"),
        ):
            if document is not None:
                candidate = execution_snapshot(document, candle.instrument)
                if candidate.observed_at_ms <= now:
                    known.append(candidate)
        if current is not None:
            if not known or current.observed_at_ms >= max(item.observed_at_ms for item in known):
                row["execution_rules_snapshot"] = current.document()
            selected = current
        else:
            selected = max(known, key=lambda item: item.observed_at_ms) if known else None
        setup["snapshot"] = selected
        ready = (
            not policy_error
            and current is not None
            and 0 <= now - current.observed_at_ms <= policy["max_age_ms"]
            and current.status == "TRADING"
            and (policy["mode"] != "strict" or not current.unsupported_filters)
        )
        if policy_error:
            health["reason"] = "invalid_execution_policy"
        elif current is not None:
            health["reason"] = (
                "rules_stale"
                if now - current.observed_at_ms > policy["max_age_ms"]
                else "rules_status"
                if current.status != "TRADING"
                else "unsupported_filters"
                if not ready
                else "rules_ready"
            )
        if selected is not None:
            health.update(
                snapshot_hash=selected.content_hash,
                observed_at_ms=selected.observed_at_ms,
                unchecked=["unsupported_filter:" + name for name in selected.unsupported_filters],
            )
            try:
                setup["constraints"] = sizing_constraints(selected)
                setup["sizing_known"] = True
            except ValidationError:
                ready = False
                health["reason"] = "unsupported_sizing_grid"
        setup["current_ready"] = ready
        health["protection_blocked"] = bool(
            decimal(row["quantity"])
            and (
                policy_error
                or not setup["sizing_known"]
                or selected is None
                or not selected.reductions_allowed
            )
        )
        health["state"] = (
            "protection_unavailable"
            if health["protection_blocked"]
            else "blocked"
            if not ready
            else "unchecked"
            if health["unchecked"]
            else "ready"
        )
        row["execution_health"] = health
        return setup

    def _execution_pin(self, row, candle, setup, proposal, *, retrospective=None):
        health = setup["health"]
        if (
            not setup["policy_valid"]
            or not setup["sizing_known"]
            or setup["snapshot"] is None
            or setup["now"] is None
        ):
            raise TradingBlocked(health["reason"])
        if not proposal.reducing and not setup["current_ready"]:
            raise TradingBlocked(health["reason"])
        policy, rules = setup["policy"], setup["snapshot"]
        result = validate_execution_order(
            candle.instrument,
            proposal,
            rules,
            now_ms=setup["now"],
            max_age_ms=policy["max_age_ms"],
            strict=policy["mode"] == "strict",
            retrospective=(candle.close_time_ms < rules.observed_at_ms)
            if retrospective is None
            else retrospective,
        )
        unchecked = list(result.unchecked)
        if not setup["current_ready"]:
            unchecked.append("current_execution_rules_unavailable")
        unchecked = sorted(set(unchecked))
        health["unchecked"] = unchecked
        if health["state"] == "ready" and unchecked:
            health["state"] = "unchecked"
        return {
            "version": 1,
            "snapshot": rules.document(),
            "decision_at_ms": setup["now"],
            "policy": dict(policy),
            "unchecked": unchecked,
            "input_source": health["input_source"],
            "retrospective": result.retrospective,
        }

    @staticmethod
    def _compact_execution(pin):
        snapshot = pin["snapshot"]
        return {key: deepcopy(value) for key, value in pin.items() if key != "snapshot"} | {
            "snapshot_hash": snapshot["content_hash"],
            "snapshot_observed_at_ms": snapshot["observed_at_ms"],
            "snapshot_source": snapshot["source"],
        }

    def detail(self, instance):
        with self.lock:
            if instance not in self.data["strategies"]:
                raise ValidationError("strategy not found")
            row = self.data["strategies"][instance]
            return {
                "instance": instance,
                **deepcopy(
                    {key: value for key, value in row.items() if key != "runtime_checkpoint"}
                ),
                **self._model_history_status(instance),
            }

    def _model_history_status(self, instance):
        engine = self.engines.get(instance)
        value = getattr(engine.module, "history_priming_ready", None) if engine else None
        ready = value if type(value) is bool else None
        return {
            "model_history_ready": ready,
            "model_history_warning": (
                "model_history_unready; model-based resizing/reversal wait; "
                "configured protection is independent"
                if ready is False
                else None
            ),
        }

    def command(self, kind, payload):
        if not isinstance(payload, dict):
            raise ValidationError("paper command payload must be an object")
        with self.lock:
            if kind == "pause":
                # Emergency entry pause never executes/copies a model. A prior
                # uncertain commit must not be overwritten with stale finance.
                if self._owner_retired:
                    raise ValidationError("paper owner is retired; use the current core")
                if (
                    self._shutdown_uncertain
                    or self._recording_manual_resume_required
                    or not self._recording_available
                ):
                    self.data["paused"] = True
                    return {"paused": True, "durable": False}
                candidate = deepcopy(self.data)
                candidate["paused"] = True
                candidate["feed"]["revision"] += 1
                try:
                    self._persist_financial_state(candidate)
                except Exception:
                    self.data["paused"] = True
                    raise
                return {"paused": True}
            self._require_owner()
            previous = self.data, self.engines, self.features
            self.data, self.engines, self.features = (
                deepcopy(self.data),
                deepcopy(self.engines),
                deepcopy(self.features),
            )
            try:
                result = self._command(kind, payload)
                if not result.get("duplicate"):
                    self.data["feed"]["revision"] += 1
                self._persist_state()
                if kind == "resume":
                    self._recording_manual_resume_required = False
                    self._recording_incident_announced = False
            except Exception:
                self.data, self.engines, self.features = previous
                self.data["paused"] = True
                self.data["feed"].update(ready=False, reason="paper_error")
                raise
            return result

    def _build(self, name, *, restore=False):
        row = self.data["strategies"][name]
        instrument = parse_instrument(row["instrument"])
        module = self.factory(row["module_id"], instrument, row["configuration"])
        self.features[name] = self.feature_factory(
            instrument, module.descriptor.required_fields, row["analysis"]
        )
        self.engines[name] = TradingEngine(module)
        if restore and "runtime_checkpoint" in row:
            checkpoint = row["runtime_checkpoint"]
            if checkpoint.get("version") != 1 or set(checkpoint) != {
                "version",
                "module",
                "features",
                "last_candle",
            }:
                raise ValidationError("invalid strategy runtime checkpoint")
            if checkpoint["last_candle"] != row["last_candle"]:
                raise ValidationError("strategy checkpoint does not match durable cursor")
            history = self.data["candles"].get(instrument.key, [])
            stateless = (
                not module.descriptor.stateful
                and not module.descriptor.required_fields
                and getattr(getattr(self.features[name], "descriptor", None), "stateful", True)
                is False
            )
            if (
                not stateless
                and row["last_candle"] is not None
                and (not history or history[-1]["open_time_ms"] != row["last_candle"])
            ):
                raise ValidationError(
                    "checkpoint is behind portfolio history; explicit model reset/replay required"
                )
            if not callable(getattr(module, "restore", None)) or not callable(
                getattr(self.features[name], "restored", None)
            ):
                raise ValidationError("module does not support exact checkpoint recovery")
            module.restore(checkpoint["module"])
            self.features[name] = self.features[name].restored(checkpoint["features"], instrument)
            if row.get("analysis_input") and not row.get("analysis_input_health", {}).get(
                "manual_recovery_required", False
            ):
                self._validate_input_checkpoint(self.features[name], row["analysis_input"])
            if stateless and history:
                row["last_candle"] = history[-1]["open_time_ms"]
            cursor = getattr(self.features[name], "last_open_time_ms", row["last_candle"])
            if row["last_candle"] is not None and cursor != row["last_candle"]:
                raise ValidationError("analysis checkpoint does not match strategy cursor")
            if callable(getattr(module, "prime_history", None)):
                model_cursor = getattr(module, "last_open_time_ms", None)
                if model_cursor is not None and model_cursor != cursor:
                    raise ValidationError("model checkpoint does not match analysis cursor")
        # Reject plugins carrying sockets/locks or other non-calculation resources
        # before they can make ordinary transactions uncopyable.
        deepcopy(self.engines[name])
        deepcopy(self.features[name])
        row["status"] = "ready"

    def _command(self, kind, p):
        if kind == "paper.feed.continue":
            if p:
                raise CommandRejected("continue preserves the existing warmup plan; no parameters")
            plans = self.data["feed"].get("warmup_plans", {})
            if not plans:
                raise CommandRejected("no saved long warmup plan to continue")
            if self._long_preparing():
                self._require_flat_warmup()
            if any(row["status"] != "ready" for row in self.data["strategies"].values()):
                raise CommandRejected("recover strategies before continuing warmup")
            self.data["feed"].update(enabled=True, ready=False, reason="warmup_continued")
            return self.feed_snapshot()
        if kind in {"pause", "resume"}:
            if kind == "resume" and self._long_preparing():
                raise CommandRejected("long warmup must finish before entry resume")
            self.data["paused"] = kind == "pause"
            return {"paused": self.data["paused"]}
        if kind == "paper.account.create":
            name, balance = identity(p.get("account_id")), decimal(p.get("balance", "10000"))
            fee = decimal(p.get("fee_rate", "0.001"))
            if name in self.data["accounts"] or len(self.data["accounts"]) >= 10:
                raise ValidationError("duplicate account or ten-account limit")
            if balance <= 0 or not 0 <= fee < 1:
                raise ValidationError("invalid paper balance or fee")
            self.data["accounts"][name] = {
                "balance": str(balance),
                "fee_rate": str(fee),
                "market": None,
            }
            return {"account_id": name}
        if kind == "paper.strategy.add":
            if set(p) - {
                "instance",
                "account_id",
                "instrument",
                "budget",
                "module_id",
                "configuration",
                "analysis",
                "analysis_snapshot",
            }:
                raise ValidationError(
                    "unsupported paper strategy parameter; "
                    "paper v1 is leverage 1 without margin modes"
                )
            if self.data["feed"]["enabled"] and self.data["feed"]["warmup_minutes"] > SHORT_WARMUP:
                self._require_flat_warmup()
            name, account = identity(p.get("instance")), identity(p.get("account_id"))
            instrument, budget = (
                parse_instrument(p.get("instrument")),
                decimal(p.get("budget", "1000")),
            )
            require_paper_settlement(instrument)
            if (
                name in self.data["strategies"]
                or len(self.data["strategies"]) >= 100
                or account not in self.data["accounts"]
                or budget <= 0
            ):
                raise ValidationError("invalid account, budget, duplicate or strategy limit")
            if any(
                s["account_id"] == account and s["instrument"] == instrument.key
                for s in self.data["strategies"].values()
            ):
                raise ValidationError("one paper strategy owner per account/instrument")
            if self.data["accounts"][account]["market"] not in {None, instrument.market.value}:
                raise ValidationError("a paper account cannot mix spot cash and USDM collateral")
            self.data["accounts"][account]["market"] = instrument.market.value
            committed = sum(
                decimal(s["budget"])
                for s in self.data["strategies"].values()
                if s["account_id"] == account
            )
            if committed + budget > decimal(self.data["accounts"][account]["balance"]):
                raise ValidationError("allocated strategy budgets exceed account balance")
            configuration, analysis = p.get("configuration", {}), p.get("analysis", {})
            if not isinstance(configuration, dict) or not isinstance(analysis, dict):
                raise ValidationError("strategy and analysis configurations must be objects")
            self.data["strategies"][name] = {
                "account_id": account,
                "instrument": instrument.key,
                "module_id": p.get("module_id"),
                "configuration": deepcopy(configuration),
                "analysis": deepcopy(analysis),
                "budget": str(budget),
                "quantity": "0",
                "entry_price": "0",
                "revision": 0,
                "last_changed_ms": 0,
                "pending": None,
                "last_candle": None,
                "status": "ready",
                "fills": [],
            }
            if "analysis_snapshot" in p:
                value = p["analysis_snapshot"]
                if (
                    not isinstance(value, dict)
                    or len(json.dumps(value, allow_nan=False).encode("utf-8")) > 65536
                ):
                    raise ValidationError("analysis snapshot must be a bounded finite JSON object")
                self.data["strategies"][name]["analysis_snapshot"] = deepcopy(value)
            self._build(name)
            if self.data["feed"]["enabled"] and self.data["feed"]["warmup_minutes"] > SHORT_WARMUP:
                self._create_warmup_plan(name)
            return {"instance": name}
        if kind == "paper.strategy.resume":
            name = identity(p.get("instance"))
            if name not in self.engines or self.data["strategies"][name]["status"] != "ready":
                raise ValidationError("recover this strategy before resetting its local stop latch")
            self.engines[name].module.resume()
            self.engines[name]._last_key = None
            return {
                "instance": name,
                "local_stop_reset": True,
                "entries_paused": self.data["paused"],
            }
        if kind in {"paper.strategy.remove", "paper.strategy.recover"}:
            name = identity(p.get("instance"))
            row = self.data["strategies"].get(name)
            if row is None:
                raise ValidationError("strategy not found")
            if kind.endswith("remove"):
                if decimal(row["quantity"]) or row["pending"]:
                    raise ValidationError("cannot remove an open or pending strategy")
                del self.data["strategies"][name]
                self.data["feed"]["warmed"].pop(name, None)
                self.data["feed"].get("warmup_plans", {}).pop(name, None)
                self.engines.pop(name, None)
                self.features.pop(name, None)
            else:
                if row["pending"]:
                    raise ValidationError("settle or explicitly cancel pending paper order first")
                reset = p.get("reset_model", False)
                if type(reset) is not bool:
                    raise ValidationError("reset_model must be boolean")
                self.data["paused"] = True
                self.data["feed"].update(ready=False, reason="manual_strategy_recovery")
                exact = "runtime_checkpoint" in row and not reset
                if not exact and self.data["feed"]["warmup_minutes"] > SHORT_WARMUP:
                    self._require_flat_warmup()
                self._build(name, restore=exact)
                self.data["feed"]["warmed"].pop(name, None)
                if exact:
                    plan = self.data["feed"].get("warmup_plans", {}).get(name)
                    if plan is None or plan["state"] == "complete":
                        if (
                            plan is not None
                            and plan["version"] == 1
                            and self._priming_mode(self.engines[name].module) == "model"
                        ):
                            raise ValidationError(
                                "old completed plan has no model history; apply a new plan"
                            )
                        if (
                            plan is not None
                            and plan.get("priming_mode") == "model"
                            and not self.engines[name].module.history_priming_ready
                        ):
                            raise ValidationError(
                                "completed model history lacks confirmed anchors; apply a new plan"
                            )
                        self.data["feed"]["warmed"][name] = True
                    else:
                        self._staged_feature(name, plan)  # Validate without replacing active state.
                        self._staged_module(name, plan)
                else:
                    row.pop("runtime_checkpoint", None)
                    row["last_candle"] = None
                    if self.data["feed"]["warmup_minutes"] > SHORT_WARMUP:
                        self._create_warmup_plan(name)
                return {
                    "instance": name,
                    "restored_checkpoint": exact,
                    "warning": "manual entry resume remains required"
                    if exact
                    else "model reset; analysis warmup starts again",
                }
            return {"instance": name}
        if kind == "paper.order.cancel":
            row = self.data["strategies"].get(identity(p.get("instance")))
            if row is None:
                raise ValidationError("strategy not found")
            row["pending"] = None
            return {"canceled": True}
        if kind == "paper.step":
            if self.data["feed"]["enabled"]:
                raise ValidationError(
                    "manual candle input is disabled while database feed owns supply"
                )
            candle = parse_candle(parse_instrument(p.get("instrument")), p.get("candle"))
            return self._step(candle)
        raise ValidationError("unsupported paper command")

    @staticmethod
    def _empty_feed():
        return {
            "enabled": False,
            "ready": False,
            "reason": "disabled",
            "revision": 0,
            "start_ms": 0,
            "warmup_minutes": 240,
            "warmed": {},
            "provenance": {},
            "warmup_plans": {},
        }

    def _public_feed(self):
        result = {
            key: deepcopy(value)
            for key, value in self.data["feed"].items()
            if key != "warmup_plans"
        }
        result["warmup_plans"] = {
            name: self._warmup_summary(plan)
            for name, plan in self.data["feed"].get("warmup_plans", {}).items()
        }
        result["long_warmup_preparing"] = self._long_preparing()
        return result

    @staticmethod
    def _warmup_summary(plan):
        return {**summary(plan), "priming_mode": plan.get("priming_mode", "features_only")}

    @staticmethod
    def _priming_mode(module):
        eligibility = getattr(module, "can_prime_history", None)
        prime = getattr(module, "prime_history", None)
        if eligibility is None and prime is None:
            return "features_only"
        if not callable(eligibility) or not callable(prime):
            raise ValidationError("incomplete TS history preparation contract")
        fresh = eligibility()
        if type(fresh) is not bool:
            raise ValidationError("TS history preparation eligibility must be boolean")
        if not fresh:
            return "preserve"
        if not all(
            callable(getattr(module, key, None)) for key in ("checkpoint", "restore")
        ) or not hasattr(module, "last_open_time_ms"):
            raise ValidationError("TS history preparation requires checkpoint and cursor support")
        return "model"

    @staticmethod
    def _priming_ready(module, mode, cursor):
        if mode == "model":
            if getattr(module, "last_open_time_ms", None) != cursor:
                raise ValidationError("model history checkpoint does not match analysis cursor")
            value = getattr(module, "history_priming_ready", None)
            if type(value) is not bool:
                raise ValidationError("TS history preparation readiness must be boolean")
            return value
        if mode == "preserve":
            previous = getattr(module, "last_open_time_ms", None)
            if previous is not None and previous != cursor:
                raise ValidationError(
                    "preserved model cursor changed; explicit reset/replay required"
                )
        return True

    def _long_preparing(self):
        return any(
            plan["state"] != "complete"
            for plan in self.data["feed"].get("warmup_plans", {}).values()
        )

    def _require_flat_warmup(self):
        if not self.data["paused"] or any(
            decimal(row["quantity"]) != 0 or row["pending"] is not None
            for row in self.data["strategies"].values()
        ):
            raise CommandRejected(
                "long warmup requires global entry pause, all positions flat and no pending orders"
            )

    def _warmup_fingerprint(self, name):
        row = self.data["strategies"][name]
        payload = {
            key: row.get(key)
            for key in ("instrument", "module_id", "configuration", "analysis", "analysis_snapshot")
        }
        payload["required"] = sorted(self.engines[name].module.descriptor.required_fields)
        payload["module_version"] = self.engines[name].module.descriptor.api_version
        payload["feature_version"] = getattr(
            getattr(self.features[name], "descriptor", None), "api_version", None
        )
        return sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()

    def _create_warmup_plan(self, name):
        row = self.data["strategies"][name]
        if row["status"] != "ready":
            raise ValidationError("recover each strategy before starting long warmup")
        required = self.engines[name].module.descriptor.required_fields
        history = self.data["candles"].get(row["instrument"], [])
        end = history[-1]["open_time_ms"] + MINUTE if history else self.data["feed"]["start_ms"]
        feature = self.feature_factory(
            parse_instrument(row["instrument"]), required, row["analysis"]
        )
        alignment = getattr(feature, "warmup_alignment_minutes", 1)
        start = end
        if required:
            start, end = aligned_window(end, self.data["feed"]["warmup_minutes"], alignment)
        if not callable(getattr(feature, "checkpoint", None)) or not callable(
            getattr(feature, "restored", None)
        ):
            raise ValidationError("long warmup requires exact feature checkpoint support")
        if required and not hasattr(feature, "last_open_time_ms"):
            raise ValidationError("long warmup requires a public feature cursor")
        module = self.engines[name].module
        mode = self._priming_mode(module)
        if mode == "model" and not required:
            raise ValidationError("historical model preparation requires declared analysis inputs")
        self.data["feed"].setdefault("warmup_plans", {})[name] = {
            "version": 2,
            "plan_id": uuid4().hex,
            "instrument": row["instrument"],
            "fingerprint": self._warmup_fingerprint(name),
            "alignment_minutes": alignment,
            "start_ms": start,
            "target_end_ms": end,
            "next_open_ms": start,
            "state": "warming" if required else "complete",
            "checkpoint": feature.checkpoint() if required else None,
            "priming_mode": mode,
            "module_checkpoint": module.checkpoint() if mode == "model" and required else None,
            "last_candle": None,
        }
        self.data["feed"]["warmed"].pop(name, None)
        if not required:
            self.data["feed"]["warmed"][name] = True
            row["history_preparation"] = mode

    def _staged_feature(self, name, plan):
        if plan["fingerprint"] != self._warmup_fingerprint(name):
            raise ValidationError("long warmup configuration changed; apply a new plan")
        row = self.data["strategies"][name]
        instrument = parse_instrument(row["instrument"])
        feature = self.feature_factory(
            instrument, self.engines[name].module.descriptor.required_fields, row["analysis"]
        )
        if getattr(feature, "warmup_alignment_minutes", 1) != plan["alignment_minutes"]:
            raise ValidationError("staged warmup alignment changed")
        restored = feature.restored(plan["checkpoint"], instrument)
        if plan["last_candle"] is not None and plan.get("analysis_input"):
            self._validate_input_checkpoint(restored, plan["analysis_input"])
        expected_cursor = plan["next_open_ms"] - MINUTE if plan["last_candle"] else None
        if getattr(restored, "last_open_time_ms", None) != expected_cursor:
            raise ValidationError("staged analysis checkpoint does not match warmup cursor")
        return restored

    def _staged_module(self, name, plan):
        active = self.engines[name].module
        if plan["version"] == 1:
            mode = self._priming_mode(active)
            if mode == "model":
                raise ValidationError(
                    "saved feature-only plan has no model history; apply a new plan"
                )
            return None, mode
        mode = plan["priming_mode"]
        if mode != "model":
            if self._priming_mode(active) == "model":
                raise ValidationError("saved plan omitted required model priming; apply a new plan")
            return None, mode
        if self._priming_mode(active) != "model":
            raise ValidationError(
                "active model changed during staged preparation; apply a new plan"
            )
        row = self.data["strategies"][name]
        module = self.factory(
            row["module_id"], parse_instrument(row["instrument"]), row["configuration"]
        )
        if not callable(getattr(module, "prime_history", None)) or not callable(
            getattr(module, "restore", None)
        ):
            raise ValidationError("installed TS cannot restore historical preparation")
        module.restore(plan["module_checkpoint"])
        cursor = plan["next_open_ms"] - MINUTE if plan["last_candle"] else None
        if getattr(module, "last_open_time_ms", None) != cursor:
            raise ValidationError("staged model checkpoint does not match warmup cursor")
        return module, mode

    def feed_snapshot(self):
        """The portfolio's durable candle tails are the only delivery cursors."""
        with self.lock:
            feed = self.data["feed"]
            instruments = {}
            warmup = {}
            analysis_inputs = {}
            live_strategies = []
            frontier = max(
                (rows[-1]["open_time_ms"] for rows in self.data["candles"].values() if rows),
                default=None,
            )
            any_live = bool(feed["warmed"]) or any(
                decimal(row["quantity"]) or row["pending"] is not None
                for row in self.data["strategies"].values()
            )
            recovery = False
            quarantined = False
            for name, row in self.data["strategies"].items():
                key = row["instrument"]
                history = self.data["candles"].get(key, [])
                feature = self.features.get(name)
                warmed = name in feed["warmed"]
                protecting = bool(decimal(row["quantity"])) or row["pending"] is not None
                cold = row["status"] == "ready" and not warmed and not protecting
                cursor = getattr(feature, "last_open_time_ms", None) if warmed else None
                next_ms = (
                    history[-1]["open_time_ms"] + MINUTE
                    if history
                    else cursor + MINUTE
                    if cursor is not None
                    else frontier + MINUTE
                    if frontier is not None and any_live
                    else feed["start_ms"]
                )
                if not cold or not any_live:
                    anchor = getattr(feature, "last_candle", None) if warmed else None
                    anchor = candle_row(anchor) if isinstance(anchor, Candle) else None
                    instruments[key] = {
                        "next_open_ms": next_ms,
                        "last_candle": deepcopy(history[-1]) if history else anchor,
                    }
                if warmed or protecting:
                    live_strategies.append(name)
                recovery |= row["status"] == "recovery_required" and not is_shutdown_row(row)
                quarantined |= row["status"] == "quarantined"
                if name not in feed["warmed"] and row["status"] == "ready":
                    required = self.engines[name].module.descriptor.required_fields
                    warmup[name] = {
                        "instrument": key,
                        "end_ms": next_ms,
                        "minutes": feed["warmup_minutes"] if required else 0,
                    }
                    stage = feed.get("warmup_plans", {}).get(name)
                    if stage is not None:
                        warmup[name].update(
                            self._warmup_summary(stage),
                            staged=True,
                            last_candle=deepcopy(stage["last_candle"]),
                        )
                channels = getattr(getattr(feature, "descriptor", None), "required_fields", ())
                channels = sorted(set(channels) - {"OHLCV"})
                if channels and row["status"] == "ready":
                    stage = feed.get("warmup_plans", {}).get(name)
                    preparing = name in warmup
                    reset = preparing and (
                        stage is None or stage["next_open_ms"] == stage["start_ms"]
                    )
                    input_state = stage if preparing and stage is not None else row
                    analysis_inputs[name] = {
                        "instrument": key,
                        "channels": channels,
                        "maximum_metric_age_ms": min(
                            (
                                value["maximum_metric_age_ms"]
                                for value in row["analysis"].get("market_sentiment", [])
                            ),
                            default=330000,
                        ),
                        "binding": sha256(
                            (
                                name
                                + ":"
                                + row["account_id"]
                                + ":"
                                + self._warmup_fingerprint(name)
                            ).encode()
                        ).hexdigest(),
                        "receipt": None if reset else deepcopy(input_state.get("analysis_input")),
                        "reset": reset,
                        "live_receipt": deepcopy(row.get("analysis_input")),
                        "live_manual_recovery_required": row.get("analysis_input_health", {}).get(
                            "manual_recovery_required", False
                        ),
                        "manual_recovery_required": False
                        if reset
                        else input_state.get("analysis_input_health", {}).get(
                            "manual_recovery_required", False
                        ),
                    }
            return {
                "epoch": self.feed_epoch,
                "revision": feed["revision"],
                "enabled": feed["enabled"],
                "ready": feed["ready"],
                "reason": feed["reason"],
                "instruments": instruments,
                "frontier_ms": frontier,
                "recovery_required": recovery,
                "quarantined": quarantined,
                "warmup": warmup,
                "analysis_inputs": analysis_inputs,
                "live_strategies": sorted(live_strategies),
                "protection_required": any(
                    decimal(row["quantity"]) or row["pending"] is not None
                    for row in self.data["strategies"].values()
                ),
                "paused": self.data["paused"],
                "warmup_plans": self._public_feed()["warmup_plans"],
                "long_warmup_preparing": self._long_preparing(),
            }

    def feed_configure(self, enabled, *, start_ms, warmup_minutes=240):
        if (
            type(enabled) is not bool
            or type(start_ms) is not int
            or start_ms < 0
            or start_ms % 60000
            or type(warmup_minutes) is not int
            or not 1 <= warmup_minutes <= MAX_WARMUP
        ):
            raise ValidationError(
                "feed requires aligned start and 1..525600 explicit warmup minutes"
            )
        with self.lock:
            self._require_owner()
            previous = deepcopy(self.data)
            feed = self.data["feed"]
            if (
                not enabled
                and feed.get("warmup_plans")
                and warmup_minutes == feed["warmup_minutes"]
            ):
                feed.update(
                    enabled=False,
                    ready=False,
                    reason="warmup_paused",
                    revision=feed["revision"] + 1,
                )
                try:
                    self._persist_state()
                except Exception:
                    self.data = previous
                    self.data["paused"] = True
                    raise
                return self.feed_snapshot()
            if enabled and warmup_minutes > SHORT_WARMUP:
                self._require_flat_warmup()
            feed.update(
                enabled=enabled,
                ready=False,
                reason="starting" if enabled else "disabled",
                start_ms=start_ms,
                warmup_minutes=warmup_minutes,
                warmed={},
                warmup_plans={},
                revision=feed["revision"] + 1,
            )
            try:
                if enabled and warmup_minutes > SHORT_WARMUP:
                    for name in sorted(self.data["strategies"]):
                        self._create_warmup_plan(name)
                self._persist_state()
            except Exception:
                self.data = previous
                self.data["paused"] = True
                raise
            return self.feed_snapshot()

    def data_gate(self, ready, reason):
        """Data recovery changes this gate, never the user's pause decision."""
        if type(ready) is not bool or not isinstance(reason, str) or not 1 <= len(reason) <= 128:
            raise ValidationError("invalid feed data gate")
        with self.lock:
            self._require_owner()
            feed = self.data["feed"]
            if (feed["ready"], feed["reason"]) == (ready, reason):
                return
            feed.update(ready=ready, reason=reason, revision=feed["revision"] + 1)
            try:
                self._persist_state()
            except Exception:
                feed.update(ready=False, reason="recording_unavailable")
                self.data["paused"] = True
                raise

    def _feed_transaction(self, expected, action):
        with self.lock:
            self._require_owner()
            if not self.data["feed"]["enabled"] or expected != self.feed_snapshot():
                raise ValidationError("feed plan changed; obtain a fresh snapshot")
            previous = self.data, self.engines, self.features
            try:
                self.data, self.engines, self.features = (
                    deepcopy(self.data),
                    deepcopy(self.engines),
                    deepcopy(self.features),
                )
                result = action()
                if not result.get("duplicate"):
                    self.data["feed"]["revision"] += 1
                    self._persist_state()
                return result
            except Exception:
                self.data, self.engines, self.features = previous
                self.data["feed"].update(ready=False, reason="feed_apply_failed")
                if self._recording_manual_resume_required:
                    self.data["paused"] = True
                raise

    def _persist_state(self):
        try:
            for name, engine in self.engines.items():
                row = self.data["strategies"][name]
                feature = self.features[name]
                if (
                    row["status"] == "ready"
                    and callable(getattr(engine.module, "checkpoint", None))
                    and callable(getattr(feature, "checkpoint", None))
                ):
                    row["runtime_checkpoint"] = {
                        "version": 1,
                        "last_candle": row["last_candle"],
                        "module": engine.module.checkpoint(),
                        "features": feature.checkpoint(),
                    }
            self.persist(deepcopy(self.data))
        except Exception:
            self._recording_available = False
            self._recording_failure_revision += 1
            if not self._recording_manual_resume_required:
                self._recording_incident_announced = False
            self._recording_manual_resume_required = True
            raise
        self._recording_available = True

    def recording_health(self):
        """IO health is separate from acknowledgement of a prior uncertain save."""
        with self.lock:
            return {
                "available": self._recording_available,
                "failure_revision": self._recording_failure_revision,
                "manual_resume_required": self._recording_manual_resume_required,
                "incident_announced": self._recording_incident_announced,
                "owner_retired": self._owner_retired,
            }

    def mark_recording_incident_announced(self):
        """Notification handshake only; this never acknowledges financial recovery."""
        with self.lock:
            self._require_owner()
            if self._recording_manual_resume_required:
                self._recording_incident_announced = True

    def retire_feed_owner(self):
        """Permanently fence this in-memory owner before the core lock is released.

        An in-progress transaction finishes under the same lock first. A blocked
        DB read holds no paper lock and can return later without write authority.
        Retirement is deliberately volatile: the next core loads normal durable
        state into a new owner/epoch and still requires manual recovery.
        """
        with self.lock:
            self._owner_retired = True

    def _require_owner(self):
        if self._owner_retired:
            raise ValidationError("paper owner is retired; use the current core")
        if self._shutdown_uncertain:
            raise TradingBlocked("paper financial commit is unconfirmed; reload durable finance")

    def feed_warmup(self, history, *, expected, now_ms, analysis_inputs=None):
        """Prepare calculation candidates without orders or advancing the live candle cursor."""

        def action():
            if (
                expected["recovery_required"]
                or not history
                or set(history) - set(expected["warmup"])
            ):
                raise ValidationError("recover strategies before warming the exact active plan")
            if any(expected["warmup"][name].get("staged") for name in history):
                raise ValidationError("long warmup requires bounded staged chunks")
            for name in history:
                plan = expected["warmup"][name]
                row = self.data["strategies"][name]
                instrument = parse_instrument(row["instrument"])
                rows = tuple(history[name])
                if len(rows) != plan["minutes"]:
                    raise ValidationError("insufficient explicit warmup history")
                feature = self.feature_factory(
                    instrument,
                    self.engines[name].module.descriptor.required_fields,
                    row["analysis"],
                )
                module = deepcopy(self.engines[name].module)
                mode = self._priming_mode(module)
                output = {}
                start = plan["end_ms"] - len(rows) * 60000
                for index, candle in enumerate(rows):
                    if (
                        not isinstance(candle, Candle)
                        or candle.instrument != instrument
                        or candle.open_time_ms != start + index * 60000
                        or candle.close_time_ms > now_ms
                    ):
                        raise ValidationError("warmup requires contiguous closed observed minutes")
                    output = self._update_feature(
                        feature, candle, analysis_inputs, name, index, now_ms
                    )
                    if mode == "model" and module.prime_history(candle, dict(output)) is not None:
                        raise ValidationError("TS history preparation must not return trade orders")
                required = self.engines[name].module.descriptor.required_fields
                if any(field not in output or output[field] is None for field in required):
                    raise ValidationError(
                        "warmup history did not prepare all required analysis fields"
                    )
                cursor = rows[-1].open_time_ms if rows else None
                if not self._priming_ready(module, mode, cursor):
                    raise ValidationError(
                        "warmup did not prepare the model's confirmed interval anchors"
                    )
                self.features[name] = feature
                if mode == "model":
                    self.engines[name] = TradingEngine(module)
                row["history_preparation"] = mode
                self.data["feed"]["warmed"][name] = True
                self._save_input(name, analysis_inputs, output=output, reset=True)
            return {"warmed": len(history), "traded": False}

        return self._feed_transaction(expected, action)

    def feed_warmup_chunk(self, name, rows, *, expected, now_ms, analysis_inputs=None):
        """Commit one bounded staged graph and cursor, with zero trading effects."""
        if (
            not isinstance(rows, (tuple, list))
            or len(rows) > CHUNK_MINUTES
            or type(now_ms) is not int
            or now_ms < 0
        ):
            raise ValidationError("invalid bounded warmup chunk")

        def action():
            self._require_flat_warmup()
            if expected["recovery_required"] or name not in expected["warmup"]:
                raise ValidationError("recover the exact staged warmup plan first")
            plan = self.data["feed"].get("warmup_plans", {}).get(name)
            if plan is None or plan["state"] != "warming":
                raise ValidationError("no unfinished staged warmup chunk")
            low = plan["next_open_ms"]
            high = min(plan["target_end_ms"], low + CHUNK_MINUTES * MINUTE)
            candles = tuple(rows)
            if low >= high or len(candles) != (high - low) // MINUTE:
                raise ValidationError("missing or oversized staged warmup chunk")
            feature = self._staged_feature(name, plan)
            module, mode = self._staged_module(name, plan)
            instrument = parse_instrument(plan["instrument"])
            output = {}
            for index, candle in enumerate(candles):
                if (
                    not isinstance(candle, Candle)
                    or candle.instrument != instrument
                    or candle.open_time_ms != low + index * MINUTE
                    or candle.close_time_ms > now_ms
                ):
                    raise ValidationError(
                        "staged warmup requires contiguous closed observed minutes"
                    )
                output = self._update_feature(feature, candle, analysis_inputs, name, index, now_ms)
                if mode == "model" and module.prime_history(candle, dict(output)) is not None:
                    raise ValidationError("TS history preparation must not return trade orders")
            # Recheck the portfolio invariant at every commit/finalization,
            # including quarantined/recovery rows and already submitted orders.
            self._require_flat_warmup()
            plan.update(
                version=2,
                next_open_ms=high,
                checkpoint=feature.checkpoint(),
                priming_mode=mode,
                module_checkpoint=module.checkpoint() if mode == "model" else None,
                last_candle=candle_row(candles[-1]),
            )
            self._save_input(
                name,
                analysis_inputs,
                feature=feature,
                output=output,
                reset=expected.get("analysis_inputs", {}).get(name, {}).get("reset", False),
                target=plan,
            )
            if mode == "model" and getattr(module, "last_open_time_ms", None) != high - MINUTE:
                raise ValidationError("staged model checkpoint does not match analysis cursor")
            if high == plan["target_end_ms"]:
                if getattr(feature, "last_open_time_ms", None) != high - MINUTE:
                    raise ValidationError("staged warmup graph cursor mismatch")
                required = self.engines[name].module.descriptor.required_fields
                prepared = self._priming_ready(
                    module if mode == "model" else self.engines[name].module, mode, high - MINUTE
                )
                if (
                    any(field not in output or output[field] is None for field in required)
                    or not prepared
                ):
                    plan["state"] = "not_ready"
                else:
                    self.features[name] = feature
                    if mode == "model":
                        self.engines[name] = TradingEngine(module)
                    self.data["strategies"][name]["history_preparation"] = mode
                    self.data["feed"]["warmed"][name] = True
                    plan.update(state="complete", checkpoint=None, module_checkpoint=None)
                    for key in ("analysis_input", "analysis_input_health"):
                        if key in plan:
                            self.data["strategies"][name][key] = plan.pop(key)
            return {
                "warmed": int(plan["state"] == "complete"),
                "traded": False,
                "plan": self._warmup_summary(plan),
            }

        return self._feed_transaction(expected, action)

    def feed_apply(self, frame, *, expected, entries_allowed, reason, now_ms, analysis_inputs=None):
        if type(entries_allowed) is not bool or not isinstance(reason, str) or len(reason) > 128:
            raise ValidationError("invalid feed frame gate")
        frame = tuple(frame)

        def action():
            keys = expected["instruments"]
            if (
                expected["recovery_required"]
                or self._long_preparing()
                or (expected["warmup"] and not expected.get("protection_required"))
            ):
                raise ValidationError("manual recovery and analysis warmup required")
            if (
                len(frame) != len(keys)
                or not frame
                or {candle.instrument.key for candle in frame if isinstance(candle, Candle)}
                != set(keys)
            ):
                raise ValidationError("feed frame must contain every active instrument once")
            times = {candle.open_time_ms for candle in frame}
            if len(times) != 1 or any(candle.close_time_ms > now_ms for candle in frame):
                raise ValidationError("feed frame requires a common closed minute")
            if all(
                self.data["candles"].get(c.instrument.key, [])
                and candle_row(c) == self.data["candles"][c.instrument.key][-1]
                for c in frame
            ):
                return {"duplicate": True, "processed": 0}
            stamp = frame[0].open_time_ms
            if any(stamp != plan["next_open_ms"] for plan in keys.values()):
                raise ValidationError("feed frame skipped or conflicted with a durable cursor")
            feed = self.data["feed"]
            feed.update(
                ready=entries_allowed and not expected["warmup"],
                reason="warmup" if expected["warmup"] else reason,
            )
            provenance = {
                candle.instrument.key: (
                    "database_committed"
                    if not self.data["candles"].get(candle.instrument.key)
                    or feed["provenance"].get(candle.instrument.key) == "database_committed"
                    else "mixed_manual_and_database"
                )
                for candle in frame
            }
            pending_before = {
                name: deepcopy(row["pending"]) for name, row in self.data["strategies"].items()
            }
            by_instrument = {candle.instrument.key: candle for candle in frame}
            open_marks = {key: candle.open for key, candle in by_instrument.items()}
            close_marks = {key: candle.close for key, candle in by_instrument.items()}
            # Settle all previously submitted orders before any new close-time
            # proposal. The same opening marks are visible to every fill.
            # Reservations are deterministic: instrument key, then strategy ID.
            for _name, row in sorted(
                self.data["strategies"].items(), key=lambda item: (item[1]["instrument"], item[0])
            ):
                if row["instrument"] in by_instrument:
                    self._fill(row, by_instrument[row["instrument"]], marks=open_marks)
            results = [
                self._step(
                    candle,
                    settled=True,
                    marks=close_marks,
                    input_source="database_committed",
                    analysis_inputs=analysis_inputs,
                    as_of_ms=now_ms,
                )
                for candle in sorted(frame, key=lambda c: c.instrument.key)
            ]
            if any(row["status"] == "quarantined" for row in self.data["strategies"].values()):
                feed.update(ready=False, reason="quarantined_strategy")
                for name, row in self.data["strategies"].items():
                    order = row["pending"]
                    if order and not order["reducing"] and order != pending_before[name]:
                        row["pending"] = None  # Never submitted; prior pending orders stay owned.
            for candle in frame:
                feed["provenance"][candle.instrument.key] = provenance[candle.instrument.key]
            return {
                "processed": sum(row["processed"] for row in results),
                "open_time_ms": stamp,
                "entries_ready": feed["ready"],
                "reason": feed["reason"],
                "reservation_priority": "instrument_key_then_strategy_instance",
                "analysis_inputs_ready": all(
                    row.get("analysis_input_health", {}).get("ready", True)
                    for row in self.data["strategies"].values()
                    if row["status"] == "ready"
                ),
            }

        return self._feed_transaction(expected, action)

    def _available(self, account, *, exclude=None, mark_at_ms=None, overrides=None):
        wallet = decimal(self.data["accounts"][account]["balance"])
        committed = sum(
            abs(decimal(r["quantity"])) * decimal(r["entry_price"])
            for r in self.data["strategies"].values()
            if r["account_id"] == account
            and parse_instrument(r["instrument"]).market == Market.USDM
        )
        unrealized = Decimal(0)
        for row in self.data["strategies"].values():
            if (
                row["account_id"] != account
                or parse_instrument(row["instrument"]).market != Market.USDM
            ):
                continue
            prices = overrides or {}
            mark = prices.get(row["instrument"])
            if mark is None:
                history = self.data["candles"].get(row["instrument"], [])
                eligible = [
                    r
                    for r in history
                    if mark_at_ms is None or r["open_time_ms"] + 60000 <= mark_at_ms
                ]
                mark = decimal(eligible[-1]["close"]) if eligible else decimal(row["entry_price"])
            unrealized += decimal(row["quantity"]) * (mark - decimal(row["entry_price"]))
        reserved = sum(
            decimal(r["pending"]["reserved"])
            for r in self.data["strategies"].values()
            if r is not exclude and r["account_id"] == account and r["pending"]
        )
        return max(Decimal(0), min(wallet, wallet + unrealized) - committed - reserved)

    def _fill(self, row, candle, *, marks=None):
        order = row["pending"]
        if order is None or candle.open_time_ms < order["eligible_at_ms"]:
            return
        side = Side(order["side"])
        quote = resolve_bar_fill(
            OrderProposal(
                side,
                decimal(order["quantity"]),
                OrderKind(order["kind"]),
                decimal(order["price"]) if order["price"] is not None else None,
                order["reducing"],
                order["reason"],
            ),
            candle,
            order["eligible_at_ms"],
        )
        if quote is None:
            return
        price, fill_at_ms = quote.price, quote.event_time_ms
        quantity, old, entry = (
            decimal(order["quantity"]),
            decimal(row["quantity"]),
            decimal(row["entry_price"]),
        )
        signed = quantity if side == Side.BUY else -quantity
        account = self.data["accounts"][row["account_id"]]
        fee = quantity * price * decimal(account["fee_rate"])
        wallet = decimal(account["balance"])
        if order["reducing"]:
            if not old or old * signed >= 0 or quantity > abs(old):
                raise ValidationError("pending paper reduction no longer matches the position")
            realized = quantity * (price - entry) * (1 if old > 0 else -1)
            if candle.instrument.market == Market.SPOT:
                wallet += quantity * price - fee
            else:
                wallet += realized - fee
            new_entry = entry if old + signed else Decimal(0)
        else:
            if old * signed < 0:
                raise ValidationError("paper fill cannot reverse in one order")
            # A price gap can exhaust buying power. Reject rather than invent credit.
            required = quantity * price + fee
            available = self._available(
                row["account_id"],
                exclude=row,
                mark_at_ms=candle.open_time_ms,
                overrides=marks if marks is not None else {candle.instrument.key: candle.open},
            )
            if required > available or abs(old) * entry + quantity * price > decimal(row["budget"]):
                row["pending"] = None
                row["last_reason"] = "paper_order_rejected_insufficient_margin_after_gap"
                return
            new_entry = (abs(old) * entry + quantity * price) / (abs(old) + quantity)
            wallet -= fee + (quantity * price if candle.instrument.market == Market.SPOT else 0)
        row.update(
            quantity=str(old + signed),
            entry_price=str(new_entry),
            revision=row["revision"] + 1,
            last_changed_ms=fill_at_ms,
            pending=None,
        )
        # Market/open-marketable fills have a known open event. A later touched
        # limit retains conservative close timing so its pre-entry wick is unknown.
        account["balance"] = str(wallet)
        row["fills"].append(
            {
                "timestamp_ms": fill_at_ms,
                "side": side.value,
                "quantity": str(quantity),
                "price": str(price),
                "fee": str(fee),
                "reason": order["reason"],
                **(
                    {"execution": self._compact_execution(order["execution"])}
                    if "execution" in order
                    else {}
                ),
            }
        )
        row["fills"] = row["fills"][-1000:]

    @staticmethod
    def _validate_input_checkpoint(feature, receipt):
        frame = getattr(feature, "last_input_frame", None)
        if (
            frame is None
            or frame.candle.close_time_ms != receipt["end_ms"]
            or market_frame_checksum(frame) != receipt["frame_digest"]
        ):
            raise ValidationError(
                "paper analysis checkpoint differs from its selected input receipt"
            )

    @staticmethod
    def _update_feature(feature, candle, selected, name, index, as_of_ms):
        channels = set(getattr(getattr(feature, "descriptor", None), "required_fields", ())) - {
            "OHLCV"
        }
        batch = (selected or {}).get(name)
        if batch is not None:
            if (
                not isinstance(batch, PaperInputBatch)
                or index >= len(batch.frames)
                or batch.frames[index].candle != candle
                or batch.frames[index].as_of_ms > as_of_ms
                or not callable(getattr(feature, "update_frame", None))
            ):
                raise ValidationError("paper selected frame conflicts with the committed candle")
            return feature.update_frame(batch.frames[index])
        if channels and callable(getattr(feature, "update_frame", None)):
            # Absence is explicit; do not reuse the previous minute's auxiliary
            # values or stop the independently observed candle/protection clock.
            return feature.update_frame(MarketFeatureFrame(candle, as_of_ms))
        return feature.update(candle)

    def _save_input(self, name, selected, *, feature=None, output=None, reset=False, target=None):
        row = self.data["strategies"][name]
        target = row if target is None else target
        graph = feature or self.features[name]
        channels = set(getattr(getattr(graph, "descriptor", None), "required_fields", ())) - {
            "OHLCV"
        }
        if not channels:
            return True
        batch = (selected or {}).get(name)
        reason = batch.reason if batch is not None else "analysis_input_not_supplied"
        manual = (batch is None or batch.receipt is None) or (
            not reset
            and target.get("analysis_input_health", {}).get("manual_recovery_required", False)
        )
        if batch is not None and batch.receipt is not None:
            binding = sha256(
                (name + ":" + row["account_id"] + ":" + self._warmup_fingerprint(name)).encode()
            ).hexdigest()
            if batch.receipt["binding"] != binding:
                raise ValidationError("paper input receipt belongs to another strategy binding")
            self._validate_input_checkpoint(graph, batch.receipt)
            target["analysis_input"] = batch.receipt
        required = self.engines[name].module.descriptor.required_fields
        ready = (
            not manual
            and not reason
            and bool(getattr(graph, "input_ready", False))
            and (output is None or all(output.get(key) is not None for key in required))
        )
        target["analysis_input_health"] = {
            "ready": ready,
            "reason": reason or ("ready" if ready else "analysis_inputs_not_ready"),
            "manual_recovery_required": manual,
        }
        return ready

    def _step(
        self,
        candle,
        *,
        settled=False,
        marks=None,
        input_source="manual_paper",
        analysis_inputs=None,
        as_of_ms=None,
    ):
        if input_source not in {"manual_paper", "database_committed"}:
            raise ValidationError("paper input source must be explicit")
        if candle.instrument.key not in self.data["candles"] and len(self.data["candles"]) >= 1000:
            raise ValidationError("paper candle instrument capacity reached")
        previous = self.data["candles"].get(candle.instrument.key, [])
        encoded = candle_row(candle)
        if previous:
            last = previous[-1]
            if encoded == last:
                return {"duplicate": True, "processed": 0}
            if candle.open_time_ms != last["open_time_ms"] + 60000:
                raise ValidationError("paper replay requires contiguous immutable minutes")
        frontier = max(
            (rows[-1]["open_time_ms"] for rows in self.data["candles"].values() if rows),
            default=candle.open_time_ms,
        )
        if candle.open_time_ms < frontier:
            raise ValidationError(
                "portfolio candles must be supplied in global chronological order"
            )
        matched = [
            (n, r)
            for n, r in sorted(self.data["strategies"].items())
            if r["instrument"] == candle.instrument.key
        ]
        # Pending orders predate this candle; a manual pause never cancels them.
        if not settled:
            for _name, row in matched:
                self._fill(row, candle)
        for name, row in matched:
            if row["status"] != "ready":
                continue
            if (
                self.data["feed"]["enabled"]
                and name not in self.data["feed"]["warmed"]
                and not decimal(row["quantity"])
                and row["pending"] is None
            ):
                continue  # A cold, flat strategy joins only after exact nontrading preparation.
            graph = self.features[name]
            auxiliary = set(getattr(getattr(graph, "descriptor", None), "required_fields", ())) - {
                "OHLCV"
            }
            selected_at = candle.close_time_ms if as_of_ms is None else as_of_ms
            previous_frame = getattr(graph, "last_input_frame", None)
            if as_of_ms is None and previous_frame is not None:
                selected_at = max(selected_at, previous_frame.as_of_ms)
            if auxiliary:
                candidate = deepcopy(graph)
                try:
                    feature = self._update_feature(
                        candidate, candle, analysis_inputs, name, 0, selected_at
                    )
                    input_ready = self._save_input(
                        name, analysis_inputs, feature=candidate, output=feature
                    )
                except Exception:
                    # A malformed selection must not undo earlier confirmed
                    # pending fills or skip this candle's local protection.
                    candidate = deepcopy(graph)
                    feature = self._update_feature(candidate, candle, None, name, 0, selected_at)
                    self._save_input(name, None, feature=candidate, output=feature)
                    row["analysis_input_health"]["reason"] = "analysis_input_invalid"
                    input_ready = False
                self.features[name] = candidate
            else:
                feature = self._update_feature(graph, candle, analysis_inputs, name, 0, selected_at)
                input_ready = True
            account = self.data["accounts"][row["account_id"]]
            fee_rate = decimal(account["fee_rate"])
            available = self._available(
                row["account_id"],
                mark_at_ms=candle.close_time_ms,
                overrides=marks if marks is not None else {candle.instrument.key: candle.close},
            ) / (1 + fee_rate)
            execution = self._execution_setup(row, candle, input_source)
            context = TradeContext(
                candle.instrument,
                candle.close,
                candle.close_time_ms,
                PositionView(
                    decimal(row["quantity"]),
                    decimal(row["entry_price"]),
                    row["revision"],
                    row["last_changed_ms"],
                ),
                BudgetView(decimal(row["budget"]), available),
                execution["constraints"],
                candle,
                feature,
                not self.data["paused"]
                and (not self.data["feed"]["enabled"] or self.data["feed"]["ready"])
                and (not execution["active"] or execution["current_ready"])
                and input_ready,
                # Auxiliary readiness affects new risk only. Modules still see
                # each candle and may submit known protective reductions.
                has_pending_order=row["pending"] is not None,
            )
            try:
                decision = self.engines[name].evaluate(context)
                if decision.cancel_pending:
                    if execution["active"] and not execution["sizing_known"]:
                        decision = self.engines[name].reject_execution(
                            decision, "execution_rules_unavailable"
                        )
                        row["last_reason"], row["last_candle"] = (
                            decision.reason,
                            candle.open_time_ms,
                        )
                        continue
                    # This executor owns a simulated order, so cancellation is
                    # synchronous and certain. A live broker must await its own
                    # acknowledgement before giving a module pending=False.
                    row["pending"] = None
                    decision = self.engines[name].evaluate(
                        replace(context, has_pending_order=False)
                    )
            except Exception as exc:
                row["status"], row["last_reason"] = "quarantined", type(exc).__name__
                continue
            row["last_reason"], row["last_candle"] = decision.reason, candle.open_time_ms
            pin = None
            if decision.orders and execution["active"]:
                try:
                    pin = self._execution_pin(row, candle, execution, decision.orders[0])
                except (TradingBlocked, ValidationError):
                    rejected = decision
                    try:
                        decision = self.engines[name].reject_execution(
                            decision, "venue_execution_rule_rejected"
                        )
                    except Exception as exc:
                        row["status"], row["last_reason"] = "quarantined", type(exc).__name__
                        continue
                    health = execution["health"]
                    health.update(
                        state="protection_unavailable"
                        if rejected.orders[0].reducing
                        else "blocked",
                        reason="venue_execution_rule_rejected",
                        protection_blocked=bool(
                            rejected.orders[0].reducing and decimal(row["quantity"])
                        ),
                    )
                    row["last_reason"] = decision.reason
            if decision.orders:
                proposal = decision.orders[0]
                reserved = (
                    Decimal(0)
                    if proposal.reducing
                    else proposal.quantity * (proposal.price or candle.close) * (1 + fee_rate)
                )
                row["pending"] = {
                    "side": proposal.side.value,
                    "kind": proposal.kind.value,
                    "quantity": str(proposal.quantity),
                    "price": str(proposal.price) if proposal.price else None,
                    "reducing": proposal.reducing,
                    "reason": proposal.reason,
                    "eligible_at_ms": max(candle.close_time_ms, as_of_ms or candle.close_time_ms),
                    "reserved": str(reserved),
                    **({"execution": pin} if pin is not None else {}),
                }
        self.data["candles"][candle.instrument.key] = [*previous[-1999:], encoded]
        if candle.instrument.key in self.data["feed"]["provenance"]:
            self.data["feed"]["provenance"][candle.instrument.key] = input_source
        # Bound total snapshot cost across many symbols. Keep at least the last
        # two candles per instrument for causal open/close account valuations.
        excess = sum(len(rows) for rows in self.data["candles"].values()) - self.history_limit
        while excess > 0:
            eligible = [rows for rows in self.data["candles"].values() if len(rows) > 2]
            if not eligible:
                break
            oldest = min(eligible, key=lambda rows: rows[0]["open_time_ms"])
            count = min(excess, len(oldest) - 2)
            del oldest[:count]
            excess -= count
        return {
            "processed": len(matched),
            "open_time_ms": candle.open_time_ms,
            "source": input_source,
        }
