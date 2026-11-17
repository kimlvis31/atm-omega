"""Independent loopback core. Desktop/mobile clients never own trading state."""

# Imports
import argparse
import json
import re
import signal
import socket
import threading
import time
from collections import deque
from copy import deepcopy
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from omega.domain.errors import (
    AuthorizationDenied,
    CommandRejected,
    StorageError,
    ValidationError,
)
from omega.infrastructure.logging import CategoryLogger
from omega.runtime.control import (
    ControlState,
    InstanceLock,
    incident_error_context,
    load_token,
)
from omega.runtime.control_notifications import NotificationLoop
from omega.runtime.mobile_origin import authorize_mobile_headers
from omega.runtime.mobile_state import paper_device_snapshot
from omega.runtime.settings import SettingsStore, atomic_json

MAX_REQUEST_BYTES = 262_144
MAX_RESPONSE_BYTES = 8_000_000


def job_summaries(research):
    if research is None:
        return []
    provider = getattr(research, "list_job_summaries", None)
    source = provider(limit=100) if callable(provider) else research.list_jobs()
    result = []
    limits = {
        "id": 128,
        "job_id": 128,
        "kind": 64,
        "state": 64,
        "status": 64,
        "result_file": 2048,
        "error": 256,
        "progress": 128,
        "dispatch_state": 64,
        "computation_state": 64,
    }
    for job in source[:100]:
        if not isinstance(job, dict):
            continue
        summary = {
            key: value[:limit]
            for key, limit in limits.items()
            if isinstance((value := job.get(key)), str)
        }
        for key in ("created_at_ms", "finished_at_ms", "updated_at_ms"):
            value = job.get(key)
            if type(value) is int and 0 <= value < 2**63:
                summary[key] = value
        value = job.get("progress")
        if type(value) in {int, float} and 0 <= value <= 100:
            summary["progress"] = value
        result.append(summary)
    return result


def research_snapshot(research):
    """A broken research status must not hide paper/control state from its owner."""
    if research is None:
        return {"state": "unavailable"}, []
    health = {"state": "unknown"}
    try:
        provider = getattr(research, "health", None)
        if callable(provider):
            observed = provider()
            if not isinstance(observed, dict):
                raise ValueError("invalid research health")
            state = observed.get("state")
            if state not in {"ready", "degraded", "closed", "unknown", "unavailable"}:
                raise ValueError("invalid research health state")
            health = {"state": state}
            for key in ("quarantined", "supervisor_alive"):
                if type(observed.get(key)) is bool:
                    health[key] = observed[key]
            if isinstance(observed.get("error"), str):
                health["error"] = observed["error"][:512]
        jobs = job_summaries(research)
    except Exception:
        return {
            **health,
            "state": "degraded",
            "error": "RESEARCH_STATUS_UNAVAILABLE",
        }, []
    return health, jobs


def research_feature_scope(research, feature, job="simulate_margin"):
    """Optional bounded metadata must never hide financial/emergency state."""
    reader = getattr(research, "handler_scope", None)
    try:
        value = reader(job, feature) if callable(reader) else None
    except Exception:
        return None
    return value if type(value) is str and 1 <= len(value) <= 128 else None


class CoreService:
    def __init__(
        self,
        root: Path,
        *,
        research=None,
        modules_provider=None,
        candles_provider=None,
        paper_provider=None,
        market_data=None,
        analysis_library=None,
        paper_feed=None,
        calendar_candles_provider=None,
        analysis_execution=None,
        auxiliary_service=None,
        auxiliary_schedule=None,
        chart_series=None,
        chart_observations=None,
        shadow_inference=None,
        causal_inference=None,
        margin_trading=None,
        depth_collection=None,
    ):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.instance = InstanceLock(self.root / "state" / "core.lock")
        self.owner_mutation_lock = threading.RLock()
        self._closing = False
        self._cleanup_complete = False
        self._root_release_attempted = False
        self._exit_completion_guard = None
        self._root_exit_correction_error = None
        self._http_cleanup_stage = 0
        self.shutdown_management = None
        self.push_delivery = None
        self.push_error = None
        self.started_at = time.time()
        try:
            self.token = load_token(self.root / "secrets" / "core_token")
            self.settings = SettingsStore(self.root / "state" / "settings.json")
            self.control = ControlState(self.root / "state" / "control.json")
            self.logger = CategoryLogger(self.root / "logs", self.settings.snapshot)
            self.notifications = NotificationLoop(self.control, self.settings.snapshot)
            self.research, self.paper = research, paper_provider
            self.market_data = market_data
            self.analysis_library, self.paper_feed = analysis_library, paper_feed
            self.analysis_execution = analysis_execution
            self.auxiliary_service = auxiliary_service
            self.auxiliary_schedule = auxiliary_schedule
            self.chart_series = chart_series
            self.chart_observations = chart_observations
            self.shadow_inference = shadow_inference
            self.causal_inference = causal_inference
            self._causal_shutdown_token = None
            self.margin_trading = margin_trading
            self.depth_collection = depth_collection
            self.depth_commands = {}
            self.shadow_start_error = None
            self.causal_start_error = None
            self.leverage_reference = None
            self.refresh_leverage_on_start = True
            self.modules_provider, self.candles_provider = (
                modules_provider,
                candles_provider,
            )
            self.calendar_candles_provider = calendar_candles_provider
            self.server = None
            self.pair_attempts = deque(maxlen=30)
            self.pair_lock = threading.Lock()
            if self.paper is not None:
                self.paper.command("pause", {})
            self.log("system", "INFO", "CORE_STARTED", mode="paper")
        except Exception:
            self.instance.close()
            raise

    def log(self, category, level, code, **parameters):
        def write():
            try:
                self.logger.write(category, level, code, **parameters)
            except (OSError, ValueError, TypeError):
                self.control.incident("DIAGNOSTIC_LOG_UNAVAILABLE")

        self._diagnostic_write(write)

    def record_incident(self, code, severity="critical", *, context=None):
        # Even a GET can fail after the bounded HTTP worker join. Its diagnostic
        # must not restore an obsolete control snapshot over the next core's file.
        self._diagnostic_write(
            lambda: self.control.incident(
                code,
                severity,
                invalidate_resume=code == "PAPER_RECORDING_UNAVAILABLE",
                **({"context": context} if context is not None else {}),
            )
        )

    def enable_web_push(self, *, subject=""):
        """Explicit local owner bootstrap only; OFF never imports optional crypto."""
        if self.server is not None or self._closing or self.push_delivery is not None:
            raise ValidationError("push can only be enabled before Core startup")
        sender = None
        try:
            if not self.settings.snapshot()["mobile"]["https_origin"]:
                raise ValidationError("private mobile HTTPS origin required")
            from omega.infrastructure.webpush_sender import WebPushSender
            from omega.runtime.push_delivery import PushDelivery
            from omega.runtime.push_subscriptions import SubscriptionStore

            sender = WebPushSender(self.root, subject=subject)
            store = SubscriptionStore(self.root / "secrets" / "push-subscriptions.json")
            self.push_delivery = PushDelivery(
                self.control,
                store,
                sender,
                # Direct bounded logger only: a push failure cannot create another push event.
                diagnostic=lambda code: self.logger.write("system", "WARNING", code),
            )
            self.push_error = None
            return {"enabled": True, "available": True}
        except Exception:
            if sender is not None:
                sender.close()
            self.push_error = "WEBPUSH_ENABLE_UNAVAILABLE"
            return {"enabled": True, "available": False, "warning": self.push_error}

    def _diagnostic_write(self, action):
        # close() holds this lock while joining providers. A provider callback
        # must observe closing and exit rather than deadlock the join.
        while not self._closing:
            if self.owner_mutation_lock.acquire(timeout=0.1):
                break
        else:
            return
        try:
            if not self._closing:
                action()
        finally:
            self.owner_mutation_lock.release()

    def state(self, *, owner=True):
        with self.control.lock:
            control = self.control.snapshot()
        # Device reads use a data-only cache; snapshot() can consult plugin state.
        if owner:
            portfolio = self.paper.snapshot() if self.paper is not None else {"accounts": []}
        elif self.paper is None:
            portfolio = {"accounts": []}
        else:
            try:
                reader = getattr(self.paper, "readonly_snapshot", None)
                portfolio = (
                    paper_device_snapshot(reader()) if callable(reader) else {"accounts": None}
                )
            except Exception:
                portfolio = {"accounts": None}
        # Independent telemetry may touch files/providers; it must not hold the
        # control command lock and delay an emergency pause.
        return self._state(control, portfolio, owner=owner)

    def _state(self, control, portfolio, *, owner=True):
        research, jobs = research_snapshot(self.research) if owner else ({}, [])
        funding_scope = research_feature_scope(self.research, "funding") if owner else None
        cost_scope = research_feature_scope(self.research, "cost_policy") if owner else None
        observation_scope = (
            research_feature_scope(self.research, "auxiliary_replay") if owner else None
        )
        bundle_v2_scope = (
            research_feature_scope(self.research, "native_bundle_v2", "export_auxiliary")
            if owner
            else None
        )
        observation_v2_scope = (
            research_feature_scope(self.research, "auxiliary_replay_v2") if owner else None
        )
        projection_scope = (
            research_feature_scope(self.research, "analysis_execution") if owner else None
        )
        projection_available = owner and (
            getattr(self.margin_trading, "analysis_execution_scope", None) == "required_fields_v1"
            or projection_scope == "required_fields_v1"
        )
        paper_paused = portfolio.get("paused", True)
        recording = portfolio.get("recording", {})
        feed = portfolio.get("feed", {})
        data_gate = not feed.get("enabled", False) or feed.get("ready", False)
        paper_recording = recording.get("available", False)
        manual_resume = recording.get("manual_resume_required", True)
        effective_entries = bool(
            self.paper is not None
            and not self._closing
            and not control["paused"]
            and not paper_paused
            and data_gate
            and paper_recording
            and control["storage_available"]
            and not manual_resume
            and not recording.get("owner_retired", True)
        )
        market = {"status": "not_connected", "source": "database", "demo": False}
        if (
            owner
            and self.candles_provider is not None
            and callable(getattr(self.candles_provider, "health", None))
        ):
            try:
                observed = self.candles_provider.health()
                if not isinstance(observed, dict):
                    raise ValueError("invalid market health")
                json.dumps(observed, allow_nan=False)
                market.update(observed)
            except Exception:
                market["reason"] = "health_unavailable"
        result = {
            "schema_version": 1,
            "mode": "paper",
            "sequence": control["sequence"],
            "core": {
                "status": "running",
                "paused": control["paused"],
                "control_paused": control["paused"],
                "paper_paused": paper_paused,
                "data_gate": data_gate,
                "data_gate_reason": feed.get("reason", "unavailable"),
                "effective_entries_allowed": effective_entries,
                "control_recording": control["storage_available"],
                "paper_recording": paper_recording,
                "paper_manual_resume_required": manual_resume,
                "epoch": control["epoch"],
                "control_revision": control["control_revision"],
                "server_time_ms": control["server_time_ms"],
                "storage_available": control["storage_available"],
                "started_at_ms": int(self.started_at * 1000),
                "live_orders_enabled": False,
                "notification_audio_owner": "core",
                "notification_speech_owner": "core",
                "speech": self.notifications.speech.snapshot() if owner else {},
                "audio_delivery_failed": self.notifications.delivery_failed,
                "web_push": (
                    self.push_delivery.status()
                    if self.push_delivery is not None
                    else {
                        "enabled": self.push_error is not None,
                        "available": False,
                        "warning": self.push_error,
                    }
                )
                if owner
                else {},
                "shadow_start_error": self.shadow_start_error if owner else None,
                "causal_start_error": self.causal_start_error if owner else None,
                "capabilities": {
                    "shutdown_management": self.shutdown_management is not None,
                    "paper": self.paper is not None,
                    "research": self.research is not None,
                    "margin_simulation": owner
                    and callable(getattr(self.research, "supports", None))
                    and self.research.supports("simulate_margin"),
                    "margin_funding": funding_scope == "minute_boundary_scenario_rate_v1",
                    "margin_funding_scope": funding_scope,
                    "margin_cost_policy": cost_scope == "fixed_adverse_full_fill_delay_v1",
                    "margin_cost_policy_scope": cost_scope,
                    "margin_native_observation_replay": observation_scope
                    == "native_trade_flow_available_cutoff_v1",
                    "margin_native_observation_replay_scope": observation_scope,
                    "native_observation_bundle_v2": bundle_v2_scope == "recorded_native_inputs_v2",
                    "native_observation_bundle_v2_scope": bundle_v2_scope,
                    "margin_native_observation_replay_v2": observation_v2_scope
                    == "native_auxiliary_available_cutoff_v2",
                    "margin_native_observation_replay_v2_scope": observation_v2_scope,
                    "margin_analysis_execution": projection_available,
                    "margin_analysis_execution_scope": "required_fields_v1"
                    if projection_available
                    else None,
                    "auxiliary_export": owner
                    and callable(getattr(self.research, "supports", None))
                    and self.research.supports("export_auxiliary"),
                    "market": self.candles_provider is not None,
                    "database": self.market_data is not None,
                    "analysis_library": self.analysis_library is not None,
                    "analysis_execution": self.analysis_execution is not None,
                    "auxiliary_jobs": self.auxiliary_service is not None,
                    "auxiliary_schedule": self.auxiliary_schedule is not None,
                    "paper_auxiliary_inputs": owner
                    and self.paper_feed is not None
                    and callable(
                        getattr(
                            getattr(self.paper_feed, "source", None),
                            "prepare_inputs",
                            None,
                        )
                    ),
                    "analysis_auxiliary_inputs": owner
                    and self.analysis_execution is not None
                    and callable(getattr(self.analysis_execution, "input_policy_factory", None)),
                    "venue_rules": owner
                    and self.paper is not None
                    and callable(getattr(self.paper, "rules_provider", None)),
                    "calendar_bars": self.calendar_candles_provider is not None,
                    "chart_series": self.chart_series is not None,
                    "chart_analysis_kinds": list(getattr(self.chart_series, "analysis_kinds", ())),
                    "chart_observations": self.chart_observations is not None,
                    "shadow_inference": self.shadow_inference is not None,
                    "causal_inference": self.causal_inference is not None,
                    "margin_trading": self.margin_trading is not None,
                    "depth_collection": self.depth_collection is not None,
                    "paper_feed": self.paper_feed is not None,
                },
            },
            "market": market,
            "accounts": portfolio.get("accounts", []),
            "strategies": portfolio.get("strategies", []) if owner else [],
            "jobs": jobs,
            "research": research,
            "execution": portfolio.get("execution", {}) if owner else {},
            "incidents": control["incidents"],
            "settings": self.settings.snapshot() if owner else {},
            "feed": {**portfolio.get("feed", {}), **self.paper_feed.status()}
            if owner and self.paper_feed is not None
            else {"state": "unavailable"},
            "leverage_reference": self.leverage_reference.status()
            if owner and self.leverage_reference is not None
            else {},
        }
        from omega.runtime.mobile_margin_view import (
            sanitize_margin_portfolio,
            unavailable_margin_portfolio,
        )

        try:
            reader = getattr(self.margin_trading, "readonly_portfolio", None)
            result["margin_portfolio"] = (
                sanitize_margin_portfolio(reader())
                if callable(reader)
                else unavailable_margin_portfolio()
            )
        except Exception:
            # An unavailable cache must not masquerade as confirmed flatness or
            # hide emergency control. Never include provider exception details.
            result["margin_portfolio"] = unavailable_margin_portfolio()
        return result

    def _execute(self, kind, payload, *, operation_id=None):
        if kind in {"pause", "resume"}:
            if self.paper is None:
                result = {"paused": kind == "pause"}
            elif kind == "resume" and callable(getattr(self.paper, "recording_health", None)):
                with self.paper.lock:
                    health = self.paper.recording_health()
                    if health["manual_resume_required"] and not health["incident_announced"]:
                        raise CommandRejected("paper recording incident notification is pending")
                    result = self.paper.command(kind, payload)
            else:
                result = self.paper.command(kind, payload)
        elif kind == "settings.patch":
            try:
                result = self.settings.update(
                    payload.get("changes"), revision=payload.get("revision")
                )
            except ValidationError as error:
                raise CommandRejected("invalid settings change") from error
            self.notifications.speech.configure(result["notifications"], stop_if_disabled=True)
        elif kind == "notifications.speech.probe":
            result = self.notifications.speech.probe()
        elif kind == "notifications.speech.preview":
            result = self.notifications.speech.preview(self.settings.snapshot()["notifications"])
        elif kind.startswith("analysis.execution."):
            result = self._analysis_command(kind, payload)
        elif kind.startswith("analysis.") and self.analysis_library is not None:
            try:
                result = self.analysis_library.command(kind, payload, operation_id=operation_id)
            except ValidationError as error:
                raise CommandRejected("invalid analysis command") from error
        elif kind == "paper.feed.configure" and self.paper is not None:
            result = self.paper.feed_configure(
                payload.get("enabled"),
                start_ms=payload.get("start_ms"),
                warmup_minutes=payload.get("warmup_minutes", 240),
            )
        elif kind == "issue_pairing":
            result = self.control.create_pairing(payload.get("name"), payload.get("permissions"))
        elif kind == "revoke_device":
            result = self.control.revoke(payload.get("device_id"))
        elif kind in {"acknowledge", "incident.acknowledge"}:
            result = self.control.acknowledge(payload.get("incident_id"))
        elif kind == "job.cancel" and self.research is not None:
            status = self.research.cancel(payload.get("job_id"))
            result = {
                key: str(status[key])[:256]
                for key in ("id", "kind", "state", "error")
                if key in status
            }
        elif kind.startswith("paper.") and self.paper is not None:
            if kind == "paper.strategy.add":
                payload = deepcopy(payload)
                payload.pop("analysis_snapshot", None)
                if "analysis_id" in payload:
                    try:
                        if self.analysis_library is None:
                            raise ValidationError("analysis library unavailable")
                        frozen = self.analysis_library.snapshot_for_strategy(
                            payload.pop("analysis_id"),
                            payload.get("instrument"),
                            expected_revision=payload.pop("analysis_revision", None),
                        )
                    except ValidationError as error:
                        raise CommandRejected("invalid analysis binding") from error
                    payload["analysis"], payload["analysis_snapshot"] = (
                        frozen["configuration"],
                        frozen,
                    )
            result = self.paper.command(kind, payload)
        else:
            raise ValidationError("unsupported core command")
        self.log("control", "INFO", "COMMAND_APPLIED", kind=kind)
        return result

    def _analysis_command(self, kind, payload):
        if self.analysis_execution is None or self.analysis_library is None:
            raise CommandRejected("independent analysis execution is unavailable")
        try:
            if kind == "analysis.execution.start":
                if set(payload) - {
                    "analysis_id",
                    "expected_revision",
                    "warmup_minutes",
                    "restart",
                } or not {"analysis_id", "expected_revision"} <= set(payload):
                    raise ValidationError("invalid analysis start fields")
                frozen = self.analysis_library.snapshot_for_execution(
                    payload["analysis_id"], payload["expected_revision"]
                )
                return self.analysis_execution.start(
                    frozen,
                    warmup_minutes=payload.get("warmup_minutes", 240),
                    restart=payload.get("restart", False),
                )
            if kind == "analysis.execution.stop":
                if (
                    set(payload) != {"analysis_id", "execution_id"}
                    or type(payload["execution_id"]) is not str
                    or re.fullmatch(r"[a-f0-9]{32}", payload["execution_id"]) is None
                ):
                    raise ValidationError("analysis stop requires the current execution ID")
                return self.analysis_execution.stop(
                    payload["analysis_id"],
                    expected_execution_id=payload["execution_id"],
                )
            raise ValidationError("unsupported analysis execution command")
        except ValidationError as error:
            raise CommandRejected(
                "invalid analysis execution command; refresh saved CA state"
            ) from error

    def request(self, method: str, target: str, token: str, body=None):
        # A retiring owner must finish every accepted mutation before releasing its
        # instance lock. Reads remain parallel; slow jobs execute outside dispatch.
        if method == "POST":
            while not self._closing:
                if self.owner_mutation_lock.acquire(timeout=0.1):
                    break
            else:
                raise ValidationError("core owner is stopping")
            try:
                if self._closing:
                    raise ValidationError("core owner is stopping")
                return self._request(method, target, token, body)
            finally:
                self.owner_mutation_lock.release()
        if self._closing:
            raise ValidationError("core owner is stopping")
        return self._request(method, target, token, body)

    def _request(self, method: str, target: str, token: str, body=None):
        """Transport-independent dispatch enables hostile-input tests without networking."""
        url = urlsplit(target)
        path = url.path
        if method == "POST" and path == "/api/pair":
            with self.pair_lock:
                now = time.monotonic()
                while self.pair_attempts and self.pair_attempts[0] <= now - 60:
                    self.pair_attempts.popleft()
                if len(self.pair_attempts) >= 10:
                    raise AuthorizationDenied("pairing attempts limited; retry later")
                self.pair_attempts.append(now)
            if (
                not isinstance(body, dict)
                or not isinstance(body.get("code"), str)
                or len(body["code"]) > 128
            ):
                raise ValidationError("invalid pairing request")
            return self.control.pair(body["code"])
        identity = self.control.authorize(token, self.token)
        if identity is None:
            raise AuthorizationDenied("authentication required")
        owner = identity == "admin"
        if path in {"/api/push/config", "/api/push/subscribe", "/api/push/unsubscribe"}:
            from omega.runtime.push_subscriptions import device_generation

            # Owner credentials never stand in for a registered phone subscription.
            device_generation(self.control, identity)
            if url.query or url.fragment:
                raise ValidationError("push API does not accept URL parameters")
            if method == "GET" and path == "/api/push/config":
                if self.push_delivery is None:
                    return {
                        "enabled": self.push_error is not None,
                        "available": False,
                        "warning": self.push_error,
                        "subscribed": False,
                        "delivery": "unverified",
                    }
                return self.push_delivery.config(identity)
            if self.push_delivery is None:
                raise ValidationError("optional push is disabled or unavailable")
            if method == "POST" and path == "/api/push/subscribe":
                return self.push_delivery.subscribe(identity, body)
            if method == "POST" and path == "/api/push/unsubscribe":
                return self.push_delivery.unsubscribe(identity, body)
            raise ValidationError("unsupported push operation")
        if path == "/api/shutdown" or path.startswith("/api/shutdown/"):
            if not owner:
                raise AuthorizationDenied("shutdown control is owner-only")
            return self._shutdown_request(method, path, url.query, body)
        if (
            method == "POST"
            and self.shutdown_management is not None
            and self.shutdown_management.blocks_new_risk
        ):
            kind = body.get("kind") if isinstance(body, dict) else None
            if (
                path in {"/api/margin-trading", "/api/accounts", "/api/strategies"}
                or path == "/api/causal-inference"
                and not (isinstance(kind, str) and kind in {"pause", "retire"})
                or path
                in {
                    "/api/causal-inference/recovery",
                    "/api/causal-inference/recovery/actions/adopt",
                }
                or path == "/api/commands"
                and (
                    kind == "resume"
                    or isinstance(kind, str)
                    and kind.startswith("paper.")
                    and kind not in {"paper.step", "paper.feed.continue", "paper.feed.pause"}
                )
            ):
                raise ValidationError("shutdown is active; new trading mutations are blocked")
        if method == "GET" and path == "/api/state":
            # Recheck under the same lock as revocation/permission changes.
            with self.control.lock:
                if not owner:
                    device = self.control.data["devices"].get(identity, {})
                    permissions = device.get("permissions")
                    if (
                        device.get("revoked") is not False
                        or type(permissions) is not list
                        or "read" not in permissions
                    ):
                        raise AuthorizationDenied("device read permission required")
            # Provider reads must not hold the control/emergency command lock.
            return self.state(owner=owner)
        if method == "GET" and path.startswith("/api/operations/"):
            return self.control.operation(identity, path.removeprefix("/api/operations/"))
        if method == "POST" and path == "/api/commands":
            response = None
            try:
                response = self.control.command(
                    identity,
                    body,
                    lambda kind, payload: self._execute(kind, payload, operation_id=body["id"]),
                )
                return response
            finally:
                needs_pause = self.control.storage_failed or (
                    response is not None and response["status"] == "unknown"
                )
                if isinstance(body, dict) and body.get("kind") in {
                    "acknowledge",
                    "incident.acknowledge",
                }:
                    # Acknowledging an existing storage alarm must not reissue a
                    # failing pause write and create a fresh alarm indefinitely.
                    needs_pause = False
                if self.paper is not None and needs_pause:
                    try:
                        self.paper.command("pause", {})
                    except Exception:
                        self.control.incident("PAPER_PAUSE_FAILED")
        if not owner:
            raise AuthorizationDenied("owner permission required")
        if path in {
            "/api/causal-inference",
            "/api/external-feature-bindings",
        } or path.startswith("/api/causal-inference/"):
            if self.causal_inference is None:
                raise ValidationError("committed inference is unavailable")
            from omega.runtime.causal_api import causal_request

            result = causal_request(
                self.causal_inference,
                method,
                path,
                url.query,
                body,
                core_epoch=self.control.snapshot()["epoch"],
            )
            if method == "POST":
                self.log(
                    "research",
                    "INFO",
                    "CAUSAL_CONTROL_RESPONSE",
                    operation_id=result.get("id"),
                    state=result.get("state"),
                    durable=result.get("durable"),
                )
            return result
        if path == "/api/chart-observations" and method == "GET":
            if self.chart_observations is None:
                raise ValidationError("auxiliary chart observations are unavailable")
            query = parse_qs(url.query, keep_blank_values=True)
            required = {"instrument", "kind", "start_ms", "end_ms", "period_minutes"}
            if (
                not required <= query.keys()
                or query.keys() - required - {"source"}
                or any(len(values) != 1 for values in query.values())
            ):
                raise ValidationError("invalid auxiliary chart query")
            parameters = {key: values[0] for key, values in query.items()}
            try:
                for key in ("start_ms", "end_ms", "period_minutes"):
                    if not parameters[key].isascii() or not parameters[key].isdigit():
                        raise ValueError
                    parameters[key] = int(parameters[key])
            except ValueError:
                raise ValidationError(
                    "auxiliary chart bounds must be native integer milliseconds"
                ) from None
            return {
                **self.chart_observations.read(parameters),
                "core_epoch": self.control.epoch,
            }
        if self.depth_collection is not None and (
            path == "/api/depth" or path.startswith("/api/depth/")
        ):
            return self._depth_request(method, path, url.query, body)
        if self.margin_trading is not None and (
            path == "/api/margin-trading" or path.startswith("/api/margin-trading/")
        ):
            return self._margin_request(method, path, url.query, body)
        if self.shadow_inference is not None and (
            path == "/api/shadow-models" or path.startswith("/api/shadow-models/")
        ):
            return self._shadow_request(method, path, url.query, body)
        if self.auxiliary_schedule is not None and (
            path == "/api/auxiliary-schedule" or path.startswith("/api/auxiliary-schedule/")
        ):
            return self._auxiliary_schedule_request(method, path, url.query, body)
        if self.auxiliary_service is not None and (
            path == "/api/auxiliary-jobs" or path.startswith("/api/auxiliary-jobs/")
        ):
            return self._auxiliary_request(method, path, url.query, body)
        if method == "GET" and path == "/api/settings":
            return self.settings.snapshot()
        if self.chart_series is not None and (
            path == "/api/chart-series" or path.startswith("/api/chart-series/")
        ):
            if url.query:
                raise ValidationError("chart series does not accept query parameters")
            if (
                method == "POST"
                and path == "/api/chart-series"
                and type(body) is dict
                and set(body) == {"id", "parameters"}
            ):
                result = self.chart_series.submit(body["id"], body["parameters"])
            elif (
                method == "POST"
                and path.startswith("/api/chart-series/")
                and path.endswith("/cancel")
                and type(body) is dict
                and not body
            ):
                identity = path.removeprefix("/api/chart-series/").removesuffix("/cancel")
                result = self.chart_series.cancel(identity)
            elif method == "GET" and path.startswith("/api/chart-series/"):
                result = self.chart_series.status(path.removeprefix("/api/chart-series/"))
            else:
                raise ValidationError("invalid chart series operation")
            return {**result, "core_epoch": self.control.snapshot()["epoch"]}
        if (
            method == "GET"
            and path == "/api/reference/leverage"
            and self.leverage_reference is not None
        ):
            return self.leverage_reference.table(parse_qs(url.query).get("symbol", [None])[0])
        if (
            method == "GET"
            and path == "/api/analysis-library"
            and self.analysis_library is not None
        ):
            result = self.analysis_library.snapshot()
            result["capabilities"]["execution_subscription"] = self.analysis_execution is not None
            return result
        if (
            method == "GET"
            and path == "/api/analysis-execution"
            and self.analysis_execution is not None
            and self.analysis_library is not None
        ):
            query = parse_qs(url.query, keep_blank_values=True)
            if set(query) - {"analysis_id"} or any(len(values) != 1 for values in query.values()):
                raise ValidationError("invalid analysis execution query")
            analysis_id = query.get("analysis_id", [None])[0]
            if analysis_id is None:
                result = self.analysis_execution.snapshot()
            else:
                saved = self.analysis_library.snapshot_for_execution
                records = self.analysis_library.snapshot()["instances"]
                row = next((row for row in records if row["id"] == analysis_id), None)
                if row is None:
                    raise ValidationError("saved CA not found")
                saved(analysis_id, row["revision"])
                result = self.analysis_execution.snapshot()
                result["instances"] = [
                    row for row in result["instances"] if row["analysis_id"] == analysis_id
                ]
            return {**result, "core_epoch": self.control.snapshot()["epoch"]}
        if method == "GET" and path == "/api/paper/feed" and self.paper_feed is not None:
            return self.paper_feed.status()
        if method == "GET" and path == "/api/database" and self.market_data is not None:
            query = parse_qs(url.query)
            return self.market_data.snapshot(
                query.get("market", ["usdm"])[0], query.get("instrument", [None])[0]
            )
        if (
            method == "GET"
            and path.startswith("/api/database/actions/")
            and self.market_data is not None
        ):
            return self.market_data.status(path.removeprefix("/api/database/actions/"))
        if method == "POST" and path == "/api/database/actions" and self.market_data is not None:
            if not isinstance(body, dict) or set(body) != {"id", "kind", "parameters"}:
                raise ValidationError("invalid database action envelope")
            # Peer credentials stay in the worker's memory, outside the control command journal.
            try:
                return self.market_data.submit(body["id"], body["kind"], body["parameters"])
            except ValidationError:
                # Validation fails before dispatch. Distinguish a known rejection from a lost
                # acceptance response; never echo parameters or exception text containing secrets.
                return {
                    "id": str(body["id"])[:96],
                    "kind": str(body["kind"])[:64],
                    "state": "rejected",
                    "error": "INVALID_DATABASE_ACTION",
                }
        if method == "GET" and path.startswith("/api/strategies/") and self.paper is not None:
            return self.paper.detail(path.removeprefix("/api/strategies/"))
        if method == "GET" and path == "/api/devices":
            return {"devices": self.control.devices()}
        if method == "GET" and path == "/api/modules":
            return {
                "ts": self.modules_provider() if self.modules_provider else [],
                "nn": self.research.list_modules()
                if self.research and hasattr(self.research, "list_modules")
                else [],
                "status": "available" if self.modules_provider else "unavailable",
            }
        if method == "GET" and path == "/api/chart-candles" and self.calendar_candles_provider:
            query = parse_qs(url.query, keep_blank_values=True)
            if set(query) - {"instrument", "period", "limit", "end_ms"} or any(
                len(value) != 1 for value in query.values()
            ):
                raise ValidationError("invalid calendar query fields")
            try:
                return self.calendar_candles_provider(
                    query.get("instrument", ["binance:usdm:BTCUSDT"])[0],
                    query.get("period", ["1w"])[0],
                    int(query["limit"][0]) if "limit" in query else None,
                    int(query["end_ms"][0]) if "end_ms" in query else None,
                )
            except ValueError:
                raise ValidationError("invalid calendar query values") from None
        if method == "GET" and path == "/api/candles":
            epoch_option = parse_qs(url.query, keep_blank_values=True).get("include_core_epoch")
            if epoch_option is not None and epoch_option != ["1"]:
                raise ValidationError("invalid candle epoch envelope option")
            # Capture the serving owner before a potentially slow provider read.
            # Pending old-owner responses must not acquire a newer GUI epoch.
            candle_epoch = self.control.epoch if epoch_option is not None else None
            query = parse_qs(url.query)
            instrument = query.get("instrument", ["binance:usdm:BTCUSDT"])[0]
            try:
                limit = int(query.get("limit", ["500"])[0])
            except ValueError:
                raise ValidationError("invalid candle limit") from None
            if not 1 <= limit <= 10_000 or len(instrument) > 128:
                raise ValidationError("candle request outside bounds")
            if self.candles_provider:
                result = self.candles_provider(instrument, limit)
            else:
                result = {
                    "instrument": instrument,
                    "candles": [],
                    "source": "database",
                    "quality": {
                        "status": "unavailable",
                        "reason": "collector_not_connected",
                    },
                }
            if candle_epoch is not None:
                if type(result) is not dict:
                    raise ValidationError("candle source did not return an epoch envelope")
                return {**result, "core_epoch": candle_epoch}
            return result
        if method == "GET" and path == "/api/jobs":
            return {"jobs": job_summaries(self.research)}
        if method == "GET" and path.startswith("/api/jobs/") and self.research:
            return self.research.status(path.removeprefix("/api/jobs/"))
        if method == "POST" and path == "/api/jobs" and self.research:
            if (
                not isinstance(body, dict)
                or not isinstance(body.get("kind"), str)
                or not isinstance(body.get("parameters", {}), dict)
            ):
                raise ValidationError("invalid job specification")
            parameters = self.job_parameters(body["kind"], body.get("parameters", {}))
            result = self.research.submit(body["kind"], parameters)
            self.log(
                "research",
                "INFO",
                "JOB_SUBMITTED",
                job_id=result.get("id"),
                kind=body["kind"],
            )
            return result
        raise LookupError("API route is unavailable")

    def _shutdown_request(self, method, path, query, body):
        manager = self.shutdown_management
        if manager is None:
            raise ValidationError("shutdown management is unavailable")
        if query:
            raise ValidationError("shutdown query parameters are unsupported")

        def receipt(row):
            row = deepcopy(row)
            row["operation_core_epoch"] = row["core_epoch"]
            row["core_epoch"] = manager.core_epoch
            return row

        if method == "GET" and path == "/api/shutdown":
            result = manager.snapshot()
            self._sync_causal_shutdown_cancel(result)
            result["operations"] = [receipt(row) for row in result["operations"]]
            return result
        if method == "GET" and path.startswith("/api/shutdown/"):
            self._sync_causal_shutdown_cancel(manager.snapshot())
            return receipt(manager.status(path.removeprefix("/api/shutdown/")))
        if method == "POST" and path == "/api/shutdown":
            if type(body) is not dict or set(body) != {
                "id",
                "core_epoch",
                "expected_revision",
                "kind",
                "parameters",
            }:
                raise ValidationError("invalid shutdown operation envelope")
            from omega.runtime.shutdown import identity, parameters

            identity(body["id"])
            parameters(body["kind"], body["parameters"])
            fenced = False
            if body["kind"] in {"save_exit", "flatten_exit"}:
                self._fence_causal_shutdown(body["id"])
                fenced = self._causal_shutdown_token == body["id"]
            try:
                return receipt(
                    manager.submit(
                        body["id"],
                        body["core_epoch"],
                        body["expected_revision"],
                        body["kind"],
                        body["parameters"],
                    )
                )
            except StorageError:
                if body["kind"] in {"save_exit", "flatten_exit"}:
                    try:
                        self.pause_for_shutdown(body["id"])
                    finally:
                        self.record_incident("SHUTDOWN_RECORDING_UNAVAILABLE", "critical")
                raise
            except ValidationError:
                if fenced and not manager.blocks_new_risk:
                    self._clear_causal_shutdown(body["id"])
                raise
        raise ValidationError("unsupported shutdown route")

    def _fence_causal_shutdown(self, operation_id):
        notify = getattr(self.causal_inference, "notify_shutdown_requested", None)
        if callable(notify):
            token = self._causal_shutdown_token or operation_id
            notify(token, active=True)
            self._causal_shutdown_token = token

    def _clear_causal_shutdown(self, operation_id):
        notify = getattr(self.causal_inference, "notify_shutdown_requested", None)
        if callable(notify) and self._causal_shutdown_token == operation_id:
            notify(operation_id, active=False)
            self._causal_shutdown_token = None

    def _sync_causal_shutdown_cancel(self, snapshot):
        """Only completed cancellation clears the install hold; it never resumes NN."""
        # A cached GET must not wait behind a POST, or clear a new shutdown's
        # fence between its memory reservation and durable manager admission.
        if not self.owner_mutation_lock.acquire(blocking=False):
            return
        try:
            self._sync_causal_shutdown_cancel_locked(snapshot)
        finally:
            self.owner_mutation_lock.release()

    def _sync_causal_shutdown_cancel_locked(self, snapshot):
        token = self._causal_shutdown_token
        if token is None or self.shutdown_management.blocks_new_risk:
            return
        operations = snapshot.get("operations", ())
        original = next((row for row in operations if row.get("id") == token), None)
        if original is None:
            return
        if original.get("state") == "canceled" or any(
            row.get("kind") == "cancel"
            and row.get("state") == "completed"
            and row.get("core_epoch") == original.get("core_epoch")
            and type(row.get("expected_revision")) is int
            and type(original.get("expected_revision")) is int
            and row["expected_revision"] > original["expected_revision"]
            for row in operations
        ):
            self._clear_causal_shutdown(token)

    def pause_for_shutdown(self, operation_id):
        self._fence_causal_shutdown(operation_id)
        identity = "shutdown_pause_" + sha256(operation_id.encode()).hexdigest()
        response = self.control.command(
            "admin", {"id": identity, "kind": "pause", "payload": {}}, self._execute
        )
        if (
            response.get("status") != "completed"
            or response.get("result", {}).get("durable") is False
        ):
            raise StorageError("shutdown pause is not durably confirmed")
        return response

    def _shadow_request(self, method, path, query, body):
        if query:
            raise ValidationError("shadow inference does not accept query parameters")
        base = "/api/shadow-models"
        if path == base and method == "GET":
            result = self.shadow_inference.snapshot()
        elif (
            path == base
            and method == "POST"
            and type(body) is dict
            and set(body) == {"id", "kind", "parameters"}
        ):
            result = self.shadow_inference.submit(body["id"], body["kind"], body["parameters"])
        elif path.startswith(base + "/") and "/" not in path[len(base) + 1 :] and method == "GET":
            result = self.shadow_inference.status(path[len(base) + 1 :])
        else:
            raise ValidationError("invalid shadow inference operation")
        origin = (
            {"operation_core_epoch": result["core_epoch"]}
            if "id" in result and "core_epoch" in result
            else {}
        )
        return {
            **result,
            **origin,
            "core_epoch": self.control.snapshot()["epoch"],
            "scope": "paper_shadow_only",
        }

    def _margin_request(self, method, path, query, body):
        ledger_base = "/api/margin-trading/accounts/"
        epoch = self.control.snapshot()["epoch"]
        if method == "GET" and path.startswith(ledger_base) and path.endswith("/retirement"):
            from omega.runtime.margin_retirement_client import (
                retirement_url,
                validate_retirement_observation,
            )

            encoded = path[len(ledger_base) : -len("/retirement")]
            if query or "/" in encoded or re.search(r"%(?![a-fA-F0-9]{2})", encoded):
                raise ValidationError("invalid retirement identity or query")
            try:
                account_id = unquote(encoded, errors="strict")
            except UnicodeError:
                raise ValidationError("invalid encoded retirement identity") from None
            retirement_url(account_id)
            result = self.margin_trading.inspect_retirement(account_id)
            return validate_retirement_observation(result, account_id=account_id, core_epoch=epoch)
        history_base = "/api/margin-trading/history"
        if (path == history_base or path.startswith(history_base + "/")) and getattr(
            self.margin_trading, "core_epoch", epoch
        ) != epoch:
            raise ValidationError("retired history belongs to another core epoch")
        if path == history_base and method == "POST":
            from omega.runtime.margin_retirement_client import (
                history_load_body,
                validate_retired_history,
            )

            if (
                query
                or type(body) is not dict
                or set(body) != {"id", "account_id", "expected_receipt_sha256", "core_epoch"}
                or body["core_epoch"] != epoch
            ):
                raise ValidationError("invalid retired-history request or stale epoch")
            expected = history_load_body(
                body["id"], body["account_id"], body["expected_receipt_sha256"], epoch
            )
            result = self.margin_trading.load_retired_history(
                expected["id"],
                expected["account_id"],
                expected["expected_receipt_sha256"],
            )
            return validate_retired_history(
                {**result, "core_epoch": epoch},
                identity=expected["id"],
                core_epoch=epoch,
            )
        if path.startswith(history_base + "/") and method == "GET":
            from omega.runtime.margin_retirement_client import (
                history_job_url,
                validate_retired_history,
            )

            encoded = path[len(history_base) + 1 :]
            if query or "/" in encoded or re.search(r"%(?![a-fA-F0-9]{2})", encoded):
                raise ValidationError("invalid retired-history job identity")
            try:
                identity = unquote(encoded, errors="strict")
            except UnicodeError:
                raise ValidationError("invalid encoded retired-history job") from None
            history_job_url(identity)
            result = self.margin_trading.retired_history_status(identity)
            return validate_retired_history(
                {**result, "core_epoch": epoch}, identity=identity, core_epoch=epoch
            )
        if method == "GET" and path.startswith(ledger_base) and path.endswith("/configuration"):
            from omega.runtime.margin_configuration_client import configuration_url

            encoded = path[len(ledger_base) : -len("/configuration")]
            if query or "/" in encoded or re.search(r"%(?![a-fA-F0-9]{2})", encoded):
                raise ValidationError("invalid configuration identity or query")
            try:
                identity = unquote(encoded, errors="strict")
            except UnicodeError:
                raise ValidationError("invalid encoded configuration identity") from None
            configuration_url(identity)
            result = self.margin_trading.read_configuration(identity)
            return {**result, "core_epoch": self.control.snapshot()["epoch"]}
        if method == "GET" and path.startswith(ledger_base) and path.endswith("/ledger"):
            from omega.runtime.ledger_client import ledger_url

            encoded = path[len(ledger_base) : -len("/ledger")]
            if "/" in encoded or re.search(r"%(?![a-fA-F0-9]{2})", encoded):
                raise ValidationError("invalid ledger identity segment")
            try:
                identity = unquote(encoded, errors="strict")
            except UnicodeError:
                raise ValidationError("invalid encoded ledger identity") from None
            if len(query) > 2048:
                raise ValidationError("ledger query exceeds limit")
            try:
                values = parse_qs(query, keep_blank_values=True, max_num_fields=6)
            except ValueError:
                raise ValidationError("too many ledger query fields") from None
            if any(len(items) != 1 for items in values.values()):
                raise ValidationError("duplicate ledger query field")
            arguments = {key: items[0] for key, items in values.items()}
            for key in ("offset", "limit", "start_ms", "end_ms"):
                if key in arguments:
                    raw = arguments[key]
                    if not re.fullmatch(r"[0-9]{1,15}", raw):
                        raise ValidationError("ledger bounds require unsigned UTC integers")
                    arguments[key] = int(raw)
            ledger_url(identity, arguments)  # Common typed bounds, unknown keys and identity.
            result = self.margin_trading.read_ledger(identity, **arguments)
            return {**result, "core_epoch": self.control.snapshot()["epoch"]}
        if query:
            raise ValidationError("margin trading does not accept query parameters")
        base = "/api/margin-trading"
        epoch = self.control.snapshot()["epoch"]
        if method == "GET" and path == base:
            result = self.margin_trading.snapshot()
        elif (
            method == "POST"
            and path == base
            and type(body) is dict
            and set(body) == {"id", "kind", "parameters", "core_epoch"}
        ):
            if body["core_epoch"] != epoch:
                raise ValidationError("margin operation belongs to another core epoch")
            if (
                body["kind"] == "retire"
                and getattr(self.margin_trading, "core_epoch", epoch) != epoch
            ):
                raise ValidationError("retirement owner belongs to another core epoch")
            result = self.margin_trading.submit(body["id"], body["kind"], body["parameters"])
        elif method == "GET" and path.startswith(base + "/") and "/" not in path[len(base) + 1 :]:
            result = self.margin_trading.status(path[len(base) + 1 :])
        else:
            raise ValidationError("invalid virtual margin operation")
        origin = (
            {"operation_core_epoch": result["core_epoch"]}
            if "id" in result and "core_epoch" in result
            else {}
        )
        return {
            **result,
            **origin,
            "core_epoch": epoch,
            "scope": "virtual_margin_v2_ohlcv_proxy",
        }

    def _depth_request(self, method, path, query, body):
        if query:
            raise ValidationError("depth does not accept query parameters")
        epoch = self.control.snapshot()["epoch"]
        if method == "GET" and path == "/api/depth":
            return {
                **self.depth_collection.snapshot(),
                "core_epoch": epoch,
                "scope": "observed_depth_only",
            }
        if method == "GET" and path.startswith("/api/depth/"):
            identity = path.removeprefix("/api/depth/")
            if identity not in self.depth_commands:
                raise ValidationError("depth command is not recorded in this core")
            return dict(self.depth_commands[identity]["result"])
        if (
            method != "POST"
            or path != "/api/depth"
            or type(body) is not dict
            or set(body) != {"id", "core_epoch", "expected_generation", "kind", "parameters"}
        ):
            raise ValidationError("invalid depth operation")
        identity = body["id"]
        if (
            type(identity) is not str
            or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", identity) is None
            or body["core_epoch"] != epoch
            or type(body["expected_generation"]) is not int
            or body["expected_generation"] < 0
        ):
            raise ValidationError("invalid depth identity, core epoch or generation")
        try:
            digest = json.dumps(body, sort_keys=True, allow_nan=False, separators=(",", ":"))
        except (ValueError, TypeError):
            raise ValidationError("depth command must contain finite JSON data") from None
        previous = self.depth_commands.get(identity)
        if previous is not None:
            if previous["digest"] != digest:
                raise ValidationError("depth command ID conflicts with its original request")
            return dict(previous["result"])
        if len(self.depth_commands) >= 128:
            raise ValidationError("depth command history is full; no automatic eviction")
        if body["expected_generation"] != self.depth_collection.snapshot()["generation"]:
            raise ValidationError("depth collection generation changed")
        parameters = body["parameters"]
        if type(parameters) is not dict:
            raise ValidationError("depth parameters must be an object")
        if (
            body["kind"] == "start"
            and set(parameters) == {"subscriptions", "resume"}
            and type(parameters["subscriptions"]) is list
            and type(parameters["resume"]) is bool
        ):
            self.depth_collection.start(parameters["subscriptions"], resume=parameters["resume"])
        elif body["kind"] == "stop" and not parameters:
            self.depth_collection.request_stop()
        else:
            raise ValidationError("unsupported explicit depth action")
        result = {
            "id": identity,
            "core_epoch": epoch,
            "state": "requested",
            "kind": body["kind"],
            "generation": self.depth_collection.snapshot()["generation"],
            "scope": "observed_depth_only",
        }
        self.depth_commands[identity] = {"digest": digest, "result": result}
        return dict(result)

    def _strategy_auxiliary_demand(self):
        demand, unsupported = {}, []
        plan = self.paper.feed_snapshot() if self.paper is not None else {}
        for instance, item in plan.get("analysis_inputs", {}).items():
            channels = set(item["channels"])
            if channels - {"trade_flow", "metric"} or (
                ":spot:" in item["instrument"] and "metric" in channels
            ):
                unsupported.append(
                    {
                        "instance": instance,
                        "instrument": item["instrument"],
                        "channels": sorted(channels),
                    }
                )
                continue
            demand.setdefault(item["instrument"], set()).update(channels)
        return (
            [
                {"instrument": key, "channels": sorted(channels)}
                for key, channels in sorted(demand.items())
            ],
            unsupported,
        )

    def _auxiliary_schedule_request(self, method, path, query, body):
        if query:
            raise ValidationError("auxiliary schedule does not accept query parameters")
        base = "/api/auxiliary-schedule"
        scheduler = self.auxiliary_schedule

        def snapshot():
            demand, unsupported = self._strategy_auxiliary_demand()
            return {
                **scheduler.snapshot(),
                "core_epoch": self.control.snapshot()["epoch"],
                "strategy_demand": demand,
                "unsupported_demand": unsupported,
            }

        if method == "GET" and path == base:
            return snapshot()
        if method != "POST" or type(body) is not dict:
            raise ValidationError("invalid auxiliary schedule operation")
        if path == base + "/configure":
            if not {"expected_revision"} <= body.keys() or body.keys() - {
                "expected_revision",
                "subscriptions",
                "from_strategies",
                "live_start_ms",
            }:
                raise ValidationError("invalid auxiliary schedule configuration")
            if ("subscriptions" in body) == ("from_strategies" in body):
                raise ValidationError("choose explicit subscriptions or current strategy demand")
            if "from_strategies" in body:
                if body["from_strategies"] is not True:
                    raise ValidationError("from_strategies requires explicit true")
                subscriptions, unsupported = self._strategy_auxiliary_demand()
                if unsupported:
                    raise ValidationError("some strategies require unsupported auxiliary sources")
            else:
                subscriptions = body["subscriptions"]
            scheduler.configure(
                subscriptions,
                expected_revision=body["expected_revision"],
                live_start_ms=body.get("live_start_ms"),
            )
            return snapshot()
        if path == base + "/stop" and set(body) == {"expected_generation"}:
            scheduler.stop(expected_generation=body["expected_generation"])
            return snapshot()
        if path == base + "/resume" and set(body) == {
            "instrument",
            "channel",
            "expected_generation",
        }:
            scheduler.resume(
                body["instrument"],
                body["channel"],
                expected_generation=body["expected_generation"],
            )
            return snapshot()
        raise ValidationError("invalid auxiliary schedule operation or generation fence")

    def _auxiliary_request(self, method, path, query, body):
        if query:
            raise ValidationError("auxiliary jobs do not accept query parameters")
        base = "/api/auxiliary-jobs"
        if path == base:
            if method == "GET":
                return {
                    **self.auxiliary_service.snapshot(),
                    "core_epoch": self.control.snapshot()["epoch"],
                }
            if (
                method == "POST"
                and type(body) is dict
                and set(body) == {"id", "kind", "parameters"}
            ):
                return self.auxiliary_service.submit(body["id"], body["kind"], body["parameters"])
            raise ValidationError("invalid auxiliary job submission")
        parts = path.removeprefix(base + "/").split("/")
        if len(parts) == 1 and method == "GET":
            return self.auxiliary_service.status(parts[0])
        if (
            len(parts) == 2
            and method == "POST"
            and parts[1] in {"cancel", "resume"}
            and type(body) is dict
            and set(body) == {"execution_id"}
            and type(body["execution_id"]) is str
            and re.fullmatch(r"[a-f0-9]{32}", body["execution_id"]) is not None
        ):
            action = (
                self.auxiliary_service.cancel
                if parts[1] == "cancel"
                else self.auxiliary_service.resume
            )
            return action(parts[0], expected_execution_id=body["execution_id"])
        raise ValidationError("invalid auxiliary job operation or execution fence")

    def job_parameters(self, kind, parameters):
        result = deepcopy(parameters)
        settings = self.settings.snapshot()
        simulation = settings["simulation"]
        result.setdefault("seed", simulation["seed"])
        result["settings_revision"] = settings["revision"]
        if kind == "simulate":
            spec = result.setdefault("spec", {})
            if not isinstance(spec, dict):
                raise ValidationError("simulation spec must be an object")
            spec.setdefault("backend", simulation["backend"])
            spec.setdefault("seed", simulation["seed"])
            routes = spec.get("routes", {})
            if not isinstance(routes, dict) or any(
                case not in {"bullish", "bearish", "doji"}
                or not isinstance(route, str)
                or route not in {"O-H-L-C", "O-L-H-C"}
                for case, route in routes.items()
            ):
                raise ValidationError("invalid simulation case routes")
            if "path" in spec:
                if not isinstance(spec["path"], str) or spec["path"] not in {
                    "random",
                    "O-H-L-C",
                    "O-L-H-C",
                }:
                    raise ValidationError("invalid simulation price path")
                if spec["path"] == "random" and routes:
                    raise ValidationError("random path cannot include deterministic case routes")
            elif routes:
                if set(routes) != {"bullish", "bearish", "doji"}:
                    raise ValidationError("partial case routes require an explicit fallback path")
                # All cases are explicit, so this baseline is never selected by candle_path.
                spec["path"] = "O-H-L-C"
            else:
                if simulation["intrabar"] == "conservative":
                    raise ValidationError(
                        "conservative fill engine is not implemented; select an explicit price path"
                    )
                spec["path"] = "random" if simulation["intrabar"] == "random" else "O-H-L-C"
        elif kind == "train":
            recording = result.setdefault("recording", {})
            if not isinstance(recording, dict):
                raise ValidationError("training recording policy must be an object")
            policy = settings["training"]
            recording.setdefault(
                "profile",
                {"light": "lightweight"}.get(policy["recording"], policy["recording"]),
            )
            recording.setdefault(
                "optional_detail_failure",
                "stop" if policy["optional_recording_failure"] == "stop" else "warn_continue",
            )
        return result

    def start(self, port=0):
        with self.owner_mutation_lock:
            if self._closing:
                raise ValidationError("core owner is stopping")
            return self._start(port)

    def _start(self, port):
        if self.server is not None:
            raise RuntimeError("core HTTP server already started")
        service = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "OmegaCore/1"

            def log_message(self, *_args):
                pass

            def do_GET(self):
                self._handle("GET")

            def do_POST(self):
                self._handle("POST")

            def _handle(self, method):
                stage = "headers"
                try:
                    if sum(len(key) + len(value) for key, value in self.headers.items()) > 16_384:
                        raise ValidationError("request headers exceed limit")
                    host = self.headers.get("Host", "")
                    origin = self.headers.get("Origin")
                    if (
                        len(self.headers.get_all("Host", [])) != 1
                        or len(self.headers.get_all("Origin", [])) > 1
                    ):
                        raise AuthorizationDenied("ambiguous request origin")
                    authorize_mobile_headers(
                        host,
                        origin,
                        port=self.server.server_port,
                        https_origin=service.settings.snapshot()["mobile"]["https_origin"],
                    )
                    if len(self.path) > 2048:
                        raise ValidationError("request target exceeds limit")
                    asset_path = urlsplit(self.path).path
                    if method == "GET" and asset_path.startswith("/mobile"):
                        from omega.mobile import load_asset

                        asset, content_type = load_asset(asset_path)
                        self._reply_bytes(200, asset, content_type, mobile=True)
                        return
                    if self.headers.get("Transfer-Encoding"):
                        raise ValidationError("transfer encoding unsupported")
                    lengths = self.headers.get_all("Content-Length", [])
                    if len(lengths) > 1:
                        raise ValidationError("duplicate request length")
                    length = int(lengths[0]) if lengths else 0
                    if not 0 <= length <= MAX_REQUEST_BYTES:
                        raise ValidationError("request exceeds limit")
                    body = None
                    if method == "POST":
                        if self.headers.get_content_type() != "application/json":
                            raise ValidationError("JSON content type required")
                        stage = "body"
                        raw = self.rfile.read(length)
                        if len(raw) != length:
                            raise ValidationError("incomplete request")
                        body = json.loads(
                            raw,
                            parse_constant=lambda _value: (_ for _ in ()).throw(
                                ValueError("nonfinite JSON")
                            ),
                        )
                    authorization = self.headers.get("Authorization", "")
                    token = (
                        authorization.removeprefix("Bearer ")
                        if authorization.startswith("Bearer ")
                        else ""
                    )
                    stage = "operation"
                    result = service.request(method, self.path, token, body)
                    stage = "response"
                    self._reply(200, result)
                except AuthorizationDenied:
                    self._reply(403, {"error": "PERMISSION_DENIED"})
                except (ValidationError, ValueError, TypeError, KeyError):
                    self._reply(400, {"error": "INVALID_REQUEST"})
                except LookupError:
                    self._reply(404, {"error": "NOT_FOUND"})
                except (OSError, TimeoutError) as error:
                    service.record_incident(
                        "CONTROL_TRANSPORT_OR_STORAGE_FAILURE",
                        context=incident_error_context(error, stage=stage, operation=method),
                    )
                    self._reply(503, {"error": "SERVICE_UNAVAILABLE"})
                except Exception:
                    service.log("system", "ERROR", "API_OPERATION_FAILED")
                    self._reply(500, {"error": "OPERATION_FAILED"})

            def _reply(self, status, result):
                payload = json.dumps(result, ensure_ascii=False, allow_nan=False).encode("utf-8")
                if len(payload) > MAX_RESPONSE_BYTES:
                    status, payload = 413, b'{"error":"RESPONSE_TOO_LARGE"}'
                self._reply_bytes(status, payload, "application/json; charset=utf-8")

            def _reply_bytes(self, status, payload, content_type, *, mobile=False):
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(payload)))
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("X-Content-Type-Options", "nosniff")
                    if mobile:
                        self.send_header(
                            "Content-Security-Policy",
                            "default-src 'none'; script-src 'self'; style-src 'self'; "
                            "connect-src 'self'; img-src 'self' data:; base-uri 'none'; "
                            "frame-ancestors 'none'; form-action 'none'; "
                            "manifest-src 'self'; worker-src 'self'",
                        )
                    self.send_header("Connection", "close")
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError, TimeoutError):
                    pass

        self.handler_class = Handler
        self.server = BoundedHTTPServer(("127.0.0.1", port), Handler)
        try:
            atomic_json(
                self.root / "state" / "core.json",
                {
                    "schema_version": 1,
                    "url": f"http://127.0.0.1:{self.server.server_port}",
                    "mode": "paper",
                    "token_path": "secrets/core_token",
                    "started_at_ms": int(self.started_at * 1000),
                },
            )
        except Exception:
            self.server.server_close()
            self.server = None
            raise
        self.thread = threading.Thread(
            target=self.server.serve_forever, name="omega-control", daemon=True
        )
        self.thread.start()
        self.notifications.start()
        if self.push_delivery is not None:
            try:
                self.push_delivery.start()
            except Exception:
                self.push_error = "WEBPUSH_START_UNAVAILABLE"
                try:
                    self.push_delivery.close()
                except Exception:
                    # Retain the owner reference: close() must confirm its retirement
                    # before the Core root lease may be released.
                    pass
        if self.leverage_reference is not None and self.refresh_leverage_on_start:
            self.leverage_reference.start()
        if self.paper_feed is not None:
            self.paper_feed.start()
        if self.auxiliary_schedule is not None:
            self.auxiliary_schedule.start()
        self._start_optional_shadow()
        self._start_optional_causal()
        if self.margin_trading is not None:
            self.margin_trading.start()
        if self.shutdown_management is not None:
            self.shutdown_management.start()
        return self.server.server_port

    def _start_optional_shadow(self):
        if self.shadow_inference is None:
            return
        try:
            self.shadow_inference.start()
        except Exception as error:
            # A damaged optional inference namespace must not prevent the
            # independent paper/data core from starting in its paused state.
            self.shadow_start_error = type(error).__name__
            self.record_incident("SHADOW_INFERENCE_UNAVAILABLE", "warning")
            self.log(
                "system",
                "WARNING",
                "SHADOW_START_UNAVAILABLE",
                failure_type=self.shadow_start_error,
            )

    def _start_optional_causal(self):
        if self.causal_inference is None:
            return
        try:
            self.causal_inference.start()
        except Exception as error:
            self.causal_start_error = type(error).__name__
            self.record_incident("CAUSAL_INFERENCE_UNAVAILABLE", "warning")
            self.log(
                "system",
                "WARNING",
                "CAUSAL_START_UNAVAILABLE",
                failure_type=self.causal_start_error,
            )

    def close(self):
        self._closing = True
        with self.owner_mutation_lock:
            if self._cleanup_complete:
                return
            if self._root_release_attempted:
                # Every child is already closed. Once released, this old core
                # must never write to a namespace a replacement may now own.
                if self._root_released():
                    self._cleanup_complete = True
                    return
                self._correct_root_exit()
                self._release_root()
                return
            self._close()

    def _root_released(self):
        try:
            released = self.instance.released
        except Exception:
            raise StorageError("core root lease observation is unavailable") from None
        if type(released) is not bool:
            raise StorageError("core root lease observation is unknown")
        return released

    def _correct_root_exit(self):
        if self._exit_completion_guard is None:
            return
        if self._root_released():
            raise StorageError("a released core cannot amend its exit receipt")
        correct = getattr(self.shutdown_management, "fail_root_release", None)
        if not callable(correct):
            raise StorageError("root release failure recording is unavailable")
        receipt = correct(
            self._exit_completion_guard,
            root_lease_held=lambda: self._root_released() is False,
        )
        if (
            type(receipt) is not dict
            or receipt.get("id") != self._exit_completion_guard.get("operation_id")
            or receipt.get("core_epoch") != self._exit_completion_guard.get("core_epoch")
            or receipt.get("state") != "failed"
            or receipt.get("error") != "root_lease_release_unconfirmed"
            or type(receipt.get("result")) is not dict
            or receipt["result"].get("core_exit_confirmed") is not False
        ):
            raise StorageError("root release failure recording is not confirmed")
        self._exit_completion_guard = None
        self._root_exit_correction_error = None

    def _release_root(self):
        self._root_release_attempted = True
        try:
            self.instance.close()
            if not self._root_released():
                raise StorageError("core root lease is still held")
        except Exception:
            try:
                # Before release, compensate only the guarded receipt from this
                # close. After release, observation only: another core may exist.
                if not self._root_released():
                    self._correct_root_exit()
            except Exception as correction_error:
                self._root_exit_correction_error = type(correction_error).__name__
            raise
        self._cleanup_complete = True

    def _close_http(self):
        server = self.server
        if server is None:
            return
        started = self.thread is not None and self.thread.ident is not None
        if self._http_cleanup_stage < 1:
            # BaseServer.shutdown waits for serve_forever; it must not be
            # invoked when listener-thread creation failed before any start.
            if started and self.thread.is_alive():
                server.shutdown()
            self._http_cleanup_stage = 1
        if self._http_cleanup_stage < 2:
            server.server_close()
            self._http_cleanup_stage = 2
        if self._http_cleanup_stage < 3:
            if started:
                self.thread.join(timeout=5)
                if self.thread.is_alive() is not False:
                    raise StorageError("core HTTP listener cleanup is not confirmed")
            self._http_cleanup_stage = 3
        # Failed phases keep the exact server/thread for explicit retry.
        self.server = None

    def _close(self):
        failures = []

        def attempt(action):
            try:
                action()
            except Exception as error:
                failures.append(error)

        if self.shutdown_management is not None:
            attempt(self.shutdown_management.close)
        if self.auxiliary_schedule is not None:
            attempt(self.auxiliary_schedule.close)
        if self.shadow_inference is not None:

            def close_shadow():
                status = self.shadow_inference.close()
                if isinstance(status, dict) and (
                    status.get("closed") is not True
                    or status.get("durable") is not True
                    or status.get("worker_alive", False) is not False
                ):
                    raise StorageError("shadow shutdown recording or cleanup is not confirmed")

            attempt(close_shadow)
        if self.causal_inference is not None:

            def close_causal():
                try:
                    can_preserve = getattr(
                        self.causal_inference,
                        "can_close_preserving_cold_namespace",
                        None,
                    )
                    preserve = getattr(
                        self.causal_inference, "close_preserving_cold_namespace", None
                    )
                    preserving = callable(can_preserve) and can_preserve() is True
                    if preserving and not callable(preserve):
                        raise StorageError("cold namespace preservation port is unavailable")
                    status = preserve() if preserving else self.causal_inference.close()
                    if (
                        type(status) is not dict
                        or status.get("closed") is not True
                        or status.get("worker_alive") is not False
                        or status.get("actor_alive") is not False
                        or not preserving
                        and status.get("durable") is not True
                        or preserving
                        and (
                            status.get("durable") is not False
                            or status.get("namespace_preserved") is not True
                            or status.get("namespace_clean") is not False
                            or status.get("current_physical_cleanup_confirmed") is not True
                            or status.get("current_required_recording_state") != "none_attempted"
                            or status.get("old_resource_close_claimed") is not False
                        )
                    ):
                        raise StorageError("causal inference shutdown is not confirmed")
                except Exception:
                    # close() holds the exclusive core fence and control is not
                    # retired yet. Preserve this failure even while the regular
                    # late-callback diagnostic entry point is intentionally off.
                    for report in (
                        lambda: self.control.incident(
                            "CAUSAL_SHUTDOWN_UNCONFIRMED",
                            "critical",
                            invalidate_resume=False,
                        ),
                        lambda: self.logger.write(
                            "system", "CRITICAL", "CAUSAL_SHUTDOWN_UNCONFIRMED"
                        ),
                    ):
                        try:
                            report()
                        except Exception:
                            pass  # Do not replace the original cleanup failure.
                    raise

            attempt(close_causal)
        if self.margin_trading is not None:
            attempt(self.margin_trading.close)
        if self.depth_collection is not None:
            attempt(self.depth_collection.close)

        if self.auxiliary_service is not None:
            try:
                status = self.auxiliary_service.close()
                if not status.get("durable", False):
                    attempt(
                        lambda: self.control.incident("AUXILIARY_RECORDING_UNAVAILABLE", "critical")
                    )
                    attempt(
                        lambda: self.logger.write(
                            "system", "CRITICAL", "AUXILIARY_SHUTDOWN_UNRECORDED"
                        )
                    )
                    failures.append(StorageError("auxiliary shutdown could not be recorded"))
            except Exception as error:
                failures.append(error)
        if self.push_delivery is not None:
            attempt(self.push_delivery.close)
        self.control.retire()

        attempt(self.notifications.close)
        if self.server:
            attempt(self._close_http)
        for resource in (
            self.chart_series,
            self.chart_observations,
            self.analysis_execution,
            self.paper_feed,
            self.leverage_reference,
            self.market_data,
            self.research,
        ):
            if resource is not None:
                attempt(resource.close)
        if self.shutdown_management is not None:

            def finish_exit():
                guard = self.shutdown_management.finish_exit(successful=not failures)
                if not failures and type(guard) is dict:
                    self._exit_completion_guard = guard

            attempt(finish_exit)
        # Keep the root lease while any child cleanup or durable exit ACK is
        # unconfirmed. A later explicit close retries the same providers; a
        # second core must not enter this partially closed namespace meanwhile.
        if not failures:
            self._release_root()
        if failures:
            raise failures[0]


class BoundedHTTPServer(HTTPServer):
    allow_reuse_address = False
    request_queue_size = 8

    def __init__(self, *args, **kwargs):
        self.slots = threading.BoundedSemaphore(8)
        self.workers = set()
        self.workers_lock = threading.Lock()
        super().__init__(*args, **kwargs)

    def server_bind(self):
        # HTTPServer's reverse hostname lookup is unnecessary for a loopback service.
        socket.socket.bind(self.socket, self.server_address)
        self.server_name = "127.0.0.1"
        self.server_port = self.socket.getsockname()[1]

    def process_request(self, request, client_address):
        self._reap_workers()
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        worker = None
        try:
            request.settimeout(5)
            worker = threading.Thread(
                target=self._worker, args=(request, client_address), daemon=True
            )
            with self.workers_lock:
                self.workers.add(worker)
            worker.start()
        except Exception:
            if worker is None or worker.ident is None:
                with self.workers_lock:
                    self.workers.discard(worker)
                try:
                    self.shutdown_request(request)
                finally:
                    self.slots.release()
            raise

    def _worker(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except (OSError, TimeoutError):
            pass
        finally:
            self.shutdown_request(request)

    def _reap_workers(self):
        # A worker cannot confirm its own native thread has exited. Keep its
        # reference and admission permit until another thread observes that fact.
        with self.workers_lock:
            finished = [worker for worker in self.workers if worker.is_alive() is False]
            for worker in finished:
                self.workers.remove(worker)
                self.slots.release()

    def server_close(self):
        super().server_close()
        with self.workers_lock:
            workers = tuple(self.workers)
        deadline = time.monotonic() + 6
        for worker in workers:
            worker.join(timeout=max(0, deadline - time.monotonic()))
        self._reap_workers()
        if any(worker.is_alive() is not False for worker in workers):
            raise StorageError("core HTTP request worker cleanup is not confirmed")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Omega independent paper control core")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error("port outside supported range")
    service = CoreService(args.root)
    stopped = threading.Event()
    for name in ("SIGINT", "SIGTERM"):
        signal.signal(getattr(signal, name), lambda *_args: stopped.set())
    try:
        port = service.start(args.port)
        print(f"Omega paper core: http://127.0.0.1:{port} (GUI may reconnect independently)")
        stopped.wait()
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
