"""One bounded startup refresh; missing authorization never becomes fake fresh data."""

import json
import threading
import time
from copy import deepcopy
from pathlib import Path

from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import is_valid_symbol
from omega.infrastructure.binance.leverage import account_scope, validate_envelope
from omega.runtime.settings import atomic_json


class LeverageReferenceService:
    def __init__(
        self,
        root: Path,
        *,
        provider=None,
        account_id="unconfigured",
        max_age_ms=86400000,
        clock_ms=None,
    ):
        if type(max_age_ms) is not int or not 1000 <= max_age_ms <= 30 * 86400000:
            raise ValidationError("invalid leverage reference freshness bound")
        self.account_id, self.provider = account_scope(account_id), provider
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.max_age_ms = max_age_ms
        root = Path(root).resolve()
        self.path = root / "state" / "reference" / f"leverage-{self.account_id}.json"
        if not self.path.resolve().is_relative_to(root):
            raise ValidationError("leverage reference cache escapes project root")
        self.lock, self.stopped = threading.RLock(), threading.Event()
        self.thread, self.started, self.cached = None, False, None
        self.state, self.error, self.attempt_ms = "not_started", None, None
        if self.path.exists():
            try:
                if self.path.stat().st_size > 8_388_608:
                    raise ValueError("cache capacity")
                data = json.loads(self.path.read_text(encoding="utf-8"))
                if (
                    not isinstance(data, dict)
                    or set(data) != {"fetched_at_ms", "reference"}
                    or type(data["fetched_at_ms"]) is not int
                    or not 0 <= data["fetched_at_ms"] < 2**63
                ):
                    raise ValueError("invalid cache")
                self.cached = {
                    "fetched_at_ms": data["fetched_at_ms"],
                    "reference": validate_envelope(data["reference"], self.account_id),
                }
            except Exception:
                self.error = "CACHE_INVALID"

    def status(self):
        with self.lock:
            stamp = self.cached["fetched_at_ms"] if self.cached else None
            age = self.clock_ms() - stamp if stamp is not None else None
            freshness = (
                "missing"
                if age is None
                else "clock_skew"
                if age < 0
                else "fresh"
                if age <= self.max_age_ms
                else "stale"
            )
            return {
                "schema_version": 1,
                "state": self.state,
                "account_id": self.account_id,
                "cached": self.cached is not None,
                "source": self.cached["reference"]["source"] if self.cached else None,
                "fetched_at_ms": stamp,
                "age_ms": age,
                "freshness": freshness,
                "last_attempt_ms": self.attempt_ms,
                "error": self.error,
                "refresh_authorized": self.provider is not None,
                "reference_only": True,
                "trading_gate_connected": False,
            }

    def table(self, symbol=None):
        if symbol is not None and not is_valid_symbol(symbol):
            raise ValidationError("invalid leverage reference symbol")
        with self.lock:
            reference = deepcopy(self.cached["reference"]) if self.cached else None
            if reference is not None and symbol is not None:
                reference["table"] = (
                    {symbol: reference["table"][symbol]} if symbol in reference["table"] else {}
                )
            return {"status": self.status(), "reference": reference}

    def start(self):
        with self.lock:
            if self.started or self.stopped.is_set():
                raise ValidationError("leverage reference service already started or closed")
            self.started = True
            if self.provider is None:
                self.state = "auth_required"
                return
            self.state, self.attempt_ms = "refreshing", self.clock_ms()
            self.thread = threading.Thread(
                target=self._refresh, name="omega-leverage-reference", daemon=True
            )
            self.thread.start()

    def _refresh(self):
        try:
            reference = validate_envelope(self.provider.fetch(), self.account_id)
            candidate = {"fetched_at_ms": self.clock_ms(), "reference": reference}
            if (
                type(candidate["fetched_at_ms"]) is not int
                or not 0 <= candidate["fetched_at_ms"] < 2**63
            ):
                raise ValidationError("invalid leverage reference timestamp")
            if len(json.dumps(candidate, allow_nan=False).encode("utf-8")) > 8_388_608:
                raise ValidationError("leverage reference exceeds cache capacity")
            with self.lock:
                if self.stopped.is_set():
                    return
                if self.cached and candidate["fetched_at_ms"] < self.cached["fetched_at_ms"]:
                    raise ValidationError("clock before prior reference")
                atomic_json(self.path, candidate)
                self.cached, self.state, self.error = candidate, "ready", None
        except Exception as exc:
            with self.lock:
                if not self.stopped.is_set():
                    self.state, self.error = "failed", type(exc).__name__

    def close(self):
        self.stopped.set()
        if self.thread:
            self.thread.join(timeout=12)
            if self.thread.is_alive():
                raise StorageError("leverage reference provider is still stopping")
        with self.lock:
            self.state = "closed"
