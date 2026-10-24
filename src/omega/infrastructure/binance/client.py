"""Authentication and error semantics shared by Spot and USD-M, not trading rules."""

import hashlib
import hmac
import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from urllib.parse import urlencode, urlsplit

from omega.domain.errors import (
    BrokerRejected,
    OrderOutcomeUnknown,
    ProtocolError,
    RateLimited,
    TradingBlocked,
    TransportError,
    ValidationError,
)
from omega.infrastructure.binance.budget import RequestBudget
from omega.infrastructure.binance.transport import HttpRequest, HttpTransport

ALLOWED_HOSTS = frozenset({"api.binance.com", "fapi.binance.com"})


@dataclass(frozen=True)
class Credentials:
    api_key: str = field(repr=False)
    secret: str = field(repr=False)

    def __post_init__(self):
        if not self.api_key or not self.secret or any(c in self.api_key for c in "\r\n"):
            raise ValidationError("invalid HMAC credentials")


class BinanceClient:
    def __init__(
        self,
        base_url: str,
        transport: HttpTransport,
        *,
        credentials: Credentials | None = None,
        enable_trading: bool = False,
        budget: RequestBudget | None = None,
        ip_scope: str = "binance-ip",
        order_scope: str = "binance-account-orders",
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        recv_window_ms: int = 5000,
    ):
        parsed = urlsplit(base_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname not in ALLOWED_HOSTS
            or parsed.port not in (None, 443)
            or parsed.path not in ("", "/")
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValidationError("unsupported Binance HTTPS origin")
        if not 1 <= recv_window_ms <= 60_000:
            raise ValidationError("invalid receive window")
        self.base_url = base_url.rstrip("/")
        self.transport = transport
        self.credentials = credentials
        self.enable_trading = enable_trading
        self.budget = budget
        self.ip_scope = ip_scope
        self.order_scope = order_scope
        self.clock_ms = clock_ms
        self.recv_window_ms = recv_window_ms

    def request(
        self,
        method: str,
        path: str,
        params: Mapping[str, object] | None = None,
        *,
        signed: bool = False,
        weight: int = 1,
        order: bool = False,
    ):
        if method not in {"GET", "POST", "DELETE"} or not path.startswith("/"):
            raise ValidationError("invalid request route")
        mutation = method != "GET"
        if mutation and not self.enable_trading:
            raise TradingBlocked("broker mutations are disabled by default")
        if mutation and self.budget is None:
            raise TradingBlocked("provider rate budgets required before broker mutations")
        values = dict(params or {})
        if {"signature", "timestamp", "recvWindow"} & values.keys():
            raise ValidationError("authentication fields are owned by the client")
        headers = {}
        if signed:
            if self.credentials is None:
                raise TradingBlocked("credentials required for private requests")
            headers["X-MBX-APIKEY"] = self.credentials.api_key
            values.update(timestamp=self.clock_ms(), recvWindow=self.recv_window_ms)
        wire = urlencode(sorted((k, self._wire(v)) for k, v in values.items()))
        if signed:
            signature = hmac.new(
                self.credentials.secret.encode(), wire.encode(), hashlib.sha256
            ).hexdigest()
            wire += "&signature=" + signature
        url = self.base_url + path
        body = None
        if method == "POST":
            body = wire.encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif wire:
            url += "?" + wire
        if self.budget is not None:
            costs = {self.ip_scope: weight}
            if order:
                costs[self.order_scope] = 1
            self.budget.reserve(costs)
        try:
            response = self.transport.send(HttpRequest(method, url, headers, body))
        except TransportError:
            if mutation:
                raise OrderOutcomeUnknown("broker mutation outcome unknown; reconcile") from None
            raise
        if self.budget:
            for header, value in response.headers.items():
                match = re.fullmatch(
                    r"x-mbx-(used-weight|order-count)-(\d+)([smhd])", header.lower()
                )
                if match:
                    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
                    seconds = int(match[2]) * units[match[3]]
                    scope = self.ip_scope if match[1] == "used-weight" else self.order_scope
                    try:
                        self.budget.observe_usage(scope, seconds, int(value))
                    except (TypeError, ValueError):
                        pass  # Malformed headers never reset an existing reservation.
        try:
            payload = json.loads(response.body, parse_float=Decimal)
        except (ValueError, UnicodeError):
            if mutation:
                raise OrderOutcomeUnknown("unreadable mutation response; reconcile") from None
            raise ProtocolError("unreadable provider response") from None
        code = payload.get("code", 0) if isinstance(payload, dict) else 0
        if mutation and (
            response.status >= 500 or (isinstance(code, int) and code in {-1006, -1007})
        ):
            raise OrderOutcomeUnknown("broker mutation outcome unknown; reconcile")
        if response.status in {418, 429}:
            retry = next(
                (v for k, v in response.headers.items() if k.lower() == "retry-after"), None
            )
            try:
                delay = max(0.0, float(retry)) if retry is not None else None
            except (TypeError, ValueError):
                delay = None
            if self.budget:
                self.budget.cooldown(delay if delay is not None else 60)
            raise RateLimited(delay)
        if not 200 <= response.status < 300 or (isinstance(code, int) and code < 0):
            raise BrokerRejected(code if isinstance(code, int) else 0, response.status)
        return payload

    @staticmethod
    def _wire(value: object) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, Decimal):
            if not value.is_finite():
                raise ValidationError("non-finite request value")
            return format(value, "f")
        if isinstance(value, (str, int)):
            return str(value)
        raise ValidationError("unsupported request value type")
