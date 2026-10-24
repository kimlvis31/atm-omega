"""Bounded HTTPS transport without automatic retries or credential redirects."""

from dataclasses import dataclass, field
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from omega.domain.errors import TransportError


@dataclass(frozen=True)
class HttpRequest:
    method: str
    url: str = field(repr=False)
    headers: dict[str, str] = field(default_factory=dict, repr=False)
    body: bytes | None = field(default=None, repr=False)


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: bytes = field(repr=False)
    headers: dict[str, str] = field(default_factory=dict)


class HttpTransport(Protocol):
    def send(self, request: HttpRequest) -> HttpResponse: ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class UrllibTransport:
    def __init__(self, *, timeout_seconds: float = 10, max_response_bytes: int = 8_388_608):
        if timeout_seconds <= 0 or max_response_bytes <= 0:
            raise ValueError("positive transport limits required")
        self.timeout = timeout_seconds
        self.max_bytes = max_response_bytes
        self._opener = build_opener(_NoRedirect())

    def send(self, request: HttpRequest) -> HttpResponse:
        req = Request(
            request.url, data=request.body, headers=request.headers, method=request.method
        )
        try:
            response = self._opener.open(req, timeout=self.timeout)
        except HTTPError as exc:
            response = exc
        except (URLError, TimeoutError, OSError):
            raise TransportError("HTTPS request failed; request details redacted") from None
        try:
            with response:
                body = response.read(self.max_bytes + 1)
                if len(body) > self.max_bytes:
                    raise TransportError("provider response exceeded configured limit")
                return HttpResponse(response.code, body, dict(response.headers.items()))
        except (TimeoutError, OSError):
            raise TransportError("HTTPS response failed; request details redacted") from None
