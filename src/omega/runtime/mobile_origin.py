"""One explicit trusted HTTPS proxy origin; the core remains bound to loopback."""

import ipaddress
import re
from urllib.parse import urlsplit

from omega.domain.errors import AuthorizationDenied, ValidationError


def canonical_mobile_origin(value):
    """Empty disables access; reject rather than silently normalize an origin."""
    if type(value) is not str or len(value) > 512:
        raise ValidationError("mobile origin must be an empty or canonical HTTPS origin")
    if value == "":
        return value
    if not value.isascii() or any(c.isspace() or ord(c) < 32 for c in value):
        raise ValidationError("mobile origin must use canonical ASCII without whitespace")
    if any(c in value for c in "\\%*?#@"):
        raise ValidationError("mobile origin cannot contain credentials, escapes or URL suffixes")
    try:
        parsed = urlsplit(value)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        raise ValidationError("invalid mobile HTTPS origin") from None
    if parsed.scheme != "https" or not host or parsed.path or parsed.query or parsed.fragment:
        raise ValidationError("mobile origin requires HTTPS without a path/query/fragment")
    if ":" in host or host.endswith(".") or len(host) > 253:
        raise ValidationError("mobile origin requires a canonical DNS name or IPv4 address")
    if all(part.isdigit() for part in host.split(".")):
        try:
            canonical_ip = str(ipaddress.IPv4Address(host))
        except ValueError:
            raise ValidationError("invalid canonical mobile IPv4 address") from None
        if canonical_ip != host:
            raise ValidationError("invalid canonical mobile IPv4 address")
    elif any(
        not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", part) for part in host.split(".")
    ):
        raise ValidationError("invalid canonical mobile DNS name")
    if port is not None and (not 1 <= port <= 65535 or port == 443):
        raise ValidationError("omit default HTTPS port443; other ports must be 1..65535")
    expected = "https://" + host + (":" + str(port) if port is not None else "")
    if value != expected:
        raise ValidationError("mobile origin must already be canonical")
    return value


def authorize_mobile_headers(host, origin, *, port, https_origin=""):
    """Use actual Host/Origin only; forwarded headers never confer authority."""
    configured = canonical_mobile_origin(https_origin)
    local = {f"127.0.0.1:{port}", f"localhost:{port}"}
    allowed_hosts = local | ({urlsplit(configured).netloc} if configured else set())
    allowed_origins = {"http://" + value for value in local}
    if configured:
        allowed_origins.add(configured)
    if type(host) is not str or host not in allowed_hosts:
        raise AuthorizationDenied("explicit local or configured mobile Host required")
    if origin is not None and (type(origin) is not str or origin not in allowed_origins):
        raise AuthorizationDenied("cross-origin request denied")
