"""Optional encryption adapter and exact-address HTTPS transport for browser push."""

import base64
import ipaddress
import os
import re
import socket
import ssl
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

from omega.domain.errors import StorageError, ValidationError

PROVIDERS = {
    "fcm.googleapis.com": r"/fcm/send/[A-Za-z0-9_:-]{1,1900}",
    "updates.push.services.mozilla.com": r"/(?:wpush/v2|push/v1)/[A-Za-z0-9_-]{1,1900}",
    "web.push.apple.com": r"/[A-Za-z0-9_-]{1,1900}",
}


def validate_endpoint(value):
    if type(value) is not str or not 1 <= len(value) <= 2048 or not value.isascii():
        raise ValidationError("invalid push endpoint")
    try:
        url = urlsplit(value)
        pattern = PROVIDERS.get(url.hostname)
        valid = (
            url.scheme == "https"
            and pattern is not None
            and url.netloc == url.hostname
            and not url.query
            and not url.fragment
            and "?" not in value
            and "#" not in value
            and re.fullmatch(pattern, url.path) is not None
            and value == "https://" + url.hostname + url.path
        )
    except ValueError:
        valid = False
    if not valid:
        raise ValidationError("push provider endpoint is not approved")
    return url


def _resolve_public_host(host, cell, completed):
    """Rootless DNS target: only a public hostname and dedicated result cell/Event."""
    try:
        cell.append(socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM))
    except Exception:
        cell.append(None)
    finally:
        completed.set()


class BoundedResolver:
    """One rootless OS lookup may outlive a deadline; it can never open a socket."""

    def __init__(self):
        self.thread = None
        self.closed = False
        self.lock = threading.Lock()

    def resolve(self, host, deadline):
        with self.lock:
            if self.closed or self.thread is not None and self.thread.is_alive():
                raise TimeoutError("push DNS slot unavailable")
            cell, completed = [], threading.Event()
            self.thread = threading.Thread(
                target=_resolve_public_host,
                args=(host, cell, completed),
                name="omega-push-rootless-dns",
                daemon=True,
            )
            self.thread.start()
        if not completed.wait(max(0, deadline - time.monotonic())):
            raise TimeoutError("push DNS deadline exceeded")
        with self.lock:
            if self.closed:
                raise TimeoutError("push DNS owner closed")
        rows = cell[0]
        if type(rows) is not list or not 1 <= len(rows) <= 32:
            raise ValueError("invalid push DNS result")
        approved = []
        for family, kind, protocol, _name, address in rows:
            if family not in {socket.AF_INET, socket.AF_INET6} or kind != socket.SOCK_STREAM:
                raise ValueError("invalid push DNS address family")
            ip = ipaddress.ip_address(address[0])
            if (
                not ip.is_global
                or ip.is_multicast
                or ip.is_unspecified
                or protocol not in {0, socket.IPPROTO_TCP}
                or ip.version != (4 if family == socket.AF_INET else 6)
                or getattr(ip, "ipv4_mapped", None) is not None
                or family == socket.AF_INET6
                and address[3] != 0
                or address[1] != 443
            ):
                raise ValueError("push DNS includes a non-public destination")
            approved.append((family, kind, protocol, address))
        return approved

    def close(self):
        with self.lock:
            self.closed = True
            return {"rootless_dns_alive": self.thread is not None and self.thread.is_alive()}


class PinnedSession:
    """The pywebpush supplied-session hook. Never follows redirects or reads proxies."""

    def __init__(self, *, resolver=None, context=None):
        self.resolver = resolver or BoundedResolver()
        self.context = context or ssl.create_default_context()
        if not self.context.check_hostname or self.context.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("verified push TLS context required")
        self.lock, self.sock, self.closed = threading.RLock(), None, False
        self._owner_changed = threading.Condition(self.lock)
        self._closing = threading.Event()
        self._owners = []
        self._attempt_active = False
        self._attempt_thread = None
        self._retirement_fault = False
        self.authorize = lambda: False

    def _require_dispatch(self):
        if self._retirement_fault:
            raise StorageError("push original transport retirement failed")
        if self.closed or self._closing.is_set():
            raise TimeoutError("push transport closed")

    def _own(self, sock):
        # Called under the owner lock. Preserve each exact object, including a
        # detached raw socket and the TLS object which took ownership from it.
        if not any(owner["socket"] is sock for owner in self._owners):
            self._owners.append({"socket": sock, "close_ack": False, "close_failed": False})
        self.sock = sock

    def _retire_owned(self, *, interrupt=False):
        # Called under the owner lock; socket.close does not perform TLS unwrap.
        # Every original failure is sticky even if retrying later appears to close.
        for owner in reversed(self._owners):
            if owner["close_ack"]:
                continue
            sock = owner["socket"]
            if interrupt:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                except Exception:
                    self._retirement_fault = True
            try:
                sock.close()
            except Exception:
                owner["close_failed"] = True
                self._retirement_fault = True
            else:
                owner["close_ack"] = True

    def _begin_attempt(self):
        with self.lock:
            self._require_dispatch()
            if self._attempt_active or any(not owner["close_ack"] for owner in self._owners):
                raise StorageError("push previous transport owner is not retired")
            # Discard only positively retired successful owners, never a failed owner.
            self._owners = []
            self.sock = None
            self._attempt_active = True
            self._attempt_thread = threading.get_ident()

    def _finish_attempt(self):
        with self._owner_changed:
            self._retire_owned()
            self._attempt_active = False
            self._attempt_thread = None
            self._owner_changed.notify_all()
            if self._retirement_fault:
                raise StorageError("push original transport retirement failed")

    @staticmethod
    def _remaining(deadline):
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError("push attempt deadline exceeded")
        return left

    def post(self, endpoint, *, data, headers, timeout):
        url = validate_endpoint(endpoint)
        if type(data) is not bytes or not 1 <= len(data) <= 4096:
            raise ValueError("push encrypted payload exceeds limit")
        if type(timeout) not in {float, int} or not 0 < timeout <= 5:
            raise ValueError("bounded push timeout required")
        self._begin_attempt()
        deadline = time.monotonic() + timeout
        try:
            approved = self.resolver.resolve(url.hostname, deadline)
            # One exact numeric sockaddr only; no second resolver or hidden retry.
            family, kind, protocol, address = approved[0]
            with self.lock:
                self._require_dispatch()
                raw = socket.socket(family, kind, protocol)
                self._own(raw)
            raw.settimeout(self._remaining(deadline))
            raw.connect(address)
            if ipaddress.ip_address(raw.getpeername()[0]) != ipaddress.ip_address(address[0]):
                raise ValueError("push connected address mismatch")
            raw.settimeout(self._remaining(deadline))
            with self.lock:
                self._require_dispatch()
                try:
                    # The non-handshaking ownership transfer and TLS publication are
                    # one locked transition. No network handshake holds this lock.
                    active = self.context.wrap_socket(
                        raw, server_hostname=url.hostname, do_handshake_on_connect=False
                    )
                except Exception:
                    # A failed handoff did not return an actual TLS owner reference.
                    # Do not infer physical retirement from the old raw object's state.
                    self._retirement_fault = True
                    raise
                self._own(active)
                self._require_dispatch()
            active.settimeout(self._remaining(deadline))
            active.do_handshake()
            with self.lock:
                self._require_dispatch()
            # Linearization point: a revoke before this check prevents HTTP submission.
            # A later revocation cannot recall bytes already submitted remotely.
            if not self.authorize():
                raise PermissionError("push subscription no longer authorized")
            allowed = {"authorization", "content-encoding", "crypto-key", "encryption", "ttl"}
            lines = [
                f"POST {url.path} HTTP/1.1",
                f"Host: {url.hostname}",
                "Connection: close",
                "Content-Type: application/octet-stream",
                f"Content-Length: {len(data)}",
            ]
            for key, value in headers.items():
                if type(key) is not str or key.lower() not in allowed:
                    raise ValueError("unexpected push transport header")
                value = str(value)
                if (
                    not value.isascii()
                    or not 1 <= len(value) <= 4096
                    or any(ord(char) < 32 or ord(char) == 127 for char in value)
                ):
                    raise ValueError("invalid push transport header")
                lines.append(key + ": " + value)
            packet = ("\r\n".join(lines) + "\r\n\r\n").encode("ascii") + data
            if len(packet) > 16_384:
                raise ValueError("push request exceeds limit")
            active.settimeout(self._remaining(deadline))
            active.sendall(packet)
            response = bytearray()
            while b"\r\n\r\n" not in response:
                active.settimeout(self._remaining(deadline))
                chunk = active.recv(min(1024, 8192 - len(response)))
                if not chunk or len(response) + len(chunk) >= 8192:
                    raise ValueError("push response headers unavailable or too large")
                response.extend(chunk)
            status_line = bytes(response).split(b"\r\n", 1)[0]
            match = re.fullmatch(rb"HTTP/1\.[01] ([1-5][0-9]{2})(?: [\x20-\x7e]*)?", status_line)
            if match is None:
                raise ValueError("invalid push HTTP response")
            # Never expose provider bodies, locations, endpoint or credentials to logs.
            return SimpleNamespace(status_code=int(match[1]), reason="", text="", headers={})
        finally:
            self._finish_attempt()

    def close(self):
        # Publish stop intent even if the ownership handoff currently holds the
        # lock. A late TLS object must be registered and retired, never dispatched.
        self._closing.set()
        dns = self.resolver.close()
        deadline = time.monotonic() + 5
        if not self.lock.acquire(timeout=self._remaining(deadline)):
            raise StorageError("push transport ownership transition is unconfirmed")
        try:
            self.closed = True
            self._retire_owned(interrupt=True)
            if self._attempt_active and self._attempt_thread == threading.get_ident():
                raise StorageError("push attempt cannot confirm its own retirement")
            while self._attempt_active:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise StorageError("push active transport retirement is unconfirmed")
                self._owner_changed.wait(timeout=remaining)
                self._retire_owned(interrupt=True)
            if self._retirement_fault or any(not owner["close_ack"] for owner in self._owners):
                raise StorageError("push original transport retirement failed")
            return dns
        finally:
            self.lock.release()


def load_vapid(root):
    """Called only by explicit owner opt-in; no crypto import or key creation when off."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec

    path = Path(root) / "secrets" / "vapid-private.pem"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink() or path.parent.is_junction() or path.is_symlink():
        raise ValidationError("push private key path is redirected")
    if not path.exists():
        key = ec.generate_private_key(ec.SECP256R1())
        encoded = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
    if path.stat().st_size > 4096 or os.name != "nt" and path.stat().st_mode & 0o077:
        raise ValidationError("push private key protection or size invalid")
    key = serialization.load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(key, ec.EllipticCurvePrivateKey) or not isinstance(key.curve, ec.SECP256R1):
        raise ValidationError("push private key must use P-256")
    public = key.public_key().public_bytes(
        serialization.Encoding.X962,
        serialization.PublicFormat.UncompressedPoint,
    )
    return key, base64.urlsafe_b64encode(public).rstrip(b"=").decode("ascii")


def validate_subject(subject):
    if type(subject) is not str or not 1 <= len(subject) <= 512 or not subject.isascii():
        raise ValidationError("owner push contact HTTPS host or mailto URI required")
    host_contact = re.fullmatch(r"https://[a-z0-9]+(?:[.-][a-z0-9]+)+", subject)
    email_contact = re.fullmatch(r"mailto:[A-Za-z0-9.!+_-]+@[a-z0-9]+(?:[.-][a-z0-9]+)+", subject)
    if not host_contact and not email_contact:
        raise ValidationError("owner push contact must be an HTTPS host or mailto URI")
    return subject


class WebPushSender:
    def __init__(self, root, *, subject="", session=None):
        subject = validate_subject(subject)
        # Lazy optional dependency; explicit enable fails clearly before any registration.
        from py_vapid import Vapid
        from pywebpush import WebPushException, webpush

        key, self.public_key = load_vapid(root)
        self.vapid, self.subject = Vapid(private_key=key), subject
        self.webpush, self.error = webpush, WebPushException
        self.session = session or PinnedSession()

    def send(self, subscription, payload, ttl, authorize):
        url = validate_endpoint(subscription["endpoint"])
        self.session.authorize = authorize
        try:
            response = self.webpush(
                subscription_info=subscription,
                data=payload,
                vapid_private_key=self.vapid,
                vapid_claims={
                    "sub": self.subject,
                    "aud": "https://" + url.hostname,
                    "exp": int(time.time()) + 3600,
                },
                content_encoding="aes128gcm",
                timeout=5.0,
                ttl=min(300, max(0, ttl)),
                requests_session=self.session,
                verbose=False,
            )
            return response.status_code
        except self.error as error:
            response = getattr(error, "response", None)
            return getattr(response, "status_code", None)
        finally:
            self.session.authorize = lambda: False

    def close(self):
        return self.session.close()
