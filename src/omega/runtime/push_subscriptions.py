"""Bounded private Web Push registrations; no financial or command authority."""

import base64
import copy
import hashlib
import json
import re
import threading
import time
import uuid
from pathlib import Path

from omega.domain.errors import AuthorizationDenied, ValidationError
from omega.infrastructure.webpush_sender import validate_endpoint
from omega.runtime.settings import atomic_json


def device_generation(control, identity):
    """A short read under the existing revocation lock, never held over networking."""
    with control.lock:
        device = control.data["devices"].get(identity, {})
        permissions = device.get("permissions")
        digest = device.get("token_hash")
        if (
            identity == "admin"
            or device.get("revoked") is not False
            or type(permissions) is not list
            or "read" not in permissions
            or type(digest) is not str
            or re.fullmatch(r"[a-f0-9]{64}", digest) is None
        ):
            raise AuthorizationDenied("registered device read permission required")
        return hashlib.sha256((identity + ":" + digest).encode("ascii")).hexdigest()


def decode_key(value, size):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value) is None:
        raise ValidationError("invalid push public key encoding")
    try:
        decoded = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except ValueError:
        raise ValidationError("invalid push public key encoding") from None
    if len(decoded) != size or base64.urlsafe_b64encode(decoded).rstrip(b"=").decode() != value:
        raise ValidationError("invalid push public key size")
    return decoded


def validate_subscription(value):
    if type(value) is not dict or set(value) - {"endpoint", "keys", "expirationTime"}:
        raise ValidationError("invalid push subscription")
    validate_endpoint(value.get("endpoint"))
    keys = value.get("keys")
    if type(keys) is not dict or set(keys) != {"p256dh", "auth"}:
        raise ValidationError("invalid push subscription keys")
    if decode_key(keys["p256dh"], 65)[0] != 4:
        raise ValidationError("uncompressed push public key required")
    decode_key(keys["auth"], 16)
    expiry = value.get("expirationTime")
    if expiry is not None and (type(expiry) is not int or not 0 < expiry < 2**53):
        raise ValidationError("invalid push subscription expiry")
    return {"endpoint": value["endpoint"], "keys": copy.deepcopy(keys), "expirationTime": expiry}


class SubscriptionStore:
    """Private atomic registry. A fresh registration invalidates all older queue tickets."""

    def __init__(self, path, *, clock=time.time):
        self.path, self.clock, self.lock = Path(path), clock, threading.RLock()
        if self.path.parent.is_symlink() or self.path.parent.is_junction():
            raise ValidationError("push registry directory is redirected")
        self.rows = {}
        if self.path.exists():
            if self.path.is_symlink() or self.path.stat().st_size > 2_000_000:
                raise ValidationError("push registry exceeds limit or is redirected")
            source = json.loads(self.path.read_text(encoding="utf-8"))
            if type(source) is not dict or set(source) != {"schema_version", "devices"}:
                raise ValidationError("invalid push registry schema")
            if source["schema_version"] != 1 or type(source["devices"]) is not dict:
                raise ValidationError("invalid push registry schema")
            if len(source["devices"]) > 100:
                raise ValidationError("push device limit exceeded")
            for identity, row in source["devices"].items():
                self._validate_row(identity, row)
            self.rows = source["devices"]

    @staticmethod
    def _validate_row(identity, row):
        if type(identity) is not str or re.fullmatch(r"[a-f0-9]{24}", identity) is None:
            raise ValidationError("invalid push device identity")
        if type(row) is not dict or set(row) != {
            "registration",
            "generation",
            "expires_at",
            "subscription",
            "seen",
        }:
            raise ValidationError("invalid push registration")
        for name, size in (("registration", 64), ("generation", 32)):
            if (
                type(row[name]) is not str
                or re.fullmatch(r"[a-f0-9]{" + str(size) + "}", row[name]) is None
            ):
                raise ValidationError("invalid push registration generation")
        if type(row["expires_at"]) is not int or not 0 < row["expires_at"] < 2**53:
            raise ValidationError("invalid push registration expiry")
        validate_subscription(row["subscription"])
        if type(row["seen"]) is not dict or len(row["seen"]) > 100:
            raise ValidationError("push deduplication limit exceeded")
        for key, value in row["seen"].items():
            if type(key) is not str or re.fullmatch(r"[a-f0-9]{16}", key) is None:
                raise ValidationError("invalid push event identity")
            if (
                type(value) is not dict
                or set(value) != {"revision", "state"}
                or type(value["revision"]) is not int
                or not 1 <= value["revision"] <= 1_000_000_000
                or type(value["state"]) is not str
                or value["state"]
                not in {"attempt_unknown", "accepted_by_service", "expired", "rejected"}
            ):
                raise ValidationError("invalid push event revision")

    def _save(self, candidate):
        if (
            self.path.is_symlink()
            or self.path.parent.is_symlink()
            or self.path.parent.is_junction()
        ):
            raise ValidationError("push registry path is redirected")
        payload = {"schema_version": 1, "devices": candidate}
        if len(json.dumps(payload).encode()) > 2_000_000:
            raise ValidationError("push registry exceeds storage limit")
        atomic_json(self.path, payload)
        self.rows = candidate

    def subscribe(self, identity, registration, subscription):
        self._validate_identity(identity, registration)
        subscription = validate_subscription(subscription)
        now = int(self.clock() * 1000)
        expiry = min(now + 30 * 86_400_000, subscription["expirationTime"] or 2**53 - 1)
        if expiry <= now:
            raise ValidationError("push subscription already expired")
        with self.lock:
            if identity not in self.rows and len(self.rows) >= 100:
                raise ValidationError("push device limit reached")
            candidate = copy.deepcopy(self.rows)
            previous = candidate.get(identity, {})
            candidate[identity] = {
                "registration": registration,
                "generation": uuid.uuid4().hex,
                "expires_at": expiry,
                "subscription": subscription,
                "seen": previous.get("seen", {})
                if previous.get("registration") == registration
                else {},
            }
            self._save(candidate)
        return {"subscribed": True, "expires_at_ms": expiry}

    @staticmethod
    def _validate_identity(identity, registration):
        if type(identity) is not str or re.fullmatch(r"[a-f0-9]{24}", identity) is None:
            raise ValidationError("invalid push device identity")
        if type(registration) is not str or re.fullmatch(r"[a-f0-9]{64}", registration) is None:
            raise ValidationError("invalid push registration generation")

    def unsubscribe(self, identity):
        with self.lock:
            candidate = copy.deepcopy(self.rows)
            candidate.pop(identity, None)
            self._save(candidate)
        return {"subscribed": False}

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.rows)

    def current(self, identity, generation, registration):
        with self.lock:
            row = self.rows.get(identity)
            if row and row["generation"] == generation and row["registration"] == registration:
                if row["expires_at"] > int(self.clock() * 1000):
                    return copy.deepcopy(row)
        return None

    def remember(self, identity, generation, event_id, revision, state="attempt_unknown"):
        if state not in {"attempt_unknown", "accepted_by_service", "expired", "rejected"}:
            raise ValidationError("invalid push attempt state")
        with self.lock:
            row = self.rows.get(identity)
            if row is None or row["generation"] != generation:
                return False
            candidate = copy.deepcopy(self.rows)
            seen = candidate[identity]["seen"]
            seen[event_id] = {"revision": revision, "state": state}
            while len(seen) > 100:
                seen.pop(next(iter(seen)))
            self._save(candidate)
            return True

    def expire(self, identity, generation):
        with self.lock:
            if self.rows.get(identity, {}).get("generation") == generation:
                candidate = copy.deepcopy(self.rows)
                candidate.pop(identity)
                self._save(candidate)
