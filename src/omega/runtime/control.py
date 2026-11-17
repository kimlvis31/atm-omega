"""Local control ownership, durable command deduplication and device credentials."""

# Imports
import copy
import hashlib
import hmac
import json
import os
import re
import secrets
import time
import uuid
from pathlib import Path
from threading import Event, RLock

from omega.domain.errors import AuthorizationDenied, CommandRejected, StorageError, ValidationError
from omega.runtime.settings import atomic_json


def token_digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def incident_error_context(error, *, stage, operation):
    """Keep bounded categories, never request contents or exception messages."""
    result = {
        "stage": stage if stage in {"headers", "body", "operation", "response"} else "unknown",
        "operation": operation if operation in {"GET", "POST"} else "unknown",
        "error_kind": "timeout" if isinstance(error, TimeoutError) else "io",
    }
    number = getattr(error, "errno", None)
    if type(number) is int and -4096 <= number <= 4096:
        result["errno"] = number
    return result


def _incident_context(value):
    if not isinstance(value, dict):
        return {}
    options = {
        "stage": {"headers", "body", "operation", "response", "unknown"},
        "operation": {"GET", "POST", "unknown"},
        "error_kind": {"timeout", "io"},
    }
    result = {
        key: item
        for key, choices in options.items()
        if isinstance(item := value.get(key), str) and item in choices
    }
    number = value.get("errno")
    if type(number) is int and -4096 <= number <= 4096:
        result["errno"] = number
    return result


class InstanceLock:
    """An OS-held lock releases on process death; stale descriptor files do not own it."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = path.open("a+b")
        if self.stream.tell() == 0:
            self.stream.write(b"0")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.stream.close()
            raise RuntimeError("an Omega core already owns this root") from None

    @property
    def released(self):
        """Observe this owned stream only; never close a remembered raw handle."""
        return self.stream.closed is True

    def close(self):
        if not self.released:
            self.stream.close()
        if not self.released:
            raise StorageError("instance lease release is not confirmed")


def load_token(path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        token = path.read_text(encoding="utf-8").strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{43,128}", token):
            raise ValidationError("invalid core credential file") from None
        return token
    token = secrets.token_urlsafe(32)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(token)
        stream.flush()
        os.fsync(stream.fileno())
    return token


class ControlState:
    def __init__(self, path: Path, *, clock=time.time):
        self.path, self.clock, self.lock = Path(path), clock, RLock()
        self.changed = Event()
        self._retired = False
        self.epoch = uuid.uuid4().hex
        self.data = {
            "schema_version": 1,
            "sequence": 0,
            "paused": True,
            "control_revision": 0,
            "commands": {},
            "devices": {},
            "incidents": [],
        }
        self.pairings = {}
        if self.path.exists():
            if self.path.stat().st_size > 8_000_000:
                raise ValidationError("control state exceeds limit")
            previous = json.loads(self.path.read_text(encoding="utf-8"))
            previous.setdefault("control_revision", 0)
            if previous.get("schema_version") != 1 or set(previous) != set(self.data):
                raise ValidationError("invalid control state schema")
            self.data = previous
            self.data["paused"] = True
        self.data["control_revision"] += 1
        self.storage_failed = False
        self._save()

    def _save(self):
        self._require_owner()
        try:
            if len(json.dumps(self.data, allow_nan=False).encode("utf-8")) > 8_000_000:
                raise OSError("control state exceeds storage limit")
            atomic_json(self.path, self.data)
        except (OSError, ValueError, TypeError):
            self.storage_failed = True
            self.data["paused"] = True
            raise OSError("control state could not be durably recorded") from None
        finally:
            self.changed.set()

    def snapshot(self):
        with self.lock:
            incidents = copy.deepcopy(self.data["incidents"])
            for item in incidents:
                seen = item.get("last_seen_epoch")
                item["observation_scope"] = (
                    "current_core"
                    if seen == self.epoch
                    else "previous_core"
                    if isinstance(seen, str) and seen
                    else "unknown"
                )
            return {
                "sequence": self.data["sequence"],
                "paused": self.data["paused"],
                "epoch": self.epoch,
                "control_revision": self.data["control_revision"],
                "server_time_ms": int(self.clock() * 1000),
                "storage_available": not self.storage_failed,
                "incidents": incidents,
            }

    def incident(self, code: str, severity="critical", *, invalidate_resume=False, context=None):
        with self.lock:
            if self._retired:
                return
            if invalidate_resume:
                self.data["control_revision"] += 1
            existing = next(
                (
                    item
                    for item in self.data["incidents"]
                    if item["code"] == code and not item["acknowledged"]
                ),
                None,
            )
            if existing is None:
                now = self.clock()
                self.data["incidents"].append(
                    {
                        "id": secrets.token_hex(8),
                        "code": code,
                        "severity": severity,
                        "timestamp": now,
                        "acknowledged": False,
                        "revision": 1,
                        "origin_epoch": self.epoch,
                        "last_seen_epoch": self.epoch,
                        "last_seen_at": now,
                        "occurrences": 1,
                        "context": _incident_context(context),
                    }
                )
                self.data["incidents"] = self.data["incidents"][-100:]
            else:
                count = existing.get("occurrences", 0)
                existing.update(
                    last_seen_epoch=self.epoch,
                    last_seen_at=self.clock(),
                    occurrences=min(1_000_000_000, max(0, count) + 1) if type(count) is int else 1,
                )
                if context is not None:
                    existing["context"] = _incident_context(context)
                ranks = {"info": 0, "warning": 1, "error": 2, "critical": 3}
                if ranks.get(severity, 0) > ranks.get(existing["severity"], 0):
                    existing.update(
                        severity=severity,
                        timestamp=self.clock(),
                        revision=existing.get("revision", 1) + 1,
                    )
            self.data["sequence"] += 1
            try:
                self._save()
            except OSError:
                pass

    def authorize(self, token: str, admin_token: str):
        if hmac.compare_digest(token, admin_token):
            return "admin"
        digest = token_digest(token)
        with self.lock:
            for identity, device in self.data["devices"].items():
                if not device["revoked"] and hmac.compare_digest(device["token_hash"], digest):
                    return identity
        return None

    def command(self, identity: str, command: dict, execute):
        if not isinstance(command, dict) or set(command) != {"id", "kind", "payload"}:
            raise ValidationError("command requires id, kind and payload")
        commandID, kind, payload = command["id"], command["kind"], command["payload"]
        if (
            not isinstance(commandID, str)
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", commandID)
            or not isinstance(kind, str)
            or not isinstance(payload, dict)
        ):
            raise ValidationError("invalid command envelope")
        if identity != "admin":
            permissions = self.data["devices"].get(identity, {}).get("permissions", [])
            if kind not in {"pause", "resume"} or kind not in permissions:
                raise AuthorizationDenied("device permission does not allow this command")
        digest = hashlib.sha256(
            json.dumps(command, sort_keys=True, allow_nan=False).encode()
        ).hexdigest()
        with self.lock:
            self._require_owner()
            if identity != "admin" and self.data["devices"].get(identity, {}).get("revoked", True):
                raise AuthorizationDenied("device access revoked")
            if kind == "resume" and payload.get("expected_epoch") != self.epoch:
                raise ValidationError("control epoch is stale")
            key = f"{identity}:{commandID}"
            previous = self.data["commands"].get(key)
            if previous:
                if previous["digest"] != digest:
                    raise ValidationError("command id reused with different payload")
                return copy.deepcopy(previous["result"])
            if kind in {"pause", "resume"}:
                self._validate_control_preconditions(kind, payload)
            if kind in {"acknowledge", "incident.acknowledge"} and (
                self.storage_failed or len(self.data["commands"]) >= 4096
            ):
                result = self.acknowledge(payload.get("incident_id"), allow_volatile=True)
                return {
                    "id": commandID,
                    "status": "volatile",
                    "code": "CONTROL_RECORDING_UNAVAILABLE",
                    "result": result,
                }
            if len(self.data["commands"]) >= 4096:
                if kind == "pause":
                    self.data["paused"] = True
                    self.data["control_revision"] += 1
                    execute(kind, {})
                    self.incident("CONTROL_HISTORY_FULL", "warning")
                    return {"id": commandID, "status": "volatile", "code": "CONTROL_HISTORY_FULL"}
                raise ValidationError(
                    "command history is full; archive before issuing more commands"
                )
            if self.storage_failed:
                if kind == "pause":
                    self.data["paused"] = True
                    self.data["control_revision"] += 1
                    execute(kind, {})
                    return {
                        "id": commandID,
                        "status": "volatile",
                        "code": "CONTROL_RECORDING_UNAVAILABLE",
                    }
                raise ValidationError("control recording unavailable; new commands blocked")
            pending = {"id": commandID, "status": "unknown", "code": "COMMAND_OUTCOME_UNKNOWN"}
            self.data["commands"][key] = {"digest": digest, "result": pending}
            self._save()
            paused_before = self.data["paused"]
            try:
                if kind == "pause":
                    self.data["paused"] = True
                elif kind == "resume":
                    self.data["paused"] = False
                provider_payload = (
                    {
                        key: value
                        for key, value in payload.items()
                        if key not in {"expected_epoch", "expected_revision", "expires_at_ms"}
                    }
                    if kind in {"pause", "resume"}
                    else payload
                )
                result = execute(kind, provider_payload)
                if kind in {"pause", "resume"}:
                    self.data["control_revision"] += 1
                response = {"id": commandID, "status": "completed", "result": result}
            except CommandRejected:
                if kind in {"pause", "resume"}:
                    self.data["paused"] = paused_before or self.storage_failed
                response = {
                    "id": commandID,
                    "status": "rejected",
                    "code": "INVALID_COMMAND",
                    "result": {"error": "VALIDATION_REJECTED"},
                }
            except Exception:
                self.data["paused"] = True
                self.data["control_revision"] += 1
                # The caller cannot infer that a provider did not act before raising.
                self.incident("COMMAND_OUTCOME_UNKNOWN")
                return copy.deepcopy(pending)
            self.data["commands"][key]["result"] = response
            self.data["sequence"] += 1
            try:
                self._save()
            except OSError:
                self.data["commands"][key]["result"] = pending
                self.incident("ALL_RECORDING_UNAVAILABLE")
                return copy.deepcopy(pending)
            return copy.deepcopy(response)

    def _validate_control_preconditions(self, kind: str, payload: dict):
        names = {"expected_epoch", "expected_revision", "expires_at_ms"}
        if kind == "pause" and not names.intersection(payload):
            return
        if not names <= set(payload):
            raise ValidationError("control epoch, revision and deadline are required")
        if payload["expected_epoch"] != self.epoch:
            raise ValidationError("control epoch is stale")
        if (
            type(payload["expected_revision"]) is not int
            or payload["expected_revision"] != self.data["control_revision"]
        ):
            raise ValidationError("control revision is stale")
        now = int(self.clock() * 1000)
        expires = payload["expires_at_ms"]
        if type(expires) is not int or not now <= expires <= now + 30_000:
            raise ValidationError("control deadline expired or exceeds 30 seconds")

    def create_pairing(self, name: str, permissions=None) -> dict:
        if not isinstance(name, str) or not 1 <= len(name) <= 64:
            raise ValidationError("device name must contain 1 to 64 characters")
        permissions = ["read", "pause", "resume"] if permissions is None else permissions
        if (
            not isinstance(permissions, list)
            or "read" not in permissions
            or set(permissions) - {"read", "pause", "resume"}
        ):
            raise ValidationError("invalid device permissions")
        with self.lock:
            self._require_owner()
            self.pairings = {
                key: value
                for key, value in self.pairings.items()
                if value["expires"] > self.clock()
            }
            if len(self.pairings) >= 10 or len(self.data["devices"]) >= 100:
                raise ValidationError("device registration limit reached")
            token = secrets.token_urlsafe(24)
            self.pairings[token_digest(token)] = {
                "name": name,
                "expires": self.clock() + 120,
                "permissions": permissions,
            }
            return {"code": token, "expires_at_ms": int((self.clock() + 120) * 1000)}

    def operation(self, identity: str, commandID: str) -> dict:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", commandID):
            raise ValidationError("invalid operation identity")
        with self.lock:
            previous = self.data["commands"].get(f"{identity}:{commandID}")
            if previous is None:
                raise LookupError("operation not found for this device")
            return copy.deepcopy(previous["result"])

    def pair(self, token: str) -> dict:
        with self.lock:
            self._require_owner()
            registration = self.pairings.pop(token_digest(token), None)
            if not registration or registration["expires"] <= self.clock():
                raise AuthorizationDenied("pairing token expired or invalid")
            identity, credential = secrets.token_hex(12), secrets.token_urlsafe(32)
            permissions = registration["permissions"]
            self.data["devices"][identity] = {
                "id": identity,
                "name": registration["name"],
                "token_hash": token_digest(credential),
                "revoked": False,
                "permissions": permissions,
            }
            self._save()
            return {"device_id": identity, "token": credential, "permissions": permissions}

    def devices(self):
        with self.lock:
            return [
                {key: value for key, value in device.items() if key != "token_hash"}
                for device in self.data["devices"].values()
            ]

    def revoke(self, identity: str):
        with self.lock:
            self._require_owner()
            if identity not in self.data["devices"]:
                raise ValidationError("unknown device")
            self.data["devices"][identity]["revoked"] = True
            self._save()
            return {"device_id": identity, "revoked": True}

    def acknowledge(self, incidentID: str, *, allow_volatile=False):
        with self.lock:
            self._require_owner()
            for incident in self.data["incidents"]:
                if incident["id"] == incidentID:
                    incident["acknowledged"] = True
                    try:
                        self._save()
                        return {"acknowledged": True, "durable": True}
                    except OSError:
                        if not allow_volatile:
                            raise
                        return {"acknowledged": True, "durable": False}
            raise ValidationError("unknown incident")

    def retire(self):
        """Fence delayed diagnostics and control writes before releasing core ownership."""
        with self.lock:
            self._retired = True
            self.pairings.clear()

    def _require_owner(self):
        if self._retired:
            raise ValidationError("control owner is retired")
