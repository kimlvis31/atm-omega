"""Optional Core-owned notification queue, isolated from local alarms and financial gates."""

import json
import re
import threading
import time
from collections import OrderedDict, deque

from omega.domain.errors import AuthorizationDenied, StorageError, ValidationError
from omega.runtime.push_subscriptions import device_generation


class PushDelivery:
    def __init__(self, control, store, sender, *, clock=time.time, diagnostic=lambda _code: None):
        self.control, self.store, self.sender = control, store, sender
        self.clock, self.diagnostic = clock, diagnostic
        self.queue, self.results = OrderedDict(), OrderedDict()
        self.registration_attempts = OrderedDict()
        self.lock, self.stopped = threading.RLock(), threading.Event()
        self.thread = None
        self.cursor = 0
        self.warning = None
        self.receipt = None

    def _warn(self, code):
        with self.lock:
            if self.warning == code:
                return
            self.warning = code
        # The supplied sink is a direct local logger, never record_incident().
        try:
            self.diagnostic(code)
        except Exception:
            pass

    def _result(self, key, state):
        with self.lock:
            self.results[key] = state
            self.results.move_to_end(key)
            while len(self.results) > 100:
                self.results.popitem(last=False)

    def config(self, identity):
        registration = device_generation(self.control, identity)
        rows = self.store.snapshot()
        row = rows.get(identity)
        valid = bool(row and self.store.current(identity, row["generation"], registration))
        states = {}
        for item in row["seen"].values() if valid else []:
            states[item["state"]] = states.get(item["state"], 0) + 1
        return {
            "enabled": True,
            "available": not self.stopped.is_set(),
            "public_key": self.sender.public_key,
            "subscribed": valid,
            "expires_at_ms": row["expires_at"] if valid else None,
            "delivery": "unverified",
            "recorded_attempts": states,
        }

    def subscribe(self, identity, body):
        if self.stopped.is_set():
            raise ValidationError("push sender is stopping")
        if type(body) is not dict or set(body) != {"subscription"}:
            raise ValidationError("push registration requires only subscription")
        registration = device_generation(self.control, identity)
        self._limit_registration(identity)
        return self.store.subscribe(identity, registration, body["subscription"])

    def unsubscribe(self, identity, body):
        if type(body) is not dict or body:
            raise ValidationError("push unsubscribe requires an empty object")
        device_generation(self.control, identity)
        self._limit_registration(identity)
        result = self.store.unsubscribe(identity)
        with self.lock:
            for key in list(self.queue):
                if key[0] == identity:
                    self.queue.pop(key)
        return result

    def _limit_registration(self, identity):
        with self.lock:
            attempts = self.registration_attempts.setdefault(identity, deque(maxlen=6))
            self.registration_attempts.move_to_end(identity)
            while len(self.registration_attempts) > 100:
                self.registration_attempts.popitem(last=False)
            now = self.clock()
            while attempts and attempts[0] <= now - 60:
                attempts.popleft()
            if len(attempts) >= 6:
                raise ValidationError("push registration attempts limited; retry later")
            attempts.append(now)

    def collect(self):
        """Polling occurs only on the push worker; incidents never call networking."""
        if self.stopped.is_set():
            return
        snapshot = self.control.snapshot()
        rows = self.store.snapshot()
        active = []
        for incident in snapshot["incidents"][-100:]:
            event_id, revision = incident.get("id"), incident.get("revision", 1)
            severity, stamp = incident.get("severity"), incident.get("timestamp")
            if (
                incident.get("acknowledged") is not False
                or incident.get("last_seen_epoch") != snapshot["epoch"]
                or type(event_id) is not str
                or re.fullmatch(r"[a-f0-9]{16}", event_id) is None
                or type(revision) is not int
                or not 1 <= revision <= 1_000_000_000
                or severity not in {"warning", "error", "critical"}
                or type(stamp) not in {float, int}
                or not 0 < stamp <= self.clock()
                or self.clock() - stamp > 300
                or str(incident.get("code", "")).startswith("WEBPUSH_")
            ):
                continue
            # Deliberately exclude incident code/context, accounts, position and financial data.
            payload = json.dumps(
                {
                    "version": 1,
                    "event_id": event_id,
                    "severity": severity,
                    "timestamp_ms": int(stamp * 1000),
                },
                separators=(",", ":"),
            )
            active.append((event_id, revision, payload, stamp + 300))
        identities = sorted(rows)
        if identities:
            offset = self.cursor % len(identities)
            identities = identities[offset:] + identities[:offset]
            self.cursor += 1
        for identity in identities:
            row = rows[identity]
            try:
                registration = device_generation(self.control, identity)
            except AuthorizationDenied:
                self.store.expire(identity, row["generation"])
                continue
            if self.store.current(identity, row["generation"], registration) is None:
                self.store.expire(identity, row["generation"])
                continue
            for event_id, revision, payload, expiry in active:
                key = identity, event_id
                if row["seen"].get(event_id, {}).get("revision", 0) >= revision:
                    continue
                with self.lock:
                    if key in self.queue and self.queue[key]["revision"] >= revision:
                        continue
                    # At most one waiting incident per device; a noisy device cannot
                    # consume the queue ahead of every other registered phone.
                    if key not in self.queue and any(item[0] == identity for item in self.queue):
                        break
                    if key not in self.queue and len(self.queue) >= 100:
                        break
                    self.queue[key] = {
                        "generation": row["generation"],
                        "registration": registration,
                        "revision": revision,
                        "payload": payload,
                        "expiry": expiry,
                        "attempt": 0,
                        "due": self.clock(),
                    }
                    self._result(key, "queued")

    def _authorized(self, identity, ticket):
        if self.stopped.is_set():
            return False
        try:
            registration = device_generation(self.control, identity)
            return (
                registration == ticket["registration"]
                and self.store.current(identity, ticket["generation"], registration) is not None
                and self.clock() < ticket["expiry"]
            )
        except Exception:
            return False

    def dispatch_one(self):
        with self.lock:
            selected = next(
                ((key, value) for key, value in self.queue.items() if value["due"] <= self.clock()),
                None,
            )
            if selected is None or self.stopped.is_set():
                return False
            key, ticket = selected
            self.queue.pop(key)
        identity, event_id = key
        if self.clock() >= ticket["expiry"]:
            self._result(key, "expired")
            return True
        if not self._authorized(identity, ticket):
            self._result(key, "rejected")
            return True
        row = self.store.current(identity, ticket["generation"], ticket["registration"])
        if row is None:
            self._result(key, "rejected")
            return True
        # Durably deduplicate BEFORE remote submission. Crash/timeout stays unknown;
        # no automatic duplicate send after restart or an ambiguous transport error.
        if not self.store.remember(identity, ticket["generation"], event_id, ticket["revision"]):
            self._result(key, "rejected")
            return True
        self._result(key, "attempt_unknown")
        try:
            status = self.sender.send(
                row["subscription"],
                ticket["payload"],
                int(ticket["expiry"] - self.clock()),
                lambda: self._authorized(identity, ticket),
            )
        except Exception:
            self._warn("WEBPUSH_ATTEMPT_UNKNOWN")
            return True
        if status == 201:
            self._result(key, "accepted_by_service")
            self.store.remember(
                identity, ticket["generation"], event_id, ticket["revision"], "accepted_by_service"
            )
        elif status in {404, 410}:
            self.store.expire(identity, ticket["generation"])
            self._result(key, "expired")
        elif status in {429, 503} and ticket["attempt"] < 2 and self._authorized(identity, ticket):
            ticket["attempt"] += 1
            ticket["due"] = self.clock() + 5 * 2 ** (ticket["attempt"] - 1)
            with self.lock:
                if key not in self.queue and len(self.queue) < 100 and not self.stopped.is_set():
                    self.queue[key] = ticket
                    self._result(key, "queued")
        elif type(status) is int:
            self._result(key, "rejected")
            self.store.remember(
                identity, ticket["generation"], event_id, ticket["revision"], "rejected"
            )
            self._warn("WEBPUSH_REJECTED")
        else:
            self._warn("WEBPUSH_ATTEMPT_UNKNOWN")
        return True

    def status(self):
        with self.lock:
            counts = {}
            for state in self.results.values():
                counts[state] = counts.get(state, 0) + 1
            return {
                "enabled": True,
                "queued": len(self.queue),
                "recent_attempts": counts,
                "warning": self.warning,
                "phone_delivery": "unverified",
            }

    def start(self):
        if self.stopped.is_set() or self.thread is not None:
            return
        self.thread = threading.Thread(target=self._run, name="omega-webpush", daemon=True)
        try:
            self.thread.start()
        except Exception:
            self.stopped.set()
            raise

    def _run(self):
        while not self.stopped.is_set():
            try:
                self.collect()
                sent = self.dispatch_one()
            except Exception:
                self._warn("WEBPUSH_LOCAL_UNAVAILABLE")
                sent = False
            self.stopped.wait(0.02 if sent else 1)

    def close(self):
        self.stopped.set()
        # Socket owner closes its own object; no remembered descriptor or foreign process.
        failure = None
        try:
            dns = self.sender.close()
        except Exception as error:
            failure = error
            dns = {"rootless_dns_alive": None}
        if self.thread is threading.current_thread():
            raise StorageError("push worker cannot confirm its own termination")
        if self.thread is not None:
            if self.thread.is_alive() or getattr(self.thread, "ident", None) is not None:
                self.thread.join(timeout=6)
            if self.thread.is_alive():
                raise StorageError("push worker retirement is unconfirmed")
        with self.lock:
            for key in self.queue:
                self._result(key, "expired")
            self.queue.clear()
        self.receipt = {
            "worker_alive": False,
            "queued": 0,
            "transport_closed": failure is None,
            **dns,
        }
        if failure is not None:
            raise StorageError("push transport retirement is unconfirmed") from None
        return dict(self.receipt)
