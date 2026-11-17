"""Private synthetic subscriptions and fake senders only; no provider communication."""

import base64
import json

import pytest

from omega.domain.errors import AuthorizationDenied, StorageError, ValidationError
from omega.runtime.control import ControlState
from omega.runtime.push_delivery import PushDelivery
from omega.runtime.push_subscriptions import (
    SubscriptionStore,
    device_generation,
    validate_subscription,
)
from test_core_service import service as service


def subscription():
    return {
        "endpoint": "https://fcm.googleapis.com/fcm/send/synthetic-token",
        "expirationTime": None,
        "keys": {
            "p256dh": base64.urlsafe_b64encode(b"\x04" + b"x" * 64).rstrip(b"=").decode(),
            "auth": base64.urlsafe_b64encode(b"a" * 16).rstrip(b"=").decode(),
        },
    }


class Sender:
    public_key = "synthetic-public-key"

    def __init__(self):
        self.calls = []
        self.status = 201
        self.before = lambda: None

    def send(self, value, payload, ttl, authorize):
        self.before()
        if not authorize():
            raise PermissionError("revoked before submission")
        self.calls.append((value, json.loads(payload), ttl))
        if isinstance(self.status, Exception):
            raise self.status
        return self.status

    def close(self):
        return {"rootless_dns_alive": False}


@pytest.fixture
def setup(tmp_path):
    now = [1_800_000_000.0]
    control = ControlState(tmp_path / "control.json", clock=lambda: now[0])
    paired = control.pair(control.create_pairing("synthetic phone")["code"])
    store = SubscriptionStore(tmp_path / "subscriptions.json", clock=lambda: now[0])
    sender = Sender()
    warnings = []
    push = PushDelivery(control, store, sender, clock=lambda: now[0], diagnostic=warnings.append)
    push.subscribe(paired["device_id"], {"subscription": subscription()})
    return now, control, paired, store, sender, push, warnings


def incident(control):
    control.incident(
        "SYNTHETIC_FINANCIAL_DETAIL_DO_NOT_SEND", "critical", context={"stage": "operation"}
    )


def test_registered_device_owns_only_its_subscription_and_no_financial_authority(service, tmp_path):
    paired = service.control.pair(service.control.create_pairing("phone", ["read"])["code"])
    sender = Sender()
    store = SubscriptionStore(tmp_path / "synthetic.json")
    service.push_delivery = PushDelivery(service.control, store, sender)
    for token in (service.token, "invalid"):
        with pytest.raises(AuthorizationDenied):
            service.request("POST", "/api/push/subscribe", token, {"subscription": subscription()})
    token = paired["token"]
    assert service.request("POST", "/api/push/subscribe", token, {"subscription": subscription()})[
        "subscribed"
    ]
    with pytest.raises(ValidationError):
        service.request(
            "POST",
            "/api/push/subscribe",
            token,
            {"subscription": subscription(), "device_id": "other"},
        )
    with pytest.raises(AuthorizationDenied):
        service.request("POST", "/api/jobs", token, {})
    with pytest.raises(AuthorizationDenied):
        service.request(
            "POST", "/api/commands", token, {"id": "fake", "kind": "resume", "payload": {}}
        )
    assert service.control.snapshot()["paused"] is True
    assert service.request("POST", "/api/push/unsubscribe", token, {}) == {"subscribed": False}
    service.control.revoke(paired["device_id"])
    with pytest.raises(AuthorizationDenied):
        service.request("GET", "/api/push/config", token)


def test_default_off_config_does_not_create_push_private_files(service):
    paired = service.control.pair(service.control.create_pairing("phone")["code"])
    result = service.request("GET", "/api/push/config", paired["token"])
    assert result["enabled"] is False and result["available"] is False
    assert not (service.root / "secrets" / "vapid-private.pem").exists()
    assert not (service.root / "secrets" / "push-subscriptions.json").exists()


@pytest.mark.parametrize(
    "change",
    [
        {"keys": {"p256dh": "bad", "auth": "bad"}},
        {"endpoint": "https://127.0.0.1/private"},
        {"expirationTime": True},
        {"expirationTime": 1.5},
        {"extra": "private"},
    ],
)
def test_malformed_registration_rejected(change):
    with pytest.raises(ValidationError):
        validate_subscription({**subscription(), **change})


def test_accepted_by_service_is_durable_dedup_not_delivery(setup):
    now, control, paired, store, sender, push, _warnings = setup
    incident(control)
    push.collect()
    assert push.dispatch_one()
    assert len(sender.calls) == 1
    value, payload, ttl = sender.calls[0]
    assert set(payload) == {"version", "event_id", "severity", "timestamp_ms"}
    assert "SYNTHETIC_FINANCIAL" not in json.dumps(payload)
    assert 0 <= ttl <= 300
    assert push.status()["recent_attempts"] == {"accepted_by_service": 1}
    assert push.config(paired["device_id"])["delivery"] == "unverified"
    restarted = SubscriptionStore(store.path, clock=lambda: now[0])
    again = PushDelivery(control, restarted, sender, clock=lambda: now[0])
    again.collect()
    assert not again.dispatch_one()
    assert len(sender.calls) == 1
    assert again.config(paired["device_id"])["recorded_attempts"] == {"accepted_by_service": 1}
    assert value["endpoint"] == subscription()["endpoint"]


def test_unknown_attempt_never_auto_retries_or_recurses(setup):
    now, control, _paired, store, sender, push, warnings = setup
    sender.status = TimeoutError("private endpoint must not appear in warning")
    incident(control)
    push.collect()
    push.dispatch_one()
    now[0] += 1
    push.collect()
    assert not push.dispatch_one()
    assert push.status()["recent_attempts"] == {"attempt_unknown": 1}
    assert warnings == ["WEBPUSH_ATTEMPT_UNKNOWN"]
    assert len(control.snapshot()["incidents"]) == 1
    assert (
        next(iter(next(iter(store.snapshot().values()))["seen"].values()))["state"]
        == "attempt_unknown"
    )


@pytest.mark.parametrize("kind", ["revoke", "permission", "generation", "unsubscribe", "expiry"])
def test_revocation_permission_generation_and_expiry_cancel_queued_work(setup, kind):
    now, control, paired, store, sender, push, _warnings = setup
    incident(control)
    push.collect()
    identity = paired["device_id"]
    if kind == "revoke":
        control.revoke(identity)
    elif kind == "permission":
        with control.lock:
            control.data["devices"][identity]["permissions"] = []
    elif kind == "generation":
        push.subscribe(identity, {"subscription": subscription()})
    elif kind == "unsubscribe":
        push.unsubscribe(identity, {})
    else:
        now[0] += 301
    push.dispatch_one()
    assert sender.calls == []
    if kind == "revoke":
        push.collect()
        assert store.snapshot() == {}


def test_revoke_after_encrypt_before_transport_submission_is_rechecked(setup):
    _now, control, paired, _store, sender, push, _warnings = setup
    incident(control)
    push.collect()
    sender.before = lambda: control.revoke(paired["device_id"])
    push.dispatch_one()
    assert sender.calls == []
    assert push.status()["recent_attempts"] == {"attempt_unknown": 1}


def test_explicit_service_retry_bounded_and_410_retires_only_same_generation(setup):
    now, control, _paired, store, sender, push, _warnings = setup
    sender.status = 503
    incident(control)
    push.collect()
    push.dispatch_one()
    assert not push.dispatch_one()
    now[0] += 5
    push.dispatch_one()
    now[0] += 10
    push.dispatch_one()
    assert len(sender.calls) == 3
    assert push.status()["recent_attempts"] == {"rejected": 1}
    control.incident("SECOND_SYNTHETIC", "warning")
    sender.status = 410
    push.collect()
    push.dispatch_one()
    assert store.snapshot() == {}


def test_queue_and_registry_are_bounded_and_fair_for_noisy_devices(setup):
    _now, control, paired, store, _sender, push, _warnings = setup
    for index in range(100):
        control.incident(f"SYNTHETIC_{index}", "warning")
    for _index in range(99):
        phone = control.pair(control.create_pairing("phone")["code"])
        push.subscribe(phone["device_id"], {"subscription": subscription()})
    push.collect()
    assert len(push.queue) == 100
    assert len({key[0] for key in push.queue}) == 100
    assert paired["device_id"] in {key[0] for key in push.queue}
    with pytest.raises(ValidationError):
        store.subscribe("f" * 24, "a" * 64, subscription())


def test_atomic_store_failure_prevents_submission_and_preserves_old_bytes(setup, monkeypatch):
    _now, control, _paired, store, sender, push, _warnings = setup
    original = store.path.read_bytes()
    incident(control)
    push.collect()

    def fail(*_args):
        raise OSError("synthetic storage failure")

    monkeypatch.setattr("omega.runtime.push_subscriptions.atomic_json", fail)
    with pytest.raises(OSError):
        push.dispatch_one()
    assert sender.calls == [] and store.path.read_bytes() == original


def test_unconfirmed_root_using_worker_is_never_blessed_cleanup(setup):
    _now, _control, _paired, _store, _sender, push, _warnings = setup

    class Alive:
        def join(self, timeout):
            assert timeout <= 6

        def is_alive(self):
            return True

    push.thread = Alive()
    with pytest.raises(StorageError):
        push.close()
    assert push.receipt is None


def test_never_started_worker_has_explicit_close_receipt_and_no_pending_tickets(setup):
    _now, control, paired, _store, sender, push, _warnings = setup
    incident(control)
    push.collect()
    assert device_generation(control, paired["device_id"])
    assert push.close() == {
        "worker_alive": False,
        "queued": 0,
        "transport_closed": True,
        "rootless_dns_alive": False,
    }
    assert sender.calls == [] and not push.dispatch_one()


def test_push_retirement_failure_retains_real_core_root_until_retry(service):
    from omega.runtime.control import InstanceLock

    class HeldPush:
        fail = True

        def close(self):
            if self.fail:
                raise StorageError("synthetic push worker retirement unknown")

    held = HeldPush()
    service.push_delivery = held
    with pytest.raises(StorageError):
        service.close()
    assert service.instance.released is False
    with pytest.raises(RuntimeError):
        InstanceLock(service.root / "state" / "core.lock")
    held.fail = False
    service.close()
    assert service.instance.released is True


def test_registration_rate_is_bounded_and_another_device_can_still_unsubscribe(setup):
    now, control, paired, _store, _sender, push, _warnings = setup
    for _ in range(5):
        push.subscribe(paired["device_id"], {"subscription": subscription()})
    with pytest.raises(ValidationError):
        push.subscribe(paired["device_id"], {"subscription": subscription()})
    other = control.pair(control.create_pairing("other")["code"])
    assert push.unsubscribe(other["device_id"], {}) == {"subscribed": False}
    now[0] += 60
    assert push.unsubscribe(paired["device_id"], {}) == {"subscribed": False}
