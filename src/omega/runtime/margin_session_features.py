"""Frozen optional external inputs; graph/model/financial owners remain independent."""

from contextlib import ExitStack, contextmanager
from copy import deepcopy
from hashlib import sha256
from time import time_ns

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.external_features import (
    ExternalFeatureBinding,
    ExternalFeatureDelivery,
    ExternalFeaturePin,
    semantic_candle_sha256,
)


def prepare_external_bindings(raw, instrument, module, provider, *, verify_provider):
    """Split declared ownership before building the pure analysis graph."""
    required = frozenset(module.descriptor.required_fields)
    external = {name for name in required if name.startswith("NN.")}
    if "external_features" not in raw:
        if external:
            raise ValidationError("required NN fields need an explicit registered provider")
        return (), required
    documents = raw["external_features"]
    if type(documents) is not list or not 1 <= len(documents) <= 8:
        raise ValidationError("one to eight explicit external feature bindings required")
    bindings, fields, identities = [], set(), set()
    for document in documents:
        binding = ExternalFeatureBinding.parse(document)
        if binding.instrument != instrument or binding.binding_sha256 in identities:
            raise ValidationError("external binding instrument or identity differs")
        if fields.intersection(binding.fields):
            raise ValidationError("external fields cannot have more than one owner")
        if verify_provider:
            if provider is None or not callable(getattr(provider, "describe", None)):
                raise ValidationError(
                    "new external input configuration needs a registered provider"
                )
            registered = provider.describe(
                binding.provider_id, binding.instance_id, binding.stream, instrument
            )
            if not isinstance(registered, ExternalFeatureBinding) or registered != binding:
                raise ValidationError("external binding differs from its registered provider")
        identities.add(binding.binding_sha256)
        fields.update(binding.fields)
        bindings.append(binding)
    if fields - required or external - fields:
        raise ValidationError(
            "external binding fields must be required, with every NN field explicitly owned"
        )
    if not callable(getattr(module, "evaluate_protection", None)):
        raise ValidationError("external-input strategy needs an independent protection method")
    return tuple(bindings), required - fields


class MarginSessionFeatures:
    """Data-only frozen proof selection plus the injected cache/pin port."""

    def __init__(self, strategies, rows, modules, provider=None, clock_ms=None):
        self.provider = provider
        self.clock_ms = clock_ms or (lambda: time_ns() // 1_000_000)
        instruments = {row["id"]: row["instrument"] for row in rows}
        self.bindings = {}
        for raw in strategies:
            bindings, _ = prepare_external_bindings(
                raw,
                instruments[raw["id"]],
                modules[raw["id"]],
                provider,
                verify_provider=False,
            )
            if bindings:
                self.bindings[raw["id"]] = bindings
        self.enabled = bool(self.bindings)
        self.errors = {}

    @staticmethod
    def consumer_id(account_id, strategy_id):
        # Stable, bounded and independent of the local filesystem path.
        return "margin-" + sha256((account_id + "\0" + strategy_id).encode()).hexdigest()

    def freeze(self, candles, cutoff):
        by_instrument = {bar.instrument: bar for bar in candles}
        result = {}
        for identity, bindings in self.bindings.items():
            self.errors.pop(identity, None)
            entries = []
            for binding in bindings:
                candle = by_instrument[binding.instrument]
                now = self.clock_ms()
                if now < cutoff:
                    raise ValidationError("external financial cutoff is ahead of the actual clock")
                try:
                    delivery = self.provider.lookup(binding, candle, decision_at_ms=cutoff)
                    if (
                        not isinstance(delivery, ExternalFeatureDelivery)
                        or delivery.binding != binding
                        or not delivery.matches_candle(candle)
                        or delivery.decision_at_ms != cutoff
                        or delivery.observed_at_ms > self.clock_ms()
                    ):
                        raise ValidationError("external cached delivery differs from its request")
                    # Pure roundtrip verifies bounded, canonical serialized proof.
                    delivery = ExternalFeatureDelivery.parse(delivery.document())
                except Exception:
                    self.errors[identity] = "provider_unavailable"
                    delivery = ExternalFeatureDelivery(
                        binding,
                        "unavailable",
                        None,
                        "unavailable",
                        "unavailable",
                        "unavailable",
                        semantic_candle_sha256(candle),
                        candle.open_time_ms,
                        cutoff,
                        now,
                        "provider_unavailable",
                    )
                entries.append({"delivery": delivery.document(), "pin": None})
            result[identity] = entries
        return result

    def pin(self, frame, candles, *, account_id):
        if not self.enabled:
            return frame
        result = deepcopy(frame)
        by_instrument = {bar.instrument: bar for bar in candles}
        for identity, entries in result["external_features"].items():
            for entry in entries:
                delivery = ExternalFeatureDelivery.parse(entry["delivery"])
                if delivery.disposition != "ready" or entry["pin"] is not None:
                    continue
                try:
                    pin = self.provider.pin(
                        self.consumer_id(account_id, identity),
                        frame["digest"],
                        delivery.snapshot,
                        by_instrument[delivery.binding.instrument],
                        decision_at_ms=frame["as_of_ms"],
                    )
                    if not isinstance(pin, ExternalFeaturePin):
                        raise ValidationError("external pin ACK must be typed")
                    entry["pin"] = pin.document()
                    self.validate(result, candles, account_id=account_id)
                except Exception:
                    entry["pin"] = None
                    self.errors[identity] = "pin_unconfirmed"
        return result

    def validate(self, frame, bars, *, account_id):
        if not self.enabled:
            if "external_features" in frame:
                raise ProtocolError("ordinary margin frame cannot acquire external inputs")
            return
        documents = frame.get("external_features")
        if type(documents) is not dict or set(documents) != set(self.bindings):
            raise ProtocolError("frozen external feature owners differ from configuration")
        by_instrument = {bar.instrument: bar for bar in bars}
        for identity, bindings in self.bindings.items():
            entries = documents[identity]
            if type(entries) is not list or len(entries) != len(bindings):
                raise ProtocolError("frozen external binding count differs")
            for binding, entry in zip(bindings, entries, strict=True):
                if type(entry) is not dict or set(entry) != {"delivery", "pin"}:
                    raise ProtocolError("invalid frozen external proof document")
                delivery = ExternalFeatureDelivery.parse(entry["delivery"])
                candle = by_instrument[binding.instrument]
                if (
                    delivery.binding != binding
                    or not delivery.matches_candle(candle)
                    or delivery.decision_at_ms != frame["as_of_ms"]
                ):
                    raise ProtocolError("frozen external proof differs from its candle/cutoff")
                if set(binding.fields).intersection(frame["features"][identity]):
                    raise ProtocolError("analysis and external feature ownership overlaps")
                if entry["pin"] is not None:
                    pin = ExternalFeaturePin.parse(entry["pin"])
                    snapshot = delivery.snapshot
                    if (
                        delivery.disposition != "ready"
                        or snapshot is None
                        or pin.provider_id != binding.provider_id
                        or pin.consumer_id != self.consumer_id(account_id, identity)
                        or pin.frame_ref != frame["digest"]
                        or pin.binding_sha256 != binding.binding_sha256
                        or pin.snapshot_sha256 != snapshot.snapshot_sha256
                        or pin.durable is not True
                        or pin.created_at_ms < snapshot.accepted_at_ms
                    ):
                        raise ProtocolError("frozen external pin differs from consumer authority")

    def digest_inputs(self, frame):
        values = frame.get("external_features")
        if type(values) is not dict or set(values) != set(self.bindings):
            raise ProtocolError("invalid frozen external owner map")
        for identity, entries in values.items():
            if type(entries) is not list or len(entries) != len(self.bindings[identity]):
                raise ProtocolError("invalid frozen external binding collection")
            if any(type(entry) is not dict or "delivery" not in entry for entry in entries):
                raise ProtocolError("frozen external delivery is missing")
        return {
            identity: [entry["delivery"] for entry in entries]
            for identity, entries in values.items()
        }

    def decision_features(self, frame, identity):
        result = deepcopy(frame["features"][identity])
        for entry in frame.get("external_features", {}).get(identity, ()):
            delivery = ExternalFeatureDelivery.parse(entry["delivery"])
            if delivery.disposition != "ready" or delivery.snapshot is None:
                continue
            for name, number in delivery.snapshot.values:
                if name in result:
                    raise ProtocolError("analysis and external feature ownership overlaps")
                result[name] = number
        return result

    def authorize(self, frame, identity, candle):
        if identity not in self.bindings:
            return True
        if self.provider is None:
            return False
        try:
            for entry in frame["external_features"][identity]:
                delivery = ExternalFeatureDelivery.parse(entry["delivery"])
                if (
                    delivery.disposition != "ready"
                    or delivery.snapshot is None
                    or entry["pin"] is None
                ):
                    return False
                pin = ExternalFeaturePin.parse(entry["pin"])
                now = self.clock_ms()
                if not delivery.snapshot.accepted_at_ms <= now < delivery.snapshot.valid_until_ms:
                    return False
                if (
                    self.provider.authorize(pin, delivery.snapshot, candle, decision_at_ms=now)
                    is not True
                ):
                    return False
            return True
        except Exception:
            return False

    @contextmanager
    def entry_guard(self, frame, identity, candle):
        if identity not in self.bindings:
            yield True
            return
        stack = ExitStack()
        allowed, snapshots = self.provider is not None, []
        try:
            entries = sorted(
                frame["external_features"][identity],
                key=lambda entry: (
                    entry["delivery"]["binding"]["provider_id"],
                    entry["delivery"]["binding"]["instance_id"],
                    entry["delivery"]["binding"]["stream"],
                    entry["delivery"]["binding"]["binding_sha256"],
                ),
            )
            expected = {binding.binding_sha256 for binding in self.bindings[identity]}
            if (
                len(entries) != len(expected)
                or {entry["delivery"]["binding"]["binding_sha256"] for entry in entries} != expected
            ):
                raise ProtocolError("entry lease bindings differ from their strategy")
            for entry in entries:
                delivery = ExternalFeatureDelivery.parse(entry["delivery"])
                if (
                    not allowed
                    or delivery.disposition != "ready"
                    or delivery.snapshot is None
                    or entry["pin"] is None
                ):
                    allowed = False
                    break
                now = self.clock_ms()
                if not delivery.snapshot.accepted_at_ms <= now < delivery.snapshot.valid_until_ms:
                    allowed = False
                    break
                pin = ExternalFeaturePin.parse(entry["pin"])
                if pin.frame_ref != frame["digest"] or not delivery.matches_candle(candle):
                    raise ProtocolError("entry lease proof differs from its financial frame")
                if (
                    stack.enter_context(
                        self.provider.entry_lease(
                            pin, delivery.snapshot, candle, decision_at_ms=now
                        )
                    )
                    is not True
                ):
                    allowed = False
                    break
                snapshots.append(delivery.snapshot)
            now = self.clock_ms()
            allowed = allowed and all(
                snapshot.accepted_at_ms <= now < snapshot.valid_until_ms for snapshot in snapshots
            )
        except Exception:
            allowed = False
        if not allowed:
            try:
                stack.close()
            except Exception:
                pass  # Admission already failed; no financial dispatch is permitted.
            yield False
            return
        try:
            yield True
        finally:
            # Acquisition failures fail closed; body/financial exceptions propagate.
            stack.close()

    def release(self, frame, *, receipt_sha256):
        if self.provider is None:
            return
        for identity, entries in frame.get("external_features", {}).items():
            for entry in entries:
                if entry["pin"] is not None:
                    try:
                        self.provider.release(
                            ExternalFeaturePin.parse(entry["pin"]),
                            consumer_receipt_sha256=receipt_sha256,
                        )
                    except Exception:
                        # Immutable evidence stays retained; no financial replay.
                        self.errors[identity] = "pin_release_unconfirmed"

    def status(self, frame, bars):
        return {
            identity: {
                "new_risk_ready": frame is not None
                and self.authorize(
                    frame,
                    identity,
                    next(bar for bar in bars if bar.instrument == bindings[0].instrument),
                ),
                "error": self.errors.get(identity),
            }
            for identity, bindings in self.bindings.items()
        }
