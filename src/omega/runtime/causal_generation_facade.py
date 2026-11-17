"""Stable feature port with original-generation borrows and checked mutation ACKs.

Only the host coordinator owns the broker's journal. This module never opens a
store, starts an actor, or interprets a provider's ``closed`` flag as evidence.
"""

import threading
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.external_features import (
    ExternalFeatureBinding,
    ExternalFeatureDelivery,
    ExternalFeaturePin,
    ExternalFeatureSnapshot,
    exact_decimal_text,
    semantic_candle_sha256,
)
from omega.domain.inference import (
    InferenceCommitAck,
    checksum,
    decoded,
    digest,
    encoded,
    identity,
    timestamp,
)
from omega.domain.models import Candle, Instrument
from omega.runtime.causal_historical_carry import CarryAcceptedAck, CarryManifest
from omega.runtime.causal_recovery_records import (
    FeatureMutationRef,
    FeatureMutationRejected,
    FeatureMutationUnknown,
    MutationJournalAck,
    MutationRecord,
)


@dataclass(frozen=True, slots=True, eq=False)
class GenerationBinding:
    core_epoch: str
    generation: int
    actor_token: str
    provider: object

    def __post_init__(self):
        identity(self.core_epoch)
        identity(self.actor_token)
        if type(self.generation) is not int or not 0 <= self.generation < 2**63:
            raise ValidationError("invalid feature generation")
        if self.provider is None:
            raise ValidationError("original generation provider required")


@dataclass(frozen=True, slots=True)
class GenerationInstallFence:
    core_epoch: str
    generation: int
    control_revision: int
    actor_token: str
    operation_sha256: str
    held_serial: int
    shutdown_serial: int


@dataclass(frozen=True, slots=True)
class GenerationFacadeStatus:
    core_epoch: str | None
    generation: int | None
    actor_token: str | None
    control_revision: int
    held: bool
    settlement_open: bool
    closing: bool
    borrows: int
    entry_leases: int
    hot_mutations: int
    unknown_mutations: int
    recording_unknown: int
    pins: int
    held_serial: int
    shutdown_serial: int


@dataclass(slots=True)
class _Mutation:
    ref: FeatureMutationRef
    arguments_json: str
    binding: GenerationBinding
    phase: str = "reserved"
    result_json: str | None = None
    recording_unknown: bool = False
    terminal_ack: MutationJournalAck | None = None
    applied_witness: object | None = None
    consumption_unknown: bool = False
    carry_manifest_sha256: str | None = None
    no_effect_witness: object | None = None


@dataclass(frozen=True, slots=True, eq=False)
class _AppliedMutationWitness:
    ref: FeatureMutationRef
    ack: MutationJournalAck
    result_sha256: str


@dataclass(frozen=True, slots=True, eq=False)
class _NoEffectWitness:
    ref: FeatureMutationRef
    binding: GenerationBinding
    arguments_sha256: str
    result_json: str
    rejection_code: str


@dataclass(frozen=True, slots=True)
class _HistoricalPin:
    pin: ExternalFeaturePin
    binding: GenerationBinding
    manifest_sha256: str


class GenerationFeatureProvider:
    """Keep this object in existing sessions when the host changes generations.

    ``mutation_dispatch(provider, ref, arguments_json)`` is an owner composition
    seam. Its production implementation must dispatch with the exact ref on the
    original provider/actor outcome cache. There is no legacy pin/release fallback.
    Broker methods may wait, but must route IO to the cooperative coordinator.
    """

    def __init__(self, broker, clock_ms, *, mutation_dispatch=None, pin_capacity=1024):
        if type(pin_capacity) is not int or not 1 <= pin_capacity <= 16384:
            raise ValidationError("invalid facade pin capacity")
        self._broker = broker
        self._clock_ms = clock_ms
        self._dispatch = mutation_dispatch or self._original_dispatch
        self._lock = threading.Lock()
        self.drain_event = threading.Event()
        self.drain_event.set()
        self._binding = None
        self._bindings = ()
        self._control_revision = 0
        self._held = True
        self._settlement_open = False
        self._closing = False
        self._held_serial = 0
        self._shutdown_serial = 0
        self._operation_sha256 = None
        self._install_fence_token = None
        self._borrows = 0
        self._entry_leases = 0
        self._hot = {}
        self._pins = {}  # (generation, full immutable pin digest) -> (binding, pin)
        self._historical_pins = {}  # full immutable pin digest -> release-only membership
        self._released = OrderedDict()  # bounded cached receipts; journal retains history
        self._pin_capacity = pin_capacity

    @staticmethod
    def _original_dispatch(provider, ref, arguments_json):
        method = getattr(provider, "dispatch_feature_mutation", None)
        if not callable(method):
            raise TradingBlocked("original provider has no checked mutation dispatch")
        return method(ref, arguments_json)

    @staticmethod
    def _metadata(binding):
        if type(binding) is not GenerationBinding:
            raise ValidationError("exact generation binding required")
        rows = binding.provider.list_bindings()
        if type(rows) is not tuple or len(rows) > 1024:
            raise ValidationError("bounded original feature bindings required")
        if any(type(row) is not ExternalFeatureBinding for row in rows):
            raise ValidationError("typed original feature binding required")
        return rows

    def bind_initial(self, binding, *, control_revision=0, held=False):
        rows = self._metadata(binding)  # Foreign callback before the facade lock.
        if type(control_revision) is not int or not 0 <= control_revision < 2**63:
            raise ValidationError("invalid host control revision")
        if type(held) is not bool:
            raise ValidationError("native initial hold flag required")
        with self._lock:
            if self._binding is not None or self._closing:
                raise TradingBlocked("feature generation is already bound or closing")
            self._binding = binding
            self._bindings = rows
            self._control_revision = control_revision
            self._held = held or self._install_fence_token is not None
            self._settlement_open = True

    def _cold_fresh_locked(self):
        return (
            self._binding is None
            and not self._closing
            and self._install_fence_token is None
            and not self._hot
            and not self._borrows
            and not self._entry_leases
            and not self._pins
            and not self._historical_pins
            and not self._released
        )

    def bind_cold_initial(self, binding, manifest, acceptance, *, control_revision=0):
        """Bind a checked new-owner candidate once, with release-only old pins.

        Host authorization comes from its original Root/certificate/head checks
        before the factory. Historical manifest data never proves old physical
        cleanup or supplies a live old ACK capability here.
        """
        if type(control_revision) is not int or not 0 <= control_revision < 2**63:
            raise ValidationError("invalid host control revision")
        if type(manifest) is not CarryManifest or type(acceptance) is not CarryAcceptedAck:
            raise ValidationError("exact cold carry manifest and acceptance required")
        with self._lock:
            if not self._cold_fresh_locked() or control_revision < self._control_revision:
                return False
            serials = (self._shutdown_serial, self._held_serial, self._control_revision)
        rows = self._metadata(binding)
        actual = binding.provider.carry_acceptance_snapshot()
        if actual is not acceptance:
            return False
        if (
            acceptance.manifest_sha256 != manifest.sha256
            or (acceptance.new_core_epoch, acceptance.new_generation, acceptance.new_actor_token)
            != (binding.core_epoch, binding.generation, binding.actor_token)
            or binding.generation != manifest.origin_generation + 1
            or binding.actor_token == manifest.origin_actor_token
            or acceptance.observed_revision != manifest.delivery_revision
            or acceptance.observed_sha256 != manifest.delivery_sha256
            or type(acceptance.commit_ack) is not InferenceCommitAck
            or acceptance.commit_ack.durable is not True
            or acceptance.commit_ack.revision != acceptance.observed_revision + 1
            or not acceptance.store_scope_token
            or not acceptance.store_binding_token
            or manifest.recording_unknown
            or manifest.unresolved_outcomes
        ):
            return False
        historical = {
            digest(pin.document()): _HistoricalPin(pin, binding, manifest.sha256)
            for pin in manifest.pins
        }
        if len(historical) != len(manifest.pins) or len(historical) > self._pin_capacity:
            return False
        for pin in manifest.pins:
            if binding.provider.contains_pin(pin) is not True:
                return False
        with self._lock:
            if not self._cold_fresh_locked() or serials != (
                self._shutdown_serial,
                self._held_serial,
                self._control_revision,
            ):
                return False
            self._binding = binding
            self._bindings = rows
            self._historical_pins = historical
            self._control_revision = control_revision
            self._operation_sha256 = None
            self._held = True
            self._settlement_open = True
            return True

    def refresh_bindings(self, binding):
        """Coordinator publishes cached metadata after a current manual action."""
        rows = self._metadata(binding)
        with self._lock:
            if self._binding is not binding or self._closing:
                return False
            self._bindings = rows
            return True

    def status(self):
        with self._lock:
            binding = self._binding
            return GenerationFacadeStatus(
                None if binding is None else binding.core_epoch,
                None if binding is None else binding.generation,
                None if binding is None else binding.actor_token,
                self._control_revision,
                self._held,
                self._settlement_open,
                self._closing,
                self._borrows,
                self._entry_leases,
                len(self._hot),
                sum(
                    row.phase.endswith("unknown") or row.consumption_unknown
                    for row in self._hot.values()
                ),
                sum(row.recording_unknown for row in self._hot.values()),
                len(self._pins) + len(self._historical_pins),
                self._held_serial,
                self._shutdown_serial,
            )

    def hold(self, operation_sha256):
        checksum(operation_sha256)
        with self._lock:
            if self._binding is None or self._closing:
                raise TradingBlocked("no recoverable feature generation")
            if self._operation_sha256 != operation_sha256:
                self._held_serial += 1
                self._control_revision += 1
            self._held = True
            self._operation_sha256 = operation_sha256
            return self._fence_locked()

    def _fence_locked(self):
        binding = self._binding
        return GenerationInstallFence(
            binding.core_epoch,
            binding.generation,
            self._control_revision,
            binding.actor_token,
            self._operation_sha256,
            self._held_serial,
            self._shutdown_serial,
        )

    def _matches_locked(self, fence):
        return (
            type(fence) is GenerationInstallFence
            and self._binding is not None
            and self._held
            and not self._closing
            and self._install_fence_token is None
            and fence == self._fence_locked()
        )

    def close_settlement(self, fence):
        with self._lock:
            if not self._matches_locked(fence):
                return False
            self._settlement_open = False
            return True

    def install_cas(self, fence, candidate):
        """Host validates durable proof first; this method only swaps the pointer.

        No-pin installation is deliberately limited. Historical carry must have
        its own checked writer port before a future install can retain old pins.
        """
        rows = self._metadata(candidate)
        with self._lock:
            old = self._binding
            if (
                not self._matches_locked(fence)
                or self._settlement_open
                or self._borrows
                or self._entry_leases
                or self._hot
                or self._pins
                or self._historical_pins
                or candidate.core_epoch != old.core_epoch
                or candidate.generation != old.generation + 1
                or candidate.actor_token == old.actor_token
                or candidate.provider is old.provider
            ):
                return False
            self._binding = candidate
            self._bindings = rows
            self._control_revision += 1
            self._operation_sha256 = None
            self._held = True
            self._settlement_open = True
            return True

    def settlement_pin_documents(self):
        """Exact current and historical obligations; IO0 private Host snapshot."""
        with self._lock:
            return self._settlement_documents_locked()

    def is_current_binding(self, binding):
        """Original pointer witness; equal generation tokens grant no authority."""
        with self._lock:
            return type(binding) is GenerationBinding and self._binding is binding

    def _settlement_documents_locked(self):
        documents = {sha: encoded(pin.document()) for (_, sha), (_, pin) in self._pins.items()}
        for sha, row in self._historical_pins.items():
            document = encoded(row.pin.document())
            if sha in documents and documents[sha] != document:
                raise ValidationError("historical feature membership conflicts")
            documents[sha] = document
        return tuple(documents[sha] for sha in sorted(documents))

    def install_carry_cas(self, fence, candidate, manifest, acceptance):
        """Install exact checked old obligations as successor release-only pins.

        The Host has already validated original physical/current-write evidence
        and durably recorded the manifest and actual candidate acceptance. Those
        checks are not inferred from these immutable values or a parsed pin.
        """
        if type(manifest) is not CarryManifest or type(acceptance) is not CarryAcceptedAck:
            raise ValidationError("exact carry manifest and acceptance required")
        rows = self._metadata(candidate)
        # All original-provider observations occur outside the facade lock.
        actual = candidate.provider.carry_acceptance_snapshot()
        if actual is not acceptance:
            return False
        if (
            acceptance.manifest_sha256 != manifest.sha256
            or (acceptance.new_core_epoch, acceptance.new_generation, acceptance.new_actor_token)
            != (candidate.core_epoch, candidate.generation, candidate.actor_token)
            or acceptance.observed_revision != manifest.delivery_revision
            or acceptance.observed_sha256 != manifest.delivery_sha256
            or type(acceptance.commit_ack) is not InferenceCommitAck
            or acceptance.commit_ack.durable is not True
            or acceptance.commit_ack.revision != acceptance.observed_revision + 1
            or not acceptance.store_scope_token
            or not acceptance.store_binding_token
            or manifest.recording_unknown
            or manifest.unresolved_outcomes
        ):
            return False
        manifest_documents = tuple(
            encoded(pin.document())
            for pin in sorted(manifest.pins, key=lambda pin: digest(pin.document()))
        )
        if len(manifest_documents) > self._pin_capacity or len(set(manifest_documents)) != len(
            manifest_documents
        ):
            return False
        for pin in manifest.pins:
            if candidate.provider.contains_pin(pin) is not True:
                return False
        with self._lock:
            old = self._binding
            if (
                not self._matches_locked(fence)
                or self._settlement_open
                or self._borrows
                or self._entry_leases
                or self._hot
                or candidate.core_epoch != old.core_epoch
                or candidate.generation != old.generation + 1
                or candidate.actor_token == old.actor_token
                or candidate.provider is old.provider
                or (
                    manifest.origin_core_epoch,
                    manifest.origin_generation,
                    manifest.origin_actor_token,
                )
                != (old.core_epoch, old.generation, old.actor_token)
                or manifest_documents != self._settlement_documents_locked()
            ):
                return False
            self._historical_pins = {
                digest(pin.document()): _HistoricalPin(pin, candidate, manifest.sha256)
                for pin in manifest.pins
            }
            self._pins.clear()
            self._binding = candidate
            self._bindings = rows
            self._control_revision += 1
            self._operation_sha256 = None
            self._held = True
            self._settlement_open = True
            return True

    def enable_current(self, generation, actor_token, *, expected_control_revision):
        """Called only after a separate explicit current manual stream action."""
        with self._lock:
            binding = self._binding
            if (
                binding is None
                or self._closing
                or self._install_fence_token is not None
                or self._hot
                or self._operation_sha256 is not None
                or type(generation) is not int
                or generation != binding.generation
                or actor_token != binding.actor_token
                or type(expected_control_revision) is not int
                or expected_control_revision != self._control_revision
            ):
                return False
            self._held = False
            return True

    def fence_install(self, operation_id):
        """Fence install/new risk before the host publishes a shutdown interlock.

        Existing original-pin settlement retains its independent open gate.
        This transition is memory-only and never invokes the original provider.
        """
        identity(operation_id)
        with self._lock:
            if self._closing:
                return False
            if self._install_fence_token is not None:
                return self._install_fence_token == operation_id
            self._install_fence_token = operation_id
            self._shutdown_serial += 1
            self._control_revision += 1
            self._held = True
            return True

    def clear_install_fence(self, operation_id):
        """Cancel only the matching interlock; remain held for explicit action."""
        identity(operation_id)
        with self._lock:
            if self._closing or self._install_fence_token != operation_id:
                return False
            self._install_fence_token = None
            self._shutdown_serial += 1
            self._control_revision += 1
            self._held = True
            return True

    def fence_shutdown(self):
        with self._lock:
            if not self._closing:
                self._shutdown_serial += 1
                self._control_revision += 1
            self._closing = True
            self._held = True
            self._settlement_open = False

    def list_bindings(self):
        with self._lock:
            return () if self._held or self._closing else self._bindings

    def describe(self, provider_id, instance_id, stream, instrument):
        for value in (provider_id, instance_id, stream):
            identity(value)
        if type(instrument) is not Instrument:
            raise ValidationError("typed external instrument required")
        return next(
            (
                row
                for row in self.list_bindings()
                if (row.provider_id, row.instance_id, row.stream, row.instrument)
                == (provider_id, instance_id, stream, instrument)
            ),
            None,
        )

    def _borrow(self, *, entry=False):
        with self._lock:
            if self._binding is None or self._held or self._closing:
                return None
            self._borrows += 1
            self._entry_leases += int(entry)
            self.drain_event.clear()
            return self._binding

    def _return_borrow(self, *, entry=False):
        with self._lock:
            self._borrows -= 1
            self._entry_leases -= int(entry)
            if self._borrows == 0:
                self.drain_event.set()

    def _unavailable(self, binding, candle):
        if type(binding) is not ExternalFeatureBinding or type(candle) is not Candle:
            raise ValidationError("typed external binding and candle required")
        now = timestamp(self._clock_ms())
        with self._lock:
            epoch = "unavailable" if self._binding is None else self._binding.core_epoch
        return ExternalFeatureDelivery(
            binding,
            "unavailable",
            None,
            epoch,
            binding.instance_id,
            "unavailable",
            semantic_candle_sha256(candle),
            candle.open_time_ms,
            now,
            now,
            "generation_held",
        )

    def _cached_call(self, method, binding, candle, **kwargs):
        if type(binding) is not ExternalFeatureBinding or type(candle) is not Candle:
            raise ValidationError("typed external binding and candle required")
        original = self._borrow()
        if original is None:
            return self._unavailable(binding, candle)
        try:
            result = getattr(original.provider, method)(binding, candle, **kwargs)
            with self._lock:
                current = self._binding is original and not self._held and not self._closing
                historical = (
                    type(result) is ExternalFeatureDelivery
                    and result.snapshot is not None
                    and any(
                        row.pin.snapshot_sha256 == result.snapshot.snapshot_sha256
                        for row in self._historical_pins.values()
                    )
                )
            if not current or historical:
                return self._unavailable(binding, candle)
            if type(result) is not ExternalFeatureDelivery:
                raise ValidationError("typed original delivery required")
            return result
        finally:
            self._return_borrow()

    def lookup(self, binding, candle, *, decision_at_ms):
        timestamp(decision_at_ms)
        return self._cached_call("lookup", binding, candle, decision_at_ms=decision_at_ms)

    def offer_committed(self, binding, candle):
        return self._cached_call("offer_committed", binding, candle)

    @staticmethod
    def _pin_key(pin, generation):
        if type(pin) is not ExternalFeaturePin:
            raise ValidationError("exact immutable feature pin required")
        return generation, digest(pin.document())

    def _member_locked(self, pin, binding):
        key = self._pin_key(pin, binding.generation)
        return key[1] not in self._historical_pins and self._pins.get(key) == (binding, pin)

    def authorize(self, pin, snapshot, candle, *, decision_at_ms):
        self._risk_arguments(pin, snapshot, candle, decision_at_ms)
        original = self._borrow()
        if original is None:
            return False
        try:
            with self._lock:
                member = self._member_locked(pin, original)
            if not member:
                return False
            result = original.provider.authorize(
                pin, snapshot, candle, decision_at_ms=decision_at_ms
            )
            with self._lock:
                return (
                    result is True
                    and self._binding is original
                    and not self._held
                    and not self._closing
                    and self._member_locked(pin, original)
                )
        finally:
            self._return_borrow()

    @staticmethod
    def _risk_arguments(pin, snapshot, candle, decision_at_ms):
        if (
            type(pin) is not ExternalFeaturePin
            or type(snapshot) is not ExternalFeatureSnapshot
            or type(candle) is not Candle
        ):
            raise ValidationError("exact external risk proofs required")
        timestamp(decision_at_ms)

    @contextmanager
    def entry_lease(self, pin, snapshot, candle, *, decision_at_ms):
        self._risk_arguments(pin, snapshot, candle, decision_at_ms)
        original = self._borrow(entry=True)
        if original is None:
            yield False
            return
        try:
            with self._lock:
                member = self._member_locked(pin, original)
            if not member:
                yield False
                return
            # Do not acquire the facade lock inside this context, even on exit.
            with original.provider.entry_lease(
                pin, snapshot, candle, decision_at_ms=decision_at_ms
            ) as admitted:
                yield admitted is True
        finally:
            # The original provider context has fully exited before this lock.
            self._return_borrow(entry=True)

    @staticmethod
    def _pin_arguments(consumer_id, frame_ref, snapshot, candle, decision_at_ms):
        identity(consumer_id)
        checksum(frame_ref)
        if type(snapshot) is not ExternalFeatureSnapshot or type(candle) is not Candle:
            raise ValidationError("exact external snapshot and candle required")
        timestamp(decision_at_ms)
        if not snapshot.matches_candle(candle):
            raise ValidationError("feature mutation candle differs")
        pin_id = digest(
            {
                "provider_id": snapshot.binding.provider_id,
                "consumer_id": consumer_id,
                "frame_ref": frame_ref,
                "binding_sha256": snapshot.binding.binding_sha256,
                "snapshot_sha256": snapshot.snapshot_sha256,
            }
        )
        return pin_id, encoded(
            {
                "consumer_id": consumer_id,
                "frame_ref": frame_ref,
                "snapshot": snapshot.document(),
                "decision_at_ms": decision_at_ms,
                "candle": {
                    "instrument": candle.instrument.key,
                    "open_time_ms": candle.open_time_ms,
                    "close_time_ms": candle.close_time_ms,
                    **{
                        name: exact_decimal_text(getattr(candle, name))
                        for name in ("open", "high", "low", "close", "volume")
                    },
                },
                "candle_sha256": semantic_candle_sha256(candle),
            }
        )

    def pin(self, consumer_id, frame_ref, snapshot, candle, *, decision_at_ms):
        pin_id, arguments_json = self._pin_arguments(
            consumer_id, frame_ref, snapshot, candle, decision_at_ms
        )
        return self._mutate("pin", pin_id, arguments_json)

    def release(self, pin, *, consumer_receipt_sha256):
        if type(pin) is not ExternalFeaturePin:
            raise ValidationError("exact immutable feature pin required")
        checksum(consumer_receipt_sha256)
        arguments_json = encoded(
            {
                "pin": pin.document(),
                "consumer_receipt_sha256": consumer_receipt_sha256,
            }
        )
        return self._mutate("release", pin.pin_id, arguments_json, pin=pin)

    def _reserve(self, kind, pin_id, arguments_json, pin):
        arguments = decoded(arguments_json)
        args_sha = digest(arguments)
        with self._lock:
            original = self._binding
            if original is None or self._closing:
                raise TradingBlocked("feature generation is unavailable")
            if kind == "pin" and self._held:
                raise TradingBlocked("feature generation is held")
            if kind == "pin" and any(
                row.pin.pin_id == pin_id
                or row.pin.snapshot_sha256 == arguments["snapshot"]["snapshot_sha256"]
                for row in self._historical_pins.values()
            ):
                raise TradingBlocked("historical feature pin permits settlement only")
            lookup_only = False
            historical = None
            if kind == "release":
                key = self._pin_key(pin, original.generation)
                receipt = decoded(arguments_json)["consumer_receipt_sha256"]
                previous = [value[0] for (_, sha), value in self._released.items() if sha == key[1]]
                if previous:
                    if any(value != receipt for value in previous):
                        raise ValidationError("consumed feature receipt conflicts")
                    return None
                historical = self._historical_pins.get(key[1])
                if historical is not None and (
                    historical.binding is not original or historical.pin != pin
                ):
                    raise TradingBlocked("historical feature membership differs")
                member = historical is not None or self._member_locked(pin, original)
                if member and not self._settlement_open:
                    raise TradingBlocked("original feature pin settlement is unavailable")
                lookup_only = not member
            for row in self._hot.values():
                if row.binding is original and row.ref.kind == kind and row.ref.pin_id == pin_id:
                    if row.ref.arguments_sha256 != args_sha:
                        raise ValidationError("immutable feature mutation arguments conflict")
                    raise FeatureMutationUnknown(
                        row.ref,
                        row.phase.split("_")[0] if row.phase.endswith("unknown") else "unknown",
                        recording_unknown=row.recording_unknown,
                    )
            if len(self._hot) >= 128 or (
                kind == "pin"
                and (
                    sum(row.ref.kind == "pin" for row in self._hot.values()) >= 96
                    or len(self._pins)
                    + len(self._historical_pins)
                    + sum(row.ref.kind == "pin" for row in self._hot.values())
                    >= self._pin_capacity
                )
            ):
                raise TradingBlocked("bounded feature mutation evidence is full")
            token = digest(
                {
                    "core_epoch": original.core_epoch,
                    "generation": original.generation,
                    "actor_token": original.actor_token,
                    "kind": kind,
                    "pin_id": pin_id,
                    "arguments_sha256": args_sha,
                }
            )
            ref = FeatureMutationRef(
                original.core_epoch,
                original.generation,
                original.actor_token,
                token,
                kind,
                pin_id,
                args_sha,
            )
            row = _Mutation(ref, arguments_json, original)
            if historical is not None:
                row.carry_manifest_sha256 = historical.manifest_sha256
            if lookup_only:
                row.phase = "lookup_only"
            self._hot[ref.mutation_token] = row
            self._borrows += 1
            self.drain_event.clear()
            return row

    @staticmethod
    def _check_ack(ack, ref, state, result_json=None):
        if (
            type(ack) is not MutationJournalAck
            or ack.durable is not True
            or ack.ref_sha256 != ref.sha256
            or ack.state != state
            or (state == "terminal" and ack.result_json != result_json)
        ):
            raise StorageError("exact durable feature mutation ACK is unconfirmed")

    def _unknown(self, row, phase, *, recording_unknown=False):
        with self._lock:
            row.phase = phase
            row.recording_unknown |= recording_unknown
        try:
            ack = self._broker.mark_unknown(row.ref, phase)
            if type(ack) is MutationJournalAck and ack.state == "terminal":
                self._check_ack(ack, row.ref, "terminal", ack.result_json)
                if decoded(ack.result_json) == {"not_dispatched": True} and (
                    phase == "prepare_unknown" or row.result_json == ack.result_json
                ):
                    return
                self._validate_result(row, ack.result_json)
                with self._lock:
                    if row.result_json is not None and row.result_json != ack.result_json:
                        raise ValidationError("late original mutation result conflicts")
                    row.result_json = ack.result_json
                    row.phase = "terminal_unknown"
            else:
                self._check_ack(ack, row.ref, "unknown")
        except TimeoutError:
            pass  # A lost broker waiter is not evidence that recording failed.
        except FeatureMutationUnknown as error:
            with self._lock:
                row.recording_unknown |= error.recording_unknown
        except FeatureMutationRejected:
            pass  # No new IO; the original preparation remains unconfirmed.
        except BaseException:
            with self._lock:
                row.recording_unknown = True

    def _mutate(self, kind, pin_id, arguments_json, *, pin=None):
        row = self._reserve(kind, pin_id, arguments_json, pin)
        if row is None:
            return None
        try:
            if row.phase == "lookup_only":
                return self._lookup_release(row)
            try:
                ack = self._broker.prepare_mutation(row.ref, row.arguments_json)
                if type(ack) is MutationJournalAck and ack.state == "terminal":
                    self._check_ack(ack, row.ref, "terminal", ack.result_json)
                else:
                    self._check_ack(ack, row.ref, "prepared")
            except FeatureMutationRejected:
                with self._lock:
                    self._hot.pop(row.ref.mutation_token, None)
                raise
            except BaseException as error:
                unknown = self._broker_recording_unknown(error)
                self._unknown(row, "prepare_unknown", recording_unknown=unknown)
                raise FeatureMutationUnknown(
                    row.ref, "prepare", recording_unknown=row.recording_unknown
                ) from error
            if ack.state == "terminal":
                if decoded(ack.result_json) == {"not_dispatched": True}:
                    self._commit(row, ack.result_json, ack)
                    raise FeatureMutationRejected("original_mutation_not_dispatched")
                return self._commit(row, ack.result_json, ack)
            with self._lock:
                row.phase = "prepared"
            try:
                result = self._dispatch(row.binding.provider, row.ref, row.arguments_json)
                result_json = self.result_json(row.ref, row.arguments_json, result)
                self._validate_result(row, result_json)
            except FeatureMutationRejected as error:
                if (
                    type(error) is not FeatureMutationRejected
                    or error.dispatched is not False
                    or error.recording_unknown is not False
                ):
                    unknown = error.recording_unknown is True
                    self._unknown(row, "dispatch_unknown", recording_unknown=unknown)
                    raise FeatureMutationUnknown(row.ref, "dispatch", unknown) from error
                self._mint_no_effect_witness(row, error.code)
                self._finish_not_dispatched(row)
                raise
            except BaseException as error:
                unknown = isinstance(error, FeatureMutationUnknown) and error.recording_unknown
                self._unknown(row, "dispatch_unknown", recording_unknown=unknown)
                raise FeatureMutationUnknown(
                    row.ref, "dispatch", recording_unknown=row.recording_unknown
                ) from error
            with self._lock:
                row.phase = "result_observed"
                row.result_json = result_json
            try:
                ack = self._broker.finish_mutation(row.ref, result_json)
                self._check_ack(ack, row.ref, "terminal", result_json)
            except BaseException as error:
                unknown = self._broker_recording_unknown(error)
                self._unknown(row, "terminal_unknown", recording_unknown=unknown)
                raise FeatureMutationUnknown(
                    row.ref, "terminal", recording_unknown=row.recording_unknown
                ) from error
            return self._commit(row, result_json, ack)
        finally:
            if row.phase == "lookup_only":
                with self._lock:
                    self._hot.pop(row.ref.mutation_token, None)
                    self._prune_released_locked()
            self._return_borrow()

    def _finish_not_dispatched(self, row):
        result_json = encoded({"not_dispatched": True})
        with self._lock:
            row.result_json = result_json
            row.phase = "no_effect_observed"
        try:
            finish = getattr(self._broker, "finish_not_dispatched_mutation", None)
            if callable(finish):
                ack = finish(row.ref, row.no_effect_witness)
            else:
                # Backward-compatible standalone test brokers have no owner
                # outcome cache. The actual Host always supplies the strong port.
                ack = self._broker.finish_mutation(row.ref, result_json)
            self._check_ack(ack, row.ref, "terminal", result_json)
        except BaseException as error:
            self._unknown(
                row, "terminal_unknown", recording_unknown=self._broker_recording_unknown(error)
            )
            raise FeatureMutationUnknown(
                row.ref, "terminal", recording_unknown=row.recording_unknown
            ) from error
        self._commit(row, result_json, ack)

    def _mint_no_effect_witness(self, row, rejection_code):
        identity(rejection_code)
        with self._lock:
            if (
                self._hot.get(row.ref.mutation_token) is not row
                or self._binding is not row.binding
                or row.phase != "prepared"
                or row.recording_unknown
                or row.result_json is not None
            ):
                raise FeatureMutationUnknown(row.ref, "dispatch", row.recording_unknown)
            result_json = encoded({"not_dispatched": True})
            row.no_effect_witness = _NoEffectWitness(
                row.ref, row.binding, row.ref.arguments_sha256, result_json, rejection_code
            )
            row.result_json = result_json
            row.phase = "no_effect_observed"

    def _no_effect_matches_locked(self, witness, ref):
        if type(witness) is not _NoEffectWitness or type(ref) is not FeatureMutationRef:
            return False
        row = self._hot.get(ref.mutation_token)
        return (
            row is not None
            and row.ref == ref
            and witness.ref is row.ref
            and row.no_effect_witness is witness
            and row.binding is witness.binding
            and self._binding is row.binding
            and not row.recording_unknown
            and row.phase in {"no_effect_observed", "terminal_unknown", "terminal_applied"}
            and witness.arguments_sha256 == row.ref.arguments_sha256
            and witness.result_json == row.result_json == encoded({"not_dispatched": True})
        )

    def no_effect_witness(self, ref):
        """Original rejection capability only; no provider callback or IO."""
        with self._lock:
            row = self._hot.get(ref.mutation_token) if type(ref) is FeatureMutationRef else None
            witness = None if row is None else row.no_effect_witness
            return witness if self._no_effect_matches_locked(witness, ref) else None

    def verify_no_effect_witness(self, witness, ref):
        """Equal data/JSON/foreign generations never create no-effect authority."""
        with self._lock:
            return self._no_effect_matches_locked(witness, ref)

    def _lookup_release(self, row):
        lookup = getattr(self._broker, "lookup_mutation", None)
        if not callable(lookup):
            raise TradingBlocked("original consumed receipt is not cached")
        record = lookup(row.ref)  # Read-only coordinator request, outside facade lock.
        if (
            type(record) is not MutationRecord
            or record.ref != row.ref
            or record.arguments_json != row.arguments_json
            or record.ack.state != "terminal"
        ):
            raise TradingBlocked("original consumed receipt is not confirmed")
        self._check_ack(record.ack, row.ref, "terminal", record.ack.result_json)
        self._validate_result(row, record.ack.result_json)
        args = decoded(row.arguments_json)
        with self._lock:
            self._cache_release_locked(row.binding.generation, args)
        return None

    def _cache_release_locked(self, generation, args):
        pin = ExternalFeaturePin.parse(args["pin"])
        key = self._pin_key(pin, generation)
        previous = self._released.get(key)
        value = (args["consumer_receipt_sha256"], pin.pin_id)
        if previous is not None and previous != value:
            raise ValidationError("original consumed receipt conflicts")
        self._released[key] = value
        self._released.move_to_end(key)
        self._prune_released_locked()

    def _prune_released_locked(self):
        # A release that races an admitted pin replay must survive until that
        # replay has rechecked membership. Only completed, unprotected cache rows
        # can evict; original durable evidence remains in the checked journal.
        protected = {
            (row.binding.generation, row.ref.pin_id)
            for row in self._hot.values()
            if row.ref.kind == "pin"
        }
        unprotected = [
            key for key, (_, pin_id) in self._released.items() if (key[0], pin_id) not in protected
        ]
        for key in unprotected[:-128]:
            del self._released[key]

    @staticmethod
    def _broker_recording_unknown(error):
        if isinstance(error, FeatureMutationUnknown):
            return error.recording_unknown
        return not isinstance(error, TimeoutError)

    @staticmethod
    def result_json(ref, arguments_json, result):
        """Owner outcome codec; result object must come from the original actor."""
        if type(ref) is not FeatureMutationRef:
            raise ValidationError("exact original mutation reference required")
        args = decoded(arguments_json)
        if digest(args) != ref.arguments_sha256:
            raise ValidationError("original mutation argument digest differs")
        if ref.kind == "pin":
            if type(result) is not ExternalFeaturePin:
                raise ValidationError("typed original feature pin result required")
            return encoded({"pin": result.document()})
        if result is not None:
            raise ValidationError("release result must be exact None")
        return encoded(
            {
                "released": True,
                "pin_sha256": digest(args["pin"]),
                "consumer_receipt_sha256": args["consumer_receipt_sha256"],
            }
        )

    @staticmethod
    def _validate_result(row, result_json):
        document = decoded(result_json)
        args = decoded(row.arguments_json)
        if row.ref.kind == "release":
            if (
                document
                != {
                    "released": True,
                    "pin_sha256": digest(args["pin"]),
                    "consumer_receipt_sha256": args["consumer_receipt_sha256"],
                }
                or type(document.get("released")) is not bool
            ):
                raise ValidationError("release result differs from exact original receipt")
            return None
        if type(document) is not dict or set(document) != {"pin"}:
            raise ValidationError("exact original pin result envelope required")
        pin = ExternalFeaturePin.parse(document["pin"])
        snapshot = ExternalFeatureSnapshot.parse(args["snapshot"])
        if (
            pin.pin_id != row.ref.pin_id
            or pin.consumer_id != args["consumer_id"]
            or pin.frame_ref != args["frame_ref"]
            or pin.binding_sha256 != snapshot.binding.binding_sha256
            or pin.snapshot_sha256 != snapshot.snapshot_sha256
            or pin.provider_id != snapshot.binding.provider_id
        ):
            raise ValidationError("original feature pin result differs from immutable request")
        return pin

    def _commit(self, row, result_json, terminal_ack):
        self._check_ack(terminal_ack, row.ref, "terminal", result_json)
        not_dispatched = decoded(result_json) == {"not_dispatched": True}
        if not_dispatched and callable(
            getattr(self._broker, "finish_not_dispatched_mutation", None)
        ):
            with self._lock:
                allowed = row.phase == "prepare_unknown" or self._no_effect_matches_locked(
                    row.no_effect_witness, row.ref
                )
                if not allowed:
                    if row.phase == "reserved":
                        row.phase = "terminal_unknown"
                    raise FeatureMutationUnknown(row.ref, "terminal", row.recording_unknown)
        result = None if not_dispatched else self._validate_result(row, result_json)
        args = decoded(row.arguments_json)
        member = False
        if row.ref.kind == "pin" and not not_dispatched:
            try:
                member = row.binding.provider.contains_pin(result)
                if type(member) is not bool:
                    raise ValidationError("native original pin membership required")
            except Exception as error:
                # Failed membership readback cannot become either positive risk
                # authority or a false absence that prevents original settlement.
                with self._lock:
                    row.result_json = result_json
                    row.phase = "terminal_unknown"
                raise FeatureMutationUnknown(row.ref, "terminal", False) from error
        with self._lock:
            if self._hot.get(row.ref.mutation_token) is not row:
                raise TradingBlocked("original mutation is no longer pending")
            if not_dispatched:
                pass
            elif row.ref.kind == "pin":
                key = self._pin_key(result, row.binding.generation)
                # An old terminal replay cannot resurrect a consumed membership.
                if (
                    member
                    and key[1] not in self._historical_pins
                    and not any(sha == key[1] for _, sha in self._released)
                ):
                    self._pins[key] = (row.binding, result)
            else:
                pin = ExternalFeaturePin.parse(args["pin"])
                key = self._pin_key(pin, row.binding.generation)
                historical = self._historical_pins.get(key[1])
                if historical is not None:
                    if (
                        historical.binding is not row.binding
                        or historical.pin != pin
                        or historical.manifest_sha256 != row.carry_manifest_sha256
                    ):
                        raise ValidationError("historical settlement lineage changed")
                self._cache_release_locked(row.binding.generation, args)
                self._pins.pop(key, None)
                if historical is not None:
                    del self._historical_pins[key[1]]
            row.result_json = result_json
            row.terminal_ack = terminal_ack
            row.phase = "terminal_applied"
            row.applied_witness = _AppliedMutationWitness(
                row.ref, terminal_ack, digest(decoded(result_json))
            )
        self._consume_applied(row)
        return result

    def applied_mutation(self, ref, ack):
        """Private IO0 witness of this original row's exact terminal application."""
        with self._lock:
            row = self._hot.get(ref.mutation_token) if type(ref) is FeatureMutationRef else None
            if (
                row is None
                or row.ref != ref
                or row.phase != "terminal_applied"
                or row.terminal_ack is not ack
                or row.result_json != ack.result_json
            ):
                return None
            return row.applied_witness

    def verify_applied_mutation(self, witness, ref, ack):
        if type(witness) is not _AppliedMutationWitness:
            return False
        return (
            witness.ref == ref and witness.ack is ack and self.applied_mutation(ref, ack) is witness
        )

    def _consume_applied(self, row):
        with self._lock:
            if self._hot.get(row.ref.mutation_token) is not row or row.phase != "terminal_applied":
                raise TradingBlocked("original terminal application is no longer pending")
            ack, witness = row.terminal_ack, row.applied_witness
        try:
            consumed = self._broker.consume_terminal(row.ref, ack, witness)
            if consumed is not True:
                raise FeatureMutationUnknown(row.ref, "terminal", False)
        except BaseException as error:
            with self._lock:
                if row.phase == "terminal_consumed":
                    return  # Another waiter positively consumed this same row.
                row.consumption_unknown = True
                if isinstance(error, FeatureMutationUnknown):
                    row.recording_unknown |= error.recording_unknown
            raise FeatureMutationUnknown(
                row.ref, "terminal", recording_unknown=row.recording_unknown
            ) from error
        with self._lock:
            if (
                row.phase == "terminal_consumed"
                and row.terminal_ack is ack
                and row.applied_witness is witness
            ):
                return
            if (
                self._hot.get(row.ref.mutation_token) is not row
                or row.terminal_ack is not ack
                or row.applied_witness is not witness
            ):
                raise TradingBlocked("original terminal application changed")
            row.phase = "terminal_consumed"
            row.consumption_unknown = False
            del self._hot[row.ref.mutation_token]
            self._prune_released_locked()

    def reconcile_mutation(self, ref, terminal_ack):
        """Host first verifies original owner settlement and durably records it.

        Recording uncertainty is a separate sticky barrier; an RPC-only unknown
        can be settled by the exact original result. This method never dispatches.
        """
        if type(ref) is not FeatureMutationRef:
            raise ValidationError("exact original mutation reference required")
        with self._lock:
            row = self._hot.get(ref.mutation_token)
            if row is None or row.ref != ref or row.recording_unknown:
                return False
            applied = row.phase == "terminal_applied"
            if not applied and row.phase not in {"dispatch_unknown", "terminal_unknown"}:
                return False
            if applied and row.terminal_ack is not terminal_ack:
                return False
        if applied:
            self._consume_applied(row)
            return True
        self._check_ack(terminal_ack, ref, "terminal", terminal_ack.result_json)
        if terminal_ack.result_json is None:
            raise ValidationError("terminal original mutation result required")
        if row.result_json is not None and row.result_json != terminal_ack.result_json:
            raise ValidationError("late terminal ACK differs from original observed result")
        self._commit(row, terminal_ack.result_json, terminal_ack)
        return True

    def settle_not_dispatched(self, ref, terminal_ack, *, witness=None):
        """Original preparation/rejection settlement; never call the provider.

        The coordinator must obtain the original preparation ticket's ACK and
        durably bind this non-dispatch outcome before passing its terminal ACK.
        """
        if type(ref) is not FeatureMutationRef:
            raise ValidationError("exact original mutation reference required")
        result_json = encoded({"not_dispatched": True})
        self._check_ack(terminal_ack, ref, "terminal", result_json)
        with self._lock:
            row = self._hot.get(ref.mutation_token)
            if row is None or row.ref != ref or row.recording_unknown:
                return False
            applied = row.phase == "terminal_applied"
            if applied:
                if row.terminal_ack is not terminal_ack:
                    return False
                if row.no_effect_witness is not None and not self._no_effect_matches_locked(
                    witness, ref
                ):
                    return False
            elif row.phase == "prepare_unknown":
                if witness is not None:
                    return False
            elif row.phase != "terminal_unknown" or not self._no_effect_matches_locked(
                witness, ref
            ):
                return False
        if applied:
            self._consume_applied(row)
            return True
        self._commit(row, result_json, terminal_ack)
        return True

    def pending_mutations(self):
        """Private host readback of immutable refs; no IO or provider callbacks."""
        with self._lock:
            return tuple(row.ref for row in self._hot.values())
