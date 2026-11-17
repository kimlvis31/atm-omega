"""Cached external-feature facade and actor-only causal proof/pin authority."""

import threading
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass, replace
from decimal import Decimal
from functools import wraps
from uuid import uuid4

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.domain.external_features import (
    ExternalFeatureBinding,
    ExternalFeatureDelivery,
    ExternalFeaturePin,
    ExternalFeatureSnapshot,
    semantic_candle_sha256,
)
from omega.domain.inference import (
    InferenceCommitAck,
    InferenceReceipt,
    checksum,
    decoded,
    digest,
    encoded,
    identity,
    timestamp,
)
from omega.domain.models import Candle
from omega.research.datasets import MarketRow
from omega.runtime.causal_feature_delivery_events import CausalFeatureEvents
from omega.runtime.causal_feature_delivery_store import (
    CausalFeatureDeliveryStore,
    producer_fingerprint,
)
from omega.runtime.causal_generation_outcomes import (
    FeatureSettlementSnapshot,
    GenerationSettlementSnapshot,
)
from omega.runtime.causal_historical_carry import CarryAcceptedAck
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_nn_proof import read_document, validate_input_document
from omega.runtime.causal_recovery_records import (
    MAX_HOT_MUTATIONS,
    RELEASE_RESERVED,
    FeatureMutationRef,
    FeatureMutationRejected,
    FeatureMutationUnknown,
    MutationJournalAck,
)


def _recording_boundary(method):
    @wraps(method)
    def protected(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except OSError:
            raise StorageError("external inference evidence recording is unconfirmed") from None

    return protected


@dataclass(frozen=True, slots=True)
class _AcknowledgedFeatureOutcome:
    outcome: FeatureSettlementSnapshot
    ack: MutationJournalAck
    confirmed: bool = False


class CausalFeatureDeliveryProvider:
    """Construction/GET have IO0. Only service actor methods register authority."""

    def __init__(self, service):
        self.service = service
        self.lock = threading.RLock()
        self.store = None
        self._current, self._latest, self._bank, self._pins = {}, {}, {}, {}
        self._proofs, self._receipts, self._tickets = {}, {}, {}
        self._generation_context = getattr(service, "_generation_context", None)
        self._generation_service = service
        self._provider_token = uuid4().hex
        self._generation_store = self._generation_raw_store = None
        self._generation_scope = self._generation_binding = None
        self._generation_recording_unknown = False
        self._generation_capture_failed = False
        self._generation_tickets, self._generation_outcomes = {}, {}
        self._generation_acknowledged = OrderedDict()
        self._generation_cache = None
        self._generation_gate_closed = False
        self._carry_acceptance_cache = None
        self.events = CausalFeatureEvents(self)

    def _actor(self):
        if self.service.thread is not threading.current_thread():
            raise StorageError("external evidence mutation must stay on its actor")
        if self._generation_context is not None:
            self._generation_identity()
            if self.service._original_actor_thread is not threading.current_thread():
                raise StorageError("external generation requires its original actor Thread")

    def _start_actor(self):
        self._actor()
        carry_install = getattr(self.service, "_carry_install", None)
        carry_options = {} if carry_install is None else {"carry_install": carry_install}
        self.store = CausalFeatureDeliveryStore(
            self.service.root,
            limits=self.service.evidence_limits,
            on_change=self.service._evidence_changed,
            leases=getattr(self.service, "_file_leases", None),
            **carry_options,
        )
        if self._generation_context is not None:
            self._bind_generation_store_actor()
            if carry_install is not None:
                acceptance = self.store.carry_acceptance_snapshot()
                if type(acceptance) is not CarryAcceptedAck:
                    raise StorageError("original historical carry acceptance unavailable")
                self._carry_acceptance_cache = acceptance
                self._restore_actor()
            self._capture_generation_settlement_actor(sealed=False)

    def carry_acceptance_snapshot(self):
        """Cached original carry-store acceptance, never a parsed replacement ACK."""
        with self.lock:
            try:
                self._generation_identity()
                raw, store = self._generation_raw_store, self._generation_store
                request = self.service._carry_install
                if (
                    request is None
                    or self.store is not store
                    or store.store is not raw
                    or self.service.closed
                    or self.service.closing
                    or self.service.error is not None
                    or raw.closed
                    or raw.failed
                    or not store._accounting_confirmed
                    or store._settlement_recording_unknown
                    or store._settlement_write_pending
                    or raw._lease_scope is not self._generation_scope
                    or raw._lease_binding is not self._generation_binding
                ):
                    return None
                result = store._carry_acceptance
                if (
                    result is not self._carry_acceptance_cache
                    or type(result) is not CarryAcceptedAck
                    or result.manifest_sha256 != request.manifest.sha256
                    or result.store_scope_token != self._generation_scope.token
                    or result.store_binding_token != self._generation_binding.token
                    or (result.new_core_epoch, result.new_generation, result.new_actor_token)
                    != (
                        self._generation_context.core_epoch,
                        self._generation_context.generation,
                        self._generation_context.actor_token,
                    )
                ):
                    return None
                return result
            except (AttributeError, ValidationError):
                return None

    def _generation_identity(self, *, host_token=None, ref=None):
        context = self._generation_context
        if (
            context is None
            or self.service is not self._generation_service
            or self.service.external_features is not self
            or self.service._generation_context is not context
            or self.service._actor_token != context.actor_token
            or self.service.core_epoch != context.core_epoch
            or host_token is not None
            and host_token != context.host_token
            or ref is not None
            and not context.matches(ref)
        ):
            raise ValidationError("original feature generation binding differs")
        return context

    def _bind_generation_store_actor(self):
        self._actor()
        self._generation_identity()
        outer, raw = self.store, self.store.store
        if type(raw) is not FileInferenceStateStore:
            raise StorageError("actual original feature file store required")
        raw._owner_guard()
        self._generation_store, self._generation_raw_store = outer, raw
        self._generation_scope, self._generation_binding = raw._lease_scope, raw._lease_binding

    def _generation_head_actor(self):
        """Inspect original owner memory only; never repair a failed recording."""
        self._actor()
        self._generation_identity()
        outer, raw = self._generation_store, self._generation_raw_store
        scope, binding = self._generation_scope, self._generation_binding
        try:
            if (
                outer is None
                or self.store is not outer
                or outer.store is not raw
                or type(raw) is not FileInferenceStateStore
                or raw._lease_scope is not scope
                or raw._lease_binding is not binding
            ):
                raise StorageError("original feature file store identity differs")
            raw._owner_guard()
            binding.guard_admission(raw)
            revision, sha = outer.revision, digest(outer.data)
            ack = outer._settlement_last_exact_ack
            exact = (
                type(ack) is InferenceCommitAck
                and ack.durable is True
                and ack is raw._last_ack
                and ack.revision == revision
                and ack.snapshot_sha256 == sha
                and outer._settlement_current_write_confirmed
            )
            unknown = (
                self._generation_recording_unknown
                or raw.failed
                or outer._accounting_confirmed is not True
                or not exact
                or outer._settlement_recording_unknown
                or outer._settlement_write_pending
            )
            self._generation_recording_unknown |= unknown
            return {
                "store_scope_token": scope.token,
                "store_binding_token": binding.token,
                "head_revision": revision,
                "head_sha256": sha,
                "last_exact_ack": ack if type(ack) is InferenceCommitAck else None,
                "current_write_confirmed": not unknown,
                "recording_unknown": bool(unknown),
                "capture_failed": self._generation_capture_failed,
            }
        except BaseException:
            self._generation_recording_unknown = True
            return {
                "store_scope_token": None if scope is None else scope.token,
                "store_binding_token": None if binding is None else binding.token,
                "head_revision": None,
                "head_sha256": None,
                "last_exact_ack": None,
                "current_write_confirmed": False,
                "recording_unknown": True,
                "capture_failed": True,
            }

    def dispatch_feature_mutation(self, ref, arguments_json):
        """Private captured-provider seam; it never redirects to another actor."""
        self._generation_identity(ref=ref)
        if type(ref) is not FeatureMutationRef or type(arguments_json) is not str:
            raise ValidationError("exact immutable feature mutation required")
        arguments = decoded(arguments_json, maximum=262144)
        if (
            type(arguments) is not dict
            or digest(arguments) != ref.arguments_sha256
            or encoded(arguments) != arguments_json
        ):
            raise ValidationError("feature mutation arguments differ from original reference")
        with self.lock:
            acknowledged = self._generation_acknowledged.get(ref.mutation_token)
            if acknowledged is not None:
                outcome = acknowledged.outcome
                if outcome.ref != ref or outcome.arguments_json != arguments_json:
                    raise ValidationError("acknowledged original feature mutation conflicts")
                if outcome.phase == "rejected":
                    raise FeatureMutationRejected("original_feature_validation_rejected")
                if outcome.phase != "complete" or outcome.result_json is None:
                    raise FeatureMutationUnknown(ref, "dispatch", outcome.recording_unknown)
                result = decoded(outcome.result_json)
                return ExternalFeaturePin.parse(result["pin"]) if ref.kind == "pin" else None
            ticket = self._generation_tickets.get(ref.mutation_token)
            if ticket is not None:
                if ticket["ref"] != ref or ticket["arguments_json"] != arguments_json:
                    raise ValidationError("original feature mutation reference conflicts")
                enqueue = False
            else:
                if self._generation_gate_closed:
                    raise FeatureMutationRejected("original_settlement_gate_closed")
                count = len(self._generation_tickets) + sum(
                    not row.confirmed for row in self._generation_acknowledged.values()
                )
                if count >= MAX_HOT_MUTATIONS or (
                    ref.kind == "pin" and count >= MAX_HOT_MUTATIONS - RELEASE_RESERVED
                ):
                    raise FeatureMutationRejected("original_mutation_capacity_unavailable")
                ticket = {
                    "kind": "generation_mutation",
                    "key": ref.mutation_token,
                    "ref": ref,
                    "arguments_json": arguments_json,
                    "event": threading.Event(),
                    "result": None,
                    "error": None,
                    "phase": "queued",
                    "recording_unknown": False,
                }
                self._generation_tickets[ref.mutation_token] = ticket
                enqueue = True
        if enqueue and not self.service._enqueue_feature_command(ticket):
            with self.lock:
                self._generation_tickets.pop(ref.mutation_token, None)
            raise FeatureMutationRejected("original_feature_actor_unavailable")
        if not ticket["event"].wait(self.service.ack_timeout_s):
            # Waiting says nothing about whether the owner has entered a write.
            raise FeatureMutationUnknown(ref, "dispatch", recording_unknown=False)
        if ticket["error"] is not None:
            raise ticket["error"]
        if ticket["phase"] != "complete":
            raise FeatureMutationUnknown(ref, "dispatch", ticket["recording_unknown"])
        result = ticket["result"]
        return ExternalFeaturePin.parse(result["pin"]) if ref.kind == "pin" else None

    def _generation_arguments_actor(self, ticket):
        ref = ticket["ref"]
        self._generation_identity(ref=ref)
        arguments = decoded(ticket["arguments_json"], maximum=262144)
        if digest(arguments) != ref.arguments_sha256:
            raise ValidationError("original mutation arguments changed")
        if ref.kind == "pin":
            if set(arguments) != {
                "consumer_id",
                "frame_ref",
                "snapshot",
                "decision_at_ms",
                "candle",
                "candle_sha256",
            }:
                raise ValidationError("exact feature pin arguments required")
            identity(arguments["consumer_id"])
            checksum(arguments["frame_ref"])
            snapshot = ExternalFeatureSnapshot.parse(arguments["snapshot"])
            row = arguments["candle"]
            if (
                type(row) is not dict
                or set(row)
                != {
                    "instrument",
                    "open_time_ms",
                    "close_time_ms",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                }
                or any(
                    type(row[name]) is not str
                    for name in ("open", "high", "low", "close", "volume")
                )
            ):
                raise ValidationError("exact decimal candle arguments required")
            candle = Candle(
                self._instrument(row["instrument"]),
                row["open_time_ms"],
                row["close_time_ms"],
                *(Decimal(row[name]) for name in ("open", "high", "low", "close", "volume")),
            )
            if semantic_candle_sha256(candle) != arguments["candle_sha256"]:
                raise ValidationError("exact feature candle digest differs")
            decision = timestamp(arguments["decision_at_ms"])
            expected = ExternalFeaturePin(
                snapshot.binding.provider_id,
                arguments["consumer_id"],
                arguments["frame_ref"],
                snapshot.binding.binding_sha256,
                snapshot.snapshot_sha256,
                self.service.clock_ms(),
            )
            if expected.pin_id != ref.pin_id:
                raise ValidationError("original feature pin reference differs")
            with self.lock:
                delivery = self._lookup(snapshot.binding, candle, decision)
                if (
                    delivery.snapshot != snapshot
                    or delivery.disposition not in {"ready", "no_signal"}
                    or self._bank.get(snapshot.snapshot_sha256) != snapshot
                ):
                    raise TradingBlocked("actual original feature authority unavailable")
            self.store.load_snapshot(snapshot.snapshot_sha256)
            return expected, snapshot, candle, decision
        if set(arguments) != {"pin", "consumer_receipt_sha256"}:
            raise ValidationError("exact feature release arguments required")
        pin = ExternalFeaturePin.parse(arguments["pin"])
        checksum(arguments["consumer_receipt_sha256"])
        if pin.pin_id != ref.pin_id:
            raise ValidationError("original feature release reference differs")
        # Positive pre-effect validation of actual immutable membership/receipt.
        if self.store._read_pin(pin.pin_id) != pin:
            raise ValidationError("release differs from actual original pin")
        consumed = self.store._path("consumed", pin.pin_id)
        if (
            consumed.exists()
            and self.store._read_consumed(pin)["consumer_receipt_sha256"]
            != arguments["consumer_receipt_sha256"]
        ):
            raise ValidationError("original consumed receipt conflicts")
        return pin, arguments["consumer_receipt_sha256"]

    def _record_generation_outcome_actor(self, ticket):
        self._actor()
        head = self._generation_head_actor()
        snapshot = FeatureSettlementSnapshot(
            ticket["ref"],
            ticket["arguments_json"],
            self._generation_context.binding_sha256,
            self._provider_token,
            phase=ticket["phase"],
            result_json=None if ticket["result"] is None else encoded(ticket["result"]),
            error_code=None
            if ticket["error"] is None
            else (
                "original_mutation_rejected"
                if isinstance(ticket["error"], FeatureMutationRejected)
                else "original_mutation_unknown"
            ),
            **head,
        )
        ticket["recording_unknown"] = snapshot.recording_unknown
        with self.lock:
            self._generation_outcomes[ticket["ref"].mutation_token] = snapshot
        return snapshot

    def _run_generation_mutation_actor(self, ticket):
        self._actor()
        try:
            ticket["phase"] = "validating"
            if self._generation_head_actor()["recording_unknown"]:
                raise StorageError("original feature recording remains unknown")
            arguments = self._generation_arguments_actor(ticket)
            ticket["phase"] = "effect_entered"
            if ticket["ref"].kind == "pin":
                pin = self.store.pin(arguments[0])
                pins = self.store.load_pins()
                with self.lock:
                    self._pins = pins
                ticket["result"] = {"pin": pin.document()}
            else:
                pin, receipt = arguments
                self.store.release(pin, receipt)
                with self.lock:
                    self._pins.pop(pin.pin_id, None)
                ticket["result"] = {
                    "released": True,
                    "pin_sha256": digest(pin.document()),
                    "consumer_receipt_sha256": receipt,
                }
            if self._generation_head_actor()["recording_unknown"]:
                raise StorageError("original feature current write ACK remains unknown")
            ticket["phase"] = "complete"
        except Exception as error:
            if ticket["phase"] == "validating" and isinstance(
                error, (ValidationError, TradingBlocked)
            ):
                ticket["phase"] = "rejected"
                ticket["error"] = FeatureMutationRejected("original_feature_validation_rejected")
            else:
                if ticket["phase"] == "effect_entered":
                    self._generation_recording_unknown = True
                ticket["phase"] = "unknown"
                ticket["error"] = FeatureMutationUnknown(
                    ticket["ref"],
                    "dispatch",
                    self._generation_head_actor()["recording_unknown"],
                )
        except BaseException:
            if ticket["phase"] == "effect_entered":
                self._generation_recording_unknown = True
            ticket["phase"] = "unknown"
            ticket["error"] = FeatureMutationUnknown(
                ticket["ref"], "dispatch", self._generation_head_actor()["recording_unknown"]
            )
            raise
        finally:
            # Preserve the owner outcome before waking or dropping any waiter.
            try:
                self._record_generation_outcome_actor(ticket)
            except BaseException:
                self._generation_capture_failed = True
                ticket["recording_unknown"] = True
                ticket["error"] = FeatureMutationUnknown(ticket["ref"], "dispatch", True)
            finally:
                ticket["event"].set()
        if ticket["recording_unknown"]:
            self.service._fault("EXTERNAL_FEATURE_RECORDING_UNCONFIRMED")

    def _capture_generation_settlement_actor(self, *, sealed):
        self._actor()
        context = self._generation_identity()
        head = self._generation_head_actor()
        pins, reservations = (), ()
        try:
            if head["capture_failed"]:
                raise StorageError("original feature head unavailable")
            actual = self._generation_store.load_pins()
            pins = tuple(actual[key] for key in sorted(actual))
            reservations = tuple(sorted(self._generation_store.data["reservations"].items()))
            if len(pins) > self._generation_store.max_pins or len(reservations) > 64:
                raise StorageError("original feature membership exceeds bounded export")
        except BaseException:
            head["capture_failed"] = True
        with self.lock:
            for key, outcome in tuple(self._generation_outcomes.items()):
                current = dict(head)
                current["recording_unknown"] |= outcome.recording_unknown
                current["capture_failed"] |= outcome.capture_failed
                current["current_write_confirmed"] &= not outcome.recording_unknown
                self._generation_outcomes[key] = replace(outcome, **current)
            for key, row in tuple(self._generation_acknowledged.items()):
                if not row.confirmed:
                    current = dict(head)
                    current["recording_unknown"] |= row.outcome.recording_unknown
                    current["capture_failed"] |= row.outcome.capture_failed
                    current["current_write_confirmed"] &= not row.outcome.recording_unknown
                    self._generation_acknowledged[key] = replace(
                        row, outcome=replace(row.outcome, **current)
                    )
            pending_outcomes = dict(self._generation_outcomes)
            pending_outcomes.update(
                {
                    key: row.outcome
                    for key, row in self._generation_acknowledged.items()
                    if not row.confirmed
                }
            )
            result = GenerationSettlementSnapshot(
                context.core_epoch,
                context.generation,
                context.actor_token,
                context.binding_sha256,
                self._provider_token,
                pins=pins,
                reservations=reservations,
                outcomes=tuple(pending_outcomes[key] for key in sorted(pending_outcomes)),
                sealed=bool(sealed and self._generation_gate_closed),
                **head,
            )
            self._generation_cache = result
        return result

    def generation_settlement_snapshot(self):
        """Cached immutable export; inspection performs no filesystem or actor call."""
        with self.lock:
            return self._generation_cache

    def _generation_control(self, operation, *, host_token, ref=None, ack=None):
        if (
            self._generation_context is None
            or type(host_token) is not str
            or (host_token != self._generation_context.host_token)
        ):
            raise ValidationError("private original host binding required")
        self._generation_identity(host_token=host_token, ref=ref)
        if self.service._original_actor_thread is threading.current_thread():
            raise StorageError("host feature settlement cannot wait on its own actor")
        with self.lock:
            if operation in {"ack", "confirm"}:
                row = self._acknowledged_feature_locked(ref, ack)
                if row is not None and (operation == "ack" or row.confirmed):
                    # This observes an already-issued original actor result;
                    # it neither retries a mutation nor creates a dead-actor ACK.
                    return True
            if operation == "seal":
                self._generation_gate_closed = True
            if self.service.closed:
                if operation == "read":
                    return self._feature_outcome_locked(ref)
                raise StorageError("retired original actor cannot acknowledge new host commands")
        ticket = {
            "kind": "generation_control",
            "key": uuid4().hex,
            "operation": operation,
            "ref": ref,
            "ack": ack,
            "host_token": host_token,
            "event": threading.Event(),
            "result": None,
            "error": None,
        }
        if not self.service._enqueue_feature_command(ticket):
            raise StorageError("original feature settlement actor unavailable")
        if not ticket["event"].wait(self.service.ack_timeout_s):
            raise StorageError("original feature settlement command remains unconfirmed")
        if ticket["error"] is not None:
            raise ticket["error"]
        return ticket["result"]

    def read_feature_settlement(self, ref, *, host_token):
        return self._generation_control("read", host_token=host_token, ref=ref)

    def ack_feature_settlement(self, ref, ack, *, host_token):
        return self._generation_control("ack", host_token=host_token, ref=ref, ack=ack)

    def confirm_feature_settlement_ack(self, ref, ack, *, host_token):
        """Host calls after consuming the original positive actor ACK."""
        return self._generation_control("confirm", host_token=host_token, ref=ref, ack=ack)

    def _acknowledged_feature_locked(self, ref, ack, *, rebind=False):
        row = self._generation_acknowledged.get(ref.mutation_token)
        if row is not None and (row.outcome.ref != ref or type(ack) is not MutationJournalAck):
            raise ValidationError("acknowledged original feature ACK conflicts")
        if row is not None and row.ack is not ack:
            if (
                ack.state != "terminal"
                or ack.durable is not True
                or ack.ref_sha256 != ref.sha256
                or ack.result_json != row.ack.result_json
                or ack.record_sha256 != row.ack.record_sha256
                or not self._generation_context.verifies_terminal_ack(ref, ack, ack.result_json)
            ):
                raise ValidationError("original reissued feature ACK is unconfirmed")
            if not rebind:
                return None  # Only the original actor changes the retained ACK binding.
            charged = len(self._generation_tickets) + sum(
                not value.confirmed for value in self._generation_acknowledged.values()
            )
            if row.confirmed and charged >= MAX_HOT_MUTATIONS:
                raise FeatureMutationUnknown(ref, "terminal", False)
            row = replace(row, ack=ack, confirmed=False)
            self._generation_acknowledged[ref.mutation_token] = row
        return row

    def _feature_outcome_locked(self, ref):
        outcome = self._generation_outcomes.get(ref.mutation_token)
        row = self._generation_acknowledged.get(ref.mutation_token)
        if outcome is None and row is not None and not row.confirmed:
            outcome = row.outcome
        if outcome is not None and outcome.ref != ref:
            raise ValidationError("original feature outcome reference conflicts")
        return outcome

    def _retain_feature_ack_locked(self, outcome, ack):
        while len(self._generation_acknowledged) >= MAX_HOT_MUTATIONS:
            evict = next(
                (key for key, row in self._generation_acknowledged.items() if row.confirmed), None
            )
            if evict is None:
                raise FeatureMutationUnknown(outcome.ref, "terminal", False)
            del self._generation_acknowledged[evict]
        self._generation_acknowledged[outcome.ref.mutation_token] = _AcknowledgedFeatureOutcome(
            outcome, ack
        )

    def seal_feature_settlement(self, *, host_token):
        return self._generation_control("seal", host_token=host_token)

    def _run_generation_control_actor(self, ticket):
        self._actor()
        try:
            context = self._generation_identity(host_token=ticket["host_token"], ref=ticket["ref"])
            operation, ref = ticket["operation"], ticket["ref"]
            if operation == "seal":
                with self.lock:
                    self._generation_gate_closed = True
                    if any(not row["event"].is_set() for row in self._generation_tickets.values()):
                        raise TradingBlocked("original feature mutations have not drained")
                ticket["result"] = self._capture_generation_settlement_actor(sealed=True)
            elif operation == "read":
                with self.lock:
                    result = self._feature_outcome_locked(ref)
                if result is not None and result.ref == ref:
                    head = self._generation_head_actor()
                    # A late read cannot erase an earlier underlying recording uncertainty.
                    head["recording_unknown"] |= result.recording_unknown
                    head["current_write_confirmed"] &= not result.recording_unknown
                    head["capture_failed"] |= result.capture_failed
                    result = replace(result, **head)
                    with self.lock:
                        row = self._generation_acknowledged.get(ref.mutation_token)
                        if row is not None:
                            self._generation_acknowledged[ref.mutation_token] = replace(
                                row, outcome=result
                            )
                        else:
                            self._generation_outcomes[ref.mutation_token] = result
                else:
                    result = None
                ticket["result"] = result
            elif operation == "ack":
                ack = ticket["ack"]
                with self.lock:
                    acknowledged = self._acknowledged_feature_locked(ref, ack, rebind=True)
                if acknowledged is not None:
                    ticket["result"] = True
                    return
                current_head = self._generation_head_actor()
                with self.lock:
                    outcome = self._generation_outcomes.get(ref.mutation_token)
                if outcome is None or outcome.ref != ref:
                    raise ValidationError("original feature outcome not found")
                result_json = (
                    encoded({"not_dispatched": True})
                    if outcome.phase == "rejected"
                    else outcome.result_json
                    if outcome.phase == "complete"
                    else None
                )
                if (
                    result_json is None
                    or outcome.recording_unknown
                    or outcome.capture_failed
                    or current_head["recording_unknown"]
                    or current_head["capture_failed"]
                    or type(ack) is not MutationJournalAck
                    or ack.state != "terminal"
                    or ack.durable is not True
                    or ack.ref_sha256 != ref.sha256
                    or ack.result_json != result_json
                    or not context.verifies_terminal_ack(ref, ack, result_json)
                ):
                    raise ValidationError("actual original durable terminal ACK required")
                with self.lock:
                    self._retain_feature_ack_locked(outcome, ack)
                    del self._generation_outcomes[ref.mutation_token]
                    self._generation_tickets.pop(ref.mutation_token, None)
                ticket["result"] = True
            elif operation == "confirm":
                with self.lock:
                    row = self._acknowledged_feature_locked(ref, ticket["ack"], rebind=True)
                    if row is None:
                        raise ValidationError("original positive feature ACK not found")
                    self._generation_acknowledged[ref.mutation_token] = replace(row, confirmed=True)
                ticket["result"] = True
            else:
                raise ValidationError("unsupported feature settlement command")
        except Exception as error:
            ticket["error"] = error
        except BaseException:
            ticket["error"] = StorageError("original feature settlement command interrupted")
            raise
        finally:
            ticket["event"].set()

    def _finalize_generation_settlement_actor(self):
        """Final owner retirement fences all future writes before original close."""
        self._actor()
        with self.lock:
            self._generation_gate_closed = True
            pending = tuple(
                row for row in self._generation_tickets.values() if not row["event"].is_set()
            )
        for ticket in pending:
            if ticket["phase"] == "queued":
                ticket["phase"] = "rejected"
                ticket["error"] = FeatureMutationRejected("original_actor_stopped_before_dispatch")
            else:
                ticket["phase"] = "unknown"
                ticket["error"] = FeatureMutationUnknown(ticket["ref"], "dispatch", True)
                self._generation_recording_unknown = True
            try:
                self._record_generation_outcome_actor(ticket)
            except BaseException:
                self._generation_capture_failed = True
                ticket["error"] = FeatureMutationUnknown(ticket["ref"], "dispatch", True)
            finally:
                ticket["event"].set()
        return self._capture_generation_settlement_actor(sealed=True)

    def _sync_actor(self):
        self._actor()
        service = self.service
        current = {}
        if (
            service.active
            and service.owner is not None
            and not service.owner.closed
            and service.activation is not None
            and service._data is not None
        ):
            row = service._data["deployments"][service.active]
            owner = service.owner.snapshot()["state"]
            contract_sha = digest(service.activation.contract.document())
            fingerprint = producer_fingerprint(
                service.activation.model.document(),
                contract_sha,
                service.owner.gate.output_bindings_sha256,
            )
            fields = tuple(value.name for value in service.owner.gate.bindings)
            for stream, state in owner["streams"].items():
                binding = ExternalFeatureBinding(
                    "causal_nn",
                    service.active,
                    stream,
                    self._instrument(state["instrument"]),
                    fingerprint,
                    contract_sha,
                    fields,
                )
                current[binding.binding_sha256] = {
                    "binding": binding,
                    "epochs": (service.core_epoch, service.active, state["epoch"]),
                    "healthy": row["state"] == "ready"
                    and state["health"] == "ready"
                    and stream not in row["uncertain_streams"],
                    "last_open_ms": state["last_open_ms"],
                    "pending_open_ms": next(
                        (
                            value["frame"]["open_time_ms"]
                            for value in owner["pending"].values()
                            if value["stream"] == stream
                        ),
                        None,
                    ),
                }
        with self.lock:
            self._current = current
            self.events.sync_locked(current)

    @staticmethod
    def _instrument(key):
        from omega.infrastructure.market_data import instrument_key

        return instrument_key(key)

    def _restore_actor(self):
        self._actor()
        self._sync_actor()
        bank, latest = {}, {}
        for binding_sha, snapshot_sha in self.store.data["latest"].items():
            snapshot = self.store.load_snapshot(snapshot_sha)
            bank[snapshot_sha] = snapshot
            latest[binding_sha] = snapshot
        pins = self.store.load_pins()
        for pin in pins.values():
            if pin.snapshot_sha256 not in bank:
                bank[pin.snapshot_sha256] = self.store.load_snapshot(pin.snapshot_sha256)
        with self.lock:
            self._bank, self._latest, self._pins = bank, latest, pins

    def list_bindings(self):
        with self.lock:
            return tuple(row["binding"] for row in self._current.values())

    def describe(self, provider_id, instance_id, stream, instrument):
        for binding in self.list_bindings():
            if (binding.provider_id, binding.instance_id, binding.stream, binding.instrument) == (
                provider_id,
                instance_id,
                stream,
                instrument,
            ):
                return binding
        return None

    def _live(self, binding):
        row = self._current.get(binding.binding_sha256)
        service = self.service
        return (
            row
            if row is not None
            and row["binding"] == binding
            and row["healthy"]
            and self.events.allowed_locked(binding, row["epochs"])
            and service.ready
            and not service.closing
            and not service.closed
            and service.error is None
            else None
        )

    def _lookup(self, binding, candle, decision_at_ms):
        now = timestamp(self.service.clock_ms())
        timestamp(decision_at_ms)
        if (
            not isinstance(binding, ExternalFeatureBinding)
            or not isinstance(candle, Candle)
            or candle.instrument != binding.instrument
            or not candle.close_time_ms <= decision_at_ms <= now
        ):
            raise ValidationError(
                "external lookup requires actual closed candle and actual financial cutoff"
            )
        current = self._live(binding)
        epochs = (
            (self.service.core_epoch, binding.instance_id, "unavailable")
            if current is None
            else current["epochs"]
        )
        snapshot = self._latest.get(binding.binding_sha256)
        disposition, reason, proof = "unavailable", "authority_unavailable", None
        if current is not None:
            if snapshot is None:
                disposition, reason = "pending", "no_committed_result"
            elif not snapshot.matches_candle(candle):
                reason = "exact_candle_result_unavailable"
            elif (snapshot.core_epoch, snapshot.deployment_epoch, snapshot.stream_epoch) != epochs:
                reason = "authority_epoch_changed"
            elif snapshot.accepted_at_ms > decision_at_ms:
                disposition, reason = "pending", "result_after_financial_cutoff"
            elif now >= snapshot.valid_until_ms:
                disposition, reason, proof = "expired", "result_expired", snapshot
            else:
                disposition, reason, proof = snapshot.disposition, "committed_result", snapshot
        return ExternalFeatureDelivery(
            binding,
            disposition,
            proof,
            *epochs,
            semantic_candle_sha256(candle),
            candle.open_time_ms,
            decision_at_ms,
            now,
            reason,
        )

    def lookup(self, binding, candle, *, decision_at_ms):
        with self.lock:
            return self._lookup(binding, candle, decision_at_ms)

    def _empty_delivery(self, binding, candle, now, reason, *, disposition="unavailable"):
        with self.lock:
            current = self._current.get(binding.binding_sha256)
            epochs = (
                (self.service.core_epoch, binding.instance_id, "unavailable")
                if current is None
                else current["epochs"]
            )
            return ExternalFeatureDelivery(
                binding,
                disposition,
                None,
                *epochs,
                semantic_candle_sha256(candle),
                candle.open_time_ms,
                now,
                now,
                reason,
            )

    def _unavailable_now(self, binding, candle, now, reason):
        # No cache lock or condition wait: used only for explicit unavailable
        # admission, carrying no snapshot/values and therefore no authority.
        return ExternalFeatureDelivery(
            binding,
            "unavailable",
            None,
            self.service.core_epoch,
            binding.instance_id,
            "unavailable",
            semantic_candle_sha256(candle),
            candle.open_time_ms,
            now,
            now,
            reason,
        )

    def _authorized(self, pin, snapshot, candle, decision_at_ms):
        if (
            not isinstance(pin, ExternalFeaturePin)
            or not isinstance(snapshot, ExternalFeatureSnapshot)
            or self._pins.get(pin.pin_id) != pin
            or self._bank.get(snapshot.snapshot_sha256) != snapshot
            or pin.snapshot_sha256 != snapshot.snapshot_sha256
            or pin.binding_sha256 != snapshot.binding.binding_sha256
            or pin.provider_id != snapshot.binding.provider_id
        ):
            return False
        delivery = self._lookup(snapshot.binding, candle, decision_at_ms)
        return delivery.disposition in {"ready", "no_signal"} and delivery.snapshot == snapshot

    def authorize(self, pin, snapshot, candle, *, decision_at_ms):
        with self.lock:
            return self._authorized(pin, snapshot, candle, decision_at_ms)

    def contains_pin(self, pin):
        """Cached settlement membership only; it grants no current risk authority."""
        if type(pin) is not ExternalFeaturePin:
            return False
        with self.lock:
            if (
                self.service is not self._generation_service
                or self.service.external_features is not self
            ):
                return False
            if self._generation_context is not None:
                try:
                    self._generation_identity()
                    raw = self._generation_raw_store
                    if (
                        self.store is not self._generation_store
                        or self.store.store is not raw
                        or raw._lease_scope is not self._generation_scope
                        or raw._lease_binding is not self._generation_binding
                    ):
                        return False
                except (AttributeError, ValidationError):
                    return False
            return self._pins.get(pin.pin_id) == pin

    @contextmanager
    def entry_lease(self, pin, snapshot, candle, *, decision_at_ms):
        with self.lock:
            yield self._authorized(pin, snapshot, candle, decision_at_ms)

    def _call(self, kind, key, arguments):
        with self.lock:
            ticket = self._tickets.get(key)
            if ticket is None:
                if len(self._tickets) >= 32:
                    raise TradingBlocked("external feature mutation queue is full")
                ticket = {
                    "kind": kind,
                    "key": key,
                    "arguments": arguments,
                    "event": threading.Event(),
                    "result": None,
                    "error": None,
                }
                self._tickets[key] = ticket
                enqueue = True
            else:
                same = ticket["arguments"] == arguments
                if kind == "pin" and ticket["kind"] == kind:
                    old, new = ticket["arguments"], arguments
                    same = old[0].pin_id == new[0].pin_id and old[1:] == new[1:]
                if ticket["kind"] != kind or not same:
                    raise ValidationError("external immutable mutation reference conflicts")
                enqueue = False
        if enqueue and not self.service._enqueue_feature_command(ticket):
            with self.lock:
                self._tickets.pop(key, None)
            raise TradingBlocked("external feature actor is unavailable")
        if not ticket["event"].wait(self.service.ack_timeout_s):
            raise StorageError(
                "external pin/release ACK unconfirmed; retry the same immutable references"
            )
        if ticket["error"] is not None:
            raise ticket["error"]
        return ticket["result"]

    def pin(self, consumer_id, frame_ref, snapshot, candle, *, decision_at_ms):
        identity(consumer_id)
        checksum(frame_ref)
        with self.lock:
            delivery = self._lookup(snapshot.binding, candle, decision_at_ms)
            if (
                delivery.snapshot != snapshot
                or delivery.disposition not in {"ready", "no_signal"}
                or self._bank.get(snapshot.snapshot_sha256) != snapshot
            ):
                raise TradingBlocked("parsed snapshot has no actual current external authority")
        expected = ExternalFeaturePin(
            snapshot.binding.provider_id,
            consumer_id,
            frame_ref,
            snapshot.binding.binding_sha256,
            snapshot.snapshot_sha256,
            self.service.clock_ms(),
        )
        return self._call("pin", expected.pin_id, (expected, snapshot, candle, decision_at_ms))

    def release(self, pin, *, consumer_receipt_sha256):
        checksum(consumer_receipt_sha256)
        return self._call(
            "release", pin.pin_id + ":" + consumer_receipt_sha256, (pin, consumer_receipt_sha256)
        )

    def _run_command_actor(self, ticket):
        self._actor()
        if ticket["kind"] == "generation_mutation":
            return self._run_generation_mutation_actor(ticket)
        if ticket["kind"] == "generation_control":
            return self._run_generation_control_actor(ticket)
        try:
            if ticket["kind"] == "pin":
                expected, snapshot, candle, decision = ticket["arguments"]
                with self.lock:
                    delivery = self._lookup(snapshot.binding, candle, decision)
                    if (
                        delivery.snapshot != snapshot
                        or self._bank.get(snapshot.snapshot_sha256) != snapshot
                    ):
                        raise TradingBlocked("external authority changed before pin recording")
                self.store.load_snapshot(snapshot.snapshot_sha256)
                pin = self.store.pin(expected)
                pins = self.store.load_pins()
                with self.lock:
                    self._pins = pins
                ticket["result"] = pin
            elif ticket["kind"] == "release":
                pin, receipt_sha = ticket["arguments"]
                self.store.release(pin, receipt_sha)
                with self.lock:
                    self._pins.pop(pin.pin_id, None)
                ticket["result"] = None
            else:
                raise ValidationError("unsupported external feature mutation")
        except Exception as error:
            ticket["error"] = (
                StorageError("external feature file recording is unconfirmed")
                if isinstance(error, OSError)
                else error
            )
        finally:
            with self.lock:
                self._tickets.pop(ticket["key"], None)
            ticket["event"].set()
        if isinstance(ticket["error"], StorageError):
            self.service._fault("EXTERNAL_FEATURE_RECORDING_UNCONFIRMED")

    @_recording_boundary
    def _before_dispatch_actor(self, request):
        self._actor()
        path = self.service.registry.root / (request.request_id + ".json")
        evidence = read_document(path)
        validate_input_document(
            evidence, request, self.service.activation, now_ms=self.service._now()
        )
        event = self.service._dispatch_feature_event
        if event is not None:
            last = MarketRow.parse(evidence["snapshot"]["observations"][-1]["row"])
            actual = Candle(
                request.frame.instrument,
                last.open_time_ms,
                last.open_time_ms + 60000,
                last.open,
                last.high,
                last.low,
                last.close,
                last.volume,
            )
            if semantic_candle_sha256(actual) != semantic_candle_sha256(event["candle"]):
                raise TradingBlocked("internal event candle differs from actual committed input")
            event["request_id"] = request.request_id
        self.store.register_input(request.request_id, path.relative_to(self.service.root))
        self._proofs[request.request_id] = request, path.relative_to(self.service.root), evidence

    def _receipt_actor(self, receipt):
        self._actor()
        if isinstance(receipt, InferenceReceipt) and receipt.request_id in self._proofs:
            self._receipts[receipt.request_id] = receipt

    @_recording_boundary
    def _accepted_actor(self, request_id, feature):
        self._actor()
        request, input_ref, evidence = self._proofs[request_id]
        receipt = self._receipts[request_id]
        evidence = read_document(self.service.root / input_ref)
        validate_input_document(
            evidence, request, self.service.activation, now_ms=self.service._now()
        )
        state = self.service.owner.snapshot()["state"]["streams"][request.stream]
        binding = next(
            row["binding"]
            for row in self._current.values()
            if row["binding"].stream == request.stream
        )
        last = MarketRow.parse(evidence["snapshot"]["observations"][-1]["row"])
        candle = Candle(
            binding.instrument,
            last.open_time_ms,
            last.open_time_ms + 60000,
            last.open,
            last.high,
            last.low,
            last.close,
            last.volume,
        )
        snapshot = ExternalFeatureSnapshot(
            binding,
            request.core_epoch,
            request.deployment_epoch,
            request.stream_epoch,
            request_id,
            request.frame.input_sha256,
            digest(request.frame.document()),
            digest(receipt.document()),
            digest(evidence),
            semantic_candle_sha256(candle),
            request.frame.open_time_ms,
            request.decision_at_ms,
            state["accepted_at_ms"],
            request.deadline_ms,
            "no_signal" if feature is None else "ready",
            () if feature is None else feature.values,
        )
        mappings = [
            {"name": row.name, "path": list(row.path)} for row in self.service.owner.gate.bindings
        ]
        self.store.record(
            snapshot, input_ref=input_ref, receipt=receipt.document(), output_bindings=mappings
        )
        # Publish the actual durable gate cursor with the proof, avoiding a stale
        # pending header when a consumer immediately offers the next minute.
        self._sync_actor()
        with self.lock:
            self._latest[binding.binding_sha256] = snapshot
            self._bank[snapshot.snapshot_sha256] = snapshot
            keep = {value.snapshot_sha256 for value in self._latest.values()} | {
                value.snapshot_sha256 for value in self._pins.values()
            }
            self._bank = {key: value for key, value in self._bank.items() if key in keep}
        self._forgot_actor(request_id)

    def _forgot_actor(self, request_id):
        self._proofs.pop(request_id, None)
        self._receipts.pop(request_id, None)

    def _close_actor(self):
        self._actor()
        self._fail_waiters_actor()
        if self.store is not None:
            self.store.close()

    def _fail_waiters_actor(self):
        self._actor()
        with self.lock:
            for ticket in self._tickets.values():
                ticket["error"] = StorageError("external feature owner stopped before ACK")
                ticket["event"].set()
            self._tickets.clear()
            self._current.clear()

    def offer_committed(self, binding, candle):
        return self.events.offer(binding, candle)
